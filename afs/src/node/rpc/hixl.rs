//! Shared Node-side HIXL errors and bounded process metrics.

use afs_metrics::{
    Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, MetricsError,
    Registry, register_collector,
};
use afs_transport::hixl::{HixlError, HixlErrorKind, HixlReadTiming, RuntimeStatus};

pub(crate) const HIXL_PROTOCOL_VERSION: u32 = 1;

pub(crate) fn runtime_error(error: HixlError) -> afs_error::Error {
    let code = match error.kind {
        HixlErrorKind::Unsupported => afs_error::NODE_TRANSFER_UNSUPPORTED,
        HixlErrorKind::InvalidArgument => afs_error::NODE_TRANSFER_INVALID,
        HixlErrorKind::ResourceExhausted => afs_error::NODE_TRANSFER_RESOURCE_EXHAUSTED,
        HixlErrorKind::DeadlineExceeded => afs_error::IO_TIMEOUT,
        HixlErrorKind::EnginePoisoned | HixlErrorKind::ShutdownIncomplete => {
            afs_error::NODE_TRANSFER_DRAIN_INCOMPLETE
        }
        HixlErrorKind::Internal => afs_error::NODE_TRANSFER_INTERNAL,
        HixlErrorKind::InvalidState
        | HixlErrorKind::NativeFailure
        | HixlErrorKind::CompletionFailed => afs_error::NODE_TRANSFER_UNAVAILABLE,
    };
    afs_error::Error::coded(code, error.to_string())
}

#[cfg(test)]
mod tests {
    use super::HixlMetrics;
    use afs_metrics::Registry;
    use afs_transport::hixl::{HixlReadTiming, ReadTransport, RuntimeState, RuntimeStatus};

    #[test]
    fn hbm_budget_is_reported_separately_from_host_allocations() {
        let registry = Registry::new();
        let metrics = HixlMetrics::register(&registry).unwrap();
        let status = RuntimeStatus {
            state: RuntimeState::Running,
            generation: 1,
            allocated_bytes: 32 * 1024 * 1024,
            quarantined_bytes: 0,
            export_slots: 4,
            receive_slots_in_use: 0,
            export_slots_in_use: 0,
            transport: ReadTransport::HccsBufferPool,
            relay_hbm_budget_bytes: 64 * 1024 * 1024,
            hccs_domain_id: Some(7),
        };
        metrics.snapshot(&status);
        let gauges: std::collections::HashMap<_, _> = registry
            .gather()
            .into_iter()
            .map(|family| {
                (
                    family.name().to_owned(),
                    family.get_metric()[0].get_gauge().get_value(),
                )
            })
            .collect();
        assert_eq!(
            gauges["afs_hixl_allocated_bytes"],
            (32 * 1024 * 1024) as f64
        );
        assert_eq!(
            gauges["afs_hixl_relay_hbm_budget_bytes"],
            (64 * 1024 * 1024) as f64
        );
    }

    #[test]
    fn lane_bytes_are_bounded_and_reported_independently() {
        let registry = Registry::new();
        let metrics = HixlMetrics::register(&registry).unwrap();
        metrics.record_lane_validated_bytes(0, 3);
        metrics.record_lane_validated_bytes(1, 5);
        metrics.record_lane_validated_bytes(16, 99);
        let family = registry
            .gather()
            .into_iter()
            .find(|family| family.name() == "afs_hixl_lane_validated_read_bytes_total")
            .unwrap();
        let values = family
            .get_metric()
            .iter()
            .map(|metric| metric.get_counter().get_value())
            .collect::<Vec<_>>();
        assert_eq!(values.len(), 2);
        assert_eq!(values.iter().sum::<f64>(), 8.0);
    }

    #[test]
    fn lane_read_duration_has_bounded_lane_and_result_labels() {
        let registry = Registry::new();
        let metrics = HixlMetrics::register(&registry).unwrap();
        metrics.observe_lane_read(0, std::time::Duration::from_millis(2), true);
        metrics.observe_lane_read(0, std::time::Duration::from_millis(3), false);
        metrics.observe_lane_read(16, std::time::Duration::from_millis(99), true);
        let family = registry
            .gather()
            .into_iter()
            .find(|family| family.name() == "afs_hixl_lane_read_duration_seconds")
            .unwrap();
        assert_eq!(family.get_metric().len(), 2);
        assert_eq!(
            family
                .get_metric()
                .iter()
                .map(|metric| metric.get_histogram().get_sample_count())
                .sum::<u64>(),
            2
        );
        assert!(
            (family
                .get_metric()
                .iter()
                .map(|metric| metric.get_histogram().get_sample_sum())
                .sum::<f64>()
                - 0.005)
                .abs()
                < f64::EPSILON
        );
        assert!(family.get_metric().iter().all(|metric| {
            metric
                .get_label()
                .iter()
                .any(|label| label.name() == "lane" && label.value() == "0")
        }));
    }

    #[test]
    fn native_batch_evidence_reports_calls_descriptors_and_bytes() {
        let registry = Registry::new();
        let metrics = HixlMetrics::register(&registry).unwrap();
        metrics.observe_native_batch(25, 100 * 1024 * 1024);
        let families: std::collections::HashMap<_, _> = registry
            .gather()
            .into_iter()
            .map(|family| (family.name().to_owned(), family))
            .collect();
        assert_eq!(
            families["afs_hixl_reader_native_batch_calls_total"].get_metric()[0]
                .get_counter()
                .get_value(),
            1.0
        );
        assert_eq!(
            families["afs_hixl_reader_native_batch_bytes_total"].get_metric()[0]
                .get_counter()
                .get_value(),
            (100 * 1024 * 1024) as f64
        );
        let descriptors =
            families["afs_hixl_reader_native_batch_descriptors"].get_metric()[0].get_histogram();
        assert_eq!(descriptors.get_sample_count(), 1);
        assert!((descriptors.get_sample_sum() - 25.0).abs() < f64::EPSILON);
        let sizes =
            families["afs_hixl_reader_native_batch_size_bytes"].get_metric()[0].get_histogram();
        assert_eq!(sizes.get_sample_sum(), (100 * 1024 * 1024) as f64);
        let covered = sizes
            .get_bucket()
            .iter()
            .find(|bucket| bucket.upper_bound() == (100 * 1024 * 1024) as f64)
            .map(|bucket| bucket.cumulative_count());
        assert_eq!(covered, Some(1));
    }

    #[test]
    fn source_checksum_reuse_is_counted_separately_from_recomputation() {
        let registry = Registry::new();
        let metrics = HixlMetrics::register(&registry).unwrap();
        metrics.observe_source_checksums(3, 1);
        let families: std::collections::HashMap<_, _> = registry
            .gather()
            .into_iter()
            .map(|family| (family.name().to_owned(), family))
            .collect();
        assert_eq!(
            families["afs_hixl_source_range_checksums_reused_total"].get_metric()[0]
                .get_counter()
                .get_value(),
            3.0
        );
        assert_eq!(
            families["afs_hixl_source_range_checksums_computed_total"].get_metric()[0]
                .get_counter()
                .get_value(),
            1.0
        );
    }

    #[test]
    fn native_reader_timings_skip_unavailable_sdk_and_copy_spans() {
        let registry = Registry::new();
        let metrics = HixlMetrics::register(&registry).unwrap();
        metrics.observe_read_timing(&HixlReadTiming {
            owner_queue: std::time::Duration::from_millis(2),
            sdk_transfer: Some(std::time::Duration::from_millis(3)),
            output_copy: None,
        });

        let samples: std::collections::HashMap<_, _> = registry
            .gather()
            .into_iter()
            .map(|family| {
                (
                    family.name().to_owned(),
                    family
                        .get_metric()
                        .iter()
                        .map(|metric| metric.get_histogram().get_sample_count())
                        .sum::<u64>(),
                )
            })
            .collect();
        assert_eq!(samples["afs_hixl_reader_owner_queue_seconds"], 1);
        assert_eq!(samples["afs_hixl_reader_sdk_transfer_seconds"], 1);
        assert_eq!(samples["afs_hixl_reader_output_copy_seconds"], 0);
    }
}

/// Labels are fixed phases, never Node IDs, addresses, chunks or grants.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    Negotiate,
    Authorize,
    Prepare,
    SourceRead,
    Connect,
    Read,
    Validate,
    Release,
    Close,
    Fallback,
}

impl Operation {
    fn label(self) -> &'static str {
        match self {
            Self::Negotiate => "negotiate",
            Self::Authorize => "authorize",
            Self::Prepare => "prepare",
            Self::SourceRead => "source_read",
            Self::Connect => "connect",
            Self::Read => "read",
            Self::Validate => "validate",
            Self::Release => "release",
            Self::Close => "close",
            Self::Fallback => "fallback",
        }
    }
}

#[derive(Clone)]
pub struct HixlMetrics {
    duration: HistogramVec,
    payload: IntCounter,
    lane_payload: IntCounterVec,
    lane_read_duration: HistogramVec,
    reader_owner_queue: Histogram,
    reader_sdk_transfer: Histogram,
    reader_output_copy: Histogram,
    reader_native_batch_calls: IntCounter,
    reader_native_batch_bytes: IntCounter,
    reader_native_batch_descriptors: Histogram,
    reader_native_batch_size_bytes: Histogram,
    source_checksums_reused: IntCounter,
    source_checksums_computed: IntCounter,
    cold_fallbacks: IntCounter,
    allocated: IntGauge,
    relay_hbm_budget: IntGauge,
    quarantined: IntGauge,
    receive_in_use: IntGauge,
    export_in_use: IntGauge,
}

impl HixlMetrics {
    pub fn register(registry: &Registry) -> Result<Self, MetricsError> {
        registry.get_or_register(|registry| {
            let duration = HistogramVec::new(
                HistogramOpts::new(
                    "afs_hixl_operation_duration_seconds",
                    "HIXL control, native READ and validation duration.",
                ),
                &["phase", "result"],
            )?;
            let payload = IntCounter::new(
                "afs_hixl_validated_read_bytes_total",
                "HIXL logical read bytes after every range checksum succeeds.",
            )?;
            let lane_payload = IntCounterVec::new(
                afs_metrics::Opts::new(
                    "afs_hixl_lane_validated_read_bytes_total",
                    "HIXL validated bytes by bounded configured lane index.",
                ),
                &["lane"],
            )?;
            let lane_read_duration = HistogramVec::new(
                HistogramOpts::new(
                    "afs_hixl_lane_read_duration_seconds",
                    "Native READ and scheduling duration per configured lane; spans can overlap.",
                ),
                &["lane", "result"],
            )?;
            let reader_owner_queue = Histogram::with_opts(HistogramOpts::new(
                "afs_hixl_reader_owner_queue_seconds",
                "Time from enqueue to dequeue for each prepared HIXL reader command.",
            ))?;
            let reader_sdk_transfer = Histogram::with_opts(HistogramOpts::new(
                "afs_hixl_reader_sdk_transfer_seconds",
                "Native HIXL SDK transfer duration when the transport reports it.",
            ))?;
            let reader_output_copy = Histogram::with_opts(HistogramOpts::new(
                "afs_hixl_reader_output_copy_seconds",
                "Native copy from the HIXL ACL Host window into the reader output buffer.",
            ))?;
            let reader_native_batch_calls = IntCounter::new(
                "afs_hixl_reader_native_batch_calls_total",
                "Successful native HIXL batch READ submissions.",
            )?;
            let reader_native_batch_bytes = IntCounter::new(
                "afs_hixl_reader_native_batch_bytes_total",
                "Bytes submitted through successful native HIXL batch READ calls.",
            )?;
            let reader_native_batch_descriptors = Histogram::with_opts(
                HistogramOpts::new(
                    "afs_hixl_reader_native_batch_descriptors",
                    "Descriptor count of each successful native HIXL batch READ call.",
                )
                .buckets(vec![1.0, 2.0, 4.0, 8.0, 16.0, 25.0, 32.0, 48.0, 64.0]),
            )?;
            let reader_native_batch_size_bytes = Histogram::with_opts(
                HistogramOpts::new(
                    "afs_hixl_reader_native_batch_size_bytes",
                    "Submitted bytes of each successful native HIXL batch READ call.",
                )
                .buckets(vec![
                    4.0 * 1024.0 * 1024.0,
                    8.0 * 1024.0 * 1024.0,
                    16.0 * 1024.0 * 1024.0,
                    32.0 * 1024.0 * 1024.0,
                    64.0 * 1024.0 * 1024.0,
                    100.0 * 1024.0 * 1024.0,
                    128.0 * 1024.0 * 1024.0,
                    256.0 * 1024.0 * 1024.0,
                ]),
            )?;
            let source_checksums_reused = IntCounter::new(
                "afs_hixl_source_range_checksums_reused_total",
                "Source range checksums reused from the verified batch instead of rehashed.",
            )?;
            let source_checksums_computed = IntCounter::new(
                "afs_hixl_source_range_checksums_computed_total",
                "Source range checksums recomputed because the window splits a batch range.",
            )?;
            let cold_fallbacks = IntCounter::new(
                "afs_hixl_cold_tcp_fallbacks_total",
                "Preferred-mode small batches served by gRPC while the peer session warms.",
            )?;
            let allocated = IntGauge::new(
                "afs_hixl_allocated_bytes",
                "Fixed ACL Host pool allocation.",
            )?;
            let relay_hbm_budget = IntGauge::new(
                "afs_hixl_relay_hbm_budget_bytes",
                "Configured capacity of both HIXL relay HBM pools, not measured usage.",
            )?;
            let quarantined = IntGauge::new(
                "afs_hixl_quarantined_bytes",
                "Host slots retained after uncertain native or control outcomes.",
            )?;
            let receive_in_use = IntGauge::new(
                "afs_hixl_receive_slots_in_use",
                "Admitted or quarantined receive slots.",
            )?;
            let export_in_use = IntGauge::new(
                "afs_hixl_export_slots_in_use",
                "Admitted or quarantined export slots.",
            )?;
            register_collector(registry, &duration)?;
            register_collector(registry, &payload)?;
            register_collector(registry, &lane_payload)?;
            register_collector(registry, &lane_read_duration)?;
            register_collector(registry, &reader_owner_queue)?;
            register_collector(registry, &reader_sdk_transfer)?;
            register_collector(registry, &reader_output_copy)?;
            register_collector(registry, &reader_native_batch_calls)?;
            register_collector(registry, &reader_native_batch_bytes)?;
            register_collector(registry, &reader_native_batch_descriptors)?;
            register_collector(registry, &reader_native_batch_size_bytes)?;
            register_collector(registry, &source_checksums_reused)?;
            register_collector(registry, &source_checksums_computed)?;
            register_collector(registry, &cold_fallbacks)?;
            for gauge in [
                &allocated,
                &relay_hbm_budget,
                &quarantined,
                &receive_in_use,
                &export_in_use,
            ] {
                register_collector(registry, gauge)?;
            }
            Ok(Self {
                duration,
                payload,
                lane_payload,
                lane_read_duration,
                reader_owner_queue,
                reader_sdk_transfer,
                reader_output_copy,
                reader_native_batch_calls,
                reader_native_batch_bytes,
                reader_native_batch_descriptors,
                reader_native_batch_size_bytes,
                source_checksums_reused,
                source_checksums_computed,
                cold_fallbacks,
                allocated,
                relay_hbm_budget,
                quarantined,
                receive_in_use,
                export_in_use,
            })
        })
    }

    pub fn observe(&self, operation: Operation, elapsed: std::time::Duration, success: bool) {
        self.duration
            .with_label_values(&[operation.label(), if success { "ok" } else { "error" }])
            .observe(elapsed.as_secs_f64());
    }

    pub fn record_validated_bytes(&self, bytes: u64) {
        self.payload.inc_by(bytes);
    }

    pub fn record_lane_validated_bytes(&self, lane: usize, bytes: u64) {
        if lane < 16 {
            self.lane_payload
                .with_label_values(&[&lane.to_string()])
                .inc_by(bytes);
        }
    }

    pub fn observe_lane_read(&self, lane: usize, elapsed: std::time::Duration, success: bool) {
        if lane < 16 {
            self.lane_read_duration
                .with_label_values(&[
                    lane.to_string().as_str(),
                    if success { "ok" } else { "error" },
                ])
                .observe(elapsed.as_secs_f64());
        }
    }

    /// Records native reader spans only when the owner can measure them.
    pub fn observe_read_timing(&self, timing: &HixlReadTiming) {
        self.reader_owner_queue
            .observe(timing.owner_queue.as_secs_f64());
        if let Some(elapsed) = timing.sdk_transfer {
            self.reader_sdk_transfer.observe(elapsed.as_secs_f64());
        }
        if let Some(elapsed) = timing.output_copy {
            self.reader_output_copy.observe(elapsed.as_secs_f64());
        }
    }

    /// Records one successful native batch READ submission.
    pub fn observe_native_batch(&self, descriptors: usize, bytes: usize) {
        self.reader_native_batch_calls.inc();
        self.reader_native_batch_descriptors
            .observe(descriptors as f64);
        self.reader_native_batch_size_bytes.observe(bytes as f64);
        self.reader_native_batch_bytes
            .inc_by(u64::try_from(bytes).unwrap_or(u64::MAX));
    }

    /// Records source range checksums that did not need a second hash pass.
    pub fn observe_source_checksums(&self, reused: usize, computed: usize) {
        self.source_checksums_reused
            .inc_by(u64::try_from(reused).unwrap_or(u64::MAX));
        self.source_checksums_computed
            .inc_by(u64::try_from(computed).unwrap_or(u64::MAX));
    }

    /// Records a small preferred-mode batch kept on gRPC while warming HIXL.
    pub fn observe_cold_fallback(&self) {
        self.cold_fallbacks.inc();
    }

    pub fn snapshot(&self, status: &RuntimeStatus) {
        // Runtime limits are <=512 MiB and <=64 slots. Saturation also keeps this
        // diagnostic boundary safe if a future runtime broadens those budgets.
        self.allocated
            .set(i64::try_from(status.allocated_bytes).unwrap_or(i64::MAX));
        self.relay_hbm_budget
            .set(i64::try_from(status.relay_hbm_budget_bytes).unwrap_or(i64::MAX));
        self.quarantined
            .set(i64::try_from(status.quarantined_bytes).unwrap_or(i64::MAX));
        self.receive_in_use
            .set(i64::try_from(status.receive_slots_in_use).unwrap_or(i64::MAX));
        self.export_in_use
            .set(i64::try_from(status.export_slots_in_use).unwrap_or(i64::MAX));
    }
}
pub(crate) mod batch;
