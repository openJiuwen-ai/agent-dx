//! Bounded measurements for the HIXL runtime and its protocol phases.

use afs_metrics::{
    Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, MetricsError,
    Registry, register_collector,
};
use afs_transport::hixl::{HixlError, HixlErrorKind, HixlReadTiming, RuntimeStatus};

/// Reports one phase into the optional bounded HIXL metrics.
pub(crate) fn observe_optional(
    metrics: Option<&HixlMetrics>,
    phase: Operation,
    elapsed: std::time::Duration,
    success: bool,
) {
    if let Some(metrics) = metrics {
        metrics.observe(phase, elapsed, success);
    }
}

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
        HixlErrorKind::InvalidState | HixlErrorKind::NativeFailure => {
            afs_error::NODE_TRANSFER_UNAVAILABLE
        }
    };
    afs_error::Error::coded(code, error.to_string())
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
    /// One periodic source-session expiry pass, including any quarantine.
    Expire,
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
            Self::Expire => "expire",
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
    ownerfs_reads: IntCounterVec,
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
            let ownerfs_reads = IntCounterVec::new(
                afs_metrics::Opts::new(
                    "afs_ownerfs_hixl_reads_total",
                    "OwnerFs reads of eligible read-only opens by window outcome.",
                ),
                &["outcome"],
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
            register_collector(registry, &ownerfs_reads)?;
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
                ownerfs_reads,
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

    /// Records a small preferred-mode batch kept on gRPC while warming HIXL.
    /// Records a CRC32C range the native window protocol cannot verify.
    /// Records one OwnerFs window read outcome; labels are a fixed set.
    pub fn observe_ownerfs_read(&self, outcome: &'static str) {
        self.ownerfs_reads.with_label_values(&[outcome]).inc();
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
