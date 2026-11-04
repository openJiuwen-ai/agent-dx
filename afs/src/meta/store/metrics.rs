//! StoreMetrics.

use super::*;

/// Process-local, fixed-cardinality observation only. Collectors retain atomic
/// handles, never a State/Store/backend, and do no I/O or authority scans.
pub(super) struct StoreMetrics {
    requests: afs_metrics::IntGauge,
    pub(super) request_bytes: afs_metrics::IntGauge,
    events: afs_metrics::IntGauge,
    event_bytes: afs_metrics::IntGauge,
    history_index_bytes: afs_metrics::IntGauge,
    pub(super) revision: afs_metrics::IntGauge,
    log_sequence: afs_metrics::IntGauge,
    pub(super) compacted_revision: afs_metrics::IntGauge,
    pub(super) checkpoint_inflight: afs_metrics::IntGauge,
    pub(super) checkpoint_bytes: afs_metrics::IntGauge,
    pub(super) checkpoint_sequence: afs_metrics::IntGauge,
    pub(super) request_evictions: afs_metrics::IntCounter,
    pub(super) event_evictions: afs_metrics::IntCounter,
    pub(super) wal_batches: afs_metrics::IntCounter,
    pub(super) wal_records: afs_metrics::IntCounter,
    pub(super) wal_framed_bytes: afs_metrics::IntCounter,
    pub(super) wal_failures: afs_metrics::IntCounter,
    pub(super) wal_capacity_rejections: afs_metrics::IntCounter,
    pub(super) checkpoints: afs_metrics::IntCounter,
    pub(super) checkpoint_failures: afs_metrics::IntCounter,
    pub(super) checkpoint_duration: afs_metrics::Histogram,
}

impl StoreMetrics {
    pub(super) fn new() -> std::result::Result<Self, afs_metrics::MetricsError> {
        Ok(Self {
            requests: afs_metrics::IntGauge::new(
                "afs_meta_requests_retained",
                "Current retained deduplication outcomes",
            )?,
            request_bytes: afs_metrics::IntGauge::new(
                "afs_meta_request_bytes",
                "Logical retained outcome capacities; not RSS",
            )?,
            events: afs_metrics::IntGauge::new(
                "afs_meta_events_retained",
                "Current retained watch events",
            )?,
            event_bytes: afs_metrics::IntGauge::new(
                "afs_meta_event_bytes",
                "Logical retained watch event capacities; not RSS",
            )?,
            history_index_bytes: afs_metrics::IntGauge::new(
                "afs_meta_history_index_bytes",
                "Fixed logical history index reserve; not RSS",
            )?,
            revision: afs_metrics::IntGauge::new(
                "afs_meta_revision",
                "Latest published business revision",
            )?,
            log_sequence: afs_metrics::IntGauge::new(
                "afs_meta_log_sequence",
                "Latest published canonical record sequence",
            )?,
            compacted_revision: afs_metrics::IntGauge::new(
                "afs_meta_compacted_revision",
                "Published watch compaction cursor",
            )?,
            checkpoint_inflight: afs_metrics::IntGauge::new(
                "afs_meta_checkpoint_inflight",
                "One while the owned checkpoint task is active",
            )?,
            checkpoint_bytes: afs_metrics::IntGauge::new(
                "afs_meta_checkpoint_bytes",
                "Latest recovered or successfully installed checkpoint payload bytes",
            )?,
            checkpoint_sequence: afs_metrics::IntGauge::new(
                "afs_meta_checkpoint_sequence",
                "Latest recovered or successfully installed checkpoint record sequence",
            )?,
            request_evictions: afs_metrics::IntCounter::new(
                "afs_meta_request_evictions_total",
                "Exact outcome removals acknowledged in this process lifetime",
            )?,
            event_evictions: afs_metrics::IntCounter::new(
                "afs_meta_watch_events_evicted_total",
                "Exact watch removals acknowledged in this process lifetime",
            )?,
            wal_batches: afs_metrics::IntCounter::new(
                "afs_meta_wal_batches_total",
                "WAL batches with a successful durable ACK in this process lifetime",
            )?,
            wal_records: afs_metrics::IntCounter::new(
                "afs_meta_wal_records_total",
                "Records in acknowledged WAL batches in this process lifetime",
            )?,
            wal_framed_bytes: afs_metrics::IntCounter::new(
                "afs_meta_wal_framed_bytes_total",
                "Framed bytes in acknowledged appends; not retained disk usage",
            )?,
            wal_failures: afs_metrics::IntCounter::new(
                "afs_meta_wal_failures_total",
                "Append ACK failures; persistence outcome may be unknown",
            )?,
            wal_capacity_rejections: afs_metrics::IntCounter::new(
                "afs_meta_wal_capacity_rejections_total",
                "Capacity waits that expired before any append",
            )?,
            checkpoints: afs_metrics::IntCounter::new(
                "afs_meta_checkpoints_total",
                "Owned checkpoint installations completed successfully",
            )?,
            checkpoint_failures: afs_metrics::IntCounter::new(
                "afs_meta_checkpoint_failures_total",
                "Owned checkpoint tasks ended unsuccessfully; observer timeout is not failure",
            )?,
            checkpoint_duration: afs_metrics::Histogram::with_opts(
                afs_metrics::HistogramOpts::new(
                    "afs_meta_checkpoint_duration_seconds",
                    "Actual owned task duration, including capture and install",
                ),
            )?,
        })
    }
    pub(super) fn register(
        &self,
        registry: &afs_metrics::Registry,
    ) -> std::result::Result<(), afs_metrics::MetricsError> {
        afs_metrics::register_collector(registry, &self.requests)?;
        afs_metrics::register_collector(registry, &self.request_bytes)?;
        afs_metrics::register_collector(registry, &self.events)?;
        afs_metrics::register_collector(registry, &self.event_bytes)?;
        afs_metrics::register_collector(registry, &self.history_index_bytes)?;
        afs_metrics::register_collector(registry, &self.revision)?;
        afs_metrics::register_collector(registry, &self.log_sequence)?;
        afs_metrics::register_collector(registry, &self.compacted_revision)?;
        afs_metrics::register_collector(registry, &self.checkpoint_inflight)?;
        afs_metrics::register_collector(registry, &self.checkpoint_bytes)?;
        afs_metrics::register_collector(registry, &self.checkpoint_sequence)?;
        afs_metrics::register_collector(registry, &self.request_evictions)?;
        afs_metrics::register_collector(registry, &self.event_evictions)?;
        afs_metrics::register_collector(registry, &self.wal_batches)?;
        afs_metrics::register_collector(registry, &self.wal_records)?;
        afs_metrics::register_collector(registry, &self.wal_framed_bytes)?;
        afs_metrics::register_collector(registry, &self.wal_failures)?;
        afs_metrics::register_collector(registry, &self.wal_capacity_rejections)?;
        afs_metrics::register_collector(registry, &self.checkpoints)?;
        afs_metrics::register_collector(registry, &self.checkpoint_failures)?;
        afs_metrics::register_collector(registry, &self.checkpoint_duration)?;
        Ok(())
    }
    pub(super) fn set(gauge: &afs_metrics::IntGauge, value: u64) {
        // Diagnostic-only saturation never changes an already durable outcome.
        gauge.set(i64::try_from(value).unwrap_or(i64::MAX));
    }
    pub(super) fn publish(&self, state: &State) {
        let retained = state.history_stats();
        Self::set(&self.requests, retained.requests as u64);
        Self::set(&self.request_bytes, retained.request_bytes);
        Self::set(&self.events, retained.events as u64);
        Self::set(&self.event_bytes, retained.event_bytes);
        Self::set(&self.history_index_bytes, retained.index_base_bytes);
        Self::set(&self.revision, state.revision.0);
        Self::set(&self.log_sequence, state.log_tip.sequence);
        Self::set(&self.compacted_revision, state.compacted_to.0);
    }
}
