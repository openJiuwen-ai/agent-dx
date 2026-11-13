//! GC progress for operators: Prometheus metrics of collection and reference backlog.
use super::{GcState, MIN_BATCH_KEYS, MetaEntity, Result};
use std::sync::Arc;

/// A pass or generation ranges from milliseconds to minutes under load.
const GC_DURATION_BUCKETS: [f64; 12] = [
    0.001, 0.01, 0.05, 0.1, 0.5, 1.0, 2.5, 5.0, 15.0, 60.0, 300.0, 1800.0,
];
/// Warn when dead keys exist and nothing was collected for this long.
const GC_STALL_WARN_SECONDS: u64 = 60;
/// Process-local GC progress for operators. A generation that stays open with
/// waiting nodes is the stall to alert on (`generation_age_seconds`); it is
/// refreshed by every collection call, and any live storage node calls every
/// few seconds. Restarting Meta restarts the age.
#[derive(Clone)]
pub(crate) struct GcMetrics {
    pub(crate) generation: afs_metrics::IntGauge,
    pub(crate) waiting_nodes: afs_metrics::IntGauge,
    pub(crate) pending_chunks: afs_metrics::IntGauge,
    pub(crate) generation_age_seconds: afs_metrics::IntGauge,
    pub(crate) dropped_nodes: afs_metrics::IntCounter,
    pub(crate) retired_references: afs_metrics::IntCounter,
    /// One per `collect_garbage` call.
    pub(crate) passes: afs_metrics::IntCounter,
    /// One per snapshot-and-plan attempt inside a pass.
    pub(crate) attempts: afs_metrics::IntCounter,
    /// Attempts whose commit lost a conflict and had to start over.
    pub(crate) conflicts: afs_metrics::IntCounter,
    /// Passes that gave up after the last attempt ("contention").
    pub(crate) exhausted_passes: afs_metrics::IntCounter,
    pub(crate) pass_duration: afs_metrics::Histogram,
    pub(crate) generation_duration: afs_metrics::Histogram,
    /// References that still protect their inode without a retirement time.
    pub(crate) live_references: afs_metrics::IntGauge,
    /// Retired reference records kept for grace and replay fencing.
    pub(crate) retired_reference_records: afs_metrics::IntGauge,
    /// Authority entities one planning attempt had to look at.
    pub(crate) planned_entities: afs_metrics::IntGauge,
    /// Placements repair claims looked at; idle cost must not follow placements.
    pub(crate) repair_placements_checked: afs_metrics::IntCounter,
    /// Keys the last plan found dead that the pass did not delete.
    pub(crate) dead_keys: afs_metrics::IntGauge,
    /// How long dead keys have waited with nothing collected; alert on it.
    pub(crate) stalled_seconds: afs_metrics::IntGauge,
    /// Commits refused for size; each halves the batch for the next attempt.
    pub(crate) rejected_batches: afs_metrics::IntCounter,
    /// In-memory reader registrations (round 2 C-1) the last plan saw.
    pub(crate) reader_registrations: afs_metrics::IntGauge,
    /// 1 while the restart gate keeps every inode and version (C-1).
    pub(crate) reader_gate_closed: afs_metrics::IntGauge,
    pub(super) last_collected: Arc<std::sync::Mutex<Option<std::time::Instant>>>,
    open_generation: Arc<std::sync::Mutex<Option<(u64, std::time::Instant)>>>,
}

impl GcMetrics {
    pub(crate) fn new() -> std::result::Result<Self, afs_metrics::MetricsError> {
        Ok(Self {
            generation: afs_metrics::IntGauge::new(
                "afs_meta_dfs_gc_generation",
                "Current DFS GC generation",
            )?,
            waiting_nodes: afs_metrics::IntGauge::new(
                "afs_meta_dfs_gc_waiting_nodes",
                "Live storage nodes that have not finished the current DFS GC generation",
            )?,
            pending_chunks: afs_metrics::IntGauge::new(
                "afs_meta_dfs_gc_pending_chunks",
                "Chunks behind the current DFS GC deletion barrier",
            )?,
            generation_age_seconds: afs_metrics::IntGauge::new(
                "afs_meta_dfs_gc_generation_age_seconds",
                "How long this Meta process has seen the current DFS GC generation waiting; 0 when none waits",
            )?,
            dropped_nodes: afs_metrics::IntCounter::new(
                "afs_meta_dfs_gc_dropped_nodes_total",
                "Nodes removed from a DFS GC generation's waiting set because their session expired",
            )?,
            retired_references: afs_metrics::IntCounter::new(
                "afs_meta_dfs_gc_retired_references_total",
                "GC references retired because their node session is no longer current and live",
            )?,
            passes: afs_metrics::IntCounter::new(
                "afs_meta_dfs_gc_passes_total",
                "DFS GC collection calls",
            )?,
            attempts: afs_metrics::IntCounter::new(
                "afs_meta_dfs_gc_attempts_total",
                "DFS GC planning attempts, including those repeated after a conflict",
            )?,
            conflicts: afs_metrics::IntCounter::new(
                "afs_meta_dfs_gc_conflicts_total",
                "DFS GC commits that lost a conflict and started over",
            )?,
            exhausted_passes: afs_metrics::IntCounter::new(
                "afs_meta_dfs_gc_exhausted_passes_total",
                "DFS GC collection calls that gave up after repeated conflicts",
            )?,
            pass_duration: afs_metrics::Histogram::with_opts(
                afs_metrics::HistogramOpts::new(
                    "afs_meta_dfs_gc_pass_duration_seconds",
                    "Duration of one DFS GC collection call",
                )
                .buckets(GC_DURATION_BUCKETS.to_vec()),
            )?,
            generation_duration: afs_metrics::Histogram::with_opts(
                afs_metrics::HistogramOpts::new(
                    "afs_meta_dfs_gc_generation_duration_seconds",
                    "How long this Meta process saw a DFS GC generation waiting for nodes",
                )
                .buckets(GC_DURATION_BUCKETS.to_vec()),
            )?,
            live_references: afs_metrics::IntGauge::new(
                "afs_meta_dfs_gc_live_references",
                "GC references without a retirement time, as of the last planning attempt",
            )?,
            retired_reference_records: afs_metrics::IntGauge::new(
                "afs_meta_dfs_gc_retired_reference_records",
                "Retired GC reference records still kept, as of the last planning attempt",
            )?,
            planned_entities: afs_metrics::IntGauge::new(
                "afs_meta_dfs_gc_planned_entities",
                "Authority entities read by the last DFS GC planning attempt",
            )?,
            repair_placements_checked: afs_metrics::IntCounter::new(
                "afs_meta_dfs_repair_placements_checked_total",
                "Placements looked at by DFS replication task claims",
            )?,
            dead_keys: afs_metrics::IntGauge::new(
                "afs_meta_dfs_gc_dead_keys",
                "Keys the last DFS GC plan found dead and has not deleted yet",
            )?,
            stalled_seconds: afs_metrics::IntGauge::new(
                "afs_meta_dfs_gc_stalled_seconds",
                "How long dead keys have waited with nothing collected; 0 when none wait",
            )?,
            rejected_batches: afs_metrics::IntCounter::new(
                "afs_meta_dfs_gc_rejected_batches_total",
                "DFS GC commits refused for size, each halving the batch",
            )?,
            reader_registrations: afs_metrics::IntGauge::new(
                "afs_meta_dfs_reader_registrations",
                "In-memory DFS reader registrations the last GC plan saw",
            )?,
            reader_gate_closed: afs_metrics::IntGauge::new(
                "afs_meta_dfs_gc_reader_gate_closed",
                "1 while GC keeps every inode and version until Node sessions report their readers after a Meta restart",
            )?,
            last_collected: Arc::new(std::sync::Mutex::new(None)),
            open_generation: Arc::new(std::sync::Mutex::new(None)),
        })
    }

    pub(crate) fn register(
        &self,
        registry: &afs_metrics::Registry,
    ) -> std::result::Result<(), afs_metrics::MetricsError> {
        afs_metrics::register_collector(registry, &self.generation)?;
        afs_metrics::register_collector(registry, &self.waiting_nodes)?;
        afs_metrics::register_collector(registry, &self.pending_chunks)?;
        afs_metrics::register_collector(registry, &self.generation_age_seconds)?;
        afs_metrics::register_collector(registry, &self.dropped_nodes)?;
        afs_metrics::register_collector(registry, &self.retired_references)?;
        afs_metrics::register_collector(registry, &self.passes)?;
        afs_metrics::register_collector(registry, &self.attempts)?;
        afs_metrics::register_collector(registry, &self.conflicts)?;
        afs_metrics::register_collector(registry, &self.exhausted_passes)?;
        afs_metrics::register_collector(registry, &self.pass_duration)?;
        afs_metrics::register_collector(registry, &self.generation_duration)?;
        afs_metrics::register_collector(registry, &self.live_references)?;
        afs_metrics::register_collector(registry, &self.retired_reference_records)?;
        afs_metrics::register_collector(registry, &self.planned_entities)?;
        afs_metrics::register_collector(registry, &self.repair_placements_checked)?;
        afs_metrics::register_collector(registry, &self.dead_keys)?;
        afs_metrics::register_collector(registry, &self.stalled_seconds)?;
        afs_metrics::register_collector(registry, &self.rejected_batches)?;
        afs_metrics::register_collector(registry, &self.reader_registrations)?;
        afs_metrics::register_collector(registry, &self.reader_gate_closed)
    }

    pub(super) fn observe_readers(&self, registrations: usize, gate_closed: bool) {
        self.reader_registrations
            .set(i64::try_from(registrations).unwrap_or(i64::MAX));
        self.reader_gate_closed.set(i64::from(gate_closed));
    }

    /// Something was collected, or nothing was dead.
    pub(super) fn observe_collected(&self) {
        if let Ok(mut last) = self.last_collected.lock() {
            *last = Some(std::time::Instant::now());
        }
        self.stalled_seconds.set(0);
    }

    /// Refreshed by every pass, failed or not: stalled time is what an operator
    /// has to see when collection stops working.
    pub(super) fn observe_stall(&self) {
        let Ok(mut last) = self.last_collected.lock() else {
            return;
        };
        let since = *last.get_or_insert_with(std::time::Instant::now);
        if self.dead_keys.get() == 0 {
            return;
        }
        let stalled = since.elapsed().as_secs();
        let previous = self.stalled_seconds.get();
        self.stalled_seconds
            .set(i64::try_from(stalled).unwrap_or(i64::MAX));
        if stalled >= GC_STALL_WARN_SECONDS
            && i64::try_from(GC_STALL_WARN_SECONDS).is_ok_and(|warn| previous < warn)
        {
            afs_logging::warn!("dfs.gc_stalled";
                "seconds" => stalled, "dead_keys" => self.dead_keys.get());
        }
    }

    /// A commit refused for capacity (too large for the input queue, or the
    /// queue staying busy) gets a smaller batch; anything else, or the
    /// smallest batch being refused, is the caller's error.
    pub(super) fn rejected_batch(
        &self,
        error: afs_error::Error,
        batch_keys: usize,
    ) -> Result<usize> {
        if error.code() != afs_error::IO_CAPACITY && error.code() != afs_error::META_STORE_BUSY {
            return Err(error);
        }
        self.rejected_batches.inc();
        if batch_keys <= MIN_BATCH_KEYS {
            return Err(error);
        }
        Ok((batch_keys / 2).max(MIN_BATCH_KEYS))
    }

    pub(super) fn observe_references(&self, entities: &[MetaEntity]) {
        let (mut live, mut retired) = (0_i64, 0_i64);
        for entity in entities {
            if let MetaEntity::DfsReference(reference) = entity {
                if reference.retired_at_unix_ms.is_some() {
                    retired += 1;
                } else {
                    live += 1;
                }
            }
        }
        self.live_references.set(live);
        self.retired_reference_records.set(retired);
        self.planned_entities
            .set(i64::try_from(entities.len()).unwrap_or(i64::MAX));
    }

    pub(super) fn observe(&self, gc: &GcState) {
        self.generation
            .set(i64::try_from(gc.generation).unwrap_or(i64::MAX));
        self.waiting_nodes
            .set(i64::try_from(gc.waiting_nodes.len()).unwrap_or(i64::MAX));
        self.pending_chunks
            .set(i64::try_from(gc.chunks.len()).unwrap_or(i64::MAX));
        let Ok(mut open) = self.open_generation.lock() else {
            return;
        };
        if gc.waiting_nodes.is_empty() {
            if let Some((_, since)) = open.take() {
                self.generation_duration
                    .observe(since.elapsed().as_secs_f64());
            }
            self.generation_age_seconds.set(0);
            return;
        }
        let since = match *open {
            Some((generation, since)) if generation == gc.generation => since,
            previous => {
                if let Some((_, since)) = previous {
                    self.generation_duration
                        .observe(since.elapsed().as_secs_f64());
                }
                let since = std::time::Instant::now();
                *open = Some((gc.generation, since));
                since
            }
        };
        self.generation_age_seconds
            .set(i64::try_from(since.elapsed().as_secs()).unwrap_or(i64::MAX));
    }
}
