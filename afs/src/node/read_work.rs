//! Fixed-cardinality work counters for immutable DFS reads, never authority or budgets.
//!
//! Local integrity scans also serve the diagnostic Chunk store without DFS.
//! Peer transport, disk-cache and request journals are compiled only for DFS
//! (or their direct unit tests), without changing that shared verification path.

#[cfg(any(feature = "dfs", test))]
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

#[cfg(any(feature = "dfs", test))]
const FILL_STAGES: [&str; 7] = [
    "lock_wait",
    "lookup",
    "prepare",
    "receive_verify",
    "store",
    "deliver",
    "total",
];

/// Low-frequency synchronous scope clock. CPU belongs only to its calling
/// thread, not transport workers or remote nodes. Failure never affects reads.
#[derive(Clone, Copy)]
#[cfg(any(feature = "dfs", test))]
pub(crate) struct PeerFillClock {
    wall: std::time::Instant,
    cpu: Option<u64>,
    thread: std::thread::ThreadId,
}

#[cfg(any(feature = "dfs", test))]
impl PeerFillClock {
    pub(crate) fn start() -> Self {
        let wall = std::time::Instant::now();
        Self {
            wall,
            cpu: thread_cpu_ns(),
            thread: std::thread::current().id(),
        }
    }

    pub(crate) fn elapsed(&self) -> (u64, Option<u64>) {
        let cpu = if self.thread == std::thread::current().id() {
            self.cpu
                .and_then(|before| thread_cpu_ns()?.checked_sub(before))
        } else {
            None
        };
        let wall = u64::try_from(self.wall.elapsed().as_nanos()).unwrap_or(u64::MAX);
        (wall, cpu)
    }
}

#[cfg(any(feature = "dfs", test))]
fn thread_cpu_ns() -> Option<u64> {
    let mut value = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: value is writable, properly aligned and lives through this call.
    // CLOCK_THREAD_CPUTIME_ID reads the current thread and retains no pointer.
    if unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut value) } != 0 {
        return None;
    }
    let seconds = u64::try_from(value.tv_sec).ok()?;
    let nanos = u64::try_from(value.tv_nsec)
        .ok()
        .filter(|n| *n < 1_000_000_000)?;
    seconds.checked_mul(1_000_000_000)?.checked_add(nanos)
}

#[cfg(feature = "dfs")]
const NATIVE_WRITE_RESOURCES: &[&str] = &[
    "live_bytes",
    "live_peak_bytes",
    "live_limit_bytes",
    "live_leases",
    "live_waiters",
    "stopped",
    "descriptor_bytes",
    "descriptor_peak_bytes",
    "descriptors",
    "descriptor_peak_count",
    "descriptor_reserved_bytes",
    "publication_scratch_reserved_bytes",
    "control_bytes",
    "control_peak_bytes",
    "control_reserve_bytes",
    "upload_inflight",
    "upload_peak",
    "upload_limit",
    "pending_generations",
    "pressure_descriptor",
    "pressure_live",
    "completed_uploads",
    "failed_uploads",
    "metadata_limit_bytes",
    "descriptor_limit_bytes",
];

#[derive(Clone, Debug)]
pub struct ReadWorkMetrics {
    #[cfg(feature = "dfs")]
    native_write_resources: afs_metrics::IntGaugeVec,
    catalog_commit_resources: afs_metrics::IntGaugeVec,
    scan_bytes: afs_metrics::IntCounter,
    hashes: afs_metrics::IntCounterVec,
    outboard_total_bytes: afs_metrics::IntGauge,
    outboard_resident_bytes: afs_metrics::IntGauge,
    outboard_total_limit: afs_metrics::IntGauge,
    outboard_resident_limit: afs_metrics::IntGauge,
    outboard_events: afs_metrics::IntCounterVec,
    sidecar_install_items: afs_metrics::IntGaugeVec,
    sidecar_install_control_bytes: afs_metrics::IntGauge,
    #[cfg(any(feature = "dfs", test))]
    disk_cache_metadata_bytes: afs_metrics::IntGauge,
    #[cfg(any(feature = "dfs", test))]
    disk_cache_metadata_peak_bytes: afs_metrics::IntGauge,
    #[cfg(feature = "dfs")]
    disk_cache_resources: afs_metrics::IntGaugeVec,
    scans: afs_metrics::IntCounter,
    copies: afs_metrics::IntCounterVec,
    #[cfg(any(feature = "dfs", test))]
    peer_payload_bytes: afs_metrics::IntGauge,
    #[cfg(any(feature = "dfs", test))]
    peer_payload_peak_bytes: afs_metrics::IntGauge,
    #[cfg(any(feature = "dfs", test))]
    peer_payload_reserved: afs_metrics::IntCounter,
    #[cfg(any(feature = "dfs", test))]
    peer_payload_released: afs_metrics::IntCounter,
    #[cfg(any(feature = "dfs", test))]
    peer_object_cache: afs_metrics::IntCounterVec,
    #[cfg(any(feature = "dfs", test))]
    peer_serve_resources: afs_metrics::IntGaugeVec,
    #[cfg(feature = "dfs")]
    source_cache_resources: afs_metrics::IntGaugeVec,
    #[cfg(any(feature = "dfs", test))]
    read_rpc_calls: afs_metrics::IntCounterVec,
    #[cfg(any(feature = "dfs", test))]
    peer_read_events: afs_metrics::IntCounterVec,
    #[cfg(feature = "dfs")]
    disk_cache_events: afs_metrics::IntCounterVec,
    #[cfg(any(feature = "dfs", test))]
    disk_cache_data_reads: afs_metrics::IntCounter,
    #[cfg(any(feature = "dfs", test))]
    disk_cache_stage_ns: afs_metrics::IntCounterVec,
    #[cfg(any(feature = "dfs", test))]
    disk_cache_stage_calls: afs_metrics::IntCounterVec,
    #[cfg(any(feature = "dfs", test))]
    disk_cache_fill_stage_ns: afs_metrics::IntCounterVec,
    #[cfg(any(feature = "dfs", test))]
    disk_cache_fill_stage_cpu_ns: afs_metrics::IntCounterVec,
    #[cfg(any(feature = "dfs", test))]
    disk_cache_fill_stage_calls: afs_metrics::IntCounterVec,
    #[cfg(any(feature = "dfs", test))]
    disk_cache_fill_stage_cpu_calls: afs_metrics::IntCounterVec,
    #[cfg(any(feature = "dfs", test))]
    disk_cache_fill_bytes: afs_metrics::IntGauge,
    #[cfg(any(feature = "dfs", test))]
    disk_cache_fill_peak_bytes: afs_metrics::IntGauge,
    #[cfg(any(feature = "dfs", test))]
    disk_cache_private_peak_bytes: afs_metrics::IntGauge,
    #[cfg(any(feature = "dfs", test))]
    stream_reserved_bytes: afs_metrics::IntGauge,
    #[cfg(any(feature = "dfs", test))]
    stream_reserved_peak_bytes: afs_metrics::IntGauge,
    #[cfg(any(feature = "dfs", test))]
    stream_private_peak_bytes: afs_metrics::IntGauge,
    #[cfg(any(feature = "dfs", test))]
    stream_frame_bytes: afs_metrics::IntGauge,
    #[cfg(any(feature = "dfs", test))]
    stream_frame_peak_bytes: afs_metrics::IntGauge,
    #[cfg(any(feature = "dfs", test))]
    stream_frames: Arc<StreamFrameAccounting>,
    #[cfg(feature = "dfs")]
    fetch_events: afs_metrics::IntCounterVec,
    #[cfg(feature = "dfs")]
    fetch_resources: afs_metrics::IntGaugeVec,
    #[cfg(any(feature = "dfs", test))]
    diagnostics: Option<Arc<ReadDiagnostics>>,
}

impl ReadWorkMetrics {
    pub fn register(registry: &afs_metrics::Registry) -> Result<Self, afs_metrics::MetricsError> {
        registry.get_or_register(|registry| {
            #[cfg(feature = "dfs")]
            let native_write_resources = {
                let metric = afs_metrics::IntGaugeVec::new(
                    afs_metrics::Opts::new(
                        "afs_dfs_native_write_resources",
                        "Native live, upload and unpublished control capacity with lifetime peaks and cumulative events; excludes S3, idle pool, allocator and RSS.",
                    ),
                    &["resource"],
                )?;
                for resource in NATIVE_WRITE_RESOURCES {
                    metric.with_label_values(&[resource]);
                }
                afs_metrics::register_collector(registry, &metric)?;
                metric
            };
            let catalog_commit_resources = afs_metrics::IntGaugeVec::new(
                afs_metrics::Opts::new("afs_dfs_catalog_commit_resources", "Catalog control capacity and completed groups; excludes caller-owned bodies and the existing oversized-batch path."),
                &["resource"],
            )?;
            for resource in ["requests", "reserved_bytes", "peak_reserved_bytes", "actual_peak_bytes", "control_limit_bytes", "groups", "max_group", "failed", "exclusive", "persist_reused_objects", "persist_reused_bytes"] {
                catalog_commit_resources.with_label_values(&[resource]);
            }
            afs_metrics::register_collector(registry, &catalog_commit_resources)?;
            let scan_bytes = afs_metrics::IntCounter::new(
                "afs_dfs_read_scan_bytes_total",
                "Logical file bytes read and hashed by complete immutable Chunk verification.",
            )?;
            let hashes = afs_metrics::IntCounterVec::new(
                afs_metrics::Opts::new(
                    "afs_dfs_read_hash_bytes_total",
                    "Actual BLAKE3 input bytes by fixed read stage, including failed work; root, outboard and legacy segment hashes are separate executions.",
                ),
                &["stage"],
            )?;
            for stage in [
                "root_local_scan", "stream_range_hash", "peer_range_in_receive",
                "root_in_receive", "root_fallback", "segment_index_bytes",
                "cache_segment_hash", "cache_full_hash", "outboard_body_hash",
                "outboard_parent_hash",
            ] { hashes.with_label_values(&[stage]); }
            #[cfg(any(feature = "dfs", test))]
            let disk_cache_metadata_bytes = afs_metrics::IntGauge::new(
                "afs_dfs_peer_disk_cache_metadata_bytes",
                "Resident cache proof and identity charges; the shared Node ledger also covers in-flight outboards and proof work. Excludes payload and directory bookkeeping.",
            )?;
            #[cfg(any(feature = "dfs", test))]
            let disk_cache_metadata_peak_bytes = afs_metrics::IntGauge::new(
                "afs_dfs_peer_disk_cache_metadata_peak_bytes",
                "Peak resident cache proof and identity charges within the shared Node outboard budget.",
            )?;
            afs_metrics::register_collector(registry, &hashes)?;
            #[cfg(any(feature = "dfs", test))]
            afs_metrics::register_collector(registry, &disk_cache_metadata_bytes)?;
            #[cfg(any(feature = "dfs", test))]
            afs_metrics::register_collector(registry, &disk_cache_metadata_peak_bytes)?;
            #[cfg(feature = "dfs")]
            let disk_cache_resources = afs_metrics::IntGaugeVec::new(
                afs_metrics::Opts::new("afs_dfs_peer_disk_cache_resources",
                    "Bounded native Peer cache resources; body_bytes is logical admission, control_bytes is charged metadata, neither is RSS."),
                &["resource"],
            )?;
            #[cfg(feature = "dfs")]
            for resource in ["operations", "flights", "fill_body_bytes", "indexed_entries", "retired_entries", "retired_disk_bytes", "control_bytes"] {
                disk_cache_resources.with_label_values(&[resource]);
            }
            #[cfg(feature = "dfs")]
            afs_metrics::register_collector(registry, &disk_cache_resources)?;
            let outboard_total_bytes = afs_metrics::IntGauge::new(
                "afs_dfs_outboard_metadata_bytes", "All live Node outboard ledger charges, including resident and working metadata; not RSS.")?;
            let outboard_resident_bytes = afs_metrics::IntGauge::new(
                "afs_dfs_outboard_resident_bytes", "Resident subset of the same Node outboard ledger, retained until the last allocation owner releases.")?;
            let outboard_total_limit = afs_metrics::IntGauge::new(
                "afs_dfs_outboard_metadata_limit_bytes", "Shared Node outboard ledger limit; resident is a subset, not another pool.")?;
            let outboard_resident_limit = afs_metrics::IntGauge::new(
                "afs_dfs_outboard_resident_limit_bytes", "Resident admission sublimit within the shared Node outboard ledger.")?;
            let outboard_events = afs_metrics::IntCounterVec::new(
                afs_metrics::Opts::new("afs_dfs_outboard_events_total",
                    "Bounded outboard admission failures and actual native write/range fallback events."), &["event"])?;
            for event in ["reserve_total_failed", "reserve_resident_failed", "write_without_proof", "range_fullscan_fallback", "range_fullscan_unretained", "sidecar_install_dropped", "sidecar_install_failed"] {
                outboard_events.with_label_values(&[event]);
            }
            let sidecar_install_items = afs_metrics::IntGaugeVec::new(
                afs_metrics::Opts::new("afs_dfs_sidecar_install_items", "Bounded optional sidecar items; queued plus active never exceeds limit."),
                &["state"],
            )?;
            for state in ["queued", "active", "limit"] {
                sidecar_install_items.with_label_values(&[state]);
            }
            afs_metrics::register_collector(registry, &sidecar_install_items)?;
            let sidecar_install_control_bytes = afs_metrics::IntGauge::new(
                "afs_dfs_sidecar_install_control_bytes", "Conservative same-ledger capacity charge for sidecar control containers, ID buffers and worker path; retained while idle until shutdown.")?;
            afs_metrics::register_collector(registry, &sidecar_install_control_bytes)?;
            for gauge in [&outboard_total_bytes, &outboard_resident_bytes, &outboard_total_limit, &outboard_resident_limit] {
                afs_metrics::register_collector(registry, gauge)?;
            }
            afs_metrics::register_collector(registry, &outboard_events)?;
            let scans = afs_metrics::IntCounter::new(
                "afs_dfs_read_scans_total",
                "Immutable Chunk verification scans, including failed scans.",
            )?;
            let copies = afs_metrics::IntCounterVec::new(
                afs_metrics::Opts::new(
                    "afs_dfs_read_copy_bytes_total",
                    "Explicit DFS read user-space copies by payload or proof-metadata stage, including local Bao verification copies; excludes transport codec, TLS and kernel copies.",
                ),
                &["stage"],
            )?;
            #[cfg(any(feature = "dfs", test))]
            let peer_payload_bytes = afs_metrics::IntGauge::new(
                "afs_dfs_peer_read_payload_bytes",
                "Charged Peer read payload capacity, including resident and still-pinned objects.",
            )?;
            #[cfg(any(feature = "dfs", test))]
            let peer_payload_peak_bytes = afs_metrics::IntGauge::new(
                "afs_dfs_peer_read_payload_peak_bytes",
                "Peak charged capacity in the shared Peer read payload budget.",
            )?;
            #[cfg(any(feature = "dfs", test))]
            let peer_payload_reserved = afs_metrics::IntCounter::new(
                "afs_dfs_peer_read_payload_reserved_bytes_total",
                "Peer read payload capacity admitted by the shared budget.",
            )?;
            #[cfg(any(feature = "dfs", test))]
            let peer_payload_released = afs_metrics::IntCounter::new(
                "afs_dfs_peer_read_payload_released_bytes_total",
                "Peer read payload capacity released after its final owner drops.",
            )?;
            #[cfg(any(feature = "dfs", test))]
            let peer_serve_resources = afs_metrics::IntGaugeVec::new(
                afs_metrics::Opts::new("afs_dfs_peer_serve_resources", "Bounded serve control owners and configured body limit; control allocations are not body or proof capacity."),
                &["resource"],
            )?;
            #[cfg(any(feature = "dfs", test))]
            for resource in ["residents", "flights", "readers", "payload_limit_bytes", "fixed_control_bytes", "control_bytes", "control_peak_bytes", "control_limit_bytes"] {
                peer_serve_resources.with_label_values(&[resource]);
            }
            #[cfg(any(feature = "dfs", test))]
            afs_metrics::register_collector(registry, &peer_serve_resources)?;
            #[cfg(any(feature = "dfs", test))]
            let peer_object_cache = afs_metrics::IntCounterVec::new(
                afs_metrics::Opts::new(
                    "afs_dfs_peer_read_object_cache_total",
                    "Verified-object LRU lookups; never a source of read authority.",
                ),
                &["result"],
            )?;
            #[cfg(feature = "dfs")]
            let source_cache_resources = afs_metrics::IntGaugeVec::new(
                afs_metrics::Opts::new(
                    "afs_dfs_source_cache_resources",
                    "Retained source metadata estimates; excludes caller-owned replies, payload and other authorization caches.",
                ),
                &["resource"],
            )?;
            #[cfg(feature = "dfs")]
            for resource in ["entries", "bytes", "peak_bytes", "limit_bytes"] {
                source_cache_resources.with_label_values(&[resource]);
            }
            #[cfg(feature = "dfs")]
            afs_metrics::register_collector(registry, &source_cache_resources)?;
            #[cfg(any(feature = "dfs", test))]
            let read_rpc_calls = afs_metrics::IntCounterVec::new(
                afs_metrics::Opts::new(
                    "afs_dfs_read_rpc_calls_total",
                    "Client read RPC invocations, including failures; not server success counts.",
                ),
                &["method"],
            )?;
            #[cfg(any(feature = "dfs", test))]
            let peer_read_events = afs_metrics::IntCounterVec::new(
                afs_metrics::Opts::new(
                    "afs_dfs_peer_read_events_total",
                    "Peer read task submissions and body-frame events; not elapsed time or completion counts.",
                ),
                &["event"],
            )?;
            #[cfg(feature = "dfs")]
            let disk_cache_events = afs_metrics::IntCounterVec::new(
                afs_metrics::Opts::new("afs_dfs_peer_disk_cache_events_total", "Disposable native Peer disk cache outcomes; no authority or durability."), &["result"],
            )?;
            #[cfg(any(feature = "dfs", test))]
            let disk_cache_data_reads = afs_metrics::IntCounter::new(
                "afs_dfs_peer_disk_cache_data_read_calls_total",
                "Actual cache body-region read attempts, including failed or interrupted attempts; excludes header and trailing EOF probes.",
            )?;
            #[cfg(any(feature = "dfs", test))]
            let disk_cache_stage_ns = afs_metrics::IntCounterVec::new(
                afs_metrics::Opts::new(
                    "afs_dfs_peer_disk_cache_stage_nanoseconds_total",
                    "Aggregated cache read-range wall time by fixed stage; not exclusive Node CPU or full FUSE latency.",
                ),
                &["stage"],
            )?;
            #[cfg(any(feature = "dfs", test))]
            let disk_cache_stage_calls = afs_metrics::IntCounterVec::new(
                afs_metrics::Opts::new(
                    "afs_dfs_peer_disk_cache_stage_observations_total",
                    "Cache stage aggregates observed once per cache operation, including failed work.",
                ),
                &["stage"],
            )?;
            #[cfg(any(feature = "dfs", test))]
            for stage in ["prepare", "data_read", "segment_verify", "stream_read_verify", "finalize"] {
                disk_cache_stage_ns.with_label_values(&[stage]);
                disk_cache_stage_calls.with_label_values(&[stage]);
            }
            #[cfg(any(feature = "dfs", test))]
            afs_metrics::register_collector(registry, &disk_cache_data_reads)?;
            #[cfg(any(feature = "dfs", test))]
            afs_metrics::register_collector(registry, &disk_cache_stage_ns)?;
            #[cfg(any(feature = "dfs", test))]
            afs_metrics::register_collector(registry, &disk_cache_stage_calls)?;
            #[cfg(any(feature = "dfs", test))]
            let disk_cache_fill_stage_ns = afs_metrics::IntCounterVec::new(
                afs_metrics::Opts::new("afs_dfs_peer_disk_cache_fill_stage_nanoseconds_total", "Whole-object fill wall time by fixed stage, including failures; total is a parent, not an extra exclusive stage."), &["stage"],
            )?;
            #[cfg(any(feature = "dfs", test))]
            let disk_cache_fill_stage_cpu_ns = afs_metrics::IntCounterVec::new(
                afs_metrics::Opts::new("afs_dfs_peer_disk_cache_fill_stage_thread_cpu_nanoseconds_total", "Calling-thread CPU for synchronous fill scopes; excludes transport workers and remote CPU, never pure network wait."), &["stage"],
            )?;
            #[cfg(any(feature = "dfs", test))]
            let disk_cache_fill_stage_calls = afs_metrics::IntCounterVec::new(
                afs_metrics::Opts::new("afs_dfs_peer_disk_cache_fill_stage_observations_total", "Fill stage observations, including failed work; prepare can contain multiple disjoint scopes per fill."), &["stage"],
            )?;
            #[cfg(any(feature = "dfs", test))]
            let disk_cache_fill_stage_cpu_calls = afs_metrics::IntCounterVec::new(
                afs_metrics::Opts::new("afs_dfs_peer_disk_cache_fill_stage_thread_cpu_observations_total", "Fill observations with a valid same-thread CPU sample; missing CPU is not an observed zero."), &["stage"],
            )?;
            #[cfg(any(feature = "dfs", test))]
            for stage in FILL_STAGES {
                disk_cache_fill_stage_ns.with_label_values(&[stage]);
                disk_cache_fill_stage_cpu_ns.with_label_values(&[stage]);
                disk_cache_fill_stage_calls.with_label_values(&[stage]);
                disk_cache_fill_stage_cpu_calls.with_label_values(&[stage]);
            }
            #[cfg(any(feature = "dfs", test))]
            afs_metrics::register_collector(registry, &disk_cache_fill_stage_ns)?;
            #[cfg(any(feature = "dfs", test))]
            afs_metrics::register_collector(registry, &disk_cache_fill_stage_cpu_ns)?;
            #[cfg(any(feature = "dfs", test))]
            afs_metrics::register_collector(registry, &disk_cache_fill_stage_calls)?;
            #[cfg(any(feature = "dfs", test))]
            afs_metrics::register_collector(registry, &disk_cache_fill_stage_cpu_calls)?;
            #[cfg(any(feature = "dfs", test))]
            let disk_cache_fill_bytes = afs_metrics::IntGauge::new("afs_dfs_peer_disk_cache_fill_bytes", "Temporary complete-object fill bytes, held until the body is dropped; no resident payload cache.")?;
            #[cfg(any(feature = "dfs", test))]
            let disk_cache_fill_peak_bytes = afs_metrics::IntGauge::new("afs_dfs_peer_disk_cache_fill_peak_bytes", "Peak temporary complete-object fill bytes, serialized by the cache operation guard.")?;
            #[cfg(any(feature = "dfs", test))]
            let disk_cache_private_peak_bytes = afs_metrics::IntGauge::new("afs_dfs_peer_disk_cache_private_peak_bytes", "Peak complete-object fill plus private output for one native read; not Node RSS.")?;
            #[cfg(any(feature = "dfs", test))]
            let stream_reserved_bytes = afs_metrics::IntGauge::new("afs_dfs_peer_disk_cache_stream_reserved_bytes", "Conservative same-budget 4MiB streaming fill reservation held until producer and frames drain; not allocated RAM or admission.")?;
            #[cfg(any(feature = "dfs", test))]
            let stream_reserved_peak_bytes = afs_metrics::IntGauge::new("afs_dfs_peer_disk_cache_stream_reserved_peak_bytes", "Peak conservative streaming fill reservation; mutually exclusive with legacy full-body fill for one cache operation.")?;
            #[cfg(any(feature = "dfs", test))]
            let stream_private_peak_bytes = afs_metrics::IntGauge::new("afs_dfs_peer_disk_cache_stream_private_peak_bytes", "Peak conservative streaming fill reservation plus private output; not Node RSS.")?;
            #[cfg(feature = "dfs")]
            let fetch_events = afs_metrics::IntCounterVec::new(afs_metrics::Opts::new(
                "afs_dfs_fetch_events_total", "Bounded native object fetch scheduling outcomes."), &["event"])?;
            #[cfg(feature = "dfs")]
            let fetch_resources = afs_metrics::IntGaugeVec::new(afs_metrics::Opts::new(
                "afs_dfs_fetch_resources", "Fetch tasks and actual metadata charges in the existing proof ledger; no body quota."), &["resource"])?;
            #[cfg(feature = "dfs")]
            for event in ["scheduled", "skip_disabled", "skip_budget", "skip_grant", "skip_busy", "joined", "cancelled", "failed"] {
                fetch_events.with_label_values(&[event]);
            }
            #[cfg(feature = "dfs")]
            for resource in ["jobs", "metadata_bytes", "metadata_peak_bytes", "job_limit"] {
                fetch_resources.with_label_values(&[resource]);
            }
            #[cfg(feature = "dfs")]
            afs_metrics::register_collector(registry, &fetch_events)?;
            #[cfg(feature = "dfs")]
            afs_metrics::register_collector(registry, &fetch_resources)?;
            #[cfg(any(feature = "dfs", test))]
            let stream_frame_bytes = afs_metrics::IntGauge::new("afs_dfs_peer_disk_cache_stream_frame_bytes", "Owned reusable payload block capacity through free pool, producer, queue and consumer; excludes bounded control fields and transport buffers.")?;
            #[cfg(any(feature = "dfs", test))]
            let stream_frame_peak_bytes = afs_metrics::IntGauge::new("afs_dfs_peer_disk_cache_stream_frame_peak_bytes", "Peak owned streaming payload backing capacity; not reservation or total transport RAM.")?;
            #[cfg(any(feature = "dfs", test))]
            for gauge in [&stream_reserved_bytes, &stream_reserved_peak_bytes, &stream_private_peak_bytes, &stream_frame_bytes, &stream_frame_peak_bytes] {
                afs_metrics::register_collector(registry, gauge)?;
            }
            #[cfg(feature = "dfs")]
            for result in ["hit", "miss", "fill", "fill_failed", "corrupt_or_io", "fallback"] { disk_cache_events.with_label_values(&[result]); }
            #[cfg(feature = "dfs")]
            afs_metrics::register_collector(registry, &disk_cache_events)?;
            #[cfg(any(feature = "dfs", test))]
            afs_metrics::register_collector(registry, &disk_cache_fill_bytes)?;
            #[cfg(any(feature = "dfs", test))]
            afs_metrics::register_collector(registry, &disk_cache_fill_peak_bytes)?;
            #[cfg(any(feature = "dfs", test))]
            afs_metrics::register_collector(registry, &disk_cache_private_peak_bytes)?;
            afs_metrics::register_collector(registry, &scan_bytes)?;
            afs_metrics::register_collector(registry, &scans)?;
            afs_metrics::register_collector(registry, &copies)?;
            #[cfg(any(feature = "dfs", test))]
            afs_metrics::register_collector(registry, &peer_payload_bytes)?;
            #[cfg(any(feature = "dfs", test))]
            afs_metrics::register_collector(registry, &peer_payload_peak_bytes)?;
            #[cfg(any(feature = "dfs", test))]
            afs_metrics::register_collector(registry, &peer_payload_reserved)?;
            #[cfg(any(feature = "dfs", test))]
            afs_metrics::register_collector(registry, &peer_payload_released)?;
            #[cfg(any(feature = "dfs", test))]
            afs_metrics::register_collector(registry, &peer_object_cache)?;
            #[cfg(any(feature = "dfs", test))]
            afs_metrics::register_collector(registry, &read_rpc_calls)?;
            #[cfg(any(feature = "dfs", test))]
            afs_metrics::register_collector(registry, &peer_read_events)?;
            #[cfg(any(feature = "dfs", test))]
            for event in [
                "stream_pipeline",
                "stream_frame",
                "stream_block_alloc",
                "stream_block_handoff",
                "stream_block_write",
                "blocking_authorize",
                "blocking_produce",
                "blocking_combined",
                "data_frame",
                "body_vec_allocation",
                "shared_frame",
                "root_fallback",
                "stream_verified_group",
                "stream_group_corrupt",
                "stream_proof_retired",
                "range_session_request",
            ] {
                peer_read_events.with_label_values(&[event]);
            }
            #[cfg(any(feature = "dfs", test))]
            for method in [
                "get_inode",
                "get_file_version",
                "get_chunk_sources",
                "validate_read_grants",
                "peer_read_ranges",
                "peer_read_session",
            ] {
                read_rpc_calls.with_label_values(&[method]);
            }
            #[cfg(any(feature = "dfs", test))]
            for result in ["hit", "miss"] {
                peer_object_cache.with_label_values(&[result]);
            }
            for stage in [
                "peer_frame_detach",
                "peer_frame_to_block",
                "scan_to_range",
                "pinned_range_to_output",
                "range_to_frame",
                "frame_to_attempt",
                "attempt_to_staging",
                "staging_to_output",
                "peer_outboard_metadata",
                "outboard_range_delivery",
            ] {
                copies.with_label_values(&[stage]);
            }
            Ok(Self {
                #[cfg(feature = "dfs")]
                native_write_resources,
                catalog_commit_resources,
                scan_bytes,
                hashes,
                outboard_total_bytes,
                outboard_resident_bytes,
                outboard_total_limit,
                outboard_resident_limit,
                outboard_events,
                sidecar_install_items,
                sidecar_install_control_bytes,
                #[cfg(any(feature = "dfs", test))]
                disk_cache_metadata_bytes,
                #[cfg(any(feature = "dfs", test))]
                disk_cache_metadata_peak_bytes,
                #[cfg(feature = "dfs")]
                disk_cache_resources,
                scans,
                copies,
                #[cfg(any(feature = "dfs", test))]
                peer_payload_bytes,
                #[cfg(any(feature = "dfs", test))]
                peer_payload_peak_bytes,
                #[cfg(any(feature = "dfs", test))]
                peer_payload_reserved,
                #[cfg(any(feature = "dfs", test))]
                peer_payload_released,
                #[cfg(any(feature = "dfs", test))]
                peer_object_cache,
                #[cfg(any(feature = "dfs", test))]
                peer_serve_resources,
                #[cfg(feature = "dfs")]
                source_cache_resources,
                #[cfg(any(feature = "dfs", test))]
                read_rpc_calls,
                #[cfg(any(feature = "dfs", test))]
                peer_read_events,
                #[cfg(feature = "dfs")]
                disk_cache_events,
                #[cfg(any(feature = "dfs", test))]
                disk_cache_data_reads,
                #[cfg(any(feature = "dfs", test))]
                disk_cache_stage_ns,
                #[cfg(any(feature = "dfs", test))]
                disk_cache_stage_calls,
                #[cfg(any(feature = "dfs", test))]
                disk_cache_fill_stage_ns,
                #[cfg(any(feature = "dfs", test))]
                disk_cache_fill_stage_cpu_ns,
                #[cfg(any(feature = "dfs", test))]
                disk_cache_fill_stage_calls,
                #[cfg(any(feature = "dfs", test))]
                disk_cache_fill_stage_cpu_calls,
                #[cfg(any(feature = "dfs", test))]
                disk_cache_fill_bytes,
                #[cfg(any(feature = "dfs", test))]
                disk_cache_fill_peak_bytes,
                #[cfg(any(feature = "dfs", test))]
                disk_cache_private_peak_bytes,
                #[cfg(any(feature = "dfs", test))]
                stream_reserved_bytes,
                #[cfg(any(feature = "dfs", test))]
                stream_reserved_peak_bytes,
                #[cfg(any(feature = "dfs", test))]
                stream_private_peak_bytes,
                #[cfg(any(feature = "dfs", test))]
                stream_frame_bytes,
                #[cfg(any(feature = "dfs", test))]
                stream_frame_peak_bytes,
                #[cfg(any(feature = "dfs", test))]
                stream_frames: Arc::new(StreamFrameAccounting::default()),
                #[cfg(feature = "dfs")]
                fetch_events,
                #[cfg(feature = "dfs")]
                fetch_resources,
                #[cfg(any(feature = "dfs", test))]
                diagnostics: None,
            })
        })
    }

    /// Enable once before publishing the Node's shared metrics. OFF allocates no
    /// journal and takes no diagnostic clock samples. This never resets a live journal.
    #[cfg(any(feature = "dfs", test))]
    pub(crate) fn with_read_diagnostics(mut self, enabled: bool) -> Self {
        if enabled && self.diagnostics.is_none() {
            self.diagnostics = Some(Arc::new(ReadDiagnostics::new()));
        }
        self
    }

    #[cfg(any(feature = "dfs", test))]
    pub(crate) fn diagnostic(&self, stage: ReadDiagnosticStage) -> Option<ReadDiagnosticSpan> {
        self.diagnostics.as_ref()?.begin(stage)
    }

    /// Compact object boundaries for one bounded diagnostic activity. OFF
    /// returns before hashing identity, taking clocks, or cloning metadata.
    #[cfg(any(feature = "dfs", test))]
    pub(crate) fn object_diagnostic(
        &self,
        stage: ReadObjectDiagnosticStage,
        chunk_id: &str,
    ) -> Option<ReadObjectDiagnosticSpan> {
        self.diagnostics.as_ref()?.begin_object(stage, chunk_id)
    }

    /// Fixed phase totals for diagnostic runs only. OFF returns before sampling a
    /// clock or cloning the journal. Timings are attempted work, can overlap,
    /// and must never be added to their enclosing request as extra elapsed time.
    pub(crate) fn coarse_diagnostic(
        &self,
        phase: &'static str,
        bytes: u64,
    ) -> Option<ReadCoarseDiagnosticSpan> {
        #[cfg(any(feature = "dfs", test))]
        {
            let journal = self.diagnostics.as_ref()?;
            let index = READ_COARSE_PHASES.iter().position(|name| *name == phase)?;
            Some(ReadCoarseDiagnosticSpan {
                journal: journal.clone(),
                index,
                start_ns: journal.now(),
                bytes,
            })
        }
        #[cfg(not(any(feature = "dfs", test)))]
        {
            let _ = (phase, bytes);
            None
        }
    }

    /// Read only outside a measured operation; serialization is not on the read path.
    #[cfg(any(feature = "dfs", test))]
    pub(crate) fn diagnostics_snapshot(&self) -> Option<ReadDiagnosticsSnapshot> {
        self.diagnostics.as_ref().map(|journal| journal.snapshot())
    }

    pub(crate) fn hash(&self, stage: &'static str, bytes: u64) {
        if matches!(
            stage,
            "root_local_scan"
                | "stream_range_hash"
                | "peer_range_in_receive"
                | "root_in_receive"
                | "root_fallback"
                | "segment_index_bytes"
                | "cache_segment_hash"
                | "cache_full_hash"
                | "outboard_body_hash"
                | "outboard_parent_hash"
        ) {
            self.hashes.with_label_values(&[stage]).inc_by(bytes);
        }
    }

    /// Observes the cache's enforcing metadata ledger, never grants admission.
    #[cfg(any(feature = "dfs", test))]
    pub(crate) fn peer_cache_metadata(&self, current: u64, peak: u64) {
        if let (Ok(current), Ok(peak)) = (i64::try_from(current), i64::try_from(peak)) {
            self.disk_cache_metadata_bytes.set(current);
            self.disk_cache_metadata_peak_bytes.set(peak);
        }
    }

    #[cfg(feature = "dfs")]
    pub(crate) fn fetch_event(&self, event: &'static str) {
        if matches!(
            event,
            "scheduled"
                | "skip_disabled"
                | "skip_budget"
                | "skip_grant"
                | "skip_busy"
                | "joined"
                | "cancelled"
                | "failed"
        ) {
            self.fetch_events.with_label_values(&[event]).inc();
        }
    }

    #[cfg(feature = "dfs")]
    pub(crate) fn fetch_resources(&self, jobs: usize, bytes: usize, peak: usize, limit: usize) {
        for (resource, value) in [
            ("jobs", jobs),
            ("metadata_bytes", bytes),
            ("metadata_peak_bytes", peak),
            ("job_limit", limit),
        ] {
            if let Ok(value) = i64::try_from(value) {
                self.fetch_resources
                    .with_label_values(&[resource])
                    .set(value);
            }
        }
    }

    #[cfg(feature = "dfs")]
    pub(crate) fn peer_cache_resources(&self, resources: &[(&'static str, u64)]) {
        for &(resource, value) in resources {
            if matches!(
                resource,
                "operations"
                    | "flights"
                    | "fill_body_bytes"
                    | "indexed_entries"
                    | "retired_entries"
                    | "retired_disk_bytes"
                    | "control_bytes"
            ) && let Ok(value) = i64::try_from(value)
            {
                self.disk_cache_resources
                    .with_label_values(&[resource])
                    .set(value);
            }
        }
    }

    #[cfg(any(feature = "dfs", test))]
    pub(crate) fn peer_cache_data_reads(&self, calls: u64) {
        self.disk_cache_data_reads.inc_by(calls);
    }

    #[cfg(any(feature = "dfs", test))]
    pub(crate) fn peer_cache_stage_ns(&self, stage: &'static str, ns: u64) {
        if ns != 0
            && matches!(
                stage,
                "prepare" | "data_read" | "segment_verify" | "stream_read_verify" | "finalize"
            )
        {
            self.disk_cache_stage_ns
                .with_label_values(&[stage])
                .inc_by(ns);
            self.disk_cache_stage_calls
                .with_label_values(&[stage])
                .inc();
        }
    }

    #[cfg(any(feature = "dfs", test))]
    pub(crate) fn peer_cache_fill_stage(
        &self,
        stage: &'static str,
        wall_ns: u64,
        cpu_ns: Option<u64>,
    ) {
        if !FILL_STAGES.contains(&stage) {
            return;
        }
        self.disk_cache_fill_stage_ns
            .with_label_values(&[stage])
            .inc_by(wall_ns);
        self.disk_cache_fill_stage_calls
            .with_label_values(&[stage])
            .inc();
        if let Some(cpu_ns) = cpu_ns {
            self.disk_cache_fill_stage_cpu_ns
                .with_label_values(&[stage])
                .inc_by(cpu_ns);
            self.disk_cache_fill_stage_cpu_calls
                .with_label_values(&[stage])
                .inc();
        }
    }

    #[cfg(feature = "dfs")]
    pub(crate) fn peer_disk_cache_event(&self, result: &'static str) {
        if matches!(
            result,
            "hit" | "miss" | "fill" | "fill_failed" | "corrupt_or_io" | "fallback"
        ) {
            self.disk_cache_events.with_label_values(&[result]).inc();
        }
    }

    /// Observation only. The cache operation guard and read budget enforce admission.
    #[cfg(any(feature = "dfs", test))]
    pub(crate) fn peer_disk_cache_fill_work(
        &self,
        bytes: usize,
        output: usize,
    ) -> PeerCacheFillWork {
        if let (Ok(bytes), Ok(total)) = (
            i64::try_from(bytes),
            i64::try_from(bytes.saturating_add(output)),
        ) {
            self.disk_cache_fill_bytes.add(bytes);
            self.disk_cache_fill_peak_bytes.set(
                self.disk_cache_fill_peak_bytes
                    .get()
                    .max(self.disk_cache_fill_bytes.get()),
            );
            self.disk_cache_private_peak_bytes
                .set(self.disk_cache_private_peak_bytes.get().max(total));
        }
        PeerCacheFillWork {
            metrics: self.clone(),
            bytes,
        }
    }

    /// Observation owned once by the shared stream session, never admission.
    #[cfg(any(feature = "dfs", test))]
    pub(crate) fn peer_stream_pipeline(&self, output: usize) -> PeerCacheStreamWork {
        const RESERVED: i64 = 4 * 1024 * 1024;
        self.stream_reserved_bytes.add(RESERVED);
        self.stream_reserved_peak_bytes.set(
            self.stream_reserved_peak_bytes
                .get()
                .max(self.stream_reserved_bytes.get()),
        );
        self.peer_stream_private_output(output);
        PeerCacheStreamWork {
            metrics: self.clone(),
        }
    }

    /// Per-operation output peak; only the shared stream session owns the
    /// conservative reservation. Concurrent outputs are not a Node RSS bound.
    #[cfg(any(feature = "dfs", test))]
    pub(crate) fn peer_stream_private_output(&self, output: usize) {
        let total = i64::try_from(output)
            .unwrap_or(i64::MAX)
            .saturating_add(4 * 1024 * 1024);
        self.stream_private_peak_bytes
            .set(self.stream_private_peak_bytes.get().max(total));
    }

    /// Fixed payload owners and control permits enforce bounds; this guard follows ownership.
    #[cfg(any(feature = "dfs", test))]
    pub(crate) fn peer_stream_frame(&self, backing_bytes: usize) -> PeerStreamFrameWork {
        let bytes = u64::try_from(backing_bytes)
            .ok()
            .filter(|v| *v <= i64::MAX as u64)
            .unwrap_or(0);
        let current = self
            .stream_frames
            .current
            .fetch_add(bytes, Ordering::Relaxed)
            + bytes;
        self.stream_frame_bytes.add(bytes as i64);
        let previous = self
            .stream_frames
            .peak
            .fetch_max(current, Ordering::Relaxed);
        if current > previous {
            // Commutative increments preserve the peak even when frame owners race.
            self.stream_frame_peak_bytes
                .add((current - previous) as i64);
        }
        PeerStreamFrameWork {
            metrics: self.clone(),
            bytes,
        }
    }

    pub(crate) fn scan(&self) -> ScanWork<'_> {
        ScanWork {
            metrics: self,
            bytes: 0,
            copied: 0,
        }
    }

    pub(crate) fn outboard_limits(&self, total: usize, resident: usize) {
        if let (Ok(total), Ok(resident)) = (i64::try_from(total), i64::try_from(resident)) {
            self.outboard_total_limit.set(total);
            self.outboard_resident_limit.set(resident);
        }
    }
    pub(crate) fn outboard_adjust(&self, resident: bool, bytes: usize, add: bool) {
        if let Ok(bytes) = i64::try_from(bytes) {
            let gauge = if resident {
                &self.outboard_resident_bytes
            } else {
                &self.outboard_total_bytes
            };
            if add {
                gauge.add(bytes);
            } else {
                gauge.sub(bytes);
            }
        }
    }
    pub(crate) fn outboard_event(&self, event: &'static str) {
        if matches!(
            event,
            "reserve_total_failed"
                | "reserve_resident_failed"
                | "write_without_proof"
                | "range_fullscan_fallback"
                | "range_fullscan_unretained"
                | "sidecar_install_dropped"
                | "sidecar_install_failed"
        ) {
            self.outboard_events.with_label_values(&[event]).inc();
        }
    }

    #[cfg(feature = "dfs")]
    pub(crate) fn native_write_live(&self, state: &crate::node::chunk::NativeWriteBudgetSnapshot) {
        self.native_write_resources(&[
            ("live_bytes", state.live_bytes as u64),
            ("live_peak_bytes", state.live_peak_bytes as u64),
            ("live_limit_bytes", state.limit as u64),
            ("live_leases", state.leases as u64),
            ("live_waiters", state.waiters as u64),
            ("stopped", u64::from(state.stopped)),
        ]);
    }

    #[cfg(feature = "dfs")]
    pub(crate) fn native_write_resources(&self, values: &[(&str, u64)]) {
        for (resource, value) in values {
            if NATIVE_WRITE_RESOURCES.contains(resource)
                && let Ok(value) = i64::try_from(*value)
            {
                self.native_write_resources
                    .with_label_values(&[resource])
                    .set(value);
            }
        }
    }

    pub(crate) fn catalog_commit_resources(&self, values: &[(&str, u64)]) {
        for (resource, value) in values {
            if matches!(
                *resource,
                "requests"
                    | "reserved_bytes"
                    | "peak_reserved_bytes"
                    | "actual_peak_bytes"
                    | "control_limit_bytes"
                    | "groups"
                    | "max_group"
                    | "failed"
                    | "exclusive"
                    | "persist_reused_objects"
                    | "persist_reused_bytes"
            ) && let Ok(value) = i64::try_from(*value)
            {
                self.catalog_commit_resources
                    .with_label_values(&[resource])
                    .set(value);
            }
        }
    }

    pub(crate) fn sidecar_install_counts(
        &self,
        queued: usize,
        active: usize,
        limit: usize,
        control_bytes: usize,
    ) {
        for (state, count) in [("queued", queued), ("active", active), ("limit", limit)] {
            if let Ok(count) = i64::try_from(count) {
                self.sidecar_install_items
                    .with_label_values(&[state])
                    .set(count);
            }
        }
        if let Ok(bytes) = i64::try_from(control_bytes) {
            self.sidecar_install_control_bytes.set(bytes);
        }
    }

    pub(crate) fn copy(&self, stage: &'static str, bytes: u64) {
        self.copies.with_label_values(&[stage]).inc_by(bytes);
    }

    #[cfg(any(feature = "dfs", test))]
    pub(crate) fn peer_serve_resources(
        &self,
        residents: usize,
        flights: usize,
        readers: usize,
        payload_limit: u64,
        fixed_control: usize,
    ) {
        for (resource, value) in [
            ("residents", residents as u64),
            ("flights", flights as u64),
            ("readers", readers as u64),
            ("payload_limit_bytes", payload_limit),
            ("fixed_control_bytes", fixed_control as u64),
            ("control_limit_bytes", 64 * 1024),
        ] {
            if let Ok(value) = i64::try_from(value) {
                self.peer_serve_resources
                    .with_label_values(&[resource])
                    .set(value);
            }
        }
    }

    #[cfg(any(feature = "dfs", test))]
    pub(crate) fn peer_serve_control(&self, delta: i64, peak_increment: u64) {
        self.peer_serve_resources
            .with_label_values(&["control_bytes"])
            .add(delta);
        if let Ok(peak) = i64::try_from(peak_increment) {
            self.peer_serve_resources
                .with_label_values(&["control_peak_bytes"])
                .add(peak);
        }
    }

    #[cfg(feature = "dfs")]
    pub(crate) fn source_cache_resources(
        &self,
        entries: usize,
        bytes: usize,
        peak: usize,
        limit: usize,
    ) {
        for (resource, value) in [
            ("entries", entries),
            ("bytes", bytes),
            ("peak_bytes", peak),
            ("limit_bytes", limit),
        ] {
            if let Ok(value) = i64::try_from(value) {
                self.source_cache_resources
                    .with_label_values(&[resource])
                    .set(value);
            }
        }
    }

    #[cfg(any(feature = "dfs", test))]
    pub(crate) fn peer_payload_reserved(&self, bytes: u64, peak_increment: u64) {
        // The enforcing budget has a configurable 4..=64MiB limit. Metrics remain optional
        // observations and cannot grant admission, even for an invalid input.
        if let (Ok(bytes_i64), Ok(peak_i64)) = (i64::try_from(bytes), i64::try_from(peak_increment))
        {
            self.peer_payload_bytes.add(bytes_i64);
            self.peer_payload_peak_bytes.add(peak_i64);
            self.peer_payload_reserved.inc_by(bytes);
        }
    }

    #[cfg(any(feature = "dfs", test))]
    pub(crate) fn peer_payload_released(&self, bytes: u64) {
        if let Ok(bytes_i64) = i64::try_from(bytes) {
            self.peer_payload_bytes.sub(bytes_i64);
            self.peer_payload_released.inc_by(bytes);
        }
    }

    #[cfg(any(feature = "dfs", test))]
    pub(crate) fn peer_object_cache(&self, result: &'static str) {
        if matches!(result, "hit" | "miss") {
            self.peer_object_cache.with_label_values(&[result]).inc();
        }
    }

    #[cfg(any(feature = "dfs", test))]
    pub(crate) fn read_rpc(&self, method: &'static str) {
        if matches!(
            method,
            "get_inode"
                | "get_file_version"
                | "get_chunk_sources"
                | "validate_read_grants"
                | "peer_read_ranges"
                | "peer_read_session"
        ) {
            self.read_rpc_calls.with_label_values(&[method]).inc();
        }
    }

    #[cfg(any(feature = "dfs", test))]
    pub(crate) fn peer_read_event(&self, event: &'static str) {
        if matches!(
            event,
            "stream_pipeline"
                | "stream_frame"
                | "stream_block_alloc"
                | "stream_block_handoff"
                | "stream_block_write"
                | "blocking_authorize"
                | "blocking_produce"
                | "blocking_combined"
                | "data_frame"
                | "body_vec_allocation"
                | "shared_frame"
                | "root_fallback"
                | "stream_verified_group"
                | "stream_group_corrupt"
                | "stream_proof_retired"
                | "range_session_request"
        ) {
            self.peer_read_events.with_label_values(&[event]).inc();
        }
    }
}

#[derive(Debug, Default)]
#[cfg(any(feature = "dfs", test))]
struct StreamFrameAccounting {
    current: AtomicU64,
    peak: AtomicU64,
}

#[cfg(any(feature = "dfs", test))]
pub(crate) struct PeerCacheStreamWork {
    metrics: ReadWorkMetrics,
}

#[cfg(any(feature = "dfs", test))]
impl Drop for PeerCacheStreamWork {
    fn drop(&mut self) {
        self.metrics.stream_reserved_bytes.sub(4 * 1024 * 1024);
    }
}

#[cfg(any(feature = "dfs", test))]
pub(crate) struct PeerStreamFrameWork {
    metrics: ReadWorkMetrics,
    bytes: u64,
}

#[cfg(any(feature = "dfs", test))]
impl Drop for PeerStreamFrameWork {
    fn drop(&mut self) {
        self.metrics
            .stream_frames
            .current
            .fetch_sub(self.bytes, Ordering::Relaxed);
        self.metrics.stream_frame_bytes.sub(self.bytes as i64);
    }
}

#[cfg(any(feature = "dfs", test))]
pub(crate) struct PeerCacheFillWork {
    metrics: ReadWorkMetrics,
    bytes: usize,
}

#[cfg(any(feature = "dfs", test))]
impl Drop for PeerCacheFillWork {
    fn drop(&mut self) {
        if let Ok(bytes) = i64::try_from(self.bytes) {
            self.metrics.disk_cache_fill_bytes.sub(bytes);
        }
    }
}

pub(crate) struct ScanWork<'a> {
    metrics: &'a ReadWorkMetrics,
    pub(crate) bytes: u64,
    pub(crate) copied: u64,
}

impl Drop for ScanWork<'_> {
    fn drop(&mut self) {
        // Aggregate once per scan rather than adding atomics to each 64KiB read.
        self.metrics.scans.inc();
        self.metrics.scan_bytes.inc_by(self.bytes);
        self.metrics.hash("root_local_scan", self.bytes);
        self.metrics.copy("scan_to_range", self.copied);
    }
}

// The only retained diagnostic data is this fixed journal. No payload, pathname,
// inode or user identity is retained. Slots are never recycled during a Node epoch.
#[cfg(any(feature = "dfs", test))]
const READ_DIAGNOSTIC_CAPACITY: usize = 256;
#[cfg(any(feature = "dfs", test))]
const READ_DIAGNOSTIC_MAX_METADATA_BYTES: usize = 64 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
#[repr(u64)]
#[cfg(any(feature = "dfs", test))]
pub(crate) enum ReadDiagnosticStage {
    FuseRead,
    MetaLookup,
    MetaGetInode,
    MetaGetFileVersion,
    MetaGetChunkSources,
    MetaValidateReadGrants,
    MetaLookupNodeLocation,
    PeerObject,
    PeerServe,
    PersistBatch,
    MetaCommitFileVersion,
    MetaPlacementSnapshot,
    MetaCreate,
    MetaOpenWrite,
    MetaResolveWriteAuthority,
    MetaRenewWriteLease,
    MetaSyncInodeMetadata,
}

#[cfg(any(feature = "dfs", test))]
impl ReadDiagnosticStage {
    fn from_raw(raw: u64) -> Self {
        match raw {
            1 => Self::MetaLookup,
            2 => Self::MetaGetInode,
            3 => Self::MetaGetFileVersion,
            4 => Self::MetaGetChunkSources,
            5 => Self::MetaValidateReadGrants,
            6 => Self::MetaLookupNodeLocation,
            7 => Self::PeerObject,
            8 => Self::PeerServe,
            9 => Self::PersistBatch,
            10 => Self::MetaCommitFileVersion,
            11 => Self::MetaPlacementSnapshot,
            12 => Self::MetaCreate,
            13 => Self::MetaOpenWrite,
            14 => Self::MetaResolveWriteAuthority,
            15 => Self::MetaRenewWriteLease,
            16 => Self::MetaSyncInodeMetadata,
            _ => Self::FuseRead,
        }
    }
}

#[derive(Debug, Default)]
#[cfg(any(feature = "dfs", test))]
struct ReadDiagnosticSlot {
    stage: AtomicU64,
    start_ns: AtomicU64,
    end_ns: AtomicU64,
    // 0 = active; 1 = success; 2 = error/cancel/drop. A missing endpoint is not zero work.
    status: AtomicU64,
    first_data_ns: AtomicU64,
    body_end_ns: AtomicU64,
    eof_ns: AtomicU64,
    consumer_ns: AtomicU64,
    consumer_before_body_ns: AtomicU64,
    consumer_calls: AtomicU64,
    consumer_work: [ReadDiagnosticWorkSlot; 4],
    producer_root_work: ReadDiagnosticWorkSlot,
    root_segment_overlap_ns: AtomicU64,
    root_segment_overlap_pairs: AtomicU64,
    overlap_complete: AtomicU64,
    interval_overflow: AtomicU64,
}

#[derive(Debug, Default)]
#[cfg(any(feature = "dfs", test))]
struct ReadDiagnosticWorkSlot {
    wall_ns: AtomicU64,
    calls: AtomicU64,
    bytes: AtomicU64,
}

#[derive(Clone, Copy, Debug, Default, serde::Serialize)]
#[cfg(any(feature = "dfs", test))]
pub(crate) struct ReadDiagnosticWork {
    pub(crate) wall_ns: u64,
    pub(crate) calls: u64,
    pub(crate) bytes: u64,
}

/// Exclusive measured operations inside the synchronous consumer. These are
/// children of consumer_ns, not extra elapsed time alongside the Peer parent.
#[derive(Clone, Copy, Debug, Default, serde::Serialize)]
#[cfg(any(feature = "dfs", test))]
pub(crate) struct ReadDiagnosticConsumerWork {
    /// Entire sink.write call, including validation and optional cache-failure
    /// fallback; bytes/calls are attempts, not successful disk syscalls or fsync.
    pub(crate) sink_write: ReadDiagnosticWork,
    pub(crate) range_extract: ReadDiagnosticWork,
    /// Whole-root work on legacy synchronous decode only; native pipeline uses
    /// producer_root_work instead and keeps this field zero.
    pub(crate) root_blake3: ReadDiagnosticWork,
    pub(crate) segment_blake3: ReadDiagnosticWork,
}

#[derive(Clone, Copy, PartialEq, Eq)]
#[cfg(any(feature = "dfs", test))]
pub(crate) enum ReadDiagnosticConsumerStage {
    SinkWrite,
    RangeExtract,
    RootBlake3,
    SegmentBlake3,
}

#[cfg(any(feature = "dfs", test))]
impl ReadDiagnosticConsumerWork {
    fn stage_mut(&mut self, stage: ReadDiagnosticConsumerStage) -> &mut ReadDiagnosticWork {
        match stage {
            ReadDiagnosticConsumerStage::SinkWrite => &mut self.sink_write,
            ReadDiagnosticConsumerStage::RangeExtract => &mut self.range_extract,
            ReadDiagnosticConsumerStage::RootBlake3 => &mut self.root_blake3,
            ReadDiagnosticConsumerStage::SegmentBlake3 => &mut self.segment_blake3,
        }
    }

    fn from_slots(slots: &[ReadDiagnosticWorkSlot; 4]) -> Self {
        let values = slots.each_ref().map(|slot| ReadDiagnosticWork {
            wall_ns: slot.wall_ns.load(Ordering::Relaxed),
            calls: slot.calls.load(Ordering::Relaxed),
            bytes: slot.bytes.load(Ordering::Relaxed),
        });
        let [sink_write, range_extract, root_blake3, segment_blake3] = values;
        Self {
            sink_write,
            range_extract,
            root_blake3,
            segment_blake3,
        }
    }

    fn publish(self, slots: &[ReadDiagnosticWorkSlot; 4]) {
        for (value, slot) in [
            self.sink_write,
            self.range_extract,
            self.root_blake3,
            self.segment_blake3,
        ]
        .into_iter()
        .zip(slots)
        {
            slot.wall_ns.store(value.wall_ns, Ordering::Relaxed);
            slot.calls.store(value.calls, Ordering::Relaxed);
            slot.bytes.store(value.bytes, Ordering::Release);
        }
    }
}

// One fixed entry per phase, not one event per frame or syscall. First/last
// endpoints describe the aggregate envelope, not a contiguous critical path.
#[cfg(any(feature = "dfs", test))]
const READ_COARSE_PHASES: [&str; 23] = [
    "peer_slot_wait",
    "peer_read_verify",
    "peer_first_frame_enqueue",
    "cache_lock_wait",
    "cache_hit_read_verify",
    "peer_block_wait",
    "peer_frame_handoff_copy",
    "peer_block_handoff_wait",
    "persist_batch",
    "persist_body_write",
    "persist_body_sync",
    "persist_chunks_dir_sync",
    "persist_catalog_write",
    "persist_catalog_sync",
    "persist_catalog_parent_sync",
    "persist_proof_install",
    "meta_publish_rpc",
    "meta_placement_rpc",
    "meta_create_rpc",
    "meta_open_write_rpc",
    "meta_resolve_write_rpc",
    "meta_renew_write_lease_rpc",
    "meta_sync_inode_metadata_rpc",
];

#[derive(Debug, Default)]
#[cfg(any(feature = "dfs", test))]
struct ReadCoarseDiagnosticSlot {
    first_start_ns: AtomicU64,
    last_end_ns: AtomicU64,
    wall_ns: AtomicU64,
    calls: AtomicU64,
    bytes: AtomicU64,
}

#[derive(Debug, serde::Serialize)]
#[cfg(any(feature = "dfs", test))]
pub(crate) struct ReadCoarseDiagnosticRecord {
    pub(crate) phase: &'static str,
    pub(crate) first_start_ns: u64,
    pub(crate) last_end_ns: u64,
    pub(crate) wall_ns: u64,
    pub(crate) calls: u64,
    pub(crate) bytes: u64,
}

/// Owns diagnostic metadata only. Dropping a guard records attempted work even
/// on error; it never catches, substitutes or changes an operation's result.
pub(crate) struct ReadCoarseDiagnosticSpan {
    #[cfg(any(feature = "dfs", test))]
    journal: Arc<ReadDiagnostics>,
    #[cfg(any(feature = "dfs", test))]
    index: usize,
    #[cfg(any(feature = "dfs", test))]
    start_ns: u64,
    #[cfg(any(feature = "dfs", test))]
    bytes: u64,
}

#[cfg(any(feature = "dfs", test))]
impl Drop for ReadCoarseDiagnosticSpan {
    fn drop(&mut self) {
        let end_ns = self.journal.now();
        if self.start_ns == 0 || end_ns < self.start_ns {
            return;
        }
        let slot = &self.journal.coarse[self.index];
        // Ignore the initial zero when finding the first measured endpoint.
        let _ = slot.first_start_ns.compare_exchange(
            0,
            self.start_ns,
            Ordering::Relaxed,
            Ordering::Relaxed,
        );
        slot.first_start_ns
            .fetch_min(self.start_ns, Ordering::Relaxed);
        slot.last_end_ns.fetch_max(end_ns, Ordering::Relaxed);
        slot.wall_ns
            .fetch_add(end_ns - self.start_ns, Ordering::Relaxed);
        slot.bytes.fetch_add(self.bytes, Ordering::Relaxed);
        slot.calls.fetch_add(1, Ordering::Release);
    }
}

// Separate from the 256 request records: 112 B indexed-hit attempts fit
// one 64MiB first+repeat diagnostic; overflow remains explicit, never wraps.
#[cfg(any(feature = "dfs", test))]
const READ_OBJECT_DIAGNOSTIC_CAPACITY: usize = 120;
#[cfg(any(feature = "dfs", test))]
const READ_OBJECT_TAG_MASK: u64 = (1_u64 << 56) - 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
#[cfg(any(feature = "dfs", test))]
#[repr(u64)]
pub(crate) enum ReadObjectDiagnosticStage {
    PeerReadVerify = 1,
    CacheHitReadVerify = 2,
    CacheHitRootFallback = 3,
}

#[derive(Debug, Default)]
#[cfg(any(feature = "dfs", test))]
struct ReadObjectDiagnosticSlot {
    // 56-bit diagnostic tag, 4-bit stage, 4-bit status. Not object authority.
    identity: AtomicU64,
    start_ns: AtomicU64,
    end_ns: AtomicU64,
}

#[derive(Debug, serde::Serialize)]
#[cfg(any(feature = "dfs", test))]
pub(crate) struct ReadObjectDiagnosticRecord {
    pub(crate) id: usize,
    pub(crate) tag: u64,
    pub(crate) stage: ReadObjectDiagnosticStage,
    pub(crate) start_ns: u64,
    pub(crate) end_ns: u64,
    pub(crate) completed: bool,
    pub(crate) success: bool,
}

/// Only metadata is retained. A dropped/error guard is never a successful read.
#[cfg(any(feature = "dfs", test))]
pub(crate) struct ReadObjectDiagnosticSpan {
    journal: Arc<ReadDiagnostics>,
    index: usize,
    finished: bool,
}

#[cfg(any(feature = "dfs", test))]
impl ReadObjectDiagnosticSpan {
    pub(crate) fn finish(mut self, success: bool) {
        self.record(success);
    }

    fn record(&mut self, success: bool) {
        if !self.finished {
            let slot = &self.journal.objects[self.index];
            slot.end_ns.store(self.journal.now(), Ordering::Relaxed);
            slot.identity
                .fetch_or((if success { 1 } else { 2 }) << 60, Ordering::Release);
            self.finished = true;
        }
    }
}

#[cfg(any(feature = "dfs", test))]
impl Drop for ReadObjectDiagnosticSpan {
    fn drop(&mut self) {
        self.record(false);
    }
}

#[derive(Debug)]
#[cfg(any(feature = "dfs", test))]
struct ReadDiagnostics {
    slots: Box<[ReadDiagnosticSlot; READ_DIAGNOSTIC_CAPACITY]>,
    coarse: [ReadCoarseDiagnosticSlot; READ_COARSE_PHASES.len()],
    next: AtomicU64,
    overflow: AtomicU64,
    clock_failures: AtomicU64,
    objects: [ReadObjectDiagnosticSlot; READ_OBJECT_DIAGNOSTIC_CAPACITY],
    object_next: AtomicU64,
    object_overflow: AtomicU64,
}

#[cfg(any(feature = "dfs", test))]
const _: () = assert!(
    std::mem::size_of::<ReadDiagnosticSlot>() * READ_DIAGNOSTIC_CAPACITY
        + std::mem::size_of::<ReadDiagnostics>()
        + 2 * std::mem::size_of::<ReadDiagnosticIntervals>()
        <= READ_DIAGNOSTIC_MAX_METADATA_BYTES
);

#[derive(Debug, serde::Serialize)]
#[cfg(any(feature = "dfs", test))]
pub(crate) struct ReadDiagnosticRecord {
    pub(crate) id: usize,
    pub(crate) stage: ReadDiagnosticStage,
    pub(crate) start_ns: u64,
    pub(crate) end_ns: u64,
    pub(crate) success: bool,
    pub(crate) completed: bool,
    /// B: first nonempty decoded frame, before staging/hash. A PeerServe:
    /// first data frame accepted by the local channel, not on-wire delivery.
    pub(crate) first_data_ns: u64,
    /// Last expected nonempty data frame decoded, before its copy or consumer work.
    /// Does not imply protocol EOF, verification, authorization or successful output.
    pub(crate) body_end_ns: u64,
    pub(crate) eof_ns: u64,
    /// Synchronous consumer calls only, excluding queue wait and producer work.
    pub(crate) consumer_ns: u64,
    /// Consumer interval intersection with [start_ns, body_end_ns]. Not additive
    /// with the remote parent because producer and consumer can overlap.
    pub(crate) consumer_before_body_ns: u64,
    pub(crate) consumer_calls: u64,
    pub(crate) consumer_work: ReadDiagnosticConsumerWork,
    pub(crate) producer_root_work: ReadDiagnosticWork,
    pub(crate) root_segment_overlap_ns: Option<u64>,
    pub(crate) root_segment_overlap_pairs: u64,
    pub(crate) interval_overflow: u64,
}

#[derive(Debug, serde::Serialize)]
#[cfg(any(feature = "dfs", test))]
pub(crate) struct ReadDiagnosticsSnapshot {
    pub(crate) schema_version: u32,
    /// Sampled only while exporting an enabled diagnostic snapshot. These are
    /// process totals, not CPU attributed to any request or remote component.
    pub(crate) sampled_monotonic_ns: Option<u64>,
    pub(crate) process_cpu_ns: Option<u64>,
    pub(crate) capacity: usize,
    pub(crate) metadata_bytes: usize,
    pub(crate) overflow: u64,
    pub(crate) clock_failures: u64,
    pub(crate) records: Vec<ReadDiagnosticRecord>,
    pub(crate) coarse_phases: Vec<ReadCoarseDiagnosticRecord>,
    pub(crate) object_capacity: usize,
    pub(crate) object_overflow: u64,
    pub(crate) object_records: Vec<ReadObjectDiagnosticRecord>,
}

#[cfg(any(feature = "dfs", test))]
impl ReadDiagnostics {
    fn new() -> Self {
        Self {
            slots: Box::new(std::array::from_fn(|_| ReadDiagnosticSlot::default())),
            coarse: std::array::from_fn(|_| ReadCoarseDiagnosticSlot::default()),
            next: AtomicU64::new(0),
            overflow: AtomicU64::new(0),
            clock_failures: AtomicU64::new(0),
            objects: std::array::from_fn(|_| ReadObjectDiagnosticSlot::default()),
            object_next: AtomicU64::new(0),
            object_overflow: AtomicU64::new(0),
        }
    }

    fn now(&self) -> u64 {
        match monotonic_ns() {
            Some(value) => value,
            None => {
                self.clock_failures.fetch_add(1, Ordering::Relaxed);
                0
            }
        }
    }

    fn begin(self: &Arc<Self>, stage: ReadDiagnosticStage) -> Option<ReadDiagnosticSpan> {
        let index = self.next.fetch_add(1, Ordering::Relaxed);
        let Some(slot) = usize::try_from(index)
            .ok()
            .and_then(|index| self.slots.get(index))
        else {
            self.overflow.fetch_add(1, Ordering::Relaxed);
            return None;
        };
        slot.stage.store(stage as u64, Ordering::Relaxed);
        slot.start_ns.store(self.now(), Ordering::Release);
        Some(ReadDiagnosticSpan {
            token: ReadDiagnosticToken {
                journal: self.clone(),
                index: index as usize,
            },
            finished: false,
        })
    }

    fn begin_object(
        self: &Arc<Self>,
        stage: ReadObjectDiagnosticStage,
        chunk_id: &str,
    ) -> Option<ReadObjectDiagnosticSpan> {
        let index = self.object_next.fetch_add(1, Ordering::Relaxed);
        let Some(slot) = usize::try_from(index)
            .ok()
            .and_then(|index| self.objects.get(index))
        else {
            self.object_overflow.fetch_add(1, Ordering::Relaxed);
            return None;
        };
        let digest = blake3::hash(chunk_id.as_bytes());
        let mut prefix = [0; 8];
        prefix.copy_from_slice(&digest.as_bytes()[..8]);
        let tag = u64::from_le_bytes(prefix) & READ_OBJECT_TAG_MASK;
        slot.identity
            .store(tag | ((stage as u64) << 56), Ordering::Relaxed);
        slot.start_ns.store(self.now(), Ordering::Release);
        Some(ReadObjectDiagnosticSpan {
            journal: self.clone(),
            index: index as usize,
            finished: false,
        })
    }

    fn snapshot(&self) -> ReadDiagnosticsSnapshot {
        let count = self
            .next
            .load(Ordering::Acquire)
            .min(READ_DIAGNOSTIC_CAPACITY as u64);
        let records = self.slots[..count as usize]
            .iter()
            .enumerate()
            .map(|(index, slot)| {
                let status = slot.status.load(Ordering::Acquire);
                ReadDiagnosticRecord {
                    id: index + 1,
                    stage: ReadDiagnosticStage::from_raw(slot.stage.load(Ordering::Relaxed)),
                    start_ns: slot.start_ns.load(Ordering::Acquire),
                    end_ns: slot.end_ns.load(Ordering::Relaxed),
                    success: status == 1,
                    completed: status != 0,
                    first_data_ns: slot.first_data_ns.load(Ordering::Acquire),
                    body_end_ns: slot.body_end_ns.load(Ordering::Acquire),
                    eof_ns: slot.eof_ns.load(Ordering::Acquire),
                    consumer_ns: slot.consumer_ns.load(Ordering::Relaxed),
                    consumer_before_body_ns: slot.consumer_before_body_ns.load(Ordering::Relaxed),
                    consumer_calls: slot.consumer_calls.load(Ordering::Relaxed),
                    consumer_work: ReadDiagnosticConsumerWork::from_slots(&slot.consumer_work),
                    producer_root_work: ReadDiagnosticWork {
                        wall_ns: slot.producer_root_work.wall_ns.load(Ordering::Relaxed),
                        calls: slot.producer_root_work.calls.load(Ordering::Relaxed),
                        bytes: slot.producer_root_work.bytes.load(Ordering::Relaxed),
                    },
                    root_segment_overlap_ns: (slot.overlap_complete.load(Ordering::Acquire) != 0)
                        .then(|| slot.root_segment_overlap_ns.load(Ordering::Relaxed)),
                    root_segment_overlap_pairs: slot
                        .root_segment_overlap_pairs
                        .load(Ordering::Relaxed),
                    interval_overflow: slot.interval_overflow.load(Ordering::Relaxed),
                }
            })
            .collect();
        ReadDiagnosticsSnapshot {
            schema_version: 5,
            sampled_monotonic_ns: monotonic_ns(),
            process_cpu_ns: process_cpu_ns(),
            capacity: READ_DIAGNOSTIC_CAPACITY,
            metadata_bytes: std::mem::size_of::<ReadDiagnosticSlot>() * READ_DIAGNOSTIC_CAPACITY
                + std::mem::size_of::<ReadDiagnostics>(),
            overflow: self.overflow.load(Ordering::Acquire),
            clock_failures: self.clock_failures.load(Ordering::Acquire),
            object_capacity: READ_OBJECT_DIAGNOSTIC_CAPACITY,
            object_overflow: self.object_overflow.load(Ordering::Acquire),
            object_records: self.objects[..self
                .object_next
                .load(Ordering::Acquire)
                .min(READ_OBJECT_DIAGNOSTIC_CAPACITY as u64)
                as usize]
                .iter()
                .enumerate()
                .map(|(index, slot)| {
                    let identity = slot.identity.load(Ordering::Acquire);
                    let status = identity >> 60;
                    ReadObjectDiagnosticRecord {
                        id: index + 1,
                        tag: identity & READ_OBJECT_TAG_MASK,
                        stage: match (identity >> 56) & 15 {
                            1 => ReadObjectDiagnosticStage::PeerReadVerify,
                            3 => ReadObjectDiagnosticStage::CacheHitRootFallback,
                            _ => ReadObjectDiagnosticStage::CacheHitReadVerify,
                        },
                        start_ns: slot.start_ns.load(Ordering::Acquire),
                        end_ns: slot.end_ns.load(Ordering::Relaxed),
                        completed: status != 0,
                        success: status == 1,
                    }
                })
                .collect(),
            records,
            coarse_phases: READ_COARSE_PHASES
                .iter()
                .zip(&self.coarse)
                .map(|(phase, slot)| {
                    let calls = slot.calls.load(Ordering::Acquire);
                    ReadCoarseDiagnosticRecord {
                        phase,
                        first_start_ns: slot.first_start_ns.load(Ordering::Relaxed),
                        last_end_ns: slot.last_end_ns.load(Ordering::Relaxed),
                        wall_ns: slot.wall_ns.load(Ordering::Relaxed),
                        calls,
                        bytes: slot.bytes.load(Ordering::Relaxed),
                    }
                })
                .collect(),
        }
    }
}

#[cfg(any(feature = "dfs", test))]
fn monotonic_ns() -> Option<u64> {
    let mut value = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: value is a valid writable timespec and clock_gettime retains no pointer.
    if unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut value) } != 0 {
        return None;
    }
    let seconds = u64::try_from(value.tv_sec).ok()?;
    let nanos = u64::try_from(value.tv_nsec)
        .ok()
        .filter(|n| *n < 1_000_000_000)?;
    seconds.checked_mul(1_000_000_000)?.checked_add(nanos)
}

#[cfg(any(feature = "dfs", test))]
fn process_cpu_ns() -> Option<u64> {
    let mut value = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: value is writable and aligned for this call; the process clock
    // reads this process's CPU total and retains no pointer or other state.
    if unsafe { libc::clock_gettime(libc::CLOCK_PROCESS_CPUTIME_ID, &mut value) } != 0 {
        return None;
    }
    let seconds = u64::try_from(value.tv_sec).ok()?;
    let nanos = u64::try_from(value.tv_nsec)
        .ok()
        .filter(|n| *n < 1_000_000_000)?;
    seconds.checked_mul(1_000_000_000)?.checked_add(nanos)
}

/// Shared slot reference, never an owner of payload, grants or cache state.
#[derive(Clone)]
#[cfg(any(feature = "dfs", test))]
pub(crate) struct ReadDiagnosticToken {
    journal: Arc<ReadDiagnostics>,
    index: usize,
}

#[cfg(any(feature = "dfs", test))]
impl ReadDiagnosticToken {
    pub(crate) fn now(&self) -> u64 {
        self.journal.now()
    }

    pub(crate) fn first_data(&self) {
        let slot = &self.journal.slots[self.index];
        if slot.first_data_ns.load(Ordering::Relaxed) == 0 {
            let _ = slot.first_data_ns.compare_exchange(
                0,
                self.now(),
                Ordering::Release,
                Ordering::Relaxed,
            );
        }
    }

    #[cfg(feature = "dfs")]
    pub(crate) fn body_complete(&self) {
        self.journal.slots[self.index]
            .body_end_ns
            .store(self.now(), Ordering::Release);
    }

    #[cfg(feature = "dfs")]
    pub(crate) fn eof(&self) {
        self.journal.slots[self.index]
            .eof_ns
            .store(self.now(), Ordering::Release);
    }

    #[cfg(feature = "dfs")]
    pub(crate) fn consumer(&self, start_ns: u64) {
        let end_ns = self.now();
        self.consumer_interval(start_ns, end_ns);
    }

    fn consumer_interval(&self, start_ns: u64, end_ns: u64) {
        let slot = &self.journal.slots[self.index];
        if start_ns == 0 || end_ns < start_ns {
            return;
        }
        let body_end = slot.body_end_ns.load(Ordering::Acquire);
        let before_end = if body_end == 0 {
            end_ns
        } else {
            end_ns.min(body_end)
        };
        slot.consumer_ns
            .fetch_add(end_ns - start_ns, Ordering::Relaxed);
        slot.consumer_before_body_ns
            .fetch_add(before_end.saturating_sub(start_ns), Ordering::Relaxed);
        slot.consumer_calls.fetch_add(1, Ordering::Relaxed);
    }
}

#[cfg(any(feature = "dfs", test))]
const READ_DIAGNOSTIC_INTERVAL_CAPACITY: usize = 64;

#[derive(Clone, Copy, Default)]
#[cfg(any(feature = "dfs", test))]
struct ReadDiagnosticInterval {
    start: u64,
    end: u64,
}

/// Diagnostic-only fixed metadata, never a payload queue or retained event journal.
#[derive(Clone, Copy)]
#[cfg(any(feature = "dfs", test))]
pub(crate) struct ReadDiagnosticIntervals {
    intervals: [ReadDiagnosticInterval; READ_DIAGNOSTIC_INTERVAL_CAPACITY],
    length: usize,
    overflow: u64,
    invalid: bool,
}

#[cfg(any(feature = "dfs", test))]
impl Default for ReadDiagnosticIntervals {
    fn default() -> Self {
        Self {
            intervals: [ReadDiagnosticInterval::default(); READ_DIAGNOSTIC_INTERVAL_CAPACITY],
            length: 0,
            overflow: 0,
            invalid: false,
        }
    }
}

#[cfg(any(feature = "dfs", test))]
impl ReadDiagnosticIntervals {
    fn record(&mut self, start: u64, end: u64) {
        if start == 0
            || end < start
            || self.length != 0 && start < self.intervals[self.length - 1].end
        {
            self.invalid = true;
            return;
        }
        if self.length == self.intervals.len() {
            self.overflow = self.overflow.saturating_add(1);
            return;
        }
        self.intervals[self.length] = ReadDiagnosticInterval { start, end };
        self.length += 1;
    }

    fn intersection(&self, other: &Self) -> Option<(u64, u64)> {
        if self.length == 0
            || other.length == 0
            || self.invalid
            || other.invalid
            || self.overflow != 0
            || other.overflow != 0
        {
            return None;
        }
        let (mut left, mut right, mut ns, mut pairs) = (0, 0, 0_u64, 0_u64);
        while left < self.length && right < other.length {
            let a = self.intervals[left];
            let b = other.intervals[right];
            let overlap = a.end.min(b.end).saturating_sub(a.start.max(b.start));
            if overlap != 0 {
                ns = ns.saturating_add(overlap);
                pairs += 1;
            }
            if a.end <= b.end {
                left += 1;
            } else {
                right += 1;
            }
        }
        Some((ns, pairs))
    }
}

/// One producer-owned root work ledger; drop publishes aggregates on success,
/// RPC failure and task cancellation without extending any payload lifetime.
#[cfg(any(feature = "dfs", test))]
pub(crate) struct ReadDiagnosticProducer {
    token: ReadDiagnosticToken,
    work: std::cell::Cell<ReadDiagnosticWork>,
    intervals: std::cell::Cell<ReadDiagnosticIntervals>,
}

#[cfg(any(feature = "dfs", test))]
impl ReadDiagnosticProducer {
    pub(crate) fn new(token: ReadDiagnosticToken) -> Self {
        Self {
            token,
            work: std::cell::Cell::new(ReadDiagnosticWork::default()),
            intervals: std::cell::Cell::new(ReadDiagnosticIntervals::default()),
        }
    }

    pub(crate) fn measure<T>(&self, bytes: u64, operation: impl FnOnce() -> T) -> T {
        let start = self.token.now();
        let result = operation();
        let end = self.token.now();
        let mut value = self.work.get();
        value.calls = value.calls.saturating_add(1);
        value.bytes = value.bytes.saturating_add(bytes);
        if start != 0 && end >= start {
            value.wall_ns = value.wall_ns.saturating_add(end - start);
        }
        self.work.set(value);
        let mut intervals = self.intervals.get();
        intervals.record(start, end);
        self.intervals.set(intervals);
        result
    }

    #[cfg(feature = "dfs")]
    pub(crate) fn intervals(&self) -> ReadDiagnosticIntervals {
        self.intervals.get()
    }
}

#[cfg(any(feature = "dfs", test))]
impl Drop for ReadDiagnosticProducer {
    fn drop(&mut self) {
        let slot = &self.token.journal.slots[self.token.index];
        let value = self.work.get();
        slot.producer_root_work
            .wall_ns
            .store(value.wall_ns, Ordering::Relaxed);
        slot.producer_root_work
            .calls
            .store(value.calls, Ordering::Relaxed);
        slot.producer_root_work
            .bytes
            .store(value.bytes, Ordering::Release);
        slot.interval_overflow
            .fetch_add(self.intervals.get().overflow, Ordering::Relaxed);
    }
}

#[cfg(feature = "dfs")]
impl ReadDiagnosticToken {
    pub(crate) fn root_segment_overlap(
        &self,
        root: &ReadDiagnosticIntervals,
        segment: &ReadDiagnosticIntervals,
    ) {
        if let Some((ns, pairs)) = root.intersection(segment) {
            let slot = &self.journal.slots[self.index];
            slot.root_segment_overlap_ns.store(ns, Ordering::Relaxed);
            slot.root_segment_overlap_pairs
                .store(pairs, Ordering::Relaxed);
            slot.overlap_complete.store(1, Ordering::Release);
        }
    }
}

/// Optional per-object stack aggregate. It never owns payload, grants, sinks or
/// task handles. The caller drops it after draining, before finishing its parent.
#[cfg(any(feature = "dfs", test))]
pub(crate) struct ReadDiagnosticConsumer {
    token: ReadDiagnosticToken,
    work: std::cell::Cell<ReadDiagnosticConsumerWork>,
    segment_intervals: std::cell::Cell<ReadDiagnosticIntervals>,
}

#[cfg(any(feature = "dfs", test))]
impl ReadDiagnosticConsumer {
    pub(crate) fn new(token: ReadDiagnosticToken) -> Self {
        Self {
            token,
            work: std::cell::Cell::new(ReadDiagnosticConsumerWork::default()),
            segment_intervals: std::cell::Cell::new(ReadDiagnosticIntervals::default()),
        }
    }

    pub(crate) fn measure<T>(
        &self,
        stage: ReadDiagnosticConsumerStage,
        bytes: u64,
        operation: impl FnOnce() -> T,
    ) -> T {
        let start = self.token.now();
        let result = operation();
        let end = self.token.now();
        let mut work = self.work.get();
        let value = work.stage_mut(stage);
        value.calls = value.calls.saturating_add(1);
        value.bytes = value.bytes.saturating_add(bytes);
        if start != 0 && end >= start {
            value.wall_ns = value.wall_ns.saturating_add(end - start);
        }
        self.work.set(work);
        if stage == ReadDiagnosticConsumerStage::SegmentBlake3 {
            let mut intervals = self.segment_intervals.get();
            intervals.record(start, end);
            self.segment_intervals.set(intervals);
        }
        result
    }

    #[cfg(feature = "dfs")]
    pub(crate) fn segment_intervals(&self) -> ReadDiagnosticIntervals {
        self.segment_intervals.get()
    }
}

#[cfg(any(feature = "dfs", test))]
impl Drop for ReadDiagnosticConsumer {
    fn drop(&mut self) {
        let slot = &self.token.journal.slots[self.token.index];
        self.work.get().publish(&slot.consumer_work);
        slot.interval_overflow
            .fetch_add(self.segment_intervals.get().overflow, Ordering::Relaxed);
    }
}

#[cfg(any(feature = "dfs", test))]
pub(crate) struct ReadDiagnosticSpan {
    token: ReadDiagnosticToken,
    finished: bool,
}

#[cfg(any(feature = "dfs", test))]
impl ReadDiagnosticSpan {
    pub(crate) fn token(&self) -> ReadDiagnosticToken {
        self.token.clone()
    }

    pub(crate) fn finish(mut self, success: bool) {
        self.record(success);
    }

    fn record(&mut self, success: bool) {
        if !self.finished {
            let slot = &self.token.journal.slots[self.token.index];
            slot.end_ns.store(self.token.now(), Ordering::Relaxed);
            slot.status
                .store(if success { 1 } else { 2 }, Ordering::Release);
            self.finished = true;
        }
    }
}

#[cfg(any(feature = "dfs", test))]
impl Drop for ReadDiagnosticSpan {
    fn drop(&mut self) {
        self.record(false);
    }
}

#[cfg(test)]
mod tests {
    use super::ReadWorkMetrics;

    #[test]
    fn diagnostic_off_has_no_journal_and_no_scopes() {
        let registry = afs_metrics::Registry::new();
        let metrics = ReadWorkMetrics::register(&registry).unwrap();
        assert!(metrics.diagnostics.is_none());
        assert!(
            metrics
                .diagnostic(super::ReadDiagnosticStage::FuseRead)
                .is_none()
        );
        assert!(metrics.coarse_diagnostic("persist_body_sync", 1).is_none());
        assert!(metrics.diagnostics_snapshot().is_none());
        assert!(metrics.with_read_diagnostics(false).diagnostics.is_none());
    }

    #[test]
    fn object_diagnostics_off_has_no_identity_or_clock_storage() {
        let registry = afs_metrics::Registry::new();
        let metrics = ReadWorkMetrics::register(&registry).unwrap();
        assert!(
            metrics
                .object_diagnostic(
                    super::ReadObjectDiagnosticStage::PeerReadVerify,
                    "not-retained",
                )
                .is_none()
        );
        assert!(metrics.diagnostics.is_none());
    }

    #[test]
    fn object_diagnostics_are_bounded_and_distinguish_active_success_and_drop() {
        let registry = afs_metrics::Registry::new();
        let metrics = ReadWorkMetrics::register(&registry)
            .unwrap()
            .with_read_diagnostics(true);
        let stage = super::ReadObjectDiagnosticStage::CacheHitReadVerify;
        let active = metrics.object_diagnostic(stage, "same-object").unwrap();
        let snapshot = metrics.diagnostics_snapshot().unwrap();
        assert!(!snapshot.object_records[0].completed);
        active.finish(true);
        drop(metrics.object_diagnostic(stage, "same-object").unwrap());
        for _ in 2..super::READ_OBJECT_DIAGNOSTIC_CAPACITY {
            metrics
                .object_diagnostic(stage, "same-object")
                .unwrap()
                .finish(true);
        }
        assert!(metrics.object_diagnostic(stage, "overflow").is_none());
        let snapshot = metrics.diagnostics_snapshot().unwrap();
        assert_eq!(snapshot.object_records.len(), 120);
        assert_eq!(snapshot.object_overflow, 1);
        assert_eq!(snapshot.records.len(), 0);
        assert_eq!(snapshot.clock_failures, 0);
        assert!(snapshot.sampled_monotonic_ns.is_some());
        assert!(snapshot.process_cpu_ns.is_some());
        let next_snapshot = metrics.diagnostics_snapshot().unwrap();
        assert!(next_snapshot.sampled_monotonic_ns >= snapshot.sampled_monotonic_ns);
        assert!(next_snapshot.process_cpu_ns >= snapshot.process_cpu_ns);
        assert!(snapshot.object_records[0].completed && snapshot.object_records[0].success);
        assert!(snapshot.object_records[1].completed && !snapshot.object_records[1].success);
        assert_eq!(
            snapshot.object_records[0].tag,
            snapshot.object_records[1].tag
        );
        assert_eq!(snapshot.object_records[0].stage, stage);
        assert!(
            snapshot
                .object_records
                .iter()
                .all(|entry| entry.end_ns >= entry.start_ns)
        );
        assert!(
            snapshot.metadata_bytes + 2 * std::mem::size_of::<super::ReadDiagnosticIntervals>()
                <= super::READ_DIAGNOSTIC_MAX_METADATA_BYTES
        );
        assert_eq!(std::mem::size_of::<super::ReadObjectDiagnosticSlot>(), 24);
    }

    #[test]
    fn coarse_diagnostics_are_bounded_without_consuming_request_slots() {
        let registry = afs_metrics::Registry::new();
        let metrics = ReadWorkMetrics::register(&registry)
            .unwrap()
            .with_read_diagnostics(true);
        assert!(metrics.coarse_diagnostic("unknown_phase", 7).is_none());
        for _ in 0..1024 {
            let _guard = metrics
                .coarse_diagnostic("persist_body_sync", 4096)
                .unwrap();
        }
        let span = metrics
            .diagnostic(super::ReadDiagnosticStage::PeerServe)
            .unwrap();
        let token = span.token();
        token.first_data();
        let first = metrics.diagnostics_snapshot().unwrap().records[0].first_data_ns;
        token.first_data();
        span.finish(true);
        let snapshot = metrics.diagnostics_snapshot().unwrap();
        assert_eq!(snapshot.schema_version, 5);
        assert_eq!(snapshot.records.len(), 1);
        assert_eq!(snapshot.records[0].first_data_ns, first);
        assert!(first >= snapshot.records[0].start_ns);
        assert!(first <= snapshot.records[0].end_ns);
        let phase = snapshot
            .coarse_phases
            .iter()
            .find(|phase| phase.phase == "persist_body_sync")
            .unwrap();
        assert_eq!(phase.calls, 1024);
        assert_eq!(phase.bytes, 1024 * 4096);
        assert!(phase.first_start_ns > 0);
        assert!(phase.last_end_ns >= phase.first_start_ns);
        assert!(
            snapshot.metadata_bytes + 2 * std::mem::size_of::<super::ReadDiagnosticIntervals>()
                <= super::READ_DIAGNOSTIC_MAX_METADATA_BYTES
        );
        assert_eq!(snapshot.overflow, 0);
    }

    #[test]
    fn coarse_diagnostic_error_drop_preserves_error_and_request_failure() {
        let registry = afs_metrics::Registry::new();
        let metrics = ReadWorkMetrics::register(&registry)
            .unwrap()
            .with_read_diagnostics(true);
        let result: Result<(), u32> = (|| {
            let _span = metrics.diagnostic(super::ReadDiagnosticStage::PersistBatch);
            let _phase = metrics.coarse_diagnostic("persist_body_write", 4096);
            Err::<(), u32>(73)?;
            Ok(())
        })();
        assert_eq!(result, Err(73));
        let snapshot = metrics.diagnostics_snapshot().unwrap();
        assert_eq!(snapshot.records.len(), 1);
        assert!(snapshot.records[0].completed && !snapshot.records[0].success);
        let phase = snapshot
            .coarse_phases
            .iter()
            .find(|phase| phase.phase == "persist_body_write")
            .unwrap();
        assert_eq!(phase.calls, 1);
        assert_eq!(phase.bytes, 4096);
    }

    #[test]
    fn diagnostic_slots_bound_metadata_and_report_overflow_without_recycling() {
        let registry = afs_metrics::Registry::new();
        let metrics = ReadWorkMetrics::register(&registry)
            .unwrap()
            .with_read_diagnostics(true);
        for _ in 0..super::READ_DIAGNOSTIC_CAPACITY {
            metrics
                .diagnostic(super::ReadDiagnosticStage::FuseRead)
                .unwrap()
                .finish(true);
        }
        assert!(
            metrics
                .diagnostic(super::ReadDiagnosticStage::PeerObject)
                .is_none()
        );
        assert!(
            metrics
                .diagnostic(super::ReadDiagnosticStage::MetaLookup)
                .is_none()
        );
        let snapshot = metrics.diagnostics_snapshot().unwrap();
        assert_eq!(snapshot.records.len(), super::READ_DIAGNOSTIC_CAPACITY);
        assert_eq!(snapshot.overflow, 2);
        assert_eq!(snapshot.clock_failures, 0);
        assert!(snapshot.metadata_bytes <= super::READ_DIAGNOSTIC_MAX_METADATA_BYTES);
        assert!(
            snapshot
                .records
                .iter()
                .all(|record| record.success && record.completed)
        );
        assert!(
            snapshot
                .records
                .iter()
                .all(|record| record.start_ns > 0 && record.end_ns >= record.start_ns)
        );
        // Enabling again cannot clear evidence or reuse slots while old spans exist.
        assert_eq!(
            metrics
                .with_read_diagnostics(true)
                .diagnostics_snapshot()
                .unwrap()
                .overflow,
            2
        );
    }

    #[test]
    fn diagnostic_consumer_intersection_is_not_added_to_peer_parent() {
        let registry = afs_metrics::Registry::new();
        let metrics = ReadWorkMetrics::register(&registry)
            .unwrap()
            .with_read_diagnostics(true);
        let span = metrics
            .diagnostic(super::ReadDiagnosticStage::PeerObject)
            .unwrap();
        let token = span.token();
        let base = token.now();
        token.journal.slots[token.index]
            .body_end_ns
            .store(base + 50, std::sync::atomic::Ordering::Release);
        token.consumer_interval(base, base + 20);
        token.consumer_interval(base + 40, base + 60);
        token.consumer_interval(base + 60, base + 90);
        // Parent has not completed although body reception can have completed.
        let pending = metrics.diagnostics_snapshot().unwrap();
        assert!(!pending.records[0].completed);
        assert_eq!(pending.records[0].consumer_ns, 70);
        assert_eq!(pending.records[0].consumer_before_body_ns, 30);
        assert_eq!(pending.records[0].consumer_calls, 3);
        span.finish(true);
        assert!(metrics.diagnostics_snapshot().unwrap().records[0].success);
    }

    #[test]
    fn diagnostic_consumer_publishes_fixed_work_once_after_error_and_drop() {
        use super::{ReadDiagnosticConsumer, ReadDiagnosticConsumerStage, ReadDiagnosticStage};
        let registry = afs_metrics::Registry::new();
        let metrics = ReadWorkMetrics::register(&registry)
            .unwrap()
            .with_read_diagnostics(true);
        let span = metrics.diagnostic(ReadDiagnosticStage::PeerObject).unwrap();
        {
            let work = ReadDiagnosticConsumer::new(span.token());
            let result: Result<(), u8> =
                work.measure(ReadDiagnosticConsumerStage::SinkWrite, 19, || Err(7));
            assert_eq!(result, Err(7), "diagnostics preserve errors unchanged");
            let result = work.measure(ReadDiagnosticConsumerStage::RangeExtract, 11, || 23);
            assert_eq!(result, 23);
            work.measure(ReadDiagnosticConsumerStage::RootBlake3, 31, || {});
            work.measure(ReadDiagnosticConsumerStage::RootBlake3, 0, || {});
            work.measure(ReadDiagnosticConsumerStage::SegmentBlake3, 31, || {});
            // No per-call atomic publication or expanding event list.
            let pending = metrics.diagnostics_snapshot().unwrap();
            assert_eq!(pending.records.len(), 1);
            assert_eq!(pending.records[0].consumer_work.sink_write.calls, 0);
        }
        drop(span);
        let snapshot = metrics.diagnostics_snapshot().unwrap();
        assert_eq!(snapshot.schema_version, 5);
        assert!(snapshot.metadata_bytes <= super::READ_DIAGNOSTIC_MAX_METADATA_BYTES);
        let record = &snapshot.records[0];
        assert!(record.completed && !record.success);
        assert_eq!(record.consumer_work.sink_write.calls, 1);
        assert_eq!(record.consumer_work.sink_write.bytes, 19);
        assert_eq!(record.consumer_work.range_extract.calls, 1);
        assert_eq!(record.consumer_work.range_extract.bytes, 11);
        assert_eq!(record.consumer_work.root_blake3.calls, 2);
        assert_eq!(record.consumer_work.root_blake3.bytes, 31);
        assert_eq!(record.consumer_work.segment_blake3.calls, 1);
        assert_eq!(record.consumer_work.segment_blake3.bytes, 31);
    }

    #[test]
    fn diagnostic_root_segment_intersection_preserves_gaps_and_invalidity() {
        use super::ReadDiagnosticIntervals;
        let mut root = ReadDiagnosticIntervals::default();
        root.record(10, 20);
        root.record(40, 50);
        let mut segment = ReadDiagnosticIntervals::default();
        segment.record(15, 45);
        assert_eq!(
            root.intersection(&segment),
            Some((10, 2)),
            "idle gap20..40 is not overlap"
        );
        let mut serial = ReadDiagnosticIntervals::default();
        serial.record(60, 70);
        assert_eq!(root.intersection(&serial), Some((0, 0)));
        assert_eq!(root.intersection(&ReadDiagnosticIntervals::default()), None);
        let mut overflow = ReadDiagnosticIntervals::default();
        for i in 0..=super::READ_DIAGNOSTIC_INTERVAL_CAPACITY {
            overflow.record(1 + i as u64 * 2, 2 + i as u64 * 2);
        }
        assert_eq!(overflow.overflow, 1);
        assert_eq!(root.intersection(&overflow), None);
        let mut invalid = ReadDiagnosticIntervals::default();
        invalid.record(0, 5);
        assert_eq!(root.intersection(&invalid), None);
    }

    #[test]
    fn diagnostic_producer_drop_publishes_partial_root_without_consumer_double_count() {
        use super::{ReadDiagnosticProducer, ReadDiagnosticStage};
        let registry = afs_metrics::Registry::new();
        let metrics = ReadWorkMetrics::register(&registry)
            .unwrap()
            .with_read_diagnostics(true);
        let span = metrics.diagnostic(ReadDiagnosticStage::PeerObject).unwrap();
        {
            let producer = ReadDiagnosticProducer::new(span.token());
            assert_eq!(producer.measure(13, || Err::<(), _>(7)), Err(7));
            producer.measure(0, || {});
        }
        drop(span);
        let snapshot = metrics.diagnostics_snapshot().unwrap();
        let record = &snapshot.records[0];
        assert_eq!(record.producer_root_work.bytes, 13);
        assert_eq!(record.producer_root_work.calls, 2);
        assert_eq!(record.consumer_work.root_blake3.calls, 0);
        assert!(!record.success);
        assert!(record.root_segment_overlap_ns.is_none());
        assert_eq!(record.interval_overflow, 0);
        assert!(
            snapshot.metadata_bytes + 2 * std::mem::size_of::<super::ReadDiagnosticIntervals>()
                <= super::READ_DIAGNOSTIC_MAX_METADATA_BYTES
        );
    }

    #[test]
    fn diagnostic_concurrent_scopes_remain_distinct_and_drop_is_failure() {
        let registry = afs_metrics::Registry::new();
        let metrics = ReadWorkMetrics::register(&registry)
            .unwrap()
            .with_read_diagnostics(true);
        let pending = metrics
            .diagnostic(super::ReadDiagnosticStage::FuseRead)
            .unwrap();
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let metrics = metrics.clone();
                std::thread::spawn(move || {
                    metrics
                        .diagnostic(super::ReadDiagnosticStage::MetaGetInode)
                        .unwrap()
                        .finish(true);
                })
            })
            .collect();
        for thread in threads {
            thread.join().unwrap();
        }
        drop(pending);
        let snapshot = metrics.diagnostics_snapshot().unwrap();
        assert_eq!(snapshot.records.len(), 9);
        assert!(snapshot.records[0].completed && !snapshot.records[0].success);
        assert_eq!(
            snapshot
                .records
                .iter()
                .filter(|record| record.success)
                .count(),
            8
        );
        assert_eq!(snapshot.overflow, 0);
    }

    #[test]
    fn streaming_work_keeps_last_frame_charged_and_reservation_distinct() {
        let registry = afs_metrics::Registry::new();
        let metrics = ReadWorkMetrics::register(&registry).unwrap();
        let same = ReadWorkMetrics::register(&registry).unwrap();
        let pipeline = metrics.peer_stream_pipeline(1024 * 1024);
        let frame = metrics.peer_stream_frame(65536);
        let queued = same.peer_stream_frame(131072);
        assert_eq!(same.stream_frame_bytes.get(), 196608);
        assert_eq!(same.stream_frame_peak_bytes.get(), 196608);
        assert_eq!(same.stream_reserved_bytes.get(), 4 * 1024 * 1024);
        assert_eq!(same.stream_private_peak_bytes.get(), 5 * 1024 * 1024);
        metrics.peer_stream_private_output(4 * 1024 * 1024);
        same.peer_stream_private_output(2 * 1024 * 1024);
        assert_eq!(
            same.stream_reserved_bytes.get(),
            4 * 1024 * 1024,
            "per-request private observations cannot duplicate the shared reservation"
        );
        assert_eq!(same.stream_private_peak_bytes.get(), 8 * 1024 * 1024);
        assert_eq!(same.disk_cache_fill_bytes.get(), 0);
        std::thread::spawn(move || drop(queued)).join().unwrap();
        assert_eq!(same.stream_frame_bytes.get(), 65536);
        drop(frame);
        assert_eq!(same.stream_frame_bytes.get(), 0);
        assert_eq!(same.stream_frame_peak_bytes.get(), 196608);
        drop(pipeline);
        assert_eq!(same.stream_reserved_bytes.get(), 0);
    }

    #[test]
    fn fill_stage_metrics_keep_missing_cpu_separate_and_labels_bounded() {
        let registry = afs_metrics::Registry::new();
        let metrics = ReadWorkMetrics::register(&registry).unwrap();
        let same = ReadWorkMetrics::register(&registry).unwrap();
        metrics.peer_cache_fill_stage("receive_verify", 100, Some(30));
        metrics.peer_cache_fill_stage("receive_verify", 50, None);
        metrics.peer_cache_fill_stage("store", 0, Some(0));
        metrics.peer_cache_fill_stage("chunk-id-must-not-be-a-label", 999, Some(999));
        assert_eq!(
            same.disk_cache_fill_stage_ns
                .with_label_values(&["receive_verify"])
                .get(),
            150
        );
        assert_eq!(
            same.disk_cache_fill_stage_cpu_ns
                .with_label_values(&["receive_verify"])
                .get(),
            30
        );
        assert_eq!(
            same.disk_cache_fill_stage_calls
                .with_label_values(&["receive_verify"])
                .get(),
            2
        );
        assert_eq!(
            same.disk_cache_fill_stage_cpu_calls
                .with_label_values(&["receive_verify"])
                .get(),
            1
        );
        assert_eq!(
            same.disk_cache_fill_stage_cpu_calls
                .with_label_values(&["store"])
                .get(),
            1
        );
        for family in registry
            .gather()
            .into_iter()
            .filter(|f| f.name().starts_with("afs_dfs_peer_disk_cache_fill_stage_"))
        {
            assert_eq!(family.get_metric().len(), 7);
        }
    }

    #[test]
    fn fill_clock_never_subtracts_cpu_from_another_thread() {
        let clock = super::PeerFillClock::start();
        let cpu = std::thread::spawn(move || clock.elapsed().1)
            .join()
            .unwrap();
        assert_eq!(cpu, None);
    }

    #[test]
    fn disk_read_work_observes_actual_calls_and_fixed_stage_aggregates() {
        let registry = afs_metrics::Registry::new();
        let metrics = ReadWorkMetrics::register(&registry).unwrap();
        let same = ReadWorkMetrics::register(&registry).unwrap();
        metrics.peer_cache_data_reads(2);
        metrics.peer_cache_stage_ns("data_read", 100);
        metrics.peer_cache_stage_ns("data_read", 30);
        metrics.peer_cache_stage_ns("segment_verify", 40);
        metrics.peer_cache_stage_ns("chunk-id-must-not-be-a-label", 50);
        assert_eq!(same.disk_cache_data_reads.get(), 2);
        assert_eq!(
            same.disk_cache_stage_ns
                .with_label_values(&["data_read"])
                .get(),
            130
        );
        assert_eq!(
            same.disk_cache_stage_calls
                .with_label_values(&["data_read"])
                .get(),
            2
        );
        let stages = registry
            .gather()
            .into_iter()
            .find(|family| family.name() == "afs_dfs_peer_disk_cache_stage_nanoseconds_total")
            .unwrap();
        assert_eq!(stages.get_metric().len(), 5);
    }

    #[test]
    fn hash_and_metadata_metrics_are_fixed_and_do_not_grant_budget() {
        let registry = afs_metrics::Registry::new();
        let metrics = ReadWorkMetrics::register(&registry).unwrap();
        let same = ReadWorkMetrics::register(&registry).unwrap();
        metrics.hash("root_in_receive", 4096);
        metrics.hash("segment_index_bytes", 4096);
        metrics.hash("outboard_body_hash", 8192);
        metrics.hash("outboard_parent_hash", 64);
        metrics.hash("request-id-must-not-be-a-label", 1);
        metrics.peer_cache_metadata(256, 512);
        assert_eq!(
            same.hashes.with_label_values(&["root_in_receive"]).get(),
            4096
        );
        assert_eq!(
            same.hashes
                .with_label_values(&["segment_index_bytes"])
                .get(),
            4096
        );
        assert_eq!(
            same.hashes.with_label_values(&["stream_range_hash"]).get(),
            0
        );
        assert_eq!(same.disk_cache_metadata_bytes.get(), 256);
        assert_eq!(same.disk_cache_metadata_peak_bytes.get(), 512);
        assert_eq!(
            same.hashes.with_label_values(&["outboard_body_hash"]).get(),
            8192
        );
        assert_eq!(
            same.hashes
                .with_label_values(&["outboard_parent_hash"])
                .get(),
            64
        );
        let hashes = registry
            .gather()
            .into_iter()
            .find(|family| family.name() == "afs_dfs_read_hash_bytes_total")
            .unwrap();
        assert_eq!(hashes.get_metric().len(), 10);
    }

    #[test]
    fn peer_disk_fill_observation_releases_only_when_work_guard_drops() {
        let registry = afs_metrics::Registry::new();
        let metrics = ReadWorkMetrics::register(&registry).unwrap();
        {
            let _work = metrics.peer_disk_cache_fill_work(4 * 1024 * 1024, 1024 * 1024);
            assert_eq!(metrics.disk_cache_fill_bytes.get(), 4 * 1024 * 1024);
            assert_eq!(metrics.disk_cache_fill_peak_bytes.get(), 4 * 1024 * 1024);
            assert_eq!(metrics.disk_cache_private_peak_bytes.get(), 5 * 1024 * 1024);
        }
        assert_eq!(metrics.disk_cache_fill_bytes.get(), 0);
        assert_eq!(metrics.disk_cache_fill_peak_bytes.get(), 4 * 1024 * 1024);
    }

    #[test]
    fn peer_read_event_metrics_are_fixed_and_shared() {
        let registry = afs_metrics::Registry::new();
        let metrics = ReadWorkMetrics::register(&registry).unwrap();
        let same = ReadWorkMetrics::register(&registry).unwrap();
        for event in [
            "stream_pipeline",
            "stream_frame",
            "stream_block_alloc",
            "stream_block_handoff",
            "stream_block_write",
            "blocking_authorize",
            "blocking_produce",
            "blocking_combined",
            "data_frame",
            "body_vec_allocation",
            "shared_frame",
            "root_fallback",
            "stream_verified_group",
            "stream_group_corrupt",
            "stream_proof_retired",
            "range_session_request",
        ] {
            metrics.peer_read_event(event);
            assert_eq!(same.peer_read_events.with_label_values(&[event]).get(), 1);
        }
        metrics.peer_read_event("inode-or-request-id-must-not-be-a-label");
        let families = registry.gather();
        let events = families
            .iter()
            .find(|family| family.name() == "afs_dfs_peer_read_events_total")
            .unwrap();
        assert_eq!(events.get_metric().len(), 16);
    }

    #[test]
    fn native_rpc_metrics_have_fixed_methods_and_shared_registration() {
        let registry = afs_metrics::Registry::new();
        let metrics = ReadWorkMetrics::register(&registry).unwrap();
        let same = ReadWorkMetrics::register(&registry).unwrap();
        for method in [
            "get_inode",
            "get_file_version",
            "get_chunk_sources",
            "validate_read_grants",
            "peer_read_ranges",
            "peer_read_session",
        ] {
            metrics.read_rpc(method);
            assert_eq!(same.read_rpc_calls.with_label_values(&[method]).get(), 1);
        }
        metrics.read_rpc("inode-id-is-not-a-label");
        let families = registry.gather();
        let calls = families
            .iter()
            .find(|family| family.name() == "afs_dfs_read_rpc_calls_total")
            .unwrap();
        assert_eq!(calls.get_metric().len(), 6);
    }

    #[test]
    fn peer_payload_metrics_account_shared_lifetimes_and_reuse_registration() {
        let registry = afs_metrics::Registry::new();
        let metrics = ReadWorkMetrics::register(&registry).unwrap();
        let same = ReadWorkMetrics::register(&registry).unwrap();
        metrics.peer_payload_reserved(4 * 1024 * 1024, 4 * 1024 * 1024);
        same.peer_payload_reserved(1024 * 1024, 1024 * 1024);
        same.peer_payload_released(4 * 1024 * 1024);
        assert_eq!(metrics.peer_payload_bytes.get(), 1024 * 1024);
        assert_eq!(metrics.peer_payload_peak_bytes.get(), 5 * 1024 * 1024);
        metrics.peer_payload_released(1024 * 1024);
        assert_eq!(same.peer_payload_bytes.get(), 0);
        assert_eq!(
            metrics.peer_payload_reserved.get(),
            metrics.peer_payload_released.get()
        );
        metrics.peer_object_cache("hit");
        metrics.peer_object_cache("miss");
        metrics.peer_object_cache("unbounded-label");
        assert_eq!(same.peer_object_cache.with_label_values(&["hit"]).get(), 1);
        assert_eq!(same.peer_object_cache.with_label_values(&["miss"]).get(), 1);
    }
}
