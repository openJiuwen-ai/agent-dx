//! Fixed-cardinality work counters for immutable DFS reads, never authority or budgets.
//!
//! Local integrity scans also serve the diagnostic Chunk store without DFS.
//! Peer transport and disk-cache counters are compiled only for DFS (or their
//! direct unit tests), without changing that shared verification path. The
//! optional request-timing journal lives in `read_timing`.

#[cfg(any(feature = "dfs", test))]
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use super::read_timing::ReadCoarseDiagnosticSpan;
#[cfg(any(feature = "dfs", test))]
use super::read_timing::{
    ReadDiagnosticSpan, ReadDiagnosticStage, ReadDiagnostics, ReadDiagnosticsSnapshot,
    ReadObjectDiagnosticSpan, ReadObjectDiagnosticStage,
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

/// Fixed, prebound counters: no label lookup in the per-frame hot loop.
#[cfg(any(feature = "dfs", test))]
#[derive(Clone, Copy)]
pub(crate) enum ReadCheck {
    RequestValidate,
    ServeState,
    CatalogOpen,
    CatalogFence,
    DescriptorEncode,
    DescriptorDecode,
    AuthorizeFull,
    LocalProbe,
    BatchGroup,
    FullGrant,
}
#[cfg(any(feature = "dfs", test))]
const READ_CHECK_NAMES: [&str; 10] = [
    "request_validate",
    "serve_state",
    "catalog_open",
    "catalog_fence",
    "descriptor_encode",
    "descriptor_decode",
    "authorize_full",
    "local_probe",
    "batch_group",
    "full_grant",
];

#[derive(Clone, Debug)]
pub struct ReadWorkMetrics {
    #[cfg(feature = "dfs")]
    pub(crate) activities: super::super::read::activity::Activities,
    #[cfg(any(feature = "dfs", test))]
    checks: [afs_metrics::IntCounter; 10],
    #[cfg(feature = "dfs")]
    native_write_resources: afs_metrics::IntGaugeVec,
    catalog_commit_resources: afs_metrics::IntGaugeVec,
    scan_bytes: afs_metrics::IntCounter,
    hashes: afs_metrics::IntCounterVec,
    #[cfg(any(feature = "dfs", test))]
    read_buffer_events: afs_metrics::IntCounterVec,
    #[cfg(any(feature = "dfs", test))]
    read_buffer_bytes: afs_metrics::IntCounterVec,
    #[cfg(any(feature = "dfs", test))]
    read_buffer_idle: afs_metrics::IntGaugeVec,
    #[cfg(any(feature = "dfs", test))]
    read_buffer_capacity: afs_metrics::IntGaugeVec,
    crc_read_bytes: afs_metrics::IntCounterVec,
    crc_read_calls: afs_metrics::IntCounterVec,
    #[cfg(feature = "dfs")]
    authorization_ns: afs_metrics::IntCounterVec,
    #[cfg(feature = "dfs")]
    authorization_calls: afs_metrics::IntCounterVec,
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
    pub(super) diagnostics: Option<Arc<ReadDiagnostics>>,
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
            for resource in ["requests", "reserved_bytes", "peak_reserved_bytes", "actual_peak_bytes", "control_limit_bytes", "groups", "max_group", "failed", "exclusive"] {
                catalog_commit_resources.with_label_values(&[resource]);
            }
            afs_metrics::register_collector(registry, &catalog_commit_resources)?;
            let scan_bytes = afs_metrics::IntCounter::new(
                "afs_dfs_read_scan_bytes_total",
                "Logical file bytes read and hashed by complete immutable Chunk verification.",
            )?;
            #[cfg(feature = "dfs")]
            let authorization_ns = afs_metrics::IntCounterVec::new(afs_metrics::Opts::new("afs_dfs_read_authorization_nanoseconds_total", "Complete authorization scope wall time; scopes can nest and overlap."), &["stage"])?;
            #[cfg(feature = "dfs")]
            let authorization_calls = afs_metrics::IntCounterVec::new(afs_metrics::Opts::new("afs_dfs_read_authorization_calls_total", "Authorization scope calls, including cache hits and failures."), &["stage"])?;
            #[cfg(feature = "dfs")]
            for stage in ["total", "cache_maintenance", "meta_rpc"] { authorization_ns.with_label_values(&[stage]); authorization_calls.with_label_values(&[stage]); }
            #[cfg(feature = "dfs")]
            afs_metrics::register_collector(registry, &authorization_ns)?;
            #[cfg(feature = "dfs")]
            afs_metrics::register_collector(registry, &authorization_calls)?;
            #[cfg(any(feature = "dfs", test))]
            let checks = afs_metrics::IntCounterVec::new(afs_metrics::Opts::new("afs_dfs_read_checks_total", "Executed read control operations; fixed-stage counters, not elapsed time."), &["stage"])?;
            #[cfg(any(feature = "dfs", test))]
            let stages = [ReadCheck::RequestValidate, ReadCheck::ServeState, ReadCheck::CatalogOpen, ReadCheck::CatalogFence, ReadCheck::DescriptorEncode, ReadCheck::DescriptorDecode, ReadCheck::AuthorizeFull, ReadCheck::LocalProbe, ReadCheck::BatchGroup, ReadCheck::FullGrant];
            #[cfg(any(feature = "dfs", test))]
            let checks_bound = std::array::from_fn(|i| checks.with_label_values(&[READ_CHECK_NAMES[stages[i] as usize]]));
            #[cfg(any(feature = "dfs", test))]
            afs_metrics::register_collector(registry, &checks)?;
            #[cfg(any(feature = "dfs", test))]
            let read_buffer_events = afs_metrics::IntCounterVec::new(afs_metrics::Opts::new("afs_dfs_read_buffer_events_total", "Read buffer allocation/reuse/return events; zero counts explicit initialization."), &["role", "event"])?;
            #[cfg(any(feature = "dfs", test))]
            let read_buffer_bytes = afs_metrics::IntCounterVec::new(afs_metrics::Opts::new("afs_dfs_read_buffer_bytes_total", "Actual capacity allocated/reused/returned or explicit bytes zeroed."), &["role", "event"])?;
            #[cfg(any(feature = "dfs", test))]
            let read_buffer_idle = afs_metrics::IntGaugeVec::new(afs_metrics::Opts::new("afs_dfs_read_buffer_idle_bytes", "Idle initialized buffer capacity; native async idle plus active storage shares a fixed byte budget."), &["role"])?;
            #[cfg(any(feature = "dfs", test))]
            for role in ["serve", "reply"] {
                read_buffer_idle.with_label_values(&[role]);
                for event in ["allocate", "reuse", "return", "zero"] {
                    read_buffer_events.with_label_values(&[role,event]);
                    read_buffer_bytes.with_label_values(&[role,event]);
                }
            }
            #[cfg(any(feature = "dfs", test))]
            afs_metrics::register_collector(registry, &read_buffer_events)?;
            #[cfg(any(feature = "dfs", test))]
            afs_metrics::register_collector(registry, &read_buffer_bytes)?;
            #[cfg(any(feature = "dfs", test))]
            afs_metrics::register_collector(registry, &read_buffer_idle)?;
            #[cfg(any(feature = "dfs", test))]
            let read_buffer_capacity=afs_metrics::IntGaugeVec::new(afs_metrics::Opts::new("afs_dfs_read_buffer_capacity_bytes","Pool-owned storage by role/state; active plus idle equals total; legacy detached Vec storage excluded."), &["role","state"])?;
            #[cfg(any(feature = "dfs", test))]
            afs_metrics::register_collector(registry,&read_buffer_capacity)?;
            let crc_read_bytes = afs_metrics::IntCounterVec::new(
                afs_metrics::Opts::new("afs_dfs_crc32c_read_bytes_total", "Actual CRC32C read input bytes, including failed work; descriptor hashes are separate."), &["stage"],
            )?;
            let crc_read_calls = afs_metrics::IntCounterVec::new(
                afs_metrics::Opts::new("afs_dfs_crc32c_read_calls_total", "Actual CRC32C read checksum invocations, including failed work."), &["stage"],
            )?;
            for stage in ["storage_segments", "range_send", "range_receive", "trusted_segments_receive"] {
                crc_read_bytes.with_label_values(&[stage]);
                crc_read_calls.with_label_values(&[stage]);
            }
            afs_metrics::register_collector(registry, &crc_read_bytes)?;
            afs_metrics::register_collector(registry, &crc_read_calls)?;
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
                "outboard_parent_hash", "crc_descriptor_hash",
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
                "crc_storage_to_range",
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
                activities: super::super::read::activity::Activities::register(registry)?,
                #[cfg(any(feature = "dfs", test))]
                checks: checks_bound,
                #[cfg(feature = "dfs")]
                native_write_resources,
                catalog_commit_resources,
                scan_bytes,
                hashes,
                #[cfg(any(feature = "dfs", test))]
                read_buffer_events,
                #[cfg(any(feature = "dfs", test))]
                read_buffer_bytes,
                #[cfg(any(feature = "dfs", test))]
                read_buffer_idle,
                #[cfg(any(feature = "dfs", test))]
                read_buffer_capacity,
                crc_read_bytes,
                crc_read_calls,
                #[cfg(feature = "dfs")]
                authorization_ns,
                #[cfg(feature = "dfs")]
                authorization_calls,
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
            self.diagnostics.as_ref()?.begin_coarse(phase, bytes)
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

    #[cfg(feature = "dfs")]
    pub(crate) fn authorization_scope(&self, stage: &'static str) -> ReadAuthorizationScope {
        ReadAuthorizationScope {
            metrics: self.clone(),
            stage,
            started: std::time::Instant::now(),
        }
    }

    pub(crate) fn crc_read(&self, stage: &'static str, bytes: u64) {
        self.crc_read_bytes
            .with_label_values(&[stage])
            .inc_by(bytes);
        self.crc_read_calls.with_label_values(&[stage]).inc();
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
                | "crc_descriptor_hash"
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
    pub(crate) fn native_write_live(&self, state: &crate::node::chunk::DirtyBudgetSnapshot) {
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

    #[cfg(feature = "dfs")]
    pub(crate) fn read_buffer_counters(
        &self,
        role: &'static str,
        event: &'static str,
    ) -> (afs_metrics::IntCounter, afs_metrics::IntCounter) {
        (
            self.read_buffer_events.with_label_values(&[role, event]),
            self.read_buffer_bytes.with_label_values(&[role, event]),
        )
    }
    #[cfg(feature = "dfs")]
    pub(crate) fn read_buffer_gauge(
        &self,
        role: &'static str,
        state: &'static str,
    ) -> afs_metrics::IntGauge {
        self.read_buffer_capacity.with_label_values(&[role, state])
    }
    #[cfg(any(feature = "dfs", test))]
    pub(crate) fn read_buffer(&self, role: &'static str, event: &'static str, bytes: u64) {
        self.read_buffer_events
            .with_label_values(&[role, event])
            .inc();
        self.read_buffer_bytes
            .with_label_values(&[role, event])
            .inc_by(bytes);
    }
    #[cfg(any(feature = "dfs", test))]
    pub(crate) fn read_buffer_capacity(
        &self,
        role: &'static str,
        total: u64,
        idle: u64,
        limit: u64,
        peak: u64,
    ) {
        for (name, value) in [
            ("total", total),
            ("active", total.saturating_sub(idle)),
            ("idle", idle),
            ("limit", limit),
            ("peak", peak),
        ] {
            if let Ok(value) = i64::try_from(value) {
                self.read_buffer_capacity
                    .with_label_values(&[role, name])
                    .set(value);
            }
        }
    }
    #[cfg(any(feature = "dfs", test))]
    pub(crate) fn read_buffer_idle(&self, role: &'static str, bytes: u64) {
        if let Ok(bytes) = i64::try_from(bytes) {
            self.read_buffer_idle.with_label_values(&[role]).set(bytes);
        }
    }
    #[cfg(any(feature = "dfs", test))]
    pub(crate) fn check(&self, stage: ReadCheck) {
        self.checks[stage as usize].inc();
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

/// Cumulative counters independent of the bounded per-request journal.
#[cfg(feature = "dfs")]
pub(crate) struct ReadAuthorizationScope {
    metrics: ReadWorkMetrics,
    stage: &'static str,
    started: std::time::Instant,
}
#[cfg(feature = "dfs")]
impl Drop for ReadAuthorizationScope {
    fn drop(&mut self) {
        self.metrics
            .authorization_ns
            .with_label_values(&[self.stage])
            .inc_by(self.started.elapsed().as_nanos().min(u64::MAX as u128) as u64);
        self.metrics
            .authorization_calls
            .with_label_values(&[self.stage])
            .inc();
    }
}

#[cfg(test)]
mod tests {
    use super::ReadWorkMetrics;

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
        metrics.hash("crc_descriptor_hash", 128);
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
        assert_eq!(
            same.hashes
                .with_label_values(&["crc_descriptor_hash"])
                .get(),
            128
        );
        assert_eq!(hashes.get_metric().len(), 11);
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
        assert_eq!(events.get_metric().len(), 15);
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
        assert_eq!(calls.get_metric().len(), 5);
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
