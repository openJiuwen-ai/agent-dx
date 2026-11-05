//! DFS read-timing diagnostics: the bounded per-request journal behind
//! `dfs_read_diagnostics_enabled` and REST `GET /v1/diagnostics/dfs-read-timing`.
//!
//! Default OFF at run time: `ReadWorkMetrics::with_read_diagnostics(false)` allocates
//! no journal and takes no diagnostic clock samples. No payload, pathname, inode or
//! user identity is retained.
// The only retained diagnostic data is this fixed journal. No payload, pathname,
// inode or user identity is retained. Slots are never recycled during a Node epoch.
#[cfg(any(feature = "dfs", test))]
const READ_DIAGNOSTIC_CAPACITY: usize = 256;
#[cfg(any(feature = "dfs", test))]
const READ_DIAGNOSTIC_MAX_METADATA_BYTES: usize = 64 * 1024;

#[cfg(any(feature = "dfs", test))]
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

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
const READ_COARSE_PHASES: [&str; 28] = [
    "crc_storage",
    "crc_range_send",
    "crc_range_receive",
    "crc_trusted_receive",
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
    "persist_proof_install_sync",
    "persist_proof_dir_sync",
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
pub(super) struct ReadDiagnostics {
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
    pub(super) fn new() -> Self {
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

    /// Fixed phase totals; unknown phase names are not recorded.
    pub(super) fn begin_coarse(
        self: &Arc<Self>,
        phase: &'static str,
        bytes: u64,
    ) -> Option<ReadCoarseDiagnosticSpan> {
        let index = READ_COARSE_PHASES.iter().position(|name| *name == phase)?;
        Some(ReadCoarseDiagnosticSpan {
            journal: self.clone(),
            index,
            start_ns: self.now(),
            bytes,
        })
    }

    pub(super) fn begin(
        self: &Arc<Self>,
        stage: ReadDiagnosticStage,
    ) -> Option<ReadDiagnosticSpan> {
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

    pub(super) fn begin_object(
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

    pub(super) fn snapshot(&self) -> ReadDiagnosticsSnapshot {
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
            schema_version: 4,
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
    use super::super::work::ReadWorkMetrics;

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
        assert_eq!(snapshot.schema_version, 4);
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
        assert_eq!(snapshot.schema_version, 4);
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
}
