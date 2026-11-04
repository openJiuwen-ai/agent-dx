//! Peer disk cache records and in-memory state.

use super::*;

pub(super) type FileIdentity = (u64, u64, u64, i64, i64, i64, i64);

#[derive(Debug)]
pub(super) struct VerifiedDiskOutboard {
    pub(super) identity: FileIdentity,
    pub(super) outboard: SharedOutboard,
    // The Arc owns encoded storage; this lease charges only our binding wrapper.
    pub(super) _lease: OutboardLease,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct PeerCacheHashWork {
    pub cache_segment_bytes: u64,
    pub cache_fullscan_bytes: u64,
    pub segment_index_bytes: u64,
    /// Metadata CV inputs hashed while authenticating covered groups.
    pub outboard_parent_bytes: u64,
}

/// Low-frequency aggregate wall time inside read_range only. Admission lock,
/// operation root checks, Meta, final engine authority and metric emission are
/// outside these boundaries. `stream_read_verify_ns` is the inclusive local
/// operation; data_read/segment_verify are child intervals and must not be
/// added to it. Complete-root fallback cannot separate read and hash.
#[derive(Clone, Copy, Debug, Default)]
pub struct PeerCacheReadStages {
    pub prepare_ns: u64,
    pub data_read_ns: u64,
    pub segment_verify_ns: u64,
    pub stream_read_verify_ns: u64,
    pub finalize_ns: u64,
}

#[derive(Clone, Debug, Default)]
pub struct PeerCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub corruptions: u64,
    pub evictions: u64,
    pub hash_bytes_total: u64,
    /// Actual body-read attempts, excluding header and metadata reads.
    pub data_read_calls: u64,
    /// Explicit local cache body copies, including Bao encode/decode and
    /// final private-output copies, including failed reads. The historical
    /// metric label is `cache_to_output`; it is not just final delivery bytes.
    pub copied_bytes_total: u64,
    pub disk_bytes: u64,
    /// Peak charged ready and temporary blocks, including conservative reservations.
    pub disk_peak_bytes: u64,
    pub entries: usize,
    pub scratch_peak_bytes: u64,
    pub proof_metadata_bytes: u64,
    pub proof_metadata_peak_bytes: u64,
    pub hash_work: PeerCacheHashWork,
    pub read_stages: PeerCacheReadStages,
}

#[derive(Debug)]
pub struct DiskPeerCache {
    pub(super) options: PeerCacheOptions,
    pub(super) root: File,
    pub(super) _marker: File,
    pub(super) started: Instant,
    pub(super) state: Mutex<CacheState>,
    pub(super) changed: Condvar,
    pub(super) asynchronous_change: tokio::sync::Notify,
    pub(super) max_flights: usize,
    pub(super) read_work: Option<super::super::read_work::ReadWorkMetrics>,
    pub(super) proof_budget: Arc<OutboardBudget>,
}

#[derive(Debug)]
pub(super) struct CacheState {
    pub(super) budget: Arc<OutboardBudget>,
    pub(super) closed: bool,
    pub(super) entries: BTreeMap<String, Entry>,
    pub(super) retired: BTreeMap<String, RetiredEntry>,
    pub(super) fill_active: bool,
    pub(super) stream_active: usize,
    pub(super) foreground_waiters: usize,
    pub(super) next_pending_generation: u64,
    pub(super) pending: BTreeMap<u64, PendingEntry>,
    pub(super) operations: usize,
    pub(super) flights: BTreeMap<String, Arc<FillFlight>>,
    pub(super) flight_live: usize,
    pub(super) deleting_slots: usize,
    pub(super) local_index_available: bool,
    // Rust drops fields in declaration order: entries and their proof wrappers
    // must release before the reservation covering their pointer slots.
    pub(super) index_lease: Option<OutboardLease>,
    pub(super) used_bytes: u64,
    pub(super) pending_charge: u64,
    pub(super) pending_metadata_charge: usize,
    pub(super) operation_metadata_bytes: usize,
    pub(super) entry_metadata_bytes: usize,
    pub(super) retired_disk_bytes: u64,
    pub(super) flight_metadata_bytes: usize,
    pub(super) stats: PeerCacheStats,
    #[cfg(test)]
    pub(super) fail_remove: bool,
    #[cfg(test)]
    pub(super) fail_stream_write: bool,
    #[cfg(test)]
    pub(super) fail_stream_metadata: bool,
    #[cfg(test)]
    pub(super) proof_limit: Option<u64>,
}

#[derive(Debug)]
pub(super) struct Entry {
    pub(super) chunk: ChunkObject,
    pub(super) charge: u64,
    pub(super) identity: (u64, u64),
    pub(super) touched: u64,
    pub(super) proof: Option<Arc<VerifiedDiskOutboard>>,
    pub(super) generation: Arc<()>,
    pub(super) _metadata: Option<OutboardLease>,
    #[cfg(test)]
    pub(super) drop_observation: Option<DropObservation>,
}

#[cfg(test)]
#[derive(Debug)]
pub(super) struct DropObservation {
    pub(super) budget: Arc<OutboardBudget>,
    pub(super) observed: Arc<std::sync::atomic::AtomicUsize>,
}

#[cfg(test)]
impl Drop for DropObservation {
    fn drop(&mut self) {
        self.observed
            .store(self.budget.used(), std::sync::atomic::Ordering::SeqCst);
    }
}

#[derive(Serialize, Deserialize)]
pub(super) struct Header {
    pub(super) version: u32,
    pub(super) namespace_id: NamespaceId,
    pub(super) chunk: ChunkObject,
    #[serde(default)]
    pub(super) outboard_length: u32,
}

pub struct PeerCacheOperation<'a> {
    pub(super) cache: &'a DiskPeerCache,
    // Per-operation work only, never a MutexGuard or a body allocation.
    pub(super) state: CacheState,
    pub(super) fill: Option<FillPermit<'a>>,
    pub(super) _metadata: Option<OutboardLease>,
}

#[derive(Debug)]
pub(super) struct RetiredEntry {
    pub(super) entry: Entry,
    pub(super) deleting: bool,
}

pub(super) struct EntryPin<'a> {
    pub(super) cache: &'a DiskPeerCache,
    pub(super) generation: Option<Arc<()>>,
}

pub(super) struct FillPermit<'a> {
    pub(super) cache: &'a DiskPeerCache,
    pub(super) streaming: bool,
}

#[derive(Debug)]
pub(super) struct PendingEntry {
    pub(super) charge: u64,
    pub(super) identity: Option<(u64, u64)>,
    pub(super) active: bool,
    pub(super) metadata_charge: usize,
    pub(super) _metadata: Option<OutboardLease>,
}

pub(super) fn remove_pending_charge(
    state: &mut CacheState,
    generation: u64,
) -> Option<PendingEntry> {
    let pending = state.pending.remove(&generation)?;
    state.used_bytes = state.used_bytes.saturating_sub(pending.charge);
    state.pending_charge = state.pending_charge.saturating_sub(pending.charge);
    state.pending_metadata_charge = state
        .pending_metadata_charge
        .saturating_sub(pending.metadata_charge);
    Some(pending)
}

#[derive(Clone, Debug)]
pub(crate) enum FillOutcome {
    Ready,
    Uncached,
    Failed(Error),
}

#[derive(Debug)]
pub(super) struct FillFlight {
    pub(super) result: Mutex<Option<FillOutcome>>,
    pub(super) changed: Condvar,
    pub(super) asynchronous_change: tokio::sync::Notify,
    pub(super) _metadata: OutboardLease,
}

pub(crate) struct FillLeader<'a> {
    pub(super) cache: &'a DiskPeerCache,
    pub(super) key: String,
    pub(super) flight: Option<Arc<FillFlight>>,
    pub(super) completed: bool,
}

pub(crate) struct OwnedFillLeader {
    pub(super) cache: Arc<DiskPeerCache>,
    pub(super) key: String,
    pub(super) flight: Option<Arc<FillFlight>>,
    pub(super) completed: bool,
}

pub(crate) enum FillRole<'a> {
    Leader(FillLeader<'a>),
    Finished(FillOutcome),
}

impl CacheState {
    pub(super) fn new_local(budget: Arc<OutboardBudget>) -> Self {
        let mut state = Self::new_without_index(budget);
        state.local_index_available = true;
        state
    }

    fn new_without_index(budget: Arc<OutboardBudget>) -> Self {
        Self {
            budget,
            closed: false,
            entries: BTreeMap::new(),
            retired: BTreeMap::new(),
            fill_active: false,
            stream_active: 0,
            foreground_waiters: 0,
            next_pending_generation: 1,
            pending: BTreeMap::new(),
            operations: 0,
            flights: BTreeMap::new(),
            flight_live: 0,
            deleting_slots: 0,
            local_index_available: false,
            index_lease: None,
            used_bytes: 0,
            pending_charge: 0,
            pending_metadata_charge: 0,
            operation_metadata_bytes: 0,
            entry_metadata_bytes: 0,
            retired_disk_bytes: 0,
            flight_metadata_bytes: 0,
            stats: PeerCacheStats::default(),
            #[cfg(test)]
            fail_remove: false,
            #[cfg(test)]
            fail_stream_write: false,
            #[cfg(test)]
            fail_stream_metadata: false,
            #[cfg(test)]
            proof_limit: None,
        }
    }

    pub(super) fn new(budget: Arc<OutboardBudget>) -> Self {
        // All optional proof slots are bounded up front. A small shared budget
        // can disable proof acceleration without disabling complete verification.
        let index_lease = budget.try_reserve_resident(
            MAX_PEER_CACHE_ENTRIES * std::mem::size_of::<Option<Box<VerifiedDiskOutboard>>>(),
        );
        Self {
            budget,
            index_lease,
            closed: false,
            entries: BTreeMap::new(),
            retired: BTreeMap::new(),
            fill_active: false,
            stream_active: 0,
            foreground_waiters: 0,
            next_pending_generation: 1,
            pending: BTreeMap::new(),
            operations: 0,
            flights: BTreeMap::new(),
            flight_live: 0,
            deleting_slots: 0,
            local_index_available: false,
            used_bytes: 0,
            pending_charge: 0,
            pending_metadata_charge: 0,
            operation_metadata_bytes: 0,
            entry_metadata_bytes: 0,
            retired_disk_bytes: 0,
            flight_metadata_bytes: 0,
            stats: PeerCacheStats::default(),
            #[cfg(test)]
            fail_remove: false,
            #[cfg(test)]
            fail_stream_write: false,
            #[cfg(test)]
            fail_stream_metadata: false,
            #[cfg(test)]
            proof_limit: None,
        }
    }
}
