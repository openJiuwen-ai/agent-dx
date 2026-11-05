//! Serving local Chunks to peers: budgets, admission and single-flight verification.

use super::*;

/// Shared across the current verified object, in-flight ranges and RDMA packed
/// payloads. This replaces independent per-request payload allowances; it does
/// not add a second retained-cache budget.
#[cfg(any(feature = "dfs", test))]
pub(crate) const PEER_READ_PAYLOAD_LIMIT: usize = 16 * 1024 * 1024;

#[cfg(any(feature = "dfs", test))]
pub(crate) const PEER_READ_OBJECT_LIMIT: usize = 4 * 1024 * 1024;

#[derive(Debug)]
#[cfg(any(feature = "dfs", test))]
pub(super) struct PeerReadBudget {
    pub(super) used: AtomicU64,
    pub(super) peak: AtomicU64,
    pub(super) limit: u64,
    pub(super) control_used: AtomicU64,
    pub(super) control_peak: AtomicU64,
    // This lock only pairs admission predicates with release notifications.
    // Never acquire the serve table or destroy charged owners while holding it.
    admission: Mutex<()>,
    pub(super) changed: Condvar,
    async_changed: tokio::sync::Notify,
    stopped: std::sync::atomic::AtomicBool,
    pub(super) wait: Duration,
    #[cfg(test)]
    pub(super) before_wait: Mutex<Option<PeerFillTestBlock>>,
}

#[cfg(any(feature = "dfs", test))]
impl Default for PeerReadBudget {
    fn default() -> Self {
        Self {
            used: AtomicU64::new(0),
            peak: AtomicU64::new(0),
            limit: PEER_READ_PAYLOAD_LIMIT as u64,
            control_used: AtomicU64::new(0),
            control_peak: AtomicU64::new(0),
            admission: Mutex::new(()),
            changed: Condvar::new(),
            async_changed: tokio::sync::Notify::new(),
            stopped: std::sync::atomic::AtomicBool::new(false),
            wait: Duration::from_millis(20),
            #[cfg(test)]
            before_wait: Mutex::new(None),
        }
    }
}

// This is the bounded envelope of control allocations, not a second body pool.
// Fixed inline slots plus actual retained key/error/Arc capacities fit in 64KiB.
#[cfg(any(feature = "dfs", test))]
pub(super) const PEER_SERVE_CONTROL_LIMIT: usize = 64 * 1024;

#[cfg(any(feature = "dfs", test))]
pub(super) const PEER_SERVE_MAX_RESIDENTS: usize = 16;

#[cfg(any(feature = "dfs", test))]
pub(super) const PEER_SERVE_MAX_FLIGHTS: usize = 8;

#[cfg(any(feature = "dfs", test))]
const PEER_SERVE_ID_LIMIT: usize = 512;

#[cfg(any(feature = "dfs", test))]
pub(super) fn peer_serve_fixed_control_bytes() -> usize {
    std::mem::size_of::<Mutex<PeerServeState>>()
        + std::mem::size_of::<PeerReadBudget>()
        + std::mem::size_of::<Condvar>()
        + 64
}

#[derive(Debug)]
#[cfg(any(feature = "dfs", test))]
pub(super) struct PeerControlLease {
    budget: Arc<PeerReadBudget>,
    pub(super) bytes: u64,
    pub(super) metrics: Option<super::super::read_work::ReadWorkMetrics>,
}

#[cfg(any(feature = "dfs", test))]
impl Drop for PeerControlLease {
    fn drop(&mut self) {
        let _admission = self
            .budget
            .admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.budget
            .control_used
            .fetch_sub(self.bytes, Ordering::AcqRel);
        if let Some(metrics) = &self.metrics {
            metrics.peer_serve_control(-(self.bytes as i64), 0);
        }
        self.budget.changed.notify_all();
        self.budget.async_changed.notify_waiters();
    }
}

#[cfg(any(feature = "dfs", test))]
pub(super) type PeerServeResult = Result<Option<Arc<PeerVerifiedObject>>>;

#[derive(Debug)]
#[cfg(any(feature = "dfs", test))]
pub(super) struct PeerServeFlight {
    chunk_id: ChunkId,
    result: Mutex<Option<PeerServeResult>>,
    pub(super) changed: Condvar,
    _control: PeerControlLease,
}

#[derive(Debug)]
#[cfg(any(feature = "dfs", test))]
pub(super) struct PeerServeState {
    pub(super) resident_limit: usize,
    pub(super) residents: [Option<Arc<PeerVerifiedObject>>; PEER_SERVE_MAX_RESIDENTS],
    pub(super) flights: [Option<Arc<PeerServeFlight>>; PEER_SERVE_MAX_FLIGHTS],
    pub(super) readers: usize,
    closed: bool,
    pub(super) stop_deadline: Option<Instant>,
}

#[cfg(any(feature = "dfs", test))]
impl Default for PeerServeState {
    fn default() -> Self {
        Self {
            resident_limit: 2,
            residents: std::array::from_fn(|_| None),
            flights: std::array::from_fn(|_| None),
            readers: 0,
            closed: false,
            stop_deadline: None,
        }
    }
}

#[cfg(any(feature = "dfs", test))]
impl PeerServeState {
    pub(super) fn resident(&self, chunk_id: &ChunkId) -> Option<Arc<PeerVerifiedObject>> {
        self.residents
            .iter()
            .flatten()
            .find(|object| {
                object
                    .identity
                    .as_ref()
                    .is_some_and(|(record, _)| record.chunk.id == *chunk_id)
            })
            .cloned()
    }

    fn take_resident(&mut self, index: usize) -> Option<Arc<PeerVerifiedObject>> {
        let removed = self.residents[index].take();
        for position in index..PEER_SERVE_MAX_RESIDENTS - 1 {
            self.residents[position] = self.residents[position + 1].take();
        }
        removed
    }

    pub(super) fn remove_resident(
        &mut self,
        chunk_id: &ChunkId,
    ) -> Option<Arc<PeerVerifiedObject>> {
        let index = self.residents.iter().position(|object| {
            object.as_ref().is_some_and(|object| {
                object
                    .identity
                    .as_ref()
                    .is_some_and(|(record, _)| record.chunk.id == *chunk_id)
            })
        })?;
        self.take_resident(index)
    }

    fn install(&mut self, object: Arc<PeerVerifiedObject>) -> Option<Arc<PeerVerifiedObject>> {
        let victim = self.residents[self.resident_limit - 1].take();
        for position in (1..self.resident_limit).rev() {
            self.residents[position] = self.residents[position - 1].take();
        }
        self.residents[0] = Some(object);
        victim
    }
}

#[cfg(any(feature = "dfs", test))]
pub(super) struct PeerServeReadGuard<'a>(&'a LocalChunkStore);

#[cfg(any(feature = "dfs", test))]
impl Drop for PeerServeReadGuard<'_> {
    fn drop(&mut self) {
        if let Ok(mut state) = self.0.peer_read_cache.lock() {
            state.readers = state.readers.saturating_sub(1);
            self.0.observe_peer_serve(&state);
        }
        self.0.peer_serve_changed.notify_all();
    }
}

#[cfg(any(feature = "dfs", test))]
struct PeerServeLeader<'a> {
    store: &'a LocalChunkStore,
    flight: Arc<PeerServeFlight>,
    finished: bool,
}

#[cfg(any(feature = "dfs", test))]
impl PeerServeLeader<'_> {
    fn finish(&mut self, outcome: PeerServeResult) {
        if !self.finished
            && let Ok(mut result) = self.flight.result.lock()
        {
            *result = Some(outcome);
            self.finished = true;
            self.flight.changed.notify_all();
        }
    }
}

#[cfg(any(feature = "dfs", test))]
impl Drop for PeerServeLeader<'_> {
    fn drop(&mut self) {
        self.finish(Err(peer_serve_unavailable(
            "Peer object fill ended before completion",
        )));
        let removed = if let Ok(mut state) = self.store.peer_read_cache.lock() {
            let index = state.flights.iter().position(|flight| {
                flight
                    .as_ref()
                    .is_some_and(|flight| Arc::ptr_eq(flight, &self.flight))
            });
            let removed = index.and_then(|index| state.flights[index].take());
            self.store.observe_peer_serve(&state);
            removed
        } else {
            None
        };
        drop(removed);
        self.store.peer_serve_changed.notify_all();
    }
}

#[derive(Debug)]
#[cfg(any(feature = "dfs", test))]
pub(crate) struct PeerReadReservation {
    budget: Arc<PeerReadBudget>,
    pub(super) bytes: u64,
    pub(super) metrics: Option<super::super::read_work::ReadWorkMetrics>,
}

#[cfg(any(feature = "dfs", test))]
impl PeerReadReservation {
    // Vec::try_reserve_exact normally gives the requested capacity; still
    // account the allocator's actual capacity before handing a payload onward.
    pub(crate) fn retain_capacity(&mut self, capacity: usize) -> Result<()> {
        let capacity = u64::try_from(capacity).map_err(|_| peer_payload_capacity())?;
        if capacity > self.bytes {
            let extra = capacity - self.bytes;
            let _admission = self
                .budget
                .admission
                .lock()
                .map_err(|_| peer_serve_unavailable("Peer budget admission poisoned"))?;
            // This lease was admitted before stop. Drain still waits for its
            // Arc, including a zero-byte owner; actual capacity must remain
            // charged without admitting another owner or exceeding the limit.
            charge_peer_bytes_locked(&self.budget, extra, self.metrics.as_ref())?;
            self.bytes = capacity;
        }
        Ok(())
    }
}

#[cfg(any(feature = "dfs", test))]
impl Drop for PeerReadReservation {
    fn drop(&mut self) {
        let _admission = self
            .budget
            .admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.budget.used.fetch_sub(self.bytes, Ordering::AcqRel);
        if let Some(metrics) = &self.metrics {
            metrics.peer_payload_released(self.bytes);
        }
        self.budget.changed.notify_all();
        self.budget.async_changed.notify_waiters();
    }
}

#[cfg(any(feature = "dfs", test))]
fn peer_payload_capacity() -> Error {
    Error::coded(
        afs_error::IO_CAPACITY,
        "shared Peer read payload budget exhausted",
    )
}

#[cfg(any(feature = "dfs", test))]
fn peer_serve_unavailable(message: &str) -> Error {
    Error::coded(afs_error::IO_UNAVAILABLE, message)
}

#[cfg(any(feature = "dfs", test))]
fn reserve_peer_bytes(
    budget: &PeerReadBudget,
    bytes: u64,
    metrics: Option<&super::super::read_work::ReadWorkMetrics>,
) -> Result<()> {
    let _admission = budget
        .admission
        .lock()
        .map_err(|_| peer_serve_unavailable("Peer budget admission poisoned"))?;
    reserve_peer_bytes_locked(budget, bytes, metrics)
}

#[cfg(any(feature = "dfs", test))]
fn reserve_peer_bytes_locked(
    budget: &PeerReadBudget,
    bytes: u64,
    metrics: Option<&super::super::read_work::ReadWorkMetrics>,
) -> Result<()> {
    if budget.stopped.load(Ordering::Acquire) {
        return Err(peer_serve_unavailable("Peer serve is stopped"));
    }
    charge_peer_bytes_locked(budget, bytes, metrics)
}

#[cfg(any(feature = "dfs", test))]
fn charge_peer_bytes_locked(
    budget: &PeerReadBudget,
    bytes: u64,
    metrics: Option<&super::super::read_work::ReadWorkMetrics>,
) -> Result<()> {
    let mut used = budget.used.load(Ordering::Acquire);
    loop {
        let next = used
            .checked_add(bytes)
            .filter(|next| *next <= budget.limit)
            .ok_or_else(peer_payload_capacity)?;
        match budget
            .used
            .compare_exchange_weak(used, next, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => {
                let old_peak = budget.peak.fetch_max(next, Ordering::AcqRel);
                if let Some(metrics) = metrics {
                    metrics.peer_payload_reserved(bytes, next.saturating_sub(old_peak));
                }
                return Ok(());
            }
            Err(actual) => used = actual,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct PeerFileIdentity {
    pub(super) length: u64,
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
    #[cfg(unix)]
    modified: (i64, i64),
    #[cfg(unix)]
    pub(super) changed: (i64, i64),
}

impl PeerFileIdentity {
    pub(super) fn read(file: &File) -> Result<Self> {
        let metadata = file.metadata().map_err(Error::from)?;
        #[cfg(unix)]
        use std::os::unix::fs::MetadataExt;
        Ok(Self {
            length: metadata.len(),
            #[cfg(unix)]
            device: metadata.dev(),
            #[cfg(unix)]
            inode: metadata.ino(),
            #[cfg(unix)]
            modified: (metadata.mtime(), metadata.mtime_nsec()),
            #[cfg(unix)]
            changed: (metadata.ctime(), metadata.ctime_nsec()),
        })
    }
}

#[derive(Debug)]
#[cfg(any(feature = "dfs", test))]
pub(super) struct PeerVerifiedObject {
    pub(super) bytes: super::super::read::buffer::ReadBuffer,
    pub(super) outboard: Option<SharedOutboard>,
    _reservation: PeerReadReservation,
    // A range fallback is never resident and needs no reusable identity.
    pub(super) identity: Option<(LocalChunkRecord, PeerFileIdentity)>,
    _control: PeerControlLease,
}

/// A view into an immutable private allocation. Its shared reservation remains
/// charged even after its LRU entry is retired or an RPC is cancelled.
#[derive(Debug)]
#[cfg(any(feature = "dfs", test))]
pub(crate) struct PeerReadPayload {
    pub(super) object: Arc<PeerVerifiedObject>,
    start: usize,
    pub(super) length: usize,
}

#[cfg(any(feature = "dfs", test))]
impl PeerReadPayload {
    pub(crate) fn as_slice(&self) -> &[u8] {
        &self.object.bytes[self.start..self.start + self.length]
    }

    #[cfg(feature = "dfs")]
    pub(crate) fn len(&self) -> usize {
        self.length
    }

    /// Content-only proof from the complete verified immutable allocation.
    /// A subrange or nonresident fallback still needs its own range digest;
    /// neither transport headers nor callers can manufacture this capability.
    pub(crate) fn verified_full_object_digest(&self) -> Option<ContentDigest> {
        let (record, identity) = self.object.identity.as_ref()?;
        if self.start != 0
            || self.length != self.object.bytes.len()
            || self.length as u64 != record.chunk.length
            || identity.length != record.chunk.length
            || record.stored_length != record.chunk.length
            || record.stored_checksum != record.chunk.content_digest
        {
            return None;
        }
        Some(record.chunk.content_digest.clone())
    }

    pub(crate) fn outboard(&self) -> Option<SharedOutboard> {
        self.verified_full_object_digest()?;
        self.object.outboard.clone()
    }

    /// Frame slices retain both immutable bytes and their original budget lease.
    pub(crate) fn into_shared_bytes(self) -> prost::bytes::Bytes {
        prost::bytes::Bytes::from_owner(self)
    }
}

#[cfg(any(feature = "dfs", test))]
impl AsRef<[u8]> for PeerReadPayload {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

// A stream owns only two coverage-group allocations. Returning a frame to
// tonic does not return its block: Bytes' last reference performs the return.
#[cfg(feature = "dfs")]
pub(super) struct PeerStreamBlock {
    pub(super) bytes: Vec<u8>,
    _reservation: PeerReadReservation,
}

#[cfg(feature = "dfs")]
pub(super) struct PeerStreamPool {
    free: Mutex<[Option<PeerStreamBlock>; 2]>,
    pub(super) changed: Condvar,
    _control: PeerControlLease,
}

#[cfg(feature = "dfs")]
struct PeerStreamFrame {
    pub(super) block: Option<PeerStreamBlock>,
    pub(super) pool: Arc<PeerStreamPool>,
}

#[cfg(feature = "dfs")]
impl AsRef<[u8]> for PeerStreamFrame {
    fn as_ref(&self) -> &[u8] {
        self.block.as_ref().map_or(&[], |block| &block.bytes)
    }
}

#[cfg(feature = "dfs")]
impl Drop for PeerStreamFrame {
    fn drop(&mut self) {
        let Some(block) = self.block.take() else {
            return;
        };
        // Poisoning cannot allow early budget release or abandon a waiter.
        // These slots only move owners; no user callback runs under this lock.
        let mut free = self
            .pool
            .free
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(slot) = free.iter_mut().find(|slot| slot.is_none()) {
            *slot = Some(block);
        }
        self.pool.changed.notify_one();
    }
}

/// Pinned immutable object, root-bound proof, and two budgeted frame buffers.
/// Each stream verifies independently; no resident body cache is populated.
#[cfg(feature = "dfs")]
pub(crate) struct PeerGroupReader<'a> {
    store: &'a LocalChunkStore,
    reader: PinnedChunkReader,
    record: LocalChunkRecord,
    pub(super) identity: PeerFileIdentity,
    proof: SharedOutboard,
    pub(super) pool: Arc<PeerStreamPool>,
    offset: u64,
    _reader: PeerServeReadGuard<'a>,
}

#[cfg(feature = "dfs")]
impl PeerGroupReader<'_> {
    pub(crate) fn outboard(&self) -> SharedOutboard {
        self.proof.clone()
    }

    pub(crate) fn digest(&self) -> ContentDigest {
        self.record.chunk.content_digest.clone()
    }

    fn check_identity(&self, check_path: bool) -> Result<()> {
        self.store.ensure_peer_serve_open()?;
        if self.store.peer_catalog_record(&self.record.chunk.id)? != self.record
            || PeerFileIdentity::read(&self.reader.file)? != self.identity
        {
            return Err(corrupt(
                &self.record.chunk.id,
                "identity changed during group stream",
            ));
        }
        // The same descriptor supplies root-verified bytes throughout. Opening
        // the pathname per group adds no integrity: check it at stream entry
        // and before Completion, and reject a replaced path at that boundary.
        if check_path {
            let current =
                File::open(self.store.path_for_record(&self.record)?).map_err(Error::from)?;
            if PeerFileIdentity::read(&current)? != self.identity {
                return Err(corrupt(
                    &self.record.chunk.id,
                    "path changed during group stream",
                ));
            }
        }
        Ok(())
    }

    fn retire_failed_proof(&self) {
        let mut hash = std::collections::hash_map::DefaultHasher::new();
        self.reader.outboard_path.hash(&mut hash);
        let index = (hash.finish() % OUTBOARD_REPAIR_SHARDS as u64) as usize;
        let Ok(_repair) = self.reader.outboard_repairs[index].lock() else {
            return;
        };
        // Disposable proof damage must not poison every future stream. Remove
        // only our observed bytes under the existing installer/repair gate;
        // never remove a newer repaired proof or the immutable body/catalog.
        if outboard_file_matches(&self.reader.outboard_path, &self.proof).unwrap_or(false)
            && fs::remove_file(&self.reader.outboard_path).is_ok()
        {
            self.store.record_peer_read_event("stream_proof_retired");
        }
    }

    /// Cancellation, expiry and stop are checked even while both output blocks
    /// are retained downstream. No body is freed/refunded to unblock a writer.
    pub(crate) fn next_group(
        &mut self,
        check_caller: impl Fn() -> Result<()>,
    ) -> Result<Option<prost::bytes::Bytes>> {
        check_caller()?;
        self.check_identity(self.offset == 0 || self.offset == self.record.chunk.length)?;
        if self.offset == self.record.chunk.length {
            return Ok(None);
        }
        let mut block = loop {
            check_caller()?;
            self.store.ensure_peer_serve_open()?;
            let mut free = self
                .pool
                .free
                .lock()
                .map_err(|_| peer_serve_unavailable("Peer stream pool poisoned"))?;
            if let Some(block) = free.iter_mut().find_map(Option::take) {
                break block;
            }
            let (guard, _) = self
                .pool
                .changed
                .wait_timeout(free, Duration::from_millis(20))
                .map_err(|_| peer_serve_unavailable("Peer stream pool poisoned"))?;
            drop(guard);
        };
        let length = usize::try_from(
            (self.record.chunk.length - self.offset).min(outboard::OUTBOARD_GROUP_BYTES as u64),
        )
        .map_err(|_| peer_payload_capacity())?;
        block.bytes.resize(length, 0);
        let mut work = OutboardReadWork::default();
        let read_verify = self
            .store
            .coarse_diagnostic("peer_read_verify", length as u64);
        let result = self.proof.read_ranges_into_with_work(
            &self.reader.file,
            0,
            &self.record.chunk,
            &[VerifiedRangeCopy {
                chunk_offset: self.offset,
                length,
                output_offset: 0,
            }],
            &mut block.bytes,
            &mut work,
        );
        drop(read_verify);
        #[cfg(test)]
        VERIFIED_SCAN_BYTES.with(|count| count.set(count.get() + work.hash_bytes));
        if let Some(metrics) = &self.store.read_work {
            metrics.hash("outboard_body_hash", work.hash_bytes);
            metrics.hash("outboard_parent_hash", work.parent_hash_bytes);
            metrics.copy("outboard_range_delivery", work.copied_bytes);
        }
        // Failed/unverified bytes remain private and cannot enter the channel.
        let checked = result.and_then(|counts| {
            if counts != [length] {
                return Err(corrupt(
                    &self.record.chunk.id,
                    "group ended before expected length",
                ));
            }
            check_caller()?;
            self.check_identity(false)
        });
        if let Err(error) = checked {
            if verified_copy_is_invalid(&error) {
                self.store.record_peer_read_event("stream_group_corrupt");
                self.retire_failed_proof();
                self.store
                    .lifecycle()
                    .quarantine(&self.record.chunk.id, QuarantineTrigger::PeerStreamGroup);
            }
            return Err(error);
        }
        self.offset += length as u64;
        self.store.record_peer_read_event("stream_verified_group");
        Ok(Some(prost::bytes::Bytes::from_owner(PeerStreamFrame {
            block: Some(block),
            pool: self.pool.clone(),
        })))
    }
}

#[cfg(test)]
pub(super) type PeerFillTestBlock = (std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<()>);

impl LocalChunkStore {
    #[cfg(any(feature = "dfs", test))]
    pub(crate) fn with_peer_serve_config(
        mut self,
        resident_objects: usize,
        payload_bytes: u64,
    ) -> Result<Self> {
        if !(1..=PEER_SERVE_MAX_RESIDENTS).contains(&resident_objects)
            || !(4 * 1024 * 1024..=64 * 1024 * 1024).contains(&payload_bytes)
        {
            return Err(invalid(
                "Peer serve requires 1..=16 residents and 4..=64MiB payload",
            ));
        }
        let state = self
            .peer_read_cache
            .get_mut()
            .map_err(|_| peer_serve_unavailable("Peer serve state poisoned"))?;
        if state.closed
            || state.readers != 0
            || state.flights.iter().any(Option::is_some)
            || state.residents.iter().any(Option::is_some)
            || Arc::strong_count(&self.peer_read_budget) != 1
        {
            return Err(invalid("cannot reconfigure active Peer serve owners"));
        }
        let budget = Arc::get_mut(&mut self.peer_read_budget)
            .ok_or_else(|| invalid("Peer serve budget still has owners"))?;
        if budget.used.load(Ordering::Acquire) != 0
            || budget.control_used.load(Ordering::Acquire) != 0
        {
            return Err(invalid("cannot reconfigure charged Peer serve budget"));
        }
        budget.limit = payload_bytes;
        state.resident_limit = resident_objects;
        Ok(self)
    }

    /// Admission waits share one fixed entry deadline. This does not limit
    /// active I/O, a shared object's completion, or downstream frame lifetime.
    #[cfg(any(feature = "dfs", test))]
    pub(crate) fn with_peer_serve_wait(mut self, wait: Duration) -> Result<Self> {
        if wait > Duration::from_millis(50) {
            return Err(invalid("Peer serve admission wait must be 0..=50 ms"));
        }
        let state = self
            .peer_read_cache
            .get_mut()
            .map_err(|_| peer_serve_unavailable("Peer serve state poisoned"))?;
        if state.closed
            || state.readers != 0
            || state.flights.iter().any(Option::is_some)
            || state.residents.iter().any(Option::is_some)
        {
            return Err(invalid("cannot reconfigure active Peer serve owners"));
        }
        let budget = Arc::get_mut(&mut self.peer_read_budget)
            .ok_or_else(|| invalid("Peer serve budget still has owners"))?;
        if budget.used.load(Ordering::Acquire) != 0
            || budget.control_used.load(Ordering::Acquire) != 0
            || budget.stopped.load(Ordering::Acquire)
        {
            return Err(invalid("cannot reconfigure charged Peer serve budget"));
        }
        budget.wait = wait;
        Ok(self)
    }

    #[cfg(any(feature = "dfs", test))]
    pub(crate) fn peer_serve_deadline(&self) -> Instant {
        Instant::now() + self.peer_read_budget.wait
    }

    #[cfg(any(feature = "dfs", test))]
    pub(super) fn observe_peer_serve(&self, state: &PeerServeState) {
        if let Some(metrics) = &self.read_work {
            metrics.peer_serve_resources(
                state.residents.iter().flatten().count(),
                state.flights.iter().flatten().count(),
                state.readers,
                self.peer_read_budget.limit,
                peer_serve_fixed_control_bytes(),
            );
        }
    }

    #[cfg(test)]
    pub(super) fn reserve_peer_control(&self, bytes: usize) -> Result<PeerControlLease> {
        self.reserve_peer_control_until(bytes, Instant::now())
    }

    #[cfg(any(feature = "dfs", test))]
    pub(super) fn reserve_peer_control_until(
        &self,
        bytes: usize,
        deadline: Instant,
    ) -> Result<PeerControlLease> {
        let fixed = peer_serve_fixed_control_bytes();
        let limit = PEER_SERVE_CONTROL_LIMIT.saturating_sub(fixed) as u64;
        let bytes = u64::try_from(bytes).map_err(|_| peer_payload_capacity())?;
        if bytes > limit {
            return Err(peer_payload_capacity());
        }
        let mut admission = self
            .peer_read_budget
            .admission
            .lock()
            .map_err(|_| peer_serve_unavailable("Peer budget admission poisoned"))?;
        loop {
            if self.peer_read_budget.stopped.load(Ordering::Acquire) {
                return Err(peer_serve_unavailable("Peer serve is stopped"));
            }
            let used = self.peer_read_budget.control_used.load(Ordering::Acquire);
            let Some(next) = used.checked_add(bytes).filter(|next| *next <= limit) else {
                admission = self.wait_peer_budget(admission, deadline)?;
                continue;
            };
            match self.peer_read_budget.control_used.compare_exchange_weak(
                used,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    let peak = self
                        .peer_read_budget
                        .control_peak
                        .fetch_max(next, Ordering::AcqRel);
                    if let Some(metrics) = &self.read_work {
                        metrics.peer_serve_control(bytes as i64, next.saturating_sub(peak));
                    }
                    return Ok(PeerControlLease {
                        budget: self.peer_read_budget.clone(),
                        bytes,
                        metrics: self.read_work.clone(),
                    });
                }
                Err(_) => continue,
            }
        }
    }

    #[cfg(any(feature = "dfs", test))]
    fn peer_object_control(
        &self,
        record: Option<&LocalChunkRecord>,
        deadline: Instant,
    ) -> Result<PeerControlLease> {
        // Includes Arc headers, field owners and conservative allocator padding;
        // body Vec capacity and shared proof allocation are charged separately.
        let strings = record.map_or(0, |record| {
            let PhysicalLocation::PerChunkFile { relative_path } = &record.location;
            record.chunk.id.0.capacity() + record.device_id.capacity() + relative_path.capacity()
        });
        self.reserve_peer_control_until(
            std::mem::size_of::<PeerVerifiedObject>() + 128 + strings,
            deadline,
        )
    }

    #[cfg(test)]
    pub(crate) fn waiting_peer_fill_readers_for_test(&self) -> usize {
        self.peer_read_cache
            .lock()
            .unwrap()
            .flights
            .iter()
            .flatten()
            .map(|flight| Arc::strong_count(flight).saturating_sub(2))
            .sum()
    }

    #[cfg(test)]
    pub(crate) fn pause_next_peer_fill_for_test(
        &self,
        chunk_id: ChunkId,
    ) -> (std::sync::mpsc::Receiver<()>, std::sync::mpsc::Sender<()>) {
        let (entered, observed) = std::sync::mpsc::channel();
        let (release, wait) = std::sync::mpsc::channel();
        self.peer_fill_test_blocks
            .lock()
            .unwrap()
            .insert(chunk_id, (entered, wait));
        (observed, release)
    }

    #[cfg(test)]
    pub(super) fn clear_peer_residents_for_test(&self) {
        let residents = {
            let mut state = self.peer_read_cache.lock().unwrap();
            std::mem::replace(&mut state.residents, std::array::from_fn(|_| None))
        };
        drop(residents);
    }

    #[cfg(any(feature = "dfs", test))]
    fn begin_peer_serve_read(&self) -> Result<PeerServeReadGuard<'_>> {
        let mut state = self
            .peer_read_cache
            .lock()
            .map_err(|_| peer_serve_unavailable("Peer serve state poisoned"))?;
        if state.closed {
            return Err(peer_serve_unavailable("Peer serve is stopped"));
        }
        state.readers = state
            .readers
            .checked_add(1)
            .ok_or_else(peer_payload_capacity)?;
        self.observe_peer_serve(&state);
        Ok(PeerServeReadGuard(self))
    }

    // A stale Ready holder may observe another physical inode after a newer
    // flight has installed a healthy resident. Retire only our observed Arc.
    #[cfg(any(feature = "dfs", test))]
    fn retire_observed_peer_resident(&self, observed: Option<&Arc<PeerVerifiedObject>>) {
        let Some(observed) = observed else {
            return;
        };
        let retired = if let Ok(mut state) = self.peer_read_cache.lock() {
            let index = state.residents.iter().position(|current| {
                current
                    .as_ref()
                    .is_some_and(|current| Arc::ptr_eq(current, observed))
            });
            let retired = index.and_then(|index| state.take_resident(index));
            self.observe_peer_serve(&state);
            retired
        } else {
            None
        };
        drop(retired);
    }

    #[cfg(any(feature = "dfs", test))]
    pub(crate) fn ensure_peer_serve_open(&self) -> Result<()> {
        self.record_read_check(super::super::read_work::ReadCheck::ServeState);
        let state = self
            .peer_read_cache
            .lock()
            .map_err(|_| peer_serve_unavailable("Peer serve state poisoned"))?;
        if state.closed {
            Err(peer_serve_unavailable("Peer serve is stopped"))
        } else {
            Ok(())
        }
    }

    #[cfg(any(feature = "dfs", test))]
    pub(crate) fn close_peer_serve_admission(&self) {
        // Do not nest this lock with the serve table: a last charged owner can
        // be dropped while metadata is retired by another thread.
        {
            let _admission = self
                .peer_read_budget
                .admission
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            self.peer_read_budget.stopped.store(true, Ordering::Release);
            self.peer_read_budget.changed.notify_all();
            self.peer_read_budget.async_changed.notify_waiters();
        }
        let residents = if let Ok(mut state) = self.peer_read_cache.lock() {
            state.closed = true;
            state
                .stop_deadline
                .get_or_insert_with(|| Instant::now() + Duration::from_secs(10));
            for flight in state.flights.iter().flatten() {
                flight.changed.notify_all();
            }
            let residents = std::mem::replace(&mut state.residents, std::array::from_fn(|_| None));
            self.observe_peer_serve(&state);
            residents
        } else {
            std::array::from_fn(|_| None)
        };
        drop(residents);
        self.peer_serve_changed.notify_all();
    }

    #[cfg(any(feature = "dfs", test))]
    pub(crate) fn shutdown_peer_serve(&self) -> Result<()> {
        self.close_peer_serve_admission();
        let mut state = self
            .peer_read_cache
            .lock()
            .map_err(|_| peer_serve_unavailable("Peer serve state poisoned"))?;
        let deadline = state
            .stop_deadline
            .ok_or_else(|| peer_serve_unavailable("Peer serve stop deadline missing"))?;
        loop {
            if state.readers == 0
                && state.flights.iter().all(Option::is_none)
                && self.peer_read_budget.used.load(Ordering::Acquire) == 0
                && self.peer_read_budget.control_used.load(Ordering::Acquire) == 0
                // A zero-byte lease still owns the right to retain capacity.
                // All aliases are private real owners (or test observations).
                && Arc::strong_count(&self.peer_read_budget) == 1
            {
                self.observe_peer_serve(&state);
                return Ok(());
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(Error::coded(
                    afs_error::IO_TIMEOUT,
                    "Peer serve drain deadline exceeded",
                ));
            }
            state = self
                .peer_serve_changed
                .wait_timeout(state, remaining.min(Duration::from_millis(25)))
                .map_err(|_| peer_serve_unavailable("Peer serve drain state poisoned"))?
                .0;
        }
    }

    #[cfg(any(all(feature = "dfs", feature = "rdma"), test))]
    fn reserve_peer_read_payload_inner(&self, bytes: usize) -> Result<PeerReadReservation> {
        self.reserve_peer_read_payload_until(bytes, Instant::now())
    }

    #[cfg(any(feature = "dfs", test))]
    fn wait_peer_budget<'a>(
        &'a self,
        admission: std::sync::MutexGuard<'a, ()>,
        deadline: Instant,
    ) -> Result<std::sync::MutexGuard<'a, ()>> {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(peer_payload_capacity());
        }
        #[cfg(test)]
        {
            let block = self.peer_read_budget.before_wait.lock().unwrap().take();
            if let Some((entered, release)) = block {
                entered.send(()).unwrap();
                release.recv_timeout(Duration::from_secs(3)).unwrap();
            }
        }
        // Predicate and release notifications use this same mutex. The wait
        // atomically releases it, so release cannot fall into a check/wait gap.
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(peer_payload_capacity());
        }
        let _wait = self.coarse_diagnostic("peer_slot_wait", 0);
        self.peer_read_budget
            .changed
            .wait_timeout(admission, remaining)
            .map(|(guard, _)| guard)
            .map_err(|_| peer_serve_unavailable("Peer budget admission wait poisoned"))
    }

    #[cfg(any(feature = "dfs", test))]
    pub(super) fn reserve_peer_read_payload_until(
        &self,
        bytes: usize,
        deadline: Instant,
    ) -> Result<PeerReadReservation> {
        let bytes = u64::try_from(bytes).map_err(|_| peer_payload_capacity())?;
        if bytes > self.peer_read_budget.limit {
            return Err(peer_payload_capacity());
        }
        let mut result = reserve_peer_bytes(&self.peer_read_budget, bytes, self.read_work.as_ref());
        while result
            .as_ref()
            .is_err_and(|error| error.code() == afs_error::IO_CAPACITY)
        {
            // Remove only a genuinely unpinned LRU; actual destruction and
            // ledger release happen outside the table lock.
            let victim = {
                let mut state = self
                    .peer_read_cache
                    .lock()
                    .map_err(|_| peer_serve_unavailable("Peer serve state poisoned"))?;
                let index = state.residents.iter().rposition(|object| {
                    object
                        .as_ref()
                        .is_some_and(|object| Arc::strong_count(object) == 1)
                });
                let victim = index.and_then(|index| state.take_resident(index));
                self.observe_peer_serve(&state);
                victim
            };
            if victim.is_some() {
                drop(victim);
                result = reserve_peer_bytes(&self.peer_read_budget, bytes, self.read_work.as_ref());
                continue;
            }
            let admission = self
                .peer_read_budget
                .admission
                .lock()
                .map_err(|_| peer_serve_unavailable("Peer budget admission poisoned"))?;
            // Another last-reference release may have happened during LRU
            // inspection. Recheck the pure atomic budget while holding the
            // notification mutex; never acquire the LRU table from this scope.
            result =
                reserve_peer_bytes_locked(&self.peer_read_budget, bytes, self.read_work.as_ref());
            if result
                .as_ref()
                .is_err_and(|error| error.code() == afs_error::IO_CAPACITY)
            {
                drop(self.wait_peer_budget(admission, deadline)?);
                result = reserve_peer_bytes(&self.peer_read_budget, bytes, self.read_work.as_ref());
            }
        }
        result?;
        Ok(PeerReadReservation {
            budget: self.peer_read_budget.clone(),
            bytes,
            metrics: self.read_work.clone(),
        })
    }

    /// RDMA packed and object payloads share one configured actual-capacity ledger.
    #[cfg(any(all(feature = "dfs", feature = "rdma"), test))]
    pub(crate) fn reserve_peer_read_payload(&self, bytes: usize) -> Result<PeerReadReservation> {
        let _reader = self.begin_peer_serve_read()?;
        #[cfg(test)]
        {
            let block = self.peer_payload_test_block.lock().unwrap().take();
            if let Some((entered, release)) = block {
                entered.send(()).unwrap();
                release.recv_timeout(Duration::from_secs(5)).unwrap();
            }
        }
        self.reserve_peer_read_payload_inner(bytes)
    }

    #[cfg(any(feature = "dfs", test))]
    pub(super) fn peer_catalog_record(&self, chunk_id: &ChunkId) -> Result<LocalChunkRecord> {
        let record = self
            .catalog
            .lock()
            .map_err(|_| invalid("local chunk catalog lock is poisoned"))?
            .records
            .get(chunk_id)
            .cloned()
            .ok_or_else(|| invalid(format!("local Chunk '{}' is not cataloged", chunk_id.0)))?;
        if record.state != LocalChunkState::Durable {
            return Err(corrupt(chunk_id, "local copy is quarantined"));
        }
        if record.chunk.id != *chunk_id
            || record.device_id != self.device_id
            || record.device_epoch != self.device_epoch
            || record.stored_length != record.chunk.length
            || record.stored_checksum != record.chunk.content_digest
            || record.encoding != PhysicalEncoding::Raw
        {
            return Err(invalid(
                "durable local Chunk record is internally inconsistent",
            ));
        }
        self.path_for_record(&record)?;
        if let Some(expected) = ChunkObject::from_content_addressed_id(chunk_id)
            && expected != record.chunk
        {
            return Err(corrupt(
                chunk_id,
                "catalog differs from content-addressed identity",
            ));
        }
        Ok(record)
    }

    #[cfg(feature = "dfs")]
    pub(crate) fn peer_crc32c_table(
        &self,
        chunk_id: &ChunkId,
    ) -> Result<Option<super::super::chunk_crc32c::CrcTable>> {
        let record = self.peer_catalog_record(chunk_id)?;
        if record.chunk.is_segment_crc32c() {
            let table = record
                .segment_crc
                .ok_or_else(|| corrupt(chunk_id, "CRC descriptor missing"))?;
            if !table.valid_shape(record.chunk.length)
                || table.descriptor_digest() != record.chunk.content_digest
            {
                return Err(corrupt(chunk_id, "CRC descriptor identity differs"));
            }
            Ok(Some(table))
        } else {
            Ok(None)
        }
    }

    /// Enter streaming only before any header is sent. Missing/legacy proofs
    /// or unavailable optional working capacity preserve the full-scan path.
    #[cfg(all(feature = "dfs", test))]
    pub(crate) fn open_peer_group_stream(
        &self,
        chunk_id: &ChunkId,
        offset: u64,
        length: usize,
    ) -> Result<Option<PeerGroupReader<'_>>> {
        self.open_peer_group_stream_until(chunk_id, offset, length, self.peer_serve_deadline())
    }

    #[cfg(feature = "dfs")]
    pub(crate) fn open_peer_group_stream_until(
        &self,
        chunk_id: &ChunkId,
        offset: u64,
        length: usize,
        deadline: Instant,
    ) -> Result<Option<PeerGroupReader<'_>>> {
        let guard = self.begin_peer_serve_read()?;
        let record = self.peer_catalog_record(chunk_id)?;
        if offset != 0
            || length as u64 != record.chunk.length
            || record.chunk.length == 0
            || record.chunk.length > PEER_READ_OBJECT_LIMIT as u64
            || ChunkObject::from_content_addressed_id(chunk_id).as_ref() != Some(&record.chunk)
        {
            return Ok(None);
        }
        let reader = self.open_cataloged(chunk_id)?;
        let Some(working) = self.outboard_budget.reserve_range_work(
            &[VerifiedRangeCopy {
                chunk_offset: 0,
                length: length.min(outboard::OUTBOARD_GROUP_BYTES),
                output_offset: 0,
            }],
            record.chunk.length,
        ) else {
            return Ok(None);
        };
        let proof = match reader.load_outboard() {
            Ok(Some(proof)) => proof,
            Ok(None) => return Ok(None),
            Err(error) if error.code() == afs_error::IO_CAPACITY => return Ok(None),
            Err(error) => return Err(error),
        };
        drop(working);
        let PhysicalLocation::PerChunkFile { relative_path } = &record.location;
        let control_bytes = std::mem::size_of::<PeerGroupReader<'_>>()
            + std::mem::size_of::<PeerStreamPool>()
            + 2 * std::mem::size_of::<PeerStreamFrame>()
            + 256
            + record.chunk.id.0.capacity()
            + record.device_id.capacity()
            + relative_path.capacity()
            + reader.chunk.id.0.capacity()
            + reader.outboard_path.capacity();
        let control = match self.reserve_peer_control_until(control_bytes, deadline) {
            Ok(control) => control,
            Err(error) if error.code() == afs_error::IO_CAPACITY => return Ok(None),
            Err(error) => return Err(error),
        };
        let allocate = || -> Result<PeerStreamBlock> {
            let mut reservation =
                self.reserve_peer_read_payload_until(outboard::OUTBOARD_GROUP_BYTES, deadline)?;
            let mut bytes = Vec::new();
            bytes
                .try_reserve_exact(outboard::OUTBOARD_GROUP_BYTES)
                .map_err(|_| peer_payload_capacity())?;
            reservation.retain_capacity(bytes.capacity())?;
            Ok(PeerStreamBlock {
                bytes,
                _reservation: reservation,
            })
        };
        let blocks = match (|| Ok::<_, Error>([Some(allocate()?), Some(allocate()?)]))() {
            Ok(blocks) => blocks,
            Err(error) if error.code() == afs_error::IO_CAPACITY => return Ok(None),
            Err(error) => return Err(error),
        };
        let identity = PeerFileIdentity::read(&reader.file)?;
        if identity.length != record.chunk.length {
            return Err(corrupt(chunk_id, "length mismatch"));
        }
        let stream = PeerGroupReader {
            store: self,
            reader,
            record,
            identity,
            proof,
            pool: Arc::new(PeerStreamPool {
                free: Mutex::new(blocks),
                changed: Condvar::new(),
                _control: control,
            }),
            offset: 0,
            _reader: guard,
        };
        stream.check_identity(true)?;
        Ok(Some(stream))
    }

    /// Object singleflight shares only immutable content, never read authority.
    #[cfg(test)]
    pub(crate) fn read_peer_range(
        &self,
        chunk_id: &ChunkId,
        offset: u64,
        length: usize,
    ) -> Result<PeerReadPayload> {
        self.read_peer_range_until(
            chunk_id,
            offset,
            length,
            self.peer_serve_deadline(),
            self.peer_read_crc_mode,
        )
    }

    /// `crc_mode` is the requesting peer's policy. It only decides sender-side
    /// body verification of CRC-format objects, whose reads stay
    /// request-private and are never shared through the object flight.
    #[cfg(any(feature = "dfs", test))]
    pub(crate) fn read_peer_range_until(
        &self,
        chunk_id: &ChunkId,
        offset: u64,
        length: usize,
        deadline: Instant,
        crc_mode: crate::config::DfsReadCrcMode,
    ) -> Result<PeerReadPayload> {
        let _reader = self.begin_peer_serve_read()?;
        // Legacy long identities remain readable, but are not retained as keys.
        let cacheable = chunk_id.0.len() <= PEER_SERVE_ID_LIMIT;
        if !cacheable {
            let payload = self.read_peer_range_private(
                chunk_id, offset, length, &mut None, false, deadline, crc_mode,
            )?;
            self.ensure_peer_serve_open()?;
            return Ok(payload);
        }
        let (flight, leading) = self.join_peer_serve_flight(chunk_id, deadline)?;
        if !leading {
            return self.serve_peer_follower(&flight, chunk_id, offset, length, deadline, crc_mode);
        }
        self.serve_peer_leader(flight, chunk_id, offset, length, deadline, crc_mode)
    }

    /// Join the object's flight, or install a new one as its leader (true),
    /// waiting for a free fixed slot until `deadline`.
    #[cfg(any(feature = "dfs", test))]
    fn join_peer_serve_flight(
        &self,
        chunk_id: &ChunkId,
        deadline: Instant,
    ) -> Result<(Arc<PeerServeFlight>, bool)> {
        let existing = {
            let state = self
                .peer_read_cache
                .lock()
                .map_err(|_| peer_serve_unavailable("Peer serve state poisoned"))?;
            state
                .flights
                .iter()
                .flatten()
                .find(|flight| flight.chunk_id == *chunk_id)
                .cloned()
        };
        if let Some(flight) = existing {
            return Ok((flight, false));
        }
        // Fixed slots bound table storage. Charge keys/terminal error and Arc
        // allocation before retention; no proof-ledger or body-budget loan.
        let control = self.reserve_peer_control_until(
            std::mem::size_of::<PeerServeFlight>() + PEER_SERVE_ID_LIMIT + 512 + 128,
            deadline,
        )?;
        let id = chunk_id.clone();
        if id.0.capacity() > PEER_SERVE_ID_LIMIT {
            return Err(peer_payload_capacity());
        }
        let candidate = Arc::new(PeerServeFlight {
            chunk_id: id,
            result: Mutex::new(None),
            changed: Condvar::new(),
            _control: control,
        });
        let mut state = self
            .peer_read_cache
            .lock()
            .map_err(|_| peer_serve_unavailable("Peer serve state poisoned"))?;
        loop {
            if state.closed {
                return Err(peer_serve_unavailable("Peer serve is stopped"));
            }
            if let Some(flight) = state
                .flights
                .iter()
                .flatten()
                .find(|flight| flight.chunk_id == *chunk_id)
                .cloned()
            {
                drop(state);
                drop(candidate);
                return Ok((flight, false));
            }
            if let Some(free) = state.flights.iter_mut().find(|flight| flight.is_none()) {
                *free = Some(candidate.clone());
                self.observe_peer_serve(&state);
                return Ok((candidate, true));
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(peer_payload_capacity());
            }
            let _wait = self.coarse_diagnostic("peer_slot_wait", 0);
            state = self
                .peer_serve_changed
                .wait_timeout(state, remaining)
                .map_err(|_| peer_serve_unavailable("Peer serve admission wait poisoned"))?
                .0;
        }
    }

    /// Wait for the leader's result, then serve this range from it.
    #[cfg(any(feature = "dfs", test))]
    fn serve_peer_follower(
        &self,
        flight: &PeerServeFlight,
        chunk_id: &ChunkId,
        offset: u64,
        length: usize,
        deadline: Instant,
        crc_mode: crate::config::DfsReadCrcMode,
    ) -> Result<PeerReadPayload> {
        let waited = self.coarse_diagnostic("peer_slot_wait", 0);
        let ready = loop {
            self.ensure_peer_serve_open()?;
            let outcome = flight
                .result
                .lock()
                .map_err(|_| peer_serve_unavailable("Peer object flight poisoned"))?;
            if let Some(outcome) = outcome.as_ref() {
                break outcome.clone()?;
            }
            drop(
                flight
                    .changed
                    .wait_timeout(outcome, Duration::from_millis(25))
                    .map_err(|_| peer_serve_unavailable("Peer object flight wait poisoned"))?,
            );
        };
        drop(waited);
        // A full immutable object can serve all ranges after the existing
        // catalog/file checks. Exact partial results remain request-private.
        #[cfg(test)]
        {
            let block = self.peer_ready_test_block.lock().unwrap().take();
            if let Some((entered, release)) = block {
                entered.send(()).unwrap();
                release.recv_timeout(Duration::from_secs(5)).unwrap();
            }
        }
        let mut slot = ready;
        let payload = self.read_peer_range_private(
            chunk_id, offset, length, &mut slot, true, deadline, crc_mode,
        )?;
        self.ensure_peer_serve_open()?;
        Ok(payload)
    }

    /// Read as the flight leader, then install a complete current object as
    /// resident and finish the flight for its followers.
    #[cfg(any(feature = "dfs", test))]
    fn serve_peer_leader(
        &self,
        flight: Arc<PeerServeFlight>,
        chunk_id: &ChunkId,
        offset: u64,
        length: usize,
        deadline: Instant,
        crc_mode: crate::config::DfsReadCrcMode,
    ) -> Result<PeerReadPayload> {
        let mut leader = PeerServeLeader {
            store: self,
            flight,
            finished: false,
        };
        let mut slot = {
            let state = self
                .peer_read_cache
                .lock()
                .map_err(|_| peer_serve_unavailable("Peer serve state poisoned"))?;
            state.resident(chunk_id)
        };
        let result = self.read_peer_range_private(
            chunk_id, offset, length, &mut slot, true, deadline, crc_mode,
        );
        let ready = result.as_ref().ok().and_then(|payload| {
            payload
                .object
                .identity
                .as_ref()
                .filter(|(record, _)| {
                    payload.object.bytes.len() as u64 == record.chunk.length
                        && record.chunk.length <= PEER_READ_OBJECT_LIMIT as u64
                })
                .map(|_| payload.object.clone())
        });
        if let Some(object) = ready.as_ref() {
            // Catalog→table is the only nested metadata-lock order. File I/O and
            // hash are already complete; stale/closed flights cannot install.
            let catalog = self
                .catalog
                .lock()
                .map_err(|_| invalid("local chunk catalog lock is poisoned"))?;
            let current = object
                .identity
                .as_ref()
                .is_some_and(|(record, _)| catalog.records.get(chunk_id) == Some(record));
            let retired = {
                let mut state = self
                    .peer_read_cache
                    .lock()
                    .map_err(|_| peer_serve_unavailable("Peer serve state poisoned"))?;
                if state.closed || !current {
                    drop(state);
                    drop(catalog);
                    leader.finish(Err(peer_serve_unavailable(
                        "Peer object identity or serve admission changed before publication",
                    )));
                    return Err(peer_serve_unavailable(
                        "Peer object identity or serve admission changed before publication",
                    ));
                }
                let old = state.remove_resident(chunk_id);
                let victim = state.install(object.clone());
                self.observe_peer_serve(&state);
                (old, victim)
            };
            drop(catalog);
            drop(retired);
        } else if result.is_ok() {
            self.ensure_peer_serve_open()?;
        }
        let shared = match &result {
            Ok(_) => Ok(ready),
            Err(error) => {
                let mut end = error.message().len().min(512);
                while !error.message().is_char_boundary(end) {
                    end -= 1;
                }
                Err(Error::new(
                    error.code(),
                    error.kind(),
                    &error.message()[..end],
                ))
            }
        };
        leader.finish(shared);
        result
    }

    /// The private fill works without the shared table lock. Optional proof
    /// repair keeps its existing shared gates, outside that lock as well.
    #[cfg(any(feature = "dfs", test))]
    #[allow(clippy::too_many_arguments)]
    fn read_peer_range_private(
        &self,
        chunk_id: &ChunkId,
        offset: u64,
        length: usize,
        slot: &mut Option<Arc<PeerVerifiedObject>>,
        cacheable: bool,
        deadline: Instant,
        crc_mode: crate::config::DfsReadCrcMode,
    ) -> Result<PeerReadPayload> {
        // Includes catalog/file identity work and actual read/verification;
        // excludes slot acquisition and the optional sidecar repair below.
        let read_verify = self.coarse_diagnostic("peer_read_verify", length as u64);
        let object_diagnostic = self.read_work.as_ref().and_then(|metrics| {
            metrics.object_diagnostic(
                super::super::read_work::ReadObjectDiagnosticStage::PeerReadVerify,
                &chunk_id.0,
            )
        });
        let mut repair_proof = None;
        self.record_read_check(super::super::read_work::ReadCheck::CatalogFence);
        let request = PeerRangeRequest {
            chunk_id,
            offset,
            length,
            cacheable,
            deadline,
            crc_mode,
        };
        let result = self.read_peer_range_verified(&request, slot, &mut repair_proof);
        if result.is_err()
            && slot.as_ref().is_some_and(|object| {
                object
                    .identity
                    .as_ref()
                    .is_some_and(|(record, _)| record.chunk.id == *chunk_id)
            })
        {
            self.retire_observed_peer_resident(slot.as_ref());
            slot.take();
        }
        if let Some(diagnostic) = object_diagnostic {
            diagnostic.finish(result.is_ok());
        }
        drop(read_verify);
        // Optional proof I/O never holds the serve table; reads and the W-0
        // installer share bounded repair gates and a read-only health comparison.
        if result.is_ok()
            && let Some(proof) = repair_proof.as_ref()
        {
            repair_sidecar_on_read(
                &self.outboards.join(format!("{}.bao", safe_id(&chunk_id.0))),
                proof,
                &self.outboard_repairs,
            );
        }
        if let Err(error) = &result
            && verified_copy_is_invalid(error)
        {
            self.lifecycle()
                .quarantine(chunk_id, QuarantineTrigger::PeerRangeServe);
        }
        result
    }

    /// Catalog/file identity, then the first applicable way to serve the
    /// range: CRC-format range, the resident object, an exact verified range,
    /// or a complete-root fill.
    #[cfg(any(feature = "dfs", test))]
    fn read_peer_range_verified(
        &self,
        request: &PeerRangeRequest<'_>,
        slot: &mut Option<Arc<PeerVerifiedObject>>,
        repair_proof: &mut Option<SharedOutboard>,
    ) -> Result<PeerReadPayload> {
        let PeerRangeRequest {
            chunk_id,
            offset,
            length,
            cacheable: _,
            deadline: _,
            crc_mode: _,
        } = *request;
        let record = self.peer_catalog_record(chunk_id)?;
        let file = File::open(self.path_for_record(&record)?).map_err(Error::from)?;
        let before = PeerFileIdentity::read(&file)?;
        if before.length != record.chunk.length {
            return Err(corrupt(chunk_id, "length mismatch"));
        }
        let allowed = usize::try_from(
            record
                .chunk
                .length
                .saturating_sub(offset)
                .min(length as u64),
        )
        .map_err(|_| invalid("Chunk read length is too large"))?;
        let view = PeerObjectView {
            record,
            file,
            before,
            allowed,
        };
        if view.record.chunk.is_segment_crc32c() {
            return self.read_peer_crc_range(request, &view);
        }
        if let Some(payload) = self.read_peer_resident(request, slot, &view)? {
            return Ok(payload);
        }
        // A changed identity for the same formerly verified object must
        // re-establish its complete root before any proof can be reused.
        let invalidated_current = slot.as_ref().is_some_and(|object| {
            object
                .identity
                .as_ref()
                .is_some_and(|(old, _)| old.chunk.id == *chunk_id)
        });
        if invalidated_current {
            self.retire_observed_peer_resident(slot.as_ref());
        }
        if let Some(metrics) = &self.read_work {
            metrics.peer_object_cache("miss");
        }
        #[cfg(test)]
        {
            // Remove the hook under its own mutex, then wait without that
            // mutex: it must not introduce serialization of its own.
            let block = self.peer_fill_test_blocks.lock().unwrap().remove(chunk_id);
            if let Some((entered, release)) = block {
                entered.send(()).unwrap();
                release.recv_timeout(Duration::from_secs(5)).unwrap();
            }
        }
        let mut capacity_fallback = false;
        if !invalidated_current
            && (offset != 0 || view.allowed as u64 != view.record.chunk.length)
            && ChunkObject::from_content_addressed_id(chunk_id).as_ref() == Some(&view.record.chunk)
        {
            match self.read_peer_exact_range(request, slot, &view)? {
                ExactRange::Served(payload) => return Ok(payload),
                ExactRange::Fallback {
                    capacity_fallback: capacity,
                } => capacity_fallback = capacity,
            }
        }
        self.read_peer_full_fill(request, slot, view, capacity_fallback, repair_proof)
    }

    /// CRC-format objects: request-private, never shared through the flight.
    #[cfg(any(feature = "dfs", test))]
    fn read_peer_crc_range(
        &self,
        request: &PeerRangeRequest<'_>,
        view: &PeerObjectView,
    ) -> Result<PeerReadPayload> {
        let PeerRangeRequest {
            chunk_id,
            offset,
            length: _,
            cacheable: _,
            deadline,
            crc_mode,
        } = *request;
        let PeerObjectView {
            record,
            file,
            before,
            allowed,
        } = view;
        let allowed = *allowed;
        let scratch = super::super::chunk_crc32c::SEGMENT;
        let mut reservation =
            self.reserve_peer_read_payload_until(allowed.saturating_add(scratch), deadline)?;
        let mut bytes = self
            .peer_buffer_pool
            .take(allowed, self.read_work.clone())?;
        reservation.retain_capacity(bytes.capacity().saturating_add(scratch))?;
        let mut reader = self.open_cataloged(chunk_id)?;
        let segment = super::super::chunk_crc32c::SEGMENT as u64;
        let complete = offset.is_multiple_of(segment)
            && ((allowed as u64).is_multiple_of(segment)
                || offset + allowed as u64 == record.chunk.length);
        let verify_body = crc_mode == crate::config::DfsReadCrcMode::Both
            || (crc_mode == crate::config::DfsReadCrcMode::Receiver && !complete);
        reader.verify_crc_body = verify_body;
        if reader.read_ranges_at_uncommitted(
            &[VerifiedRangeCopy {
                chunk_offset: offset,
                length: allowed,
                output_offset: 0,
            }],
            &mut bytes,
        )? != vec![allowed]
            || self.peer_catalog_record(chunk_id)? != *record
            || PeerFileIdentity::read(file)? != *before
            || PeerFileIdentity::read(&File::open(self.path_for_record(record)?)?)? != *before
        {
            return Err(corrupt(
                chunk_id,
                "CRC peer range changed during verification",
            ));
        }
        Ok(PeerReadPayload {
            object: Arc::new(PeerVerifiedObject {
                bytes,
                outboard: None,
                _reservation: reservation,
                identity: None,
                _control: self.peer_object_control(None, deadline)?,
            }),
            start: 0,
            length: allowed,
        })
    }

    /// The resident verified object when its identity is still current.
    #[cfg(any(feature = "dfs", test))]
    fn read_peer_resident(
        &self,
        request: &PeerRangeRequest<'_>,
        slot: &Option<Arc<PeerVerifiedObject>>,
        view: &PeerObjectView,
    ) -> Result<Option<PeerReadPayload>> {
        let PeerRangeRequest {
            chunk_id,
            offset,
            length: _,
            cacheable: _,
            deadline: _,
            crc_mode: _,
        } = *request;
        let PeerObjectView {
            record,
            file,
            before,
            allowed,
        } = view;
        let allowed = *allowed;
        if let Some(object) = slot.as_ref()
            && object.identity.as_ref() == Some(&(record.clone(), before.clone()))
        {
            if self.peer_catalog_record(chunk_id)? != *record
                || PeerFileIdentity::read(file)? != *before
            {
                return Err(corrupt(chunk_id, "identity changed during cached read"));
            }
            if let Some(metrics) = &self.read_work {
                metrics.peer_object_cache("hit");
            }
            return Ok(Some(PeerReadPayload {
                object: object.clone(),
                start: usize::try_from(offset.min(record.chunk.length))
                    .map_err(|_| invalid("Chunk offset is too large"))?,
                length: allowed,
            }));
        }
        Ok(None)
    }

    /// A partial range of a content-addressed object through its stored
    /// outboard proof, without a complete-root scan.
    #[cfg(any(feature = "dfs", test))]
    fn read_peer_exact_range(
        &self,
        request: &PeerRangeRequest<'_>,
        slot: &mut Option<Arc<PeerVerifiedObject>>,
        view: &PeerObjectView,
    ) -> Result<ExactRange> {
        let PeerRangeRequest {
            chunk_id,
            offset,
            length: _,
            cacheable: _,
            deadline,
            crc_mode: _,
        } = *request;
        let PeerObjectView {
            record,
            file,
            before,
            allowed,
        } = view;
        let allowed = *allowed;
        let reader = self.open_cataloged(chunk_id)?;
        let loaded = self
            .outboard_budget
            .reserve_range_work(
                &[VerifiedRangeCopy {
                    chunk_offset: offset,
                    length: allowed,
                    output_offset: 0,
                }],
                record.chunk.length,
            )
            .ok_or_else(outboard_capacity)
            .and_then(|working| reader.load_outboard().map(|proof| (proof, working)));
        match loaded {
            Ok((Some(proof), working)) => {
                let mut reservation = match self.reserve_peer_read_payload_until(allowed, deadline)
                {
                    Ok(reservation) => reservation,
                    Err(error) => {
                        // An unrelated slot is retained for ordinary range
                        // reads, but may be reclaimed when admission needs
                        // its genuinely unpinned body capacity.
                        if slot
                            .as_ref()
                            .is_some_and(|object| Arc::strong_count(object) == 1)
                        {
                            slot.take();
                        }
                        self.reserve_peer_read_payload_until(allowed, deadline)
                            .map_err(|_| error)?
                    }
                };
                let mut bytes = Vec::new();
                bytes
                    .try_reserve_exact(allowed)
                    .map_err(|_| peer_payload_capacity())?;
                reservation.retain_capacity(bytes.capacity())?;
                bytes.resize(allowed, 0);
                let mut work = OutboardReadWork::default();
                let verified = proof.read_ranges_into_with_reserved_work(
                    file,
                    0,
                    &record.chunk,
                    &[VerifiedRangeCopy {
                        chunk_offset: offset,
                        length: allowed,
                        output_offset: 0,
                    }],
                    &mut bytes,
                    OutboardReadContext {
                        work: &mut work,
                        working,
                    },
                );
                #[cfg(test)]
                VERIFIED_SCAN_BYTES.with(|count| count.set(count.get() + work.hash_bytes));
                if let Some(metrics) = &self.read_work {
                    metrics.hash("outboard_body_hash", work.hash_bytes);
                    metrics.hash("outboard_parent_hash", work.parent_hash_bytes);
                    metrics.copy("outboard_range_delivery", work.copied_bytes);
                }
                if verified.is_ok()
                    && PeerFileIdentity::read(file)? == *before
                    && self.peer_catalog_record(chunk_id)? == *record
                {
                    return Ok(ExactRange::Served(PeerReadPayload {
                        object: Arc::new(PeerVerifiedObject {
                            bytes: bytes.into(),
                            outboard: None,
                            _reservation: reservation,
                            identity: None,
                            _control: self.peer_object_control(None, deadline)?,
                        }),
                        start: 0,
                        length: allowed,
                    }));
                }
                let capacity_fallback = verified
                    .as_ref()
                    .is_err_and(|error| error.code() == afs_error::IO_CAPACITY);
                // Drop the exact-range private allocation before the old
                // full-root fallback attempts its original body admission.
                drop(bytes);
                drop(reservation);
                Ok(ExactRange::Fallback { capacity_fallback })
            }
            Err(error) if error.code() == afs_error::IO_CAPACITY => Ok(ExactRange::Fallback {
                capacity_fallback: true,
            }),
            _ => Ok(ExactRange::Fallback {
                capacity_fallback: false,
            }),
        }
    }

    /// Complete-root verified fill; a cacheable small object becomes the
    /// resident slot and may seed a proof for sidecar repair.
    #[cfg(any(feature = "dfs", test))]
    fn read_peer_full_fill(
        &self,
        request: &PeerRangeRequest<'_>,
        slot: &mut Option<Arc<PeerVerifiedObject>>,
        view: PeerObjectView,
        capacity_fallback: bool,
        repair_proof: &mut Option<SharedOutboard>,
    ) -> Result<PeerReadPayload> {
        let PeerRangeRequest {
            chunk_id,
            offset,
            length: _,
            cacheable,
            deadline,
            crc_mode: _,
        } = *request;
        let PeerObjectView {
            record,
            file,
            before,
            allowed,
        } = view;
        // Only a full fill needs this slot's body capacity. A successful
        // partial range above must not evict an unrelated healthy object.
        // Externally held old allocations remain charged until last drop.
        slot.take();
        let resident = cacheable && record.chunk.length <= PEER_READ_OBJECT_LIMIT as u64;
        let allocation = if resident {
            usize::try_from(record.chunk.length).map_err(|_| peer_payload_capacity())?
        } else {
            allowed
        };
        let mut reservation = self.reserve_peer_read_payload_until(allocation, deadline)?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(allocation)
            .map_err(|_| peer_payload_capacity())?;
        reservation.retain_capacity(bytes.capacity())?;
        bytes.resize(allocation, 0);
        let builder = (!capacity_fallback
            && ChunkObject::from_content_addressed_id(chunk_id).as_ref() == Some(&record.chunk))
        .then(|| OutboardBuilder::new(record.chunk.length, self.outboard_budget.clone()));
        if capacity_fallback {
            self.outboard_budget.record_range_fullscan_fallback();
        }
        let (_, outboard) = verified_ranges_into_building(
            &file,
            &record.chunk,
            &[VerifiedRangeCopy {
                chunk_offset: if resident { 0 } else { offset },
                length: allocation,
                output_offset: 0,
            }],
            &mut bytes,
            true,
            self.read_work.as_ref(),
            builder,
        )?;
        if PeerFileIdentity::read(&file)? != before || self.peer_catalog_record(chunk_id)? != record
        {
            return Err(corrupt(chunk_id, "identity changed during verification"));
        }
        // Reopen the path as well: a pinned old descriptor must not seed a
        // cache after another inode has replaced its cataloged pathname.
        let current = File::open(self.path_for_record(&record)?).map_err(Error::from)?;
        if PeerFileIdentity::read(&current)? != before {
            return Err(corrupt(chunk_id, "path changed during verification"));
        }
        // Residency is a subset of the same proof lease, not an extra
        // allocation/ledger. Refusal keeps the complete-root body result.
        *repair_proof = outboard.as_ref().map(Arc::clone);
        let outboard = outboard.filter(|proof| proof.try_mark_resident());
        let control = self.peer_object_control(resident.then_some(&record), deadline)?;
        let object = Arc::new(PeerVerifiedObject {
            bytes: bytes.into(),
            outboard,
            _reservation: reservation,
            identity: resident.then_some((record, before)),
            _control: control,
        });
        if resident {
            *slot = Some(object.clone());
        }
        Ok(PeerReadPayload {
            object,
            start: if resident {
                usize::try_from(offset.min(allocation as u64))
                    .map_err(|_| invalid("Chunk offset is too large"))?
            } else {
                0
            },
            length: allowed,
        })
    }
}

/// One peer range request as received by `read_peer_range_private`.
#[cfg(any(feature = "dfs", test))]
#[derive(Clone, Copy)]
struct PeerRangeRequest<'r> {
    chunk_id: &'r ChunkId,
    offset: u64,
    length: usize,
    cacheable: bool,
    deadline: Instant,
    crc_mode: crate::config::DfsReadCrcMode,
}

/// The cataloged record and the opened file whose identity every way of
/// serving the range rechecks.
#[cfg(any(feature = "dfs", test))]
struct PeerObjectView {
    record: LocalChunkRecord,
    file: File,
    before: PeerFileIdentity,
    allowed: usize,
}

#[cfg(any(feature = "dfs", test))]
enum ExactRange {
    Served(PeerReadPayload),
    Fallback { capacity_fallback: bool },
}
