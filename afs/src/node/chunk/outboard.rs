//! Bounded standard BLAKE3/Bao metadata. Content proofs never authorize reads.

use super::VerifiedRangeCopy;
use crate::dfs::{ChunkObject, ContentDigest, DigestAlgorithm};
use afs_error::{Error, Result};
use bao_tree::{
    BaoTree, BlockSize, ChunkNum, ChunkRanges, TreeNode,
    io::{
        BaoContentItem,
        outboard::PreOrderMemOutboard,
        sync::{DecodeResponseIter, Outboard, OutboardMut, ReadAt, Size},
    },
    iter::{BaoChunk, PostOrderChunkIter},
};
use blake3::hazmat::{HasherExt, Mode, merge_subtrees_non_root, merge_subtrees_root};
use std::{
    cell::RefCell,
    fs::File,
    io,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

pub(crate) const DEFAULT_OUTBOARD_BUDGET_BYTES: usize = 1024 * 1024;
pub(crate) const OUTBOARD_GROUP_BYTES: usize = 32 * 1024;
pub(crate) const MAX_ENCODED_OUTBOARD_BYTES: usize = 8192;

const RANGE_WORKING_BYTES: usize = 2 * OUTBOARD_GROUP_BYTES + MAX_ENCODED_OUTBOARD_BYTES + 4096;
const ALIGNED_RANGE_WORKING_BYTES: usize = OUTBOARD_GROUP_BYTES + 4096;
const DEFAULT_RANGE_WORK_WAIT: Duration = Duration::from_millis(2);

#[derive(Debug, Default)]
struct RangeWorkState {
    stopped: bool,
    #[cfg(test)]
    waiters: usize,
    #[cfg(test)]
    before_wait: Option<(
        std::sync::mpsc::SyncSender<()>,
        std::sync::mpsc::Receiver<()>,
    )>,
}

#[derive(Debug)]
pub struct OutboardBudget {
    limit: usize,
    used: AtomicUsize,
    resident_limit: usize,
    resident_used: AtomicUsize,
    metrics: Option<super::super::read_work::ReadWorkMetrics>,
    work_wait: Duration,
    work_state: Mutex<RangeWorkState>,
    work_changed: Condvar,
}
impl OutboardBudget {
    pub fn new(limit: usize) -> Arc<Self> {
        Self::create(limit, None, DEFAULT_RANGE_WORK_WAIT)
    }
    #[cfg(test)]
    pub(crate) fn new_with_metrics(
        limit: usize,
        metrics: super::super::read_work::ReadWorkMetrics,
    ) -> Arc<Self> {
        Self::create(limit, Some(metrics), DEFAULT_RANGE_WORK_WAIT)
    }
    #[cfg(feature = "dfs")]
    pub(crate) fn new_with_metrics_and_work_wait(
        limit: usize,
        metrics: super::super::read_work::ReadWorkMetrics,
        work_wait: Duration,
    ) -> Arc<Self> {
        Self::create(limit, Some(metrics), work_wait)
    }
    fn create(
        limit: usize,
        metrics: Option<super::super::read_work::ReadWorkMetrics>,
        work_wait: Duration,
    ) -> Arc<Self> {
        if let Some(metrics) = &metrics {
            metrics.outboard_limits(limit, limit / 2);
        }
        Arc::new(Self {
            limit,
            used: AtomicUsize::new(0),
            resident_limit: limit / 2,
            resident_used: AtomicUsize::new(0),
            metrics,
            work_wait,
            work_state: Mutex::new(RangeWorkState::default()),
            work_changed: Condvar::new(),
        })
    }
    pub fn limit(&self) -> usize {
        self.limit
    }
    pub fn used(&self) -> usize {
        self.used.load(Ordering::Acquire)
    }
    pub fn resident_limit(&self) -> usize {
        self.resident_limit
    }
    pub fn resident_used(&self) -> usize {
        self.resident_used.load(Ordering::Acquire)
    }
    fn event(&self, event: &'static str) {
        if let Some(metrics) = &self.metrics {
            metrics.outboard_event(event);
        }
    }
    pub(crate) fn record_write_without_proof(&self) {
        self.event("write_without_proof");
    }
    pub(crate) fn record_range_fullscan_fallback(&self) {
        self.event("range_fullscan_fallback");
    }
    #[cfg(any(feature = "dfs", test))]
    pub(crate) fn record_range_fullscan_unretained(&self) {
        self.event("range_fullscan_unretained");
    }
    pub(crate) fn try_reserve(self: &Arc<Self>, bytes: usize) -> Option<OutboardLease> {
        let mut used = self.used();
        loop {
            let Some(next) = used.checked_add(bytes).filter(|next| *next <= self.limit) else {
                self.event("reserve_total_failed");
                return None;
            };
            match self
                .used
                .compare_exchange_weak(used, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => {
                    if let Some(metrics) = &self.metrics {
                        metrics.outboard_adjust(false, bytes, true);
                    }
                    return Some(OutboardLease {
                        budget: self.clone(),
                        bytes,
                        resident: Mutex::new(OutboardResidence::default()),
                    });
                }
                Err(actual) => used = actual,
            }
        }
    }
    pub(crate) fn try_reserve_resident(self: &Arc<Self>, bytes: usize) -> Option<OutboardLease> {
        let lease = self.try_reserve(bytes)?;
        lease.try_mark_resident().then_some(lease)
    }
    #[cfg(test)]
    fn try_reserve_range_work(self: &Arc<Self>) -> Option<OutboardLease> {
        self.reserve_work_until_deadline(RANGE_WORKING_BYTES)
    }
    pub(crate) fn reserve_range_work(
        self: &Arc<Self>,
        ranges: &[VerifiedRangeCopy],
        object_length: u64,
    ) -> Option<OutboardLease> {
        self.reserve_work_until_deadline(range_working_bytes(ranges, object_length))
    }
    pub(crate) fn pin_sidecar(self: &Arc<Self>, proof: &SharedOutboard) -> Option<SidecarOutboard> {
        if !proof.uses_budget(self) {
            return None;
        }
        let mut residence = proof.lease.resident.lock().ok()?;
        if residence.resident || self.limit.saturating_sub(self.used()) < 4 * RANGE_WORKING_BYTES {
            return None;
        }
        residence.sidecar_pins = residence.sidecar_pins.checked_add(1)?;
        // No new allocation or ledger charge: the original proof remains
        // transient until every sidecar pin is gone. The headroom check above
        // is instantaneous, not a permanent partition from builder admission.
        Some(SidecarOutboard {
            proof: proof.clone(),
        })
    }
    pub(crate) fn try_reserve_sidecar_control(
        self: &Arc<Self>,
        bytes: usize,
    ) -> Option<OutboardLease> {
        let lease = self.try_reserve(bytes)?;
        if self.limit.saturating_sub(self.used()) < 4 * RANGE_WORKING_BYTES {
            drop(lease);
            return None;
        }
        Some(lease)
    }
    pub(crate) fn sidecar_event(&self, event: &'static str) {
        self.event(event);
    }
    fn reserve_work_until_deadline(self: &Arc<Self>, bytes: usize) -> Option<OutboardLease> {
        // Start before taking the coordination lock: contention and spurious
        // wakeups cannot silently give a request a fresh waiting interval.
        let deadline = Instant::now().checked_add(self.work_wait)?;
        let mut state = self.work_state.lock().ok()?;
        if state.stopped || (!self.work_wait.is_zero() && Instant::now() >= deadline) {
            return None;
        }
        if let Some(lease) = self.try_reserve(bytes) {
            return Some(lease);
        }
        loop {
            #[cfg(test)]
            if let Some((checked, resume)) = state.before_wait.take() {
                checked.send(()).ok()?;
                resume.recv().ok()?;
            }
            let remaining = deadline.checked_duration_since(Instant::now())?;
            #[cfg(test)]
            {
                state.waiters += 1;
            }
            // release/stop take this same mutex. They cannot notify between
            // our capacity check and the atomic unlock-and-wait operation.
            let (next, _) = self.work_changed.wait_timeout(state, remaining).ok()?;
            state = next;
            #[cfg(test)]
            {
                state.waiters -= 1;
            }
            if state.stopped || Instant::now() >= deadline {
                return None;
            }
            if let Some(lease) = self.try_reserve(bytes) {
                return Some(lease);
            }
        }
    }
    /// Stop only range-work admission; existing holders retain their charges
    /// until destruction and callers retain their original full-scan fallback.
    #[cfg(any(feature = "dfs", test))]
    pub(crate) fn stop_range_work(&self) {
        let mut state = self
            .work_state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        state.stopped = true;
        self.work_changed.notify_all();
    }
    fn admit_resident(&self, bytes: usize) -> bool {
        let mut used = self.resident_used();
        loop {
            let Some(next) = used
                .checked_add(bytes)
                .filter(|next| *next <= self.resident_limit)
            else {
                self.event("reserve_resident_failed");
                return false;
            };
            match self.resident_used.compare_exchange_weak(
                used,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    if let Some(metrics) = &self.metrics {
                        metrics.outboard_adjust(true, bytes, true);
                    }
                    return true;
                }
                Err(actual) => used = actual,
            }
        }
    }
    fn release(&self, resident: bool, bytes: usize) {
        // Only total bytes unblock range work. The same mutex orders the
        // capacity change against the waiter's check-and-wait boundary.
        let _state = (!resident).then(|| {
            self.work_state
                .lock()
                .unwrap_or_else(|error| error.into_inner())
        });
        let counter = if resident {
            &self.resident_used
        } else {
            &self.used
        };
        counter.fetch_sub(bytes, Ordering::AcqRel);
        if let Some(metrics) = &self.metrics {
            metrics.outboard_adjust(resident, bytes, false);
        }
        if !resident && bytes != 0 {
            self.work_changed.notify_all();
        }
    }
}

fn range_working_bytes(ranges: &[VerifiedRangeCopy], object_length: u64) -> usize {
    if let [range] = ranges {
        let allowed = object_length
            .saturating_sub(range.chunk_offset)
            .min(range.length as u64);
        let stop = range.chunk_offset.saturating_add(allowed);
        if allowed != 0
            && range
                .chunk_offset
                .is_multiple_of(OUTBOARD_GROUP_BYTES as u64)
            && (stop.is_multiple_of(OUTBOARD_GROUP_BYTES as u64) || stop == object_length)
        {
            // BorrowedRange needs one Bao coverage-group scratch and retains
            // the original 4KiB metadata allowance (including result counts).
            // No encoded response or decode leaf lives in this branch.
            return ALIGNED_RANGE_WORKING_BYTES;
        }
    }
    // Partial/multiple ranges own an encoded response and decoded leaf at the
    // same time. They must not borrow the aligned branch's smaller charge.
    RANGE_WORKING_BYTES
}

#[derive(Debug)]
pub(crate) struct OutboardLease {
    budget: Arc<OutboardBudget>,
    bytes: usize,
    // This mutex only serializes the one-way admission of this exact allocation.
    // It never protects I/O. Arc clones share one admission until final drop.
    resident: Mutex<OutboardResidence>,
}
#[derive(Debug, Default)]
struct OutboardResidence {
    resident: bool,
    sidecar_pins: usize,
}
#[derive(Debug)]
pub(crate) struct SidecarOutboard {
    proof: SharedOutboard,
}
impl SidecarOutboard {
    pub(crate) fn proof(&self) -> &SharedOutboard {
        &self.proof
    }
}
impl Drop for SidecarOutboard {
    fn drop(&mut self) {
        let mut residence = self
            .proof
            .lease
            .resident
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        residence.sidecar_pins -= 1;
        // Dropping this guard only releases its own Arc. Other staged/Peer or
        // unknown-commit holders keep the original allocation and charge live.
    }
}
impl OutboardLease {
    fn try_mark_resident(&self) -> bool {
        let Ok(mut retained) = self.resident.lock() else {
            return false;
        };
        if retained.resident {
            return true;
        }
        if retained.sidecar_pins != 0 {
            return false;
        }
        if !self.budget.admit_resident(self.bytes) {
            return false;
        }
        retained.resident = true;
        true
    }
    pub(crate) fn retain_capacity(&mut self, bytes: usize) -> Result<()> {
        let retained = self.resident.get_mut().map_err(|_| capacity())?.resident;
        if bytes > self.bytes {
            let mut extra = if retained {
                self.budget.try_reserve_resident(bytes - self.bytes)
            } else {
                self.budget.try_reserve(bytes - self.bytes)
            }
            .ok_or_else(capacity)?;
            // Transfer both total and (when applicable) resident accounting.
            extra.bytes = 0;
        } else {
            if retained {
                self.budget.release(true, self.bytes - bytes);
            }
            self.budget.release(false, self.bytes - bytes);
        }
        self.bytes = bytes;
        Ok(())
    }
}
impl Drop for OutboardLease {
    fn drop(&mut self) {
        // Poisoning cannot erase a successfully admitted resident reservation.
        let retained = match self.resident.get_mut() {
            Ok(value) => value.resident,
            Err(error) => error.get_ref().resident,
        };
        if retained {
            self.budget.release(true, self.bytes);
        }
        self.budget.release(false, self.bytes);
        drop_event("lease_released", self.budget.used());
    }
}

pub(crate) type SharedOutboard = Arc<ChunkOutboard>;
const HEADER: usize = 64;
const MAGIC: &[u8; 8] = b"AFSBAO1\0";
const MAX_OBJECT_BYTES: u64 = 4 * 1024 * 1024;
const GROUP_LOG: u8 = 5;
fn tree(length: u64) -> BaoTree {
    BaoTree::new(length, BlockSize::from_chunk_log(GROUP_LOG))
}

#[derive(Debug)]
pub(crate) struct ChunkOutboard {
    length: u64,
    digest: ContentDigest,
    encoded: Vec<u8>,
    lease: OutboardLease,
}
impl Drop for ChunkOutboard {
    fn drop(&mut self) {
        // Explicitly retire the backing allocation before the field-order
        // teardown releases its metadata lease.
        drop(std::mem::take(&mut self.encoded));
        drop_event("encoded_freed", self.lease.budget.used());
    }
}

struct ChargedEncoded {
    encoded: Vec<u8>,
    lease: OutboardLease,
}
impl Drop for ChargedEncoded {
    fn drop(&mut self) {
        drop(std::mem::take(&mut self.encoded));
        drop_event("encoded_freed", self.lease.budget.used());
    }
}

impl ChunkOutboard {
    #[cfg(feature = "dfs")]
    pub(crate) fn object_length(&self) -> u64 {
        self.length
    }
    #[cfg(feature = "dfs")]
    pub(crate) fn digest(&self) -> &ContentDigest {
        &self.digest
    }
    pub(crate) fn encoded(&self) -> &[u8] {
        &self.encoded
    }
    pub(crate) fn raw_cv_bytes(&self) -> &[u8] {
        &self.encoded[HEADER..]
    }
    pub(crate) fn try_mark_resident(&self) -> bool {
        self.lease.try_mark_resident()
    }
    pub(crate) fn uses_budget(&self, budget: &Arc<OutboardBudget>) -> bool {
        Arc::ptr_eq(&self.lease.budget, budget)
    }
    #[cfg(any(feature = "dfs", test))]
    pub(crate) fn charged_bytes(&self) -> usize {
        self.lease.bytes
    }
    pub(crate) fn resident_charge(encoded_capacity: usize) -> Result<usize> {
        encoded_capacity
            .checked_add(std::mem::size_of::<Self>())
            .and_then(|bytes| bytes.checked_add(2 * std::mem::size_of::<usize>()))
            .ok_or_else(capacity)
    }
    /// Structural/root binding only. The body and every selected parent path
    /// remain untrusted until the official Bao range validator checks them.
    pub(crate) fn validate_encoded_shape(expected: &ChunkObject, encoded: &[u8]) -> Result<()> {
        let bad = || super::corrupt(&expected.id, "outboard format/root binding mismatch");
        if expected.length > MAX_OBJECT_BYTES
            || encoded.len() < HEADER
            || encoded.len() > MAX_ENCODED_OUTBOARD_BYTES
            || &encoded[..8] != MAGIC
            || encoded[8..10] != 1_u16.to_le_bytes()
            || encoded[10] != GROUP_LOG
            || encoded[11..16] != [0; 5]
            || encoded[60..64] != [0; 4]
            || encoded[16..24] != expected.length.to_le_bytes()
            || encoded[24..56] != expected.content_digest.bytes
        {
            return Err(bad());
        }
        let raw = u32::from_le_bytes(encoded[56..60].try_into().map_err(|_| bad())?) as usize;
        if raw as u64 != tree(expected.length).outboard_size() || encoded.len() != HEADER + raw {
            return Err(bad());
        }
        Ok(())
    }
    pub(crate) fn from_encoded(
        expected: &ChunkObject,
        encoded: Vec<u8>,
        lease: OutboardLease,
    ) -> Result<SharedOutboard> {
        let mut owned = ChargedEncoded { encoded, lease };
        Self::validate_encoded_shape(expected, &owned.encoded)?;
        owned
            .lease
            .retain_capacity(Self::resident_charge(owned.encoded.capacity())?)?;
        let encoded = std::mem::take(&mut owned.encoded);
        let empty = OutboardLease {
            budget: owned.lease.budget.clone(),
            bytes: 0,
            resident: Mutex::new(OutboardResidence::default()),
        };
        let lease = std::mem::replace(&mut owned.lease, empty);
        Ok(Arc::new(Self {
            length: expected.length,
            digest: expected.content_digest.clone(),
            encoded,
            lease,
        }))
    }
    #[cfg(test)]
    pub(crate) fn read_ranges_into(
        &self,
        file: &File,
        body_start: u64,
        expected: &ChunkObject,
        ranges: &[VerifiedRangeCopy],
        out: &mut [u8],
    ) -> Result<Vec<usize>> {
        self.read_ranges_into_with_work(
            file,
            body_start,
            expected,
            ranges,
            out,
            &mut OutboardReadWork::default(),
        )
    }
    #[cfg(any(feature = "dfs", test))]
    pub(crate) fn read_ranges_into_with_work(
        &self,
        file: &File,
        body_start: u64,
        expected: &ChunkObject,
        ranges: &[VerifiedRangeCopy],
        out: &mut [u8],
        work: &mut OutboardReadWork,
    ) -> Result<Vec<usize>> {
        let working = self
            .lease
            .budget
            .reserve_range_work(ranges, expected.length)
            .ok_or_else(capacity)?;
        self.read_ranges_into_with_reserved_work(
            file,
            body_start,
            expected,
            ranges,
            out,
            OutboardReadContext { work, working },
        )
    }
    pub(crate) fn read_ranges_into_with_reserved_work(
        &self,
        file: &File,
        body_start: u64,
        expected: &ChunkObject,
        ranges: &[VerifiedRangeCopy],
        out: &mut [u8],
        context: OutboardReadContext<'_>,
    ) -> Result<Vec<usize>> {
        let OutboardReadContext { work, working } = context;
        Self::validate_encoded_shape(expected, &self.encoded)?;
        if self.length != expected.length || self.digest != expected.content_digest {
            return Err(super::corrupt(&expected.id, "outboard identity differs"));
        }
        let retained = working.resident.lock().map_err(|_| capacity())?.resident;
        if !Arc::ptr_eq(&working.budget, &self.lease.budget)
            || working.bytes < range_working_bytes(ranges, expected.length)
            || retained
        {
            return Err(capacity());
        }
        // Own the original reservation through encoder/decoder destruction. No
        // second reservation after loading/retaining proof metadata.
        let _working = working;
        let end = body_start
            .checked_add(self.length)
            .ok_or_else(|| super::invalid("outboard body offset overflow"))?;
        if file.metadata().map_err(Error::from)?.len() < end {
            return Err(super::corrupt(&expected.id, "outboard body truncated"));
        }
        let mut groups = [false; 128];
        let mut counts = Vec::with_capacity(ranges.len());
        for range in ranges {
            let allowed = self
                .length
                .saturating_sub(range.chunk_offset)
                .min(range.length as u64) as usize;
            let output_end = range
                .output_offset
                .checked_add(allowed)
                .filter(|end| *end <= out.len())
                .ok_or_else(|| super::invalid("outboard output range exceeds buffer"))?;
            let _ = output_end;
            counts.push(allowed);
            if allowed != 0 {
                let first = range.chunk_offset / OUTBOARD_GROUP_BYTES as u64;
                let last = (range.chunk_offset + allowed as u64 - 1) / OUTBOARD_GROUP_BYTES as u64;
                for group in first..=last {
                    groups[group as usize] = true;
                }
            }
        }
        let observed = RefCell::new(OutboardReadWork::default());
        let source = OffsetFile {
            file,
            base: body_start,
            length: self.length,
            work: &observed,
        };
        let root = blake3::Hash::from(self.digest.bytes);
        let bao = PreOrderMemOutboard {
            root,
            tree: tree(self.length),
            data: self.raw_cv_bytes(),
        };
        // Only complete coverage groups can use the borrowed view: the public
        // validator then reads each selected group into its fixed scratch once,
        // without constructing a partial-leaf wire response. The caller owns
        // `out` privately and must not publish it on any error (including a
        // later file-identity or authorization failure).
        if let ([range], [allowed]) = (ranges, counts.as_slice()) {
            let stop = range.chunk_offset.saturating_add(*allowed as u64);
            if *allowed != 0
                && range
                    .chunk_offset
                    .is_multiple_of(OUTBOARD_GROUP_BYTES as u64)
                && (stop.is_multiple_of(OUTBOARD_GROUP_BYTES as u64) || stop == self.length)
            {
                let result = (|| {
                    let bytes = &mut out[range.output_offset..range.output_offset + *allowed];
                    source
                        .read_exact_at(range.chunk_offset, bytes)
                        .map_err(Error::from)?;
                    observed.borrow_mut().scratch_peak = OUTBOARD_GROUP_BYTES;
                    let input = BorrowedRange {
                        bytes,
                        start: range.chunk_offset,
                        work: &observed,
                    };
                    let checked = ObservedOutboard {
                        inner: bao,
                        work: &observed,
                    };
                    let query = ChunkRanges::from(
                        ChunkNum(range.chunk_offset / 1024)..ChunkNum::chunks(stop),
                    );
                    let verify_started = Instant::now();
                    let verified = bao_tree::io::sync::encode_ranges_validated(
                        input,
                        checked,
                        &query,
                        io::sink(),
                    )
                    .map_err(|error| match error {
                        bao_tree::io::EncodeError::Io(error) => Error::from(error),
                        other => bad_proof(&format!("Bao range verification failed: {other}")),
                    });
                    observed.borrow_mut().verify_ns += nanos(verify_started);
                    verified
                })();
                work.add(&observed.into_inner());
                return result.map(|()| counts);
            }
        }
        if let ([range], [allowed]) = (ranges, counts.as_slice())
            && *allowed != 0
        {
            let start =
                range.chunk_offset / OUTBOARD_GROUP_BYTES as u64 * OUTBOARD_GROUP_BYTES as u64;
            let stop = (start + OUTBOARD_GROUP_BYTES as u64).min(self.length);
            if range.chunk_offset + *allowed as u64 <= stop {
                let result =
                    read_single_coverage_group(&source, bao, *range, *allowed, start..stop, out);
                work.add(&observed.into_inner());
                return result.map(|()| counts);
            }
        }
        if ranges.len() <= groups.len()
            && nonaliased_ranges(ranges, &counts)
            && ranges.iter().zip(&counts).any(|(range, allowed)| {
                *allowed != 0
                    && range.chunk_offset.div_ceil(OUTBOARD_GROUP_BYTES as u64)
                        < range.chunk_offset.saturating_add(*allowed as u64)
                            / OUTBOARD_GROUP_BYTES as u64
            })
        {
            // Fill only the requested bytes, once per contiguous range. The
            // validator borrows those exact private results; uncovered edge
            // bytes go straight from the file into its existing group scratch.
            // Tiny edge-only reads and aliasing destinations/sources retain the
            // original codec; the metadata walk is bounded to 128 ranges.
            let result = (|| {
                for (range, allowed) in ranges.iter().zip(&counts) {
                    if *allowed != 0 {
                        source
                            .read_exact_at(
                                range.chunk_offset,
                                &mut out[range.output_offset..range.output_offset + *allowed],
                            )
                            .map_err(Error::from)?;
                    }
                }
                observed.borrow_mut().scratch_peak = OUTBOARD_GROUP_BYTES;
                let mut group = 0;
                while group < groups.len() {
                    if !groups[group] {
                        group += 1;
                        continue;
                    }
                    let first = group;
                    while group < groups.len() && groups[group] {
                        group += 1;
                    }
                    let start = first as u64 * OUTBOARD_GROUP_BYTES as u64;
                    let stop = (group as u64 * OUTBOARD_GROUP_BYTES as u64).min(self.length);
                    let query = ChunkRanges::from(ChunkNum(start / 1024)..ChunkNum::chunks(stop));
                    let checked = ObservedOutboard {
                        inner: PreOrderMemOutboard {
                            root,
                            tree: tree(self.length),
                            data: self.raw_cv_bytes(),
                        },
                        work: &observed,
                    };
                    let input = PrivateRanges {
                        source: &source,
                        bytes: out,
                        ranges,
                        counts: &counts,
                    };
                    let verify_started = Instant::now();
                    let verified = bao_tree::io::sync::encode_ranges_validated(
                        input,
                        checked,
                        &query,
                        io::sink(),
                    )
                    .map_err(|error| match error {
                        bao_tree::io::EncodeError::Io(error) => Error::from(error),
                        other => bad_proof(&format!("Bao range verification failed: {other}")),
                    });
                    observed.borrow_mut().verify_ns += nanos(verify_started);
                    verified?;
                }
                Ok(counts)
            })();
            work.add(&observed.into_inner());
            return result;
        }
        let mut encoded = Vec::new();
        encoded
            .try_reserve_exact(OUTBOARD_GROUP_BYTES + MAX_ENCODED_OUTBOARD_BYTES)
            .map_err(|_| capacity())?;
        let result = (|| {
            for (group, selected) in groups.into_iter().enumerate() {
                if !selected {
                    continue;
                }
                let start = group as u64 * OUTBOARD_GROUP_BYTES as u64;
                let stop = (start + OUTBOARD_GROUP_BYTES as u64).min(self.length);
                let query = ChunkRanges::from(ChunkNum(start / 1024)..ChunkNum::chunks(stop));
                encoded.clear();
                observed.borrow_mut().scratch_peak = encoded.capacity() + OUTBOARD_GROUP_BYTES;
                bao_tree::io::sync::encode_ranges(&source, &bao, &query, &mut encoded)
                    .map_err(|error| Error::from(io::Error::from(error)))?;
                observed.borrow_mut().copied_bytes += stop - start;
                let verify_started = Instant::now();
                let verified = (|| {
                    for item in
                        DecodeResponseIter::new(root, tree(self.length), &encoded[..], &query)
                    {
                        let item = match item {
                            Ok(item) => item,
                            Err(error) => {
                                if matches!(error, bao_tree::io::DecodeError::ParentHashMismatch(_))
                                {
                                    observed.borrow_mut().parent_hash_bytes += 64;
                                }
                                if matches!(error, bao_tree::io::DecodeError::LeafHashMismatch(_)) {
                                    let mut work = observed.borrow_mut();
                                    work.hash_bytes += stop - start;
                                    work.copied_bytes += stop - start;
                                }
                                return Err(bad_proof(&format!(
                                    "Bao response verification failed: {error}"
                                )));
                            }
                        };
                        if matches!(item, BaoContentItem::Parent(_)) {
                            observed.borrow_mut().parent_hash_bytes += 64;
                        }
                        if let BaoContentItem::Leaf(leaf) = item {
                            observed.borrow_mut().hash_bytes += leaf.data.len() as u64;
                            observed.borrow_mut().copied_bytes += leaf.data.len() as u64;
                            let leaf_end = leaf.offset + leaf.data.len() as u64;
                            for (range, allowed) in ranges.iter().zip(&counts) {
                                let from = range.chunk_offset.max(leaf.offset);
                                let to = (range.chunk_offset + *allowed as u64).min(leaf_end);
                                if from < to {
                                    let n = (to - from) as usize;
                                    let dst =
                                        range.output_offset + (from - range.chunk_offset) as usize;
                                    out[dst..dst + n].copy_from_slice(
                                        &leaf.data[(from - leaf.offset) as usize
                                            ..(to - leaf.offset) as usize],
                                    );
                                    observed.borrow_mut().copied_bytes += n as u64;
                                }
                            }
                        }
                    }
                    Ok(())
                })();
                observed.borrow_mut().verify_ns += nanos(verify_started);
                verified?;
            }
            Ok(counts)
        })();
        work.add(&observed.into_inner());
        result
    }
}

// Keep the extra coverage buffer out of the aligned-read stack frame: only
// callers holding the larger partial-range work ticket enter this helper.
#[inline(never)]
fn read_single_coverage_group(
    source: &OffsetFile<'_>,
    bao: PreOrderMemOutboard<&[u8]>,
    range: VerifiedRangeCopy,
    allowed: usize,
    coverage: std::ops::Range<u64>,
    out: &mut [u8],
) -> Result<()> {
    // Authenticate the complete group before copying its requested subrange.
    // This buffer and Bao's official validation scratch fit the original
    // partial-range ticket, without an encoded body or retained allocation.
    let mut group = [0; OUTBOARD_GROUP_BYTES];
    let length = (coverage.end - coverage.start) as usize;
    source
        .read_exact_at(coverage.start, &mut group[..length])
        .map_err(Error::from)?;
    source.work.borrow_mut().scratch_peak = 2 * OUTBOARD_GROUP_BYTES;
    let input = BorrowedRange {
        bytes: &group[..length],
        start: coverage.start,
        work: source.work,
    };
    let checked = ObservedOutboard {
        inner: bao,
        work: source.work,
    };
    let query = ChunkRanges::from(ChunkNum(coverage.start / 1024)..ChunkNum::chunks(coverage.end));
    let verify_started = Instant::now();
    let verified = bao_tree::io::sync::encode_ranges_validated(input, checked, &query, io::sink())
        .map_err(|error| match error {
            bao_tree::io::EncodeError::Io(error) => Error::from(error),
            other => bad_proof(&format!("Bao range verification failed: {other}")),
        });
    source.work.borrow_mut().verify_ns += nanos(verify_started);
    verified?;
    let from = (range.chunk_offset - coverage.start) as usize;
    out[range.output_offset..range.output_offset + allowed]
        .copy_from_slice(&group[from..from + allowed]);
    source.work.borrow_mut().copied_bytes += allowed as u64;
    Ok(())
}

// Object-relative ReadAt over the already-filled private result. Only the
// requested aligned interval is accessible; all other offsets fail or EOF.
struct BorrowedRange<'a> {
    bytes: &'a [u8],
    start: u64,
    work: &'a RefCell<OutboardReadWork>,
}
impl ReadAt for BorrowedRange<'_> {
    fn read_at(&self, offset: u64, out: &mut [u8]) -> io::Result<usize> {
        let start = offset
            .checked_sub(self.start)
            .and_then(|relative| usize::try_from(relative).ok())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Bao read outside private range",
                )
            })?;
        let Some(bytes) = self.bytes.get(start..) else {
            return Ok(0);
        };
        let count = bytes.len().min(out.len());
        out[..count].copy_from_slice(&bytes[..count]);
        let mut work = self.work.borrow_mut();
        // The official validator consumes this exact scratch for one complete
        // subtree hash. This is still a user-space copy, not zero-copy hashing.
        work.copied_bytes += count as u64;
        work.hash_bytes += count as u64;
        Ok(count)
    }
}

fn nonaliased_ranges(ranges: &[VerifiedRangeCopy], counts: &[usize]) -> bool {
    for (index, (range, allowed)) in ranges.iter().zip(counts).enumerate() {
        if *allowed == 0 {
            continue;
        }
        let end = range.output_offset.saturating_add(*allowed);
        for (other, count) in ranges[index + 1..].iter().zip(&counts[index + 1..]) {
            if *count != 0
                && ((range.output_offset < other.output_offset.saturating_add(*count)
                    && other.output_offset < end)
                    || (range.chunk_offset < other.chunk_offset.saturating_add(*count as u64)
                        && other.chunk_offset < range.chunk_offset.saturating_add(*allowed as u64)))
            {
                return false;
            }
        }
    }
    true
}

// A bounded view, not another payload owner. The official validator authenticates
// all selected coverage groups, including bytes outside the requested intervals.
struct PrivateRanges<'view, 'file> {
    source: &'view OffsetFile<'file>,
    bytes: &'view [u8],
    ranges: &'view [VerifiedRangeCopy],
    counts: &'view [usize],
}

impl ReadAt for PrivateRanges<'_, '_> {
    fn read_at(&self, offset: u64, out: &mut [u8]) -> io::Result<usize> {
        let count = self
            .source
            .length
            .saturating_sub(offset)
            .min(out.len() as u64) as usize;
        let mut filled = 0;
        while filled < count {
            let position = offset + filled as u64;
            let mut next = offset + count as u64;
            let mut covered = None;
            for (range, allowed) in self.ranges.iter().zip(self.counts) {
                if *allowed == 0 {
                    continue;
                }
                let end = range.chunk_offset.saturating_add(*allowed as u64);
                if range.chunk_offset <= position && position < end {
                    let length = (end.min(next) - position) as usize;
                    let start = range.output_offset + (position - range.chunk_offset) as usize;
                    covered = Some((start, length));
                    break;
                }
                if range.chunk_offset > position {
                    next = next.min(range.chunk_offset);
                }
            }
            if let Some((start, length)) = covered {
                out[filled..filled + length].copy_from_slice(&self.bytes[start..start + length]);
                self.source.work.borrow_mut().copied_bytes += length as u64;
                filled += length;
            } else {
                let length = (next - position) as usize;
                self.source
                    .read_exact_at(position, &mut out[filled..filled + length])?;
                filled += length;
            }
        }
        self.source.work.borrow_mut().hash_bytes += count as u64;
        Ok(count)
    }
}

struct ObservedOutboard<'a> {
    inner: PreOrderMemOutboard<&'a [u8]>,
    work: &'a RefCell<OutboardReadWork>,
}
impl Outboard for ObservedOutboard<'_> {
    fn root(&self) -> blake3::Hash {
        self.inner.root()
    }
    fn tree(&self) -> BaoTree {
        self.inner.tree()
    }
    fn load(&self, node: TreeNode) -> io::Result<Option<(blake3::Hash, blake3::Hash)>> {
        let pair = self.inner.load(node)?;
        if pair.is_some() {
            self.work.borrow_mut().parent_hash_bytes += 64;
        }
        Ok(pair)
    }
}

struct OffsetFile<'a> {
    file: &'a File,
    base: u64,
    length: u64,
    work: &'a RefCell<OutboardReadWork>,
}
impl ReadAt for OffsetFile<'_> {
    fn read_at(&self, offset: u64, out: &mut [u8]) -> io::Result<usize> {
        let offset = self.base.checked_add(offset).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "outboard positioned offset overflow",
            )
        })?;
        let start = Instant::now();
        let result = ReadAt::read_at(self.file, offset, out);
        let mut work = self.work.borrow_mut();
        work.read_calls += 1;
        work.read_ns += nanos(start);
        result
    }
}
impl Size for OffsetFile<'_> {
    fn size(&self) -> io::Result<Option<u64>> {
        Ok(Some(self.length))
    }
}

/// One range call's observations and already charged working ownership.
/// The consuming read keeps the ticket alive until all codec scratch drops.
pub(crate) struct OutboardReadContext<'a> {
    pub(crate) work: &'a mut OutboardReadWork,
    pub(crate) working: OutboardLease,
}

#[derive(Debug, Default)]
pub(crate) struct OutboardReadWork {
    pub read_calls: u64,
    pub hash_bytes: u64,
    pub parent_hash_bytes: u64,
    pub copied_bytes: u64,
    pub read_ns: u64,
    pub verify_ns: u64,
    pub scratch_peak: usize,
}
impl OutboardReadWork {
    fn add(&mut self, other: &Self) {
        self.read_calls += other.read_calls;
        self.hash_bytes += other.hash_bytes;
        self.parent_hash_bytes += other.parent_hash_bytes;
        self.copied_bytes += other.copied_bytes;
        self.read_ns += other.read_ns;
        self.verify_ns += other.verify_ns;
        self.scratch_peak = self.scratch_peak.max(other.scratch_peak);
    }
}
#[derive(Debug)]
pub(crate) struct OutboardBuild {
    pub digest: ContentDigest,
    pub outboard: Option<SharedOutboard>,
    pub hash_body_bytes: u64,
    pub parent_hash_bytes: u64,
}
#[derive(Debug)]
pub(crate) struct OutboardBuilder {
    length: u64,
    seen: u64,
    expected: Option<ContentDigest>,
    // Keep the fallback hasher inline: metadata exhaustion must not require
    // another heap allocation merely to retain complete-root verification.
    root_hash: Option<blake3::Hasher>,
    state: BuildState,
    _working: Option<OutboardLease>,
    invalid_peer: bool,
    error: Option<Error>,
}
#[derive(Debug)]
enum BuildState {
    Root,
    Tree(Box<TreeBuilder>),
}
#[derive(Debug)]
struct TreeBuilder {
    iter: PostOrderChunkIter,
    leaf: Option<(usize, bool)>,
    hash: blake3::Hasher,
    stack: [blake3::Hash; 16],
    depth: usize,
    outboard: PreOrderMemOutboard<Vec<u8>>,
    lease: OutboardLease,
    peer: Option<SharedOutboard>,
    parent_hash_bytes: u64,
}
impl TreeBuilder {
    fn advance(&mut self) -> Result<bool> {
        for item in self.iter.by_ref() {
            match item {
                BaoChunk::Parent { node, is_root, .. } => {
                    if self.depth < 2 {
                        return Err(super::invalid("invalid Bao parent traversal"));
                    }
                    let right = self.stack[self.depth - 1];
                    let left = self.stack[self.depth - 2];
                    self.depth -= 2;
                    if let Some(peer) = &self.peer {
                        let offset = self
                            .outboard
                            .tree
                            .pre_order_offset(node)
                            .ok_or_else(|| super::invalid("invalid Bao node"))?
                            as usize
                            * 64;
                        if peer.raw_cv_bytes()[offset..offset + 32] != *left.as_bytes()
                            || peer.raw_cv_bytes()[offset + 32..offset + 64] != *right.as_bytes()
                        {
                            return Err(bad_proof("peer outboard differs from complete body"));
                        }
                    }
                    self.outboard
                        .save(node, &(left, right))
                        .map_err(Error::from)?;
                    let merged = if is_root {
                        merge_subtrees_root(left.as_bytes(), right.as_bytes(), Mode::Hash)
                    } else {
                        blake3::Hash::from(merge_subtrees_non_root(
                            left.as_bytes(),
                            right.as_bytes(),
                            Mode::Hash,
                        ))
                    };
                    self.parent_hash_bytes += 64;
                    self.stack[self.depth] = merged;
                    self.depth += 1;
                }
                BaoChunk::Leaf {
                    start_chunk,
                    size,
                    is_root,
                    ..
                } => {
                    self.hash = blake3::Hasher::new();
                    if !is_root {
                        self.hash.set_input_offset(start_chunk.to_bytes());
                    }
                    self.leaf = Some((size, is_root));
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }
    fn finish_leaf(&mut self, root: bool) {
        let hash = if root {
            self.hash.finalize()
        } else {
            blake3::Hash::from(self.hash.finalize_non_root())
        };
        self.stack[self.depth] = hash;
        self.depth += 1;
        self.leaf = None;
    }
}
impl OutboardBuilder {
    /// Construction-time eligibility only, never proof that body bytes have
    /// been verified. The consumer fixes its complete-root mode before input.
    #[cfg(test)]
    pub(crate) fn has_peer_tree(&self) -> bool {
        self.seen == 0
            && !self.invalid_peer
            && self.error.is_none()
            && matches!(&self.state, BuildState::Tree(builder) if builder.peer.is_some())
    }
    /// Header-time eligibility for one standard-root builder, including a
    /// locally generated tree. This cannot change modes after body input.
    #[cfg(any(feature = "dfs", test))]
    pub(crate) fn has_tree(&self) -> bool {
        self.seen == 0
            && !self.invalid_peer
            && self.error.is_none()
            && matches!(&self.state, BuildState::Tree(_))
    }

    pub(crate) fn new(length: u64, budget: Arc<OutboardBudget>) -> Self {
        Self::create(length, None, None, budget)
    }
    #[cfg(any(feature = "dfs", test))]
    pub(crate) fn validating(
        expected: ChunkObject,
        peer: SharedOutboard,
        budget: Arc<OutboardBudget>,
    ) -> Self {
        Self::create(
            expected.length,
            Some(expected.content_digest),
            Some(peer),
            budget,
        )
    }
    fn create(
        length: u64,
        expected: Option<ContentDigest>,
        peer: Option<SharedOutboard>,
        budget: Arc<OutboardBudget>,
    ) -> Self {
        let invalid_peer = peer.as_ref().is_some_and(|peer| {
            peer.length != length
                || expected
                    .as_ref()
                    .is_some_and(|expected| *expected != peer.digest)
        });
        let fallback = || Self {
            length,
            seen: 0,
            expected: expected.clone(),
            root_hash: Some(blake3::Hasher::new()),
            state: BuildState::Root,
            _working: None,
            invalid_peer,
            error: None,
        };
        if length > MAX_OBJECT_BYTES {
            return fallback();
        }
        let geometry = tree(length);
        let encoded_size = HEADER + geometry.outboard_size() as usize;
        let Some(mut lease) =
            budget.try_reserve(ChunkOutboard::resident_charge(encoded_size).unwrap_or(usize::MAX))
        else {
            return fallback();
        };
        let Some(working) =
            budget.try_reserve(std::mem::size_of::<TreeBuilder>() + std::mem::size_of::<Self>())
        else {
            return fallback();
        };
        let mut raw = Vec::new();
        if raw.try_reserve_exact(encoded_size).is_err() {
            return fallback();
        }
        // The same allocation becomes the final encoded outboard; no CV table copy.
        raw.resize(encoded_size, 0);
        if lease
            .retain_capacity(ChunkOutboard::resident_charge(raw.capacity()).unwrap_or(usize::MAX))
            .is_err()
        {
            return fallback();
        }
        let builder = TreeBuilder {
            iter: geometry.post_order_chunks_iter(),
            leaf: None,
            hash: blake3::Hasher::new(),
            stack: [blake3::Hash::from([0; 32]); 16],
            depth: 0,
            outboard: PreOrderMemOutboard {
                root: blake3::Hash::from([0; 32]),
                tree: geometry,
                data: raw,
            },
            lease,
            peer,
            parent_hash_bytes: 0,
        };
        Self {
            length,
            seen: 0,
            expected,
            root_hash: None,
            state: BuildState::Tree(Box::new(builder)),
            _working: Some(working),
            invalid_peer,
            error: None,
        }
    }
    pub(crate) fn update(&mut self, bytes: &[u8]) -> Result<()> {
        if let Some(error) = &self.error {
            return Err(error.clone());
        }
        let result = self.update_inner(bytes);
        if let Err(error) = &result {
            self.error = Some(error.clone());
        }
        result
    }
    fn update_inner(&mut self, mut bytes: &[u8]) -> Result<()> {
        if self.invalid_peer {
            return Err(bad_proof("peer outboard trusted identity differs"));
        }
        let next = self
            .seen
            .checked_add(bytes.len() as u64)
            .filter(|seen| *seen <= self.length)
            .ok_or_else(|| super::invalid("outboard body exceeds declared length"))?;
        match &mut self.state {
            BuildState::Root => {
                self.root_hash
                    .as_mut()
                    .ok_or_else(|| super::invalid("missing complete-root fallback hasher"))?
                    .update(bytes);
            }
            BuildState::Tree(builder) => {
                while !bytes.is_empty() {
                    if builder.leaf.is_none() && !builder.advance()? {
                        return Err(super::invalid("Bao traversal ended before body"));
                    }
                    let (remaining, root) = builder
                        .leaf
                        .ok_or_else(|| super::invalid("Bao leaf missing"))?;
                    let n = remaining.min(bytes.len());
                    builder.hash.update(&bytes[..n]);
                    bytes = &bytes[n..];
                    builder.leaf = Some((remaining - n, root));
                    if n == remaining {
                        builder.finish_leaf(root);
                    }
                }
            }
        }
        self.seen = next;
        Ok(())
    }
    pub(crate) fn finish(mut self) -> Result<OutboardBuild> {
        if let Some(error) = self.error.take() {
            return Err(error);
        }
        if self.invalid_peer {
            return Err(bad_proof("peer outboard trusted identity differs"));
        }
        if self.seen != self.length {
            return Err(super::invalid("outboard body shorter than declared length"));
        }
        let (digest, outboard, parent_hash_bytes) = match self.state {
            BuildState::Root => (
                ContentDigest {
                    algorithm: DigestAlgorithm::Blake3,
                    bytes: *self
                        .root_hash
                        .take()
                        .ok_or_else(|| super::invalid("missing complete-root fallback hasher"))?
                        .finalize()
                        .as_bytes(),
                },
                None,
                0,
            ),
            BuildState::Tree(ref mut builder) => {
                if self.length == 0 && builder.leaf.is_none() {
                    builder.advance()?;
                    builder.finish_leaf(true);
                }
                if builder.advance()? || builder.depth != 1 {
                    return Err(super::invalid("Bao traversal incomplete at EOF"));
                }
                let digest = ContentDigest {
                    algorithm: DigestAlgorithm::Blake3,
                    bytes: *builder.stack[0].as_bytes(),
                };
                let mut encoded = std::mem::take(&mut builder.outboard.data);
                // save() used offsets into data's raw table. Move the table in-place
                // only within this tiny metadata allocation to prepend its header.
                let raw_size = tree(self.length).outboard_size() as usize;
                encoded.copy_within(0..raw_size, HEADER);
                encoded[..8].copy_from_slice(MAGIC);
                encoded[8..10].copy_from_slice(&1_u16.to_le_bytes());
                encoded[10] = GROUP_LOG;
                encoded[11..16].fill(0);
                encoded[16..24].copy_from_slice(&self.length.to_le_bytes());
                encoded[24..56].copy_from_slice(&digest.bytes);
                encoded[56..60].copy_from_slice(&(raw_size as u32).to_le_bytes());
                encoded[60..64].fill(0);
                let replacement = OutboardLease {
                    budget: builder.lease.budget.clone(),
                    bytes: 0,
                    resident: Mutex::new(OutboardResidence::default()),
                };
                let lease = std::mem::replace(&mut builder.lease, replacement);
                let proof = Arc::new(ChunkOutboard {
                    length: self.length,
                    digest: digest.clone(),
                    encoded,
                    lease,
                });
                (digest, Some(proof), builder.parent_hash_bytes)
            }
        };
        if self
            .expected
            .as_ref()
            .is_some_and(|expected| *expected != digest)
        {
            return Err(bad_proof(
                "complete outboard body differs from trusted root",
            ));
        }
        Ok(OutboardBuild {
            digest,
            outboard,
            hash_body_bytes: self.seen,
            parent_hash_bytes,
        })
    }
}
pub(crate) fn build_borrowed(body: &[u8], budget: &Arc<OutboardBudget>) -> OutboardBuild {
    let mut builder = OutboardBuilder::new(body.len() as u64, budget.clone());
    // Exact borrowed input length is known here; any internal failure must not
    // make optional proof metadata alter the established write contract.
    if builder.update(body).is_ok()
        && let Ok(built) = builder.finish()
    {
        return built;
    }
    OutboardBuild {
        digest: super::digest(body),
        outboard: None,
        hash_body_bytes: body.len() as u64,
        parent_hash_bytes: 0,
    }
}
fn nanos(start: Instant) -> u64 {
    start.elapsed().as_nanos().min(u64::MAX as u128) as u64
}
fn bad_proof(message: &str) -> Error {
    Error::coded(afs_error::NODE_TRANSFER_CORRUPT_DATA, message)
}
fn capacity() -> Error {
    Error::coded(
        afs_error::IO_CAPACITY,
        "shared outboard metadata budget exhausted",
    )
}

#[cfg(test)]
std::thread_local! {
    static OUTBOARD_DROP_EVENTS: RefCell<Vec<(&'static str, usize)>> = const { RefCell::new(Vec::new()) };
}
fn drop_event(event: &'static str, used: usize) {
    #[cfg(test)]
    OUTBOARD_DROP_EVENTS.with(|events| events.borrow_mut().push((event, used)));
    #[cfg(not(test))]
    let _ = (event, used);
}

#[cfg(test)]
mod tests {
    use super::*;
    use bao_tree::{BlockSize, io::outboard::PreOrderMemOutboard};

    #[test]
    fn p2_w0_sidecar_headroom_is_instantaneous_and_uses_largest_work_ticket() {
        let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
        let proof = build_borrowed(&vec![37; 128 * 1024], &budget)
            .outboard
            .unwrap();
        let pressure = budget
            .try_reserve(budget.limit() - budget.used() - 4 * RANGE_WORKING_BYTES)
            .unwrap();
        let before = budget.used();
        let pin = budget.pin_sidecar(&proof).unwrap();
        assert_eq!(
            budget.used(),
            before,
            "Arc pin cannot add another proof byte charge"
        );
        assert!(std::ptr::eq(
            proof.encoded().as_ptr(),
            pin.proof().encoded().as_ptr()
        ));
        // Original builder admission remains nonblocking and can consume this
        // headroom later; the queue never claims a permanent new partition.
        let extra = budget.try_reserve(1).unwrap();
        assert!(budget.pin_sidecar(&proof).is_none());
        drop(extra);
        assert!(budget.pin_sidecar(&proof).is_some());
        drop(pin);
        drop(pressure);
        drop(proof);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn p2_w0_sidecar_pins_keep_shared_allocation_transient_until_last_pin() {
        let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
        let proof = build_borrowed(&vec![31; 128 * 1024], &budget)
            .outboard
            .unwrap();
        let charge = proof.charged_bytes();
        let first = budget.pin_sidecar(&proof).unwrap();
        let second = budget.pin_sidecar(&proof).unwrap();
        assert!(!proof.try_mark_resident());
        assert_eq!((budget.used(), budget.resident_used()), (charge, 0));
        drop(first);
        assert!(!proof.try_mark_resident());
        drop(second);
        assert!(proof.try_mark_resident());
        assert!(
            budget.pin_sidecar(&proof).is_none(),
            "resident ownership must never be demoted for the queue"
        );
        assert_eq!((budget.used(), budget.resident_used()), (charge, charge));
        drop(proof);
        assert_eq!((budget.used(), budget.resident_used()), (0, 0));
    }

    #[test]
    fn p2_w0_sidecar_rejects_foreign_ledger_and_retains_original_last_owner() {
        let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
        let foreign = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
        let proof = build_borrowed(&vec![13; 128 * 1024], &budget)
            .outboard
            .unwrap();
        assert!(foreign.pin_sidecar(&proof).is_none());
        let pin = budget.pin_sidecar(&proof).unwrap();
        let independent = proof.clone();
        drop(proof);
        drop(pin);
        assert!(budget.used() > 0);
        assert_eq!(foreign.used(), 0);
        drop(independent);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn p2_r3_header_tree_eligibility_includes_local_tree_without_peer_proof() {
        let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
        let mut local = OutboardBuilder::new(128 * 1024, budget);
        assert!(local.has_tree());
        assert!(!local.has_peer_tree());
        local.update(&[37]).unwrap();
        assert!(!local.has_tree());
        assert!(!OutboardBuilder::new(128 * 1024, OutboardBudget::new(0)).has_tree());
    }

    #[test]
    fn p2_c1a_default_range_admission_waits_until_original_deadline() {
        let budget = OutboardBudget::new(RANGE_WORKING_BYTES);
        let occupied = budget.try_reserve(RANGE_WORKING_BYTES).unwrap();
        let entered = Instant::now();
        assert!(budget.try_reserve_range_work().is_none());
        let returned_after = entered.elapsed();
        eprintln!("range-work default exhausted return delay: {returned_after:?}");
        assert!(
            returned_after >= std::time::Duration::from_millis(2),
            "default range-work wait must use the original 2ms deadline"
        );
        assert_eq!(budget.used(), RANGE_WORKING_BYTES);
        drop(occupied);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn p2_c1a_last_holder_release_wakes_range_admission_without_early_refund() {
        use std::{sync::mpsc, thread};
        let budget = OutboardBudget::create(RANGE_WORKING_BYTES, None, Duration::from_millis(50));
        let occupied = Arc::new(budget.try_reserve(RANGE_WORKING_BYTES).unwrap());
        let last = occupied.clone();
        drop(occupied);
        assert_eq!(budget.used(), RANGE_WORKING_BYTES);
        let (checked, checked_rx) = mpsc::sync_channel(0);
        let (resume, resume_rx) = mpsc::sync_channel(0);
        budget.work_state.lock().unwrap().before_wait = Some((checked, resume_rx));
        let waiting_budget = budget.clone();
        let waiter = thread::spawn(move || waiting_budget.try_reserve_range_work());
        checked_rx.recv().unwrap();
        // Capacity was checked while holding work_state. Start the final
        // release at that exact check/wait boundary; its lock must serialize
        // with the waiter's atomic unlock-and-wait, so no notification is lost.
        let (releasing, releasing_rx) = mpsc::sync_channel(0);
        let releaser = thread::spawn(move || {
            releasing.send(()).unwrap();
            drop(last);
        });
        releasing_rx.recv().unwrap();
        assert_eq!(budget.used(), RANGE_WORKING_BYTES);
        resume.send(()).unwrap();
        let ticket = waiter.join().unwrap();
        releaser.join().unwrap();
        assert!(
            ticket.is_some(),
            "boundary release must wake range admission"
        );
        assert_eq!(budget.used(), RANGE_WORKING_BYTES);
        drop(ticket);
        assert_eq!(budget.used(), 0);
    }

    fn c1a_wait_for_waiters(budget: &OutboardBudget, count: usize) {
        let guard = Instant::now() + Duration::from_secs(2);
        loop {
            if budget.work_state.lock().unwrap().waiters == count {
                return;
            }
            assert!(
                Instant::now() < guard,
                "expected {count} parked range waiters"
            );
            std::thread::yield_now();
        }
    }

    #[test]
    fn p2_c1a_zero_wait_and_builder_resident_remain_nonblocking() {
        let budget = OutboardBudget::create(RANGE_WORKING_BYTES, None, Duration::ZERO);
        let held = budget.try_reserve(RANGE_WORKING_BYTES).unwrap();
        assert!(budget.try_reserve_range_work().is_none());
        assert_eq!(budget.work_state.lock().unwrap().waiters, 0);
        assert!(budget.try_reserve(1).is_none());
        assert!(budget.try_reserve_resident(1).is_none());
        drop(held);
        assert!(budget.try_reserve_range_work().is_some());
    }

    #[test]
    fn p2_c1a_stop_wakes_all_waiters_without_refunding_held_tickets() {
        let budget = OutboardBudget::create(RANGE_WORKING_BYTES, None, Duration::from_millis(50));
        let held = budget.try_reserve(RANGE_WORKING_BYTES).unwrap();
        let waiters: Vec<_> = (0..8)
            .map(|_| {
                let budget = budget.clone();
                std::thread::spawn(move || budget.try_reserve_range_work().is_none())
            })
            .collect();
        c1a_wait_for_waiters(&budget, 8);
        budget.stop_range_work();
        for waiter in waiters {
            assert!(waiter.join().unwrap());
        }
        assert_eq!(budget.used(), RANGE_WORKING_BYTES);
        drop(held);
        assert_eq!(budget.used(), 0);
        assert!(budget.try_reserve_range_work().is_none());
        // Stop affects range workers, not the builder/resident admission API.
        assert!(budget.try_reserve(1).is_some());
    }

    #[test]
    fn p2_c1a_spurious_notifications_do_not_extend_entry_deadline() {
        let budget = OutboardBudget::create(RANGE_WORKING_BYTES, None, Duration::from_millis(5));
        let held = budget.try_reserve(RANGE_WORKING_BYTES).unwrap();
        let waiting = budget.clone();
        let waiter = std::thread::spawn(move || {
            let entered = Instant::now();
            let result = waiting.try_reserve_range_work();
            (result.is_none(), entered.elapsed())
        });
        c1a_wait_for_waiters(&budget, 1);
        // Keep waking while capacity remains unavailable. Completion must not
        // depend on the notifications ending or the held allocation retiring.
        let notify_until = Instant::now() + Duration::from_millis(500);
        while !waiter.is_finished() && Instant::now() < notify_until {
            budget.work_changed.notify_all();
            std::thread::yield_now();
        }
        assert!(
            waiter.is_finished(),
            "spurious wakeups renewed the waiting interval"
        );
        let (rejected, elapsed) = waiter.join().unwrap();
        assert!(rejected);
        eprintln!("5ms entry deadline under spurious wakeups: returned after {elapsed:?}");
        assert_eq!(budget.used(), RANGE_WORKING_BYTES);
        drop(held);
    }

    #[test]
    fn p2_c1a_expired_check_wait_boundary_does_not_start_another_wait() {
        use std::sync::mpsc;
        let budget = OutboardBudget::create(RANGE_WORKING_BYTES, None, Duration::from_millis(2));
        let held = budget.try_reserve(RANGE_WORKING_BYTES).unwrap();
        let (checked, checked_rx) = mpsc::sync_channel(0);
        let (resume, resume_rx) = mpsc::sync_channel(0);
        budget.work_state.lock().unwrap().before_wait = Some((checked, resume_rx));
        let waiting = budget.clone();
        let waiter = std::thread::spawn(move || waiting.try_reserve_range_work().is_none());
        checked_rx.recv().unwrap();
        std::thread::sleep(Duration::from_millis(5));
        resume.send(()).unwrap();
        assert!(waiter.join().unwrap());
        assert_eq!(budget.work_state.lock().unwrap().waiters, 0);
        assert_eq!(budget.used(), RANGE_WORKING_BYTES);
        drop(held);
    }

    #[test]
    fn p2_c1a_aligned_and_partial_work_account_their_real_lifetimes() {
        let aligned = [VerifiedRangeCopy {
            chunk_offset: 0,
            length: 128 * 1024,
            output_offset: 0,
        }];
        let partial = [VerifiedRangeCopy {
            chunk_offset: 1,
            length: 128 * 1024,
            output_offset: 0,
        }];
        assert_eq!(
            range_working_bytes(&aligned, 4 * 1024 * 1024),
            ALIGNED_RANGE_WORKING_BYTES
        );
        assert_eq!(
            range_working_bytes(&partial, 4 * 1024 * 1024),
            RANGE_WORKING_BYTES
        );
        let budget = OutboardBudget::new(ALIGNED_RANGE_WORKING_BYTES);
        let lease = budget
            .reserve_range_work(&aligned, 4 * 1024 * 1024)
            .unwrap();
        assert_eq!(budget.used(), ALIGNED_RANGE_WORKING_BYTES);
        drop(lease);
        assert_eq!(budget.used(), 0);
        assert!(
            budget
                .reserve_range_work(&partial, 4 * 1024 * 1024)
                .is_none()
        );
    }

    #[test]
    #[ignore = "fixed resource acceptance: run with --release and --ignored; debug timings are not performance criteria"]
    fn p2_c1a_32_threads_hot_object_with_resident_sublimit_full() {
        use std::{
            os::unix::fs::FileExt,
            sync::{Barrier, atomic::AtomicUsize},
        };
        let body = vec![59; 4 * 1024 * 1024];
        let chunk = expected(&body);
        let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
        let proofs: Vec<_> = (0..60)
            .map(|_| {
                let proof = build_borrowed(&body, &budget).outboard.unwrap();
                assert!(proof.try_mark_resident());
                proof
            })
            .collect();
        let remaining = budget.resident_limit() - budget.resident_used();
        let resident_padding = budget.try_reserve_resident(remaining).unwrap();
        assert_eq!(budget.resident_used(), budget.resident_limit());
        let file = tempfile::tempfile().unwrap();
        file.write_all_at(&body, 0).unwrap();
        let start = Arc::new(Barrier::new(32));
        let fallbacks = AtomicUsize::new(0);
        let started = Instant::now();
        std::thread::scope(|scope| {
            for worker in 0..32 {
                let (proof, file, chunk, start, fallbacks) =
                    (&proofs[0], &file, &chunk, &start, &fallbacks);
                scope.spawn(move || {
                    let mut out = vec![0; 128 * 1024];
                    start.wait();
                    for iteration in 0..1000 {
                        let offset = ((worker + iteration) % 32) * out.len();
                        let result = proof.read_ranges_into_with_work(
                            file,
                            0,
                            chunk,
                            &[VerifiedRangeCopy {
                                chunk_offset: offset as u64,
                                length: out.len(),
                                output_offset: 0,
                            }],
                            &mut out,
                            &mut OutboardReadWork::default(),
                        );
                        match result {
                            Ok(counts) => {
                                assert_eq!(counts, vec![out.len()]);
                                assert!(out.iter().all(|byte| *byte == 59));
                            }
                            Err(error) => {
                                assert_eq!(error.code(), afs_error::IO_CAPACITY);
                                fallbacks.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                });
            }
        });
        let count = fallbacks.load(Ordering::Relaxed);
        eprintln!(
            "C-1a fixed hot range load: reads=32000 capacity_fallbacks={count} elapsed={:?} resident={} total={}",
            started.elapsed(),
            budget.resident_used(),
            budget.used()
        );
        assert!(
            count * 100 < 32000 * 5,
            "range full-scan fallback must be below 5%"
        );
        assert_eq!(budget.used(), budget.resident_used());
        drop(resident_padding);
        drop(proofs);
        assert_eq!((budget.used(), budget.resident_used()), (0, 0));
    }

    #[test]
    fn wi6_borrowed_builder_matches_official_standard_root_and_outboard() {
        for length in [
            0,
            1,
            1023,
            1024,
            32767,
            32768,
            32769,
            65536,
            4 * 1024 * 1024,
        ] {
            let body = (0..length).map(|n| (n % 251) as u8).collect::<Vec<_>>();
            let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
            let built = build_borrowed(&body, &budget);
            let official = PreOrderMemOutboard::create(&body, BlockSize::from_chunk_log(5));
            assert_eq!(built.digest.bytes, *blake3::hash(&body).as_bytes());
            assert_eq!(built.digest.bytes, *official.root.as_bytes());
            assert_eq!(
                built
                    .outboard
                    .as_ref()
                    .expect("outboard required within budget")
                    .raw_cv_bytes(),
                official.data
            );
            drop(built);
            assert_eq!(budget.used(), 0);
        }
    }

    #[test]
    fn wi6_last_owner_releases_encoded_allocation_before_lease() {
        let body = vec![37; 150_001];
        let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
        let proof = build_borrowed(&body, &budget).outboard.unwrap();
        let held = proof.clone();
        OUTBOARD_DROP_EVENTS.with(|events| events.borrow_mut().clear());
        drop(proof);
        assert!(OUTBOARD_DROP_EVENTS.with(|events| events.borrow().is_empty()));
        let charge = held.charged_bytes();
        drop(held);
        let events = OUTBOARD_DROP_EVENTS.with(|events| events.borrow().clone());
        assert_eq!(
            events,
            vec![("encoded_freed", charge), ("lease_released", 0)]
        );
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn wi6_rejected_encoded_input_frees_allocation_before_refund() {
        let body = vec![37; 150_001];
        let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
        let proof = build_borrowed(&body, &budget).outboard.unwrap();
        let mut encoded = proof.encoded().to_vec();
        encoded[0] ^= 1;
        let charge = ChunkOutboard::resident_charge(encoded.capacity()).unwrap();
        let lease = budget.try_reserve(charge).unwrap();
        drop(proof);
        OUTBOARD_DROP_EVENTS.with(|events| events.borrow_mut().clear());
        let error = ChunkOutboard::from_encoded(&expected(&body), encoded, lease).unwrap_err();
        assert_eq!(error.code(), afs_error::NODE_TRANSFER_CORRUPT_DATA);
        assert_eq!(
            OUTBOARD_DROP_EVENTS.with(|events| events.borrow().clone()),
            vec![("encoded_freed", charge), ("lease_released", 0)]
        );
        assert_eq!(budget.used(), 0);
    }

    fn expected(body: &[u8]) -> ChunkObject {
        super::super::StagedChunk::new(crate::dfs::OperationId::new("outboard-test"), body.to_vec())
            .chunk
    }

    #[test]
    fn checkpoint_small_range_avoids_encoded_body_and_keeps_one_coverage_read() {
        use std::io::Write;
        let body = (0..3 * OUTBOARD_GROUP_BYTES + 777)
            .map(|n| (n % 251) as u8)
            .collect::<Vec<_>>();
        let chunk = expected(&body);
        let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
        let proof = build_borrowed(&body, &budget).outboard.unwrap();
        let before = budget.used();
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(&[0xa5; 4096]).unwrap();
        file.write_all(&body).unwrap();
        for (offset, length) in [
            (19, 4096),
            (OUTBOARD_GROUP_BYTES + 17, 4096),
            (3 * OUTBOARD_GROUP_BYTES + 13, 900),
        ] {
            let allowed = length.min(body.len() - offset);
            let group_start = offset / OUTBOARD_GROUP_BYTES * OUTBOARD_GROUP_BYTES;
            let coverage = OUTBOARD_GROUP_BYTES.min(body.len() - group_start);
            let mut out = vec![0xa5; allowed + 14];
            let mut work = OutboardReadWork::default();
            assert_eq!(
                proof
                    .read_ranges_into_with_work(
                        &file,
                        4096,
                        &chunk,
                        &[VerifiedRangeCopy {
                            chunk_offset: offset as u64,
                            length,
                            output_offset: 7
                        }],
                        &mut out,
                        &mut work,
                    )
                    .unwrap(),
                vec![allowed]
            );
            assert_eq!(&out[7..7 + allowed], &body[offset..offset + allowed]);
            assert!(
                out[..7]
                    .iter()
                    .chain(&out[7 + allowed..])
                    .all(|byte| *byte == 0xa5)
            );
            assert_eq!(work.read_calls, 1);
            assert_eq!(work.hash_bytes, coverage as u64);
            assert_eq!(
                work.copied_bytes,
                (coverage + allowed) as u64,
                "coverage into official verifier scratch, then only the requested output; no encoded body"
            );
            assert!(work.scratch_peak <= 2 * OUTBOARD_GROUP_BYTES);
            assert_eq!(budget.used(), before);
        }
    }

    #[test]
    fn checkpoint_small_range_checks_unrequested_padding_before_delivery() {
        use std::io::Write;
        let body = vec![7; 2 * OUTBOARD_GROUP_BYTES];
        let chunk = expected(&body);
        let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
        let proof = build_borrowed(&body, &budget).outboard.unwrap();
        let mut damaged = body.clone();
        damaged[20 * 1024] ^= 1;
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(&damaged).unwrap();
        let mut out = vec![0xa5; 4096];
        assert!(
            proof
                .read_ranges_into(
                    &file,
                    0,
                    &chunk,
                    &[VerifiedRangeCopy {
                        chunk_offset: 17,
                        length: out.len(),
                        output_offset: 0
                    }],
                    &mut out,
                )
                .is_err()
        );
        assert!(out.iter().all(|byte| *byte == 0xa5));
    }

    #[test]
    fn wi6_aligned_range_reads_file_once_and_copies_only_into_official_scratch() {
        use std::io::Write;
        let body = (0..4 * 1024 * 1024)
            .map(|n| (n % 251) as u8)
            .collect::<Vec<_>>();
        let chunk = expected(&body);
        let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
        let proof = build_borrowed(&body, &budget).outboard.unwrap();
        let before = budget.used();
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(&[0xa5; 4096]).unwrap();
        file.write_all(&body).unwrap();
        file.write_all(b"trailer").unwrap();
        for offset in [0, 1024 * 1024, 3 * 1024 * 1024] {
            let length = 1024 * 1024;
            let mut out = vec![0xa5; length + 14];
            let mut work = OutboardReadWork::default();
            let counts = proof
                .read_ranges_into_with_work(
                    &file,
                    4096,
                    &chunk,
                    &[VerifiedRangeCopy {
                        chunk_offset: offset as u64,
                        length,
                        output_offset: 7,
                    }],
                    &mut out,
                    &mut work,
                )
                .unwrap();
            assert_eq!(counts, vec![length]);
            assert_eq!(&out[7..7 + length], &body[offset..offset + length]);
            assert!(
                out[..7]
                    .iter()
                    .chain(out[7 + length..].iter())
                    .all(|byte| *byte == 0xa5)
            );
            assert_eq!(
                work.read_calls, 1,
                "one contiguous file read, not one per group"
            );
            assert_eq!(work.hash_bytes, length as u64);
            assert_eq!(
                work.copied_bytes, length as u64,
                "only borrowed output into Bao validation scratch"
            );
            assert!(work.scratch_peak <= OUTBOARD_GROUP_BYTES);
            assert!(work.parent_hash_bytes > 0);
            assert_eq!(budget.used(), before);
        }
    }

    #[test]
    fn wi6_aligned_range_validates_a_short_last_group_and_rejects_tampering() {
        use std::io::Write;
        use std::os::unix::fs::FileExt;
        let length = 3 * OUTBOARD_GROUP_BYTES + 37;
        let body = vec![37; length];
        let chunk = expected(&body);
        let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
        let proof = build_borrowed(&body, &budget).outboard.unwrap();
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(&body).unwrap();
        let range = VerifiedRangeCopy {
            chunk_offset: OUTBOARD_GROUP_BYTES as u64,
            length,
            output_offset: 3,
        };
        let allowed = length - OUTBOARD_GROUP_BYTES;
        let mut out = vec![0xa5; length + 3];
        let mut work = OutboardReadWork::default();
        assert_eq!(
            proof
                .read_ranges_into_with_work(&file, 0, &chunk, &[range], &mut out, &mut work)
                .unwrap(),
            vec![allowed]
        );
        assert_eq!(&out[3..3 + allowed], &body[OUTBOARD_GROUP_BYTES..]);
        assert_eq!(work.read_calls, 1);
        assert_eq!(work.hash_bytes, allowed as u64);
        assert_eq!(work.copied_bytes, allowed as u64);
        // Output is caller-private until successful validation. On corruption
        // it may contain untrusted bytes, but must never be returned as success.
        file.write_at(b"X", (length - 1) as u64).unwrap();
        assert_eq!(
            proof
                .read_ranges_into(
                    &file,
                    0,
                    &chunk,
                    &[VerifiedRangeCopy {
                        chunk_offset: 0,
                        length,
                        output_offset: 0
                    }],
                    &mut out
                )
                .unwrap_err()
                .code(),
            afs_error::NODE_TRANSFER_CORRUPT_DATA
        );
        file.write_at(&[37], (length - 1) as u64).unwrap();
        let mut encoded = proof.encoded().to_vec();
        encoded[HEADER] ^= 1;
        let lease = budget
            .try_reserve(ChunkOutboard::resident_charge(encoded.capacity()).unwrap())
            .unwrap();
        let damaged = ChunkOutboard::from_encoded(&chunk, encoded, lease).unwrap();
        assert_eq!(
            damaged
                .read_ranges_into(
                    &file,
                    0,
                    &chunk,
                    &[VerifiedRangeCopy {
                        chunk_offset: 0,
                        length,
                        output_offset: 0
                    }],
                    &mut out
                )
                .unwrap_err()
                .code(),
            afs_error::NODE_TRANSFER_CORRUPT_DATA
        );
    }

    #[test]
    fn wi6_aligned_range_rejects_bad_bounds_and_exhausted_budget_before_read() {
        use std::io::Write;
        let body = vec![19; 2 * OUTBOARD_GROUP_BYTES];
        let chunk = expected(&body);
        let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
        let proof = build_borrowed(&body, &budget).outboard.unwrap();
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(&body).unwrap();
        let mut out = vec![0xa5; body.len()];
        let mut work = OutboardReadWork::default();
        assert!(
            proof
                .read_ranges_into_with_work(
                    &file,
                    0,
                    &chunk,
                    &[VerifiedRangeCopy {
                        chunk_offset: 0,
                        length: body.len(),
                        output_offset: 1
                    }],
                    &mut out,
                    &mut work
                )
                .is_err()
        );
        assert_eq!(work.read_calls, 0);
        assert!(out.iter().all(|byte| *byte == 0xa5));
        let held = budget.try_reserve(budget.limit() - budget.used()).unwrap();
        assert_eq!(
            proof
                .read_ranges_into_with_work(
                    &file,
                    0,
                    &chunk,
                    &[VerifiedRangeCopy {
                        chunk_offset: 0,
                        length: body.len(),
                        output_offset: 0
                    }],
                    &mut out,
                    &mut work
                )
                .unwrap_err()
                .code(),
            afs_error::IO_CAPACITY
        );
        assert_eq!(work.read_calls, 0);
        assert!(out.iter().all(|byte| *byte == 0xa5));
        drop(held);
        file.set_len(body.len() as u64 - 1).unwrap();
        assert_eq!(
            proof
                .read_ranges_into(
                    &file,
                    0,
                    &chunk,
                    &[VerifiedRangeCopy {
                        chunk_offset: 0,
                        length: body.len(),
                        output_offset: 0
                    }],
                    &mut out
                )
                .unwrap_err()
                .code(),
            afs_error::NODE_TRANSFER_CORRUPT_DATA
        );
    }

    #[test]
    fn wi6_thousand_random_ranges_match_body_with_nonzero_file_offset() {
        use std::io::Write;
        let body = (0..150_001).map(|n| (n % 251) as u8).collect::<Vec<_>>();
        let chunk = expected(&body);
        let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
        let proof = build_borrowed(&body, &budget).outboard.unwrap();
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(&vec![0xa5; 4096]).unwrap();
        file.write_all(&body).unwrap();
        file.write_all(b"trailer").unwrap();
        let mut seed = 0x3ce6_72d9_018a_c45b_u64;
        for _ in 0..1000 {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            let offset = seed % (body.len() as u64 + 100);
            let length = ((seed >> 19) % 8192) as usize;
            let mut out = vec![0xa5; length + 11];
            let counts = proof
                .read_ranges_into(
                    &file,
                    4096,
                    &chunk,
                    &[VerifiedRangeCopy {
                        chunk_offset: offset,
                        length,
                        output_offset: 7,
                    }],
                    &mut out,
                )
                .unwrap();
            let allowed = (body.len() as u64)
                .saturating_sub(offset)
                .min(length as u64) as usize;
            assert_eq!(counts, vec![allowed]);
            if allowed != 0 {
                assert_eq!(
                    &out[7..7 + allowed],
                    &body[offset as usize..offset as usize + allowed]
                );
            }
            assert_eq!(&out[..7], &[0xa5; 7]);
            assert!(out[7 + allowed..].iter().all(|byte| *byte == 0xa5));
        }
    }

    #[test]
    fn wi6_shape_body_and_cv_tampering_report_structured_corruption() {
        use std::io::Write;
        let body = vec![37; 150_001];
        let chunk = expected(&body);
        let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
        let proof = build_borrowed(&body, &budget).outboard.unwrap();
        for byte in [0, 8, 10, 11, 16, 24, 56, 60] {
            let mut raw = proof.encoded().to_vec();
            raw[byte] ^= 1;
            assert_eq!(
                ChunkOutboard::validate_encoded_shape(&chunk, &raw)
                    .unwrap_err()
                    .code(),
                afs_error::NODE_TRANSFER_CORRUPT_DATA
            );
        }
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(&body).unwrap();
        let mut damaged = proof.encoded().to_vec();
        damaged[64] ^= 1;
        let lease = budget
            .try_reserve(ChunkOutboard::resident_charge(damaged.capacity()).unwrap())
            .unwrap();
        let damaged = ChunkOutboard::from_encoded(&chunk, damaged, lease).unwrap();
        let mut out = [0xa5; 17];
        assert_eq!(
            damaged
                .read_ranges_into(
                    &file,
                    0,
                    &chunk,
                    &[VerifiedRangeCopy {
                        chunk_offset: 3,
                        length: 17,
                        output_offset: 0
                    }],
                    &mut out
                )
                .unwrap_err()
                .code(),
            afs_error::NODE_TRANSFER_CORRUPT_DATA
        );
        assert_eq!(out, [0xa5; 17]);
        #[cfg(unix)]
        std::os::unix::fs::FileExt::write_at(&file, b"X", 3).unwrap();
        let mut work = OutboardReadWork::default();
        assert_eq!(
            proof
                .read_ranges_into_with_work(
                    &file,
                    0,
                    &chunk,
                    &[VerifiedRangeCopy {
                        chunk_offset: 3,
                        length: 17,
                        output_offset: 0
                    }],
                    &mut out,
                    &mut work
                )
                .unwrap_err()
                .code(),
            afs_error::NODE_TRANSFER_CORRUPT_DATA
        );
        assert_eq!(work.hash_bytes, OUTBOARD_GROUP_BYTES as u64);
        assert!(work.parent_hash_bytes > 0);
        assert_eq!(out, [0xa5; 17]);
        let mut validating = OutboardBuilder::validating(chunk, damaged, budget);
        let result = validating.update(&body).and_then(|_| validating.finish());
        assert_eq!(
            result.unwrap_err().code(),
            afs_error::NODE_TRANSFER_CORRUPT_DATA
        );
    }

    #[test]
    fn wi6_budget_zero_large_objects_and_failed_streams_keep_safe_root_fallback() {
        for length in [0, 100_001, 4 * 1024 * 1024 + 1] {
            let body = vec![13; length];
            let budget = OutboardBudget::new(0);
            let mut builder = OutboardBuilder::new(length as u64, budget.clone());
            for frame in body.chunks(4099) {
                builder.update(frame).unwrap();
            }
            let result = builder.finish().unwrap();
            assert_eq!(result.digest.bytes, *blake3::hash(&body).as_bytes());
            assert!(result.outboard.is_none());
            assert_eq!(budget.used(), 0);
        }
        let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
        let mut short = OutboardBuilder::new(4, budget.clone());
        short.update(b"abc").unwrap();
        assert!(short.finish().is_err());
        let mut long = OutboardBuilder::new(3, budget.clone());
        long.update(b"abc").unwrap();
        assert!(long.update(b"d").is_err());
        assert!(long.finish().is_err());
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn wi6_noncontiguous_and_unaligned_ranges_keep_verified_fallback() {
        use std::io::Write;
        let body = (0..3 * OUTBOARD_GROUP_BYTES + 17)
            .map(|n| (n % 251) as u8)
            .collect::<Vec<_>>();
        let chunk = expected(&body);
        let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
        let proof = build_borrowed(&body, &budget).outboard.unwrap();
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(&body).unwrap();
        let ranges = [
            VerifiedRangeCopy {
                chunk_offset: 3,
                length: 21,
                output_offset: 2,
            },
            VerifiedRangeCopy {
                chunk_offset: (2 * OUTBOARD_GROUP_BYTES + 3) as u64,
                length: 17,
                output_offset: 40,
            },
        ];
        let mut out = [0xa5; 64];
        let mut work = OutboardReadWork::default();
        assert_eq!(
            proof
                .read_ranges_into_with_work(&file, 0, &chunk, &ranges, &mut out, &mut work)
                .unwrap(),
            vec![21, 17]
        );
        assert_eq!(&out[2..23], &body[3..24]);
        assert_eq!(
            &out[40..57],
            &body[2 * OUTBOARD_GROUP_BYTES + 3..2 * OUTBOARD_GROUP_BYTES + 20]
        );
        assert!(
            out[..2]
                .iter()
                .chain(out[23..40].iter())
                .chain(out[57..].iter())
                .all(|byte| *byte == 0xa5)
        );
        assert_eq!(work.hash_bytes, (2 * OUTBOARD_GROUP_BYTES) as u64);
        assert_eq!(work.read_calls, 2);
    }

    #[test]
    fn r8_unaligned_mebibyte_reads_middle_once_with_bounded_edges() {
        use std::io::Write;
        let body: Vec<u8> = (0..4 * 1024 * 1024).map(|n| (n % 251) as u8).collect();
        let chunk = expected(&body);
        let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
        let proof = build_borrowed(&body, &budget).outboard.unwrap();
        let retained = budget.used();
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(&[0xa5; 4096]).unwrap();
        file.write_all(&body).unwrap();
        let length = 1024 * 1024;
        let mut out = vec![0xa5; length + 14];
        let mut work = OutboardReadWork::default();
        assert_eq!(
            proof
                .read_ranges_into_with_work(
                    &file,
                    4096,
                    &chunk,
                    &[VerifiedRangeCopy {
                        chunk_offset: 7,
                        length,
                        output_offset: 7
                    }],
                    &mut out,
                    &mut work,
                )
                .unwrap(),
            vec![length]
        );
        assert_eq!(&out[7..7 + length], &body[7..7 + length]);
        assert!(
            out[..7]
                .iter()
                .chain(out[7 + length..].iter())
                .all(|byte| *byte == 0xa5)
        );
        let covered = (length + OUTBOARD_GROUP_BYTES) as u64;
        assert_eq!(
            work.hash_bytes, covered,
            "both partial edge groups still authenticate completely"
        );
        assert_eq!(
            budget.used(),
            retained,
            "range scratch retires before return"
        );
        eprintln!(
            "R8 unaligned: reads={} hash={} copy={} scratch={}",
            work.read_calls, work.hash_bytes, work.copied_bytes, work.scratch_peak
        );
        assert!(
            work.read_calls <= 3,
            "one middle read plus at most two edge reads"
        );
        assert!(
            work.copied_bytes <= covered + (2 * OUTBOARD_GROUP_BYTES) as u64,
            "codec encode/decode cannot add two complete coverage copies"
        );
        assert!(work.scratch_peak <= RANGE_WORKING_BYTES);
    }

    #[test]
    fn r8_noncontiguous_large_interiors_scale_reads_with_ranges_not_groups() {
        use std::io::Write;
        let body: Vec<u8> = (0..4 * 1024 * 1024).map(|n| (n % 251) as u8).collect();
        let chunk = expected(&body);
        let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
        let proof = build_borrowed(&body, &budget).outboard.unwrap();
        let retained = budget.used();
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(&body).unwrap();
        let length = 512 * 1024;
        let second_source = 2 * 1024 * 1024 + 11;
        let second_output = length + 19;
        let ranges = [
            VerifiedRangeCopy {
                chunk_offset: 7,
                length,
                output_offset: 7,
            },
            VerifiedRangeCopy {
                chunk_offset: second_source as u64,
                length,
                output_offset: second_output,
            },
        ];
        let mut out = vec![0xa5; second_output + length + 7];
        let mut work = OutboardReadWork::default();
        assert_eq!(
            proof
                .read_ranges_into_with_work(&file, 0, &chunk, &ranges, &mut out, &mut work,)
                .unwrap(),
            vec![length, length]
        );
        assert_eq!(&out[7..7 + length], &body[7..7 + length]);
        assert_eq!(
            &out[second_output..second_output + length],
            &body[second_source..second_source + length]
        );
        assert!(
            out[..7]
                .iter()
                .chain(out[7 + length..second_output].iter())
                .chain(out[second_output + length..].iter())
                .all(|byte| *byte == 0xa5)
        );
        let covered = (2 * (length + OUTBOARD_GROUP_BYTES)) as u64;
        assert_eq!(work.hash_bytes, covered);
        assert_eq!(budget.used(), retained);
        eprintln!(
            "R8 multi-range: reads={} hash={} copy={} scratch={}",
            work.read_calls, work.hash_bytes, work.copied_bytes, work.scratch_peak
        );
        assert!(
            work.read_calls <= 6,
            "two middle reads plus at most four edge reads"
        );
        assert!(work.copied_bytes <= covered + (4 * OUTBOARD_GROUP_BYTES) as u64);
        assert!(work.scratch_peak <= RANGE_WORKING_BYTES);
    }

    #[test]
    fn r8_aliasing_outputs_preserve_coverage_group_write_order() {
        use std::io::Write;
        let body: Vec<u8> = (0..4 * OUTBOARD_GROUP_BYTES + 17)
            .map(|n| (n % 251) as u8)
            .collect();
        let chunk = expected(&body);
        let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
        let proof = build_borrowed(&body, &budget).outboard.unwrap();
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(&body).unwrap();
        let ranges = [
            VerifiedRangeCopy {
                chunk_offset: (OUTBOARD_GROUP_BYTES + 1) as u64,
                length: OUTBOARD_GROUP_BYTES,
                output_offset: 7,
            },
            VerifiedRangeCopy {
                chunk_offset: 3,
                length: OUTBOARD_GROUP_BYTES / 2,
                output_offset: 7,
            },
        ];
        let mut out = vec![0xa5; OUTBOARD_GROUP_BYTES + 14];
        let mut work = OutboardReadWork::default();
        assert_eq!(
            proof
                .read_ranges_into_with_work(&file, 0, &chunk, &ranges, &mut out, &mut work,)
                .unwrap(),
            vec![OUTBOARD_GROUP_BYTES, OUTBOARD_GROUP_BYTES / 2]
        );
        // Group 0 writes the second mapping first. Groups 1/2 overwrite it
        // with the first mapping, despite the second mapping's later position.
        assert_eq!(
            &out[7..7 + OUTBOARD_GROUP_BYTES],
            &body[OUTBOARD_GROUP_BYTES + 1..2 * OUTBOARD_GROUP_BYTES + 1]
        );
        assert!(
            out[..7]
                .iter()
                .chain(out[7 + OUTBOARD_GROUP_BYTES..].iter())
                .all(|byte| *byte == 0xa5)
        );
        assert_eq!(work.hash_bytes, (3 * OUTBOARD_GROUP_BYTES) as u64);
        assert!(work.scratch_peak <= RANGE_WORKING_BYTES);
    }

    #[test]
    fn r8_private_edges_authenticate_padding_and_bound_short_tail_before_delivery() {
        use std::{io::Write, os::unix::fs::FileExt};
        let body: Vec<u8> = (0..3 * OUTBOARD_GROUP_BYTES + 37)
            .map(|n| (n % 251) as u8)
            .collect();
        let chunk = expected(&body);
        let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
        let proof = build_borrowed(&body, &budget).outboard.unwrap();
        let retained = budget.used();
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(&body).unwrap();
        let length = 2 * OUTBOARD_GROUP_BYTES + 23;
        for corrupt_offset in [0, OUTBOARD_GROUP_BYTES + 17, 2 * OUTBOARD_GROUP_BYTES + 31] {
            file.write_all_at(&body, 0).unwrap();
            file.write_all_at(&[body[corrupt_offset] ^ 1], corrupt_offset as u64)
                .unwrap();
            let mut private = vec![0xa5; length + 14];
            let error = proof
                .read_ranges_into_with_work(
                    &file,
                    0,
                    &chunk,
                    &[VerifiedRangeCopy {
                        chunk_offset: 7,
                        length,
                        output_offset: 7,
                    }],
                    &mut private,
                    &mut OutboardReadWork::default(),
                )
                .unwrap_err();
            assert_eq!(error.code(), afs_error::NODE_TRANSFER_CORRUPT_DATA);
            assert!(
                private[..7]
                    .iter()
                    .chain(private[7 + length..].iter())
                    .all(|byte| *byte == 0xa5)
            );
            assert_eq!(budget.used(), retained);
        }
        file.write_all_at(&body, 0).unwrap();
        let tail_offset = 3;
        let allowed = body.len() - tail_offset;
        let mut private = vec![0xa5; allowed + 14];
        let mut work = OutboardReadWork::default();
        assert_eq!(
            proof
                .read_ranges_into_with_work(
                    &file,
                    0,
                    &chunk,
                    &[VerifiedRangeCopy {
                        chunk_offset: tail_offset as u64,
                        length: 4 * OUTBOARD_GROUP_BYTES,
                        output_offset: 7
                    }],
                    &mut private,
                    &mut work,
                )
                .unwrap(),
            vec![allowed]
        );
        assert_eq!(&private[7..7 + allowed], &body[tail_offset..]);
        assert!(
            private[..7]
                .iter()
                .chain(private[7 + allowed..].iter())
                .all(|byte| *byte == 0xa5)
        );
        assert_eq!(
            work.hash_bytes,
            body.len() as u64,
            "the short final coverage group remains complete after a long interior"
        );
        assert!(work.read_calls <= 2);
        assert!(work.scratch_peak <= RANGE_WORKING_BYTES);
        let mut rejected = vec![0xa5; allowed];
        let mut work = OutboardReadWork::default();
        assert!(
            proof
                .read_ranges_into_with_work(
                    &file,
                    0,
                    &chunk,
                    &[VerifiedRangeCopy {
                        chunk_offset: tail_offset as u64,
                        length: 4 * OUTBOARD_GROUP_BYTES,
                        output_offset: 1
                    }],
                    &mut rejected,
                    &mut work,
                )
                .is_err()
        );
        assert_eq!(work.read_calls, 0);
        assert!(rejected.iter().all(|byte| *byte == 0xa5));
        assert_eq!(budget.used(), retained);
    }

    #[test]
    fn r8_source_aliases_keep_codec_verification_for_each_destination() {
        use std::io::Write;
        let body: Vec<u8> = (0..4 * OUTBOARD_GROUP_BYTES)
            .map(|n| (n % 251) as u8)
            .collect();
        let chunk = expected(&body);
        let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
        let proof = build_borrowed(&body, &budget).outboard.unwrap();
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(&body).unwrap();
        let second_output = OUTBOARD_GROUP_BYTES + 32;
        let ranges = [
            VerifiedRangeCopy {
                chunk_offset: 7,
                length: OUTBOARD_GROUP_BYTES + 17,
                output_offset: 0,
            },
            VerifiedRangeCopy {
                chunk_offset: (OUTBOARD_GROUP_BYTES / 2) as u64,
                length: OUTBOARD_GROUP_BYTES,
                output_offset: second_output,
            },
        ];
        let mut out = vec![0xa5; second_output + OUTBOARD_GROUP_BYTES + 7];
        let mut work = OutboardReadWork::default();
        assert_eq!(
            proof
                .read_ranges_into_with_work(&file, 0, &chunk, &ranges, &mut out, &mut work,)
                .unwrap(),
            vec![OUTBOARD_GROUP_BYTES + 17, OUTBOARD_GROUP_BYTES]
        );
        assert_eq!(
            &out[..OUTBOARD_GROUP_BYTES + 17],
            &body[7..OUTBOARD_GROUP_BYTES + 24]
        );
        assert_eq!(
            &out[second_output..second_output + OUTBOARD_GROUP_BYTES],
            &body[OUTBOARD_GROUP_BYTES / 2..3 * OUTBOARD_GROUP_BYTES / 2]
        );
        assert!(
            out[OUTBOARD_GROUP_BYTES + 17..second_output]
                .iter()
                .chain(out[second_output + OUTBOARD_GROUP_BYTES..].iter())
                .all(|byte| *byte == 0xa5)
        );
        assert_eq!(
            work.read_calls, 2,
            "shared source groups remain deduplicated"
        );
        assert_eq!(work.hash_bytes, (2 * OUTBOARD_GROUP_BYTES) as u64);
        assert!(work.scratch_peak <= RANGE_WORKING_BYTES);
    }

    #[test]
    fn wi6_peer_tree_eligibility_is_frozen_before_any_body_and_rejects_root_fallback() {
        let body = vec![37; 2 * OUTBOARD_GROUP_BYTES];
        let chunk = expected(&body);
        let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
        let proof = build_borrowed(&body, &budget).outboard.unwrap();
        let mut builder = OutboardBuilder::validating(chunk.clone(), proof.clone(), budget.clone());
        assert!(builder.has_peer_tree());
        builder.update(&body[..1]).unwrap();
        assert!(!builder.has_peer_tree());
        builder.update(&body[1..]).unwrap();
        assert_eq!(builder.finish().unwrap().digest, chunk.content_digest);
        let fallback =
            OutboardBuilder::validating(chunk.clone(), proof.clone(), OutboardBudget::new(0));
        assert!(!fallback.has_peer_tree());
        assert!(!OutboardBuilder::new(body.len() as u64, budget.clone()).has_peer_tree());
        let mut wrong = chunk.clone();
        wrong.content_digest.bytes[0] ^= 1;
        assert!(!OutboardBuilder::validating(wrong, proof.clone(), budget.clone()).has_peer_tree());
        let mut failed = OutboardBuilder::validating(chunk, proof, budget);
        assert!(failed.update(&vec![0; body.len() + 1]).is_err());
        assert!(!failed.has_peer_tree());
    }

    #[test]
    fn wi6_incremental_builder_accepts_borrowed_frames_without_rescan() {
        let body = vec![37; 150_001];
        let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
        let mut builder = OutboardBuilder::new(body.len() as u64, budget.clone());
        for frame in body.chunks(173) {
            builder.update(frame).unwrap();
        }
        let built = builder.finish().unwrap();
        assert_eq!(built.digest.bytes, *blake3::hash(&body).as_bytes());
        assert!(built.outboard.is_some());
    }
    #[test]
    fn pr60_resident_mark_is_shared_until_last_owner_and_not_a_second_pool() {
        let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
        let body = vec![31; 64 * 1024];
        let proof = build_borrowed(&body, &budget).outboard.unwrap();
        let charge = proof.charged_bytes();
        assert_eq!(budget.resident_used(), 0);
        assert_eq!(budget.used(), charge);
        let barrier = Arc::new(std::sync::Barrier::new(17));
        let mut threads = Vec::new();
        for _ in 0..16 {
            let proof = proof.clone();
            let barrier = barrier.clone();
            threads.push(std::thread::spawn(move || {
                barrier.wait();
                proof.try_mark_resident()
            }));
        }
        barrier.wait();
        let admitted: Vec<_> = threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect();
        assert!(admitted.into_iter().all(|value| value));
        assert_eq!(
            budget.resident_used(),
            charge,
            "one allocation, one resident admission"
        );
        assert_eq!(
            budget.used(),
            charge,
            "promotion cannot double-charge total"
        );
        let last = proof.clone();
        drop(proof);
        assert_eq!(budget.resident_used(), charge);
        drop(last);
        assert_eq!(budget.resident_used(), 0);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn pr60_resident_lease_resize_and_rejection_keep_one_exact_total() {
        let budget = OutboardBudget::new(1024);
        let mut resident = budget.try_reserve_resident(512).unwrap();
        assert_eq!((budget.used(), budget.resident_used()), (512, 512));
        assert!(budget.try_reserve_resident(1).is_none());
        assert_eq!((budget.used(), budget.resident_used()), (512, 512));
        let working = budget.try_reserve(512).unwrap();
        assert_eq!((budget.used(), budget.resident_used()), (1024, 512));
        drop(working);
        resident.retain_capacity(256).unwrap();
        assert_eq!((budget.used(), budget.resident_used()), (256, 256));
        resident.retain_capacity(384).unwrap();
        assert_eq!((budget.used(), budget.resident_used()), (384, 384));
        assert!(resident.retain_capacity(513).is_err());
        assert_eq!((budget.used(), budget.resident_used()), (384, 384));
        drop(resident);
        assert_eq!((budget.used(), budget.resident_used()), (0, 0));
    }

    #[test]
    fn pr60_range_work_ticket_keeps_ledger_identity_and_is_not_reserved_twice() {
        let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
        let body = vec![43; 64 * 1024];
        let chunk = expected(&body);
        let proof = build_borrowed(&body, &budget).outboard.unwrap();
        let file = tempfile::tempfile().unwrap();
        use std::os::unix::fs::FileExt;
        file.write_all_at(&body, 0).unwrap();
        let range = [VerifiedRangeCopy {
            chunk_offset: 0,
            output_offset: 0,
            length: OUTBOARD_GROUP_BYTES,
        }];
        let mut out = vec![0; OUTBOARD_GROUP_BYTES];
        let foreign = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
        let work = foreign.reserve_range_work(&range, chunk.length).unwrap();
        let mut observed = OutboardReadWork::default();
        assert!(
            proof
                .read_ranges_into_with_reserved_work(
                    &file,
                    0,
                    &chunk,
                    &range,
                    &mut out,
                    OutboardReadContext {
                        work: &mut observed,
                        working: work
                    },
                )
                .is_err()
        );
        assert_eq!(observed.read_calls, 0);
        assert_eq!(foreign.used(), 0);
        let undersized = budget.try_reserve(1).unwrap();
        assert!(
            proof
                .read_ranges_into_with_reserved_work(
                    &file,
                    0,
                    &chunk,
                    &range,
                    &mut out,
                    OutboardReadContext {
                        work: &mut observed,
                        working: undersized
                    },
                )
                .is_err()
        );
        let ticket = budget.reserve_range_work(&range, chunk.length).unwrap();
        assert_eq!(ticket.bytes, ALIGNED_RANGE_WORKING_BYTES);
        let pressure = budget.try_reserve(budget.limit() - budget.used()).unwrap();
        assert_eq!(budget.used(), budget.limit());
        proof
            .read_ranges_into_with_reserved_work(
                &file,
                0,
                &chunk,
                &range,
                &mut out,
                OutboardReadContext {
                    work: &mut observed,
                    working: ticket,
                },
            )
            .unwrap();
        assert_eq!(out, body[..OUTBOARD_GROUP_BYTES]);
        assert!(observed.hash_bytes > 0);
        drop(pressure);
        drop(proof);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn pr60_ledger_metrics_do_not_call_read_builder_fallback_a_native_write() {
        let registry = afs_metrics::Registry::new();
        let metrics = super::super::super::read_work::ReadWorkMetrics::register(&registry).unwrap();
        let budget = OutboardBudget::new_with_metrics(1024, metrics);
        let resident = budget.try_reserve_resident(512).unwrap();
        let working = budget.try_reserve(256).unwrap();
        let built = build_borrowed(&[11; 4096], &budget);
        assert!(built.outboard.is_none());
        let text = afs_metrics::encode_text(&registry).unwrap();
        assert!(text.contains("afs_dfs_outboard_events_total{event=\"write_without_proof\"} 0\n"));
        assert!(text.contains("afs_dfs_outboard_metadata_bytes 768\n"));
        assert!(text.contains("afs_dfs_outboard_resident_bytes 512\n"));
        assert!(budget.try_reserve_resident(1).is_none());
        assert!(budget.try_reserve(1024).is_none());
        budget.record_write_without_proof();
        budget.record_range_fullscan_fallback();
        budget.record_range_fullscan_unretained();
        drop(working);
        drop(resident);
        let text = afs_metrics::encode_text(&registry).unwrap();
        assert!(text.contains("afs_dfs_outboard_metadata_bytes 0\n"));
        assert!(text.contains("afs_dfs_outboard_resident_bytes 0\n"));
        for event in [
            "reserve_resident_failed",
            "write_without_proof",
            "range_fullscan_fallback",
            "range_fullscan_unretained",
        ] {
            assert!(
                text.contains(&format!(
                    "afs_dfs_outboard_events_total{{event=\"{event}\"}} 1\n"
                )),
                "{text}"
            );
        }
    }
}
