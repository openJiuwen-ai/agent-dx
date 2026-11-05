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
    test_hooks: RangeWorkTestHooks,
}

#[cfg(test)]
#[derive(Debug, Default)]
struct RangeWorkTestHooks {
    pub(super) waiters: usize,
    pub(super) before_wait: Option<(
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
    metrics: Option<super::super::metrics::work::ReadWorkMetrics>,
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
        metrics: super::super::metrics::work::ReadWorkMetrics,
    ) -> Arc<Self> {
        Self::create(limit, Some(metrics), DEFAULT_RANGE_WORK_WAIT)
    }
    #[cfg(feature = "dfs")]
    pub(crate) fn new_with_metrics_and_work_wait(
        limit: usize,
        metrics: super::super::metrics::work::ReadWorkMetrics,
        work_wait: Duration,
    ) -> Arc<Self> {
        Self::create(limit, Some(metrics), work_wait)
    }
    fn create(
        limit: usize,
        metrics: Option<super::super::metrics::work::ReadWorkMetrics>,
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
            if let Some((checked, resume)) = state.test_hooks.before_wait.take() {
                checked.send(()).ok()?;
                resume.recv().ok()?;
            }
            let remaining = deadline.checked_duration_since(Instant::now())?;
            #[cfg(test)]
            {
                state.test_hooks.waiters += 1;
            }
            // release/stop take this same mutex. They cannot notify between
            // our capacity check and the atomic unlock-and-wait operation.
            let (next, _) = self.work_changed.wait_timeout(state, remaining).ok()?;
            state = next;
            #[cfg(test)]
            {
                state.test_hooks.waiters -= 1;
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
const MAGIC: &[u8; 8] = &crate::format::NODE_CHUNK_SIDECAR_MAGIC;
const VERSION: u16 = crate::format::version::NODE_CHUNK_SIDECAR;
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
        // Version first. A sidecar is derived data (`Disposition::Discard`):
        // another version stays a corruption-class error, so every caller
        // keeps today's handling — a local reader discards it, verifies the
        // body in full and rewrites the sidecar; a peer proof is refused.
        if encoded.len() >= 10 && &encoded[..8] == MAGIC {
            let found = u16::from_le_bytes([encoded[8], encoded[9]]);
            if found != VERSION {
                return Err(super::corrupt(
                    &expected.id,
                    &crate::format::message(
                        crate::format::FormatKind::NodeChunkSidecar,
                        "header",
                        u64::from(found),
                    ),
                ));
            }
        }
        if expected.length > MAX_OBJECT_BYTES
            || encoded.len() < HEADER
            || encoded.len() > MAX_ENCODED_OUTBOARD_BYTES
            || &encoded[..8] != MAGIC
            || encoded[8..10] != VERSION.to_le_bytes()
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
        let (groups, counts) = self.range_groups(ranges, out.len())?;
        let observed = RefCell::new(OutboardReadWork::default());
        let source = OffsetFile {
            file,
            base: body_start,
            length: self.length,
            work: &observed,
        };
        let root = blake3::Hash::from(self.digest.bytes);
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
                let result = self.verify_aligned_range(&source, root, range, *allowed, stop, out);
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
            let result = self.verify_private_ranges(&source, root, ranges, &counts, &groups, out);
            work.add(&observed.into_inner());
            return result.map(|()| counts);
        }
        let mut encoded = Vec::new();
        encoded
            .try_reserve_exact(OUTBOARD_GROUP_BYTES + MAX_ENCODED_OUTBOARD_BYTES)
            .map_err(|_| capacity())?;
        let result = self.verify_by_reencode(&source, ranges, &counts, groups, &mut encoded, out);
        work.add(&observed.into_inner());
        result.map(|()| counts)
    }

    /// Per-range allowed lengths and the coverage groups they touch.
    fn range_groups(
        &self,
        ranges: &[VerifiedRangeCopy],
        out_len: usize,
    ) -> Result<([bool; 128], Vec<usize>)> {
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
                .filter(|end| *end <= out_len)
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
        Ok((groups, counts))
    }

    /// One group-aligned range validated in place from the private output.
    fn verify_aligned_range(
        &self,
        source: &OffsetFile<'_>,
        root: blake3::Hash,
        range: &VerifiedRangeCopy,
        allowed: usize,
        stop: u64,
        out: &mut [u8],
    ) -> Result<()> {
        let bao = PreOrderMemOutboard {
            root,
            tree: tree(self.length),
            data: self.raw_cv_bytes(),
        };
        let bytes = &mut out[range.output_offset..range.output_offset + allowed];
        source
            .read_exact_at(range.chunk_offset, bytes)
            .map_err(Error::from)?;
        source.work.borrow_mut().scratch_peak = OUTBOARD_GROUP_BYTES;
        let input = BorrowedRange {
            bytes,
            start: range.chunk_offset,
            work: source.work,
        };
        let checked = ObservedOutboard {
            inner: bao,
            work: source.work,
        };
        let query = ChunkRanges::from(ChunkNum(range.chunk_offset / 1024)..ChunkNum::chunks(stop));
        let verify_started = Instant::now();
        let verified =
            bao_tree::io::sync::encode_ranges_validated(input, checked, &query, io::sink())
                .map_err(|error| match error {
                    bao_tree::io::EncodeError::Io(error) => Error::from(error),
                    other => bad_proof(&format!("Bao range verification failed: {other}")),
                });
        source.work.borrow_mut().verify_ns += nanos(verify_started);
        verified
    }

    /// Requested bytes filled once; each contiguous group run validated over
    /// the private results plus file-backed edges.
    fn verify_private_ranges(
        &self,
        source: &OffsetFile<'_>,
        root: blake3::Hash,
        ranges: &[VerifiedRangeCopy],
        counts: &[usize],
        groups: &[bool; 128],
        out: &mut [u8],
    ) -> Result<()> {
        for (range, allowed) in ranges.iter().zip(counts) {
            if *allowed != 0 {
                source
                    .read_exact_at(
                        range.chunk_offset,
                        &mut out[range.output_offset..range.output_offset + *allowed],
                    )
                    .map_err(Error::from)?;
            }
        }
        source.work.borrow_mut().scratch_peak = OUTBOARD_GROUP_BYTES;
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
                work: source.work,
            };
            let input = PrivateRanges {
                source,
                bytes: out,
                ranges,
                counts,
            };
            let verify_started = Instant::now();
            let verified =
                bao_tree::io::sync::encode_ranges_validated(input, checked, &query, io::sink())
                    .map_err(|error| match error {
                        bao_tree::io::EncodeError::Io(error) => Error::from(error),
                        other => bad_proof(&format!("Bao range verification failed: {other}")),
                    });
            source.work.borrow_mut().verify_ns += nanos(verify_started);
            verified?;
        }
        Ok(())
    }

    /// The original codec: re-encode each selected group and decode-verify it
    /// into the requested output ranges.
    fn verify_by_reencode(
        &self,
        source: &OffsetFile<'_>,
        ranges: &[VerifiedRangeCopy],
        counts: &[usize],
        groups: [bool; 128],
        encoded: &mut Vec<u8>,
        out: &mut [u8],
    ) -> Result<()> {
        let root = blake3::Hash::from(self.digest.bytes);
        let bao = PreOrderMemOutboard {
            root,
            tree: tree(self.length),
            data: self.raw_cv_bytes(),
        };
        for (group, selected) in groups.into_iter().enumerate() {
            if !selected {
                continue;
            }
            let start = group as u64 * OUTBOARD_GROUP_BYTES as u64;
            let stop = (start + OUTBOARD_GROUP_BYTES as u64).min(self.length);
            let query = ChunkRanges::from(ChunkNum(start / 1024)..ChunkNum::chunks(stop));
            encoded.clear();
            source.work.borrow_mut().scratch_peak = encoded.capacity() + OUTBOARD_GROUP_BYTES;
            bao_tree::io::sync::encode_ranges(source, &bao, &query, &mut *encoded)
                .map_err(|error| Error::from(io::Error::from(error)))?;
            source.work.borrow_mut().copied_bytes += stop - start;
            let verify_started = Instant::now();
            let verified = (|| {
                for item in DecodeResponseIter::new(root, tree(self.length), &encoded[..], &query) {
                    let item = match item {
                        Ok(item) => item,
                        Err(error) => {
                            if matches!(error, bao_tree::io::DecodeError::ParentHashMismatch(_)) {
                                source.work.borrow_mut().parent_hash_bytes += 64;
                            }
                            if matches!(error, bao_tree::io::DecodeError::LeafHashMismatch(_)) {
                                let mut work = source.work.borrow_mut();
                                work.hash_bytes += stop - start;
                                work.copied_bytes += stop - start;
                            }
                            return Err(bad_proof(&format!(
                                "Bao response verification failed: {error}"
                            )));
                        }
                    };
                    if matches!(item, BaoContentItem::Parent(_)) {
                        source.work.borrow_mut().parent_hash_bytes += 64;
                    }
                    if let BaoContentItem::Leaf(leaf) = item {
                        source.work.borrow_mut().hash_bytes += leaf.data.len() as u64;
                        source.work.borrow_mut().copied_bytes += leaf.data.len() as u64;
                        let leaf_end = leaf.offset + leaf.data.len() as u64;
                        for (range, allowed) in ranges.iter().zip(counts) {
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
                                source.work.borrow_mut().copied_bytes += n as u64;
                            }
                        }
                    }
                }
                Ok(())
            })();
            source.work.borrow_mut().verify_ns += nanos(verify_started);
            verified?;
        }
        Ok(())
    }
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
                encoded[8..10].copy_from_slice(&VERSION.to_le_bytes());
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
mod tests;
