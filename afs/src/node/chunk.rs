//! Crash-safe local storage for immutable DFS chunks.
//!
//! A batch becomes durable in three ordered barriers: every staging file is
//! synced, final names are installed without replacement and the chunk
//! directory is synced, then one LocalCatalog transaction is appended and
//! synced. Only the catalog revision produced by the final barrier may appear
//! in a `ReplicaAck`.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    fs::{self, File, OpenOptions},
    hash::{Hash, Hasher},
    io::{BufRead, BufReader, Write},
    ops::{Bound, Deref, RangeBounds},
    path::{Path, PathBuf},
    sync::{
        Arc, Condvar, Mutex, Weak,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

mod catalog_commit;
mod gc;
#[cfg(any(feature = "dfs", test))]
mod native_live;
#[cfg(test)]
mod native_live_tests;
pub mod outboard;
use catalog_commit::CatalogCommitter;
#[cfg(feature = "dfs")]
pub(crate) use native_live::NativeWriteBudgetSnapshot;
#[cfg(any(feature = "dfs", test))]
pub(crate) use native_live::{
    NativeWriteAdmission, NativeWriteBudget, NativeWriteLease, NativeWriteOwner,
};
pub use outboard::OutboardBudget;
use outboard::{
    ChunkOutboard, DEFAULT_OUTBOARD_BUDGET_BYTES, MAX_ENCODED_OUTBOARD_BYTES, OutboardBuilder,
    OutboardLease, OutboardReadContext, OutboardReadWork, SharedOutboard, SidecarOutboard,
};

use afs_error::{Error, Result};
use afs_tracing::tracing;
use serde::{Deserialize, Serialize};
#[cfg(unix)]
use std::os::unix::fs::FileExt;

#[cfg(test)]
use crate::dfs::ReplicaGroupId;
use crate::{
    dfs::{
        ChunkEncoding, ChunkId, ChunkObject, ChunkReceipt, ContentDigest, DigestAlgorithm,
        OperationId, ReplicaAck, ReplicaTarget, StorageDeviceDescriptor,
    },
    node::{storage::localfs::capacity_from_statvfs_fd, vfs::types::FilesystemCapacity},
};

// A generation identifies one checkout, even when jemalloc or the idle pool
// reuses the same address. It is diagnostic identity, never persistent identity.
static WRITE_ALLOCATION_GENERATION: AtomicU64 = AtomicU64::new(1);
const MAX_IDLE_WRITE_BUFFERS: usize = 1024;
const OUTBOARD_REPAIR_SHARDS: usize = 64;
type OutboardRepairGates = [Mutex<()>; OUTBOARD_REPAIR_SHARDS];

#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct WriteBufferPoolStats {
    pub live_bytes: usize,
    pub live_buffers: usize,
    pub live_peak_bytes: usize,
    pub idle_bytes: usize,
    pub idle_buffers: usize,
    pub allocations: u64,
    pub reuses: u64,
    pub released_bytes: u64,
    pub released_buffers: u64,
    pub copied_bytes: u64,
    pub copy_operations: u64,
}

#[derive(Debug, Default)]
struct WriteBufferPoolState {
    idle: Vec<Vec<u8>>,
    stats: WriteBufferPoolStats,
    stopped: bool,
    metrics: Option<WriteBufferPoolMetrics>,
    #[cfg(test)]
    fail_next_allocation: bool,
    #[cfg(test)]
    next_allocation_capacity: Option<usize>,
}

#[derive(Debug)]
struct WriteBufferPoolMetrics {
    live_bytes: afs_metrics::IntGauge,
    live_buffers: afs_metrics::IntGauge,
    live_peak_bytes: afs_metrics::IntGauge,
    idle_bytes: afs_metrics::IntGauge,
    idle_buffers: afs_metrics::IntGauge,
    allocations: afs_metrics::IntCounter,
    reuses: afs_metrics::IntCounter,
    released_bytes: afs_metrics::IntCounter,
    released_buffers: afs_metrics::IntCounter,
    copied_bytes: afs_metrics::IntCounter,
    copy_operations: afs_metrics::IntCounter,
    previous: WriteBufferPoolStats,
}

impl WriteBufferPoolMetrics {
    fn register(
        registry: &afs_metrics::Registry,
    ) -> std::result::Result<Self, afs_metrics::MetricsError> {
        let live_bytes = afs_metrics::IntGauge::new(
            "afs_dfs_write_buffer_live_bytes",
            "Actual capacity of final write allocations still held by any owner.",
        )?;
        let live_buffers = afs_metrics::IntGauge::new(
            "afs_dfs_write_buffer_live_allocations",
            "Final write allocations still held by any owner.",
        )?;
        let live_peak_bytes = afs_metrics::IntGauge::new(
            "afs_dfs_write_buffer_live_peak_bytes",
            "Peak actual live final write allocation capacity.",
        )?;
        let idle_bytes = afs_metrics::IntGauge::new(
            "afs_dfs_write_buffer_idle_bytes",
            "Actual capacity retained only by the bounded idle list.",
        )?;
        let idle_buffers = afs_metrics::IntGauge::new(
            "afs_dfs_write_buffer_idle_allocations",
            "Allocations retained only by the bounded idle list.",
        )?;
        let allocations = afs_metrics::IntCounter::new(
            "afs_dfs_write_buffer_allocations_total",
            "Successful new final write payload allocations.",
        )?;
        let reuses = afs_metrics::IntCounter::new(
            "afs_dfs_write_buffer_reuses_total",
            "Idle allocations checked out without moving payload storage.",
        )?;
        let released_bytes = afs_metrics::IntCounter::new(
            "afs_dfs_write_buffer_released_bytes_total",
            "Allocation capacity released from the pool to the allocator, not a physical RSS measurement.",
        )?;
        let released_buffers = afs_metrics::IntCounter::new(
            "afs_dfs_write_buffer_released_allocations_total",
            "Final write allocations released to the allocator.",
        )?;
        let copied_bytes = afs_metrics::IntCounter::new(
            "afs_dfs_write_buffer_input_copy_bytes_total",
            "Actual input bytes copied into final mutable write buffers.",
        )?;
        let copy_operations = afs_metrics::IntCounter::new(
            "afs_dfs_write_buffer_input_copies_total",
            "Actual slice-copy or extend entries into final mutable write buffers.",
        )?;
        afs_metrics::register_collector(registry, &live_bytes)?;
        afs_metrics::register_collector(registry, &live_buffers)?;
        afs_metrics::register_collector(registry, &live_peak_bytes)?;
        afs_metrics::register_collector(registry, &idle_bytes)?;
        afs_metrics::register_collector(registry, &idle_buffers)?;
        afs_metrics::register_collector(registry, &allocations)?;
        afs_metrics::register_collector(registry, &reuses)?;
        afs_metrics::register_collector(registry, &released_bytes)?;
        afs_metrics::register_collector(registry, &released_buffers)?;
        afs_metrics::register_collector(registry, &copied_bytes)?;
        afs_metrics::register_collector(registry, &copy_operations)?;
        Ok(Self {
            live_bytes,
            live_buffers,
            live_peak_bytes,
            idle_bytes,
            idle_buffers,
            allocations,
            reuses,
            released_bytes,
            released_buffers,
            copied_bytes,
            copy_operations,
            previous: WriteBufferPoolStats::default(),
        })
    }

    fn publish(&mut self, stats: WriteBufferPoolStats) {
        self.live_bytes
            .set(i64::try_from(stats.live_bytes).unwrap_or(i64::MAX));
        self.live_buffers
            .set(i64::try_from(stats.live_buffers).unwrap_or(i64::MAX));
        self.live_peak_bytes
            .set(i64::try_from(stats.live_peak_bytes).unwrap_or(i64::MAX));
        self.idle_bytes
            .set(i64::try_from(stats.idle_bytes).unwrap_or(i64::MAX));
        self.idle_buffers
            .set(i64::try_from(stats.idle_buffers).unwrap_or(i64::MAX));
        self.allocations
            .inc_by(stats.allocations.saturating_sub(self.previous.allocations));
        self.reuses
            .inc_by(stats.reuses.saturating_sub(self.previous.reuses));
        self.released_bytes.inc_by(
            stats
                .released_bytes
                .saturating_sub(self.previous.released_bytes),
        );
        self.released_buffers.inc_by(
            stats
                .released_buffers
                .saturating_sub(self.previous.released_buffers),
        );
        self.copied_bytes.inc_by(
            stats
                .copied_bytes
                .saturating_sub(self.previous.copied_bytes),
        );
        self.copy_operations.inc_by(
            stats
                .copy_operations
                .saturating_sub(self.previous.copy_operations),
        );
        self.previous = stats;
    }
}

impl WriteBufferPoolState {
    fn publish_metrics(&mut self) {
        if let Some(metrics) = &mut self.metrics {
            metrics.publish(self.stats);
        }
    }
}

/// Nonblocking reuse of released write allocations. The baseline bounds only
/// idle capacity; it never grants, waits for or limits live payload capacity.
#[derive(Debug)]
pub struct WriteBufferPool {
    idle_baseline_bytes: usize,
    outboard_budget: Option<Arc<OutboardBudget>>,
    state: Mutex<WriteBufferPoolState>,
}

impl WriteBufferPool {
    pub fn new(idle_baseline_bytes: usize) -> Arc<Self> {
        Arc::new(Self {
            idle_baseline_bytes,
            outboard_budget: None,
            state: Mutex::new(WriteBufferPoolState::default()),
        })
    }

    #[cfg(any(feature = "dfs", test))]
    pub(crate) fn new_with_outboard_budget(
        idle_baseline_bytes: usize,
        outboard_budget: Arc<OutboardBudget>,
    ) -> Arc<Self> {
        Arc::new(Self {
            idle_baseline_bytes,
            outboard_budget: Some(outboard_budget),
            state: Mutex::new(WriteBufferPoolState::default()),
        })
    }

    /// Register exact lifecycle counters with the process-owned registry.
    pub fn register_metrics(
        &self,
        registry: &afs_metrics::Registry,
    ) -> std::result::Result<(), afs_metrics::MetricsError> {
        let metrics = WriteBufferPoolMetrics::register(registry)?;
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.metrics = Some(metrics);
        state.publish_metrics();
        Ok(())
    }

    pub fn snapshot(&self) -> WriteBufferPoolStats {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.stats
    }

    /// Release retained idle allocations outside the pool lock. Live owners
    /// continue to own their allocation; future returns cannot enter the pool.
    pub fn stop(&self) {
        let idle = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.stopped = true;
            state.stats.released_bytes = state
                .stats
                .released_bytes
                .saturating_add(state.stats.idle_bytes as u64);
            state.stats.released_buffers = state
                .stats
                .released_buffers
                .saturating_add(state.stats.idle_buffers as u64);
            state.stats.idle_bytes = 0;
            state.stats.idle_buffers = 0;
            state.publish_metrics();
            std::mem::take(&mut state.idle)
        };
        drop(idle);
    }

    fn checkout(self: &Arc<Self>, capacity: usize) -> Result<ChunkBuffer> {
        #[cfg(any(feature = "dfs", test))]
        {
            self.checkout_with_native_lease(capacity, None)
        }
        #[cfg(not(any(feature = "dfs", test)))]
        {
            self.checkout_with_native_lease(capacity)
        }
    }

    fn checkout_with_native_lease(
        self: &Arc<Self>,
        capacity: usize,
        #[cfg(any(feature = "dfs", test))] mut native_live_lease: Option<NativeWriteLease>,
    ) -> Result<ChunkBuffer> {
        #[cfg(any(feature = "dfs", test))]
        if native_live_lease
            .as_ref()
            .is_some_and(|lease| lease.charged_bytes() < capacity)
        {
            return Err(invalid(
                "native write lease does not cover requested capacity",
            ));
        }
        {
            let mut state = self
                .state
                .lock()
                .map_err(|_| invalid("write buffer pool is poisoned"))?;
            let index = if state.stopped {
                None
            } else {
                state
                    .idle
                    .iter()
                    .enumerate()
                    .filter(|(_, bytes)| {
                        bytes.capacity() >= capacity
                            && bytes.capacity() < capacity.saturating_mul(2)
                            && {
                                #[cfg(any(feature = "dfs", test))]
                                {
                                    native_live_lease.as_ref().is_none_or(|lease| {
                                        bytes.capacity() <= lease.charged_bytes()
                                    })
                                }
                                #[cfg(not(any(feature = "dfs", test)))]
                                {
                                    true
                                }
                            }
                    })
                    .min_by_key(|(_, bytes)| bytes.capacity())
                    .map(|(index, _)| index)
            };
            if let Some(index) = index {
                let bytes = state.idle.swap_remove(index);
                state.stats.idle_bytes -= bytes.capacity();
                state.stats.idle_buffers -= 1;
                state.stats.live_bytes += bytes.capacity();
                state.stats.live_buffers += 1;
                state.stats.live_peak_bytes =
                    state.stats.live_peak_bytes.max(state.stats.live_bytes);
                state.stats.reuses += 1;
                state.publish_metrics();
                drop(state);
                #[cfg(any(feature = "dfs", test))]
                if let Some(lease) = &mut native_live_lease {
                    lease.shrink_to(bytes.capacity());
                }
                let allocation_id = WRITE_ALLOCATION_GENERATION.fetch_add(1, Ordering::Relaxed);
                afs_logging::debug!("dfs.write_buffer.checkout";
                    "event" => "checkout",
                    "reused" => true,
                    "allocation_id" => allocation_id,
                    "capacity" => bytes.capacity() as u64,
                    "body_pointer" => bytes.as_ptr() as usize,
                );
                return Ok(ChunkBuffer {
                    bytes,
                    pool: Arc::downgrade(self),
                    allocation_id,
                    #[cfg(any(feature = "dfs", test))]
                    native_live_lease,
                });
            }
            #[cfg(test)]
            if std::mem::take(&mut state.fail_next_allocation) {
                return Err(write_allocation_error());
            }
        }
        // Only metadata/list lookup is under the lock. Payload allocation and
        // allocator failure handling happen outside it.
        let mut bytes = Vec::new();
        #[cfg(test)]
        let allocation_capacity = self
            .state
            .lock()
            .map_err(|_| invalid("write buffer pool is poisoned"))?
            .next_allocation_capacity
            .take()
            .unwrap_or(capacity);
        #[cfg(not(test))]
        let allocation_capacity = capacity;
        bytes
            .try_reserve_exact(allocation_capacity)
            .map_err(|_| write_allocation_error())?;
        #[cfg(any(feature = "dfs", test))]
        if let Some(lease) = &mut native_live_lease {
            // Vec may expose more actual capacity than requested. Complete the
            // old owner's charge without waiting, before publishing the buffer.
            if !lease.try_grow_to(bytes.capacity())? {
                drop(bytes);
                return Err(Error::coded(
                    afs_error::IO_WOULD_BLOCK,
                    "native actual allocation capacity needs additional admission",
                ));
            }
            lease.shrink_to(bytes.capacity());
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| invalid("write buffer pool is poisoned"))?;
        state.stats.live_bytes += bytes.capacity();
        state.stats.live_buffers += 1;
        state.stats.live_peak_bytes = state.stats.live_peak_bytes.max(state.stats.live_bytes);
        state.stats.allocations += 1;
        state.publish_metrics();
        drop(state);
        let allocation_id = WRITE_ALLOCATION_GENERATION.fetch_add(1, Ordering::Relaxed);
        afs_logging::debug!("dfs.write_buffer.checkout";
            "event" => "checkout",
            "reused" => false,
            "allocation_id" => allocation_id,
            "capacity" => bytes.capacity() as u64,
            "body_pointer" => bytes.as_ptr() as usize,
        );
        Ok(ChunkBuffer {
            bytes,
            pool: Arc::downgrade(self),
            allocation_id,
            #[cfg(any(feature = "dfs", test))]
            native_live_lease,
        })
    }

    fn record_copy(&self, bytes: usize) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.stats.copied_bytes = state.stats.copied_bytes.saturating_add(bytes as u64);
        state.stats.copy_operations = state.stats.copy_operations.saturating_add(1);
        state.publish_metrics();
    }

    #[cfg(test)]
    pub(super) fn fail_next_allocation(&self) {
        self.state.lock().unwrap().fail_next_allocation = true;
    }
}

#[derive(Debug)]
struct ChunkBuffer {
    bytes: Vec<u8>,
    pool: Weak<WriteBufferPool>,
    allocation_id: u64,
    // Drops after the body is returned to idle or really freed in Drop. Every
    // SharedChunkBytes slice keeps this same single allocation owner alive.
    #[cfg(any(feature = "dfs", test))]
    native_live_lease: Option<NativeWriteLease>,
}

impl ChunkBuffer {
    fn unpooled(bytes: Vec<u8>) -> Self {
        Self {
            bytes,
            pool: Weak::new(),
            allocation_id: WRITE_ALLOCATION_GENERATION.fetch_add(1, Ordering::Relaxed),
            #[cfg(any(feature = "dfs", test))]
            native_live_lease: None,
        }
    }
}

impl Drop for ChunkBuffer {
    fn drop(&mut self) {
        let capacity = self.bytes.capacity();
        #[cfg(any(feature = "dfs", test))]
        debug_assert!(
            self.native_live_lease
                .as_ref()
                .is_none_or(|lease| lease.charged_bytes() == capacity)
        );
        let body_pointer = self.bytes.as_ptr() as usize;
        let Some(pool) = self.pool.upgrade() else {
            let bytes = std::mem::take(&mut self.bytes);
            drop(bytes);
            afs_logging::debug!("dfs.write_buffer.last_owner_release";
                "event" => "last_owner_release",
                "allocation_id" => self.allocation_id,
                "capacity" => capacity as u64,
                "body_pointer" => body_pointer,
                "reason" => "pool_unavailable",
            );
            return;
        };
        let mut bytes = std::mem::take(&mut self.bytes);
        // Clear the initialized logical length. On reuse, resize initializes
        // each newly exposed byte before any caller can read a previous body.
        bytes.clear();
        {
            let mut state = pool
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.stats.live_bytes -= capacity;
            state.stats.live_buffers -= 1;
            if !state.stopped
                && capacity != 0
                && state.idle.len() < MAX_IDLE_WRITE_BUFFERS
                && capacity
                    <= pool
                        .idle_baseline_bytes
                        .saturating_sub(state.stats.idle_bytes)
            {
                state.stats.idle_bytes += capacity;
                state.stats.idle_buffers += 1;
                state.idle.push(bytes);
                state.publish_metrics();
                drop(state);
                afs_logging::debug!("dfs.write_buffer.last_owner_return";
                    "event" => "last_owner_return",
                    "allocation_id" => self.allocation_id,
                    "capacity" => capacity as u64,
                    "body_pointer" => body_pointer,
                );
                return;
            }
            state.stats.released_bytes = state.stats.released_bytes.saturating_add(capacity as u64);
            state.stats.released_buffers = state.stats.released_buffers.saturating_add(1);
            state.publish_metrics();
        }
        // Ineligible allocations are freed only after releasing the pool lock.
        drop(bytes);
        afs_logging::debug!("dfs.write_buffer.last_owner_release";
            "event" => "last_owner_release",
            "allocation_id" => self.allocation_id,
            "capacity" => capacity as u64,
            "body_pointer" => body_pointer,
            "reason" => "idle_not_retained",
        );
    }
}

/// Shared immutable view of one final allocation. Cloning or slicing moves
/// ownership metadata only; the final reference retires the entire allocation.
#[derive(Clone, Debug)]
pub struct SharedChunkBytes {
    buffer: Arc<ChunkBuffer>,
    start: usize,
    end: usize,
}

impl SharedChunkBytes {
    pub fn from_vec(bytes: Vec<u8>) -> Self {
        Self::from_buffer(ChunkBuffer::unpooled(bytes))
    }

    fn from_buffer(buffer: ChunkBuffer) -> Self {
        let end = buffer.bytes.len();
        Self {
            buffer: Arc::new(buffer),
            start: 0,
            end,
        }
    }

    pub fn len(&self) -> usize {
        self.end - self.start
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn as_ptr(&self) -> *const u8 {
        self.as_ref().as_ptr()
    }
    pub fn capacity(&self) -> usize {
        self.buffer.bytes.capacity()
    }
    pub fn allocation_id(&self) -> u64 {
        self.buffer.allocation_id
    }

    /// Share a subrange without changing the retained allocation or its charge.
    ///
    /// # Panics
    /// Panics when the requested range is outside this view or overflows usize.
    pub fn slice(&self, range: impl RangeBounds<usize>) -> Self {
        let start = match range.start_bound() {
            Bound::Included(value) => *value,
            Bound::Excluded(value) => value
                .checked_add(1)
                .expect("shared chunk range start overflow"),
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(value) => value
                .checked_add(1)
                .expect("shared chunk range end overflow"),
            Bound::Excluded(value) => *value,
            Bound::Unbounded => self.len(),
        };
        assert!(
            start <= end && end <= self.len(),
            "shared chunk range is outside its view"
        );
        Self {
            buffer: self.buffer.clone(),
            start: self.start + start,
            end: self.start + end,
        }
    }
}

impl From<Vec<u8>> for SharedChunkBytes {
    fn from(bytes: Vec<u8>) -> Self {
        Self::from_vec(bytes)
    }
}

impl AsRef<[u8]> for SharedChunkBytes {
    fn as_ref(&self) -> &[u8] {
        &self.buffer.bytes[self.start..self.end]
    }
}

impl Deref for SharedChunkBytes {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

/// One mutable final payload allocation. Pooled builders are fixed capacity;
/// the legacy Default builder remains fallibly growable for other callers.
#[derive(Debug)]
pub struct ChunkBuilder {
    buffer: ChunkBuffer,
    fixed_capacity: Option<usize>,
}

impl Default for ChunkBuilder {
    fn default() -> Self {
        Self {
            buffer: ChunkBuffer::unpooled(Vec::new()),
            fixed_capacity: None,
        }
    }
}

impl ChunkBuilder {
    /// Acquire capacity without a live quota. Allocation refusal returns ENOMEM.
    pub fn with_pool(pool: Arc<WriteBufferPool>, capacity: usize) -> Result<Self> {
        Ok(Self {
            buffer: pool.checkout(capacity)?,
            fixed_capacity: Some(capacity),
        })
    }

    /// Consume admission already obtained outside inode and operation locks.
    /// No wait occurs here. ENOMEM means allocator refusal; WOULD_BLOCK means
    /// actual capacity requires new admission. Both failures retire this lease.
    #[cfg(any(feature = "dfs", test))]
    pub(crate) fn with_native_lease(
        pool: Arc<WriteBufferPool>,
        capacity: usize,
        lease: NativeWriteLease,
    ) -> Result<Self> {
        Ok(Self {
            buffer: pool.checkout_with_native_lease(capacity, Some(lease))?,
            fixed_capacity: Some(capacity),
        })
    }

    pub fn len(&self) -> usize {
        self.buffer.bytes.len()
    }
    pub fn is_empty(&self) -> bool {
        self.buffer.bytes.is_empty()
    }
    pub fn capacity(&self) -> usize {
        self.buffer.bytes.capacity()
    }
    /// Usable body limit; actual allocation capacity may be larger on reuse.
    pub fn writable_capacity(&self) -> usize {
        self.fixed_capacity.unwrap_or_else(|| self.capacity())
    }
    pub fn as_bytes(&self) -> &[u8] {
        &self.buffer.bytes
    }
    pub fn data_ptr(&self) -> *const u8 {
        self.buffer.bytes.as_ptr()
    }
    pub fn allocation_id(&self) -> u64 {
        self.buffer.allocation_id
    }

    /// Copy accepted input directly to the final storage. Fixed builders never
    /// reallocate; allocation errors leave the previous bytes and length intact.
    pub fn write_at(&mut self, offset: u64, data: &[u8]) -> Result<usize> {
        if data.is_empty() {
            return Ok(0);
        }
        let offset = usize::try_from(offset).map_err(|_| invalid("chunk offset is too large"))?;
        let end = offset
            .checked_add(data.len())
            .ok_or_else(|| invalid("chunk write range overflow"))?;
        if self.fixed_capacity.is_some_and(|capacity| end > capacity) {
            return Err(write_allocation_error());
        }
        if end > self.buffer.bytes.capacity() {
            self.buffer
                .bytes
                .try_reserve_exact(end - self.buffer.bytes.len())
                .map_err(|_| write_allocation_error())?;
        }
        let old_len = self.buffer.bytes.len();
        let overwritten = old_len.saturating_sub(offset).min(data.len());
        if overwritten != 0 {
            self.buffer.bytes[offset..offset + overwritten].copy_from_slice(&data[..overwritten]);
            self.record_input_copy(offset, overwritten);
        }
        if overwritten < data.len() {
            if offset > old_len {
                self.buffer.bytes.resize(offset, 0);
            }
            // Capacity has already been proved sufficient, so this extend
            // initializes only accepted input and cannot move the allocation.
            self.buffer.bytes.extend_from_slice(&data[overwritten..]);
            self.record_input_copy(offset + overwritten, data.len() - overwritten);
        }
        Ok(data.len())
    }

    fn record_input_copy(&self, offset: usize, bytes: usize) {
        if let Some(pool) = self.buffer.pool.upgrade() {
            pool.record_copy(bytes);
        }
        afs_logging::debug!("dfs.write_buffer.input_copy";
            "event" => "input_copy",
            "allocation_id" => self.allocation_id(),
            "capacity" => self.capacity() as u64,
            "body_pointer" => self.data_ptr() as usize,
            "buffer_offset" => offset as u64,
            "copied_bytes" => bytes as u64,
        );
    }

    /// Shrink logical content without releasing or reallocating its storage.
    pub fn truncate(&mut self, length: usize) {
        self.buffer.bytes.truncate(length);
    }

    /// Legacy full-body ownership transfer; pooled builders are not used here.
    pub fn replace(&mut self, bytes: Vec<u8>) {
        self.buffer = ChunkBuffer::unpooled(bytes);
        self.fixed_capacity = None;
    }

    pub fn freeze(self) -> SharedChunkBytes {
        let pointer_before = self.data_ptr() as usize;
        let frozen = SharedChunkBytes::from_buffer(self.buffer);
        afs_logging::debug!("dfs.write_buffer.freeze";
            "event" => "freeze",
            "allocation_id" => frozen.allocation_id(),
            "capacity" => frozen.capacity() as u64,
            "body_bytes" => frozen.len() as u64,
            "pointer_before" => pointer_before,
            "pointer_after" => frozen.as_ptr() as usize,
        );
        frozen
    }

    pub fn stage(self, operation_id: OperationId) -> StagedChunk {
        StagedChunk::from_shared(operation_id, self.freeze())
    }
}

fn write_allocation_error() -> Error {
    Error::coded(
        afs_error::IO_OUT_OF_MEMORY,
        "write buffer capacity allocation refused",
    )
}

/// Shared producer/replica budget per immutable staged chunk, never a file limit.
#[cfg(feature = "dfs")]
pub(crate) const MAX_STAGED_CHUNK_BYTES: usize = 4 * 1024 * 1024;

#[derive(Clone, Debug)]
pub struct StagedChunk {
    pub operation_id: OperationId,
    pub chunk: ChunkObject,
    bytes: SharedChunkBytes,
    outboard: Option<SharedOutboard>,
    segment_crc: Option<Arc<super::chunk_crc32c::CrcTable>>,
    // This private identity binds the immutable allocation to its initial integrity
    // descriptor. CRC descriptors bind segment checksums, not cryptographic body proofs.
    #[cfg(feature = "dfs")]
    sealed_chunk: ChunkObject,
    #[cfg(feature = "dfs")]
    write_budget: Vec<WriteBudgetReservation>,
}

impl StagedChunk {
    /// Rebuild a received/repaired body under its original immutable identity.
    pub(crate) fn from_replica(
        operation_id: OperationId,
        expected: &ChunkObject,
        bytes: Vec<u8>,
    ) -> Result<Self> {
        let staged = if expected.is_segment_crc32c() {
            Self::from_shared_crc32c_id(
                operation_id,
                SharedChunkBytes::from_buffer(ChunkBuffer::unpooled(bytes)),
                expected.id.clone(),
            )?
        } else {
            Self::new(operation_id, bytes)
        };
        if &staged.chunk != expected {
            return Err(corrupt(
                &expected.id,
                "replica body differs from immutable descriptor",
            ));
        }
        Ok(staged)
    }

    pub fn new(operation_id: OperationId, bytes: Vec<u8>) -> Self {
        Self::from_shared(
            operation_id,
            SharedChunkBytes::from_buffer(ChunkBuffer::unpooled(bytes)),
        )
    }

    pub fn from_shared(operation_id: OperationId, bytes: SharedChunkBytes) -> Self {
        tracing::info_span!(
            "node.dfs.chunk.stage",
            operation_id = %operation_id.0,
            bytes = bytes.len() as u64,
            allocation_id = bytes.allocation_id(),
            capacity = bytes.capacity() as u64,
            body_pointer = bytes.as_ptr() as usize,
            chunk_id = tracing::field::Empty,
        )
        .in_scope(|| {
            let bytes_len = bytes.len() as u64;
            let built = bytes
                .buffer
                .pool
                .upgrade()
                .and_then(|pool| pool.outboard_budget.clone())
                .map(|budget| {
                    let built = tracing::info_span!("node.dfs.chunk.hash", bytes = bytes_len,)
                        .in_scope(|| outboard::build_borrowed(&bytes, &budget));
                    if built.outboard.is_none() {
                        budget.record_write_without_proof();
                    }
                    built
                });
            let content_digest = built
                .as_ref()
                .map(|built| built.digest.clone())
                .unwrap_or_else(|| {
                    tracing::info_span!("node.dfs.chunk.hash", bytes = bytes_len,)
                        .in_scope(|| digest(&bytes))
                });
            let outboard = built.and_then(|built| built.outboard);
            let chunk = ChunkObject {
                id: ChunkId::new(format!(
                    "blake3-{}-{}",
                    digest_hex(&content_digest),
                    bytes.len()
                )),
                length: bytes.len() as u64,
                content_digest,
                encoding: ChunkEncoding::Raw,
            };
            tracing::Span::current().record("chunk_id", chunk.id.0.as_str());
            Self {
                operation_id,
                #[cfg(feature = "dfs")]
                sealed_chunk: chunk.clone(),
                chunk,
                bytes,
                outboard,
                segment_crc: None,
                #[cfg(feature = "dfs")]
                write_budget: Vec::new(),
            }
        })
    }

    #[cfg(test)]
    pub(crate) fn new_crc32c(operation_id: OperationId, bytes: Vec<u8>) -> Result<Self> {
        Self::from_shared_crc32c(
            operation_id,
            SharedChunkBytes::from_buffer(ChunkBuffer::unpooled(bytes)),
        )
    }

    /// Immutable native payload, bound to its persisted CRC descriptor.
    pub(crate) fn from_shared_crc32c(
        operation_id: OperationId,
        bytes: SharedChunkBytes,
    ) -> Result<Self> {
        let id = ChunkId::new(format!(
            "crc32c-v2-{}-{}",
            ChunkObject::generation_id(&operation_id),
            uuid::Uuid::new_v4().simple()
        ));
        Self::from_shared_crc32c_id(operation_id, bytes, id)
    }

    pub(crate) fn from_shared_crc32c_id(
        operation_id: OperationId,
        bytes: SharedChunkBytes,
        id: ChunkId,
    ) -> Result<Self> {
        let table = super::chunk_crc32c::CrcTable::build(&bytes)?;
        let chunk = ChunkObject {
            id,
            length: bytes.len() as u64,
            content_digest: table.descriptor_digest(),
            encoding: ChunkEncoding::Raw,
        };
        if !chunk.valid_immutable_identity() {
            return Err(invalid("invalid CRC32C object identity"));
        }
        Ok(Self {
            operation_id,
            #[cfg(feature = "dfs")]
            sealed_chunk: chunk.clone(),
            chunk,
            bytes,
            outboard: None,
            segment_crc: Some(Arc::new(table)),
            #[cfg(feature = "dfs")]
            write_budget: Vec::new(),
        })
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Content proof only: operation identity, authorization and persistence
    /// still require their own checks. No mutable payload is exposed.
    #[cfg(feature = "dfs")]
    pub(super) fn has_sealed_identity(&self) -> bool {
        self.chunk == self.sealed_chunk && self.chunk.length == self.bytes.len() as u64
    }

    #[cfg(feature = "dfs")]
    pub(super) fn write_budget_reservations(&self) -> &[WriteBudgetReservation] {
        &self.write_budget
    }

    #[cfg(feature = "dfs")]
    pub(super) fn retain_write_budget(&mut self, reservations: &[WriteBudgetReservation]) {
        self.write_budget.extend_from_slice(reservations);
    }

    #[cfg(feature = "dfs")]
    pub(crate) fn shared_bytes(&self) -> SharedChunkBytes {
        self.bytes.clone()
    }

    /// Heap capacity owned by the staged metadata, excluding the separately
    /// charged final body, shared outboard and shared budget owners. Callers
    /// account for the inline StagedChunk/container storage themselves.
    #[cfg(feature = "dfs")]
    pub(crate) fn native_retained_metadata_bytes(&self) -> usize {
        self.operation_id
            .0
            .capacity()
            .saturating_add(self.chunk.id.0.capacity())
            .saturating_add(self.sealed_chunk.id.0.capacity())
            .saturating_add(self.segment_crc.as_ref().map_or(0, |t| {
                std::mem::size_of::<super::chunk_crc32c::CrcTable>()
                    + 16
                    + t.checksums.capacity() * 4
            }))
            .saturating_add(
                self.write_budget
                    .capacity()
                    .saturating_mul(std::mem::size_of::<WriteBudgetReservation>()),
            )
    }
}

#[cfg(feature = "dfs")]
impl AsRef<[u8]> for StagedChunk {
    fn as_ref(&self) -> &[u8] {
        self.bytes()
    }
}

// The VFS owns admission limits. These typed leases live beside their immutable
// payload so HTTP body/retry ownership can outlive a ticket or a dirty window.
#[cfg(feature = "dfs")]
#[derive(Debug, Default)]
pub(super) struct DeferredWriteBudget {
    pub(super) state: Mutex<DeferredWriteBudgetState>,
}

#[cfg(feature = "dfs")]
#[derive(Debug, Default)]
pub(super) struct DeferredWriteBudgetState {
    pub(super) normal_bytes: u64,
    pub(super) publisher_bytes: u64,
    pub(super) descriptor_bytes: u64,
    pub(super) descriptors: usize,
}

#[cfg(feature = "dfs")]
#[derive(Clone, Debug)]
pub(super) struct WriteBudgetReservation {
    _inner: Arc<WriteBudgetReservationInner>,
}

#[cfg(feature = "dfs")]
#[derive(Debug)]
struct WriteBudgetReservationInner {
    budget: Arc<DeferredWriteBudget>,
    normal_bytes: u64,
    publisher_bytes: u64,
    descriptor_bytes: u64,
    descriptors: usize,
}

#[cfg(feature = "dfs")]
impl WriteBudgetReservation {
    pub(super) fn reduce_normal_to(&mut self, bytes: u64) -> Result<()> {
        let inner = Arc::get_mut(&mut self._inner)
            .ok_or_else(|| invalid("shared payload lease cannot be resized"))?;
        if bytes > inner.normal_bytes {
            return Err(invalid("payload lease reduction cannot grow its charge"));
        }
        let mut state = inner.budget.state.lock().map_err(|_| {
            Error::coded(
                afs_error::NODE_VFS_UNAVAILABLE,
                "DFS deferred write budget is poisoned",
            )
        })?;
        state.normal_bytes = state
            .normal_bytes
            .saturating_sub(inner.normal_bytes - bytes);
        inner.normal_bytes = bytes;
        Ok(())
    }

    pub(super) fn new(
        budget: Arc<DeferredWriteBudget>,
        normal_bytes: u64,
        publisher_bytes: u64,
        descriptor_bytes: u64,
        descriptors: usize,
    ) -> Self {
        Self {
            _inner: Arc::new(WriteBudgetReservationInner {
                budget,
                normal_bytes,
                publisher_bytes,
                descriptor_bytes,
                descriptors,
            }),
        }
    }
}

#[cfg(feature = "dfs")]
impl Drop for WriteBudgetReservationInner {
    fn drop(&mut self) {
        if let Ok(mut state) = self.budget.state.lock() {
            state.normal_bytes = state.normal_bytes.saturating_sub(self.normal_bytes);
            state.publisher_bytes = state.publisher_bytes.saturating_sub(self.publisher_bytes);
            state.descriptor_bytes = state.descriptor_bytes.saturating_sub(self.descriptor_bytes);
            state.descriptors = state.descriptors.saturating_sub(self.descriptors);
        }
    }
}

/// A native persistence receipt with an explicit, conservative local release
/// decision. Only the native replication implementation may establish Required
/// policy and the matching local durable ACK; the default never grants it.
#[derive(Clone, Debug)]
pub struct NativeChunkReceipt {
    pub receipt: ChunkReceipt,
    pub local_required_and_durable: bool,
}

pub trait ChunkStore: Send + Sync {
    fn put_batch(&self, staged: Vec<StagedChunk>) -> Result<Vec<ChunkReceipt>>;

    /// Reserve native control metadata before accepting a body. Production
    /// replication overrides this from its cached authoritative placement.
    /// The fallback is only a conservative allowance for small custom stores:
    /// retained actual capacity must still be checked against that allowance.
    fn native_descriptor_reservation_bytes(&self, operation_id: &OperationId) -> Result<usize> {
        const FALLBACK_BYTES: usize = 16 * 1024;
        if operation_id.0.capacity() > FALLBACK_BYTES / 4 {
            return Err(Error::coded(
                afs_error::IO_CAPACITY,
                "native operation metadata exceeds the custom-store reservation allowance",
            ));
        }
        Ok(FALLBACK_BYTES)
    }

    fn put_native_batch(&self, staged: Vec<StagedChunk>) -> Result<Vec<NativeChunkReceipt>> {
        self.put_batch(staged).map(|receipts| {
            receipts
                .into_iter()
                .map(|receipt| NativeChunkReceipt {
                    receipt,
                    local_required_and_durable: false,
                })
                .collect()
        })
    }

    /// Read an unpublished native object anchored by its validated receipt.
    /// Unsupported stores must keep the original body until publication.
    fn read_native_uploaded_at(
        &self,
        _receipt: &ChunkReceipt,
        _offset: u64,
        _out: &mut [u8],
    ) -> Result<usize> {
        Err(Error::coded(
            afs_error::NODE_VFS_UNIMPLEMENTED,
            "native uploaded local read is unsupported for this backend",
        ))
    }

    fn supports_deferred_put(&self) -> bool {
        false
    }

    /// Submit owned chunks without waiting for persistence. Unsupported backends
    /// reject explicitly; callers must check supports_deferred_put first.
    fn put_batch_deferred(&self, _staged: Vec<StagedChunk>) -> Result<Box<dyn DeferredChunkPut>> {
        Err(Error::coded(
            afs_error::NODE_VFS_UNIMPLEMENTED,
            "deferred chunk PUT is unsupported for this backend",
        ))
    }

    fn put(&self, staged: StagedChunk) -> Result<ChunkReceipt> {
        self.put_batch(vec![staged])?
            .pop()
            .ok_or_else(|| invalid("ChunkStore returned no receipt for one staged Chunk"))
    }

    fn read_at(&self, chunk_id: &ChunkId, offset: u64, out: &mut [u8]) -> Result<usize>;

    fn capacity(&self) -> Result<FilesystemCapacity> {
        Err(Error::coded(
            afs_error::NODE_VFS_UNIMPLEMENTED,
            "ChunkStore capacity is unsupported for this backend",
        ))
    }
}

pub trait DeferredChunkPut: Send {
    fn try_wait(&mut self) -> Result<Option<Vec<ChunkReceipt>>> {
        Ok(None)
    }

    fn wait(self: Box<Self>) -> Result<Vec<ChunkReceipt>>;
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum LocalChunkState {
    Durable,
    Quarantined,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum PhysicalLocation {
    PerChunkFile { relative_path: String },
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum PhysicalEncoding {
    Raw,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct LocalChunkRecord {
    pub chunk: ChunkObject,
    pub stored_length: u64,
    pub stored_checksum: ContentDigest,
    pub device_id: String,
    pub device_epoch: u64,
    pub location: PhysicalLocation,
    pub encoding: PhysicalEncoding,
    pub state: LocalChunkState,
    pub catalog_revision: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) segment_crc: Option<super::chunk_crc32c::CrcTable>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RecoveryReport {
    pub removed_staging_files: u64,
    pub durable_chunks: u64,
    pub orphan_files: u64,
}

#[derive(Debug)]
pub struct PinnedChunkReader {
    segment_crc: Option<super::chunk_crc32c::CrcTable>,
    file: File,
    chunk: ChunkObject,
    outboard_path: PathBuf,
    outboard_budget: Arc<OutboardBudget>,
    outboard_repairs: Arc<OutboardRepairGates>,
    outboard: Mutex<Option<SharedOutboard>>,
    proof_identity: Mutex<PeerFileIdentity>,
    read_work: Option<super::read_work::ReadWorkMetrics>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct VerifiedRangeCopy {
    pub chunk_offset: u64,
    pub length: usize,
    pub output_offset: usize,
}

impl PinnedChunkReader {
    fn crc_ranges(&self, ranges: &[VerifiedRangeCopy], out: &mut [u8]) -> Result<Vec<usize>> {
        let table = self
            .segment_crc
            .as_ref()
            .ok_or_else(|| invalid("CRC descriptor missing"))?;
        if !self.chunk.is_segment_crc32c()
            || !self.chunk.valid_immutable_identity()
            || !table.valid_shape(self.chunk.length)
            || table.descriptor_digest() != self.chunk.content_digest
        {
            return Err(corrupt(&self.chunk.id, "CRC descriptor identity differs"));
        }
        let before = PeerFileIdentity::read(&self.file)?;
        if before.length != self.chunk.length {
            return Err(corrupt(&self.chunk.id, "CRC body length differs"));
        }
        let mut scratch = vec![0u8; super::chunk_crc32c::SEGMENT];
        let mut counts = Vec::with_capacity(ranges.len());
        for r in ranges {
            let allowed =
                (self.chunk.length.saturating_sub(r.chunk_offset)).min(r.length as u64) as usize;
            let output_end = r
                .output_offset
                .checked_add(allowed)
                .ok_or_else(|| invalid("CRC output overflow"))?;
            if output_end > out.len() {
                return Err(invalid("CRC output exceeds buffer"));
            }
            if allowed == 0 {
                counts.push(0);
                continue;
            }
            let end = r.chunk_offset + allowed as u64;
            for index in (r.chunk_offset as usize / super::chunk_crc32c::SEGMENT)
                ..=((end as usize - 1) / super::chunk_crc32c::SEGMENT)
            {
                let begin = index * super::chunk_crc32c::SEGMENT;
                let n = (self.chunk.length as usize - begin).min(scratch.len());
                let mut read = 0;
                while read < n {
                    let got =
                        read_positioned(&self.file, (begin + read) as u64, &mut scratch[read..n])?;
                    if got == 0 {
                        return Err(corrupt(&self.chunk.id, "short CRC body"));
                    }
                    read += got;
                }
                if super::chunk_crc32c::checksum(&scratch[..n]) != table.checksums[index] {
                    return Err(corrupt(&self.chunk.id, "CRC32C segment mismatch"));
                }
                let a = (begin as u64).max(r.chunk_offset);
                let b = ((begin + n) as u64).min(end);
                let dest = r.output_offset + (a - r.chunk_offset) as usize;
                out[dest..dest + (b - a) as usize]
                    .copy_from_slice(&scratch[(a as usize - begin)..(b as usize - begin)]);
            }
            counts.push(allowed);
        }
        if PeerFileIdentity::read(&self.file)? != before {
            return Err(corrupt(&self.chunk.id, "CRC body identity changed"));
        }
        Ok(counts)
    }

    #[cfg(feature = "dfs")]
    pub(crate) fn chunk(&self) -> &ChunkObject {
        &self.chunk
    }

    fn load_outboard(&self) -> Result<Option<SharedOutboard>> {
        if self.chunk.length > 4 * 1024 * 1024
            || ChunkObject::from_content_addressed_id(&self.chunk.id).as_ref() != Some(&self.chunk)
        {
            return Ok(None);
        }
        let Ok(mut cached) = self.outboard.lock() else {
            return Ok(None);
        };
        if let Some(proof) = cached.as_ref() {
            return Ok(Some(proof.clone()));
        }
        match load_outboard_file(&self.outboard_path, &self.chunk, &self.outboard_budget) {
            Ok(proof) => {
                if proof.try_mark_resident() {
                    *cached = Some(proof.clone());
                }
                // Resident pressure limits retention, not this already charged
                // temporary proof's ability to validate the current range.
                Ok(Some(proof))
            }
            Err(error) if error.code() == afs_error::IO_CAPACITY => Err(error),
            Err(_) => Ok(None),
        }
    }

    fn remember_outboard(&self, proof: Option<SharedOutboard>) {
        if let Some(proof) = proof {
            // Transient proof remains in the original total ledger throughout
            // repair; a full resident subset must not prevent bounded repair.
            repair_outboard_if_needed(&self.outboard_path, &proof, &self.outboard_repairs);
            if proof.try_mark_resident()
                && let Ok(mut cached) = self.outboard.lock()
            {
                *cached = Some(proof);
            }
        }
    }

    fn full_scan(
        &self,
        ranges: &[VerifiedRangeCopy],
        out: &mut [u8],
        force: bool,
        build_outboard: bool,
    ) -> Result<Vec<usize>> {
        if self.segment_crc.is_some() {
            return self.crc_ranges(ranges, out);
        }
        let before = PeerFileIdentity::read(&self.file)?;
        let builder = (build_outboard
            && ChunkObject::from_content_addressed_id(&self.chunk.id).as_ref()
                == Some(&self.chunk))
        .then(|| OutboardBuilder::new(self.chunk.length, self.outboard_budget.clone()));
        let (counts, proof) = verified_ranges_into_building(
            &self.file,
            &self.chunk,
            ranges,
            out,
            force,
            self.read_work.as_ref(),
            builder,
        )?;
        if PeerFileIdentity::read(&self.file)? != before {
            return Err(corrupt(
                &self.chunk.id,
                "identity changed during full verification",
            ));
        }
        *self
            .proof_identity
            .lock()
            .map_err(|_| invalid("pinned proof identity lock poisoned"))? = before;
        self.remember_outboard(proof);
        Ok(counts)
    }

    fn verify_full_and_build(&self) -> Result<()> {
        if self.segment_crc.is_some() {
            let mut out = vec![0; super::chunk_crc32c::SEGMENT];
            let mut offset = 0;
            while offset < self.chunk.length {
                let length = (self.chunk.length - offset).min(out.len() as u64) as usize;
                self.crc_ranges(
                    &[VerifiedRangeCopy {
                        chunk_offset: offset,
                        length,
                        output_offset: 0,
                    }],
                    &mut out,
                )?;
                offset += length as u64;
            }
            return Ok(());
        }

        self.full_scan(&[], &mut [], true, true).map(|_| ())
    }

    fn outboard_ranges(
        &self,
        ranges: &[VerifiedRangeCopy],
        out: &mut [u8],
    ) -> Option<Result<Vec<usize>>> {
        let before = PeerFileIdentity::read(&self.file).ok()?;
        if self.proof_identity.lock().ok().as_deref() != Some(&before)
            || self.chunk.length > 4 * 1024 * 1024
            || ChunkObject::from_content_addressed_id(&self.chunk.id).as_ref() != Some(&self.chunk)
        {
            return None;
        }
        // Reserve once before loading a resident proof; transfer the same work
        // lease into validation so resident growth cannot steal its scratch.
        let Some(working) = self
            .outboard_budget
            .reserve_range_work(ranges, self.chunk.length)
        else {
            return Some(Err(outboard_capacity()));
        };
        let proof = match self.load_outboard() {
            Ok(Some(proof)) => proof,
            Ok(None) => return None,
            Err(error) => return Some(Err(error)),
        };
        let mut work = OutboardReadWork::default();
        let result = proof.read_ranges_into_with_reserved_work(
            &self.file,
            0,
            &self.chunk,
            ranges,
            out,
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
        match result {
            Ok(counts) if PeerFileIdentity::read(&self.file).ok().as_ref() == Some(&before) => {
                Some(Ok(counts))
            }
            Err(error) if error.code() == afs_error::IO_CAPACITY => Some(Err(error)),
            _ => {
                // Capacity is not corruption. Only an invalid proof/identity
                // retires the cached metadata and permits a bounded rebuild.
                if let Ok(mut cached) = self.outboard.lock() {
                    cached.take();
                }
                None
            }
        }
    }

    fn read_or_full_scan(
        &self,
        ranges: &[VerifiedRangeCopy],
        out: &mut [u8],
        force: bool,
    ) -> Result<Vec<usize>> {
        match self.outboard_ranges(ranges, out) {
            Some(Ok(counts)) => Ok(counts),
            Some(Err(error)) if error.code() == afs_error::IO_CAPACITY => {
                if force
                    || ranges
                        .iter()
                        .any(|range| range.length != 0 && range.chunk_offset < self.chunk.length)
                {
                    self.outboard_budget.record_range_fullscan_fallback();
                }
                self.full_scan(ranges, out, force, false)
            }
            Some(Err(error)) => Err(error),
            None => self.full_scan(ranges, out, force, true),
        }
    }

    pub fn read_at(&self, offset: u64, out: &mut [u8]) -> Result<usize> {
        if offset >= self.chunk.length || out.is_empty() {
            return Ok(0);
        }
        let allowed = usize::try_from((self.chunk.length - offset).min(out.len() as u64))
            .map_err(|_| invalid("Chunk read length is too large"))?;
        // A pinned descriptor protects identity across rename, not integrity
        // after disk damage. Return the exact bytes covered by this digest,
        // rather than verifying and then issuing another unverified read.
        let mut bytes = vec![0; allowed];
        let ranges = [VerifiedRangeCopy {
            chunk_offset: offset,
            length: allowed,
            output_offset: 0,
        }];
        self.read_or_full_scan(&ranges, &mut bytes, true)?;
        out[..allowed].copy_from_slice(&bytes);
        if let Some(metrics) = &self.read_work {
            metrics.copy("pinned_range_to_output", allowed as u64);
        }
        Ok(allowed)
    }

    /// Verify the pinned file once and scatter requested ranges into an
    /// uncommitted staging buffer. The staging buffer may be dirtied before an
    /// error is returned; callers must publish it only after the outer read
    /// batch succeeds.
    #[cfg(any(feature = "dfs", test))]
    pub(crate) fn read_ranges_at_uncommitted(
        &self,
        ranges: &[VerifiedRangeCopy],
        out: &mut [u8],
    ) -> Result<Vec<usize>> {
        self.read_or_full_scan(ranges, out, false)
    }

    #[cfg(feature = "dfs")]
    pub(crate) fn read_ranges_at_uncommitted_fullscan(
        &self,
        ranges: &[VerifiedRangeCopy],
        out: &mut [u8],
    ) -> Result<Vec<usize>> {
        if self.segment_crc.is_some() {
            return self.crc_ranges(ranges, out);
        }
        verified_ranges_into(
            &self.file,
            &self.chunk,
            ranges,
            out,
            false,
            self.read_work.as_ref(),
        )
    }
}

#[cfg(unix)]
fn read_positioned(file: &File, offset: u64, out: &mut [u8]) -> Result<usize> {
    loop {
        match file.read_at(out, offset) {
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            result => return result.map_err(Error::from),
        }
    }
}

#[cfg(not(unix))]
fn read_positioned(file: &File, offset: u64, out: &mut [u8]) -> Result<usize> {
    let mut file = file.try_clone().map_err(Error::from)?;
    use std::io::{Seek, SeekFrom};
    file.seek(SeekFrom::Start(offset)).map_err(Error::from)?;
    file.read(out).map_err(Error::from)
}

#[derive(Debug, Deserialize, Serialize)]
struct CatalogTxn {
    revision: u64,
    records: Vec<LocalChunkRecord>,
    checksum: String,
}

impl CatalogTxn {
    #[cfg(test)]
    fn new(revision: u64, records: Vec<LocalChunkRecord>) -> Result<Self> {
        let checksum = catalog_checksum(revision, &records)?;
        Ok(Self {
            revision,
            records,
            checksum,
        })
    }

    fn verify(&self) -> Result<()> {
        if self.checksum != catalog_checksum(self.revision, &self.records)? {
            return Err(invalid("local chunk catalog transaction checksum mismatch"));
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct LocalCatalog {
    revision: u64,
    gc_generation: u64,
    records: HashMap<ChunkId, LocalChunkRecord>,
}

impl LocalCatalog {
    pub fn revision(&self) -> u64 {
        self.revision
    }

    fn recover(path: &Path) -> Result<Self> {
        let mut catalog =
            gc::load_checkpoint(&path.with_file_name("catalog.snapshot"))?.unwrap_or_default();
        let base_revision = catalog.revision;
        if !path.exists() {
            return Ok(catalog);
        }
        let file = File::open(path).map_err(Error::from)?;
        let mut reader = BufReader::new(file);
        let mut valid_bytes = 0_u64;
        let mut line = Vec::new();
        loop {
            line.clear();
            let read = reader.read_until(b'\n', &mut line).map_err(Error::from)?;
            if read == 0 {
                break;
            }
            let next_offset = valid_bytes
                .checked_add(
                    u64::try_from(read).map_err(|_| invalid("local catalog is too large"))?,
                )
                .ok_or_else(|| invalid("local catalog offset overflow"))?;
            match std::str::from_utf8(&line) {
                Ok(text) if text.trim().is_empty() => {
                    valid_bytes = next_offset;
                    continue;
                }
                // A partial final code point can be a torn write, but a known
                // invalid code unit is corruption even when JSON reports EOF.
                Err(error) if error.error_len().is_some() => {
                    return Err(invalid("local chunk catalog contains invalid UTF-8"));
                }
                _ => {}
            }
            let txn: CatalogTxn = match serde_json::from_slice(&line) {
                Ok(txn) => txn,
                Err(error) if error.is_eof() && !line.ends_with(b"\n") => {
                    // read_until reaches an unterminated line only at EOF.
                    // Keep every validated transaction byte-for-byte; never
                    // discard complete malformed/checksum-invalid records.
                    let file = OpenOptions::new()
                        .write(true)
                        .open(path)
                        .map_err(Error::from)?;
                    file.set_len(valid_bytes).map_err(Error::from)?;
                    file.sync_all().map_err(Error::from)?;
                    return Ok(catalog);
                }
                Err(error) => {
                    return Err(invalid(format!("local chunk catalog is corrupt: {error}")));
                }
            };
            txn.verify()?;
            if txn.revision <= base_revision {
                valid_bytes = next_offset;
                continue;
            }
            if txn.revision != catalog.revision.saturating_add(1) {
                return Err(invalid("local chunk catalog revision is not contiguous"));
            }
            for record in txn.records {
                if record.catalog_revision != txn.revision {
                    return Err(invalid("local chunk record has the wrong catalog revision"));
                }
                catalog.records.insert(record.chunk.id.clone(), record);
            }
            catalog.revision = txn.revision;
            valid_bytes = next_offset;
        }
        Ok(catalog)
    }
}

#[cfg(test)]
mod catalog_tail_recovery_tests {
    use super::*;

    fn record(revision: u64, label: &str) -> LocalChunkRecord {
        let staged = StagedChunk::new(OperationId::new(label), label.as_bytes().to_vec());
        LocalChunkRecord {
            segment_crc: None,
            stored_length: staged.chunk.length,
            stored_checksum: staged.chunk.content_digest.clone(),
            location: PhysicalLocation::PerChunkFile {
                relative_path: format!("chunks/{}", staged.chunk.id.0),
            },
            chunk: staged.chunk,
            device_id: "local-0".into(),
            device_epoch: 1,
            encoding: PhysicalEncoding::Raw,
            state: LocalChunkState::Durable,
            catalog_revision: revision,
        }
    }

    fn encoded(txn: &CatalogTxn) -> Vec<u8> {
        let mut bytes = serde_json::to_vec(txn).unwrap();
        bytes.push(b'\n');
        bytes
    }

    #[test]
    fn catalog_tail_recovers_only_the_valid_prefix_and_can_append_again() {
        let first = CatalogTxn::new(1, vec![record(1, "confirmed")]).unwrap();
        let next = CatalogTxn::new(2, vec![record(2, "new-a"), record(2, "new-b")]).unwrap();
        let prefix = encoded(&first);
        let body = serde_json::to_string(&next).unwrap();
        let records_start = body.find("\"records\":[").unwrap() + "\"records\":[".len();
        let record_length = serde_json::to_vec(&next.records[0]).unwrap().len();
        let checksum_start = body.rfind("\"checksum\":\"").unwrap() + "\"checksum\":\"".len();
        for cut in [
            1,
            records_start + 1,
            records_start + record_length / 2,
            records_start + record_length + 1,
            checksum_start + 8,
            body.len() - 1,
        ] {
            let temp = tempfile::tempdir().unwrap();
            let path = temp.path().join("catalog.wal");
            let mut bytes = prefix.clone();
            bytes.extend_from_slice(&body.as_bytes()[..cut]);
            fs::write(&path, bytes).unwrap();
            let recovered = LocalCatalog::recover(&path).unwrap();
            assert_eq!(recovered.revision, 1, "cut {cut}");
            assert_eq!(recovered.records.len(), 1, "cut {cut}");
            assert_eq!(
                recovered.records.get(&first.records[0].chunk.id),
                Some(&first.records[0])
            );
            assert_eq!(fs::read(&path).unwrap(), prefix, "cut {cut}");
            append_catalog_txn(&path, &next, None).unwrap();
            let recovered = LocalCatalog::recover(&path).unwrap();
            assert_eq!(recovered.revision, 2);
            assert_eq!(recovered.records.len(), 3);
            for item in &next.records {
                assert_eq!(recovered.records.get(&item.chunk.id), Some(item));
            }
        }
    }

    #[test]
    fn catalog_tail_multibyte_utf8_cut_is_incomplete_but_invalid_utf8_is_not() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("catalog.wal");
        let prefix = encoded(&CatalogTxn::new(1, vec![record(1, "confirmed")]).unwrap());
        let mut item = record(2, "unicode");
        item.device_id = "device-节点".into();
        let body = serde_json::to_string(&CatalogTxn::new(2, vec![item]).unwrap()).unwrap();
        let character = body.find('节').unwrap();
        for cut in [character + 1, character + 2] {
            let mut bytes = prefix.clone();
            bytes.extend_from_slice(&body.as_bytes()[..cut]);
            fs::write(&path, bytes).unwrap();
            assert_eq!(LocalCatalog::recover(&path).unwrap().revision, 1);
            assert_eq!(fs::read(&path).unwrap(), prefix);
        }
        // An invalid code unit is corruption, not an incomplete trailing code point.
        let mut invalid = prefix.clone();
        invalid.extend_from_slice(b"{\"revision\":2,\"records\":[{\"device_id\":\"");
        invalid.push(0xff);
        fs::write(&path, &invalid).unwrap();
        assert!(LocalCatalog::recover(&path).is_err());
        assert_eq!(fs::read(&path).unwrap(), invalid);
    }

    #[test]
    fn catalog_tail_complete_json_without_newline_still_recovers_unchanged() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("catalog.wal");
        let txn = CatalogTxn::new(1, vec![record(1, "complete")]).unwrap();
        let bytes = serde_json::to_vec(&txn).unwrap();
        fs::write(&path, &bytes).unwrap();
        let recovered = LocalCatalog::recover(&path).unwrap();
        assert_eq!(recovered.revision, 1);
        assert_eq!(recovered.records.len(), 1);
        assert_eq!(fs::read(&path).unwrap(), bytes);
        let next = CatalogTxn::new(2, vec![record(2, "after-complete")]).unwrap();
        append_catalog_txn(&path, &next, None).unwrap();
        let recovered = LocalCatalog::recover(&path).unwrap();
        assert_eq!(recovered.revision, 2);
        assert_eq!(recovered.records.len(), 2);
        let mut expected = bytes;
        expected.push(b'\n');
        expected.extend_from_slice(&encoded(&next));
        assert_eq!(fs::read(&path).unwrap(), expected);
    }

    #[test]
    fn catalog_tail_complete_malformed_json_is_not_discarded() {
        let prefix = encoded(&CatalogTxn::new(1, vec![record(1, "confirmed")]).unwrap());
        for malformed in [
            "{]",
            "{\"revision\":2,\"records\":[],\"checksum\":\"x\",}",
            "{\"revision\":2,\"records\":[]}",
            "[]",
            "null",
        ] {
            for newline in [false, true] {
                let temp = tempfile::tempdir().unwrap();
                let path = temp.path().join("catalog.wal");
                let mut bytes = prefix.clone();
                bytes.extend_from_slice(malformed.as_bytes());
                if newline {
                    bytes.push(b'\n');
                }
                fs::write(&path, &bytes).unwrap();
                assert!(
                    LocalCatalog::recover(&path).is_err(),
                    "{malformed:?}, newline={newline}"
                );
                assert_eq!(fs::read(&path).unwrap(), bytes);
            }
        }
    }

    #[test]
    fn catalog_tail_checksum_revision_and_non_tail_damage_remain_errors() {
        let prefix = encoded(&CatalogTxn::new(1, vec![record(1, "confirmed")]).unwrap());
        let mut bad_checksum = CatalogTxn::new(2, vec![record(2, "bad-checksum")]).unwrap();
        bad_checksum.checksum = "0".repeat(64);
        let bad_revision = CatalogTxn::new(3, vec![record(3, "revision-gap")]).unwrap();
        let bad_record = CatalogTxn::new(2, vec![record(3, "record-revision")]).unwrap();
        for txn in [bad_checksum, bad_revision, bad_record] {
            let temp = tempfile::tempdir().unwrap();
            let path = temp.path().join("catalog.wal");
            let mut bytes = prefix.clone();
            bytes.extend_from_slice(&serde_json::to_vec(&txn).unwrap());
            fs::write(&path, &bytes).unwrap();
            assert!(LocalCatalog::recover(&path).is_err());
            assert_eq!(fs::read(&path).unwrap(), bytes);
        }
        for tail in [
            b"{\"revision\":2\n".as_slice(),
            b"{\"revision\":2\n{}\n".as_slice(),
        ] {
            let temp = tempfile::tempdir().unwrap();
            let path = temp.path().join("catalog.wal");
            let mut bytes = prefix.clone();
            bytes.extend_from_slice(tail);
            fs::write(&path, &bytes).unwrap();
            assert!(LocalCatalog::recover(&path).is_err());
            assert_eq!(fs::read(&path).unwrap(), bytes);
        }
    }

    #[test]
    fn catalog_tail_recovery_does_not_remove_uncataloged_chunk_bodies() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let confirmed = StagedChunk::new(OperationId::new("confirmed"), b"confirmed".to_vec());
        store.put(confirmed.clone()).unwrap();
        let extra = [record(2, "orphan-a"), record(2, "orphan-b")];
        for item in &extra {
            fs::write(store.chunk_path(&item.chunk.id), item.chunk.id.0.as_bytes()).unwrap();
        }
        let prefix = fs::read(&store.catalog_path).unwrap();
        let tail = serde_json::to_vec(&CatalogTxn::new(2, extra.to_vec()).unwrap()).unwrap();
        let mut bytes = prefix.clone();
        bytes.extend_from_slice(&tail[..tail.len() / 2]);
        fs::write(&store.catalog_path, bytes).unwrap();
        drop(store);
        let recovered = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        assert_eq!(recovered.recover().unwrap().orphan_files, 2);
        assert_eq!(fs::read(&recovered.catalog_path).unwrap(), prefix);
        let mut output = [0; 9];
        assert_eq!(
            recovered
                .read_at(&confirmed.chunk.id, 0, &mut output)
                .unwrap(),
            9
        );
        assert_eq!(&output, b"confirmed");
        for item in &extra {
            assert_eq!(
                fs::read(recovered.chunk_path(&item.chunk.id)).unwrap(),
                item.chunk.id.0.as_bytes()
            );
        }
    }
}

/// Shared across the current verified object, in-flight ranges and RDMA packed
/// payloads. This replaces independent per-request payload allowances; it does
/// not add a second retained-cache budget.
#[cfg(any(feature = "dfs", test))]
pub(crate) const PEER_READ_PAYLOAD_LIMIT: usize = 16 * 1024 * 1024;
#[cfg(any(feature = "dfs", test))]
pub(crate) const PEER_READ_OBJECT_LIMIT: usize = 4 * 1024 * 1024;

#[derive(Debug)]
#[cfg(any(feature = "dfs", test))]
struct PeerReadBudget {
    used: AtomicU64,
    peak: AtomicU64,
    limit: u64,
    control_used: AtomicU64,
    control_peak: AtomicU64,
    // This lock only pairs admission predicates with release notifications.
    // Never acquire the serve table or destroy charged owners while holding it.
    admission: Mutex<()>,
    changed: Condvar,
    stopped: std::sync::atomic::AtomicBool,
    wait: Duration,
    #[cfg(test)]
    before_wait: Mutex<Option<PeerFillTestBlock>>,
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
const PEER_SERVE_CONTROL_LIMIT: usize = 64 * 1024;
#[cfg(any(feature = "dfs", test))]
const PEER_SERVE_MAX_RESIDENTS: usize = 16;
#[cfg(any(feature = "dfs", test))]
const PEER_SERVE_MAX_FLIGHTS: usize = 8;
#[cfg(any(feature = "dfs", test))]
const PEER_SERVE_ID_LIMIT: usize = 512;

#[cfg(any(feature = "dfs", test))]
fn peer_serve_fixed_control_bytes() -> usize {
    std::mem::size_of::<Mutex<PeerServeState>>()
        + std::mem::size_of::<PeerReadBudget>()
        + std::mem::size_of::<Condvar>()
        + 64
}

#[derive(Debug)]
#[cfg(any(feature = "dfs", test))]
struct PeerControlLease {
    budget: Arc<PeerReadBudget>,
    bytes: u64,
    metrics: Option<super::read_work::ReadWorkMetrics>,
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
    }
}

#[cfg(any(feature = "dfs", test))]
type PeerServeResult = Result<Option<Arc<PeerVerifiedObject>>>;

#[derive(Debug)]
#[cfg(any(feature = "dfs", test))]
struct PeerServeFlight {
    chunk_id: ChunkId,
    result: Mutex<Option<PeerServeResult>>,
    changed: Condvar,
    _control: PeerControlLease,
}

#[derive(Debug)]
#[cfg(any(feature = "dfs", test))]
struct PeerServeState {
    resident_limit: usize,
    residents: [Option<Arc<PeerVerifiedObject>>; PEER_SERVE_MAX_RESIDENTS],
    flights: [Option<Arc<PeerServeFlight>>; PEER_SERVE_MAX_FLIGHTS],
    readers: usize,
    closed: bool,
    stop_deadline: Option<Instant>,
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
    fn resident(&self, chunk_id: &ChunkId) -> Option<Arc<PeerVerifiedObject>> {
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

    fn remove_resident(&mut self, chunk_id: &ChunkId) -> Option<Arc<PeerVerifiedObject>> {
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
struct PeerServeReadGuard<'a>(&'a LocalChunkStore);
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
    bytes: u64,
    metrics: Option<super::read_work::ReadWorkMetrics>,
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
    metrics: Option<&super::read_work::ReadWorkMetrics>,
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
    metrics: Option<&super::read_work::ReadWorkMetrics>,
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
    metrics: Option<&super::read_work::ReadWorkMetrics>,
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
struct PeerFileIdentity {
    length: u64,
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
    #[cfg(unix)]
    modified: (i64, i64),
    #[cfg(unix)]
    changed: (i64, i64),
}

impl PeerFileIdentity {
    fn read(file: &File) -> Result<Self> {
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
struct PeerVerifiedObject {
    bytes: Vec<u8>,
    outboard: Option<SharedOutboard>,
    _reservation: PeerReadReservation,
    // A range fallback is never resident and needs no reusable identity.
    identity: Option<(LocalChunkRecord, PeerFileIdentity)>,
    _control: PeerControlLease,
}

/// A view into an immutable private allocation. Its shared reservation remains
/// charged even after its LRU entry is retired or an RPC is cancelled.
#[derive(Debug)]
#[cfg(any(feature = "dfs", test))]
pub(crate) struct PeerReadPayload {
    object: Arc<PeerVerifiedObject>,
    start: usize,
    length: usize,
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
struct PeerStreamBlock {
    bytes: Vec<u8>,
    _reservation: PeerReadReservation,
}

#[cfg(feature = "dfs")]
struct PeerStreamPool {
    free: Mutex<[Option<PeerStreamBlock>; 2]>,
    changed: Condvar,
    _control: PeerControlLease,
}

#[cfg(feature = "dfs")]
struct PeerStreamFrame {
    block: Option<PeerStreamBlock>,
    pool: Arc<PeerStreamPool>,
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
    identity: PeerFileIdentity,
    proof: SharedOutboard,
    pool: Arc<PeerStreamPool>,
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
                self.store.try_quarantine(&self.record.chunk.id);
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

const SIDECAR_INSTALL_LIMIT: usize = 64;
const SIDECAR_INSTALL_BATCH: usize = 32;
const SIDECAR_STOP_WAIT: Duration = Duration::from_secs(10);
const SIDECAR_ID_CAPACITY: usize = 96;

#[derive(Debug)]
struct SidecarInstallItem {
    chunk_id: ChunkId,
    proof: SidecarOutboard,
}

#[derive(Debug, Default)]
struct SidecarInstallState {
    pending: VecDeque<SidecarInstallItem>,
    unfinished: HashSet<ChunkId>,
    active: usize,
    deadline: Option<Instant>,
    control: Option<OutboardLease>,
    control_bytes: usize,
    directory_capacity: usize,
}

#[derive(Debug, Default)]
struct SidecarInstallQueue {
    state: Mutex<SidecarInstallState>,
    changed: Condvar,
}

#[derive(Debug)]
struct SidecarInstaller {
    limit: usize,
    queue: Arc<SidecarInstallQueue>,
    worker: Mutex<Option<std::thread::JoinHandle<()>>>,
    joining: Mutex<()>,
    metrics: Option<super::read_work::ReadWorkMetrics>,
}

#[derive(Clone, Debug)]
struct SidecarInstallContext {
    directory: PathBuf,
    repairs: Arc<OutboardRepairGates>,
    budget: Arc<OutboardBudget>,
    metrics: Option<super::read_work::ReadWorkMetrics>,
    #[cfg(test)]
    test: Arc<SidecarInstallTestControl>,
}

// Fields are deliberately dropped before lease: optional metadata failure must
// free the actual backing capacities before refunding the shared ledger.
struct SidecarControlBuffers {
    pending: VecDeque<SidecarInstallItem>,
    unfinished: HashSet<ChunkId>,
    lease: OutboardLease,
    bytes: usize,
}

fn sidecar_control_bytes(
    queue_capacity: usize,
    hash_capacity: usize,
    limit: usize,
    path_capacity: usize,
) -> Option<usize> {
    // std HashSet reports usable entries, not buckets. Rounding up bounds its
    // backing key/control table; 4KiB covers fixed queue/context/Arc metadata.
    let buckets = hash_capacity.max(1).checked_next_power_of_two()?;
    queue_capacity
        .checked_mul(std::mem::size_of::<SidecarInstallItem>())?
        .checked_add(buckets.checked_mul(std::mem::size_of::<ChunkId>() + 1)?)?
        .checked_add(limit.checked_mul(2 * SIDECAR_ID_CAPACITY)?)?
        .checked_add(sidecar_path_bytes(path_capacity)?)?
        .checked_add(4096)
}

fn sidecar_path_bytes(directory_capacity: usize) -> Option<usize> {
    // Include the retained worker directory and the temporary formal/tmp
    // joins and ID formatting used during optional installation. Joins may
    // grow their backing buffers; this is deliberately conservative.
    directory_capacity
        .checked_add(SIDECAR_ID_CAPACITY)?
        .checked_add(128)?
        .checked_mul(8)
}

fn prepare_sidecar_control(store: &LocalChunkStore, limit: usize) -> Option<SidecarControlBuffers> {
    // HashSet's smallest allocation can hold three entries even for limit=1.
    // A minimum of four buckets and two per allowed entry bounds that case.
    let hash_bound = limit.checked_mul(2)?.max(4).checked_next_power_of_two()?;
    let bytes = sidecar_control_bytes(limit, hash_bound, limit, store.outboards.as_os_str().len())?;
    let lease = store.outboard_budget.try_reserve_sidecar_control(bytes)?;
    let mut pending = VecDeque::new();
    pending.try_reserve_exact(limit).ok()?;
    let mut unfinished = HashSet::new();
    unfinished.try_reserve(limit).ok()?;
    let observed = sidecar_control_bytes(
        pending.capacity(),
        unfinished.capacity(),
        limit,
        store.outboards.as_os_str().len(),
    )?;
    if observed > bytes {
        // Never keep a container whose real retained capacity exceeds its
        // conservative ticket; failed optional admission does not affect ACK.
        return None;
    }
    Some(SidecarControlBuffers {
        pending,
        unfinished,
        lease,
        bytes,
    })
}

fn sidecar_chunk_id(id: &ChunkId) -> Option<ChunkId> {
    if id.0.len() > SIDECAR_ID_CAPACITY {
        return None;
    }
    let mut copied = String::new();
    copied.try_reserve_exact(id.0.len()).ok()?;
    copied.push_str(&id.0);
    (copied.capacity() <= SIDECAR_ID_CAPACITY).then(|| ChunkId::new(copied))
}

impl Default for SidecarInstaller {
    fn default() -> Self {
        Self {
            limit: SIDECAR_INSTALL_LIMIT,
            queue: Arc::new(SidecarInstallQueue::default()),
            worker: Mutex::new(None),
            joining: Mutex::new(()),
            metrics: None,
        }
    }
}

impl SidecarInstaller {
    fn observe(&self, state: &SidecarInstallState) {
        if let Some(metrics) = &self.metrics {
            metrics.sidecar_install_counts(
                state.pending.len(),
                state.active,
                self.limit,
                state.control_bytes,
            );
        }
    }

    fn enqueue_batch(&self, store: &LocalChunkStore, staged: &[StagedChunk]) {
        if !staged.iter().any(|item| item.outboard.is_some()) {
            return;
        }
        let Ok(mut worker) = self.worker.lock() else {
            store
                .outboard_budget
                .sidecar_event("sidecar_install_dropped");
            return;
        };
        let Ok(mut state) = self.queue.state.lock() else {
            store
                .outboard_budget
                .sidecar_event("sidecar_install_dropped");
            return;
        };
        if state.deadline.is_some() {
            for item in staged {
                if item.outboard.is_some() {
                    store
                        .outboard_budget
                        .sidecar_event("sidecar_install_dropped");
                }
            }
            return;
        }
        if state.control.is_none() {
            drop(state);
            let Some(buffers) = prepare_sidecar_control(store, self.limit) else {
                store
                    .outboard_budget
                    .sidecar_event("sidecar_install_dropped");
                return;
            };
            state = self
                .queue
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if state.deadline.is_some() {
                drop(state);
                drop(buffers);
                store
                    .outboard_budget
                    .sidecar_event("sidecar_install_dropped");
                return;
            }
            let SidecarControlBuffers {
                pending,
                unfinished,
                lease,
                bytes,
            } = buffers;
            state.pending = pending;
            state.unfinished = unfinished;
            state.control = Some(lease);
            state.control_bytes = bytes;
            self.observe(&state);
        }
        if worker.is_none() && state.deadline.is_none() {
            #[cfg(test)]
            if store
                .sidecar_install_test
                .fail_worker_start
                .swap(false, Ordering::Relaxed)
            {
                store
                    .outboard_budget
                    .sidecar_event("sidecar_install_failed");
                store
                    .outboard_budget
                    .sidecar_event("sidecar_install_dropped");
                return;
            }
            let mut directory = PathBuf::new();
            if directory
                .try_reserve_exact(store.outboards.as_os_str().len())
                .is_err()
            {
                store
                    .outboard_budget
                    .sidecar_event("sidecar_install_dropped");
                return;
            }
            directory.push(&store.outboards);
            if sidecar_control_bytes(
                state.pending.capacity(),
                state.unfinished.capacity(),
                self.limit,
                directory.capacity(),
            )
            .is_none_or(|observed| observed > state.control_bytes)
            {
                store
                    .outboard_budget
                    .sidecar_event("sidecar_install_dropped");
                return;
            }
            state.directory_capacity = directory.capacity();
            let context = SidecarInstallContext {
                directory,
                repairs: store.outboard_repairs.clone(),
                budget: store.outboard_budget.clone(),
                metrics: store.read_work.clone(),
                #[cfg(test)]
                test: store.sidecar_install_test.clone(),
            };
            let queue = self.queue.clone();
            let limit = self.limit;
            match std::thread::Builder::new()
                .name("afs-sidecar-install".into())
                .spawn(move || {
                    run_sidecar_installer(queue, context, limit);
                }) {
                Ok(handle) => *worker = Some(handle),
                Err(_) => {
                    store
                        .outboard_budget
                        .sidecar_event("sidecar_install_failed");
                    store
                        .outboard_budget
                        .sidecar_event("sidecar_install_dropped");
                    return;
                }
            }
        }
        for item in staged {
            let Some(proof) = &item.outboard else {
                continue;
            };
            if state.unfinished.contains(&item.chunk.id) {
                continue;
            }
            if state.deadline.is_some()
                || state.unfinished.len() >= self.limit
                || ChunkObject::from_content_addressed_id(&item.chunk.id).as_ref()
                    != Some(&item.chunk)
                || ChunkOutboard::validate_encoded_shape(&item.chunk, proof.encoded()).is_err()
            {
                store
                    .outboard_budget
                    .sidecar_event("sidecar_install_dropped");
                continue;
            }
            let Some(proof) = store.outboard_budget.pin_sidecar(proof) else {
                store
                    .outboard_budget
                    .sidecar_event("sidecar_install_dropped");
                continue;
            };
            let Some(key) = sidecar_chunk_id(&item.chunk.id) else {
                drop(state);
                drop(proof);
                self.queue.changed.notify_all();
                store
                    .outboard_budget
                    .sidecar_event("sidecar_install_dropped");
                return;
            };
            let Some(queued_id) = sidecar_chunk_id(&item.chunk.id) else {
                drop(state);
                drop(proof);
                self.queue.changed.notify_all();
                store
                    .outboard_budget
                    .sidecar_event("sidecar_install_dropped");
                return;
            };
            state.unfinished.insert(key);
            state.pending.push_back(SidecarInstallItem {
                chunk_id: queued_id,
                proof,
            });
        }
        self.observe(&state);
        self.queue.changed.notify_all();
    }

    fn close(&self, deadline: Instant) {
        let mut state = self
            .queue
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        // Repeated stop requests can shorten, never renew, the first deadline.
        state.deadline = Some(state.deadline.map_or(deadline, |old| old.min(deadline)));
        self.queue.changed.notify_all();
    }

    fn shutdown(&self) -> Result<()> {
        self.close(Instant::now() + SIDECAR_STOP_WAIT);
        let _joining = self
            .joining
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let handle = self
            .worker
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        let result = if let Some(handle) = handle {
            // A synchronous filesystem operation cannot safely be cancelled.
            // Keep owning it and its proof; the existing process watchdog is
            // the final bound, never detach or report a clean incomplete stop.
            handle
                .join()
                .map_err(|_| invalid("sidecar installer worker failed"))
        } else {
            Ok(())
        };
        let (pending, unfinished, control) = {
            let mut state = self
                .queue
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            state.active = 0;
            (
                std::mem::take(&mut state.pending),
                std::mem::take(&mut state.unfinished),
                state.control.take(),
            )
        };
        // The worker's owned directory has already been freed by join. Free
        // every retained container/string before refunding its metadata lease.
        drop(pending);
        drop(unfinished);
        drop(control);
        let mut state = self
            .queue
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        state.control_bytes = 0;
        state.directory_capacity = 0;
        self.observe(&state);
        result
    }

    #[cfg(test)]
    fn wait_idle(&self) {
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut state = self.queue.state.lock().unwrap();
        while !state.unfinished.is_empty() {
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .expect("sidecar installer did not become idle");
            state = self.queue.changed.wait_timeout(state, remaining).unwrap().0;
        }
    }
}

impl Drop for SidecarInstaller {
    fn drop(&mut self) {
        self.close(Instant::now());
        let _ = self.shutdown();
    }
}

fn run_sidecar_installer(
    queue: Arc<SidecarInstallQueue>,
    context: SidecarInstallContext,
    limit: usize,
) {
    loop {
        let mut state = queue
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        while state.pending.is_empty() && state.deadline.is_none() {
            state = queue
                .changed
                .wait(state)
                .unwrap_or_else(|error| error.into_inner());
        }
        if state.pending.is_empty() {
            return;
        }
        let count = state.pending.len().min(SIDECAR_INSTALL_BATCH);
        let mut batch: [Option<SidecarInstallItem>; SIDECAR_INSTALL_BATCH] =
            std::array::from_fn(|_| None);
        for slot in batch.iter_mut().take(count) {
            *slot = state.pending.pop_front();
        }
        state.active += count;
        if let Some(metrics) = &context.metrics {
            metrics.sidecar_install_counts(
                state.pending.len(),
                state.active,
                limit,
                state.control_bytes,
            );
        }
        drop(state);
        let mut installed_items = 0usize;
        for item in batch.iter().flatten() {
            let expired = queue
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .deadline
                .is_some_and(|deadline| Instant::now() >= deadline);
            if expired {
                context.budget.sidecar_event("sidecar_install_dropped");
                continue;
            }
            #[cfg(test)]
            context.test.pause_if_requested();
            let path = context
                .directory
                .join(format!("{}.bao", safe_id(&item.chunk_id.0)));
            let _diagnostic = context.metrics.as_ref().and_then(|metrics| {
                metrics.coarse_diagnostic(
                    "persist_proof_install_sync",
                    item.proof.proof().encoded().len() as u64,
                )
            });
            let mut hash = std::collections::hash_map::DefaultHasher::new();
            path.hash(&mut hash);
            let index = (hash.finish() % OUTBOARD_REPAIR_SHARDS as u64) as usize;
            let installed = context.repairs[index]
                .lock()
                .map_err(|_| invalid("sidecar repair gate poisoned"))
                .and_then(|_repair| {
                    if !outboard_file_matches(&path, item.proof.proof()).unwrap_or(false) {
                        install_outboard_file(&path, item.proof.proof())?;
                    }
                    Ok(())
                });
            if installed.is_ok() {
                installed_items += 1;
            } else {
                context.budget.sidecar_event("sidecar_install_failed");
            }
        }
        #[cfg(test)]
        if installed_items != 0 {
            context.test.pause_before_directory_sync();
        }
        let directory_expired = queue
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline);
        if installed_items != 0 && directory_expired {
            // Files already installed remain optional and may be repaired on
            // a later read. Do not begin another filesystem barrier after the
            // absolute stop decision; active references retire only below.
            for _ in 0..installed_items {
                context.budget.sidecar_event("sidecar_install_dropped");
            }
        } else if installed_items != 0 {
            let _diagnostic = context
                .metrics
                .as_ref()
                .and_then(|metrics| metrics.coarse_diagnostic("persist_proof_dir_sync", 0));
            #[cfg(test)]
            let fail_directory = context
                .test
                .fail_directory_sync
                .swap(false, Ordering::Relaxed);
            #[cfg(not(test))]
            let fail_directory = false;
            if fail_directory || sync_outboard_directory(&context.directory).is_err() {
                context.budget.sidecar_event("sidecar_install_failed");
            }
        }
        #[cfg(test)]
        {
            let io = OUTBOARD_PERSIST_IO.with(|counts| std::mem::take(&mut *counts.borrow_mut()));
            let mut total = context.test.optional_io.lock().unwrap();
            total.writes += io.writes;
            total.file_syncs += io.file_syncs;
            total.renames += io.renames;
            total.directory_syncs += io.directory_syncs;
        }
        for item in batch.into_iter().flatten() {
            let SidecarInstallItem { chunk_id, proof } = item;
            // Never drop a charged proof while holding the queue mutex.
            drop(proof);
            let mut state = queue
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            state.unfinished.remove(&chunk_id);
            state.active -= 1;
            if let Some(metrics) = &context.metrics {
                metrics.sidecar_install_counts(
                    state.pending.len(),
                    state.active,
                    limit,
                    state.control_bytes,
                );
            }
            queue.changed.notify_all();
        }
    }
}

#[cfg(test)]
#[derive(Debug, Default)]
struct SidecarInstallTestControl {
    block: Mutex<Option<(std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<()>)>>,
    before_directory_sync:
        Mutex<Option<(std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<()>)>>,
    optional_io: Mutex<OutboardPersistIo>,
    fail_directory_sync: std::sync::atomic::AtomicBool,
    fail_worker_start: std::sync::atomic::AtomicBool,
}

#[cfg(test)]
impl SidecarInstallTestControl {
    fn pause_if_requested(&self) {
        let block = self.block.lock().unwrap().take();
        if let Some((entered, release)) = block {
            let _ = entered.send(());
            let _ = release.recv();
        }
    }
    fn pause_before_directory_sync(&self) {
        let block = self.before_directory_sync.lock().unwrap().take();
        if let Some((entered, release)) = block {
            let _ = entered.send(());
            let _ = release.recv();
        }
    }
}

#[cfg(test)]
#[derive(Debug, Default)]
struct CatalogCommitTestControl {
    ready: Mutex<Option<Arc<std::sync::Barrier>>>,
    before_prepare: Mutex<Option<PeerFillTestBlock>>,
    before_wal: Mutex<VecDeque<PeerFillTestBlock>>,
    fail_wal_step: std::sync::atomic::AtomicUsize,
    fail_chunks_dir: std::sync::atomic::AtomicBool,
    fail_prepare_after: std::sync::atomic::AtomicUsize,
}

#[cfg(test)]
impl CatalogCommitTestControl {
    fn before_group(&self) {
        let barrier = self.ready.lock().unwrap().clone();
        if let Some(barrier) = barrier {
            barrier.wait();
        }
    }

    fn pause_prepare(&self) -> Result<()> {
        let remaining = self.fail_prepare_after.load(Ordering::Acquire);
        if remaining != 0 && self.fail_prepare_after.fetch_sub(1, Ordering::AcqRel) == 1 {
            return Err(Error::coded(
                afs_error::IO_OTHER,
                "injected later body preparation failure",
            ));
        }
        let block = self.before_prepare.lock().unwrap().take();
        if let Some((entered, release)) = block {
            let _ = entered.send(());
            let _ = release.recv();
        }
        Ok(())
    }

    fn pause_wal(&self) {
        let block = self.before_wal.lock().unwrap().pop_front();
        if let Some((entered, release)) = block {
            let _ = entered.send(());
            let _ = release.recv();
        }
    }
}

#[cfg(test)]
type PeerFillTestBlock = (std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<()>);

#[derive(Debug)]
pub struct LocalChunkStore {
    node_id: String,
    device_id: String,
    device_epoch: u64,
    root: PathBuf,
    root_dir: File,
    chunks: PathBuf,
    staging: PathBuf,
    outboards: PathBuf,
    outboard_budget: Arc<OutboardBudget>,
    outboard_repairs: Arc<OutboardRepairGates>,
    catalog_path: PathBuf,
    catalog: Mutex<LocalCatalog>,
    catalog_committer: CatalogCommitter,
    next_staging: AtomicU64,
    gc_inventory_cursor: AtomicU64,
    read_work: Option<super::read_work::ReadWorkMetrics>,
    sidecar_installer: SidecarInstaller,
    #[cfg(test)]
    catalog_commit_test: CatalogCommitTestControl,
    #[cfg(test)]
    sidecar_install_test: Arc<SidecarInstallTestControl>,
    #[cfg(test)]
    peer_fill_test_blocks: Mutex<HashMap<ChunkId, PeerFillTestBlock>>,
    #[cfg(test)]
    peer_ready_test_block: Mutex<Option<PeerFillTestBlock>>,
    #[cfg(test)]
    peer_payload_test_block: Mutex<Option<PeerFillTestBlock>>,
    #[cfg(any(feature = "dfs", test))]
    peer_read_budget: Arc<PeerReadBudget>,
    #[cfg(any(feature = "dfs", test))]
    peer_read_cache: Mutex<PeerServeState>,
    #[cfg(any(feature = "dfs", test))]
    peer_serve_changed: Condvar,
}

impl LocalChunkStore {
    pub fn open(root: impl AsRef<Path>, node_id: impl Into<String>) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        let chunks = root.join("chunks");
        let staging = root.join("staging");
        let outboards = root.join("outboards");
        let mut new_ancestors = Vec::new();
        let mut ancestor = root.as_path();
        while !ancestor.as_os_str().is_empty() && !ancestor.exists() {
            if let Some(parent) = ancestor.parent() {
                new_ancestors.push(if parent.as_os_str().is_empty() {
                    PathBuf::from(".")
                } else {
                    parent.to_path_buf()
                });
                ancestor = parent;
            } else {
                break;
            }
        }
        fs::create_dir_all(&outboards).map_err(Error::from)?;
        fs::create_dir_all(&chunks).map_err(Error::from)?;
        fs::create_dir_all(&staging).map_err(Error::from)?;
        // Also repair a previous process's unconfirmed rename before healthy
        // replay can ACK it. A same-OS reopen is not itself a directory barrier.
        sync_store_directory(&chunks)?;
        sync_store_directory(&root)?;
        for parent in new_ancestors {
            sync_store_directory(&parent)?;
        }
        let device_epoch = open_device_epoch(&root)?;
        let root_dir = File::open(&root).map_err(Error::from)?;
        let catalog_path = root.join("catalog.wal");
        let catalog = LocalCatalog::recover(&catalog_path)?;
        // A prior failed sync may have left a complete transaction in the OS
        // cache. Validate first, then persist both WAL and its name before a
        // reopened store may issue a healthy replay ACK.
        if catalog_path.exists() {
            sync_store_catalog(&catalog_path)?;
            sync_store_directory(&root)?;
        }
        let store = Self {
            node_id: node_id.into(),
            device_id: "local-0".into(),
            device_epoch,
            root,
            root_dir,
            chunks,
            staging,
            outboards,
            outboard_budget: OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES),
            outboard_repairs: Arc::new(std::array::from_fn(|_| Mutex::new(()))),
            catalog_path,
            catalog: Mutex::new(catalog),
            catalog_committer: CatalogCommitter::default(),
            next_staging: AtomicU64::new(1),
            gc_inventory_cursor: AtomicU64::new(0),
            read_work: None,
            sidecar_installer: SidecarInstaller::default(),
            #[cfg(test)]
            catalog_commit_test: CatalogCommitTestControl::default(),
            #[cfg(test)]
            sidecar_install_test: Arc::new(SidecarInstallTestControl::default()),
            #[cfg(test)]
            peer_fill_test_blocks: Mutex::new(HashMap::new()),
            #[cfg(test)]
            peer_ready_test_block: Mutex::new(None),
            #[cfg(test)]
            peer_payload_test_block: Mutex::new(None),
            #[cfg(any(feature = "dfs", test))]
            peer_read_budget: Arc::new(PeerReadBudget::default()),
            #[cfg(any(feature = "dfs", test))]
            peer_read_cache: Mutex::new(PeerServeState::default()),
            #[cfg(any(feature = "dfs", test))]
            peer_serve_changed: Condvar::new(),
        };
        store.recover()?;
        Ok(store)
    }

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
    fn observe_peer_serve(&self, state: &PeerServeState) {
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
    fn reserve_peer_control(&self, bytes: usize) -> Result<PeerControlLease> {
        self.reserve_peer_control_until(bytes, Instant::now())
    }

    #[cfg(any(feature = "dfs", test))]
    fn reserve_peer_control_until(
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
    fn clear_peer_residents_for_test(&self) {
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

    #[cfg(any(feature = "dfs", test))]
    pub(crate) fn with_outboard_budget(mut self, budget: Arc<OutboardBudget>) -> Self {
        self.outboard_budget = budget;
        self
    }

    #[cfg(any(feature = "dfs", test))]
    pub(crate) fn with_read_work_metrics(
        mut self,
        metrics: super::read_work::ReadWorkMetrics,
    ) -> Self {
        self.catalog_committer.set_metrics(metrics.clone());
        self.read_work = Some(metrics);
        self.sidecar_installer.metrics = self.read_work.clone();
        if let Ok(state) = self.peer_read_cache.lock() {
            self.observe_peer_serve(&state);
        }
        {
            let state = self
                .sidecar_installer
                .queue
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            self.sidecar_installer.observe(&state);
        }
        self
    }

    #[cfg(any(feature = "dfs", test))]
    pub(crate) fn with_sidecar_install_queue(mut self, limit: usize) -> Result<Self> {
        if !(1..=SIDECAR_INSTALL_LIMIT).contains(&limit) {
            return Err(invalid("sidecar install queue must be 1..=64"));
        }
        self.sidecar_installer.limit = limit;
        {
            let state = self
                .sidecar_installer
                .queue
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            self.sidecar_installer.observe(&state);
        }
        Ok(self)
    }

    #[cfg(feature = "dfs")]
    pub(crate) fn stop_sidecar_installation(&self) {
        self.sidecar_installer
            .close(Instant::now() + SIDECAR_STOP_WAIT);
    }

    #[cfg(feature = "dfs")]
    pub(crate) fn shutdown_sidecar_installer(&self) -> Result<()> {
        self.sidecar_installer.shutdown()
    }

    #[cfg(any(feature = "dfs", test))]
    pub(crate) fn with_catalog_commit_queue(mut self, limit: usize) -> Result<Self> {
        self.catalog_committer.configure(limit)?;
        Ok(self)
    }

    #[cfg(any(feature = "dfs", test))]
    pub(crate) fn note_catalog_stop(&self) {
        self.catalog_committer
            .note_stop(Instant::now() + SIDECAR_STOP_WAIT);
    }

    #[cfg(any(feature = "dfs", test))]
    pub(crate) fn shutdown_catalog_committer(&self) -> Result<()> {
        self.catalog_committer.shutdown()
    }

    #[cfg(test)]
    pub(crate) fn catalog_commit_stats(&self) -> catalog_commit::CatalogCommitStats {
        self.catalog_committer.stats()
    }

    pub(super) fn coarse_diagnostic(
        &self,
        phase: &'static str,
        bytes: u64,
    ) -> Option<super::read_work::ReadCoarseDiagnosticSpan> {
        self.read_work.as_ref()?.coarse_diagnostic(phase, bytes)
    }

    #[cfg(any(feature = "dfs", test))]
    pub(super) fn read_diagnostic(
        &self,
        stage: super::read_work::ReadDiagnosticStage,
    ) -> Option<super::read_work::ReadDiagnosticSpan> {
        self.read_work.as_ref()?.diagnostic(stage)
    }

    #[cfg(feature = "dfs")]
    pub(crate) fn record_peer_disk_cache_event(&self, result: &'static str) {
        if let Some(metrics) = &self.read_work {
            metrics.peer_disk_cache_event(result);
        }
    }

    #[cfg(feature = "dfs")]
    pub(crate) fn record_peer_cache_fill_stage(
        &self,
        stage: &'static str,
        wall_ns: u64,
        cpu_ns: Option<u64>,
    ) {
        if let Some(metrics) = &self.read_work {
            metrics.peer_cache_fill_stage(stage, wall_ns, cpu_ns);
        }
    }

    #[cfg(feature = "dfs")]
    pub(crate) fn peer_disk_cache_fill_work(
        &self,
        bytes: usize,
        output: usize,
    ) -> Option<super::read_work::PeerCacheFillWork> {
        self.read_work
            .as_ref()
            .map(|metrics| metrics.peer_disk_cache_fill_work(bytes, output))
    }

    #[cfg(feature = "dfs")]
    pub(crate) fn record_source_cache_resources(
        &self,
        entries: usize,
        bytes: usize,
        peak: usize,
        limit: usize,
    ) {
        if let Some(metrics) = &self.read_work {
            metrics.source_cache_resources(entries, bytes, peak, limit);
        }
    }

    #[cfg(feature = "dfs")]
    pub(crate) fn record_read_hash(&self, stage: &'static str, bytes: u64) {
        if let Some(metrics) = &self.read_work {
            metrics.hash(stage, bytes);
        }
    }

    #[cfg(feature = "dfs")]
    pub(crate) fn record_read_copy(&self, stage: &'static str, bytes: u64) {
        if let Some(metrics) = &self.read_work {
            metrics.copy(stage, bytes);
        }
    }

    #[cfg(feature = "dfs")]
    pub(crate) fn record_peer_read_event(&self, event: &'static str) {
        if let Some(metrics) = &self.read_work {
            metrics.peer_read_event(event);
        }
    }

    pub fn capacity(&self) -> Result<FilesystemCapacity> {
        capacity_from_statvfs_fd(&self.root_dir).map_err(Error::from)
    }

    pub fn device_descriptor(&self) -> Result<StorageDeviceDescriptor> {
        let catalog = self
            .catalog
            .lock()
            .map_err(|_| invalid("local chunk catalog lock is poisoned"))?;
        Ok(StorageDeviceDescriptor {
            device_id: self.device_id.clone(),
            device_epoch: self.device_epoch,
            catalog_revision: catalog.revision,
            failure_domain: self.node_id.clone(),
        })
    }

    pub fn recover(&self) -> Result<RecoveryReport> {
        let mut report = RecoveryReport::default();
        for entry in fs::read_dir(&self.staging).map_err(Error::from)? {
            let entry = entry.map_err(Error::from)?;
            if entry.file_type().map_err(Error::from)?.is_file() {
                fs::remove_file(entry.path()).map_err(Error::from)?;
                report.removed_staging_files = report.removed_staging_files.saturating_add(1);
            }
        }
        for entry in fs::read_dir(&self.outboards).map_err(Error::from)? {
            let entry = entry.map_err(Error::from)?;
            if entry.file_type().map_err(Error::from)?.is_file()
                && entry.file_name().to_string_lossy().ends_with(".tmp")
            {
                fs::remove_file(entry.path()).map_err(Error::from)?;
            }
        }
        // Preserve the original startup metadata scan without cloning the whole
        // historical catalog. Hashing/quarantine/WAL work below runs after unlock.
        let catalog = self
            .catalog
            .lock()
            .map_err(|_| invalid("local chunk catalog lock is poisoned"))?;
        let mut damaged = Vec::new();
        for record in catalog.records.values() {
            if record.chunk.is_segment_crc32c()
                && (!record.chunk.valid_immutable_identity()
                    || !record.segment_crc.as_ref().is_some_and(|t| {
                        t.valid_shape(record.chunk.length)
                            && t.descriptor_digest() == record.chunk.content_digest
                    }))
            {
                return Err(corrupt(
                    &record.chunk.id,
                    "invalid persisted CRC32C descriptor",
                ));
            }
            if record.device_id != self.device_id
                || record.device_epoch != self.device_epoch
                || record.stored_length != record.chunk.length
                || record.stored_checksum != record.chunk.content_digest
            {
                return Err(invalid(
                    "durable local Chunk record is internally inconsistent",
                ));
            }
            if record.state != LocalChunkState::Durable {
                continue;
            }
            let path = self.path_for_record(record)?;
            match path.metadata() {
                Ok(metadata) if metadata.is_file() && metadata.len() == record.chunk.length => {
                    report.durable_chunks = report.durable_chunks.saturating_add(1);
                }
                Ok(_) => damaged.push(record.clone()),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    damaged.push(record.clone());
                }
                Err(error) => return Err(Error::from(error)),
            }
        }
        drop(catalog);
        for record in damaged {
            self.quarantine_if_invalid(&record.chunk.id)?;
        }
        for entry in fs::read_dir(&self.chunks).map_err(Error::from)? {
            let entry = entry.map_err(Error::from)?;
            let id = ChunkId::new(entry.file_name().to_string_lossy().into_owned());
            if !self
                .catalog
                .lock()
                .map_err(|_| invalid("local chunk catalog lock is poisoned"))?
                .records
                .contains_key(&id)
            {
                report.orphan_files = report.orphan_files.saturating_add(1);
            }
        }
        Ok(report)
    }

    fn chunk_path(&self, id: &ChunkId) -> PathBuf {
        self.chunks.join(&id.0)
    }

    fn path_for_record(&self, record: &LocalChunkRecord) -> Result<PathBuf> {
        match &record.location {
            PhysicalLocation::PerChunkFile { relative_path } => {
                if relative_path != &format!("chunks/{}", record.chunk.id.0) {
                    return Err(invalid("local chunk record has a non-canonical path"));
                }
                let path = self.root.join(relative_path);
                if !path.starts_with(&self.chunks) {
                    return Err(invalid("local chunk record escapes the chunk directory"));
                }
                Ok(path)
            }
        }
    }

    pub fn persist_batch(
        &self,
        staged: &[StagedChunk],
        target: &ReplicaTarget,
        placement_revision: u64,
        placement_epoch: u64,
    ) -> Result<Vec<ReplicaAck>> {
        let _diagnostic = self.coarse_diagnostic("persist_batch", 0);
        #[cfg(any(feature = "dfs", test))]
        let batch_diagnostic =
            self.read_diagnostic(super::read_work::ReadDiagnosticStage::PersistBatch);
        if target.node_id != self.node_id
            || target.device.device_id != self.device_id
            || target.device.device_epoch != self.device_epoch
        {
            return Err(invalid(
                "replica target does not identify this local chunk store",
            ));
        }
        let admission = self.admit_catalog(staged)?;
        let result = (|| -> Result<Vec<ReplicaAck>> {
            if self
                .catalog
                .lock()
                .map_err(|_| invalid("local chunk catalog lock is poisoned"))?
                .revision
                < target.device.catalog_revision
            {
                return Err(invalid(
                    "local catalog is older than the placement revision floor",
                ));
            }

            let mut records = Vec::new();
            let mut installed_chunk_ids = HashSet::new();
            for item in staged {
                let mut replace_existing = false;
                let existing = self
                    .catalog
                    .lock()
                    .map_err(|_| invalid("local chunk catalog lock is poisoned"))?
                    .records
                    .get(&item.chunk.id)
                    .cloned();
                let set_capacity = installed_chunk_ids.capacity()
                    * (std::mem::size_of::<ChunkId>() + std::mem::size_of::<usize>() + 1)
                    * 2
                    + installed_chunk_ids
                        .iter()
                        .map(|id: &ChunkId| id.0.capacity())
                        .sum::<usize>();
                self.observe_catalog_preparing(
                    &admission,
                    catalog_commit::record_capacity(&records)
                        + set_capacity
                        + existing
                            .as_ref()
                            .map_or(0, catalog_commit::record_allocation),
                )?;
                #[cfg(test)]
                self.catalog_commit_test.pause_prepare()?;
                if let Some(existing) = existing.as_ref() {
                    if existing.chunk != item.chunk {
                        return Err(invalid(
                            "immutable Chunk ID collides with another local record",
                        ));
                    }
                    if existing.state == LocalChunkState::Quarantined {
                        replace_existing = true;
                    } else {
                        match if existing.segment_crc.is_some() {
                            self.verify_crc_file(
                                &self.path_for_record(existing)?,
                                &item.chunk,
                                existing.segment_crc.as_ref().ok_or_else(|| {
                                    corrupt(&item.chunk.id, "CRC descriptor missing")
                                })?,
                            )
                        } else {
                            verify_file(&self.path_for_record(existing)?, &item.chunk)
                        } {
                            Ok(()) => continue,
                            Err(error) if verified_copy_is_invalid(&error) => {
                                replace_existing = true
                            }
                            Err(error) => return Err(error),
                        }
                    }
                }
                let temp_path = self.staging.join(format!(
                    "{}.{}.tmp",
                    safe_id(&item.operation_id.0),
                    self.next_staging.fetch_add(1, Ordering::Relaxed)
                ));
                let mut file = OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .open(&temp_path)
                    .map_err(Error::from)?;
                {
                    let _diagnostic =
                        self.coarse_diagnostic("persist_body_write", item.chunk.length);
                    file.write_all(item.bytes()).map_err(Error::from)?;
                }
                {
                    let _diagnostic =
                        self.coarse_diagnostic("persist_body_sync", item.chunk.length);
                    file.sync_all().map_err(Error::from)?;
                }
                let final_path = self.chunk_path(&item.chunk.id);
                if replace_existing {
                    // Publish a new inode; never truncate a file held by readers.
                    // Its immutable content identity is unchanged. Directory and
                    // catalog barriers below still precede the durable ACK.
                    fs::rename(&temp_path, &final_path).map_err(Error::from)?;
                    admission.namespace_changed();
                } else {
                    match fs::hard_link(&temp_path, &final_path) {
                        Ok(()) => admission.namespace_changed(),
                        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                            if let Some(table) = &item.segment_crc {
                                self.verify_crc_file(&final_path, &item.chunk, table)?;
                                // Adopting a synced but unpublished orphan still
                                // requires the namespace barrier before its ACK.
                                admission.namespace_changed();
                            } else {
                                verify_file(&final_path, &item.chunk)?;
                            }
                        }
                        Err(error) => return Err(Error::from(error)),
                    }
                    fs::remove_file(&temp_path).map_err(Error::from)?;
                }
                if installed_chunk_ids.insert(item.chunk.id.clone()) {
                    records.push(LocalChunkRecord {
                        location: PhysicalLocation::PerChunkFile {
                            relative_path: format!("chunks/{}", item.chunk.id.0),
                        },
                        segment_crc: item.segment_crc.as_deref().cloned(),
                        stored_length: item.chunk.length,
                        stored_checksum: item.chunk.content_digest.clone(),
                        device_id: self.device_id.clone(),
                        device_epoch: self.device_epoch,
                        chunk: item.chunk.clone(),
                        encoding: PhysicalEncoding::Raw,
                        state: LocalChunkState::Durable,
                        catalog_revision: 0,
                    });
                }
                let set_capacity = installed_chunk_ids.capacity()
                    * (std::mem::size_of::<ChunkId>() + std::mem::size_of::<usize>() + 1)
                    * 2
                    + installed_chunk_ids
                        .iter()
                        .map(|id| id.0.capacity())
                        .sum::<usize>();
                self.observe_catalog_preparing(
                    &admission,
                    catalog_commit::record_capacity(&records)
                        + set_capacity
                        + temp_path.capacity()
                        + final_path.capacity()
                        + existing
                            .as_ref()
                            .map_or(0, catalog_commit::record_allocation),
                )?;
            }
            drop(installed_chunk_ids);
            let catalog_revision = self.commit_catalog(&admission, records)?;
            admission.confirm();
            // Optional proof installation remains after every required durability barrier.
            self.sidecar_installer.enqueue_batch(self, staged);
            let acks = staged
                .iter()
                .map(|item| ReplicaAck {
                    operation_id: item.operation_id.clone(),
                    chunk_id: item.chunk.id.clone(),
                    placement_revision,
                    placement_epoch,
                    node_id: target.node_id.clone(),
                    node_epoch: target.node_epoch,
                    device_id: target.device.device_id.clone(),
                    device_epoch: target.device.device_epoch,
                    catalog_revision,
                    persisted_bytes: item.chunk.length,
                    verified_digest: item.chunk.content_digest.clone(),
                })
                .collect();
            #[cfg(any(feature = "dfs", test))]
            if let Some(diagnostic) = batch_diagnostic {
                diagnostic.finish(true);
            }
            Ok(acks)
        })();
        if let Err(error) = &result {
            admission.fail_if_unconfirmed(error.clone());
        }
        result
    }

    pub fn persist(
        &self,
        staged: &StagedChunk,
        target: &ReplicaTarget,
        placement_revision: u64,
        placement_epoch: u64,
    ) -> Result<ReplicaAck> {
        self.persist_batch(
            std::slice::from_ref(staged),
            target,
            placement_revision,
            placement_epoch,
        )?
        .pop()
        .ok_or_else(|| invalid("local batch finalize returned no ReplicaAck"))
    }

    pub fn read_at(&self, chunk_id: &ChunkId, offset: u64, out: &mut [u8]) -> Result<usize> {
        self.open_verified(chunk_id)?
            .read_at(offset, out)
            .inspect_err(|error| {
                if verified_copy_is_invalid(error) {
                    self.try_quarantine(chunk_id);
                }
            })
    }

    /// Fill a caller-owned, unpublished range with one complete integrity scan.
    /// The output may be dirtied on failure and must not be returned to a peer
    /// until this operation succeeds. No verified reader or cache is retained.
    #[cfg(any(all(feature = "dfs", feature = "rdma"), test))]
    pub(crate) fn read_range_at_uncommitted(
        &self,
        chunk_id: &ChunkId,
        offset: u64,
        out: &mut [u8],
    ) -> Result<usize> {
        let reader = self.open_cataloged(chunk_id)?;
        let result = verified_ranges_into(
            &reader.file,
            &reader.chunk,
            &[VerifiedRangeCopy {
                chunk_offset: offset,
                length: out.len(),
                output_offset: 0,
            }],
            out,
            true,
            self.read_work.as_ref(),
        );
        result.map(|counts| counts[0]).inspect_err(|error| {
            if verified_copy_is_invalid(error) {
                self.try_quarantine(chunk_id);
            }
        })
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
    fn reserve_peer_read_payload_until(
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
    fn peer_catalog_record(&self, chunk_id: &ChunkId) -> Result<LocalChunkRecord> {
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
    ) -> Result<Option<super::chunk_crc32c::CrcTable>> {
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
        self.read_peer_range_until(chunk_id, offset, length, self.peer_serve_deadline())
    }

    #[cfg(any(feature = "dfs", test))]
    pub(crate) fn read_peer_range_until(
        &self,
        chunk_id: &ChunkId,
        offset: u64,
        length: usize,
        deadline: Instant,
    ) -> Result<PeerReadPayload> {
        let _reader = self.begin_peer_serve_read()?;
        // Legacy long identities remain readable, but are not retained as keys.
        let cacheable = chunk_id.0.len() <= PEER_SERVE_ID_LIMIT;
        if !cacheable {
            let payload =
                self.read_peer_range_private(chunk_id, offset, length, &mut None, false, deadline)?;
            self.ensure_peer_serve_open()?;
            return Ok(payload);
        }
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
        let (flight, leading) = if let Some(flight) = existing {
            (flight, false)
        } else {
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
                    break (flight, false);
                }
                if let Some(free) = state.flights.iter_mut().find(|flight| flight.is_none()) {
                    *free = Some(candidate.clone());
                    self.observe_peer_serve(&state);
                    break (candidate, true);
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
        };
        if !leading {
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
            let payload =
                self.read_peer_range_private(chunk_id, offset, length, &mut slot, true, deadline)?;
            self.ensure_peer_serve_open()?;
            return Ok(payload);
        }
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
        let result =
            self.read_peer_range_private(chunk_id, offset, length, &mut slot, true, deadline);
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
    fn read_peer_range_private(
        &self,
        chunk_id: &ChunkId,
        offset: u64,
        length: usize,
        slot: &mut Option<Arc<PeerVerifiedObject>>,
        cacheable: bool,
        deadline: Instant,
    ) -> Result<PeerReadPayload> {
        // Includes catalog/file identity work and actual read/verification;
        // excludes slot acquisition and the optional sidecar repair below.
        let read_verify = self.coarse_diagnostic("peer_read_verify", length as u64);
        let object_diagnostic = self.read_work.as_ref().and_then(|metrics| {
            metrics.object_diagnostic(
                super::read_work::ReadObjectDiagnosticStage::PeerReadVerify,
                &chunk_id.0,
            )
        });
        let mut repair_proof = None;
        let result = (|| {
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
            if record.chunk.is_segment_crc32c() {
                let scratch = super::chunk_crc32c::SEGMENT;
                let mut reservation = self
                    .reserve_peer_read_payload_until(allowed.saturating_add(scratch), deadline)?;
                let mut bytes = Vec::new();
                bytes
                    .try_reserve_exact(allowed)
                    .map_err(|_| peer_payload_capacity())?;
                reservation.retain_capacity(bytes.capacity().saturating_add(scratch))?;
                bytes.resize(allowed, 0);
                let reader = self.open_cataloged(chunk_id)?;
                if reader.read_ranges_at_uncommitted(
                    &[VerifiedRangeCopy {
                        chunk_offset: offset,
                        length: allowed,
                        output_offset: 0,
                    }],
                    &mut bytes,
                )? != vec![allowed]
                    || self.peer_catalog_record(chunk_id)? != record
                    || PeerFileIdentity::read(&file)? != before
                    || PeerFileIdentity::read(&File::open(self.path_for_record(&record)?)?)?
                        != before
                {
                    return Err(corrupt(
                        chunk_id,
                        "CRC peer range changed during verification",
                    ));
                }
                return Ok(PeerReadPayload {
                    object: Arc::new(PeerVerifiedObject {
                        bytes,
                        outboard: None,
                        _reservation: reservation,
                        identity: None,
                        _control: self.peer_object_control(None, deadline)?,
                    }),
                    start: 0,
                    length: allowed,
                });
            }
            if let Some(object) = slot.as_ref()
                && object.identity.as_ref() == Some(&(record.clone(), before.clone()))
            {
                if self.peer_catalog_record(chunk_id)? != record
                    || PeerFileIdentity::read(&file)? != before
                {
                    return Err(corrupt(chunk_id, "identity changed during cached read"));
                }
                if let Some(metrics) = &self.read_work {
                    metrics.peer_object_cache("hit");
                }
                return Ok(PeerReadPayload {
                    object: object.clone(),
                    start: usize::try_from(offset.min(record.chunk.length))
                        .map_err(|_| invalid("Chunk offset is too large"))?,
                    length: allowed,
                });
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
                && (offset != 0 || allowed as u64 != record.chunk.length)
                && ChunkObject::from_content_addressed_id(chunk_id).as_ref() == Some(&record.chunk)
            {
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
                        let mut reservation =
                            match self.reserve_peer_read_payload_until(allowed, deadline) {
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
                            &file,
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
                            && PeerFileIdentity::read(&file)? == before
                            && self.peer_catalog_record(chunk_id)? == record
                        {
                            return Ok(PeerReadPayload {
                                object: Arc::new(PeerVerifiedObject {
                                    bytes,
                                    outboard: None,
                                    _reservation: reservation,
                                    identity: None,
                                    _control: self.peer_object_control(None, deadline)?,
                                }),
                                start: 0,
                                length: allowed,
                            });
                        }
                        capacity_fallback = verified
                            .as_ref()
                            .is_err_and(|error| error.code() == afs_error::IO_CAPACITY);
                        // Drop the exact-range private allocation before the old
                        // full-root fallback attempts its original body admission.
                        drop(bytes);
                        drop(reservation);
                    }
                    Err(error) if error.code() == afs_error::IO_CAPACITY => {
                        capacity_fallback = true;
                    }
                    _ => {}
                }
            }
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
                && ChunkObject::from_content_addressed_id(chunk_id).as_ref()
                    == Some(&record.chunk))
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
            if PeerFileIdentity::read(&file)? != before
                || self.peer_catalog_record(chunk_id)? != record
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
            repair_proof = outboard.as_ref().map(Arc::clone);
            let outboard = outboard.filter(|proof| proof.try_mark_resident());
            let control = self.peer_object_control(resident.then_some(&record), deadline)?;
            let object = Arc::new(PeerVerifiedObject {
                bytes,
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
        })();
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
            repair_outboard_if_needed(
                &self.outboards.join(format!("{}.bao", safe_id(&chunk_id.0))),
                proof,
                &self.outboard_repairs,
            );
        }
        if let Err(error) = &result
            && verified_copy_is_invalid(error)
        {
            self.try_quarantine(chunk_id);
        }
        result
    }

    pub fn open_verified(&self, chunk_id: &ChunkId) -> Result<PinnedChunkReader> {
        let reader = self.open_cataloged(chunk_id)?;
        reader.verify_full_and_build().inspect_err(|error| {
            if verified_copy_is_invalid(error) {
                self.try_quarantine(chunk_id);
            }
        })?;
        Ok(reader)
    }

    /// Pin the exact receipt object without an eager full scan. Returning this
    /// descriptor is not an integrity claim: read_at/read_ranges still validate
    /// the delivered coverage groups or use the existing full-scan fallback.
    #[cfg(any(feature = "dfs", test))]
    pub(crate) fn open_receipt_reader(&self, expected: &ChunkObject) -> Result<PinnedChunkReader> {
        let reader = self.open_cataloged(&expected.id)?;
        if reader.chunk != *expected {
            return Err(corrupt(
                &expected.id,
                "catalog differs from native receipt object",
            ));
        }
        Ok(reader)
    }

    // Pin the cataloged inode without claiming its integrity. Callers must
    // verify requested coverage groups or the complete object before delivery.
    // Direct verifier also handles unregistered orphans and missing files. It must
    // not call open_cataloged, whose read-error quarantine hook would recurse.
    fn verify_crc_file(
        &self,
        path: &Path,
        chunk: &ChunkObject,
        table: &super::chunk_crc32c::CrcTable,
    ) -> Result<()> {
        let file = File::open(path).map_err(Error::from)?;
        let identity = PeerFileIdentity::read(&file)?;
        let reader = PinnedChunkReader {
            segment_crc: Some(table.clone()),
            file,
            chunk: chunk.clone(),
            outboard_path: PathBuf::new(),
            outboard_budget: self.outboard_budget.clone(),
            outboard_repairs: self.outboard_repairs.clone(),
            outboard: Mutex::new(None),
            proof_identity: Mutex::new(identity),
            read_work: None,
        };
        reader.verify_full_and_build()
    }

    fn open_cataloged(&self, chunk_id: &ChunkId) -> Result<PinnedChunkReader> {
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
        let file = File::open(self.path_for_record(&record)?)
            .map_err(Error::from)
            .inspect_err(|error| {
                if verified_copy_is_invalid(error) {
                    self.try_quarantine(chunk_id);
                }
            })?;
        if let Some(expected) = ChunkObject::from_content_addressed_id(chunk_id)
            && expected != record.chunk
        {
            return Err(corrupt(
                chunk_id,
                "catalog differs from content-addressed identity",
            ));
        }
        if record.chunk.is_segment_crc32c() != record.segment_crc.is_some() {
            return Err(corrupt(
                chunk_id,
                "CRC32C format and checksum descriptor differ",
            ));
        }
        if record.chunk.is_segment_crc32c() {
            let table = record
                .segment_crc
                .as_ref()
                .ok_or_else(|| corrupt(chunk_id, "CRC descriptor missing"))?;
            if !table.valid_shape(record.chunk.length)
                || table.descriptor_digest() != record.chunk.content_digest
            {
                return Err(corrupt(chunk_id, "CRC descriptor identity differs"));
            }
        }
        let proof_identity = Mutex::new(PeerFileIdentity::read(&file)?);
        Ok(PinnedChunkReader {
            segment_crc: record.segment_crc.clone(),
            file,
            proof_identity,
            outboard_path: self
                .outboards
                .join(format!("{}.bao", safe_id(&record.chunk.id.0))),
            outboard_budget: self.outboard_budget.clone(),
            outboard_repairs: self.outboard_repairs.clone(),
            outboard: Mutex::new(None),
            chunk: record.chunk,
            read_work: self.read_work.clone(),
        })
    }

    /// Serialize with installation, then verify outside the publication lock.
    /// An old failed reader must not quarantine a healthy replacement inode.
    pub(crate) fn quarantine_if_invalid(&self, chunk_id: &ChunkId) -> Result<()> {
        let admission = self.catalog_committer.admit(
            std::iter::once(chunk_id),
            catalog_commit::charge_for(std::iter::once(chunk_id), &self.device_id),
        )?;
        let record = self
            .catalog
            .lock()
            .map_err(|_| invalid("local chunk catalog lock is poisoned"))?
            .records
            .get(chunk_id)
            .cloned();
        let Some(mut record) = record else {
            return Ok(());
        };
        if record.state == LocalChunkState::Quarantined {
            return Ok(());
        }
        match if record.segment_crc.is_some() {
            self.verify_crc_file(
                &self.path_for_record(&record)?,
                &record.chunk,
                record
                    .segment_crc
                    .as_ref()
                    .ok_or_else(|| corrupt(&record.chunk.id, "CRC descriptor missing"))?,
            )
        } else {
            verify_file(&self.path_for_record(&record)?, &record.chunk)
        } {
            Ok(()) => Ok(()),
            Err(error) if verified_copy_is_invalid(&error) => {
                record.state = LocalChunkState::Quarantined;
                self.commit_catalog(&admission, vec![record]).map(|_| ())
            }
            Err(error) => Err(error),
        }
    }

    pub(crate) fn try_quarantine(&self, chunk_id: &ChunkId) {
        if let Err(error) = self.quarantine_if_invalid(chunk_id) {
            // Read fallback can still succeed. No durable quarantine or report
            // is claimed if its catalog barrier fails; every read rechecks.
            afs_logging::warn!("dfs.local_quarantine_failed";
                "chunk" => chunk_id.0.clone(), "error" => error.to_string());
        }
    }

    #[cfg(any(feature = "dfs", test))]
    pub(crate) fn quarantined_chunks(&self) -> Result<Vec<LocalChunkRecord>> {
        let catalog = self
            .catalog
            .lock()
            .map_err(|_| invalid("local chunk catalog lock is poisoned"))?;
        let mut records: Vec<_> = catalog
            .records
            .values()
            .filter(|record| record.state == LocalChunkState::Quarantined)
            .cloned()
            .collect();
        records.sort_by(|left, right| {
            (left.catalog_revision, &left.chunk.id).cmp(&(right.catalog_revision, &right.chunk.id))
        });
        Ok(records)
    }
}

static OUTBOARD_FILE_SEQUENCE: AtomicU64 = AtomicU64::new(1);
fn load_outboard_file(
    path: &Path,
    chunk: &ChunkObject,
    budget: &Arc<OutboardBudget>,
) -> Result<SharedOutboard> {
    use std::io::Read;
    let mut file = File::open(path).map_err(Error::from)?;
    let length = usize::try_from(file.metadata().map_err(Error::from)?.len())
        .map_err(|_| invalid("outboard file length too large"))?;
    if length > MAX_ENCODED_OUTBOARD_BYTES {
        return Err(corrupt(&chunk.id, "oversized outboard"));
    }
    let lease = budget
        .try_reserve(ChunkOutboard::resident_charge(MAX_ENCODED_OUTBOARD_BYTES)?)
        .ok_or_else(|| {
            Error::coded(afs_error::IO_CAPACITY, "outboard metadata budget exhausted")
        })?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(length)
        .map_err(|_| Error::coded(afs_error::IO_CAPACITY, "outboard allocation refused"))?;
    bytes.resize(length, 0);
    file.read_exact(&mut bytes).map_err(Error::from)?;
    if file.metadata().map_err(Error::from)?.len() != length as u64 {
        return Err(corrupt(&chunk.id, "outboard length changed"));
    }
    ChunkOutboard::from_encoded(chunk, bytes, lease)
}
fn outboard_capacity() -> Error {
    Error::coded(
        afs_error::IO_CAPACITY,
        "outboard range working capacity exhausted",
    )
}

// A healthy sidecar is disposable metadata: compare every byte before reuse,
// without adding its optional file barrier back to a durable body ACK.
fn outboard_file_matches(path: &Path, proof: &SharedOutboard) -> Result<bool> {
    use std::io::Read;
    let mut file = File::open(path).map_err(Error::from)?;
    let metadata = file.metadata().map_err(Error::from)?;
    if !metadata.is_file() || metadata.len() != proof.encoded().len() as u64 {
        return Ok(false);
    }
    let mut scratch = [0; 1024];
    for expected in proof.encoded().chunks(scratch.len()) {
        let observed = &mut scratch[..expected.len()];
        file.read_exact(observed).map_err(Error::from)?;
        if observed != expected {
            return Ok(false);
        }
    }
    Ok(file.metadata().map_err(Error::from)?.len() == proof.encoded().len() as u64)
}

fn repair_outboard_if_needed(path: &Path, proof: &SharedOutboard, gates: &OutboardRepairGates) {
    let mut hash = std::collections::hash_map::DefaultHasher::new();
    path.hash(&mut hash);
    let index = (hash.finish() % OUTBOARD_REPAIR_SHARDS as u64) as usize;
    let Ok(_repair) = gates[index].lock() else {
        return;
    };
    // Compare again under the shared gate: another reader may have repaired
    // this exact sidecar after our body scan. No catalog or body is rewritten.
    if !outboard_file_matches(path, proof).unwrap_or(false) {
        let _ = persist_outboard_file(path, proof);
    }
}

fn persist_outboard_file(path: &Path, proof: &SharedOutboard) -> Result<()> {
    install_outboard_file(path, proof)?;
    let parent = path
        .parent()
        .ok_or_else(|| invalid("outboard path has no parent"))?;
    sync_outboard_directory(parent)
}

fn install_outboard_file(path: &Path, proof: &SharedOutboard) -> Result<()> {
    #[cfg(test)]
    OUTBOARD_INSTALL_HOOK.with(|hook| {
        if let Some(hook) = hook.borrow_mut().take() {
            hook();
        }
    });
    let parent = path
        .parent()
        .ok_or_else(|| invalid("outboard path has no parent"))?;
    let sequence = OUTBOARD_FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let temp = parent.join(format!(".{}.{}.tmp", std::process::id(), sequence));
    let result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)
            .map_err(Error::from)?;
        file.write_all(proof.encoded()).map_err(Error::from)?;
        #[cfg(test)]
        OUTBOARD_PERSIST_IO.with(|counts| counts.borrow_mut().writes += 1);
        file.sync_all().map_err(Error::from)?;
        #[cfg(test)]
        OUTBOARD_PERSIST_IO.with(|counts| counts.borrow_mut().file_syncs += 1);
        fs::rename(&temp, path).map_err(Error::from)?;
        #[cfg(test)]
        OUTBOARD_PERSIST_IO.with(|counts| counts.borrow_mut().renames += 1);
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result
}

fn sync_outboard_directory(path: &Path) -> Result<()> {
    #[cfg(test)]
    if FAIL_OUTBOARD_DIRECTORY_SYNC.with(|fail| fail.replace(false)) {
        return Err(Error::from(std::io::Error::other(
            "injected outboard directory-sync failure",
        )));
    }
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(Error::from)?;
    #[cfg(test)]
    OUTBOARD_PERSIST_IO.with(|counts| counts.borrow_mut().directory_syncs += 1);
    Ok(())
}

fn verified_copy_is_invalid(error: &Error) -> bool {
    matches!(
        error.kind(),
        afs_error::ErrorKind::DataLoss | afs_error::ErrorKind::NotFound
    )
}

#[cfg(test)]
impl ChunkStore for LocalChunkStore {
    fn put_batch(&self, staged: Vec<StagedChunk>) -> Result<Vec<ChunkReceipt>> {
        let descriptor = self.device_descriptor()?;
        let target = ReplicaTarget {
            node_id: self.node_id.clone(),
            node_epoch: 1,
            data_endpoint: String::new(),
            device: descriptor,
        };
        let acks = self.persist_batch(&staged, &target, 1, 1)?;
        Ok(staged
            .into_iter()
            .zip(acks)
            .map(|(item, ack)| ChunkReceipt {
                external_commit: None,
                operation_id: item.operation_id,
                chunk: item.chunk,
                placement_revision: 1,
                placement_epoch: 1,
                replica_group_id: ReplicaGroupId::new(format!("local:{}", self.node_id)),
                durable_acks: vec![ack],
            })
            .collect())
    }

    fn read_at(&self, chunk_id: &ChunkId, offset: u64, out: &mut [u8]) -> Result<usize> {
        LocalChunkStore::read_at(self, chunk_id, offset, out)
    }

    fn capacity(&self) -> Result<FilesystemCapacity> {
        LocalChunkStore::capacity(self)
    }
}

#[cfg(test)]
fn append_catalog_txn(
    path: &Path,
    txn: &CatalogTxn,
    metrics: Option<&super::read_work::ReadWorkMetrics>,
) -> Result<()> {
    append_catalog_txn_observed(path, txn, metrics, |_| Ok(()))
}

fn append_catalog_txn_observed(
    path: &Path,
    txn: &CatalogTxn,
    metrics: Option<&super::read_work::ReadWorkMetrics>,
    observe_capacity: impl FnOnce(usize) -> Result<()>,
) -> Result<()> {
    let mut encoded = serde_json::to_vec(txn)
        .map_err(|error| invalid(format!("cannot encode local chunk catalog: {error}")))?;
    encoded.push(b'\n');
    // Reserve room for the optional separator before measuring actual capacity.
    encoded.reserve(1);
    observe_capacity(encoded.capacity())?;
    let mut file = OpenOptions::new()
        .create(true)
        .read(true)
        .append(true)
        .open(path)
        .map_err(Error::from)?;
    let length = file.metadata().map_err(Error::from)?.len();
    if length != 0 {
        let mut last = [0_u8; 1];
        if read_positioned(&file, length - 1, &mut last)? != 1 {
            return Err(invalid("local catalog changed while preparing an append"));
        }
        if last[0] != b'\n' {
            // Recovery accepts a complete final JSON transaction without its
            // newline. Preserve that transaction and separate the next one.
            encoded.insert(0, b'\n');
        }
    }
    {
        let _diagnostic = metrics.and_then(|metrics| {
            metrics.coarse_diagnostic("persist_catalog_write", encoded.len() as u64)
        });
        file.write_all(&encoded).map_err(Error::from)?;
    }
    {
        let _diagnostic = metrics.and_then(|metrics| {
            metrics.coarse_diagnostic("persist_catalog_sync", encoded.len() as u64)
        });
        file.sync_all().map_err(Error::from)?;
    }
    let parent = path
        .parent()
        .ok_or_else(|| invalid("local chunk catalog has no parent directory"))?;
    let _diagnostic =
        metrics.and_then(|metrics| metrics.coarse_diagnostic("persist_catalog_parent_sync", 0));
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(Error::from)
}

fn catalog_checksum(revision: u64, records: &[LocalChunkRecord]) -> Result<String> {
    let encoded = serde_json::to_vec(&(revision, records))
        .map_err(|error| invalid(format!("cannot checksum local chunk catalog: {error}")))?;
    Ok(blake3::hash(&encoded).to_hex().to_string())
}

fn sync_store_catalog(path: &Path) -> Result<()> {
    #[cfg(test)]
    if FAIL_STORE_CATALOG_SYNC.with(|fail| fail.replace(false)) {
        return Err(Error::coded(
            afs_error::IO_OTHER,
            "injected startup catalog file barrier failure",
        ));
    }
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(Error::from)?;
    #[cfg(test)]
    STORE_CATALOG_SYNCS.with(|syncs| {
        if let Some(syncs) = syncs.borrow_mut().as_mut() {
            syncs.push(path.to_path_buf());
        }
    });
    Ok(())
}

fn sync_store_directory(path: &Path) -> Result<()> {
    #[cfg(test)]
    if FAIL_STORE_DIRECTORY_SYNC.with(|fail| fail.replace(false)) {
        return Err(Error::coded(
            afs_error::IO_OTHER,
            "injected startup directory barrier failure",
        ));
    }
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(Error::from)?;
    #[cfg(test)]
    STORE_DIRECTORY_SYNCS.with(|syncs| {
        if let Some(syncs) = syncs.borrow_mut().as_mut() {
            syncs.push(path.to_path_buf());
        }
    });
    Ok(())
}

fn open_device_epoch(root: &Path) -> Result<u64> {
    let path = root.join("device_epoch");
    if path.exists() {
        let value = fs::read_to_string(path).map_err(Error::from)?;
        return value
            .trim()
            .parse::<u64>()
            .map_err(|_| invalid("local chunk device epoch is corrupt"));
    }
    let epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| invalid("system clock is before UNIX epoch"))?
        .as_nanos() as u64
        ^ u64::from(std::process::id());
    let temp = root.join("device_epoch.tmp");
    if temp.exists() {
        fs::remove_file(&temp).map_err(Error::from)?;
    }
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temp)
        .map_err(Error::from)?;
    writeln!(file, "{epoch}").map_err(Error::from)?;
    file.sync_all().map_err(Error::from)?;
    fs::rename(&temp, &path).map_err(Error::from)?;
    File::open(root)
        .and_then(|directory| directory.sync_all())
        .map_err(Error::from)?;
    Ok(epoch)
}

fn verify_file(path: &Path, chunk: &ChunkObject) -> Result<()> {
    let file = File::open(path).map_err(Error::from)?;
    verified_range(&file, chunk, 0, 0, None).map(|_| ())
}

/// Hash the pinned file once and retain only the requested overlap. The
/// temporary range is bounded by the caller's output; scan memory is 64KiB.
/// Nothing is published to the caller until all content checks pass.
fn verified_range(
    file: &File,
    chunk: &ChunkObject,
    offset: u64,
    length: usize,
    metrics: Option<&super::read_work::ReadWorkMetrics>,
) -> Result<Vec<u8>> {
    offset
        .checked_add(length as u64)
        .filter(|end| *end <= chunk.length)
        .ok_or_else(|| invalid("verified Chunk range exceeds its length"))?;
    let mut bytes = vec![0; length];
    verified_ranges_into(
        file,
        chunk,
        &[VerifiedRangeCopy {
            chunk_offset: offset,
            length,
            output_offset: 0,
        }],
        &mut bytes,
        true,
        metrics,
    )?;
    Ok(bytes)
}

fn verified_ranges_into(
    file: &File,
    chunk: &ChunkObject,
    ranges: &[VerifiedRangeCopy],
    out: &mut [u8],
    force_scan: bool,
    metrics: Option<&super::read_work::ReadWorkMetrics>,
) -> Result<Vec<usize>> {
    verified_ranges_into_building(file, chunk, ranges, out, force_scan, metrics, None)
        .map(|(counts, _)| counts)
}

fn verified_ranges_into_building(
    file: &File,
    chunk: &ChunkObject,
    ranges: &[VerifiedRangeCopy],
    out: &mut [u8],
    force_scan: bool,
    metrics: Option<&super::read_work::ReadWorkMetrics>,
    mut builder: Option<OutboardBuilder>,
) -> Result<(Vec<usize>, Option<SharedOutboard>)> {
    let mut counts = Vec::with_capacity(ranges.len());
    let mut active = Vec::new();
    for range in ranges.iter().copied() {
        let allowed = if range.chunk_offset >= chunk.length || range.length == 0 {
            0
        } else {
            usize::try_from((chunk.length - range.chunk_offset).min(range.length as u64))
                .map_err(|_| invalid("Chunk read length is too large"))?
        };
        let end = range
            .output_offset
            .checked_add(allowed)
            .ok_or_else(|| invalid("verified Chunk output range overflow"))?;
        if end > out.len() {
            return Err(invalid("verified Chunk output range exceeds buffer"));
        }
        counts.push(allowed);
        if allowed != 0 {
            active.push((range, allowed));
        }
    }
    if active.is_empty() && !force_scan {
        return Ok((counts, None));
    }
    if file.metadata().map_err(Error::from)?.len() != chunk.length {
        return Err(corrupt(&chunk.id, "length mismatch"));
    }
    let mut work = metrics.map(super::read_work::ReadWorkMetrics::scan);
    let mut scratch = [0; 64 * 1024];
    let mut hasher = blake3::Hasher::new();
    let mut position = 0;
    while position < chunk.length {
        let limit = (chunk.length - position).min(scratch.len() as u64) as usize;
        let count = read_positioned(file, position, &mut scratch[..limit])?;
        if count == 0 {
            return Err(corrupt(&chunk.id, "ended during verification"));
        }
        if let Some(builder) = &mut builder {
            builder.update(&scratch[..count])?;
        } else {
            hasher.update(&scratch[..count]);
        }
        if let Some(work) = &mut work {
            work.bytes += count as u64;
        }
        #[cfg(test)]
        VERIFIED_SCAN_BYTES.with(|bytes| bytes.set(bytes.get() + count as u64));
        let next = position + count as u64;
        for (range, allowed) in &active {
            let range_end = range.chunk_offset + *allowed as u64;
            let overlap_start = position.max(range.chunk_offset);
            let overlap_end = next.min(range_end);
            if overlap_start < overlap_end {
                let output_start =
                    range.output_offset + (overlap_start - range.chunk_offset) as usize;
                let output_end = range.output_offset + (overlap_end - range.chunk_offset) as usize;
                if let Some(work) = &mut work {
                    work.copied += (output_end - output_start) as u64;
                }
                out[output_start..output_end].copy_from_slice(
                    &scratch
                        [(overlap_start - position) as usize..(overlap_end - position) as usize],
                );
            }
        }
        position = next;
    }
    if file.metadata().map_err(Error::from)?.len() != chunk.length {
        return Err(corrupt(&chunk.id, "length changed during verification"));
    }
    let (actual, proof) = if let Some(builder) = builder {
        let built = builder.finish()?;
        debug_assert_eq!(built.hash_body_bytes, position);
        if let Some(metrics) = metrics {
            metrics.hash("outboard_parent_hash", built.parent_hash_bytes);
        }
        (built.digest, built.outboard)
    } else {
        (
            ContentDigest {
                algorithm: DigestAlgorithm::Blake3,
                bytes: *hasher.finalize().as_bytes(),
            },
            None,
        )
    };
    if actual != chunk.content_digest {
        return Err(corrupt(&chunk.id, "digest mismatch"));
    }
    Ok((counts, proof))
}

fn digest(bytes: &[u8]) -> ContentDigest {
    ContentDigest {
        algorithm: DigestAlgorithm::Blake3,
        bytes: *blake3::hash(bytes).as_bytes(),
    }
}

fn digest_hex(digest: &ContentDigest) -> String {
    digest
        .bytes
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn safe_id(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                character
            } else {
                '_'
            }
        })
        .collect()
}

fn corrupt(id: &ChunkId, detail: &str) -> Error {
    Error::coded(
        afs_error::NODE_TRANSFER_CORRUPT_DATA,
        format!("local Chunk '{}' {detail}", id.0),
    )
}

fn invalid(message: impl Into<String>) -> Error {
    Error::coded(afs_error::NODE_STORAGE_INVALID, message)
}

#[cfg(test)]
std::thread_local! {
    static FAIL_STORE_CATALOG_SYNC: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static STORE_CATALOG_SYNCS: std::cell::RefCell<Option<Vec<PathBuf>>> = const { std::cell::RefCell::new(None) };
    static FAIL_STORE_DIRECTORY_SYNC: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static STORE_DIRECTORY_SYNCS: std::cell::RefCell<Option<Vec<PathBuf>>> = const { std::cell::RefCell::new(None) };
    static VERIFIED_SCAN_BYTES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static OUTBOARD_PERSIST_IO: std::cell::RefCell<OutboardPersistIo> = const {
        std::cell::RefCell::new(OutboardPersistIo {
            writes: 0,
            file_syncs: 0,
            renames: 0,
            directory_syncs: 0,
        })
    };
    static OUTBOARD_INSTALL_HOOK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const {
        std::cell::RefCell::new(None)
    };
    static FAIL_OUTBOARD_DIRECTORY_SYNC: std::cell::Cell<bool> = const {
        std::cell::Cell::new(false)
    };
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
// Count only successful real I/O operations; injected failures do not count.
struct OutboardPersistIo {
    writes: usize,
    file_syncs: usize,
    renames: usize,
    directory_syncs: usize,
}

#[cfg(test)]
mod replica_replay_tests {
    use super::*;

    #[test]
    fn p2_w3_uncontrolled_sixteen_requests_report_actual_groups_and_barriers() {
        let temp = tempfile::tempdir().unwrap();
        let registry = afs_metrics::Registry::new();
        let metrics = super::super::read_work::ReadWorkMetrics::register(&registry)
            .unwrap()
            .with_read_diagnostics(true);
        let store = Arc::new(
            LocalChunkStore::open(temp.path(), "node-a")
                .unwrap()
                .with_read_work_metrics(metrics.clone()),
        );
        assert!(store.catalog_commit_test.ready.lock().unwrap().is_none());
        let start = Arc::new(std::sync::Barrier::new(17));
        let staged: Vec<_> = (0..16)
            .map(|index| {
                StagedChunk::new(
                    OperationId::new(format!("uncontrolled-{index}")),
                    vec![index; 64],
                )
            })
            .collect();
        let workers: Vec<_> = staged
            .iter()
            .cloned()
            .enumerate()
            .map(|(index, item)| {
                let store = store.clone();
                let start = start.clone();
                std::thread::spawn(move || {
                    start.wait(); // Entry only: no ready / batch-taking barrier.
                    (index, store.put(item).unwrap())
                })
            })
            .collect();
        start.wait();
        let receipts: Vec<_> = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect();
        let wal = fs::read(&store.catalog_path).unwrap();
        let transactions: Vec<CatalogTxn> = wal
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_slice(line).unwrap())
            .collect();
        let stats = store.catalog_commit_stats();
        assert!((1..=16).contains(&transactions.len()));
        assert_eq!(stats.groups, transactions.len() as u64);
        assert_eq!(
            transactions
                .iter()
                .map(|txn| txn.records.len())
                .sum::<usize>(),
            16
        );
        assert_eq!(
            stats.max_group,
            transactions
                .iter()
                .map(|txn| txn.records.len())
                .max()
                .unwrap()
        );
        assert_eq!((stats.requests, stats.ready, stats.waiters), (0, 0, 0));
        assert!(!stats.writer && !stats.failed);
        for (index, txn) in transactions.iter().enumerate() {
            assert_eq!(txn.revision, index as u64 + 1);
            assert_eq!(
                catalog_checksum(txn.revision, &txn.records).unwrap(),
                txn.checksum
            );
            assert!(
                txn.records
                    .iter()
                    .all(|record| record.catalog_revision == txn.revision)
            );
        }
        let snapshot = metrics.diagnostics_snapshot().unwrap();
        let calls = |phase: &str| {
            snapshot
                .coarse_phases
                .iter()
                .find(|item| item.phase == phase)
                .map_or(0, |item| item.calls)
        };
        for phase in [
            "persist_chunks_dir_sync",
            "persist_catalog_write",
            "persist_catalog_sync",
            "persist_catalog_parent_sync",
        ] {
            assert_eq!(
                calls(phase),
                stats.groups,
                "actual successful {phase} must match WAL groups"
            );
        }
        assert_eq!(calls("persist_body_sync"), 16);
        for (index, receipt) in &receipts {
            assert_eq!(receipt.operation_id, staged[*index].operation_id);
            assert_eq!(receipt.chunk, staged[*index].chunk);
            assert_eq!(receipt.durable_acks.len(), 1);
            let revision = receipt.durable_acks[0].catalog_revision;
            let txn = transactions
                .iter()
                .find(|txn| txn.revision == revision)
                .unwrap();
            assert!(
                txn.records
                    .iter()
                    .any(|record| record.chunk == receipt.chunk)
            );
        }
        eprintln!(
            "W3_UNCONTROLLED_LEDGER {}",
            serde_json::json!({
                "requests": receipts.iter().map(|(index, receipt)| serde_json::json!({
                    "request": index, "operation_id": receipt.operation_id.0,
                    "chunk_id": receipt.chunk.id.0, "revision": receipt.durable_acks[0].catalog_revision,
                })).collect::<Vec<_>>(),
                "groups": transactions.iter().map(|txn| serde_json::json!({
                    "revision": txn.revision, "records": txn.records.len(),
                })).collect::<Vec<_>>(),
                "barriers": {
                    "body_sync": calls("persist_body_sync"),
                    "chunks_dir_sync": calls("persist_chunks_dir_sync"),
                    "wal_write": calls("persist_catalog_write"),
                    "wal_sync": calls("persist_catalog_sync"),
                    "catalog_parent_sync": calls("persist_catalog_parent_sync"),
                },
            })
        );
        drop(store);
        let recovered = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        for item in staged {
            let mut output = [0; 65];
            assert_eq!(
                recovered.read_at(&item.chunk.id, 0, &mut output).unwrap(),
                64
            );
            assert_eq!(&output[..64], item.bytes());
            assert_eq!(output[64], 0);
        }
    }

    fn p2_w3_pre_ack_chunks() -> Vec<StagedChunk> {
        (0..4)
            .map(|index| {
                StagedChunk::new(
                    OperationId::new(format!("pre-ack-{index}")),
                    vec![51 + index; 97],
                )
            })
            .collect()
    }

    #[test]
    #[ignore = "child process only; parent executes both bounded pre-ACK crash modes"]
    fn p2_w3_pre_ack_child() {
        let Some(root) = std::env::var_os("AFS_TEST_W3_PRE_ACK_ROOT") else {
            return;
        };
        let mode = std::env::var("AFS_TEST_W3_PRE_ACK_MODE").unwrap();
        let registry = afs_metrics::Registry::new();
        let metrics = super::super::read_work::ReadWorkMetrics::register(&registry)
            .unwrap()
            .with_read_diagnostics(true);
        let store = Arc::new(
            LocalChunkStore::open(root, "node-a")
                .unwrap()
                .with_read_work_metrics(metrics.clone()),
        );
        if mode == "before-wal" {
            let (block, entered, _release) = p2_w3_block();
            store
                .catalog_commit_test
                .before_wal
                .lock()
                .unwrap()
                .push_back(block);
            let worker_store = store.clone();
            let _blocked_worker = std::thread::spawn(move || {
                worker_store.put_batch(p2_w3_pre_ack_chunks()).unwrap();
                panic!("pre-ACK worker unexpectedly returned a successful receipt");
            });
            entered.recv_timeout(Duration::from_secs(5)).unwrap();
            let snapshot = metrics.diagnostics_snapshot().unwrap();
            let calls = |phase: &str| {
                snapshot
                    .coarse_phases
                    .iter()
                    .find(|item| item.phase == phase)
                    .map_or(0, |item| item.calls)
            };
            assert_eq!(calls("persist_body_sync"), 4);
            assert_eq!(calls("persist_chunks_dir_sync"), 1);
            assert_eq!(calls("persist_catalog_write"), 0);
            eprintln!(
                "W3_PRE_ACK before-wal body_sync=4 chunks_dir_sync=1 wal_write=0 successful_ack=0"
            );
            // No unwinding or store Drop: the writer is still blocked before WAL.
            std::process::exit(74);
        }
        assert_eq!(mode, "half-wal");
        store
            .catalog_commit_test
            .fail_wal_step
            .store(1, Ordering::Release);
        assert!(store.put_batch(p2_w3_pre_ack_chunks()).is_err());
        assert!(store.catalog_commit_stats().failed);
        eprintln!("W3_PRE_ACK half-wal real_partial_write=true successful_ack=0");
        // Existing test hook writes a real partial multi-record transaction;
        // it does not emulate physical fsync EIO or loss of OS page cache.
        std::process::exit(75);
    }

    fn p2_w3_run_pre_ack_child(root: &Path, mode: &str) {
        let log = root.join(format!("pre-ack-{mode}.log"));
        let output = File::create(&log).unwrap();
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "node::chunk::replica_replay_tests::p2_w3_pre_ack_child",
                "--nocapture",
                "--test-threads=1",
            ])
            .env("AFS_TEST_W3_PRE_ACK_ROOT", root)
            .env("AFS_TEST_W3_PRE_ACK_MODE", mode)
            .stdout(output.try_clone().unwrap())
            .stderr(output)
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(15);
        let status = loop {
            match child.try_wait() {
                Ok(Some(status)) => break status,
                Ok(None) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(10))
                }
                failure => {
                    let _ = child.kill();
                    let _ = child.wait();
                    panic!(
                        "pre-ACK child timeout / wait error {failure:?}: {}",
                        fs::read_to_string(&log).unwrap_or_default()
                    );
                }
            }
        };
        assert_eq!(
            status.code(),
            Some(if mode == "before-wal" { 74 } else { 75 }),
            "{}",
            fs::read_to_string(&log).unwrap()
        );
        eprintln!("{}", fs::read_to_string(log).unwrap());
    }

    #[test]
    fn p2_w3_pre_ack_process_exits_keep_prefix_orphans_and_exact_retry() {
        for mode in ["before-wal", "half-wal"] {
            let temp = tempfile::tempdir().unwrap();
            let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
            let prefix = StagedChunk::new(
                OperationId::new("already-acked-prefix"),
                b"prefix-durable".to_vec(),
            );
            let prefix_ack = store.put(prefix.clone()).unwrap();
            assert_eq!(prefix_ack.durable_acks[0].catalog_revision, 1);
            let prefix_wal = fs::read(&store.catalog_path).unwrap();
            drop(store);
            p2_w3_run_pre_ack_child(temp.path(), mode);
            let expected = p2_w3_pre_ack_chunks();
            let original_files: Vec<_> = expected
                .iter()
                .map(|item| {
                    let path = temp.path().join("chunks").join(&item.chunk.id.0);
                    let body = fs::read(&path).unwrap();
                    assert_eq!(body, item.bytes());
                    (path, body)
                })
                .collect();
            let crash_wal = fs::read(temp.path().join("catalog.wal")).unwrap();
            assert!(crash_wal.starts_with(&prefix_wal));
            if mode == "before-wal" {
                assert_eq!(crash_wal, prefix_wal);
            } else {
                assert!(crash_wal.len() > prefix_wal.len());
                let error = serde_json::from_slice::<CatalogTxn>(&crash_wal[prefix_wal.len()..])
                    .unwrap_err();
                assert!(
                    error.is_eof(),
                    "partial final multi-record transaction: {error}"
                );
            }
            let recovered = LocalChunkStore::open(temp.path(), "node-a").unwrap();
            assert_eq!(fs::read(&recovered.catalog_path).unwrap(), prefix_wal);
            let report = recovered.recover().unwrap();
            assert_eq!(report.durable_chunks, 1);
            assert_eq!(report.orphan_files, 4);
            let mut output = [9; 14];
            assert_eq!(
                recovered.read_at(&prefix.chunk.id, 0, &mut output).unwrap(),
                prefix.bytes().len()
            );
            assert_eq!(&output[..prefix.bytes().len()], prefix.bytes());
            for (path, body) in &original_files {
                assert_eq!(
                    &fs::read(path).unwrap(),
                    body,
                    "orphan bytes must not be deleted or rewritten on recovery"
                );
            }
            let retried = recovered.put_batch(expected.clone()).unwrap();
            assert_eq!(retried.len(), 4);
            for (receipt, item) in retried.iter().zip(&expected) {
                assert_eq!(receipt.operation_id, item.operation_id);
                assert_eq!(receipt.chunk, item.chunk);
                assert_eq!(receipt.durable_acks[0].catalog_revision, 2);
            }
            assert_eq!(recovered.recover().unwrap().orphan_files, 0);
            drop(recovered);
            let reopened = LocalChunkStore::open(temp.path(), "node-a").unwrap();
            assert_eq!(reopened.recover().unwrap().durable_chunks, 5);
            for item in &expected {
                let mut body = [0; 98];
                assert_eq!(reopened.read_at(&item.chunk.id, 0, &mut body).unwrap(), 97);
                assert_eq!(&body[..97], item.bytes());
                assert_eq!(body[97], 0);
            }
            eprintln!(
                "W3_PRE_ACK_RECOVERY mode={mode} acked_prefix=1 orphans_preserved=4 retry_records=4 retry_revision=2 restart_durable=5"
            );
        }
    }

    #[test]
    fn p2_w3_sixteen_ready_requests_share_one_durable_group() {
        let temp = tempfile::tempdir().unwrap();
        let registry = afs_metrics::Registry::new();
        let metrics = super::super::read_work::ReadWorkMetrics::register(&registry)
            .unwrap()
            .with_read_diagnostics(true);
        let store = Arc::new(
            LocalChunkStore::open(temp.path(), "node-a")
                .unwrap()
                .with_read_work_metrics(metrics.clone()),
        );
        *store.catalog_commit_test.ready.lock().unwrap() =
            Some(Arc::new(std::sync::Barrier::new(16)));
        let staged: Vec<_> = (0..16)
            .map(|index| {
                StagedChunk::new(OperationId::new(format!("group-{index}")), vec![index; 64])
            })
            .collect();
        let workers: Vec<_> = staged
            .iter()
            .cloned()
            .map(|item| {
                let store = store.clone();
                std::thread::spawn(move || store.put(item))
            })
            .collect();
        let receipts: Vec<_> = workers
            .into_iter()
            .map(|worker| worker.join().unwrap().unwrap())
            .collect();
        let snapshot = metrics.diagnostics_snapshot().unwrap();
        for phase in [
            "persist_chunks_dir_sync",
            "persist_catalog_write",
            "persist_catalog_sync",
            "persist_catalog_parent_sync",
        ] {
            assert_eq!(
                snapshot
                    .coarse_phases
                    .iter()
                    .find(|item| item.phase == phase)
                    .unwrap()
                    .calls,
                1,
                "{phase}: all sixteen requests were ready before taking the group"
            );
        }
        assert_eq!(
            snapshot
                .coarse_phases
                .iter()
                .find(|item| item.phase == "persist_body_sync")
                .unwrap()
                .calls,
            16
        );
        for receipt in &receipts {
            assert_eq!(receipt.durable_acks[0].catalog_revision, 1);
        }
        *store.catalog_commit_test.ready.lock().unwrap() = None;
        let stats = store.catalog_commit_stats();
        assert_eq!(stats.requests, 0);
        assert_eq!(stats.ready, 0);
        assert_eq!(stats.waiters, 0);
        assert!(!stats.writer && !stats.failed && !stats.exclusive);
        assert_eq!(stats.groups, 1);
        assert_eq!(stats.max_group, 16);
        assert!(stats.reserved_bytes <= stats.peak_reserved_bytes);
        assert!(stats.actual_peak_bytes <= stats.peak_reserved_bytes);
        assert!(stats.peak_reserved_bytes <= 1024 * 1024);
        drop(store);
        let recovered = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        for item in staged {
            let mut output = [0; 64];
            assert_eq!(
                recovered.read_at(&item.chunk.id, 0, &mut output).unwrap(),
                64
            );
            assert_eq!(output.as_slice(), item.bytes());
        }
    }

    fn p2_w3_block() -> (
        PeerFillTestBlock,
        std::sync::mpsc::Receiver<()>,
        std::sync::mpsc::Sender<()>,
    ) {
        let (entered, observed) = std::sync::mpsc::channel();
        let (release, wait) = std::sync::mpsc::channel();
        ((entered, wait), observed, release)
    }

    fn p2_w3_wait(
        store: &LocalChunkStore,
        predicate: impl Fn(catalog_commit::CatalogCommitStats) -> bool,
    ) {
        let deadline = Instant::now() + Duration::from_secs(3);
        while !predicate(store.catalog_commit_stats()) {
            assert!(
                Instant::now() < deadline,
                "catalog condition did not become ready"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    #[test]
    fn p2_w3_prepare_does_not_hold_catalog_or_block_a_different_id() {
        let temp = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
        let (block, observed, release) = p2_w3_block();
        *store.catalog_commit_test.before_prepare.lock().unwrap() = Some(block);
        let first_store = store.clone();
        let first = std::thread::spawn(move || {
            first_store.put(StagedChunk::new(
                OperationId::new("blocked-prepare"),
                b"first".to_vec(),
            ))
        });
        observed.recv_timeout(Duration::from_secs(3)).unwrap();
        let other_store = store.clone();
        let (done, completion) = std::sync::mpsc::channel();
        let second = std::thread::spawn(move || {
            let result = other_store.device_descriptor().and_then(|_| {
                other_store.put(StagedChunk::new(
                    OperationId::new("independent"),
                    b"second".to_vec(),
                ))
            });
            done.send(result).unwrap();
        });
        let while_blocked = completion.recv_timeout(Duration::from_secs(1));
        release.send(()).unwrap();
        assert!(first.join().unwrap().is_ok());
        second.join().unwrap();
        assert!(
            while_blocked.unwrap().is_ok(),
            "different object must progress while preparation is blocked"
        );
    }

    #[test]
    fn p2_w3_same_id_replay_and_quarantine_wait_for_the_install_flight() {
        let temp = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
        let staged = StagedChunk::new(OperationId::new("same-flight"), b"same".to_vec());
        let (block, observed, release) = p2_w3_block();
        store
            .catalog_commit_test
            .before_wal
            .lock()
            .unwrap()
            .push_back(block);
        let first_store = store.clone();
        let first_item = staged.clone();
        let first = std::thread::spawn(move || first_store.put(first_item));
        observed.recv_timeout(Duration::from_secs(3)).unwrap();
        let replay_store = store.clone();
        let replay_item = staged.clone();
        let replay = std::thread::spawn(move || replay_store.put(replay_item));
        let quarantine_store = store.clone();
        let id = staged.chunk.id.clone();
        let quarantine = std::thread::spawn(move || quarantine_store.quarantine_if_invalid(&id));
        p2_w3_wait(&store, |stats| stats.waiters == 2);
        release.send(()).unwrap();
        let ack = first.join().unwrap().unwrap();
        let replay_ack = replay.join().unwrap().unwrap();
        assert_eq!(
            ack.durable_acks[0].catalog_revision,
            replay_ack.durable_acks[0].catalog_revision
        );
        assert!(quarantine.join().unwrap().is_ok());
        assert!(store.quarantined_chunks().unwrap().is_empty());
        assert_eq!(store.catalog_commit_stats().groups, 1);
    }

    #[test]
    fn p2_w3_first_group_ack_does_not_wait_for_a_blocked_following_group() {
        let temp = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
        let (block1, observed1, release1) = p2_w3_block();
        let (block2, observed2, release2) = p2_w3_block();
        store
            .catalog_commit_test
            .before_wal
            .lock()
            .unwrap()
            .extend([block1, block2]);
        let first_store = store.clone();
        let (done, completion) = std::sync::mpsc::channel();
        let first = std::thread::spawn(move || {
            done.send(first_store.put(StagedChunk::new(
                OperationId::new("first-group"),
                b"first".to_vec(),
            )))
            .unwrap()
        });
        observed1.recv_timeout(Duration::from_secs(3)).unwrap();
        let second_store = store.clone();
        let second = std::thread::spawn(move || {
            second_store.put(StagedChunk::new(
                OperationId::new("second-group"),
                b"second".to_vec(),
            ))
        });
        p2_w3_wait(&store, |stats| stats.ready == 1);
        release1.send(()).unwrap();
        observed2.recv_timeout(Duration::from_secs(3)).unwrap();
        let first_ack = completion.recv_timeout(Duration::from_secs(1));
        release2.send(()).unwrap();
        first.join().unwrap();
        assert!(second.join().unwrap().is_ok());
        assert!(
            first_ack.unwrap().is_ok(),
            "first ACK must not be held by continuously arriving later groups"
        );
        assert_eq!(store.catalog_commit_stats().groups, 2);
    }

    #[test]
    fn p2_w3_oversized_batch_progresses_as_one_exclusive_original_transaction() {
        let temp = tempfile::tempdir().unwrap();
        let store = Arc::new(
            LocalChunkStore::open(temp.path(), "node-a")
                .unwrap()
                .with_catalog_commit_queue(1)
                .unwrap(),
        );
        let staged: Vec<_> = (0..80)
            .map(|index| {
                StagedChunk::new(OperationId::new(format!("large-{index}")), vec![index; 64])
            })
            .collect();
        let (block, observed, release) = p2_w3_block();
        store
            .catalog_commit_test
            .before_wal
            .lock()
            .unwrap()
            .push_back(block);
        let worker_store = store.clone();
        let worker = std::thread::spawn(move || worker_store.put_batch(staged));
        observed.recv_timeout(Duration::from_secs(3)).unwrap();
        let held = store.catalog_commit_stats();
        assert!(held.exclusive);
        assert_eq!(
            held.requests, 0,
            "large original metadata must not enter the bounded new queue"
        );
        release.send(()).unwrap();
        let receipts = worker.join().unwrap().unwrap();
        assert_eq!(receipts.len(), 80);
        assert!(
            receipts
                .iter()
                .all(|receipt| receipt.durable_acks[0].catalog_revision == 1)
        );
        assert_eq!(store.catalog_commit_stats().groups, 1);
        assert!(!store.catalog_commit_stats().exclusive);
        drop(store);
        let recovered = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        assert_eq!(recovered.recover().unwrap().durable_chunks, 80);
    }

    #[test]
    fn p2_w3_partial_write_sync_and_parent_failures_prevent_every_later_append() {
        for step in [1, 2, 3] {
            let temp = tempfile::tempdir().unwrap();
            let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
            let healthy = StagedChunk::new(OperationId::new("healthy"), b"healthy".to_vec());
            store.put(healthy.clone()).unwrap();
            store
                .catalog_commit_test
                .fail_wal_step
                .store(step, Ordering::Release);
            assert!(
                store
                    .put(StagedChunk::new(
                        OperationId::new("failed-group"),
                        b"failed".to_vec()
                    ))
                    .is_err()
            );
            let failed_bytes = fs::read(&store.catalog_path).unwrap();
            assert!(store.catalog_commit_stats().failed);
            assert!(store.put(healthy.clone()).is_err());
            fs::write(store.chunk_path(&healthy.chunk.id), b"damaged").unwrap();
            assert!(store.quarantine_if_invalid(&healthy.chunk.id).is_err());
            assert!(
                store
                    .put(StagedChunk::new(
                        OperationId::new("later"),
                        b"later".to_vec()
                    ))
                    .is_err()
            );
            assert_eq!(fs::read(&store.catalog_path).unwrap(), failed_bytes);
            drop(store);
            let recovered = LocalChunkStore::open(temp.path(), "node-a").unwrap();
            assert!(recovered.put(healthy).is_ok());
        }
    }

    #[test]
    fn p2_w3_stop_waits_for_owned_io_and_keeps_the_final_drain_commit() {
        let temp = tempfile::tempdir().unwrap();
        let store = Arc::new(
            LocalChunkStore::open(temp.path(), "node-a")
                .unwrap()
                .with_catalog_commit_queue(1)
                .unwrap(),
        );
        store.note_catalog_stop();
        // The existing DFS drain invokes persist after the stop notification.
        assert!(
            store
                .put(StagedChunk::new(
                    OperationId::new("final-drain"),
                    b"drain".to_vec()
                ))
                .is_ok()
        );
        let (block, observed, release) = p2_w3_block();
        store
            .catalog_commit_test
            .before_wal
            .lock()
            .unwrap()
            .push_back(block);
        let worker_store = store.clone();
        let worker = std::thread::spawn(move || {
            worker_store.put(StagedChunk::new(
                OperationId::new("owned-io"),
                b"owned".to_vec(),
            ))
        });
        observed.recv_timeout(Duration::from_secs(3)).unwrap();
        let waiting_store = store.clone();
        let waiter = std::thread::spawn(move || {
            waiting_store.put(StagedChunk::new(
                OperationId::new("queued"),
                b"queued".to_vec(),
            ))
        });
        p2_w3_wait(&store, |stats| stats.waiters == 1);
        let stopping_store = store.clone();
        let (done, stopped) = std::sync::mpsc::channel();
        let stopping = std::thread::spawn(move || {
            done.send(stopping_store.shutdown_catalog_committer())
                .unwrap()
        });
        p2_w3_wait(&store, |stats| stats.closed);
        let still_owned = stopped.recv_timeout(Duration::from_millis(30)).is_err();
        release.send(()).unwrap();
        assert!(worker.join().unwrap().is_ok());
        assert!(waiter.join().unwrap().is_err());
        assert!(
            stopped
                .recv_timeout(Duration::from_secs(3))
                .unwrap()
                .is_ok()
        );
        stopping.join().unwrap();
        assert!(still_owned, "stop must not detach active I/O");
        assert_eq!(store.catalog_commit_stats().requests, 0);
        assert!(
            store
                .put(StagedChunk::new(
                    OperationId::new("after-close"),
                    b"no".to_vec()
                ))
                .is_err()
        );
    }

    #[test]
    fn p2_w3_unconfirmed_replacement_cannot_replay_until_a_reopen_directory_barrier() {
        for later_prepare_failure in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
            let healthy =
                StagedChunk::new(OperationId::new("healthy-replace"), b"healthy".to_vec());
            store.put(healthy.clone()).unwrap();
            fs::write(store.chunk_path(&healthy.chunk.id), b"BROKEN!").unwrap();
            if later_prepare_failure {
                store
                    .catalog_commit_test
                    .fail_prepare_after
                    .store(2, Ordering::Release);
            } else {
                store
                    .catalog_commit_test
                    .fail_chunks_dir
                    .store(true, Ordering::Release);
            }
            let mut staged = vec![healthy.clone()];
            if later_prepare_failure {
                staged.push(StagedChunk::new(
                    OperationId::new("second-body"),
                    b"second".to_vec(),
                ));
            }
            assert!(store.put_batch(staged).is_err());
            assert_eq!(
                fs::read(store.chunk_path(&healthy.chunk.id)).unwrap(),
                b"healthy"
            );
            assert!(store.catalog_commit_stats().failed);
            let wal = fs::read(&store.catalog_path).unwrap();
            assert!(
                store.put(healthy.clone()).is_err(),
                "unconfirmed rename cannot gain a healthy replay ACK"
            );
            assert_eq!(fs::read(&store.catalog_path).unwrap(), wal);
            drop(store);
            FAIL_STORE_DIRECTORY_SYNC.with(|fail| fail.set(true));
            assert!(
                LocalChunkStore::open(temp.path(), "node-a").is_err(),
                "reopen must complete its chunks namespace barrier"
            );
            STORE_DIRECTORY_SYNCS.with(|syncs| *syncs.borrow_mut() = Some(Vec::new()));
            let recovered = LocalChunkStore::open(temp.path(), "node-a").unwrap();
            let syncs = STORE_DIRECTORY_SYNCS.with(|syncs| syncs.borrow_mut().take().unwrap());
            assert_eq!(syncs.first(), Some(&temp.path().join("chunks")));
            assert!(syncs.contains(&temp.path().to_path_buf()));
            assert!(recovered.put(healthy).is_ok());
            assert_eq!(
                recovered.catalog_commit_stats().groups,
                0,
                "healthy replay uses the proven startup barriers"
            );
        }
    }

    #[test]
    fn p2_w3_multi_key_requests_do_not_deadlock_and_owner_drop_releases_admission() {
        let temp = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
        let a = StagedChunk::new(OperationId::new("key-a"), b"a".to_vec());
        let b = StagedChunk::new(OperationId::new("key-b"), b"b".to_vec());
        let cancelled = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _admission = store.admit_catalog(&[a.clone(), b.clone()]).unwrap();
            panic!("test-owned caller unwind before I/O");
        }));
        assert!(cancelled.is_err());
        assert_eq!(store.catalog_commit_stats().requests, 0);
        let first_store = store.clone();
        let forward = vec![a.clone(), b.clone()];
        let first = std::thread::spawn(move || first_store.put_batch(forward));
        let second_store = store.clone();
        let second = std::thread::spawn(move || second_store.put_batch(vec![b, a]));
        assert_eq!(first.join().unwrap().unwrap().len(), 2);
        assert_eq!(second.join().unwrap().unwrap().len(), 2);
        assert_eq!(store.catalog_commit_stats().groups, 1);
    }

    #[test]
    fn p2_w3_shared_group_failure_wakes_every_waiter_without_ack() {
        for step in [2, 3] {
            let temp = tempfile::tempdir().unwrap();
            let store = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
            let healthy = StagedChunk::new(
                OperationId::new("previously-confirmed"),
                b"confirmed".to_vec(),
            );
            store.put(healthy.clone()).unwrap();
            *store.catalog_commit_test.ready.lock().unwrap() =
                Some(Arc::new(std::sync::Barrier::new(16)));
            store
                .catalog_commit_test
                .fail_wal_step
                .store(step, Ordering::Release);
            let workers: Vec<_> = (0..16)
                .map(|index| {
                    let store = store.clone();
                    std::thread::spawn(move || {
                        store.put(StagedChunk::new(
                            OperationId::new(format!("shared-error-{index}")),
                            vec![index; 64],
                        ))
                    })
                })
                .collect();
            let errors: Vec<_> = workers
                .into_iter()
                .map(|worker| worker.join().unwrap().unwrap_err())
                .collect();
            assert_eq!(errors.len(), 16);
            assert!(
                errors.iter().all(|error| error == &errors[0]),
                "every failed group member must observe the same structured error"
            );
            *store.catalog_commit_test.ready.lock().unwrap() = None;
            let stats = store.catalog_commit_stats();
            assert!(stats.failed);
            assert_eq!(stats.requests, 0);
            assert_eq!(
                stats.groups, 1,
                "only the previous confirmed request may count as a successful group"
            );
            assert_eq!(stats.max_group, 16);
            let bytes = fs::read(&store.catalog_path).unwrap();
            assert!(store.put(healthy.clone()).is_err());
            assert!(store.quarantine_if_invalid(&healthy.chunk.id).is_err());
            assert!(
                store
                    .put(StagedChunk::new(
                        OperationId::new("after-shared-failure"),
                        b"later".to_vec()
                    ))
                    .is_err()
            );
            assert_eq!(
                fs::read(&store.catalog_path).unwrap(),
                bytes,
                "failed group cannot be followed by another append"
            );
            drop(store);
            let recovered = LocalChunkStore::open(temp.path(), "node-a").unwrap();
            let mut output = [0; 9];
            assert_eq!(
                recovered
                    .read_at(&healthy.chunk.id, 0, &mut output)
                    .unwrap(),
                9
            );
            assert_eq!(&output, b"confirmed");
        }
    }

    #[test]
    fn p2_w3_reopen_syncs_complete_unsynced_wal_before_replay_ack() {
        for newline in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
            assert!(
                !store.catalog_path.exists(),
                "empty startup must not invent a WAL file"
            );
            let healthy =
                StagedChunk::new(OperationId::new("previous-confirmed"), b"previous".to_vec());
            store.put(healthy).unwrap();
            let unconfirmed = StagedChunk::new(
                OperationId::new("complete-unsynced"),
                b"unconfirmed".to_vec(),
            );
            let mut body = File::create(store.chunk_path(&unconfirmed.chunk.id)).unwrap();
            body.write_all(unconfirmed.bytes()).unwrap();
            body.sync_all().unwrap();
            let record = LocalChunkRecord {
                chunk: unconfirmed.chunk.clone(),
                segment_crc: None,
                stored_length: unconfirmed.chunk.length,
                stored_checksum: unconfirmed.chunk.content_digest.clone(),
                device_id: store.device_id.clone(),
                device_epoch: store.device_epoch,
                location: PhysicalLocation::PerChunkFile {
                    relative_path: format!("chunks/{}", unconfirmed.chunk.id.0),
                },
                encoding: PhysicalEncoding::Raw,
                state: LocalChunkState::Durable,
                catalog_revision: 2,
            };
            let mut encoded =
                serde_json::to_vec(&CatalogTxn::new(2, vec![record]).unwrap()).unwrap();
            if newline {
                encoded.push(b'\n');
            }
            // Deliberately omit sync_all: complete bytes are visible in the same
            // OS but must not be mistaken for a completed durability barrier.
            OpenOptions::new()
                .append(true)
                .open(&store.catalog_path)
                .unwrap()
                .write_all(&encoded)
                .unwrap();
            let wal = fs::read(&store.catalog_path).unwrap();
            drop(store);
            FAIL_STORE_CATALOG_SYNC.with(|fail| fail.set(true));
            assert!(LocalChunkStore::open(temp.path(), "node-a").is_err());
            STORE_CATALOG_SYNCS.with(|syncs| *syncs.borrow_mut() = Some(Vec::new()));
            let recovered = LocalChunkStore::open(temp.path(), "node-a").unwrap();
            let syncs = STORE_CATALOG_SYNCS.with(|syncs| syncs.borrow_mut().take().unwrap());
            assert_eq!(syncs, vec![temp.path().join("catalog.wal")]);
            assert_eq!(
                fs::read(&recovered.catalog_path).unwrap(),
                wal,
                "valid full transaction bytes are preserved"
            );
            let receipt = recovered.put(unconfirmed).unwrap();
            assert_eq!(receipt.durable_acks[0].catalog_revision, 2);
            assert_eq!(recovered.catalog_commit_stats().groups, 0);
            drop(recovered);
            // Complete malformed content remains a validation error, never a
            // recovery-sync fast path that silently accepts it.
            OpenOptions::new()
                .append(true)
                .open(temp.path().join("catalog.wal"))
                .unwrap()
                .write_all(b"\n{}\n")
                .unwrap();
            STORE_CATALOG_SYNCS.with(|syncs| *syncs.borrow_mut() = Some(Vec::new()));
            assert!(LocalChunkStore::open(temp.path(), "node-a").is_err());
            assert!(
                STORE_CATALOG_SYNCS
                    .with(|syncs| syncs.borrow_mut().take().unwrap())
                    .is_empty()
            );
        }
    }

    #[test]
    fn p2_w3_catalog_failure_latches_until_reopen() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let healthy = StagedChunk::new(OperationId::new("healthy"), b"healthy".to_vec());
        store.put(healthy.clone()).unwrap();
        let backup = temp.path().join("catalog-backup.wal");
        fs::rename(&store.catalog_path, &backup).unwrap();
        fs::create_dir(&store.catalog_path).unwrap();
        assert!(
            store
                .put(StagedChunk::new(
                    OperationId::new("failed"),
                    b"failed".to_vec()
                ))
                .is_err()
        );
        fs::remove_dir(&store.catalog_path).unwrap();
        fs::rename(&backup, &store.catalog_path).unwrap();
        let bytes = fs::read(&store.catalog_path).unwrap();
        assert!(
            store.put(healthy.clone()).is_err(),
            "a failed catalog cannot issue a later successful replay ACK"
        );
        assert!(
            store
                .put(StagedChunk::new(
                    OperationId::new("later"),
                    b"later".to_vec()
                ))
                .is_err()
        );
        assert_eq!(fs::read(&store.catalog_path).unwrap(), bytes);
        drop(store);
        let recovered = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        assert!(recovered.put(healthy).is_ok());
    }

    #[test]
    fn p2_w0_blocked_installer_does_not_delay_durable_ack() {
        use std::{sync::mpsc, thread, time::Duration};
        let temp = tempfile::tempdir().unwrap();
        let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
        let staged = pooled_outboard_chunks(&budget, 1, 128 * 1024)
            .pop()
            .unwrap();
        let store = Arc::new(
            LocalChunkStore::open(temp.path(), "node-a")
                .unwrap()
                .with_outboard_budget(budget),
        );
        let (entered, entered_rx) = mpsc::channel();
        let (release, release_rx) = mpsc::channel();
        *store.sidecar_install_test.block.lock().unwrap() = Some((entered, release_rx));
        let worker_store = store.clone();
        let worker_staged = staged.clone();
        let (completed, completed_rx) = mpsc::channel();
        let writer = thread::spawn(move || {
            completed.send(worker_store.put(worker_staged)).unwrap();
        });
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        // This is an explicit in-installer barrier, not a file-presence race.
        // The required body/catalog transaction has completed, while optional
        // sidecar installation remains blocked on the test-owned receiver.
        assert_eq!(
            fs::read(store.chunk_path(&staged.chunk.id)).unwrap(),
            staged.bytes()
        );
        assert!(!fs::read(&store.catalog_path).unwrap().is_empty());
        let while_blocked = completed_rx.recv_timeout(Duration::from_millis(100));
        release.send(()).unwrap();
        writer.join().unwrap();
        assert!(
            while_blocked.is_ok(),
            "durable persist ACK was blocked by the optional sidecar installer"
        );
        let receipt = while_blocked.unwrap().unwrap();
        assert_eq!(receipt.chunk, staged.chunk);
        assert_eq!(receipt.durable_acks.len(), 1);
        let ack = &receipt.durable_acks[0];
        assert_eq!(ack.chunk_id, staged.chunk.id);
        assert_eq!(ack.persisted_bytes, staged.chunk.length);
        assert_eq!(ack.verified_digest, staged.chunk.content_digest);
    }

    fn block_next_sidecar_install(
        store: &LocalChunkStore,
    ) -> (std::sync::mpsc::Receiver<()>, std::sync::mpsc::Sender<()>) {
        let (entered, entered_rx) = std::sync::mpsc::channel();
        let (release, release_rx) = std::sync::mpsc::channel();
        *store.sidecar_install_test.block.lock().unwrap() = Some((entered, release_rx));
        (entered_rx, release)
    }

    #[test]
    fn p2_w0_required_barriers_precede_ack_while_optional_installer_is_blocked() {
        let temp = tempfile::tempdir().unwrap();
        let registry = afs_metrics::Registry::new();
        let metrics = super::super::read_work::ReadWorkMetrics::register(&registry)
            .unwrap()
            .with_read_diagnostics(true);
        let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
        let staged = pooled_outboard_chunks(&budget, 1, 128 * 1024)
            .pop()
            .unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a")
            .unwrap()
            .with_outboard_budget(budget)
            .with_read_work_metrics(metrics.clone());
        let (entered, release) = block_next_sidecar_install(&store);
        let receipt = store.put(staged.clone()).unwrap();
        entered.recv_timeout(Duration::from_secs(5)).unwrap();
        let snapshot = metrics.diagnostics_snapshot().unwrap();
        let mut previous_end = 0;
        for name in [
            "persist_body_write",
            "persist_body_sync",
            "persist_chunks_dir_sync",
            "persist_catalog_write",
            "persist_catalog_sync",
            "persist_catalog_parent_sync",
        ] {
            let phase = snapshot
                .coarse_phases
                .iter()
                .find(|phase| phase.phase == name)
                .unwrap();
            assert_eq!(phase.calls, 1, "{name}");
            assert!(
                phase.first_start_ns >= previous_end,
                "required barrier order changed at {name}"
            );
            previous_end = phase.last_end_ns;
        }
        for name in ["persist_proof_install_sync", "persist_proof_dir_sync"] {
            assert_eq!(
                snapshot
                    .coarse_phases
                    .iter()
                    .find(|phase| phase.phase == name)
                    .unwrap()
                    .calls,
                0
            );
        }
        assert_eq!(receipt.durable_acks.len(), 1);
        assert_eq!(
            *store.sidecar_install_test.optional_io.lock().unwrap(),
            OutboardPersistIo::default()
        );
        release.send(()).unwrap();
        store.sidecar_installer.wait_idle();
    }

    #[test]
    fn p2_w0_queue_limit_includes_active_and_releases_only_its_own_proof_refs() {
        let temp = tempfile::tempdir().unwrap();
        let registry = afs_metrics::Registry::new();
        let metrics = super::super::read_work::ReadWorkMetrics::register(&registry).unwrap();
        let budget =
            OutboardBudget::new_with_metrics(DEFAULT_OUTBOARD_BUDGET_BYTES, metrics.clone());
        let staged = pooled_outboard_chunks(&budget, 65, 4096);
        let store = LocalChunkStore::open(temp.path(), "node-a")
            .unwrap()
            .with_outboard_budget(budget.clone())
            .with_read_work_metrics(metrics);
        let (entered, release) = block_next_sidecar_install(&store);
        store.put(staged[0].clone()).unwrap();
        entered.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(store.put_batch(staged[1..].to_vec()).unwrap().len(), 64);
        {
            let state = store.sidecar_installer.queue.state.lock().unwrap();
            assert_eq!(
                (state.pending.len(), state.active, state.unfinished.len()),
                (63, 1, 64)
            );
            let actual_bound = sidecar_control_bytes(
                state.pending.capacity(),
                state.unfinished.capacity(),
                store.sidecar_installer.limit,
                state.directory_capacity,
            )
            .unwrap();
            assert!(actual_bound <= state.control_bytes);
            assert!(state.control_bytes > 0);
        }
        let charged = budget.used();
        assert!(charged > 0 && charged <= budget.limit());
        assert_eq!(budget.resident_used(), 0);
        assert!(
            afs_metrics::encode_text(&registry)
                .unwrap()
                .contains("afs_dfs_outboard_events_total{event=\"sidecar_install_dropped\"} 1\n")
        );
        release.send(()).unwrap();
        store.sidecar_installer.wait_idle();
        assert_eq!(
            budget.used(),
            charged,
            "staged owners still retain every proof allocation"
        );
        let io = *store.sidecar_install_test.optional_io.lock().unwrap();
        assert_eq!(
            (io.writes, io.file_syncs, io.renames, io.directory_syncs),
            (64, 64, 64, 3)
        );
        let retained = staged[0].outboard.clone();
        drop(staged);
        assert!(
            budget.used() > 0,
            "last independent holder must keep its charge"
        );
        drop(retained);
        let idle_control = store
            .sidecar_installer
            .queue
            .state
            .lock()
            .unwrap()
            .control_bytes;
        assert_eq!(
            budget.used(),
            idle_control,
            "bounded idle control capacity must remain charged"
        );
        store.sidecar_installer.shutdown().unwrap();
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn p2_w0_installer_failure_keeps_ack_and_safe_read_repairs_once() {
        let temp = tempfile::tempdir().unwrap();
        let registry = afs_metrics::Registry::new();
        let metrics = super::super::read_work::ReadWorkMetrics::register(&registry).unwrap();
        let budget =
            OutboardBudget::new_with_metrics(DEFAULT_OUTBOARD_BUDGET_BYTES, metrics.clone());
        let staged = pooled_outboard_chunks(&budget, 1, 128 * 1024)
            .pop()
            .unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a")
            .unwrap()
            .with_outboard_budget(budget)
            .with_read_work_metrics(metrics);
        let path = store
            .outboards
            .join(format!("{}.bao", safe_id(&staged.chunk.id.0)));
        fs::create_dir(&path).unwrap();
        let receipt = store.put(staged.clone()).unwrap();
        store.sidecar_installer.wait_idle();
        assert_eq!(receipt.chunk, staged.chunk);
        assert!(
            afs_metrics::encode_text(&registry)
                .unwrap()
                .contains("afs_dfs_outboard_events_total{event=\"sidecar_install_failed\"} 1\n")
        );
        fs::remove_dir(&path).unwrap();
        let catalog = fs::read(&store.catalog_path).unwrap();
        take_outboard_persist_io();
        for _ in 0..2 {
            let reader = store.open_verified(&staged.chunk.id).unwrap();
            let mut out = [0; 19];
            assert_eq!(reader.read_at(3, &mut out).unwrap(), out.len());
            assert_eq!(out, staged.bytes()[3..22]);
        }
        assert_eq!(take_outboard_persist_io().writes, 1);
        assert_eq!(fs::read(&store.catalog_path).unwrap(), catalog);
        assert_eq!(
            fs::read(path).unwrap(),
            staged.outboard.as_ref().unwrap().encoded()
        );
    }

    #[test]
    fn p2_w0_ack_before_sidecar_keeps_verified_full_scan_fallback_and_repair() {
        let temp = tempfile::tempdir().unwrap();
        let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
        let staged = pooled_outboard_chunks(&budget, 1, 4 * 1024 * 1024)
            .pop()
            .unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a")
            .unwrap()
            .with_outboard_budget(budget);
        let (entered, release) = block_next_sidecar_install(&store);
        let receipt = store.put(staged.clone()).unwrap();
        entered.recv_timeout(Duration::from_secs(5)).unwrap();
        let path = store
            .outboards
            .join(format!("{}.bao", safe_id(&staged.chunk.id.0)));
        assert!(
            !path.exists(),
            "installer is explicitly blocked before sidecar installation"
        );
        let catalog = fs::read(&store.catalog_path).unwrap();
        VERIFIED_SCAN_BYTES.with(|count| count.set(0));
        take_outboard_persist_io();
        let payload = store.read_peer_range(&staged.chunk.id, 3, 17).unwrap();
        assert_eq!(payload.as_slice(), &staged.bytes()[3..20]);
        assert_eq!(
            VERIFIED_SCAN_BYTES.with(|count| count.get()),
            staged.chunk.length
        );
        assert_eq!(
            take_outboard_persist_io().writes,
            1,
            "missing optional proof is safely repaired once"
        );
        assert_eq!(
            fs::read(&path).unwrap(),
            staged.outboard.as_ref().unwrap().encoded()
        );
        assert_eq!(fs::read(&store.catalog_path).unwrap(), catalog);
        assert_eq!(receipt.chunk, staged.chunk);
        release.send(()).unwrap();
        store.sidecar_installer.wait_idle();
        assert_eq!(
            store
                .sidecar_install_test
                .optional_io
                .lock()
                .unwrap()
                .writes,
            0,
            "installer rechecks the sidecar repaired by the verified read"
        );
    }

    #[test]
    fn p2_w0_control_metadata_admission_failure_drops_optional_work_not_ack() {
        let temp = tempfile::tempdir().unwrap();
        let registry = afs_metrics::Registry::new();
        let metrics = super::super::read_work::ReadWorkMetrics::register(&registry).unwrap();
        let budget =
            OutboardBudget::new_with_metrics(DEFAULT_OUTBOARD_BUDGET_BYTES, metrics.clone());
        let staged = pooled_outboard_chunks(&budget, 1, 128 * 1024)
            .pop()
            .unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a")
            .unwrap()
            .with_outboard_budget(budget.clone())
            .with_read_work_metrics(metrics);
        let pressure = budget
            .try_reserve(budget.limit() - budget.used() - 1)
            .unwrap();
        let before = budget.used();
        let receipt = store.put(staged.clone()).unwrap();
        assert_eq!(receipt.chunk, staged.chunk);
        assert_eq!(budget.used(), before);
        assert_eq!(
            store
                .sidecar_installer
                .queue
                .state
                .lock()
                .unwrap()
                .control_bytes,
            0
        );
        assert!(store.sidecar_installer.worker.lock().unwrap().is_none());
        assert!(
            afs_metrics::encode_text(&registry)
                .unwrap()
                .contains("afs_dfs_outboard_events_total{event=\"sidecar_install_dropped\"} 1\n")
        );
        drop(pressure);
        take_outboard_persist_io();
        drop(store.open_verified(&staged.chunk.id).unwrap());
        assert_eq!(take_outboard_persist_io().writes, 1);
    }

    #[test]
    fn p2_w0_control_charge_is_observed_before_optional_worker_start_failure() {
        let temp = tempfile::tempdir().unwrap();
        let registry = afs_metrics::Registry::new();
        let metrics = super::super::read_work::ReadWorkMetrics::register(&registry).unwrap();
        let budget =
            OutboardBudget::new_with_metrics(DEFAULT_OUTBOARD_BUDGET_BYTES, metrics.clone());
        let staged = pooled_outboard_chunks(&budget, 1, 128 * 1024)
            .pop()
            .unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a")
            .unwrap()
            .with_outboard_budget(budget.clone())
            .with_read_work_metrics(metrics)
            .with_sidecar_install_queue(2)
            .unwrap();
        assert!(
            afs_metrics::encode_text(&registry)
                .unwrap()
                .contains("afs_dfs_sidecar_install_items{state=\"limit\"} 2\n")
        );
        store
            .sidecar_install_test
            .fail_worker_start
            .store(true, Ordering::Relaxed);
        let receipt = store.put(staged.clone()).unwrap();
        assert_eq!(receipt.chunk, staged.chunk);
        let control_bytes = store
            .sidecar_installer
            .queue
            .state
            .lock()
            .unwrap()
            .control_bytes;
        assert!(control_bytes > 0);
        let text = afs_metrics::encode_text(&registry).unwrap();
        assert!(
            text.contains(&format!(
                "afs_dfs_sidecar_install_control_bytes {control_bytes}\n"
            )),
            "{text}"
        );
        assert!(
            text.contains("afs_dfs_outboard_events_total{event=\"sidecar_install_failed\"} 1\n")
        );
        assert!(store.sidecar_installer.worker.lock().unwrap().is_none());
        drop(staged);
        assert_eq!(budget.used(), control_bytes);
        store.sidecar_installer.shutdown().unwrap();
        assert_eq!(budget.used(), 0);
        assert!(
            afs_metrics::encode_text(&registry)
                .unwrap()
                .contains("afs_dfs_sidecar_install_control_bytes 0\n")
        );
    }

    #[test]
    fn p2_w0_deadline_between_install_and_directory_barrier_skips_new_sync() {
        let temp = tempfile::tempdir().unwrap();
        let registry = afs_metrics::Registry::new();
        let metrics = super::super::read_work::ReadWorkMetrics::register(&registry).unwrap();
        let budget =
            OutboardBudget::new_with_metrics(DEFAULT_OUTBOARD_BUDGET_BYTES, metrics.clone());
        let staged = pooled_outboard_chunks(&budget, 1, 128 * 1024)
            .pop()
            .unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a")
            .unwrap()
            .with_outboard_budget(budget)
            .with_read_work_metrics(metrics);
        let (entered, entered_rx) = std::sync::mpsc::channel();
        let (release, release_rx) = std::sync::mpsc::channel();
        *store
            .sidecar_install_test
            .before_directory_sync
            .lock()
            .unwrap() = Some((entered, release_rx));
        let receipt = store.put(staged.clone()).unwrap();
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(
            fs::read(
                store
                    .outboards
                    .join(format!("{}.bao", safe_id(&staged.chunk.id.0)))
            )
            .unwrap(),
            staged.outboard.as_ref().unwrap().encoded()
        );
        store.sidecar_installer.close(Instant::now());
        release.send(()).unwrap();
        store.sidecar_installer.wait_idle();
        let io = *store.sidecar_install_test.optional_io.lock().unwrap();
        assert_eq!(
            (io.writes, io.file_syncs, io.renames, io.directory_syncs),
            (1, 1, 1, 0)
        );
        assert!(
            afs_metrics::encode_text(&registry)
                .unwrap()
                .contains("afs_dfs_outboard_events_total{event=\"sidecar_install_dropped\"} 1\n")
        );
        assert_eq!(receipt.chunk, staged.chunk);
        assert_eq!(
            fs::read(store.chunk_path(&staged.chunk.id)).unwrap(),
            staged.bytes()
        );
        store.sidecar_installer.shutdown().unwrap();
    }

    #[test]
    fn p2_w0_all_configured_queue_sizes_can_admit_real_sidecars() {
        for limit in [1, 32, 64] {
            let temp = tempfile::tempdir().unwrap();
            let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
            let staged = pooled_outboard_chunks(&budget, 1, 128 * 1024)
                .pop()
                .unwrap();
            let store = LocalChunkStore::open(temp.path(), "node-a")
                .unwrap()
                .with_outboard_budget(budget.clone())
                .with_sidecar_install_queue(limit)
                .unwrap();
            store.put(staged.clone()).unwrap();
            store.sidecar_installer.wait_idle();
            assert_eq!(
                fs::read(
                    store
                        .outboards
                        .join(format!("{}.bao", safe_id(&staged.chunk.id.0)))
                )
                .unwrap(),
                staged.outboard.as_ref().unwrap().encoded(),
                "queue limit={limit}"
            );
            drop(staged);
            store.sidecar_installer.shutdown().unwrap();
            assert_eq!(budget.used(), 0, "queue limit={limit}");
        }
    }

    #[test]
    fn p2_w0_control_path_allowance_covers_long_lengths_and_checks_overflow() {
        for length in [0, 64, 1024, 4095] {
            let allowance = sidecar_path_bytes(length).unwrap();
            let directory = length;
            let formal_join = 2 * (length + SIDECAR_ID_CAPACITY);
            let temporary_join = 2 * (length + 128);
            assert!(
                allowance >= directory + formal_join + temporary_join + SIDECAR_ID_CAPACITY + 128
            );
            assert!(sidecar_control_bytes(64, 112, 64, length).unwrap() >= allowance);
        }
        assert!(sidecar_path_bytes(usize::MAX).is_none());
        assert!(sidecar_control_bytes(usize::MAX, 112, 64, 4095).is_none());
    }

    #[test]
    fn p2_w0_stop_rejects_new_items_discards_pending_and_keeps_active_owned() {
        let temp = tempfile::tempdir().unwrap();
        let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
        let staged = pooled_outboard_chunks(&budget, 3, 4096);
        let store = LocalChunkStore::open(temp.path(), "node-a")
            .unwrap()
            .with_outboard_budget(budget.clone());
        let (entered, release) = block_next_sidecar_install(&store);
        store.put(staged[0].clone()).unwrap();
        entered.recv_timeout(Duration::from_secs(5)).unwrap();
        store.put(staged[1].clone()).unwrap();
        store.sidecar_installer.close(Instant::now());
        let deadline = store.sidecar_installer.queue.state.lock().unwrap().deadline;
        store
            .sidecar_installer
            .close(Instant::now() + Duration::from_secs(1));
        assert_eq!(
            store.sidecar_installer.queue.state.lock().unwrap().deadline,
            deadline
        );
        store.put(staged[2].clone()).unwrap();
        assert!(budget.used() > 0);
        release.send(()).unwrap();
        store.sidecar_installer.shutdown().unwrap();
        let state = store.sidecar_installer.queue.state.lock().unwrap();
        assert_eq!(
            (state.pending.len(), state.active, state.unfinished.len()),
            (0, 0, 0)
        );
        drop(state);
        for item in &staged[1..] {
            assert!(
                !store
                    .outboards
                    .join(format!("{}.bao", safe_id(&item.chunk.id.0)))
                    .exists()
            );
        }
        assert!(budget.used() > 0);
        drop(staged);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    #[ignore = "child process only; invoked by the bounded crash-recovery test"]
    fn p2_w0_crash_after_ack_child() {
        let Ok(root) = std::env::var("AFS_TEST_SIDECAR_CRASH_ROOT") else {
            return;
        };
        let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
        let staged = pooled_outboard_chunks(&budget, 1, 128 * 1024)
            .pop()
            .unwrap();
        let store = LocalChunkStore::open(root, "node-a")
            .unwrap()
            .with_outboard_budget(budget);
        let (entered, _release) = block_next_sidecar_install(&store);
        store.put(staged).unwrap();
        entered.recv_timeout(Duration::from_secs(5)).unwrap();
        std::process::exit(73);
    }

    #[test]
    fn p2_w0_crash_after_ack_recovers_body_and_repairs_sidecar_once() {
        let temp = tempfile::tempdir().unwrap();
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "node::chunk::replica_replay_tests::p2_w0_crash_after_ack_child",
                "--nocapture",
            ])
            .env("AFS_TEST_SIDECAR_CRASH_ROOT", temp.path())
            .output()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(73),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let staged = StagedChunk::new(OperationId::new("expected"), vec![0; 128 * 1024]);
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let catalog = fs::read(&store.catalog_path).unwrap();
        take_outboard_persist_io();
        for _ in 0..2 {
            let reader = store.open_verified(&staged.chunk.id).unwrap();
            let mut out = vec![0xa5; staged.bytes().len() + 1];
            assert_eq!(reader.read_at(0, &mut out).unwrap(), staged.bytes().len());
            assert_eq!(&out[..staged.bytes().len()], staged.bytes());
            assert_eq!(out[staged.bytes().len()], 0xa5);
            assert_eq!(reader.read_at(staged.chunk.length, &mut [0; 1]).unwrap(), 0);
        }
        assert_eq!(take_outboard_persist_io().writes, 1);
        assert_eq!(fs::read(&store.catalog_path).unwrap(), catalog);
    }

    #[test]
    #[ignore = "child process only; invoked by the blocked-worker watchdog test"]
    fn p2_w0_blocked_worker_watchdog_child() {
        if std::env::var_os("AFS_TEST_SIDECAR_WATCHDOG").is_none() {
            return;
        }
        let temp = tempfile::tempdir().unwrap();
        let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
        let staged = pooled_outboard_chunks(&budget, 1, 128 * 1024)
            .pop()
            .unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a")
            .unwrap()
            .with_outboard_budget(budget);
        let (entered, _release) = block_next_sidecar_install(&store);
        store.put(staged).unwrap();
        entered.recv_timeout(Duration::from_secs(5)).unwrap();
        let watchdog = crate::runtime::ShutdownDeadline::new(Duration::from_millis(100)).unwrap();
        watchdog.trigger().arm();
        // Joining must keep ownership of this blocked worker. Only the
        // independent process watchdog is allowed to end this failure case.
        store.sidecar_installer.shutdown().unwrap();
        watchdog.complete();
    }

    #[test]
    fn p2_w0_blocked_worker_uses_existing_failure_watchdog_not_detach() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "node::chunk::replica_replay_tests::p2_w0_blocked_worker_watchdog_child",
                "--nocapture",
            ])
            .env("AFS_TEST_SIDECAR_WATCHDOG", "1")
            .output()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(124),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn pooled_outboard_chunks(
        budget: &Arc<OutboardBudget>,
        count: usize,
        length: usize,
    ) -> Vec<StagedChunk> {
        let pool = WriteBufferPool::new_with_outboard_budget(count * length, budget.clone());
        (0..count)
            .map(|index| {
                let mut builder = ChunkBuilder::with_pool(pool.clone(), length).unwrap();
                builder.write_at(0, &vec![index as u8; length]).unwrap();
                let staged = builder.stage(OperationId::new(format!("batch-proof-{index}")));
                assert!(staged.outboard.as_ref().unwrap().uses_budget(budget));
                staged
            })
            .collect()
    }

    fn take_outboard_persist_io() -> OutboardPersistIo {
        OUTBOARD_PERSIST_IO.with(|counts| std::mem::take(&mut *counts.borrow_mut()))
    }

    fn take_store_outboard_persist_io(store: &LocalChunkStore) -> OutboardPersistIo {
        store.sidecar_installer.wait_idle();
        let mut io = take_outboard_persist_io();
        let background =
            std::mem::take(&mut *store.sidecar_install_test.optional_io.lock().unwrap());
        io.writes += background.writes;
        io.file_syncs += background.file_syncs;
        io.renames += background.renames;
        io.directory_syncs += background.directory_syncs;
        io
    }

    #[test]
    fn coarse_persist_diagnostics_preserve_body_proof_barriers_and_receipts() {
        for enabled in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let registry = afs_metrics::Registry::new();
            let metrics = super::super::read_work::ReadWorkMetrics::register(&registry)
                .unwrap()
                .with_read_diagnostics(enabled);
            let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
            let staged = pooled_outboard_chunks(&budget, 2, 128 * 1024);
            let store = LocalChunkStore::open(temp.path(), "node-a")
                .unwrap()
                .with_outboard_budget(budget)
                .with_read_work_metrics(metrics.clone());
            take_store_outboard_persist_io(&store);
            let receipts = store.put_batch(staged.clone()).unwrap();
            store.sidecar_installer.wait_idle();
            assert_eq!(receipts.len(), 2);
            assert_eq!(
                take_store_outboard_persist_io(&store),
                OutboardPersistIo {
                    writes: 2,
                    file_syncs: 2,
                    renames: 2,
                    directory_syncs: 1,
                }
            );
            for (receipt, item) in receipts.iter().zip(&staged) {
                assert_eq!(receipt.chunk, item.chunk);
                assert_eq!(
                    receipt.durable_acks[0].verified_digest,
                    item.chunk.content_digest
                );
                assert_eq!(
                    fs::read(store.chunk_path(&item.chunk.id)).unwrap(),
                    item.bytes()
                );
            }
            match metrics.diagnostics_snapshot() {
                None => assert!(!enabled),
                Some(snapshot) => {
                    assert!(enabled);
                    assert_eq!(snapshot.records.len(), 1);
                    assert_eq!(
                        snapshot.records[0].stage,
                        super::super::read_work::ReadDiagnosticStage::PersistBatch
                    );
                    assert!(snapshot.records[0].success);
                    assert_eq!(snapshot.overflow, 0);
                    for (name, calls) in [
                        ("persist_batch", 1),
                        ("persist_body_write", 2),
                        ("persist_body_sync", 2),
                        ("persist_chunks_dir_sync", 1),
                        ("persist_catalog_write", 1),
                        ("persist_catalog_sync", 1),
                        ("persist_catalog_parent_sync", 1),
                        ("persist_proof_install_sync", 2),
                        ("persist_proof_dir_sync", 1),
                    ] {
                        let phase = snapshot
                            .coarse_phases
                            .iter()
                            .find(|phase| phase.phase == name)
                            .unwrap();
                        assert_eq!(phase.calls, calls, "{name}");
                        assert!(phase.last_end_ns >= phase.first_start_ns, "{name}");
                    }
                }
            }
        }
    }

    #[test]
    fn pr60_healthy_sidecars_are_read_only_across_peer_objects_and_pinned_opens() {
        let temp = tempfile::tempdir().unwrap();
        let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
        let staged = pooled_outboard_chunks(&budget, 3, 128 * 1024);
        let store = LocalChunkStore::open(temp.path(), "node-a")
            .unwrap()
            .with_outboard_budget(budget);
        store.put_batch(staged.clone()).unwrap();
        store.sidecar_installer.wait_idle();
        let catalog = fs::read(&store.catalog_path).unwrap();
        let catalog_identity =
            PeerFileIdentity::read(&File::open(&store.catalog_path).unwrap()).unwrap();
        let bodies = staged
            .iter()
            .map(|item| {
                let path = store.chunks.join(&item.chunk.id.0);
                (
                    fs::read(&path).unwrap(),
                    PeerFileIdentity::read(&File::open(path).unwrap()).unwrap(),
                )
            })
            .collect::<Vec<_>>();
        take_store_outboard_persist_io(&store);
        for item in &staged {
            let payload = store
                .read_peer_range(&item.chunk.id, 0, item.bytes().len())
                .unwrap();
            assert_eq!(payload.as_slice(), item.bytes());
            drop(payload);
            let reader = store.open_verified(&item.chunk.id).unwrap();
            let mut out = [0; 23];
            assert_eq!(reader.read_at(7, &mut out).unwrap(), out.len());
            assert_eq!(&out, &item.bytes()[7..30]);
        }
        assert_eq!(
            take_store_outboard_persist_io(&store),
            OutboardPersistIo::default(),
            "healthy read paths must not write or fsync sidecars"
        );
        assert_eq!(fs::read(&store.catalog_path).unwrap(), catalog);
        assert_eq!(
            PeerFileIdentity::read(&File::open(&store.catalog_path).unwrap()).unwrap(),
            catalog_identity
        );
        for (item, (body, identity)) in staged.iter().zip(bodies) {
            let path = store.chunks.join(&item.chunk.id.0);
            assert_eq!(fs::read(&path).unwrap(), body);
            assert_eq!(
                PeerFileIdentity::read(&File::open(path).unwrap()).unwrap(),
                identity
            );
        }
    }

    #[test]
    fn pr60_new_pinned_readers_share_one_missing_sidecar_repair() {
        let temp = tempfile::tempdir().unwrap();
        let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
        let staged = pooled_outboard_chunks(&budget, 1, 128 * 1024)
            .pop()
            .unwrap();
        let store = Arc::new(
            LocalChunkStore::open(temp.path(), "node-a")
                .unwrap()
                .with_outboard_budget(budget),
        );
        store.put(staged.clone()).unwrap();
        store.sidecar_installer.wait_idle();
        let proof_path = store
            .outboards
            .join(format!("{}.bao", safe_id(&staged.chunk.id.0)));
        fs::remove_file(&proof_path).unwrap();
        let catalog = fs::read(&store.catalog_path).unwrap();
        let barrier = Arc::new(std::sync::Barrier::new(5));
        let mut threads = Vec::new();
        for _ in 0..4 {
            let store = store.clone();
            let staged = staged.clone();
            let barrier = barrier.clone();
            threads.push(std::thread::spawn(move || {
                take_outboard_persist_io();
                barrier.wait();
                let reader = store.open_verified(&staged.chunk.id).unwrap();
                let mut out = [0; 19];
                assert_eq!(reader.read_at(3, &mut out).unwrap(), out.len());
                assert_eq!(&out, &staged.bytes()[3..22]);
                take_outboard_persist_io()
            }));
        }
        barrier.wait();
        let mut total = OutboardPersistIo::default();
        for thread in threads {
            let io = thread.join().unwrap();
            total.writes += io.writes;
            total.file_syncs += io.file_syncs;
            total.renames += io.renames;
            total.directory_syncs += io.directory_syncs;
        }
        assert_eq!(
            total,
            OutboardPersistIo {
                writes: 1,
                file_syncs: 1,
                renames: 1,
                directory_syncs: 1
            }
        );
        assert_eq!(
            fs::read(&proof_path).unwrap(),
            staged.outboard.as_ref().unwrap().encoded()
        );
        assert_eq!(fs::read(&store.catalog_path).unwrap(), catalog);
        assert_eq!(
            fs::read(store.chunks.join(&staged.chunk.id.0)).unwrap(),
            staged.bytes()
        );
    }

    #[test]
    fn pr60_sidecar_repair_runs_after_releasing_peer_slot() {
        let temp = tempfile::tempdir().unwrap();
        let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
        let staged = pooled_outboard_chunks(&budget, 1, 128 * 1024)
            .pop()
            .unwrap();
        let store = Arc::new(
            LocalChunkStore::open(temp.path(), "node-a")
                .unwrap()
                .with_outboard_budget(budget),
        );
        store.put(staged.clone()).unwrap();
        store.sidecar_installer.wait_idle();
        fs::remove_file(
            store
                .outboards
                .join(format!("{}.bao", safe_id(&staged.chunk.id.0))),
        )
        .unwrap();
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let worker_store = store.clone();
        let worker = std::thread::spawn(move || {
            OUTBOARD_INSTALL_HOOK.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move || {
                    entered_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                }))
            });
            worker_store
                .read_peer_range(&staged.chunk.id, 0, staged.bytes().len())
                .unwrap()
        });
        entered_rx
            .recv_timeout(std::time::Duration::from_secs(3))
            .unwrap();
        let available = store.peer_read_cache.try_lock().is_ok();
        release_tx.send(()).unwrap();
        let payload = worker.join().unwrap();
        assert_eq!(payload.as_slice(), &[0; 128 * 1024]);
        assert!(
            available,
            "optional proof repair must not hold the global body-cache slot"
        );
    }

    #[test]
    fn pr60_corrupt_sidecar_repairs_once_without_rewriting_body_or_catalog() {
        let temp = tempfile::tempdir().unwrap();
        let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
        let staged = pooled_outboard_chunks(&budget, 1, 128 * 1024)
            .pop()
            .unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a")
            .unwrap()
            .with_outboard_budget(budget);
        store.put(staged.clone()).unwrap();
        store.sidecar_installer.wait_idle();
        let path = store
            .outboards
            .join(format!("{}.bao", safe_id(&staged.chunk.id.0)));
        let mut bytes = fs::read(&path).unwrap();
        bytes[64] ^= 1;
        fs::write(&path, bytes).unwrap();
        let catalog = fs::read(&store.catalog_path).unwrap();
        take_store_outboard_persist_io(&store);
        for _ in 0..2 {
            drop(store.open_verified(&staged.chunk.id).unwrap());
        }
        assert_eq!(
            take_store_outboard_persist_io(&store),
            OutboardPersistIo {
                writes: 1,
                file_syncs: 1,
                renames: 1,
                directory_syncs: 1
            }
        );
        assert_eq!(
            fs::read(&path).unwrap(),
            staged.outboard.as_ref().unwrap().encoded()
        );
        assert_eq!(fs::read(&store.catalog_path).unwrap(), catalog);
        assert_eq!(
            fs::read(store.chunks.join(&staged.chunk.id.0)).unwrap(),
            staged.bytes()
        );
    }

    #[test]
    fn pr60_capacity_fallback_preserves_healthy_pinned_proof_and_sidecar() {
        let temp = tempfile::tempdir().unwrap();
        let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
        let staged = pooled_outboard_chunks(&budget, 1, 128 * 1024)
            .pop()
            .unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a")
            .unwrap()
            .with_outboard_budget(budget.clone());
        store.put(staged.clone()).unwrap();
        store.sidecar_installer.wait_idle();
        let reader = store.open_verified(&staged.chunk.id).unwrap();
        let proof = reader.load_outboard().unwrap().unwrap();
        let sidecar = fs::read(&reader.outboard_path).unwrap();
        let before = budget.used();
        let held = budget.try_reserve(budget.limit() - budget.used()).unwrap();
        take_store_outboard_persist_io(&store);
        let mut out = [0xa5; 23];
        assert_eq!(reader.read_at(3, &mut out).unwrap(), out.len());
        assert_eq!(&out, &staged.bytes()[3..26]);
        assert!(Arc::ptr_eq(
            reader.outboard.lock().unwrap().as_ref().unwrap(),
            &proof
        ));
        assert_eq!(
            take_store_outboard_persist_io(&store),
            OutboardPersistIo::default()
        );
        assert_eq!(fs::read(&reader.outboard_path).unwrap(), sidecar);
        drop(held);
        assert_eq!(budget.used(), before);
        let payload = store
            .read_peer_range(&staged.chunk.id, 0, staged.bytes().len())
            .unwrap();
        assert!(
            payload
                .object
                .outboard
                .as_ref()
                .unwrap()
                .try_mark_resident()
        );
    }

    #[test]
    fn pr60_hot_peer_hit_skips_sidecar_repair_but_a_later_miss_repairs() {
        let temp = tempfile::tempdir().unwrap();
        let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
        let staged = pooled_outboard_chunks(&budget, 1, 128 * 1024)
            .pop()
            .unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a")
            .unwrap()
            .with_outboard_budget(budget);
        store.put(staged.clone()).unwrap();
        store.sidecar_installer.wait_idle();
        let cached = store
            .read_peer_range(&staged.chunk.id, 0, staged.bytes().len())
            .unwrap();
        assert!(
            cached.object.outboard.is_some(),
            "exercise the resident proof hit branch"
        );
        let path = store
            .outboards
            .join(format!("{}.bao", safe_id(&staged.chunk.id.0)));
        fs::remove_file(&path).unwrap();
        take_store_outboard_persist_io(&store);
        let hit = store.read_peer_range(&staged.chunk.id, 3, 17).unwrap();
        assert_eq!(hit.as_slice(), &staged.bytes()[3..20]);
        assert!(Arc::ptr_eq(&hit.object, &cached.object));
        assert!(
            !path.exists(),
            "a verified hot hit must not recreate an optional sidecar"
        );
        assert_eq!(
            take_store_outboard_persist_io(&store),
            OutboardPersistIo::default()
        );
        // Retain the old payload to prove slot eviction does not retire an
        // active allocation, while the next miss independently repairs proof.
        store.clear_peer_residents_for_test();
        let miss = store
            .read_peer_range(&staged.chunk.id, 0, staged.bytes().len())
            .unwrap();
        assert_eq!(miss.as_slice(), staged.bytes());
        assert!(!Arc::ptr_eq(&miss.object, &cached.object));
        assert_eq!(hit.as_slice(), &staged.bytes()[3..20]);
        assert_eq!(
            fs::read(&path).unwrap(),
            staged.outboard.as_ref().unwrap().encoded()
        );
        assert_eq!(
            take_store_outboard_persist_io(&store),
            OutboardPersistIo {
                writes: 1,
                file_syncs: 1,
                renames: 1,
                directory_syncs: 1,
            }
        );
    }

    #[test]
    fn pr60_partial_other_object_and_missing_id_do_not_evict_a_healthy_peer_slot() {
        let temp = tempfile::tempdir().unwrap();
        let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
        let staged = pooled_outboard_chunks(&budget, 2, 128 * 1024);
        let store = LocalChunkStore::open(temp.path(), "node-a")
            .unwrap()
            .with_outboard_budget(budget);
        store.put_batch(staged.clone()).unwrap();
        store.sidecar_installer.wait_idle();
        let cached = store
            .read_peer_range(&staged[0].chunk.id, 0, staged[0].bytes().len())
            .unwrap();
        let range = store.read_peer_range(&staged[1].chunk.id, 3, 17).unwrap();
        assert_eq!(range.as_slice(), &staged[1].bytes()[3..20]);
        assert!(Arc::ptr_eq(
            store.peer_read_cache.lock().unwrap().residents[0]
                .as_ref()
                .unwrap(),
            &cached.object
        ));
        assert!(
            store
                .read_peer_range(&ChunkId::new("missing-pr60"), 0, 17)
                .is_err()
        );
        assert!(Arc::ptr_eq(
            store.peer_read_cache.lock().unwrap().residents[0]
                .as_ref()
                .unwrap(),
            &cached.object
        ));
    }

    #[test]
    fn pr60_two_hundred_pinned_readers_leave_writer_and_range_work_capacity() {
        let temp = tempfile::tempdir().unwrap();
        let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
        let pool = WriteBufferPool::new_with_outboard_budget(4 * 1024 * 1024, budget.clone());
        let store = LocalChunkStore::open(temp.path(), "node-a")
            .unwrap()
            .with_outboard_budget(budget.clone());
        let mut readers = Vec::new();
        let mut body = vec![37; 4 * 1024 * 1024];
        for index in 0..200_u64 {
            body[..8].copy_from_slice(&index.to_le_bytes());
            let mut builder = ChunkBuilder::with_pool(pool.clone(), body.len()).unwrap();
            builder.write_at(0, &body).unwrap();
            let staged = builder.stage(OperationId::new(format!("natural-pinned-{index}")));
            assert!(
                staged.outboard.is_some(),
                "resident proofs must not starve writer proof generation"
            );
            let id = staged.chunk.id.clone();
            store.put(staged).unwrap();
            store.sidecar_installer.wait_idle();
            let reader = store.open_verified(&id).unwrap();
            let mut out = [0; 8];
            VERIFIED_SCAN_BYTES.with(|count| count.set(0));
            assert_eq!(reader.read_at(0, &mut out).unwrap(), 8);
            assert_eq!(
                VERIFIED_SCAN_BYTES.with(|count| count.get()),
                outboard::OUTBOARD_GROUP_BYTES as u64,
                "resident pressure must use a transient sidecar, not rescan 4MiB"
            );
            assert_eq!(out, index.to_le_bytes());
            readers.push(reader);
            assert!(budget.used() <= budget.limit());
            assert!(budget.resident_used() <= budget.resident_limit());
        }
        assert!(
            budget
                .reserve_range_work(
                    &[VerifiedRangeCopy {
                        chunk_offset: 0,
                        length: 8,
                        output_offset: 0,
                    }],
                    body.len() as u64,
                )
                .is_some()
        );
        drop(readers);
        store.sidecar_installer.wait_idle();
        store.sidecar_installer.shutdown().unwrap();
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn wi6_outboard_batch_syncs_directory_once_and_replay_skips_writes() {
        let temp = tempfile::tempdir().unwrap();
        let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
        let staged = pooled_outboard_chunks(&budget, 16, 4 * 1024 * 1024);
        let store = LocalChunkStore::open(temp.path(), "node-a")
            .unwrap()
            .with_outboard_budget(budget);
        take_store_outboard_persist_io(&store);
        let receipts = store.put_batch(staged.clone()).unwrap();
        store.sidecar_installer.wait_idle();
        assert_eq!(receipts.len(), 16);
        assert_eq!(
            take_store_outboard_persist_io(&store),
            OutboardPersistIo {
                writes: 16,
                file_syncs: 16,
                renames: 16,
                directory_syncs: 1,
            }
        );
        for item in &staged {
            let path = store
                .outboards
                .join(format!("{}.bao", safe_id(&item.chunk.id.0)));
            assert_eq!(
                fs::read(path).unwrap(),
                item.outboard.as_ref().unwrap().encoded()
            );
        }
        let revision = store.device_descriptor().unwrap().catalog_revision;
        let mut replay = staged.clone();
        replay.extend(staged);
        assert_eq!(store.put_batch(replay).unwrap().len(), 32);
        assert_eq!(
            store.device_descriptor().unwrap().catalog_revision,
            revision
        );
        assert_eq!(
            take_store_outboard_persist_io(&store),
            OutboardPersistIo {
                writes: 0,
                file_syncs: 0,
                renames: 0,
                directory_syncs: 1,
            }
        );
    }

    #[test]
    fn wi6_outboard_batch_deduplicates_installs_and_tracks_actual_batches() {
        let temp = tempfile::tempdir().unwrap();
        let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
        let staged = pooled_outboard_chunks(&budget, 4, 128 * 1024);
        let store = LocalChunkStore::open(temp.path(), "node-a")
            .unwrap()
            .with_outboard_budget(budget);
        take_store_outboard_persist_io(&store);
        for pair in staged.chunks(2) {
            let mut batch = pair.to_vec();
            batch.extend_from_slice(pair);
            assert_eq!(store.put_batch(batch).unwrap().len(), 4);
            store.sidecar_installer.wait_idle();
        }
        assert_eq!(
            take_store_outboard_persist_io(&store),
            OutboardPersistIo {
                writes: 4,
                file_syncs: 4,
                renames: 4,
                directory_syncs: 2,
            }
        );
        store.put_batch(Vec::new()).unwrap();
        store.sidecar_installer.wait_idle();
        store
            .put(StagedChunk::new(
                OperationId::new("no-proof"),
                vec![8; 4096],
            ))
            .unwrap();
        assert_eq!(
            take_store_outboard_persist_io(&store),
            OutboardPersistIo::default()
        );
    }

    #[test]
    fn wi6_outboard_batch_repairs_corrupt_cv_even_with_matching_header() {
        let temp = tempfile::tempdir().unwrap();
        let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
        let staged = pooled_outboard_chunks(&budget, 1, 128 * 1024)
            .pop()
            .unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a")
            .unwrap()
            .with_outboard_budget(budget);
        store.put(staged.clone()).unwrap();
        store.sidecar_installer.wait_idle();
        let path = store
            .outboards
            .join(format!("{}.bao", safe_id(&staged.chunk.id.0)));
        let mut corrupt = fs::read(&path).unwrap();
        corrupt[64] ^= 1;
        ChunkOutboard::validate_encoded_shape(&staged.chunk, &corrupt).unwrap();
        fs::write(&path, corrupt).unwrap();
        take_store_outboard_persist_io(&store);
        store.put(staged.clone()).unwrap();
        store.sidecar_installer.wait_idle();
        assert_eq!(
            fs::read(path).unwrap(),
            staged.outboard.as_ref().unwrap().encoded()
        );
        assert_eq!(
            take_store_outboard_persist_io(&store),
            OutboardPersistIo {
                writes: 1,
                file_syncs: 1,
                renames: 1,
                directory_syncs: 1,
            }
        );
    }

    #[test]
    fn wi6_outboard_batch_optional_failures_keep_acks_and_retry_barriers() {
        let temp = tempfile::tempdir().unwrap();
        let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
        let staged = pooled_outboard_chunks(&budget, 2, 128 * 1024);
        let store = LocalChunkStore::open(temp.path(), "node-a")
            .unwrap()
            .with_outboard_budget(budget);
        let blocked = store
            .outboards
            .join(format!("{}.bao", safe_id(&staged[0].chunk.id.0)));
        fs::create_dir(&blocked).unwrap();
        take_store_outboard_persist_io(&store);
        assert_eq!(store.put_batch(staged.clone()).unwrap().len(), 2);
        assert_eq!(
            take_store_outboard_persist_io(&store),
            OutboardPersistIo {
                writes: 2,
                file_syncs: 2,
                renames: 1,
                directory_syncs: 1,
            }
        );
        assert!(fs::read_dir(&store.outboards).unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .ends_with(".tmp")
        }));
        fs::remove_dir(blocked).unwrap();
        store
            .sidecar_install_test
            .fail_directory_sync
            .store(true, Ordering::Relaxed);
        assert_eq!(store.put_batch(staged.clone()).unwrap().len(), 2);
        assert_eq!(
            take_store_outboard_persist_io(&store),
            OutboardPersistIo {
                writes: 1,
                file_syncs: 1,
                renames: 1,
                directory_syncs: 0,
            }
        );
        assert_eq!(store.put_batch(staged.clone()).unwrap().len(), 2);
        assert_eq!(
            take_store_outboard_persist_io(&store),
            OutboardPersistIo {
                writes: 0,
                file_syncs: 0,
                renames: 0,
                directory_syncs: 1,
            }
        );
        drop(store);
        let recovered = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        for item in staged {
            let mut out = [0; 17];
            assert_eq!(recovered.read_at(&item.chunk.id, 3, &mut out).unwrap(), 17);
            assert_eq!(out, item.bytes()[3..20]);
        }
    }

    fn t1_pause_budget_wait(
        store: &LocalChunkStore,
    ) -> (std::sync::mpsc::Receiver<()>, std::sync::mpsc::Sender<()>) {
        let (entered, observed) = std::sync::mpsc::channel();
        let (release, waiting) = std::sync::mpsc::channel();
        *store.peer_read_budget.before_wait.lock().unwrap() = Some((entered, waiting));
        (observed, release)
    }

    #[test]
    fn p2_t1_last_lease_release_cannot_be_lost_before_wait() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a")
            .unwrap()
            .with_peer_serve_wait(Duration::from_millis(50))
            .unwrap();
        let held = store
            .reserve_peer_read_payload(PEER_READ_PAYLOAD_LIMIT)
            .unwrap();
        let (observed, release) = t1_pause_budget_wait(&store);
        std::thread::scope(|scope| {
            let reader = scope
                .spawn(|| store.reserve_peer_read_payload_until(17, store.peer_serve_deadline()));
            observed.recv_timeout(Duration::from_secs(3)).unwrap();
            // The waiter holds the predicate mutex here. A releaser racing this
            // gap must synchronize before refunding and notifying it.
            let (started, starting) = std::sync::mpsc::channel();
            let returning = scope.spawn(move || {
                started.send(()).unwrap();
                drop(held);
            });
            starting.recv_timeout(Duration::from_secs(3)).unwrap();
            release.send(()).unwrap();
            returning.join().unwrap();
            let admitted = reader.join().unwrap().unwrap();
            assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 17);
            drop(admitted);
        });
        assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 0);
    }

    #[test]
    fn p2_t1_control_release_and_stop_wake_budget_waiters() {
        for stop in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let store = LocalChunkStore::open(temp.path(), "node-a")
                .unwrap()
                .with_peer_serve_wait(Duration::from_millis(50))
                .unwrap();
            let limit = PEER_SERVE_CONTROL_LIMIT - peer_serve_fixed_control_bytes();
            let held = store.reserve_peer_control(limit).unwrap();
            let (observed, release) = t1_pause_budget_wait(&store);
            std::thread::scope(|scope| {
                let reader = scope
                    .spawn(|| store.reserve_peer_control_until(17, store.peer_serve_deadline()));
                observed.recv_timeout(Duration::from_secs(3)).unwrap();
                release.send(()).unwrap();
                if stop {
                    store.close_peer_serve_admission();
                    assert_eq!(
                        reader.join().unwrap().unwrap_err().code(),
                        afs_error::IO_UNAVAILABLE
                    );
                    assert_eq!(
                        store.peer_read_budget.control_used.load(Ordering::Acquire),
                        limit as u64
                    );
                    drop(held);
                } else {
                    drop(held);
                    let admitted = reader.join().unwrap().unwrap();
                    assert_eq!(
                        store.peer_read_budget.control_used.load(Ordering::Acquire),
                        17
                    );
                    drop(admitted);
                }
            });
            assert_eq!(
                store.peer_read_budget.control_used.load(Ordering::Acquire),
                0
            );
        }
    }

    #[test]
    fn p2_t1_zero_wait_and_expired_entry_deadline_never_enter_wait() {
        for wait in [Duration::ZERO, Duration::from_millis(20)] {
            let temp = tempfile::tempdir().unwrap();
            let store = LocalChunkStore::open(temp.path(), "node-a")
                .unwrap()
                .with_peer_serve_wait(wait)
                .unwrap();
            let held = store
                .reserve_peer_read_payload(PEER_READ_PAYLOAD_LIMIT)
                .unwrap();
            let (_observed, _release) = t1_pause_budget_wait(&store);
            assert_eq!(
                store
                    .reserve_peer_read_payload_until(1, Instant::now())
                    .unwrap_err()
                    .code(),
                afs_error::IO_CAPACITY
            );
            assert!(
                store.peer_read_budget.before_wait.lock().unwrap().is_some(),
                "expired/zero allowance cannot be renewed at inner admission"
            );
            drop(held);
            let immediate = store
                .reserve_peer_read_payload_until(1, Instant::now())
                .unwrap();
            drop(immediate);
        }
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        assert_eq!(store.peer_read_budget.wait, Duration::from_millis(20));
        assert!(
            store
                .with_peer_serve_wait(Duration::from_millis(51))
                .is_err()
        );
    }

    #[test]
    fn p2_t1_notifications_do_not_renew_the_entry_deadline() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let held = store
            .reserve_peer_read_payload(PEER_READ_PAYLOAD_LIMIT)
            .unwrap();
        let notifying = std::sync::atomic::AtomicBool::new(true);
        let (done, completed) = std::sync::mpsc::channel();
        std::thread::scope(|scope| {
            let reader = scope.spawn(|| {
                done.send(
                    store
                        .reserve_peer_read_payload_until(1, store.peer_serve_deadline())
                        .map(drop),
                )
                .unwrap();
            });
            let notifier = scope.spawn(|| {
                while notifying.load(Ordering::Acquire) {
                    store.peer_read_budget.changed.notify_all();
                    std::thread::yield_now();
                }
            });
            // This generous bound checks termination, not a performance claim.
            // Continual spurious notifications must not restart a 20ms wait.
            let result = completed.recv_timeout(Duration::from_secs(1));
            notifying.store(false, Ordering::Release);
            if result.is_err() {
                store.close_peer_serve_admission();
            }
            notifier.join().unwrap();
            reader.join().unwrap();
            assert_eq!(result.unwrap().unwrap_err().code(), afs_error::IO_CAPACITY);
        });
        assert_eq!(
            store.peer_read_budget.used.load(Ordering::Acquire),
            PEER_READ_PAYLOAD_LIMIT as u64
        );
        drop(held);
    }

    #[cfg(feature = "dfs")]
    #[test]
    fn p2_t1_last_stream_slice_keeps_capacity_until_its_release() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a")
            .unwrap()
            .with_peer_serve_wait(Duration::from_millis(50))
            .unwrap();
        let staged = StagedChunk::new(OperationId::new("t1-pinned-frame"), vec![37; 3 * 32768]);
        store.put(staged.clone()).unwrap();
        drop(store.open_verified(&staged.chunk.id).unwrap());
        let mut stream = store
            .open_peer_group_stream(&staged.chunk.id, 0, staged.bytes().len())
            .unwrap()
            .unwrap();
        let first = stream.next_group(|| Ok(())).unwrap().unwrap();
        let retained = first.slice(3..11);
        let second = stream.next_group(|| Ok(())).unwrap().unwrap();
        drop(stream);
        drop(first);
        drop(second);
        let held = store
            .reserve_peer_read_payload(PEER_READ_PAYLOAD_LIMIT - 65536)
            .unwrap();
        assert_eq!(
            store.peer_read_budget.used.load(Ordering::Acquire),
            PEER_READ_PAYLOAD_LIMIT as u64
        );
        let (observed, release) = t1_pause_budget_wait(&store);
        std::thread::scope(|scope| {
            let reader = scope
                .spawn(|| store.reserve_peer_read_payload_until(17, store.peer_serve_deadline()));
            observed.recv_timeout(Duration::from_secs(3)).unwrap();
            assert_eq!(retained.as_ref(), &[37; 8]);
            assert_eq!(
                store.peer_read_budget.used.load(Ordering::Acquire),
                PEER_READ_PAYLOAD_LIMIT as u64
            );
            release.send(()).unwrap();
            drop(retained);
            let admitted = reader.join().unwrap().unwrap();
            assert_eq!(
                store.peer_read_budget.used.load(Ordering::Acquire),
                (PEER_READ_PAYLOAD_LIMIT - 65536 + 17) as u64
            );
            drop(admitted);
        });
        drop(held);
        assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 0);
    }

    #[cfg(feature = "dfs")]
    #[test]
    fn p2_r4_two_frames_hold_exact_budget_until_last_reference() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let staged = StagedChunk::new(OperationId::new("r4-frame-leases"), vec![37; 3 * 32768]);
        store.put(staged.clone()).unwrap();
        drop(store.open_verified(&staged.chunk.id).unwrap());
        let mut stream = store
            .open_peer_group_stream(&staged.chunk.id, 0, staged.bytes().len())
            .unwrap()
            .unwrap();
        let first = stream.next_group(|| Ok(())).unwrap().unwrap();
        let second = stream.next_group(|| Ok(())).unwrap().unwrap();
        assert_eq!(first.len(), 32768);
        assert_eq!(second.len(), 32768);
        let retained = first.slice(17..29);
        assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 65536);
        drop(stream);
        drop(first);
        assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 65536);
        drop(second);
        assert_eq!(
            store.peer_read_budget.used.load(Ordering::Acquire),
            65536,
            "the surviving sliced frame still owns the two-block pool"
        );
        assert_eq!(retained.as_ref(), &[37; 12]);
        drop(retained);
        assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 0);
        assert_eq!(
            store.peer_read_budget.control_used.load(Ordering::Acquire),
            0
        );
        assert!(
            store
                .peer_read_cache
                .lock()
                .unwrap()
                .residents
                .iter()
                .all(Option::is_none)
        );
    }

    #[cfg(feature = "dfs")]
    #[test]
    fn p2_r4_retained_frames_backpressure_without_early_refund() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let staged = StagedChunk::new(OperationId::new("r4-backpressure"), vec![37; 4 * 32768]);
        store.put(staged.clone()).unwrap();
        drop(store.open_verified(&staged.chunk.id).unwrap());
        let mut stream = store
            .open_peer_group_stream(&staged.chunk.id, 0, staged.bytes().len())
            .unwrap()
            .unwrap();
        let first = stream.next_group(|| Ok(())).unwrap().unwrap();
        let second = stream.next_group(|| Ok(())).unwrap().unwrap();
        let (entered, observed) = std::sync::mpsc::channel();
        let (done, received) = std::sync::mpsc::channel();
        std::thread::scope(|scope| {
            let producer = scope.spawn(|| {
                // Repeated checks prove we reached the no-free-block wait,
                // rather than inferring blocking from scheduler delays.
                let calls = std::cell::Cell::new(0);
                let third = stream
                    .next_group(|| {
                        calls.set(calls.get() + 1);
                        if calls.get() == 3 {
                            entered.send(()).unwrap();
                        }
                        Ok(())
                    })
                    .unwrap()
                    .unwrap();
                done.send(third).unwrap();
            });
            observed.recv_timeout(Duration::from_secs(3)).unwrap();
            assert!(received.try_recv().is_err());
            assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 65536);
            drop(first);
            let third = received.recv_timeout(Duration::from_secs(3)).unwrap();
            assert_eq!(third.as_ref(), &[37; 32768]);
            drop(third);
            producer.join().unwrap();
        });
        drop(second);
        drop(stream);
        assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 0);
    }

    #[cfg(feature = "dfs")]
    #[test]
    fn p2_r4_stop_and_caller_failure_interrupt_retained_frame_wait() {
        for stop_node in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
            let staged = StagedChunk::new(OperationId::new("r4-wait-cancel"), vec![37; 4 * 32768]);
            store.put(staged.clone()).unwrap();
            drop(store.open_verified(&staged.chunk.id).unwrap());
            let mut stream = store
                .open_peer_group_stream(&staged.chunk.id, 0, staged.bytes().len())
                .unwrap()
                .unwrap();
            let first = stream.next_group(|| Ok(())).unwrap().unwrap();
            let second = stream.next_group(|| Ok(())).unwrap().unwrap();
            let valid = std::sync::atomic::AtomicBool::new(true);
            let (entered, observed) = std::sync::mpsc::channel();
            std::thread::scope(|scope| {
                let producer = scope.spawn(|| {
                    let calls = std::cell::Cell::new(0);
                    stream.next_group(|| {
                        calls.set(calls.get() + 1);
                        if calls.get() == 3 {
                            entered.send(()).unwrap();
                        }
                        if valid.load(Ordering::Acquire) {
                            Ok(())
                        } else {
                            Err(Error::coded(
                                afs_error::IO_PERMISSION_DENIED,
                                "caller revoked",
                            ))
                        }
                    })
                });
                observed.recv_timeout(Duration::from_secs(3)).unwrap();
                if stop_node {
                    store.close_peer_serve_admission();
                } else {
                    valid.store(false, Ordering::Release);
                }
                let error = producer.join().unwrap().unwrap_err();
                assert_eq!(
                    error.code(),
                    if stop_node {
                        afs_error::IO_UNAVAILABLE
                    } else {
                        afs_error::IO_PERMISSION_DENIED
                    }
                );
            });
            drop(stream);
            assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 65536);
            drop(first);
            drop(second);
            assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 0);
        }
    }

    #[cfg(feature = "dfs")]
    #[test]
    fn p2_r4_stream_reuses_two_addresses_and_checks_each_group_root() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let body: Vec<_> = (0..150_001).map(|n| (n % 251) as u8).collect();
        let staged = StagedChunk::new(OperationId::new("r4-reuse"), body.clone());
        store.put(staged.clone()).unwrap();
        drop(store.open_verified(&staged.chunk.id).unwrap());
        VERIFIED_SCAN_BYTES.with(|count| count.set(0));
        let mut stream = store
            .open_peer_group_stream(&staged.chunk.id, 0, body.len())
            .unwrap()
            .unwrap();
        let mut received = Vec::new();
        let mut addresses = HashSet::new();
        while let Some(group) = stream.next_group(|| Ok(())).unwrap() {
            addresses.insert(group.as_ptr());
            received.extend_from_slice(&group);
            assert!(store.peer_read_budget.used.load(Ordering::Acquire) <= 65536);
        }
        assert_eq!(received, body);
        assert!(addresses.len() <= 2);
        assert_eq!(
            VERIFIED_SCAN_BYTES.with(|count| count.get()),
            body.len() as u64
        );
        drop(stream);
        assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 0);
    }

    #[cfg(feature = "dfs")]
    #[test]
    fn p2_r4_path_replacement_and_catalog_change_cannot_complete_stream() {
        for replace_path in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
            let staged =
                StagedChunk::new(OperationId::new("r4-stream-identity"), vec![37; 3 * 32768]);
            store.put(staged.clone()).unwrap();
            drop(store.open_verified(&staged.chunk.id).unwrap());
            let mut stream = store
                .open_peer_group_stream(&staged.chunk.id, 0, staged.bytes().len())
                .unwrap()
                .unwrap();
            drop(stream.next_group(|| Ok(())).unwrap().unwrap());
            if replace_path {
                let replacement = temp.path().join("replacement");
                fs::write(&replacement, staged.bytes()).unwrap();
                fs::rename(replacement, store.chunk_path(&staged.chunk.id)).unwrap();
            } else {
                store
                    .catalog
                    .lock()
                    .unwrap()
                    .records
                    .get_mut(&staged.chunk.id)
                    .unwrap()
                    .catalog_revision += 1;
            }
            let result = loop {
                match stream.next_group(|| Ok(())) {
                    Ok(Some(bytes)) => drop(bytes),
                    other => break other,
                }
            };
            assert_eq!(
                result.unwrap_err().code(),
                afs_error::NODE_TRANSFER_CORRUPT_DATA
            );
            drop(stream);
            assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 0);
        }
    }

    #[cfg(feature = "dfs")]
    #[test]
    fn p2_r4_bad_sidecar_is_retired_without_quarantining_valid_body() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let staged = StagedChunk::new(
            OperationId::new("r4-stream-proof-damage"),
            vec![37; 3 * 32768],
        );
        store.put(staged.clone()).unwrap();
        drop(store.open_verified(&staged.chunk.id).unwrap());
        let path = store
            .outboards
            .join(format!("{}.bao", safe_id(&staged.chunk.id.0)));
        let mut damaged = fs::read(&path).unwrap();
        *damaged.last_mut().unwrap() ^= 1;
        fs::write(&path, damaged).unwrap();
        let mut stream = store
            .open_peer_group_stream(&staged.chunk.id, 0, staged.bytes().len())
            .unwrap()
            .unwrap();
        let error = loop {
            match stream.next_group(|| Ok(())) {
                Ok(Some(bytes)) => drop(bytes),
                Ok(None) => panic!("damaged proof must not complete"),
                Err(error) => break error,
            }
        };
        assert!(verified_copy_is_invalid(&error));
        assert!(
            !path.exists(),
            "only the observed bad optional proof is retired"
        );
        assert!(
            store.quarantined_chunks().unwrap().is_empty(),
            "valid body is not quarantined for proof metadata damage"
        );
        drop(stream);
        assert!(
            store
                .open_peer_group_stream(&staged.chunk.id, 0, staged.bytes().len())
                .unwrap()
                .is_none()
        );
        let recovered = store
            .read_peer_range(&staged.chunk.id, 0, staged.bytes().len())
            .unwrap();
        assert_eq!(recovered.as_slice(), staged.bytes());
    }

    #[cfg(feature = "dfs")]
    #[test]
    fn p2_r4_absent_proof_and_optional_capacity_fall_back_before_header() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let staged = StagedChunk::new(OperationId::new("r4-fallback"), vec![37; 3 * 32768]);
        store.put(staged.clone()).unwrap();
        drop(store.open_verified(&staged.chunk.id).unwrap());
        let lease = store
            .reserve_peer_read_payload(PEER_READ_PAYLOAD_LIMIT - 32768)
            .unwrap();
        assert!(
            store
                .open_peer_group_stream(&staged.chunk.id, 0, staged.bytes().len())
                .unwrap()
                .is_none()
        );
        assert_eq!(
            store.peer_read_budget.used.load(Ordering::Acquire),
            (PEER_READ_PAYLOAD_LIMIT - 32768) as u64
        );
        drop(lease);
        assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 0);
        fs::remove_file(
            store
                .outboards
                .join(format!("{}.bao", safe_id(&staged.chunk.id.0))),
        )
        .unwrap();
        assert!(
            store
                .open_peer_group_stream(&staged.chunk.id, 0, staged.bytes().len())
                .unwrap()
                .is_none()
        );
        assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 0);
        assert_eq!(
            store
                .read_peer_range(&staged.chunk.id, 0, staged.bytes().len())
                .unwrap()
                .as_slice(),
            staged.bytes()
        );
    }

    #[test]
    fn peer_object_digest_capability_requires_the_complete_verified_view() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let staged = StagedChunk::new(OperationId::new("whole-proof"), vec![37; 150_001]);
        store.put(staged.clone()).unwrap();
        VERIFIED_SCAN_BYTES.with(|count| count.set(0));
        let whole = store
            .read_peer_range(&staged.chunk.id, 0, staged.bytes().len())
            .unwrap();
        assert_eq!(
            whole.verified_full_object_digest(),
            Some(staged.chunk.content_digest.clone())
        );
        assert!(
            store
                .read_peer_range(&staged.chunk.id, 0, staged.bytes().len() - 1)
                .unwrap()
                .verified_full_object_digest()
                .is_none()
        );
        assert!(
            store
                .read_peer_range(&staged.chunk.id, 1, staged.bytes().len() - 1)
                .unwrap()
                .verified_full_object_digest()
                .is_none()
        );
        assert_eq!(
            VERIFIED_SCAN_BYTES.with(|count| count.get()),
            staged.chunk.length
        );
    }

    #[test]
    fn peer_object_digest_capability_is_absent_for_full_range_fallback() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let staged = StagedChunk::new(
            OperationId::new("fallback-proof"),
            vec![19; PEER_READ_OBJECT_LIMIT + 1],
        );
        store.put(staged.clone()).unwrap();
        let whole = store
            .read_peer_range(&staged.chunk.id, 0, staged.bytes().len())
            .unwrap();
        assert_eq!(whole.as_slice(), staged.bytes());
        assert!(whole.object.identity.is_none());
        assert!(whole.verified_full_object_digest().is_none());
        assert!(
            store
                .peer_read_cache
                .lock()
                .unwrap()
                .residents
                .iter()
                .all(Option::is_none)
        );
        drop(whole);
        assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 0);
    }

    #[test]
    fn peer_object_digest_does_not_release_shared_frame_budget_early() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let staged = StagedChunk::new(OperationId::new("proof-lease"), vec![29; 150_001]);
        store.put(staged.clone()).unwrap();
        let whole = store
            .read_peer_range(&staged.chunk.id, 0, staged.bytes().len())
            .unwrap();
        let digest = whole.verified_full_object_digest().unwrap();
        let bytes = whole.into_shared_bytes();
        let frame = bytes.slice(17..100);
        store.clear_peer_residents_for_test();
        drop(bytes);
        assert_eq!(
            store.peer_read_budget.used.load(Ordering::Acquire),
            staged.chunk.length
        );
        assert_eq!(digest, staged.chunk.content_digest);
        assert_eq!(&frame[..], &[29; 83]);
        drop(frame);
        assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 0);
    }

    #[test]
    fn wi6_pinned_small_reads_do_not_repeat_complete_object_scans() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let staged = StagedChunk::new(
            OperationId::new("outboard-old-copy"),
            vec![19; 4 * 1024 * 1024],
        );
        store.put(staged.clone()).unwrap();
        let reader = store.open_verified(&staged.chunk.id).unwrap();
        VERIFIED_SCAN_BYTES.with(|count| count.set(0));
        for offset in [3, 32771] {
            let mut bytes = [0; 17];
            assert_eq!(reader.read_at(offset, &mut bytes).unwrap(), 17);
            assert_eq!(bytes, [19; 17]);
        }
        let scanned = VERIFIED_SCAN_BYTES.with(|count| count.get());
        assert!(
            scanned <= 2 * 32768,
            "two small ranges scanned {scanned} bytes"
        );
    }

    #[test]
    fn wi6_legacy_and_half_sidecars_recover_without_rewriting_body_or_catalog() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let staged = StagedChunk::new(OperationId::new("outboard-recovery"), vec![23; 150_001]);
        store.put(staged.clone()).unwrap();
        let original_revision = store.device_descriptor().unwrap().catalog_revision;
        let body_path = store.chunk_path(&staged.chunk.id);
        let identity = PeerFileIdentity::read(&File::open(&body_path).unwrap()).unwrap();
        let proof_path = store
            .outboards
            .join(format!("{}.bao", safe_id(&staged.chunk.id.0)));
        assert!(
            !proof_path.exists(),
            "unpooled writes retain old root-only behavior"
        );
        let mut permissions = fs::metadata(&body_path).unwrap().permissions();
        permissions.set_readonly(true);
        fs::set_permissions(&body_path, permissions).unwrap();
        // chmod changes ctime, record the identity after restricting the body.
        let identity = PeerFileIdentity::read(&File::open(&body_path).unwrap()).unwrap_or(identity);
        fs::write(&proof_path, b"partial").unwrap();
        fs::write(store.outboards.join(".abandoned.tmp"), b"partial").unwrap();
        drop(store);
        let recovered = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        assert!(!recovered.outboards.join(".abandoned.tmp").exists());
        let reader = recovered.open_verified(&staged.chunk.id).unwrap();
        assert_eq!(
            PeerFileIdentity::read(&File::open(&body_path).unwrap()).unwrap(),
            identity
        );
        assert_eq!(
            recovered.device_descriptor().unwrap().catalog_revision,
            original_revision
        );
        let encoded = fs::read(&proof_path).unwrap();
        ChunkOutboard::validate_encoded_shape(&staged.chunk, &encoded).unwrap();
        let mut out = [0; 19];
        assert_eq!(reader.read_at(3, &mut out).unwrap(), 19);
        assert_eq!(out, [23; 19]);
        assert!(fs::read_dir(&recovered.outboards).unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .ends_with(".tmp")
        }));
        let empty_budget = OutboardBudget::new(0);
        let constrained = LocalChunkStore::open(temp.path(), "node-a")
            .unwrap()
            .with_outboard_budget(empty_budget.clone());
        let mut out = [0; 19];
        assert_eq!(
            constrained.read_at(&staged.chunk.id, 3, &mut out).unwrap(),
            19
        );
        assert_eq!(out, [23; 19]);
        assert_eq!(empty_budget.used(), 0);
    }

    #[test]
    fn wi6_canonical_catalog_mismatch_cannot_replace_the_trusted_root() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let staged = StagedChunk::new(OperationId::new("canonical-anchor"), vec![3; 150_001]);
        store.put(staged.clone()).unwrap();
        let forged = StagedChunk::new(OperationId::new("forged-catalog"), vec![7; 150_001]);
        fs::write(store.chunk_path(&staged.chunk.id), forged.bytes()).unwrap();
        {
            let mut catalog = store.catalog.lock().unwrap();
            let record = catalog.records.get_mut(&staged.chunk.id).unwrap();
            record.chunk.content_digest = forged.chunk.content_digest.clone();
            record.stored_checksum = forged.chunk.content_digest.clone();
        }
        for error in [
            store.open_verified(&staged.chunk.id).unwrap_err(),
            store.read_peer_range(&staged.chunk.id, 3, 17).unwrap_err(),
        ] {
            assert_eq!(error.code(), afs_error::NODE_TRANSFER_CORRUPT_DATA);
        }
    }

    #[test]
    fn wi6_range_work_budget_pressure_falls_back_to_complete_root() {
        let temp = tempfile::tempdir().unwrap();
        let budget = OutboardBudget::new(16 * 1024);
        let store = LocalChunkStore::open(temp.path(), "node-a")
            .unwrap()
            .with_outboard_budget(budget.clone());
        let staged = StagedChunk::new(OperationId::new("work-budget-fallback"), vec![5; 150_001]);
        store.put(staged.clone()).unwrap();
        let reader = store.open_verified(&staged.chunk.id).unwrap();
        assert!(reader.load_outboard().unwrap().is_some());
        let before = budget.used();
        VERIFIED_SCAN_BYTES.with(|count| count.set(0));
        let mut out = [0; 17];
        assert_eq!(reader.read_at(3, &mut out).unwrap(), 17);
        assert_eq!(out, [5; 17]);
        assert_eq!(VERIFIED_SCAN_BYTES.with(|count| count.get()), 150_001);
        assert_eq!(budget.used(), before);
    }

    #[test]
    fn wi6_pooled_stage_keeps_body_address_and_persists_optional_same_budget_proof() {
        let temp = tempfile::tempdir().unwrap();
        let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
        let pool = WriteBufferPool::new_with_outboard_budget(32768, budget.clone());
        let mut builder = ChunkBuilder::with_pool(pool.clone(), 32768).unwrap();
        builder.write_at(0, &vec![7; 32768]).unwrap();
        let frozen = builder.freeze();
        let pointer = frozen.as_ptr();
        let staged = StagedChunk::from_shared(OperationId::new("outboard-pooled"), frozen);
        assert_eq!(staged.bytes().as_ptr(), pointer);
        assert!(staged.outboard.as_ref().unwrap().uses_budget(&budget));
        assert_eq!(pool.snapshot().copied_bytes, 32768);
        let store = LocalChunkStore::open(temp.path(), "node-a")
            .unwrap()
            .with_outboard_budget(budget.clone());
        store.put(staged.clone()).unwrap();
        // This assertion is specifically about a completed optional install,
        // not the earlier durable body/catalog ACK.
        store.sidecar_installer.wait_idle();
        let proof_path = store
            .outboards
            .join(format!("{}.bao", safe_id(&staged.chunk.id.0)));
        ChunkOutboard::validate_encoded_shape(&staged.chunk, &fs::read(proof_path).unwrap())
            .unwrap();
        let plain_pool = WriteBufferPool::new(32768);
        let mut plain = ChunkBuilder::with_pool(plain_pool, 32768).unwrap();
        plain.write_at(0, b"plain").unwrap();
        assert!(
            plain
                .stage(OperationId::new("root-only"))
                .outboard
                .is_none()
        );
    }

    #[test]
    fn wi6_peer_partial_uses_covering_groups_and_exact_payload_lease() {
        let temp = tempfile::tempdir().unwrap();
        let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
        let pool = WriteBufferPool::new_with_outboard_budget(4 * 1024 * 1024, budget.clone());
        let mut builder = ChunkBuilder::with_pool(pool.clone(), 4 * 1024 * 1024).unwrap();
        builder.write_at(0, &vec![11; 4 * 1024 * 1024]).unwrap();
        let staged = builder.stage(OperationId::new("outboard-partial"));
        let store = LocalChunkStore::open(temp.path(), "node-a")
            .unwrap()
            .with_outboard_budget(budget);
        store.put(staged.clone()).unwrap();
        // Measure the proof-ready range path. An ACK-before-sidecar read may
        // legitimately verify the full object, covered separately above.
        store.sidecar_installer.wait_idle();
        VERIFIED_SCAN_BYTES.with(|count| count.set(0));
        let payload = store.read_peer_range(&staged.chunk.id, 3, 17).unwrap();
        assert_eq!(payload.as_slice(), &[11; 17]);
        assert!(payload.verified_full_object_digest().is_none());
        assert!(payload.outboard().is_none());
        assert_eq!(VERIFIED_SCAN_BYTES.with(|count| count.get()), 32768);
        assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 17);
        drop(payload);
        assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 0);
    }

    #[test]
    fn peer_object_reuses_one_complete_scan_for_multiple_ranges() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let bytes = vec![37; 4 * 1024 * 1024];
        let staged = StagedChunk::new(OperationId::new("peer-reuse"), bytes.clone());
        store.put(staged.clone()).unwrap();
        VERIFIED_SCAN_BYTES.with(|count| count.set(0));
        for index in 0..4 {
            let payload = store
                .read_peer_range(&staged.chunk.id, index * 1024 * 1024, 1024 * 1024)
                .unwrap();
            assert_eq!(payload.as_slice(), &bytes[..1024 * 1024]);
        }
        assert_eq!(
            VERIFIED_SCAN_BYTES.with(|count| count.get()),
            bytes.len() as u64
        );
        assert_eq!(
            store.peer_read_budget.used.load(Ordering::Acquire),
            bytes.len() as u64
        );
    }

    #[test]
    fn peer_object_revalidates_replacement_missing_and_quarantined_files() {
        for change in [
            "replace",
            "missing",
            "quarantine",
            "corrupt",
            "epoch",
            "revision",
        ] {
            let temp = tempfile::tempdir().unwrap();
            let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
            let bytes = vec![53; 150_001];
            let staged = StagedChunk::new(OperationId::new("peer-identity"), bytes.clone());
            store.put(staged.clone()).unwrap();
            drop(store.read_peer_range(&staged.chunk.id, 3, 19).unwrap());
            VERIFIED_SCAN_BYTES.with(|count| count.set(0));
            match change {
                "replace" => {
                    let replacement = temp.path().join("replacement");
                    fs::write(&replacement, &bytes).unwrap();
                    fs::rename(replacement, store.chunk_path(&staged.chunk.id)).unwrap();
                }
                "missing" => fs::remove_file(store.chunk_path(&staged.chunk.id)).unwrap(),
                "corrupt" => {
                    let path = store.chunk_path(&staged.chunk.id);
                    let modified = fs::metadata(&path).unwrap().modified().unwrap();
                    fs::write(&path, vec![1; bytes.len()]).unwrap();
                    // Same-size writes can share one filesystem timestamp tick.
                    // Exercise revalidation with a deterministically new identity.
                    File::options()
                        .write(true)
                        .open(path)
                        .unwrap()
                        .set_times(
                            fs::FileTimes::new().set_modified(modified + Duration::from_secs(1)),
                        )
                        .unwrap();
                }
                _ => {
                    let mut catalog = store.catalog.lock().unwrap();
                    let record = catalog.records.get_mut(&staged.chunk.id).unwrap();
                    match change {
                        "quarantine" => record.state = LocalChunkState::Quarantined,
                        "epoch" => record.device_epoch += 1,
                        "revision" => record.catalog_revision += 1,
                        _ => unreachable!(),
                    }
                }
            }
            let result = store.read_peer_range(&staged.chunk.id, 3, 19);
            if change == "replace" || change == "revision" {
                assert_eq!(result.unwrap().as_slice(), &bytes[3..22]);
                assert_eq!(
                    VERIFIED_SCAN_BYTES.with(|count| count.get()),
                    bytes.len() as u64
                );
            } else {
                assert!(result.is_err(), "{change} must not reuse a stale proof");
                assert!(
                    store
                        .peer_read_cache
                        .lock()
                        .unwrap()
                        .residents
                        .iter()
                        .all(Option::is_none)
                );
            }
        }
    }

    #[test]
    fn peer_object_failed_fill_releases_budget_and_never_publishes() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let staged = StagedChunk::new(OperationId::new("peer-failed"), vec![5; 4096]);
        store.put(staged.clone()).unwrap();
        fs::write(store.chunk_path(&staged.chunk.id), vec![9; 4096]).unwrap();
        assert!(store.read_peer_range(&staged.chunk.id, 0, 3).is_err());
        assert!(
            store
                .peer_read_cache
                .lock()
                .unwrap()
                .residents
                .iter()
                .all(Option::is_none)
        );
        assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 0);
    }

    #[test]
    fn peer_object_evicted_references_and_packed_payload_share_the_limit() {
        let temp = tempfile::tempdir().unwrap();
        let payload_limit = 8 * 1024 * 1024;
        let store = LocalChunkStore::open(temp.path(), "node-a")
            .unwrap()
            .with_peer_serve_config(1, payload_limit as u64)
            .unwrap();
        let chunks: Vec<_> = (1..=3)
            .map(|byte| {
                let chunk = StagedChunk::new(
                    OperationId::new(format!("peer-budget-{byte}")),
                    vec![byte; 4 * 1024 * 1024],
                );
                store.put(chunk.clone()).unwrap();
                chunk
            })
            .collect();
        let first = store.read_peer_range(&chunks[0].chunk.id, 0, 1).unwrap();
        let second = store.read_peer_range(&chunks[1].chunk.id, 0, 1).unwrap();
        assert_eq!(
            store.peer_read_budget.used.load(Ordering::Acquire),
            payload_limit as u64
        );
        assert_eq!(
            store
                .read_peer_range(&chunks[2].chunk.id, 0, 1)
                .unwrap_err()
                .code(),
            afs_error::IO_CAPACITY
        );
        assert_eq!(
            store.reserve_peer_read_payload(1).unwrap_err().code(),
            afs_error::IO_CAPACITY
        );
        assert_eq!(first.as_slice(), &[1]);
        drop(first);
        drop(second);
        let mut packed = store.reserve_peer_read_payload(4 * 1024 * 1024).unwrap();
        packed.retain_capacity(4 * 1024 * 1024).unwrap();
        assert!(store.read_peer_range(&chunks[2].chunk.id, 0, 1).is_ok());
        assert_eq!(
            store.peer_read_budget.used.load(Ordering::Acquire),
            payload_limit as u64
        );
        drop(packed);
        assert_eq!(
            store.peer_read_budget.used.load(Ordering::Acquire),
            4 * 1024 * 1024
        );
        assert_eq!(
            store
                .reserve_peer_read_payload(payload_limit + 1)
                .unwrap_err()
                .code(),
            afs_error::IO_CAPACITY
        );
    }

    #[test]
    fn peer_object_budget_reservation_drop_and_capacity_growth_are_charged() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let mut lease = store.reserve_peer_read_payload(19).unwrap();
        lease.retain_capacity(31).unwrap();
        assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 31);
        assert!(lease.retain_capacity(PEER_READ_PAYLOAD_LIMIT + 1).is_err());
        assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 31);
        drop(lease);
        assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 0);
    }

    #[test]
    fn p2_r6_default_lru_and_control_owners_release_only_after_last_frame() {
        let temp = tempfile::tempdir().unwrap();
        let registry = afs_metrics::Registry::new();
        let store = LocalChunkStore::open(temp.path(), "node-a")
            .unwrap()
            .with_read_work_metrics(
                super::super::read_work::ReadWorkMetrics::register(&registry).unwrap(),
            );
        println!(
            "R6_CONTROL fixed={} object={} flight={} cap={}",
            peer_serve_fixed_control_bytes(),
            std::mem::size_of::<PeerVerifiedObject>(),
            std::mem::size_of::<PeerServeFlight>(),
            PEER_SERVE_CONTROL_LIMIT
        );
        assert_eq!(store.peer_read_budget.limit, 16 * 1024 * 1024);
        assert_eq!(store.peer_read_cache.lock().unwrap().resident_limit, 2);
        let chunks: Vec<_> = (1..=3)
            .map(|byte| {
                StagedChunk::new(
                    OperationId::new(format!("r6-lru-{byte}")),
                    vec![byte; 64 * 1024],
                )
            })
            .collect();
        store.put_batch(chunks.clone()).unwrap();
        drop(
            store
                .read_peer_range(&chunks[0].chunk.id, 0, 64 * 1024)
                .unwrap(),
        );
        drop(
            store
                .read_peer_range(&chunks[1].chunk.id, 0, 64 * 1024)
                .unwrap(),
        );
        // Refresh first, then third must evict second.
        drop(
            store
                .read_peer_range(&chunks[0].chunk.id, 0, 64 * 1024)
                .unwrap(),
        );
        let third = store
            .read_peer_range(&chunks[2].chunk.id, 0, 64 * 1024)
            .unwrap();
        {
            let state = store.peer_read_cache.lock().unwrap();
            assert!(state.resident(&chunks[0].chunk.id).is_some());
            assert!(state.resident(&chunks[1].chunk.id).is_none());
            assert!(state.resident(&chunks[2].chunk.id).is_some());
            assert_eq!(state.residents.iter().flatten().count(), 2);
        }
        let bytes = third.into_shared_bytes();
        let frame = bytes.slice(17..100);
        drop(bytes);
        store.clear_peer_residents_for_test();
        assert_eq!(
            store.peer_read_budget.used.load(Ordering::Acquire),
            64 * 1024
        );
        assert!(store.peer_read_budget.control_used.load(Ordering::Acquire) > 0);
        assert!(
            store.peer_read_budget.control_peak.load(Ordering::Acquire) as usize
                + peer_serve_fixed_control_bytes()
                <= PEER_SERVE_CONTROL_LIMIT
        );
        store.close_peer_serve_admission();
        assert!(store.read_peer_range(&chunks[0].chunk.id, 0, 1).is_err());
        assert_eq!(&frame[..], &[3; 83]);
        assert_eq!(
            store.peer_read_budget.used.load(Ordering::Acquire),
            64 * 1024
        );
        drop(frame);
        assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 0);
        assert_eq!(
            store.peer_read_budget.control_used.load(Ordering::Acquire),
            0
        );
        store.shutdown_peer_serve().unwrap();
        let text = afs_metrics::encode_text(&registry).unwrap();
        assert!(text.contains("afs_dfs_peer_serve_resources{resource=\"control_bytes\"} 0\n"));
    }

    #[test]
    fn p2_r6_eight_keys_are_bounded_and_stop_does_not_release_active_flights() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let chunks: Vec<_> = (0..9)
            .map(|index| {
                StagedChunk::new(
                    OperationId::new(format!("r6-keys-{index}")),
                    vec![index; 64 * 1024],
                )
            })
            .collect();
        store.put_batch(chunks.clone()).unwrap();
        let mut blocks = Vec::new();
        for chunk in &chunks[..8] {
            blocks.push(store.pause_next_peer_fill_for_test(chunk.chunk.id.clone()));
        }
        std::thread::scope(|scope| {
            let mut readers = Vec::new();
            for chunk in &chunks[..8] {
                readers.push(scope.spawn(|| store.read_peer_range(&chunk.chunk.id, 0, 64 * 1024)));
            }
            for (entered, _) in &blocks {
                entered.recv_timeout(Duration::from_secs(3)).unwrap();
            }
            {
                let state = store.peer_read_cache.lock().unwrap();
                assert_eq!(state.flights.iter().flatten().count(), 8);
                assert_eq!(state.readers, 8);
            }
            assert_eq!(
                store
                    .read_peer_range(&chunks[8].chunk.id, 0, 64 * 1024)
                    .unwrap_err()
                    .code(),
                afs_error::IO_CAPACITY
            );
            let charged = store.peer_read_budget.control_used.load(Ordering::Acquire);
            store.close_peer_serve_admission();
            let deadline = store.peer_read_cache.lock().unwrap().stop_deadline;
            store.close_peer_serve_admission();
            assert_eq!(
                store.peer_read_cache.lock().unwrap().stop_deadline,
                deadline
            );
            assert_eq!(
                store.peer_read_budget.control_used.load(Ordering::Acquire),
                charged
            );
            for (_, release) in &blocks {
                release.send(()).unwrap();
            }
            for reader in readers {
                assert!(reader.join().unwrap().is_err());
            }
        });
        assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 0);
        assert_eq!(
            store.peer_read_budget.control_used.load(Ordering::Acquire),
            0
        );
        store.shutdown_peer_serve().unwrap();
        let state = store.peer_read_cache.lock().unwrap();
        assert_eq!(state.readers, 0);
        assert!(state.flights.iter().all(Option::is_none));
    }

    #[test]
    fn p2_r6_live_owner_blocks_reconfiguration_and_control_pressure_is_bounded() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let chunk = StagedChunk::new(OperationId::new("r6-reconfigure"), vec![91; 64 * 1024]);
        store.put(chunk.clone()).unwrap();
        let payload = store
            .read_peer_range(&chunk.chunk.id, 0, 64 * 1024)
            .unwrap();
        let bytes = payload.into_shared_bytes();
        store.clear_peer_residents_for_test();
        assert!(store.with_peer_serve_config(1, 4 * 1024 * 1024).is_err());
        assert_eq!(bytes[17], 91);
        drop(bytes);

        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let control = store
            .reserve_peer_control(PEER_SERVE_CONTROL_LIMIT - peer_serve_fixed_control_bytes())
            .unwrap();
        assert_eq!(
            store
                .read_peer_range(&chunk.chunk.id, 0, 64 * 1024)
                .unwrap_err()
                .code(),
            afs_error::IO_CAPACITY
        );
        assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 0);
        drop(control);
        let payload = store
            .read_peer_range(&chunk.chunk.id, 0, 64 * 1024)
            .unwrap();
        assert_eq!(payload.as_slice(), chunk.bytes());
        drop(payload);
        store.shutdown_peer_serve().unwrap();
    }

    #[test]
    fn p2_r6_bad_fill_shares_one_failure_and_never_installs_bytes() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let chunk = StagedChunk::new(OperationId::new("r6-bad-flight"), vec![13; 64 * 1024]);
        store.put(chunk.clone()).unwrap();
        let (entered, release) = store.pause_next_peer_fill_for_test(chunk.chunk.id.clone());
        std::thread::scope(|scope| {
            let leader = scope.spawn(|| store.read_peer_range(&chunk.chunk.id, 0, 64 * 1024));
            entered.recv_timeout(Duration::from_secs(3)).unwrap();
            let followers: Vec<_> = (0..7)
                .map(|_| scope.spawn(|| store.read_peer_range(&chunk.chunk.id, 0, 64 * 1024)))
                .collect();
            let deadline = Instant::now() + Duration::from_secs(3);
            while store.waiting_peer_fill_readers_for_test() < 7 {
                assert!(
                    Instant::now() < deadline,
                    "followers did not join the blocked fill"
                );
                std::thread::yield_now();
            }
            fs::write(store.chunk_path(&chunk.chunk.id), vec![17; 64 * 1024]).unwrap();
            release.send(()).unwrap();
            let error = leader.join().unwrap().unwrap_err();
            for follower in followers {
                let failure = follower.join().unwrap().unwrap_err();
                assert_eq!(
                    (failure.code(), failure.kind()),
                    (error.code(), error.kind())
                );
            }
        });
        assert!(
            store
                .peer_read_cache
                .lock()
                .unwrap()
                .residents
                .iter()
                .all(Option::is_none)
        );
        assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 0);
        assert_eq!(
            store.peer_read_budget.control_used.load(Ordering::Acquire),
            0
        );
        store.shutdown_peer_serve().unwrap();
    }

    #[test]
    fn p2_r6_zero_byte_lease_is_a_real_owner_until_its_last_drop() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let mut lease = store.reserve_peer_read_payload(0).unwrap();
        assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 0);
        assert_eq!(Arc::strong_count(&store.peer_read_budget), 2);
        store.close_peer_serve_admission();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        std::thread::scope(|scope| {
            let stopping = scope.spawn(|| {
                started_tx.send(()).unwrap();
                let result = store.shutdown_peer_serve();
                done_tx.send(()).unwrap();
                result
            });
            started_rx.recv_timeout(Duration::from_secs(3)).unwrap();
            assert!(done_rx.recv_timeout(Duration::from_millis(100)).is_err());
            // A pre-existing owner can retain capacity; shutdown must still see
            // that real ownership even when its initial byte charge was zero.
            lease.retain_capacity(31).unwrap();
            assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 31);
            drop(lease);
            stopping.join().unwrap().unwrap();
        });
        assert_eq!(Arc::strong_count(&store.peer_read_budget), 1);
        assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 0);
    }

    #[test]
    fn p2_t1_stop_rejects_new_owners_but_keeps_existing_capacity_charged() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let mut lease = store.reserve_peer_read_payload(0).unwrap();
        store.close_peer_serve_admission();
        for bytes in [0, 1] {
            assert_eq!(
                store.reserve_peer_read_payload(bytes).unwrap_err().code(),
                afs_error::IO_UNAVAILABLE
            );
            assert_eq!(
                store
                    .reserve_peer_read_payload_until(bytes, store.peer_serve_deadline())
                    .unwrap_err()
                    .code(),
                afs_error::IO_UNAVAILABLE
            );
        }
        lease.retain_capacity(31).unwrap();
        assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 31);
        assert_eq!(store.peer_read_budget.peak.load(Ordering::Acquire), 31);
        assert_eq!(
            lease
                .retain_capacity(PEER_READ_PAYLOAD_LIMIT + 1)
                .unwrap_err()
                .code(),
            afs_error::IO_CAPACITY
        );
        assert_eq!(lease.bytes, 31);
        assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 31);
        assert_eq!(Arc::strong_count(&store.peer_read_budget), 2);
        drop(lease);
        assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 0);
        assert_eq!(Arc::strong_count(&store.peer_read_budget), 1);
        store.shutdown_peer_serve().unwrap();
    }

    #[test]
    fn p2_r6_packed_admission_cannot_reopen_budget_after_shutdown() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let (drained_tx, drained_rx) = std::sync::mpsc::channel();
        *store.peer_payload_test_block.lock().unwrap() = Some((entered_tx, release_rx));
        let drained = std::sync::atomic::AtomicBool::new(false);
        let (premature, admitted_after_drain) = std::thread::scope(|scope| {
            let allocating = scope.spawn(|| {
                let lease = store.reserve_peer_read_payload(64 * 1024);
                let late = lease.is_ok() && drained.load(Ordering::Acquire);
                drop(lease);
                late
            });
            entered_rx.recv_timeout(Duration::from_secs(3)).unwrap();
            store.close_peer_serve_admission();
            let stopping = scope.spawn(|| {
                let result = store.shutdown_peer_serve();
                drained.store(true, Ordering::Release);
                drained_tx.send(()).unwrap();
                result
            });
            // Both accepted I/O and rejected old admission are legal. Returning
            // successful shutdown while admission is still paused is not.
            let premature = drained_rx.recv_timeout(Duration::from_secs(2)).is_ok();
            release_tx.send(()).unwrap();
            let late = allocating.join().unwrap();
            stopping.join().unwrap().unwrap();
            (premature, late)
        });
        assert!(
            !premature && !admitted_after_drain,
            "packed admission acquired a new payload after shutdown had declared the budget drained"
        );
        assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 0);
    }

    #[test]
    fn p2_r6_old_ready_follower_cannot_retire_a_new_healthy_resident() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let chunk = StagedChunk::new(OperationId::new("r6-old-ready"), vec![73; 64 * 1024]);
        store.put(chunk.clone()).unwrap();
        let (fill_entered, fill_release) =
            store.pause_next_peer_fill_for_test(chunk.chunk.id.clone());
        let (ready_entered, ready_observed) = std::sync::mpsc::channel();
        let (ready_release, ready_wait) = std::sync::mpsc::channel();
        *store.peer_ready_test_block.lock().unwrap() = Some((ready_entered, ready_wait));
        std::thread::scope(|scope| {
            let first = scope.spawn(|| store.read_peer_range(&chunk.chunk.id, 0, 64 * 1024));
            fill_entered.recv_timeout(Duration::from_secs(3)).unwrap();
            let old_follower = scope.spawn(|| store.read_peer_range(&chunk.chunk.id, 0, 64 * 1024));
            let deadline = Instant::now() + Duration::from_secs(3);
            while store.waiting_peer_fill_readers_for_test() == 0 {
                assert!(
                    Instant::now() < deadline,
                    "old follower did not join first fill"
                );
                std::thread::yield_now();
            }
            fill_release.send(()).unwrap();
            ready_observed.recv_timeout(Duration::from_secs(3)).unwrap();
            let old = first.join().unwrap().unwrap();
            let path = store.chunk_path(&chunk.chunk.id);
            fs::rename(&path, temp.path().join("old-healthy-inode")).unwrap();
            fs::write(&path, chunk.bytes()).unwrap();
            let replacement = store
                .read_peer_range(&chunk.chunk.id, 0, 64 * 1024)
                .unwrap();
            assert!(!Arc::ptr_eq(&old.object, &replacement.object));
            assert!(Arc::ptr_eq(
                &store
                    .peer_read_cache
                    .lock()
                    .unwrap()
                    .resident(&chunk.chunk.id)
                    .unwrap(),
                &replacement.object
            ));
            ready_release.send(()).unwrap();
            assert_eq!(
                old_follower.join().unwrap().unwrap().as_slice(),
                chunk.bytes()
            );
            let indexed = store
                .peer_read_cache
                .lock()
                .unwrap()
                .resident(&chunk.chunk.id);
            assert!(
                indexed
                    .as_ref()
                    .is_some_and(|object| Arc::ptr_eq(object, &replacement.object)),
                "old Ready follower retired the newer healthy resident"
            );
            assert!(store.quarantined_chunks().unwrap().is_empty());
        });
        store.shutdown_peer_serve().unwrap();
    }

    #[test]
    fn p2_r6_different_objects_fill_outside_shared_table_lock() {
        use std::sync::mpsc;
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let first = StagedChunk::new(OperationId::new("r6-fill-first"), vec![31; 64 * 1024]);
        let second = StagedChunk::new(OperationId::new("r6-fill-second"), vec![47; 64 * 1024]);
        store
            .put_batch(vec![first.clone(), second.clone()])
            .unwrap();
        let (first_entered_tx, first_entered_rx) = mpsc::channel();
        let (first_release_tx, first_release_rx) = mpsc::channel();
        let (second_entered_tx, second_entered_rx) = mpsc::channel();
        let (second_release_tx, second_release_rx) = mpsc::channel();
        {
            let mut blocks = store.peer_fill_test_blocks.lock().unwrap();
            blocks.insert(first.chunk.id.clone(), (first_entered_tx, first_release_rx));
            blocks.insert(
                second.chunk.id.clone(),
                (second_entered_tx, second_release_rx),
            );
        }
        let concurrent = std::thread::scope(|scope| {
            let first_read =
                scope.spawn(|| store.read_peer_range(&first.chunk.id, 0, first.bytes().len()));
            first_entered_rx
                .recv_timeout(Duration::from_secs(2))
                .unwrap();
            let second_read =
                scope.spawn(|| store.read_peer_range(&second.chunk.id, 0, second.bytes().len()));
            // A bounded progress assertion, not a performance threshold. Always
            // release both workers and join before reporting the old slot bug.
            let concurrent = second_entered_rx
                .recv_timeout(Duration::from_secs(2))
                .is_ok();
            first_release_tx.send(()).unwrap();
            second_release_tx.send(()).unwrap();
            assert_eq!(
                first_read.join().unwrap().unwrap().as_slice(),
                first.bytes()
            );
            assert_eq!(
                second_read.join().unwrap().unwrap().as_slice(),
                second.bytes()
            );
            concurrent
        });
        assert!(
            concurrent,
            "an unrelated Peer fill remained behind the global object slot"
        );
    }

    #[test]
    fn peer_object_concurrent_ranges_share_the_same_verified_payload() {
        let temp = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
        let staged = StagedChunk::new(
            OperationId::new("peer-singleflight"),
            vec![73; 4 * 1024 * 1024],
        );
        store.put(staged.clone()).unwrap();
        let barrier = Arc::new(std::sync::Barrier::new(8));
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let store = store.clone();
                let id = staged.chunk.id.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    VERIFIED_SCAN_BYTES.with(|count| count.set(0));
                    let payload = store.read_peer_range(&id, 0, 4 * 1024 * 1024).unwrap();
                    (payload, VERIFIED_SCAN_BYTES.with(|count| count.get()))
                })
            })
            .collect();
        let payloads: Vec<_> = threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect();
        assert_eq!(
            payloads.iter().map(|(_, scanned)| *scanned).sum::<u64>(),
            staged.chunk.length
        );
        for (payload, _) in &payloads {
            assert!(Arc::ptr_eq(&payload.object, &payloads[0].0.object));
            assert_eq!(payload.as_slice(), staged.bytes());
        }
        assert_eq!(
            store.peer_read_budget.used.load(Ordering::Acquire),
            4 * 1024 * 1024
        );
    }

    #[test]
    fn peer_object_shared_frame_slices_retain_allocation_and_lease() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let staged = StagedChunk::new(
            OperationId::new("shared-frame-owner"),
            vec![29; 4 * 1024 * 1024],
        );
        store.put(staged.clone()).unwrap();
        let payload = store.read_peer_range(&staged.chunk.id, 17, 128).unwrap();
        let pointer = payload.as_slice().as_ptr();
        let bytes = payload.into_shared_bytes();
        assert_eq!(bytes.as_ptr(), pointer);
        let frame = bytes.slice(3..67);
        assert_eq!(frame.as_ptr(), bytes[3..].as_ptr());
        let retained = frame.clone();
        drop(frame);
        drop(bytes);
        store.clear_peer_residents_for_test();
        assert_eq!(&retained[..], &[29; 64]);
        assert_eq!(
            store.peer_read_budget.used.load(Ordering::Acquire),
            4 * 1024 * 1024
        );
        drop(retained);
        assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 0);
    }

    #[test]
    fn peer_object_oversized_chunk_uses_charged_range_fallback_without_residency() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let staged = StagedChunk::new(
            OperationId::new("peer-fallback"),
            vec![91; PEER_READ_OBJECT_LIMIT + 1],
        );
        store.put(staged.clone()).unwrap();
        let range = store
            .read_peer_range(&staged.chunk.id, PEER_READ_OBJECT_LIMIT as u64, 19)
            .unwrap();
        assert_eq!(range.as_slice(), &[91]);
        assert!(
            store
                .peer_read_cache
                .lock()
                .unwrap()
                .residents
                .iter()
                .all(Option::is_none)
        );
        assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 1);
        let pointer = range.as_slice().as_ptr();
        let bytes = range.into_shared_bytes();
        assert_eq!(bytes.as_ptr(), pointer);
        let retained = bytes.slice(..);
        drop(bytes);
        assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 1);
        assert_eq!(&retained[..], &[91]);
        drop(retained);
        assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 0);
    }

    #[test]
    fn private_range_reads_scan_once_and_copy_only_the_requested_overlap() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let bytes: Vec<_> = (0..150_001).map(|index| (index % 251) as u8).collect();
        let staged = StagedChunk::new(OperationId::new("single-scan"), bytes.clone());
        store.put(staged.clone()).unwrap();
        let mut out = [9; 19];
        VERIFIED_SCAN_BYTES.with(|bytes| bytes.set(0));
        assert_eq!(
            store.read_at(&staged.chunk.id, 65_531, &mut out).unwrap(),
            19
        );
        assert_eq!(
            VERIFIED_SCAN_BYTES.with(|bytes| bytes.get()),
            bytes.len() as u64 + 2 * outboard::OUTBOARD_GROUP_BYTES as u64
        );
        for offset in [65_531, 149_990] {
            out.fill(9);
            VERIFIED_SCAN_BYTES.with(|bytes| bytes.set(0));
            let count = store
                .read_range_at_uncommitted(&staged.chunk.id, offset, &mut out)
                .unwrap();
            let expected = 19.min(bytes.len() - offset as usize);
            assert_eq!(count, expected);
            assert_eq!(
                &out[..count],
                &bytes[offset as usize..offset as usize + count]
            );
            assert!(out[count..].iter().all(|byte| *byte == 9));
            assert_eq!(
                VERIFIED_SCAN_BYTES.with(|bytes| bytes.get()),
                bytes.len() as u64
            );
        }
    }

    #[test]
    fn private_range_pins_identity_before_its_only_scan() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let staged = StagedChunk::new(OperationId::new("single-pin"), b"abcdef".to_vec());
        store.put(staged.clone()).unwrap();
        VERIFIED_SCAN_BYTES.with(|bytes| bytes.set(0));
        let reader = store.open_cataloged(&staged.chunk.id).unwrap();
        assert_eq!(VERIFIED_SCAN_BYTES.with(|bytes| bytes.get()), 0);
        let path = temp.path().join("chunks").join(&staged.chunk.id.0);
        fs::rename(&path, temp.path().join("old-inode")).unwrap();
        fs::write(&path, b"abXdef").unwrap();
        let mut private = [9; 3];
        assert_eq!(
            reader
                .read_ranges_at_uncommitted(
                    &[VerifiedRangeCopy {
                        chunk_offset: 1,
                        length: private.len(),
                        output_offset: 0,
                    }],
                    &mut private,
                )
                .unwrap(),
            vec![3]
        );
        assert_eq!(&private, b"bcd");
        assert_eq!(VERIFIED_SCAN_BYTES.with(|bytes| bytes.get()), 6);
        assert!(
            store
                .read_range_at_uncommitted(&staged.chunk.id, 1, &mut private)
                .is_err()
        );
        assert_eq!(store.quarantined_chunks().unwrap().len(), 1);
    }

    #[test]
    fn private_range_rejects_outside_damage_and_quarantines_until_replacement() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let staged = StagedChunk::new(OperationId::new("single-corrupt"), vec![42; 150_001]);
        store.put(staged.clone()).unwrap();
        let path = temp.path().join("chunks").join(&staged.chunk.id.0);
        let mut damaged = staged.bytes().to_vec();
        damaged[150_000] ^= 1;
        fs::write(&path, damaged).unwrap();
        let mut private = [9; 19];
        assert_eq!(
            store
                .read_range_at_uncommitted(&staged.chunk.id, 65_531, &mut private)
                .unwrap_err()
                .code(),
            afs_error::NODE_TRANSFER_CORRUPT_DATA
        );
        assert_eq!(store.quarantined_chunks().unwrap().len(), 1);
        store.put(staged.clone()).unwrap();
        assert!(store.quarantined_chunks().unwrap().is_empty());
        assert_eq!(
            store
                .read_range_at_uncommitted(&staged.chunk.id, 65_531, &mut private)
                .unwrap(),
            private.len()
        );
        assert_eq!(private, [42; 19]);
    }

    #[test]
    fn private_range_and_verified_open_quarantine_missing_cataloged_files() {
        for private_read in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
            let staged = StagedChunk::new(OperationId::new("missing-file"), b"abcdef".to_vec());
            store.put(staged.clone()).unwrap();
            fs::remove_file(temp.path().join("chunks").join(&staged.chunk.id.0)).unwrap();
            if private_read {
                assert!(
                    store
                        .read_range_at_uncommitted(&staged.chunk.id, 0, &mut [9; 3])
                        .is_err()
                );
            } else {
                assert!(store.open_verified(&staged.chunk.id).is_err());
            }
            assert_eq!(store.quarantined_chunks().unwrap().len(), 1);
            store.put(staged.clone()).unwrap();
            assert!(store.quarantined_chunks().unwrap().is_empty());
            let mut out = [9; 3];
            assert_eq!(
                store
                    .read_range_at_uncommitted(&staged.chunk.id, 0, &mut out)
                    .unwrap(),
                3
            );
            assert_eq!(&out, b"abc");
        }
    }

    #[test]
    fn private_range_work_metrics_count_actual_scan_and_copy_bytes() {
        let temp = tempfile::tempdir().unwrap();
        let registry = afs_metrics::Registry::new();
        let store = LocalChunkStore::open(temp.path(), "node-a")
            .unwrap()
            .with_read_work_metrics(
                super::super::read_work::ReadWorkMetrics::register(&registry).unwrap(),
            );
        let staged = StagedChunk::new(OperationId::new("work-counts"), vec![7; 65_537]);
        store.put(staged.clone()).unwrap();
        store.read_at(&staged.chunk.id, 0, &mut [0; 3]).unwrap();
        store
            .read_range_at_uncommitted(&staged.chunk.id, 0, &mut [0; 3])
            .unwrap();
        let text = afs_metrics::encode_text(&registry).unwrap();
        assert!(text.contains("afs_dfs_read_scans_total 2\n"), "{text}");
        assert!(
            text.contains("afs_dfs_read_scan_bytes_total 131074\n"),
            "{text}"
        );
        assert!(
            text.contains("afs_dfs_read_copy_bytes_total{stage=\"scan_to_range\"} 3\n"),
            "{text}"
        );
        assert!(
            text.contains("afs_dfs_read_copy_bytes_total{stage=\"pinned_range_to_output\"} 3\n"),
            "{text}"
        );
        // The verified range passes through the official Bao codec before
        // exact extraction: two full covering-group copies plus three bytes.
        assert!(
            text.contains(
                "afs_dfs_read_copy_bytes_total{stage=\"outboard_range_delivery\"} 65539\n"
            ),
            "{text}"
        );
    }

    #[test]
    fn private_range_rejects_truncated_files_and_keeps_eof_behavior() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let staged = StagedChunk::new(OperationId::new("single-truncate"), b"abcdef".to_vec());
        store.put(staged.clone()).unwrap();
        let mut private = [9; 8];
        assert_eq!(
            store
                .read_range_at_uncommitted(&staged.chunk.id, 4, &mut private)
                .unwrap(),
            2
        );
        assert_eq!(&private, &[b'e', b'f', 9, 9, 9, 9, 9, 9]);
        assert_eq!(
            store
                .read_range_at_uncommitted(&staged.chunk.id, u64::MAX, &mut private)
                .unwrap(),
            0
        );
        assert_eq!(
            store
                .read_range_at_uncommitted(&staged.chunk.id, 0, &mut [])
                .unwrap(),
            0
        );
        fs::write(temp.path().join("chunks").join(&staged.chunk.id.0), b"abc").unwrap();
        assert_eq!(
            store
                .read_range_at_uncommitted(&staged.chunk.id, 0, &mut private)
                .unwrap_err()
                .code(),
            afs_error::NODE_TRANSFER_CORRUPT_DATA
        );
        assert_eq!(store.quarantined_chunks().unwrap().len(), 1);
    }

    #[test]
    fn corrupt_cataloged_chunk_is_replaced_by_immutable_retry() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let staged = StagedChunk::new(OperationId::new("retry-corrupt"), b"abcdef".to_vec());
        let first = store.put(staged.clone()).unwrap();
        let pin = store.open_verified(&staged.chunk.id).unwrap();
        fs::write(
            temp.path().join("chunks").join(&staged.chunk.id.0),
            b"abXdef",
        )
        .unwrap();
        let repaired = store.put(staged.clone()).unwrap();
        assert!(repaired.durable_acks[0].catalog_revision > first.durable_acks[0].catalog_revision);
        let mut out = [9; 6];
        assert!(pin.read_at(0, &mut out).is_err());
        assert_eq!(out, [9; 6]);
        // Revalidating an old pin cannot quarantine the newer healthy file.
        store.quarantine_if_invalid(&staged.chunk.id).unwrap();
        assert!(store.quarantined_chunks().unwrap().is_empty());
        assert_eq!(store.read_at(&staged.chunk.id, 0, &mut out).unwrap(), 6);
        assert_eq!(&out, b"abcdef");
        drop(store);
        let recovered = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        assert_eq!(recovered.read_at(&staged.chunk.id, 0, &mut out).unwrap(), 6);
        assert_eq!(&out, b"abcdef");
    }

    #[test]
    fn capacity_reports_fd_backed_root_after_path_replacement_and_catalog_advance() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("store");
        let store = LocalChunkStore::open(&root, "node-a").unwrap();
        let first = store.capacity().unwrap();
        assert!(first.bsize > 0);
        assert!(first.frsize > 0);
        assert!(first.namelen > 0);
        assert!(first.blocks >= first.bfree);
        assert!(first.bfree >= first.bavail);

        store
            .put(StagedChunk::new(
                OperationId::new("capacity"),
                b"bytes".to_vec(),
            ))
            .unwrap();
        let descriptor_after_write = store.device_descriptor().unwrap();
        assert!(descriptor_after_write.catalog_revision > 0);

        let moved = temp.path().join("store-moved");
        fs::rename(&root, &moved).unwrap();
        // A path-based reopen must fail; the held directory FD remains valid.
        std::os::unix::fs::symlink(temp.path().join("missing"), &root).unwrap();

        let after_replacement = store.capacity().unwrap();
        assert!(after_replacement.bsize > 0);
        assert!(after_replacement.frsize > 0);
        assert_eq!(store.device_descriptor().unwrap(), descriptor_after_write);
    }

    #[test]
    fn quarantine_survives_restart_and_verified_replacement_clears_it() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let staged = StagedChunk::new(OperationId::new("quarantine"), b"abcdef".to_vec());
        store.put(staged.clone()).unwrap();
        fs::write(
            temp.path().join("chunks").join(&staged.chunk.id.0),
            b"abXdef",
        )
        .unwrap();
        assert!(store.open_verified(&staged.chunk.id).is_err());
        let records = store.quarantined_chunks().unwrap();
        assert_eq!(records.len(), 1);
        let quarantine_revision = records[0].catalog_revision;
        drop(store);
        let recovered = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        assert_eq!(recovered.quarantined_chunks().unwrap(), records);
        assert!(recovered.open_verified(&staged.chunk.id).is_err());
        let receipt = recovered.put(staged.clone()).unwrap();
        assert!(receipt.durable_acks[0].catalog_revision > quarantine_revision);
        assert!(recovered.quarantined_chunks().unwrap().is_empty());
        let mut out = [0; 6];
        recovered.read_at(&staged.chunk.id, 0, &mut out).unwrap();
        assert_eq!(&out, b"abcdef");
    }

    #[test]
    fn recovery_quarantines_missing_or_truncated_chunks_without_losing_healthy_files() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let items: Vec<_> = ["missing", "truncated", "healthy"]
            .into_iter()
            .map(|name| StagedChunk::new(OperationId::new(name), name.as_bytes().to_vec()))
            .collect();
        for item in &items {
            store.put(item.clone()).unwrap();
        }
        fs::remove_file(temp.path().join("chunks").join(&items[0].chunk.id.0)).unwrap();
        fs::write(temp.path().join("chunks").join(&items[1].chunk.id.0), b"x").unwrap();
        drop(store);
        let recovered = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        assert_eq!(recovered.quarantined_chunks().unwrap().len(), 2);
        let mut out = [0; 7];
        recovered.read_at(&items[2].chunk.id, 0, &mut out).unwrap();
        assert_eq!(&out, b"healthy");
        for item in &items[..2] {
            recovered.put(item.clone()).unwrap();
        }
        assert!(recovered.quarantined_chunks().unwrap().is_empty());
    }

    #[test]
    fn pinned_reader_verifies_partial_ranges_and_rejects_damage_outside_them() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let bytes: Vec<_> = (0..524_289).map(|index| (index % 251) as u8).collect();
        let staged = StagedChunk::new(OperationId::new("range"), bytes.clone());
        store.put(staged.clone()).unwrap();
        let reader = store.open_verified(&staged.chunk.id).unwrap();
        let mut out = [9; 19];
        // Cover both the old 64KiB and candidate 256KiB scan boundaries,
        // including a final short scan outside either requested range.
        for offset in [65_531, 262_139] {
            assert_eq!(reader.read_at(offset, &mut out).unwrap(), 19);
            assert_eq!(&out, &bytes[offset as usize..offset as usize + 19]);
        }
        let path = temp.path().join("chunks").join(&staged.chunk.id.0);
        let mut damaged = bytes;
        damaged[524_288] ^= 1;
        fs::write(path, damaged).unwrap();
        out.fill(9);
        assert_eq!(
            reader.read_at(65_531, &mut out).unwrap_err().code(),
            afs_error::NODE_TRANSFER_CORRUPT_DATA
        );
        assert_eq!(out, [9; 19]);
    }

    #[test]
    fn verify_file_zero_range_still_hashes_the_whole_chunk() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let staged = StagedChunk::new(OperationId::new("verify-zero"), b"abcdef".to_vec());
        store.put(staged.clone()).unwrap();
        let path = temp.path().join("chunks").join(&staged.chunk.id.0);
        verify_file(&path, &staged.chunk).unwrap();
        fs::write(&path, b"abcXef").unwrap();
        assert_eq!(
            verify_file(&path, &staged.chunk).unwrap_err().code(),
            afs_error::NODE_TRANSFER_CORRUPT_DATA
        );
    }

    #[test]
    fn pinned_reader_rejects_truncation_without_publishing_a_partial_read() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let staged = StagedChunk::new(OperationId::new("truncate"), b"abcdef".to_vec());
        store.put(staged.clone()).unwrap();
        let reader = store.open_verified(&staged.chunk.id).unwrap();
        fs::write(temp.path().join("chunks").join(&staged.chunk.id.0), b"abc").unwrap();
        let mut out = [9; 6];
        assert_eq!(
            reader.read_at(0, &mut out).unwrap_err().code(),
            afs_error::NODE_TRANSFER_CORRUPT_DATA
        );
        assert_eq!(out, [9; 6]);
    }

    #[test]
    fn pinned_reader_preserves_descriptor_identity_and_eof_behavior() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let staged = StagedChunk::new(OperationId::new("pin"), b"abcdef".to_vec());
        store.put(staged.clone()).unwrap();
        let reader = store.open_verified(&staged.chunk.id).unwrap();
        let replacement = temp.path().join("replacement");
        fs::write(&replacement, b"abXdef").unwrap();
        fs::rename(
            replacement,
            temp.path().join("chunks").join(&staged.chunk.id.0),
        )
        .unwrap();
        let mut out = [9; 8];
        assert_eq!(reader.read_at(0, &mut out).unwrap(), 6);
        assert_eq!(&out, &[b'a', b'b', b'c', b'd', b'e', b'f', 9, 9]);
        out.fill(9);
        assert_eq!(reader.read_at(4, &mut out).unwrap(), 2);
        assert_eq!(&out, &[b'e', b'f', 9, 9, 9, 9, 9, 9]);
        assert_eq!(reader.read_at(u64::MAX, &mut out).unwrap(), 0);
        assert_eq!(reader.read_at(0, &mut []).unwrap(), 0);
        assert!(store.open_verified(&staged.chunk.id).is_err());
    }

    #[test]
    fn immutable_chunk_retry_preserves_content_after_catalog_advances_and_restart() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let target = ReplicaTarget {
            node_id: "node-a".into(),
            node_epoch: 3,
            data_endpoint: "https://node-a".into(),
            device: store.device_descriptor().unwrap(),
        };
        let first = StagedChunk::new(OperationId::new("op-first"), b"first".to_vec());
        let ack = store.persist(&first, &target, 4, 5).unwrap();
        store
            .persist(
                &StagedChunk::new(OperationId::new("op-second"), b"second".to_vec()),
                &target,
                4,
                5,
            )
            .unwrap();
        let replay = store.persist(&first, &target, 4, 5).unwrap();
        assert_eq!(replay.chunk_id, ack.chunk_id);
        assert_eq!(replay.verified_digest, ack.verified_digest);
        assert_eq!(replay.persisted_bytes, ack.persisted_bytes);
        assert!(replay.catalog_revision > ack.catalog_revision);
        let mut newer_target = target.clone();
        newer_target.device = store.device_descriptor().unwrap();
        let floor_ack = store.persist(&first, &newer_target, 6, 7).unwrap();
        assert_eq!(
            floor_ack.catalog_revision,
            newer_target.device.catalog_revision
        );
        drop(store);
        let recovered = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let recovered_ack = recovered.persist(&first, &target, 4, 5).unwrap();
        assert_eq!(recovered_ack.chunk_id, ack.chunk_id);
        assert_eq!(recovered_ack.verified_digest, ack.verified_digest);
        assert_eq!(recovered_ack.catalog_revision, replay.catalog_revision);
        let mut bytes = [0; 5];
        assert_eq!(
            recovered.read_at(&first.chunk.id, 0, &mut bytes).unwrap(),
            5
        );
        assert_eq!(&bytes, b"first");
        assert_eq!(
            recovered.persist(&first, &newer_target, 6, 7).unwrap(),
            floor_ack
        );
    }
}

#[cfg(test)]
mod write_buffer_tests {
    use super::*;

    #[test]
    fn writable_capacity_keeps_requested_limit_separate_from_reused_allocation() {
        let pool = WriteBufferPool::new(8192);
        drop(ChunkBuilder::with_pool(pool.clone(), 4096).unwrap());
        let mut builder = ChunkBuilder::with_pool(pool.clone(), 3000).unwrap();
        assert_eq!(builder.capacity(), 4096);
        assert_eq!(builder.writable_capacity(), 3000);
        builder.write_at(2999, &[7]).unwrap();
        assert_eq!(
            builder.write_at(3000, &[8]).unwrap_err().code(),
            afs_error::IO_OUT_OF_MEMORY
        );
        assert_eq!(pool.snapshot().live_bytes, 4096);
    }

    #[test]
    fn hot_large_allocation_cannot_inflate_a_small_patch() {
        let pool = WriteBufferPool::new(8 * 1024 * 1024);
        let large = ChunkBuilder::with_pool(pool.clone(), 4 * 1024 * 1024).unwrap();
        let large_pointer = large.data_ptr();
        drop(large);
        let mut patch = ChunkBuilder::with_pool(pool.clone(), 4096).unwrap();
        patch.write_at(0, &[3; 4096]).unwrap();
        assert_eq!(patch.capacity(), 4096);
        assert_ne!(patch.data_ptr(), large_pointer);
        assert_eq!(pool.snapshot().live_bytes, 4096);
        assert_eq!(pool.snapshot().idle_bytes, 4 * 1024 * 1024);
        assert_eq!(pool.snapshot().allocations, 2);
        assert_eq!(pool.snapshot().reuses, 0);
    }

    #[test]
    fn six_mebibyte_body_uses_two_final_allocations_without_freeze_copy() {
        const MIB: usize = 1024 * 1024;
        let pool = WriteBufferPool::new(8 * MIB);
        let mut first = ChunkBuilder::with_pool(pool.clone(), 4 * MIB).unwrap();
        let mut tail = ChunkBuilder::with_pool(pool.clone(), 4 * MIB).unwrap();
        let input = vec![37; MIB];
        for index in 0..6 {
            let (builder, offset) = if index < 4 {
                (&mut first, index * MIB)
            } else {
                (&mut tail, (index - 4) * MIB)
            };
            builder.write_at(offset as u64, &input).unwrap();
        }
        let first_pointer = first.data_ptr();
        let tail_pointer = tail.data_ptr();
        let first_generation = first.allocation_id();
        let tail_generation = tail.allocation_id();
        let first = first.stage(OperationId::new("four-mib"));
        let tail = tail.stage(OperationId::new("two-mib"));
        assert_eq!(first.bytes().len(), 4 * MIB);
        assert_eq!(tail.bytes().len(), 2 * MIB);
        assert_eq!(first.bytes().as_ptr(), first_pointer);
        assert_eq!(tail.bytes().as_ptr(), tail_pointer);
        assert_eq!(first.bytes.allocation_id(), first_generation);
        assert_eq!(tail.bytes.allocation_id(), tail_generation);
        assert!(
            first
                .bytes()
                .iter()
                .chain(tail.bytes())
                .all(|byte| *byte == 37)
        );
        let stats = pool.snapshot();
        assert_eq!(stats.live_bytes, 8 * MIB);
        assert_eq!(stats.copied_bytes, 6 * MIB as u64);
        assert_eq!(stats.copy_operations, 6);
        assert_eq!(stats.allocations, 2);
        drop(first);
        drop(tail);
        assert_eq!(pool.snapshot().live_bytes, 0);
        assert_eq!(pool.snapshot().idle_bytes, 8 * MIB);
    }

    #[test]
    fn actual_copy_entries_count_overlap_extension_and_zeroed_holes_separately() {
        let pool = WriteBufferPool::new(32);
        let mut builder = ChunkBuilder::with_pool(pool.clone(), 16).unwrap();
        builder.write_at(0, b"abc").unwrap();
        builder.write_at(2, b"def").unwrap();
        builder.write_at(8, b"g").unwrap();
        assert_eq!(builder.as_bytes(), b"abdef\0\0\0g");
        assert_eq!(pool.snapshot().copied_bytes, 7);
        // The extension crossing the initialized tail copies one existing and
        // one new range; sparse zero initialization is not input copying.
        assert_eq!(pool.snapshot().copy_operations, 4);
    }

    #[test]
    fn exact_metrics_track_last_owner_reuse_and_stop() {
        let registry = afs_metrics::Registry::new();
        let pool = WriteBufferPool::new(4096);
        pool.register_metrics(&registry).unwrap();
        let mut builder = ChunkBuilder::with_pool(pool.clone(), 4096).unwrap();
        builder.write_at(0, b"data").unwrap();
        let frozen = builder.freeze();
        let delayed = frozen.slice(..);
        drop(frozen);
        let before = afs_metrics::encode_text(&registry).unwrap();
        assert!(
            before.contains("afs_dfs_write_buffer_live_bytes 4096\n"),
            "{before}"
        );
        assert!(
            before.contains("afs_dfs_write_buffer_idle_bytes 0\n"),
            "{before}"
        );
        drop(delayed);
        let reused = ChunkBuilder::with_pool(pool.clone(), 4096).unwrap();
        drop(reused);
        pool.stop();
        let after = afs_metrics::encode_text(&registry).unwrap();
        for expected in [
            "afs_dfs_write_buffer_live_bytes 0\n",
            "afs_dfs_write_buffer_idle_bytes 0\n",
            "afs_dfs_write_buffer_live_peak_bytes 4096\n",
            "afs_dfs_write_buffer_allocations_total 1\n",
            "afs_dfs_write_buffer_reuses_total 1\n",
            "afs_dfs_write_buffer_released_bytes_total 4096\n",
            "afs_dfs_write_buffer_input_copy_bytes_total 4\n",
            "afs_dfs_write_buffer_input_copies_total 1\n",
        ] {
            assert!(after.contains(expected), "missing {expected} in {after}");
        }
    }

    #[test]
    fn freeze_and_shared_ranges_keep_allocation_until_last_owner() {
        let pool = WriteBufferPool::new(8192);
        let mut builder = ChunkBuilder::with_pool(pool.clone(), 4096).unwrap();
        builder.write_at(0, b"abcdef").unwrap();
        let pointer = builder.data_ptr();
        let generation = builder.allocation_id();
        let capacity = builder.capacity();
        let frozen = builder.freeze();
        assert_eq!(frozen.as_ptr(), pointer);
        assert_eq!(frozen.allocation_id(), generation);
        assert_eq!(frozen.capacity(), capacity);
        let range = frozen.slice(1..4);
        let staged = StagedChunk::from_shared(OperationId::new("shared-freeze"), range.clone());
        assert_eq!(staged.bytes(), b"bcd");
        assert_eq!(staged.bytes().as_ptr(), pointer.wrapping_add(1));
        assert_eq!(staged.chunk.length, 3);
        assert_eq!(pool.snapshot().copied_bytes, 6);
        drop(frozen);
        drop(range);
        assert_eq!(pool.snapshot().live_buffers, 1);
        assert_eq!(pool.snapshot().idle_bytes, 0);
        drop(staged);
        assert_eq!(pool.snapshot().live_bytes, 0);
        assert_eq!(pool.snapshot().idle_bytes, capacity);
    }

    #[test]
    fn reuse_has_new_generation_and_never_exposes_old_data() {
        let pool = WriteBufferPool::new(8192);
        let mut first = ChunkBuilder::with_pool(pool.clone(), 4096).unwrap();
        first.write_at(0, &[0xa5; 4096]).unwrap();
        let pointer = first.data_ptr();
        let generation = first.allocation_id();
        drop(first);
        let mut next = ChunkBuilder::with_pool(pool.clone(), 4096).unwrap();
        assert_eq!(next.data_ptr(), pointer);
        assert_ne!(next.allocation_id(), generation);
        assert_eq!(next.len(), 0);
        next.write_at(8, &[7]).unwrap();
        assert_eq!(next.as_bytes(), &[0, 0, 0, 0, 0, 0, 0, 0, 7]);
        next.truncate(3);
        next.write_at(8, &[9]).unwrap();
        assert_eq!(next.as_bytes(), &[0, 0, 0, 0, 0, 0, 0, 0, 9]);
        assert_eq!(pool.snapshot().allocations, 1);
        assert_eq!(pool.snapshot().reuses, 1);
    }

    #[test]
    fn idle_baseline_does_not_limit_live_and_stop_retires_late_owners() {
        let pool = WriteBufferPool::new(4096);
        let mut builders = Vec::new();
        for _ in 0..3 {
            let mut builder = ChunkBuilder::with_pool(pool.clone(), 4096).unwrap();
            builder.write_at(0, &[1; 4096]).unwrap();
            builders.push(builder);
        }
        assert_eq!(pool.snapshot().live_bytes, 3 * 4096);
        let retained = builders.pop().unwrap().freeze();
        drop(builders);
        assert_eq!(pool.snapshot().live_bytes, 4096);
        assert_eq!(pool.snapshot().idle_bytes, 4096);
        assert_eq!(pool.snapshot().released_bytes, 4096);
        pool.stop();
        assert_eq!(pool.snapshot().idle_bytes, 0);
        assert_eq!(pool.snapshot().live_bytes, 4096);
        drop(retained);
        assert_eq!(pool.snapshot().live_bytes, 0);
        assert_eq!(pool.snapshot().released_bytes, 3 * 4096);
        let after_stop = ChunkBuilder::with_pool(pool.clone(), 4096).unwrap();
        drop(after_stop);
        assert_eq!(pool.snapshot().idle_bytes, 0);
    }

    #[test]
    fn fixed_capacity_failure_keeps_previously_accepted_bytes() {
        let pool = WriteBufferPool::new(0);
        let mut builder = ChunkBuilder::with_pool(pool.clone(), 4).unwrap();
        builder.write_at(0, b"abcd").unwrap();
        let pointer = builder.data_ptr();
        let error = builder.write_at(3, b"ef").unwrap_err();
        assert_eq!(error.code(), afs_error::IO_OUT_OF_MEMORY);
        assert_eq!(builder.as_bytes(), b"abcd");
        assert_eq!(builder.data_ptr(), pointer);
        assert_eq!(pool.snapshot().copied_bytes, 4);
        assert_eq!(pool.snapshot().copy_operations, 1);
        pool.fail_next_allocation();
        assert_eq!(
            ChunkBuilder::with_pool(pool.clone(), 8).unwrap_err().code(),
            afs_error::IO_OUT_OF_MEMORY
        );
        assert_eq!(pool.snapshot().live_bytes, 4);
    }

    #[test]
    fn pooled_builder_cannot_keep_pool_alive_or_modify_a_frozen_chunk() {
        let pool = WriteBufferPool::new(4096);
        let weak = Arc::downgrade(&pool);
        let mut builder = ChunkBuilder::with_pool(pool.clone(), 4096).unwrap();
        builder.write_at(0, b"original").unwrap();
        let frozen = builder.freeze();
        drop(pool);
        assert!(weak.upgrade().is_none());
        assert_eq!(&*frozen, b"original");
        drop(frozen);
    }

    #[test]
    fn staged_vec_and_default_builder_keep_legacy_content_without_vec_to_arc_copy() {
        let bytes = b"owned".to_vec();
        let pointer = bytes.as_ptr();
        let staged = StagedChunk::new(OperationId::new("vec-owner"), bytes);
        assert_eq!(staged.bytes().as_ptr(), pointer);
        let mut builder = ChunkBuilder::default();
        builder.write_at(3, b"data").unwrap();
        assert_eq!(builder.as_bytes(), b"\0\0\0data");
        builder.replace(b"replacement".to_vec());
        assert_eq!(
            builder.stage(OperationId::new("legacy")).bytes(),
            b"replacement"
        );
    }

    #[test]
    fn mutable_overwrite_is_in_place_and_frozen_identity_is_immutable() {
        let pool = WriteBufferPool::new(8192);
        let mut builder = ChunkBuilder::with_pool(pool.clone(), 4096).unwrap();
        builder.write_at(0, b"abcdef").unwrap();
        let pointer = builder.data_ptr();
        builder.write_at(2, b"XY").unwrap();
        assert_eq!(builder.data_ptr(), pointer);
        let staged = builder.stage(OperationId::new("first"));
        assert_eq!(staged.bytes(), b"abXYef");
        let mut patch = ChunkBuilder::with_pool(pool.clone(), 2).unwrap();
        patch.write_at(0, b"zz").unwrap();
        assert_eq!(patch.stage(OperationId::new("patch")).bytes(), b"zz");
        assert_eq!(staged.bytes(), b"abXYef");
        assert_eq!(pool.snapshot().copied_bytes, 10);
    }
}

#[cfg(test)]
mod crc32c_storage_tests {
    use super::*;
    fn staged() -> StagedChunk {
        StagedChunk::from_shared_crc32c(
            OperationId::new("crc-check"),
            SharedChunkBytes::from_buffer(ChunkBuffer::unpooled(vec![73; 2 * 32768 + 13])),
        )
        .unwrap()
    }
    #[test]
    fn crc32c_known_vector_and_tail() {
        assert_eq!(
            super::super::chunk_crc32c::checksum(b"123456789"),
            0xe3069283
        );
        for n in [0, 1, 7, 8, 31, 32768, 32781] {
            let bytes = vec![73; n];
            assert_eq!(
                super::super::chunk_crc32c::checksum(&bytes),
                crc32c::crc32c(&bytes)
            );
        }
    }
    #[test]
    fn crc32c_unique_id_and_stable_retry() {
        let first = staged();
        let second = staged();
        assert_ne!(first.chunk.id, second.chunk.id);
        let retry = StagedChunk::from_shared_crc32c_id(
            first.operation_id.clone(),
            first.bytes.clone(),
            first.chunk.id.clone(),
        )
        .unwrap();
        assert_eq!(retry.chunk, first.chunk);
        assert!(first.chunk.valid_immutable_identity());
        assert!(first.outboard.is_none());
    }

    #[test]
    fn crc32c_replica_receiver_preserves_identity_and_rejects_changed_body() {
        let original = staged();
        let received = StagedChunk::from_replica(
            OperationId::new("repair-attempt"),
            &original.chunk,
            original.bytes().to_vec(),
        )
        .unwrap();
        assert_eq!(received.chunk, original.chunk);
        assert_ne!(received.operation_id, original.operation_id);
        let mut damaged = original.bytes().to_vec();
        damaged[32770] ^= 1;
        assert!(
            StagedChunk::from_replica(OperationId::new("retry"), &original.chunk, damaged).is_err()
        );
    }
    #[test]
    fn crc32c_persist_reopen_range_and_gc_without_writer_switch() {
        let temp = tempfile::tempdir().unwrap();
        let item = staged();
        let id = item.chunk.id.clone();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        store.put(item.clone()).unwrap();
        drop(store);
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        store.recover().unwrap();
        let mut out = [0; 30];
        assert_eq!(store.read_at(&id, 32760, &mut out).unwrap(), 30);
        assert_eq!(out, [73; 30]);
        assert!(
            store
                .gc_inventory()
                .unwrap()
                .iter()
                .any(|c| c == &item.chunk)
        );
        assert_eq!(store.peer_catalog_record(&id).unwrap().chunk, item.chunk);
        store
            .collect_generation(1, std::slice::from_ref(&id))
            .unwrap();
        assert!(!store.chunk_path(&id).exists());
    }
    #[test]
    fn crc32c_peer_range_preserves_exact_grant_and_detects_boundary_corruption() {
        let temp = tempfile::tempdir().unwrap();
        let item = staged();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        store.put(item.clone()).unwrap();
        let payload = store.read_peer_range(&item.chunk.id, 32760, 30).unwrap();
        assert_eq!(payload.as_slice(), &[73; 30]);
        drop(payload);
        // This byte is outside the returned range but in its covering segment.
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(store.chunk_path(&item.chunk.id))
            .unwrap();
        file.write_at(&[99], 32759).unwrap();
        assert!(store.read_peer_range(&item.chunk.id, 32760, 30).is_err());
    }

    #[test]
    fn crc32c_retry_adopts_synced_orphan_and_replays_durable_object() {
        let temp = tempfile::tempdir().unwrap();
        let item = staged();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let mut file = File::create(store.chunk_path(&item.chunk.id)).unwrap();
        file.write_all(item.bytes()).unwrap();
        file.sync_all().unwrap();
        drop(file);
        store.put(item.clone()).unwrap();
        drop(store);
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        store.recover().unwrap();
        store.put(item.clone()).unwrap();
        let mut out = [0; 20];
        assert_eq!(store.read_at(&item.chunk.id, 32760, &mut out).unwrap(), 20);
        assert_eq!(out, [73; 20]);
        assert_eq!(store.gc_inventory().unwrap().len(), 1);
    }
    #[test]
    fn crc32c_rejects_corrupt_orphan_without_catalog_publication() {
        let temp = tempfile::tempdir().unwrap();
        let item = staged();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        std::fs::write(
            store.chunk_path(&item.chunk.id),
            vec![74; item.bytes().len()],
        )
        .unwrap();
        assert!(store.put(item.clone()).is_err());
        assert!(store.gc_inventory().unwrap().is_empty());
    }
    #[test]
    fn crc32c_missing_or_damaged_descriptor_and_body_fail_closed() {
        for damage in 0..4 {
            let temp = tempfile::tempdir().unwrap();
            let item = staged();
            let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
            store.put(item.clone()).unwrap();
            if damage < 2 {
                let mut catalog = store.catalog.lock().unwrap();
                let record = catalog.records.get_mut(&item.chunk.id).unwrap();
                if damage == 0 {
                    record.segment_crc = None;
                } else {
                    record.segment_crc.as_mut().unwrap().checksums[1] ^= 1;
                }
            } else {
                let body = std::fs::OpenOptions::new()
                    .write(true)
                    .open(store.chunk_path(&item.chunk.id))
                    .unwrap();
                if damage == 2 {
                    body.set_len(10).unwrap();
                } else {
                    body.write_at(&[99], 32770).unwrap();
                }
            }
            assert!(store.read_at(&item.chunk.id, 32760, &mut [0; 30]).is_err());
        }
    }
    #[test]
    fn crc32c_missing_body_recovery_quarantines_without_recursion() {
        let temp = tempfile::tempdir().unwrap();
        let item = staged();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        store.put(item.clone()).unwrap();
        std::fs::remove_file(store.chunk_path(&item.chunk.id)).unwrap();
        store.recover().unwrap();
        assert_eq!(
            store.catalog.lock().unwrap().records[&item.chunk.id].state,
            LocalChunkState::Quarantined
        );
    }
}
