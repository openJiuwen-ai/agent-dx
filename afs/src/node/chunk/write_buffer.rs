//! Reusable write buffers and the in-progress Chunk builder.

use super::*;

// A generation identifies one checkout, even when jemalloc or the idle pool
// reuses the same address. It is diagnostic identity, never persistent identity.
static WRITE_ALLOCATION_GENERATION: AtomicU64 = AtomicU64::new(1);

const MAX_IDLE_WRITE_BUFFERS: usize = 1024;

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
pub(super) struct WriteBufferPoolState {
    idle: Vec<Vec<u8>>,
    stats: WriteBufferPoolStats,
    stopped: bool,
    pub(super) metrics: Option<WriteBufferPoolMetrics>,
    #[cfg(test)]
    pub(super) test_hooks: WriteBufferPoolTestHooks,
}

#[cfg(test)]
#[derive(Debug, Default)]
pub(super) struct WriteBufferPoolTestHooks {
    pub(super) fail_next_allocation: bool,
    pub(super) next_allocation_capacity: Option<usize>,
}

#[derive(Debug)]
pub(super) struct WriteBufferPoolMetrics {
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
    pub(super) outboard_budget: Option<Arc<OutboardBudget>>,
    pub(super) state: Mutex<WriteBufferPoolState>,
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
        #[cfg(any(feature = "dfs", test))] mut native_live_lease: Option<DirtyLease>,
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
            if std::mem::take(&mut state.test_hooks.fail_next_allocation) {
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
            .test_hooks
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
    pub(in crate::node) fn fail_next_allocation(&self) {
        self.state.lock().unwrap().test_hooks.fail_next_allocation = true;
    }
}

#[derive(Debug)]
pub(super) struct ChunkBuffer {
    pub(super) bytes: Vec<u8>,
    pub(super) pool: Weak<WriteBufferPool>,
    allocation_id: u64,
    // Drops after the body is returned to idle or really freed in Drop. Every
    // SharedChunkBytes slice keeps this same single allocation owner alive.
    #[cfg(any(feature = "dfs", test))]
    native_live_lease: Option<DirtyLease>,
}

impl ChunkBuffer {
    pub(super) fn unpooled(bytes: Vec<u8>) -> Self {
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
    pub(super) buffer: Arc<ChunkBuffer>,
    start: usize,
    end: usize,
}

impl SharedChunkBytes {
    pub fn from_vec(bytes: Vec<u8>) -> Self {
        Self::from_buffer(ChunkBuffer::unpooled(bytes))
    }

    pub(super) fn from_buffer(buffer: ChunkBuffer) -> Self {
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
    pub(super) buffer: ChunkBuffer,
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
        lease: DirtyLease,
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
