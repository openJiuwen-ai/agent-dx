//! Initialized storage is recycled only after its final owner. Native async
//! pools charge both idle and borrowed capacity to the same fixed byte limit.
use super::super::read_work::ReadWorkMetrics;
use afs_error::{Error, Result};
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};
use std::time::Instant;

const MAX_BUFFER: usize = 1024 * 1024;
const SLOTS: usize = 2;

#[derive(Debug, Default)]
struct PoolState {
    free: Vec<Vec<u8>>,
    allocated: usize,
    peak: usize,
    limit: Option<usize>,
}
#[derive(Debug)]
pub(crate) struct ReadBufferPool {
    state: Mutex<PoolState>,
    changed: tokio::sync::Notify,
    role: &'static str,
    metrics: std::sync::OnceLock<ReadWorkMetrics>,
}
impl ReadBufferPool {
    pub(crate) fn new(role: &'static str) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(PoolState::default()),
            changed: tokio::sync::Notify::new(),
            role,
            metrics: std::sync::OnceLock::new(),
        })
    }
    pub(crate) fn configure_byte_limit(&self, limit: usize) -> Result<()> {
        if limit == 0 {
            return Err(capacity("zero buffer pool limit"));
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| capacity("buffer pool poisoned"))?;
        if state.limit == Some(limit) {
            return Ok(());
        }
        while state.allocated > limit {
            let Some(bytes) = state.free.pop() else {
                return Err(capacity("active buffers exceed configured limit"));
            };
            state.allocated = state.allocated.saturating_sub(bytes.capacity());
        }
        state.limit = Some(limit);
        drop(state);
        self.changed.notify_waiters();
        Ok(())
    }
    fn observe(&self, state: &PoolState, metrics: Option<&ReadWorkMetrics>) {
        if let Some(m) = metrics.or_else(|| self.metrics.get()) {
            let idle = state.free.iter().map(Vec::capacity).sum::<usize>();
            m.read_buffer_idle(self.role, idle as u64);
            m.read_buffer_capacity(
                self.role,
                state.allocated as u64,
                idle as u64,
                state.limit.unwrap_or(0) as u64,
                state.peak as u64,
            );
        }
    }
    fn try_take(
        self: &Arc<Self>,
        size: usize,
        metrics: Option<ReadWorkMetrics>,
    ) -> Result<Option<ReadBuffer>> {
        if let Some(m) = &metrics {
            self.metrics.get_or_init(|| m.clone());
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| capacity("read buffer pool poisoned"))?;
        let charge = size.max(MAX_BUFFER);
        let slot = state
            .free
            .iter()
            .enumerate()
            // Preserve legacy most-recent reuse when capacities are equal.
            .rev()
            .filter(|(_, v)| v.len() >= size && v.capacity() <= charge)
            .min_by_key(|(_, v)| v.capacity())
            .map(|(i, _)| i);
        let bytes = if let Some(index) = slot {
            let bytes = state.free.swap_remove(index);
            if let Some(m) = &metrics {
                m.read_buffer(self.role, "reuse", bytes.capacity() as u64);
            }
            bytes
        } else {
            if let Some(limit) = state.limit {
                if size > limit {
                    return Err(capacity("read buffer exceeds pool byte limit"));
                }
                while state.allocated.checked_add(size).is_none_or(|n| n > limit) {
                    let Some(idle) = state.free.pop() else {
                        self.observe(&state, metrics.as_ref());
                        return Ok(None);
                    };
                    state.allocated = state.allocated.saturating_sub(idle.capacity());
                }
            }
            let mut bytes = Vec::new();
            bytes
                .try_reserve_exact(size)
                .map_err(|_| capacity("read buffer allocation failed"))?;
            let next = state
                .allocated
                .checked_add(bytes.capacity())
                .ok_or_else(|| capacity("buffer accounting overflow"))?;
            if state.limit.is_some_and(|limit| next > limit) {
                return Err(capacity("allocator capacity exceeds pool limit"));
            }
            bytes.resize(size, 0);
            state.allocated = next;
            state.peak = state.peak.max(next);
            if let Some(m) = &metrics {
                m.read_buffer(self.role, "allocate", bytes.capacity() as u64);
                m.read_buffer(self.role, "zero", size as u64);
            }
            bytes
        };
        self.observe(&state, metrics.as_ref());
        drop(state);
        Ok(Some(ReadBuffer {
            bytes,
            length: size,
            pool: Some(self.clone()),
            metrics,
        }))
    }
    pub(crate) fn take(
        self: &Arc<Self>,
        size: usize,
        metrics: Option<ReadWorkMetrics>,
    ) -> Result<ReadBuffer> {
        self.try_take(size, metrics)?
            .ok_or_else(|| capacity("read buffer pool busy"))
    }
    pub(crate) async fn take_until(
        self: &Arc<Self>,
        size: usize,
        metrics: Option<ReadWorkMetrics>,
        deadline: Instant,
    ) -> Result<ReadBuffer> {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if let Some(buffer) = self.try_take(size, metrics.clone())? {
                return Ok(buffer);
            }
            tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), changed)
                .await
                .map_err(|_| capacity("read buffer deadline exceeded"))?;
        }
    }
    fn detach(&self, bytes: usize, metrics: Option<&ReadWorkMetrics>) {
        if let Ok(mut state) = self.state.lock() {
            state.allocated = state.allocated.saturating_sub(bytes);
            self.observe(&state, metrics);
        }
        self.changed.notify_waiters();
    }
}
impl Drop for ReadBufferPool {
    fn drop(&mut self) {
        if let Some(m) = self.metrics.get() {
            m.read_buffer_idle(self.role, 0);
            if let Ok(state) = self.state.get_mut() {
                m.read_buffer_capacity(
                    self.role,
                    0,
                    0,
                    state.limit.unwrap_or(0) as u64,
                    state.peak as u64,
                );
            }
        }
    }
}
fn capacity(message: &'static str) -> Error {
    Error::coded(afs_error::IO_CAPACITY, message)
}

#[derive(Debug)]
pub(crate) struct ReadBuffer {
    bytes: Vec<u8>,
    length: usize,
    pool: Option<Arc<ReadBufferPool>>,
    metrics: Option<ReadWorkMetrics>,
}
impl ReadBuffer {
    pub(crate) fn capacity(&self) -> usize {
        self.bytes.capacity()
    }
    pub(crate) fn into_vec(mut self) -> Vec<u8> {
        if let Some(pool) = self.pool.take() {
            pool.detach(self.bytes.capacity(), self.metrics.as_ref());
        }
        self.bytes.truncate(self.length);
        std::mem::take(&mut self.bytes)
    }
}
impl From<Vec<u8>> for ReadBuffer {
    fn from(bytes: Vec<u8>) -> Self {
        Self {
            length: bytes.len(),
            bytes,
            pool: None,
            metrics: None,
        }
    }
}
impl Deref for ReadBuffer {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.bytes[..self.length]
    }
}
impl DerefMut for ReadBuffer {
    fn deref_mut(&mut self) -> &mut [u8] {
        &mut self.bytes[..self.length]
    }
}
impl AsRef<[u8]> for ReadBuffer {
    fn as_ref(&self) -> &[u8] {
        self
    }
}
impl Drop for ReadBuffer {
    fn drop(&mut self) {
        let Some(pool) = &self.pool else {
            return;
        };
        let bytes = std::mem::take(&mut self.bytes);
        let n = bytes.capacity();
        if let Ok(mut state) = pool.state.lock() {
            let retain = n > 0
                && match state.limit {
                    Some(limit) => n <= limit,
                    None => n <= MAX_BUFFER && state.free.len() < SLOTS,
                };
            if retain {
                state.free.push(bytes);
                if let Some(m) = &self.metrics {
                    m.read_buffer(pool.role, "return", n as u64);
                }
            } else {
                state.allocated = state.allocated.saturating_sub(n);
            }
            pool.observe(&state, self.metrics.as_ref());
        }
        pool.changed.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn buffer_reuse_waits_for_last_shared_frame_and_preserves_initialized_storage() {
        let pool = ReadBufferPool::new("serve");
        let mut b = pool.take(1024, None).unwrap();
        b.fill(17);
        let ptr = b.as_ptr();
        let bytes = prost::bytes::Bytes::from_owner(b);
        let frame = bytes.slice(16..32);
        drop(bytes);
        assert!(pool.state.lock().unwrap().free.is_empty());
        let other = pool.take(1024, None).unwrap();
        assert_ne!(other.as_ptr(), ptr);
        assert_eq!(&frame[..], &[17; 16]);
        drop(frame);
        let reused = pool.take(512, None).unwrap();
        assert_eq!(reused.as_ptr(), ptr);
        assert_eq!(reused.len(), 512);
        assert_eq!(&reused[..], &[17; 512]);
    }
    #[test]
    fn buffer_reuse_idle_capacity_is_bounded_and_error_drop_returns_owner() {
        let pool = ReadBufferPool::new("reply");
        let held: Vec<_> = (0..5)
            .map(|_| pool.take(MAX_BUFFER, None).unwrap())
            .collect();
        drop(held);
        assert_eq!(
            pool.state
                .lock()
                .unwrap()
                .free
                .iter()
                .map(Vec::capacity)
                .sum::<usize>(),
            2 * MAX_BUFFER
        );
        drop(pool.take(MAX_BUFFER + 1, None).unwrap());
        assert_eq!(
            pool.state
                .lock()
                .unwrap()
                .free
                .iter()
                .map(Vec::capacity)
                .sum::<usize>(),
            2 * MAX_BUFFER
        );
        let ptr = {
            let b = pool.take(MAX_BUFFER, None).unwrap();
            let ptr = b.as_ptr();
            drop(b);
            ptr
        };
        assert_eq!(pool.take(MAX_BUFFER, None).unwrap().as_ptr(), ptr);
    }
    #[tokio::test(flavor = "current_thread")]
    async fn read_equivalence_pool_budget_reuses_idle_and_waits_for_last_owner() {
        let pool = ReadBufferPool::new("reply");
        pool.configure_byte_limit(2 * MAX_BUFFER).unwrap();
        let first = prost::bytes::Bytes::from_owner(pool.take(MAX_BUFFER, None).unwrap());
        let last = first.clone();
        drop(first);
        let second = pool.take(MAX_BUFFER, None).unwrap();
        assert!(pool.try_take(MAX_BUFFER, None).unwrap().is_none());
        let waiting = pool.clone();
        let task = tokio::spawn(async move {
            waiting
                .take_until(
                    MAX_BUFFER,
                    None,
                    Instant::now() + std::time::Duration::from_secs(1),
                )
                .await
        });
        tokio::task::yield_now().await;
        assert!(!task.is_finished());
        drop(last);
        let borrowed = task.await.unwrap().unwrap();
        drop(borrowed);
        drop(second);
        let state = pool.state.lock().unwrap();
        assert_eq!(state.allocated, 2 * MAX_BUFFER);
        assert_eq!(
            state.free.iter().map(Vec::capacity).sum::<usize>(),
            2 * MAX_BUFFER
        );
        drop(state);
        // Incompatible idle sizes can be reclaimed; idle credits cannot deadlock allocation.
        let large = pool.take(2 * MAX_BUFFER, None).unwrap();
        assert_eq!(large.capacity(), 2 * MAX_BUFFER);
        drop(large);
        assert!(pool.take(3 * MAX_BUFFER, None).is_err());
    }

    #[test]
    fn buffer_reuse_vec_compatibility_transfers_without_copy_or_pool_alias() {
        let pool = ReadBufferPool::new("reply");
        let b = pool.take(16, None).unwrap();
        let ptr = b.as_ptr();
        let v = b.into_vec();
        assert_eq!(v.as_ptr(), ptr);
        assert!(pool.state.lock().unwrap().free.is_empty());
    }
}
