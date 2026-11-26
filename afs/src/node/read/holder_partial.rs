//! Memory-backed ranges of cache copies that are still being assembled or installed.

use std::{
    collections::HashMap,
    ops::Range,
    sync::{Arc, Mutex, MutexGuard, Weak},
};

use crate::{
    dfs::ChunkId,
    node::{
        chunk::SharedChunkBytes,
        chunk_crc32c::{CrcTable, SEGMENT, checksum},
    },
};

#[derive(Debug, Default)]
pub(crate) struct PartialBytes {
    pub(crate) data: Vec<u8>,
    pub(crate) coverage: Vec<Range<usize>>,
    pub(crate) complete: Option<SharedChunkBytes>,
    #[cfg(test)]
    pub(crate) last_zero_fill: usize,
}

impl PartialBytes {
    fn bytes(&self) -> &[u8] {
        self.complete.as_deref().unwrap_or(&self.data)
    }
}

/// An assembler-owned body with one immutable descriptor. Completion moves
/// the same allocation into shared ownership for the installer.
#[derive(Debug)]
pub(crate) struct HolderPartial {
    pub(crate) table: Arc<CrcTable>,
    bytes: Mutex<PartialBytes>,
}

impl HolderPartial {
    pub(crate) fn lock(&self) -> MutexGuard<'_, PartialBytes> {
        self.bytes.lock().unwrap_or_else(|error| error.into_inner())
    }

    fn crc_bounds(&self, offset: u64, length: u64) -> Option<Range<usize>> {
        let end = offset.checked_add(length)?;
        if length == 0 || end > self.table.length {
            return None;
        }
        let start = (offset as usize / SEGMENT) * SEGMENT;
        let end = (end as usize).div_ceil(SEGMENT) * SEGMENT;
        Some(start..end.min(self.table.length as usize))
    }

    pub(crate) fn covers(&self, offset: u64, length: u64) -> bool {
        let Some(bounds) = self.crc_bounds(offset, length) else {
            return false;
        };
        let Ok(bytes) = self.bytes.lock() else {
            return false;
        };
        bytes
            .coverage
            .iter()
            .any(|range| range.start <= bounds.start && range.end >= bounds.end)
    }

    /// Copy only the exact grant range while the caller holds a read slot.
    /// Verify every CRC segment covering a boundary fragment.
    pub(crate) fn read<T>(
        &self,
        offset: u64,
        length: u64,
        copy: impl FnOnce(&[u8]) -> afs_error::Result<T>,
    ) -> afs_error::Result<Option<T>> {
        let Some(bounds) = self.crc_bounds(offset, length) else {
            return Ok(None);
        };
        let Ok(bytes) = self.bytes.lock() else {
            return Ok(None);
        };
        if !bytes
            .coverage
            .iter()
            .any(|range| range.start <= bounds.start && range.end >= bounds.end)
        {
            return Ok(None);
        }
        let Some(cover) = bytes.bytes().get(bounds.clone()) else {
            return Ok(None);
        };
        let first = bounds.start / SEGMENT;
        if cover
            .chunks(SEGMENT)
            .enumerate()
            .any(|(i, segment)| self.table.checksums.get(first + i) != Some(&checksum(segment)))
        {
            return Err(afs_error::Error::coded(
                afs_error::NODE_TRANSFER_CORRUPT_DATA,
                "DFS holder buffer differs from its trusted CRC table",
            ));
        }
        let start = offset as usize;
        let end = start + length as usize;
        copy(&bytes.bytes()[start..end]).map(Some)
    }
}

type PartialKey = (ChunkId, u64);

#[derive(Debug)]
pub(crate) struct HolderPartials {
    bodies: Mutex<HashMap<PartialKey, Vec<Weak<HolderPartial>>>>,
    changed: Arc<tokio::sync::Notify>,
    #[cfg(test)]
    queries: std::sync::atomic::AtomicUsize,
}

impl HolderPartials {
    pub(crate) fn new(changed: Arc<tokio::sync::Notify>) -> Self {
        Self {
            bodies: Mutex::new(HashMap::new()),
            changed,
            #[cfg(test)]
            queries: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    #[cfg(test)]
    pub(crate) fn queries_for_test(&self) -> usize {
        self.queries.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub(crate) fn register(
        self: &Arc<Self>,
        chunk: ChunkId,
        epoch: u64,
        table: Arc<CrcTable>,
        bytes: PartialBytes,
    ) -> RegisteredPartial {
        let key = (chunk, epoch);
        let body = Arc::new(HolderPartial {
            table,
            bytes: Mutex::new(bytes),
        });
        self.bodies
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .entry(key.clone())
            .or_default()
            .push(Arc::downgrade(&body));
        RegisteredPartial {
            index: self.clone(),
            key,
            body,
        }
    }

    pub(crate) fn covering(
        &self,
        chunk: &ChunkId,
        epoch: u64,
        offset: u64,
        length: u64,
    ) -> Option<Arc<HolderPartial>> {
        #[cfg(test)]
        self.queries
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.bodies
            .lock()
            .ok()?
            .get(&(chunk.clone(), epoch))?
            .iter()
            .filter_map(Weak::upgrade)
            .find(|body| body.covers(offset, length))
    }
}

/// The assembler transfers this registration into the install job when it
/// freezes the allocation. Dropping either owner removes its entry.
pub(crate) struct RegisteredPartial {
    index: Arc<HolderPartials>,
    key: PartialKey,
    pub(crate) body: Arc<HolderPartial>,
}

impl Drop for RegisteredPartial {
    fn drop(&mut self) {
        let mut bodies = self
            .index
            .bodies
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(partials) = bodies.get_mut(&self.key) {
            partials.retain(|body| body.as_ptr() != Arc::as_ptr(&self.body));
            if partials.is_empty() {
                bodies.remove(&self.key);
            }
        }
        drop(bodies);
        self.index.changed.notify_waiters();
    }
}
