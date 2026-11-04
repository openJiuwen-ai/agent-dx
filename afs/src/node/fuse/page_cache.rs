//! Page-cache identity tables and per-inode cache gates.

use crate::node::vfs::PageCacheIdentity;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::RwLock;

pub(super) const PAGE_CACHE_IDENTITIES: usize = 65_536;
pub(super) const PAGE_CACHE_VERSION_BYTES: usize = 1024 * 1024;
pub(super) const PAGE_CACHE_INODE_GATES: usize = 64;

#[derive(Default)]
pub(super) struct PageCacheIdentities {
    pub(super) entries: HashMap<u64, (PageCacheIdentity, u64)>,
    pub(super) version_bytes: usize,
    clock: u64,
}

impl PageCacheIdentities {
    pub(super) fn remove(&mut self, ino: u64) {
        if let Some((identity, _)) = self.entries.remove(&ino) {
            self.version_bytes -= identity.version_id.as_ref().map_or(0, String::len);
        }
    }

    pub(super) fn observe(&mut self, ino: u64, identity: Option<PageCacheIdentity>) -> bool {
        let Some(identity) = identity else {
            self.remove(ino);
            return false;
        };
        let bytes = identity.version_id.as_ref().map_or(0, String::len);
        if bytes > PAGE_CACHE_VERSION_BYTES
            || (identity.version_id.is_none() && identity.length != 0)
        {
            self.remove(ino);
            return false;
        }
        let keep = self
            .entries
            .get(&ino)
            .is_some_and(|(previous, _)| previous == &identity);
        self.remove(ino);
        while self.entries.len() >= PAGE_CACHE_IDENTITIES
            || self.version_bytes.saturating_add(bytes) > PAGE_CACHE_VERSION_BYTES
        {
            let need_entry = self.entries.len() >= PAGE_CACHE_IDENTITIES;
            let Some(oldest) = self
                .entries
                .iter()
                .filter(|(_, (entry, _))| {
                    need_entry || entry.version_id.as_ref().is_some_and(|id| !id.is_empty())
                })
                .min_by_key(|(_, (_, age))| *age)
                .map(|(ino, _)| *ino)
            else {
                break;
            };
            self.remove(oldest);
        }
        if self.clock == u64::MAX {
            // Forgetting an optimization is safe; wrapping an LRU age is not.
            self.entries.clear();
            self.version_bytes = 0;
            self.clock = 0;
        }
        self.clock += 1;
        self.version_bytes += bytes;
        self.entries.insert(ino, (identity, self.clock));
        keep
    }
}

/// One kernel inode has one data cache. Concurrent fixed-open views that
/// disagree must use direct IO instead of filling that cache with old bytes.
/// This is an admission table for live handles, not another payload cache.
#[derive(Default)]
pub(super) struct ActiveCachedViews {
    pub(super) inodes: HashMap<u64, (PageCacheIdentity, std::collections::HashSet<u64>)>,
    pub(super) handles: usize,
    pub(super) version_bytes: usize,
}

impl ActiveCachedViews {
    pub(super) fn admit(&mut self, ino: u64, fh: u64, identity: &PageCacheIdentity) -> bool {
        if self.handles >= PAGE_CACHE_IDENTITIES {
            return false;
        }
        if let Some((previous, handles)) = self.inodes.get_mut(&ino) {
            if previous != identity {
                return false;
            }
            self.handles += usize::from(handles.insert(fh));
            return true;
        }
        let bytes = identity.version_id.as_ref().map_or(0, String::len);
        if (identity.version_id.is_none() && identity.length != 0)
            || bytes > PAGE_CACHE_VERSION_BYTES.saturating_sub(self.version_bytes)
        {
            return false;
        }
        self.inodes.insert(ino, (identity.clone(), [fh].into()));
        self.handles += 1;
        self.version_bytes += bytes;
        true
    }

    pub(super) fn release(&mut self, ino: u64, fh: u64) {
        if let Some((_, handles)) = self.inodes.get_mut(&ino) {
            self.handles -= usize::from(handles.remove(&fh));
            if handles.is_empty()
                && let Some((identity, _)) = self.inodes.remove(&ino)
            {
                self.version_bytes -= identity.version_id.as_ref().map_or(0, String::len);
            }
        }
    }
}

/// Short shard-table locks only protect gate identity and reference retirement.
/// The per-inode lock, never the table lock, spans backend work and FUSE reply.
type GateShard = Arc<Mutex<HashMap<u64, Arc<RwLock<()>>>>>;

pub(super) struct InodeCacheGates {
    pub(super) shards: [GateShard; PAGE_CACHE_INODE_GATES],
}

impl Default for InodeCacheGates {
    fn default() -> Self {
        Self {
            shards: std::array::from_fn(|_| Arc::new(Mutex::new(HashMap::new()))),
        }
    }
}

impl InodeCacheGates {
    pub(super) fn acquire(&self, ino: u64) -> std::result::Result<InodeCacheGate, i32> {
        let shard = &self.shards[(ino % PAGE_CACHE_INODE_GATES as u64) as usize];
        let mut table = shard.lock().map_err(|_| libc::EIO)?;
        let gate = table
            .entry(ino)
            .or_insert_with(|| Arc::new(RwLock::new(())))
            .clone();
        Ok(InodeCacheGate {
            shard: shard.clone(),
            ino,
            gate: Some(gate),
        })
    }
}

/// Each lease owns one reference outside the table. It cannot be cloned:
/// acquisition and final retirement must both serialize through the table lock.
pub(super) struct InodeCacheGate {
    shard: GateShard,
    ino: u64,
    pub(super) gate: Option<Arc<RwLock<()>>>,
}

impl InodeCacheGate {
    pub(super) fn read(&self) -> std::result::Result<tokio::sync::RwLockReadGuard<'_, ()>, i32> {
        let gate = self.gate.as_ref().ok_or(libc::EIO)?;
        Ok(match gate.try_read() {
            Ok(guard) => guard,
            Err(_) => gate.blocking_read(),
        })
    }

    pub(super) fn write(&self) -> std::result::Result<tokio::sync::RwLockWriteGuard<'_, ()>, i32> {
        let gate = self.gate.as_ref().ok_or(libc::EIO)?;
        Ok(match gate.try_write() {
            Ok(guard) => guard,
            Err(_) => gate.blocking_write(),
        })
    }

    pub(super) async fn read_owned_until(
        &self,
        deadline: std::time::Instant,
    ) -> std::result::Result<tokio::sync::OwnedRwLockReadGuard<()>, i32> {
        let gate = self.gate.as_ref().ok_or(libc::EIO)?.clone();
        tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), gate.read_owned())
            .await
            .map_err(|_| libc::ETIMEDOUT)
    }
}

impl Drop for InodeCacheGate {
    fn drop(&mut self) {
        let Some(gate) = self.gate.take() else {
            return;
        };
        // Even a poisoned optimization table must release leases. No backend
        // state is protected here, and acquire still fails closed on poison.
        let mut table = self
            .shard
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let same_gate = table
            .get(&self.ino)
            .is_some_and(|current| Arc::ptr_eq(current, &gate));
        // Release this lease while holding the acquisition lock. Concurrent
        // last drops cannot both observe each other's still-live reference and
        // leave an idle entry forever. A waiting acquire cannot split identity.
        drop(gate);
        if same_gate
            && table
                .get(&self.ino)
                .is_some_and(|current| Arc::strong_count(current) == 1)
        {
            table.remove(&self.ino);
        }
    }
}
