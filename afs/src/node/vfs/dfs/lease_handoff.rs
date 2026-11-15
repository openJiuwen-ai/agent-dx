use super::*;
use std::sync::{RwLock, RwLockReadGuard};

const BATCH_THRESHOLD: usize = 32;
const MAX_BATCH: usize = 256;
const MAX_IDLE_AGE: Duration = Duration::from_secs(1);
const RELEASE_BUDGET: Duration = Duration::from_millis(50);

struct LeaseReleaseBatch {
    operation: OperationId,
    leases: Vec<WriteLease>,
}

/// A release excludes admission on every selected inode from the last idle
/// check to Meta's reply. Shared admission guards preserve concurrent opens.
pub(super) struct LeaseHandoff {
    pub(super) enabled: bool,
    gates: [RwLock<()>; DFS_HANDLE_SHARDS],
    idle: Mutex<HashMap<InodeId, Instant>>,
    release: Mutex<()>,
    pending: Mutex<Option<LeaseReleaseBatch>>,
}

impl Default for LeaseHandoff {
    fn default() -> Self {
        Self {
            enabled: false,
            gates: std::array::from_fn(|_| RwLock::new(())),
            idle: Mutex::default(),
            release: Mutex::default(),
            pending: Mutex::default(),
        }
    }
}

impl DistributedFs {
    pub(crate) fn with_write_handoff_prototype(mut self, enabled: bool) -> Self {
        self.lease_handoff.enabled = enabled;
        self
    }

    pub(super) fn lease_admission(
        &self,
        inode_id: &InodeId,
    ) -> Result<Option<RwLockReadGuard<'_, ()>>> {
        if !self.lease_handoff.enabled {
            return Ok(None);
        }
        let gate = self.lease_handoff.gates[ShardedMap::<InodeId, ()>::shard(inode_id)]
            .read()
            .map_err(|_| unavailable("DFS lease admission is poisoned"))?;
        if self
            .lease_handoff
            .pending
            .lock()
            .map_err(|_| unavailable("DFS pending lease release is poisoned"))?
            .as_ref()
            .is_some_and(|batch| batch.leases.iter().any(|lease| lease.inode_id == *inode_id))
        {
            return Err(unavailable("DFS lease release is awaiting confirmation"));
        }
        Ok(Some(gate))
    }

    pub(super) fn queue_idle_lease(&self, inode_id: &InodeId) -> Result<()> {
        if !self.lease_handoff.enabled {
            return Ok(());
        }
        self.lease_handoff
            .idle
            .lock()
            .map_err(|_| unavailable("DFS idle lease queue is poisoned"))?
            .insert(inode_id.clone(), Instant::now());
        Ok(())
    }

    pub(super) fn cancel_idle_lease(&self, inode_id: &InodeId) -> Result<()> {
        if self.lease_handoff.enabled {
            self.lease_handoff
                .idle
                .lock()
                .map_err(|_| unavailable("DFS idle lease queue is poisoned"))?
                .remove(inode_id);
        }
        Ok(())
    }

    pub(crate) fn release_idle_leases(&self) -> Result<usize> {
        self.release_lease_batch(None)
    }

    pub(super) fn expedite_idle_lease(&self, inode_id: &InodeId, epoch: u64) -> Result<bool> {
        self.release_lease_batch(Some((inode_id, epoch)))
            .map(|released| released != 0)
    }

    fn release_lease_batch(&self, urgent: Option<(&InodeId, u64)>) -> Result<usize> {
        if !self.lease_handoff.enabled {
            return Ok(0);
        }
        let Ok(_release) = self.lease_handoff.release.try_lock() else {
            return Ok(0);
        };

        // After a lost reply, block admission on this batch and replay its
        // exact operation. No fresh request may revive the lease in between.
        let mut pending = self
            .lease_handoff
            .pending
            .lock()
            .map_err(|_| unavailable("DFS pending lease release is poisoned"))?;
        if let Some(batch) = pending.as_ref() {
            if urgent.is_some() {
                return Ok(0);
            }
            self.confirm_lease_release(batch)?;
            pending.take();
            return Ok(0);
        }
        drop(pending);

        let mut ids = {
            let mut idle = self
                .lease_handoff
                .idle
                .lock()
                .map_err(|_| unavailable("DFS idle lease queue is poisoned"))?;
            idle.retain(|_, since| {
                since.elapsed() < Duration::from_secs(crate::dfs::DFS_WRITE_LEASE_SECONDS)
            });
            let mut ids = idle
                .iter()
                .filter(|(_, since)| {
                    since.elapsed() >= MAX_IDLE_AGE
                        || (urgent.is_none() && idle.len() >= BATCH_THRESHOLD)
                })
                .map(|(id, since)| (id.clone(), *since))
                .collect::<Vec<_>>();
            ids.sort_unstable_by_key(|(_, since)| *since);
            ids.truncate(MAX_BATCH - usize::from(urgent.is_some()));
            ids.into_iter().map(|(id, _)| id).collect::<Vec<_>>()
        };
        if let Some((inode_id, _)) = urgent {
            ids.retain(|id| id != inode_id);
            ids.insert(0, inode_id.clone());
        }

        let mut guards = std::array::from_fn::<_, DFS_HANDLE_SHARDS, _>(|_| None);
        let mut leases = Vec::new();
        let mut remove = Vec::new();
        for (i, id) in ids.iter().enumerate() {
            let urgent_epoch = urgent
                .filter(|(urgent_id, _)| *urgent_id == id)
                .map(|(_, epoch)| epoch);
            let shard = ShardedMap::<InodeId, ()>::shard(id);
            if guards[shard].is_none() {
                guards[shard] = self.lease_handoff.gates[shard].try_write().ok();
            }
            if guards[shard].is_some()
                && let Some(lease) = self.release_candidate(id, urgent_epoch)?
            {
                leases.push(lease);
                remove.push(id);
            }
            if i == 0 && urgent.is_some() && leases.is_empty() {
                return Ok(0);
            }
        }
        if leases.is_empty() {
            return Ok(0);
        }
        {
            let mut idle = self
                .lease_handoff
                .idle
                .lock()
                .map_err(|_| unavailable("DFS idle lease queue is poisoned"))?;
            for id in remove {
                idle.remove(id);
            }
        }
        let batch = LeaseReleaseBatch {
            operation: self.identity.operation_id("release-write-leases"),
            leases,
        };
        match self.confirm_lease_release(&batch) {
            Ok(count) => Ok(count),
            Err(error) => {
                *self
                    .lease_handoff
                    .pending
                    .lock()
                    .map_err(|_| unavailable("DFS pending lease release is poisoned"))? =
                    Some(batch);
                Err(error)
            }
        }
    }

    fn release_candidate(&self, id: &InodeId, epoch: Option<u64>) -> Result<Option<WriteLease>> {
        let Some(cell) = self.write_state(id)? else {
            return Ok(None);
        };
        let Ok(state) = cell.try_lock() else {
            return Ok(None);
        };
        let lease = &state.write_lease;
        if state.open_writers != 0
            || state.dirty
            || state.metadata_dirty
            || state.kill_suidgid_dirty
            || state.in_flight.is_some()
            || state.commit_busy
            || state.operation_busy
            || state.terminal_error.is_some()
            || lazy_fill::sealed(&state.inode)
            || self.ensure_local_write_owner(lease).is_err()
            || lease.expires_at_unix_ms <= now_unix_ms()
            || epoch.is_some_and(|epoch| epoch != lease.lease_epoch)
            || self
                .locks
                .authorities
                .read(id)
                .map_err(|_| unavailable("DFS lock authority table is poisoned"))?
                .contains_key(id)
        {
            return Ok(None);
        }
        Ok(Some(lease.clone()))
    }

    fn confirm_lease_release(&self, batch: &LeaseReleaseBatch) -> Result<usize> {
        let released = crate::node::call_deadline::within_timeout(RELEASE_BUDGET, || {
            self.meta
                .release_write_leases(&batch.operation, &batch.leases)
        })?;
        for lease in &batch.leases {
            if let Some(cell) = self.write_state(&lease.inode_id)? {
                let mut state = cell
                    .lock()
                    .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
                if state.write_lease == *lease {
                    state.write_lease.expires_at_unix_ms = 0;
                }
            }
        }
        Ok(released as usize)
    }
}
