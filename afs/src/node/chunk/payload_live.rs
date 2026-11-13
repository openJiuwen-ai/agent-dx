//! DFS write-pipeline payload allocation ownership (native and S3 storage).

use std::{
    sync::{Arc, Condvar, Mutex},
    time::{Duration, Instant},
};

use afs_error::{Error, Result};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct DirtyBudgetSnapshot {
    pub(crate) limit: usize,
    pub(crate) live_bytes: usize,
    pub(crate) live_peak_bytes: usize,
    pub(crate) leases: usize,
    pub(crate) waiters: usize,
    pub(crate) epoch: u64,
    pub(crate) stopped: bool,
}

/// Failed admission carries the epoch observed under the same ledger lock.
#[derive(Debug)]
pub(crate) enum DirtyAdmission {
    Reserved(DirtyLease),
    Wait { epoch: u64 },
}

#[derive(Debug)]
pub(crate) struct DirtyBudget {
    limit: usize,
    state: Mutex<DirtyBudgetSnapshot>,
    changed: Condvar,
}

impl DirtyBudget {
    pub(crate) fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            limit,
            state: Mutex::new(DirtyBudgetSnapshot {
                limit,
                ..DirtyBudgetSnapshot::default()
            }),
            changed: Condvar::new(),
        })
    }

    /// Unowned admission is retained for isolated allocation-owner tests. Native
    /// VFS writes use try_reserve_for so the same final owner charges both caps.
    #[cfg(test)]
    pub(crate) fn try_reserve(self: &Arc<Self>, bytes: usize) -> Result<DirtyAdmission> {
        self.reserve(None, bytes, usize::MAX)
    }

    pub(crate) fn try_reserve_for(
        self: &Arc<Self>,
        owner: Arc<DirtyOwner>,
        bytes: usize,
        owner_limit: usize,
    ) -> Result<DirtyAdmission> {
        self.reserve(Some(owner), bytes, owner_limit)
    }

    // Lock order is always Node ledger -> optional inode owner. Owner snapshots
    // never acquire the Node ledger; no map or caller-supplied inode key exists.
    fn reserve(
        self: &Arc<Self>,
        owner: Option<Arc<DirtyOwner>>,
        bytes: usize,
        owner_limit: usize,
    ) -> Result<DirtyAdmission> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.stopped {
            return Err(Error::coded(
                afs_error::IO_UNAVAILABLE,
                "native write admission is stopped",
            ));
        }
        if bytes > self.limit || bytes > owner_limit {
            return Err(Error::coded(
                afs_error::IO_CAPACITY,
                "native allocation exceeds live admission limit",
            ));
        }
        let mut owner_state = owner.as_ref().map(|owner| {
            owner
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
        });
        if bytes > self.limit.saturating_sub(state.live_bytes)
            || owner_state
                .as_ref()
                .is_some_and(|owner| bytes > owner_limit.saturating_sub(owner.live_bytes))
        {
            return Ok(DirtyAdmission::Wait { epoch: state.epoch });
        }
        state.live_bytes += bytes;
        state.live_peak_bytes = state.live_peak_bytes.max(state.live_bytes);
        state.leases += 1;
        if let Some(owner) = &mut owner_state {
            if owner.live_bytes == 0 {
                owner.held_since = Some(Instant::now());
            }
            owner.live_bytes += bytes;
            owner.live_peak_bytes = owner.live_peak_bytes.max(owner.live_bytes);
        }
        drop(owner_state);
        Ok(DirtyAdmission::Reserved(DirtyLease {
            budget: self.clone(),
            owner,
            owner_limit,
            bytes,
        }))
    }

    pub(crate) fn snapshot(&self) -> DirtyBudgetSnapshot {
        *self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// The epoch check and sleep share one lock: a release between admission
    /// failure and this call is observed without sleeping for another release.
    /// The caller must recheck its own stop/deadline/fairness conditions.
    pub(crate) fn wait_for_change(
        &self,
        epoch: u64,
        maximum_wait: Duration,
    ) -> DirtyBudgetSnapshot {
        let deadline = Instant::now().checked_add(maximum_wait);
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while state.epoch == epoch && !state.stopped {
            let Some(remaining) =
                deadline.and_then(|end| end.checked_duration_since(Instant::now()))
            else {
                break;
            };
            state.waiters += 1;
            let (next, _) = self
                .changed
                .wait_timeout(state, remaining)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state = next;
            state.waiters -= 1;
        }
        *state
    }

    /// Refuse new owners and wake admission waiters. Existing owners retain
    /// their charge until their final allocation owner really retires.
    pub(crate) fn stop(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.stopped = true;
        state.epoch = state.epoch.wrapping_add(1);
        self.changed.notify_all();
    }
}

/// Shared only by VFS state for one inode. Its lifetime and small metadata are
/// part of the VFS descriptor ledger, not a new Node-global inode index.
#[derive(Debug, Default)]
pub(crate) struct DirtyOwner {
    state: Mutex<DirtyOwnerSnapshot>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct DirtyOwnerSnapshot {
    pub(crate) live_bytes: usize,
    pub(crate) live_peak_bytes: usize,
    /// When this owner last went from holding nothing to holding budget.
    pub(crate) held_since: Option<Instant>,
}

impl DirtyOwnerSnapshot {
    fn release(&mut self, bytes: usize) {
        self.live_bytes -= bytes;
        if self.live_bytes == 0 {
            self.held_since = None;
        }
    }
}

impl DirtyOwner {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Since when this owner has continuously held budget; `None` while it
    /// holds none. Orders owners by age for early commit under pressure (R91).
    pub(crate) fn held_since(&self) -> Option<Instant> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .held_since
    }

    #[cfg(test)]
    pub(crate) fn snapshot(&self) -> DirtyOwnerSnapshot {
        *self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Not Clone: this single charge moves into the final ChunkBuffer. Immutable
/// shared slices retain that buffer's Arc rather than duplicating this lease.
#[derive(Debug)]
pub(crate) struct DirtyLease {
    budget: Arc<DirtyBudget>,
    owner: Option<Arc<DirtyOwner>>,
    owner_limit: usize,
    bytes: usize,
}

impl DirtyLease {
    pub(crate) fn charged_bytes(&self) -> usize {
        self.bytes
    }

    /// Complete a previously admitted checkout, including after stop. Never
    /// wait or exceed either accepted cap; false means do not expose the body.
    pub(crate) fn try_grow_to(&mut self, capacity: usize) -> Result<bool> {
        if capacity <= self.bytes {
            return Ok(true);
        }
        if capacity > self.budget.limit || capacity > self.owner_limit {
            return Err(Error::coded(
                afs_error::IO_CAPACITY,
                "native actual capacity exceeds admission limit",
            ));
        }
        let mut state = self
            .budget
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut owner_state = self.owner.as_ref().map(|owner| {
            owner
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
        });
        let additional = capacity - self.bytes;
        if additional > self.budget.limit.saturating_sub(state.live_bytes)
            || owner_state
                .as_ref()
                .is_some_and(|owner| additional > self.owner_limit.saturating_sub(owner.live_bytes))
        {
            return Ok(false);
        }
        state.live_bytes += additional;
        state.live_peak_bytes = state.live_peak_bytes.max(state.live_bytes);
        if let Some(owner) = &mut owner_state {
            owner.live_bytes += additional;
            owner.live_peak_bytes = owner.live_peak_bytes.max(owner.live_bytes);
        }
        self.bytes = capacity;
        Ok(true)
    }

    pub(super) fn shrink_to(&mut self, capacity: usize) {
        if capacity >= self.bytes {
            return;
        }
        let mut state = self
            .budget
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let released = self.bytes - capacity;
        state.live_bytes -= released;
        if let Some(owner) = &self.owner {
            let mut owner = owner
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            owner.release(released);
        }
        self.bytes = capacity;
        state.epoch = state.epoch.wrapping_add(1);
        self.budget.changed.notify_all();
    }
}

impl Drop for DirtyLease {
    fn drop(&mut self) {
        let mut state = self
            .budget
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.live_bytes -= self.bytes;
        state.leases -= 1;
        if let Some(owner) = &self.owner {
            let mut owner = owner
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            owner.release(self.bytes);
        }
        state.epoch = state.epoch.wrapping_add(1);
        self.budget.changed.notify_all();
    }
}
