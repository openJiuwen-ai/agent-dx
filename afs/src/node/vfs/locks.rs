//! Shared advisory-lock core for OwnerFs and DFS.
//!
//! The table is intentionally inode-local: callers choose the authority inode
//! identity and keep one table per Home/inode owner.  POSIX byte-range locks and
//! BSD flock locks use separate conflict namespaces, matching Linux behaviour.

use std::{
    collections::{HashMap, HashSet},
    fmt,
    sync::{Condvar, Mutex},
};

use super::types::{FileLockConflict, FileLockKind, FileLockOwner, FileLockRange, FileLockType};

/// A blocking lock request identity supplied by the FUSE/RPC layer.
///
/// Kernel request ids are scoped to one FUSE connection.  The ingress session
/// keeps cancellation from one mount or peer from matching another mount that
/// reused the same numeric request id.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct LockWaiterId {
    pub ingress_session_id: String,
    pub request_id: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LockWaiterOutcome {
    Unknown,
    Cancelled,
    Granted,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LockError {
    WouldBlock,
    Deadlock,
    Interrupted,
    Capacity,
    DuplicateWaiter,
    InvalidRange,
    Poisoned,
}

impl LockError {
    pub fn errno(&self) -> i32 {
        match self {
            Self::WouldBlock => libc::EAGAIN,
            Self::Deadlock => libc::EDEADLK,
            Self::Interrupted => libc::EINTR,
            Self::Capacity => libc::ENOLCK,
            Self::DuplicateWaiter => libc::EINVAL,
            Self::InvalidRange => libc::EINVAL,
            Self::Poisoned => libc::EIO,
        }
    }
}

impl fmt::Display for LockError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::WouldBlock => "file lock would block",
            Self::Deadlock => "file lock would deadlock",
            Self::Interrupted => "file lock wait was interrupted",
            Self::Capacity => "file lock table is full",
            Self::DuplicateWaiter => "duplicate live file lock waiter id",
            Self::InvalidRange => "invalid file lock range",
            Self::Poisoned => "file lock table is poisoned",
        })
    }
}

impl std::error::Error for LockError {}

/// Bounded per-inode lock table.
///
/// `max_locks` bounds active merged lock segments. `max_waiters` bounds blocked
/// `F_SETLKW` requests. `max_cancelled_waiters` bounds terminal outcomes and
/// exact retired identities together. Cancellation is keyed by `LockWaiterId`,
/// including interrupts arriving before registration. Replay fences survive
/// owner release and are reclaimed only at session close or invalidation.
pub struct LockTable {
    limits: LockTableLimits,
    state: Mutex<LockTableState>,
    cv: Condvar,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LockTableLimits {
    pub max_locks: usize,
    pub max_waiters: usize,
    pub max_cancelled_waiters: usize,
}

impl Default for LockTableLimits {
    fn default() -> Self {
        Self {
            max_locks: 8192,
            max_waiters: 1024,
            max_cancelled_waiters: 4096,
        }
    }
}

#[derive(Default)]
struct LockTableState {
    locks: Vec<ActiveLock>,
    next_seq: u64,
    waiters: HashMap<LockWaiterId, WaitingLock>,
    waiter_outcomes: HashMap<LockWaiterId, StoredWaiterOutcome>,
    retired_waiters: HashMap<LockWaiterId, RetiredWaiter>,
    closed_sessions: HashSet<String>,
    closed_session_admission_closed: bool,
    invalidated: bool,
}

#[derive(Clone, Debug)]
struct ActiveLock {
    kind: FileLockKind,
    owner: FileLockOwner,
    pid: u32,
    range: FileLockRange,
    lock_type: FileLockType,
    seq: u64,
}

#[derive(Clone, Debug)]
struct WaitingLock {
    owner: FileLockOwner,
    kind: FileLockKind,
    blockers: Vec<FileLockOwner>,
}

#[derive(Clone, Debug)]
struct StoredWaiterOutcome {
    outcome: LockWaiterOutcome,
    owner: Option<FileLockOwner>,
    kind: Option<FileLockKind>,
}

#[derive(Clone, Debug)]
struct RetiredWaiter;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LockRequest {
    pub kind: FileLockKind,
    pub owner: FileLockOwner,
    pub pid: u32,
    pub range: FileLockRange,
    pub lock_type: FileLockType,
}

impl LockRequest {
    pub fn read(kind: FileLockKind, owner: FileLockOwner, pid: u32, range: FileLockRange) -> Self {
        Self {
            kind,
            owner,
            pid,
            range,
            lock_type: FileLockType::Read,
        }
    }

    pub fn write(kind: FileLockKind, owner: FileLockOwner, pid: u32, range: FileLockRange) -> Self {
        Self {
            kind,
            owner,
            pid,
            range,
            lock_type: FileLockType::Write,
        }
    }

    pub fn unlock(
        kind: FileLockKind,
        owner: FileLockOwner,
        pid: u32,
        range: FileLockRange,
    ) -> Self {
        Self {
            kind,
            owner,
            pid,
            range,
            lock_type: FileLockType::Unlock,
        }
    }
}

impl Default for LockTable {
    fn default() -> Self {
        Self::new(LockTableLimits::default())
    }
}

impl LockTable {
    pub fn new(limits: LockTableLimits) -> Self {
        Self {
            limits,
            state: Mutex::new(LockTableState::default()),
            cv: Condvar::new(),
        }
    }

    pub fn getlk(&self, request: &LockRequest) -> Result<Option<FileLockConflict>, LockError> {
        validate_range(request.range)?;
        let state = self.state.lock().map_err(|_| LockError::Poisoned)?;
        if state.invalidated {
            return Err(LockError::Interrupted);
        }
        Ok(first_conflict(&state.locks, request).map(|lock| lock.conflict()))
    }

    pub fn setlk_nonblocking(&self, request: LockRequest) -> Result<(), LockError> {
        validate_range(request.range)?;
        let mut state = self.state.lock().map_err(|_| LockError::Poisoned)?;
        if state.invalidated {
            return Err(LockError::Interrupted);
        }
        if request.lock_type != FileLockType::Unlock {
            if state
                .closed_sessions
                .contains(&request.owner.ingress_session_id)
            {
                return Err(LockError::Interrupted);
            }
            if state.closed_session_admission_closed {
                return Err(LockError::Capacity);
            }
        }
        if request.lock_type != FileLockType::Unlock
            && first_conflict(&state.locks, &request).is_some()
        {
            return Err(LockError::WouldBlock);
        }
        apply_lock(&mut state, &self.limits, request)?;
        self.cv.notify_all();
        Ok(())
    }

    pub fn setlk_blocking(
        &self,
        request: LockRequest,
        waiter_id: LockWaiterId,
    ) -> Result<(), LockError> {
        validate_range(request.range)?;
        let mut state = self.state.lock().map_err(|_| LockError::Poisoned)?;
        if state.invalidated {
            return Err(LockError::Interrupted);
        }
        if request.lock_type != FileLockType::Unlock {
            if state
                .closed_sessions
                .contains(&request.owner.ingress_session_id)
                || state
                    .closed_sessions
                    .contains(&waiter_id.ingress_session_id)
            {
                return Err(LockError::Interrupted);
            }
            if state.closed_session_admission_closed {
                return Err(LockError::Capacity);
            }
        }
        match state
            .waiter_outcomes
            .get(&waiter_id)
            .map(|stored| stored.outcome)
        {
            Some(LockWaiterOutcome::Cancelled | LockWaiterOutcome::Unknown) => {
                return Err(LockError::Interrupted);
            }
            Some(LockWaiterOutcome::Granted) => return Err(LockError::DuplicateWaiter),
            None if is_waiter_retired(&state, &waiter_id) => return Err(LockError::Interrupted),
            None => {}
        }
        let mut registered = false;
        loop {
            if !registered && state.waiters.contains_key(&waiter_id) {
                return Err(LockError::DuplicateWaiter);
            }
            if request.lock_type == FileLockType::Unlock
                || first_conflict(&state.locks, &request).is_none()
            {
                if registered {
                    state.waiters.remove(&waiter_id);
                }
                ensure_waiter_outcome_capacity(&state, &self.limits, &waiter_id)?;
                let owner = request.owner.clone();
                let kind = request.kind;
                apply_lock(&mut state, &self.limits, request)?;
                state.waiter_outcomes.insert(
                    waiter_id.clone(),
                    StoredWaiterOutcome {
                        outcome: LockWaiterOutcome::Granted,
                        owner: Some(owner),
                        kind: Some(kind),
                    },
                );
                self.cv.notify_all();
                return Ok(());
            }

            let blockers = blocking_owners(&state.locks, &request);
            if would_deadlock(&state.waiters, &request.owner, &blockers) {
                if registered {
                    state.waiters.remove(&waiter_id);
                }
                return Err(LockError::Deadlock);
            }
            if !registered {
                if state.waiters.len() >= self.limits.max_waiters {
                    return Err(LockError::Capacity);
                }
                state.waiters.insert(
                    waiter_id.clone(),
                    WaitingLock {
                        owner: request.owner.clone(),
                        kind: request.kind,
                        blockers,
                    },
                );
                registered = true;
            } else if let Some(waiter) = state.waiters.get_mut(&waiter_id) {
                waiter.blockers = blockers;
            }

            state = self.cv.wait(state).map_err(|_| LockError::Poisoned)?;
            if state.invalidated
                || state
                    .waiter_outcomes
                    .get(&waiter_id)
                    .is_some_and(|stored| stored.outcome == LockWaiterOutcome::Cancelled)
                || state
                    .closed_sessions
                    .contains(&request.owner.ingress_session_id)
                || state
                    .closed_sessions
                    .contains(&waiter_id.ingress_session_id)
                || !state.waiters.contains_key(&waiter_id)
            {
                if registered {
                    state.waiters.remove(&waiter_id);
                }
                return Err(LockError::Interrupted);
            }
        }
    }

    /// Cancels a blocking waiter and reports whether the original request was
    /// still pending, had already completed, or is unknown. Terminal outcomes
    /// are retained until explicit ack or lifecycle cleanup so a lost ACK cannot
    /// be mistaken for "no lock".
    pub fn cancel_waiter_with_outcome(
        &self,
        waiter_id: LockWaiterId,
    ) -> Result<LockWaiterOutcome, LockError> {
        let mut state = self.state.lock().map_err(|_| LockError::Poisoned)?;
        if state.invalidated {
            self.cv.notify_all();
            return Ok(LockWaiterOutcome::Unknown);
        }
        if let Some(waiting) = state.waiters.get(&waiter_id).cloned() {
            ensure_waiter_outcome_capacity(&state, &self.limits, &waiter_id)?;
            state.waiters.remove(&waiter_id);
            remember_waiter_outcome(
                &mut state,
                &self.limits,
                waiter_id,
                LockWaiterOutcome::Cancelled,
                Some(waiting.owner),
                Some(waiting.kind),
            )?;
            self.cv.notify_all();
            return Ok(LockWaiterOutcome::Cancelled);
        }
        if let Some(stored) = state.waiter_outcomes.get(&waiter_id) {
            return Ok(stored.outcome);
        }
        if is_waiter_retired(&state, &waiter_id) {
            return Ok(LockWaiterOutcome::Unknown);
        }
        if state
            .closed_sessions
            .contains(&waiter_id.ingress_session_id)
            || state.closed_session_admission_closed
        {
            self.cv.notify_all();
            return Ok(LockWaiterOutcome::Unknown);
        }
        remember_waiter_outcome(
            &mut state,
            &self.limits,
            waiter_id,
            LockWaiterOutcome::Cancelled,
            None,
            None,
        )?;
        self.cv.notify_all();
        Ok(LockWaiterOutcome::Unknown)
    }

    /// Test convenience wrapper; production callers use `cancel_waiter_with_outcome`.
    #[cfg(test)]
    pub fn cancel_waiter(&self, waiter_id: LockWaiterId) -> Result<(), LockError> {
        self.cancel_waiter_with_outcome(waiter_id).map(|_| ())
    }

    /// Acknowledges delivery of a terminal outcome and replaces it with an
    /// exact replay fence in the same bounded budget. Active locks are unchanged.
    pub fn acknowledge_waiter(
        &self,
        waiter_id: &LockWaiterId,
    ) -> Result<Option<LockWaiterOutcome>, LockError> {
        let mut state = self.state.lock().map_err(|_| LockError::Poisoned)?;
        let Some(stored) = state.waiter_outcomes.remove(waiter_id) else {
            return Ok(None);
        };
        let outcome = stored.outcome;
        state
            .retired_waiters
            .insert(waiter_id.clone(), RetiredWaiter);
        Ok(Some(outcome))
    }

    /// POSIX locks are process-owner locks: Linux releases them when any fd for
    /// the inode owned by that process is closed.  The file handle is therefore
    /// deliberately not part of `FileLockOwner`.
    pub fn release_posix_owner(&self, owner: &FileLockOwner) -> Result<(), LockError> {
        let mut state = self.state.lock().map_err(|_| LockError::Poisoned)?;
        let before = state.locks.len();
        state
            .locks
            .retain(|lock| !(lock.kind == FileLockKind::Posix && lock.owner == *owner));
        retain_waiter_outcomes(&mut state, owner, Some(FileLockKind::Posix));
        if state.locks.len() != before {
            self.cv.notify_all();
        }
        Ok(())
    }

    /// Flock locks are open-file-description locks.  The caller invokes this
    /// only when the final OFD reference is released.
    pub fn release_flock_owner(&self, owner: &FileLockOwner) -> Result<(), LockError> {
        let mut state = self.state.lock().map_err(|_| LockError::Poisoned)?;
        let before = state.locks.len();
        state
            .locks
            .retain(|lock| !(lock.kind == FileLockKind::Flock && lock.owner == *owner));
        retain_waiter_outcomes(&mut state, owner, Some(FileLockKind::Flock));
        if state.locks.len() != before {
            self.cv.notify_all();
        }
        Ok(())
    }

    /// Drop every active lock and blocked waiter that belongs to an ingress
    /// session. This is used when a FUSE mount or peer session exits: removing
    /// waiter records without notification would leave blocked threads asleep in
    /// the condition variable wait loop.
    pub fn is_idle(&self) -> Result<bool, LockError> {
        let state = self.state.lock().map_err(|_| LockError::Poisoned)?;
        Ok(state.locks.is_empty()
            && state.waiters.is_empty()
            && state.waiter_outcomes.is_empty()
            && state.retired_waiters.is_empty()
            && !state.closed_session_admission_closed)
    }

    pub fn has_owner_activity(
        &self,
        owner: &FileLockOwner,
        kind: Option<FileLockKind>,
    ) -> Result<bool, LockError> {
        let state = self.state.lock().map_err(|_| LockError::Poisoned)?;
        Ok(state
            .locks
            .iter()
            .any(|lock| lock.owner == *owner && kind.is_none_or(|kind| lock.kind == kind))
            || state.waiters.values().any(|waiter| {
                waiter.owner == *owner && kind.is_none_or(|kind| waiter.kind == kind)
            }))
    }

    pub fn invalidate(&self) -> Result<(), LockError> {
        let mut state = self.state.lock().map_err(|_| LockError::Poisoned)?;
        state.invalidated = true;
        state.locks.clear();
        state.waiters.clear();
        state.waiter_outcomes.clear();
        state.retired_waiters.clear();
        state.closed_sessions.clear();
        state.closed_session_admission_closed = false;
        self.cv.notify_all();
        Ok(())
    }

    pub fn release_session(&self, ingress_session_id: &str) -> Result<(), LockError> {
        let mut state = self.state.lock().map_err(|_| LockError::Poisoned)?;
        if state.invalidated {
            return Ok(());
        }
        let before_locks = state.locks.len();
        let before_waiters = state.waiters.len();
        state
            .locks
            .retain(|lock| lock.owner.ingress_session_id != ingress_session_id);
        state.waiters.retain(|waiter_id, waiter| {
            waiter_id.ingress_session_id != ingress_session_id
                && waiter.owner.ingress_session_id != ingress_session_id
        });
        state
            .waiter_outcomes
            .retain(|waiter_id, _| waiter_id.ingress_session_id != ingress_session_id);
        state
            .retired_waiters
            .retain(|waiter_id, _| waiter_id.ingress_session_id != ingress_session_id);
        if !state.closed_sessions.contains(ingress_session_id) {
            if self.limits.max_cancelled_waiters == 0
                || state.closed_sessions.len() >= self.limits.max_cancelled_waiters
            {
                state.closed_session_admission_closed = true;
            } else {
                state.closed_sessions.insert(ingress_session_id.to_owned());
            }
        }
        if state.locks.len() != before_locks || state.waiters.len() != before_waiters {
            self.cv.notify_all();
        }
        Ok(())
    }

    #[cfg(test)]
    fn active_locks(&self) -> Vec<FileLockConflict> {
        let mut locks: Vec<_> = self
            .state
            .lock()
            .unwrap()
            .locks
            .iter()
            .map(ActiveLock::conflict)
            .collect();
        locks.sort_by_key(|lock| (lock.range.start, lock.range.end, lock.pid));
        locks
    }
}

fn is_waiter_retired(state: &LockTableState, waiter_id: &LockWaiterId) -> bool {
    state.retired_waiters.contains_key(waiter_id)
}

fn retain_waiter_outcomes(
    state: &mut LockTableState,
    owner: &FileLockOwner,
    kind: Option<FileLockKind>,
) {
    for stored in state.waiter_outcomes.values_mut() {
        if stored.outcome == LockWaiterOutcome::Granted
            && stored.owner.as_ref() == Some(owner)
            && (kind.is_none() || stored.kind == kind)
        {
            stored.outcome = LockWaiterOutcome::Cancelled;
        }
    }
}

fn ensure_waiter_outcome_capacity(
    state: &LockTableState,
    limits: &LockTableLimits,
    waiter_id: &LockWaiterId,
) -> Result<(), LockError> {
    if !state.waiter_outcomes.contains_key(waiter_id)
        && state.waiter_outcomes.len() + state.retired_waiters.len() >= limits.max_cancelled_waiters
    {
        return Err(LockError::Capacity);
    }
    Ok(())
}

fn remember_waiter_outcome(
    state: &mut LockTableState,
    limits: &LockTableLimits,
    waiter_id: LockWaiterId,
    outcome: LockWaiterOutcome,
    owner: Option<FileLockOwner>,
    kind: Option<FileLockKind>,
) -> Result<(), LockError> {
    ensure_waiter_outcome_capacity(state, limits, &waiter_id)?;
    state.waiter_outcomes.insert(
        waiter_id,
        StoredWaiterOutcome {
            outcome,
            owner,
            kind,
        },
    );
    Ok(())
}

fn validate_range(range: FileLockRange) -> Result<(), LockError> {
    if !range.is_valid() {
        return Err(LockError::InvalidRange);
    }
    Ok(())
}

fn first_conflict<'a>(locks: &'a [ActiveLock], request: &LockRequest) -> Option<&'a ActiveLock> {
    if request.lock_type == FileLockType::Unlock {
        return None;
    }
    locks
        .iter()
        .filter(|lock| conflicts(lock, request))
        .min_by_key(|lock| (lock.range.start, lock.seq))
}

fn blocking_owners(locks: &[ActiveLock], request: &LockRequest) -> Vec<FileLockOwner> {
    let mut owners = Vec::new();
    for lock in locks.iter().filter(|lock| conflicts(lock, request)) {
        if !owners.contains(&lock.owner) {
            owners.push(lock.owner.clone());
        }
    }
    owners
}

fn would_deadlock(
    waiters: &HashMap<LockWaiterId, WaitingLock>,
    owner: &FileLockOwner,
    blockers: &[FileLockOwner],
) -> bool {
    let mut visited = HashSet::new();
    blockers
        .iter()
        .any(|blocker| waits_for_owner(waiters, blocker, owner, &mut visited))
}

fn waits_for_owner(
    waiters: &HashMap<LockWaiterId, WaitingLock>,
    current: &FileLockOwner,
    target: &FileLockOwner,
    visited: &mut HashSet<FileLockOwner>,
) -> bool {
    if current == target {
        return true;
    }
    if !visited.insert(current.clone()) {
        return false;
    }
    waiters
        .values()
        .filter(|waiter| waiter.owner == *current)
        .flat_map(|waiter| waiter.blockers.iter())
        .any(|next| waits_for_owner(waiters, next, target, visited))
}

fn conflicts(lock: &ActiveLock, request: &LockRequest) -> bool {
    lock.kind == request.kind
        && lock.owner != request.owner
        && lock.range.overlaps(&request.range)
        && (lock.lock_type == FileLockType::Write || request.lock_type == FileLockType::Write)
}

fn apply_lock(
    state: &mut LockTableState,
    limits: &LockTableLimits,
    request: LockRequest,
) -> Result<(), LockError> {
    let mut candidate = state.locks.clone();
    trim_same_owner_range(&mut candidate, request.kind, &request.owner, request.range);
    let mut next_seq = state.next_seq;
    if request.lock_type != FileLockType::Unlock {
        let seq = next_seq;
        next_seq = next_seq.wrapping_add(1);
        candidate.push(ActiveLock {
            kind: request.kind,
            owner: request.owner,
            pid: request.pid,
            range: request.range,
            lock_type: request.lock_type,
            seq,
        });
        coalesce(&mut candidate);
    }
    if candidate.len() > limits.max_locks {
        return Err(LockError::Capacity);
    }
    state.locks = candidate;
    state.next_seq = next_seq;
    Ok(())
}

fn trim_same_owner_range(
    locks: &mut Vec<ActiveLock>,
    kind: FileLockKind,
    owner: &FileLockOwner,
    remove: FileLockRange,
) {
    let mut replacement = Vec::with_capacity(locks.len());
    for lock in locks.drain(..) {
        if lock.kind != kind || lock.owner != *owner || !lock.range.overlaps(&remove) {
            replacement.push(lock);
            continue;
        }
        if lock.range.start < remove.start {
            replacement.push(ActiveLock {
                range: FileLockRange {
                    start: lock.range.start,
                    end: remove.start.saturating_sub(1),
                },
                ..lock.clone()
            });
        }
        if lock.range.end > remove.end {
            replacement.push(ActiveLock {
                range: FileLockRange {
                    start: remove.end.saturating_add(1),
                    end: lock.range.end,
                },
                ..lock
            });
        }
    }
    *locks = replacement;
}

fn coalesce(locks: &mut Vec<ActiveLock>) {
    locks.sort_by(|a, b| {
        (
            a.kind,
            &a.owner,
            a.lock_type,
            a.range.start,
            a.range.end,
            a.seq,
        )
            .cmp(&(
                b.kind,
                &b.owner,
                b.lock_type,
                b.range.start,
                b.range.end,
                b.seq,
            ))
    });
    let mut merged: Vec<ActiveLock> = Vec::with_capacity(locks.len());
    for lock in locks.drain(..) {
        if let Some(last) = merged.last_mut()
            && last.kind == lock.kind
            && last.owner == lock.owner
            && last.lock_type == lock.lock_type
            && last.range.touches_or_overlaps(&lock.range)
        {
            last.range.end = last.range.end.max(lock.range.end);
            last.seq = last.seq.min(lock.seq);
            continue;
        }
        merged.push(lock);
    }
    *locks = merged;
}

impl ActiveLock {
    fn conflict(&self) -> FileLockConflict {
        FileLockConflict {
            kind: self.kind,
            owner: self.owner.clone(),
            pid: self.pid,
            range: self.range,
            lock_type: self.lock_type,
        }
    }
}

#[cfg(test)]
mod tests;
