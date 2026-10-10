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

    /// Compatibility wrapper for FUSE interrupts that only need errno mapping.
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
mod tests {
    use std::{
        sync::mpsc,
        time::{Duration, Instant},
    };

    use super::*;

    fn owner(session: &str, kernel_owner: u64) -> FileLockOwner {
        FileLockOwner {
            ingress_session_id: session.to_owned(),
            kernel_owner,
        }
    }

    fn range(start: u64, end: u64) -> FileLockRange {
        FileLockRange { start, end }
    }

    fn waiter_id(session: &str, request_id: u64) -> LockWaiterId {
        LockWaiterId {
            ingress_session_id: session.to_owned(),
            request_id,
        }
    }

    #[test]
    fn posix_and_flock_do_not_conflict() {
        let table = LockTable::default();
        table
            .setlk_nonblocking(LockRequest::write(
                FileLockKind::Posix,
                owner("a", 1),
                101,
                range(0, 99),
            ))
            .unwrap();

        table
            .setlk_nonblocking(LockRequest::write(
                FileLockKind::Flock,
                owner("b", 2),
                202,
                range(0, u64::MAX),
            ))
            .unwrap();
    }

    #[test]
    fn nonblocking_conflict_returns_would_block_and_first_conflict() {
        let table = LockTable::default();
        table
            .setlk_nonblocking(LockRequest::write(
                FileLockKind::Posix,
                owner("a", 1),
                101,
                range(50, 60),
            ))
            .unwrap();
        table
            .setlk_nonblocking(LockRequest::write(
                FileLockKind::Posix,
                owner("c", 3),
                303,
                range(10, 20),
            ))
            .unwrap();

        let request = LockRequest::read(FileLockKind::Posix, owner("b", 2), 202, range(0, 100));
        let conflict = table.getlk(&request).unwrap().unwrap();
        assert_eq!(conflict.pid, 303);
        assert_eq!(conflict.range, range(10, 20));

        let error = table.setlk_nonblocking(request).unwrap_err();
        assert_eq!(error, LockError::WouldBlock);
        assert_eq!(error.errno(), libc::EAGAIN);
    }

    #[test]
    fn same_owner_ranges_merge_split_and_unlock_at_eof() {
        let table = LockTable::default();
        let lock_owner = owner("a", 1);
        table
            .setlk_nonblocking(LockRequest::read(
                FileLockKind::Posix,
                lock_owner.clone(),
                101,
                range(0, 9),
            ))
            .unwrap();
        table
            .setlk_nonblocking(LockRequest::read(
                FileLockKind::Posix,
                lock_owner.clone(),
                101,
                range(10, u64::MAX),
            ))
            .unwrap();
        assert_eq!(table.active_locks().len(), 1);
        assert_eq!(table.active_locks()[0].range, range(0, u64::MAX));

        table
            .setlk_nonblocking(LockRequest::unlock(
                FileLockKind::Posix,
                lock_owner,
                101,
                range(20, u64::MAX),
            ))
            .unwrap();
        let locks = table.active_locks();
        assert_eq!(locks.len(), 1);
        assert_eq!(locks[0].range, range(0, 19));
    }

    #[test]
    fn upgrade_trims_only_same_owner_ranges() {
        let table = LockTable::default();
        let lock_owner = owner("a", 1);
        table
            .setlk_nonblocking(LockRequest::read(
                FileLockKind::Posix,
                lock_owner.clone(),
                101,
                range(0, 99),
            ))
            .unwrap();
        table
            .setlk_nonblocking(LockRequest::write(
                FileLockKind::Posix,
                lock_owner,
                101,
                range(40, 59),
            ))
            .unwrap();

        let locks = table.active_locks();
        assert_eq!(locks.len(), 3);
        assert_eq!(locks[0].range, range(0, 39));
        assert_eq!(locks[0].lock_type, FileLockType::Read);
        assert_eq!(locks[1].range, range(40, 59));
        assert_eq!(locks[1].lock_type, FileLockType::Write);
        assert_eq!(locks[2].range, range(60, 99));
        assert_eq!(locks[2].lock_type, FileLockType::Read);
    }

    #[test]
    fn partial_unlock_that_would_exceed_capacity_keeps_original_locks() {
        let table = LockTable::new(LockTableLimits {
            max_locks: 1,
            max_waiters: 16,
            max_cancelled_waiters: 16,
        });
        let lock_owner = owner("a", 1);
        table
            .setlk_nonblocking(LockRequest::write(
                FileLockKind::Posix,
                lock_owner.clone(),
                101,
                range(0, 99),
            ))
            .unwrap();

        let error = table
            .setlk_nonblocking(LockRequest::unlock(
                FileLockKind::Posix,
                lock_owner,
                101,
                range(40, 59),
            ))
            .unwrap_err();
        assert_eq!(error, LockError::Capacity);
        assert_eq!(error.errno(), libc::ENOLCK);
        let locks = table.active_locks();
        assert_eq!(locks.len(), 1);
        assert_eq!(locks[0].range, range(0, 99));
    }

    #[test]
    fn flush_owner_releases_all_posix_locks_without_flock() {
        let table = LockTable::default();
        let lock_owner = owner("a", 1);
        table
            .setlk_nonblocking(LockRequest::write(
                FileLockKind::Posix,
                lock_owner.clone(),
                101,
                range(0, 9),
            ))
            .unwrap();
        table
            .setlk_nonblocking(LockRequest::write(
                FileLockKind::Flock,
                lock_owner.clone(),
                101,
                range(0, u64::MAX),
            ))
            .unwrap();

        table.release_posix_owner(&lock_owner).unwrap();
        let locks = table.active_locks();
        assert_eq!(locks.len(), 1);
        assert_eq!(locks[0].kind, FileLockKind::Flock);
    }

    #[test]
    fn release_final_ofd_releases_flock_only() {
        let table = LockTable::default();
        let lock_owner = owner("a", 1);
        table
            .setlk_nonblocking(LockRequest::write(
                FileLockKind::Posix,
                lock_owner.clone(),
                101,
                range(0, 9),
            ))
            .unwrap();
        table
            .setlk_nonblocking(LockRequest::write(
                FileLockKind::Flock,
                lock_owner.clone(),
                101,
                range(0, u64::MAX),
            ))
            .unwrap();

        table.release_flock_owner(&lock_owner).unwrap();
        let locks = table.active_locks();
        assert_eq!(locks.len(), 1);
        assert_eq!(locks[0].kind, FileLockKind::Posix);
    }

    #[test]
    fn interrupt_before_register_is_observed() {
        let table = LockTable::default();
        let waiter = waiter_id("m1", 99);
        table.cancel_waiter(waiter.clone()).unwrap();
        let error = table
            .setlk_blocking(
                LockRequest::write(FileLockKind::Posix, owner("a", 1), 101, range(0, 9)),
                waiter,
            )
            .unwrap_err();
        assert_eq!(error, LockError::Interrupted);
        assert_eq!(error.errno(), libc::EINTR);
    }

    #[test]
    fn interrupt_after_register_wakes_blocking_waiter() {
        let table = LockTable::default();
        table
            .setlk_nonblocking(LockRequest::write(
                FileLockKind::Posix,
                owner("a", 1),
                101,
                range(0, 9),
            ))
            .unwrap();

        std::thread::scope(|scope| {
            let waiter = scope.spawn(|| {
                table.setlk_blocking(
                    LockRequest::write(FileLockKind::Posix, owner("b", 2), 202, range(0, 9)),
                    waiter_id("m1", 7),
                )
            });
            while table.state.lock().unwrap().waiters.is_empty() {
                std::thread::yield_now();
            }
            table.cancel_waiter(waiter_id("m1", 7)).unwrap();
            assert_eq!(waiter.join().unwrap().unwrap_err(), LockError::Interrupted);
        });
    }

    #[test]
    fn detects_simple_blocking_deadlock_cycle() {
        let table = LockTable::default();
        let owner_a = owner("a", 1);
        let owner_b = owner("b", 2);
        table
            .setlk_nonblocking(LockRequest::write(
                FileLockKind::Posix,
                owner_a.clone(),
                101,
                range(0, 9),
            ))
            .unwrap();
        table
            .setlk_nonblocking(LockRequest::write(
                FileLockKind::Posix,
                owner_b.clone(),
                202,
                range(10, 19),
            ))
            .unwrap();

        let table_for_waiter = &table;
        std::thread::scope(|scope| {
            let waiter = scope.spawn(|| {
                table_for_waiter.setlk_blocking(
                    LockRequest::write(FileLockKind::Posix, owner_a, 101, range(10, 19)),
                    waiter_id("m1", 1),
                )
            });
            while table.state.lock().unwrap().waiters.is_empty() {
                std::thread::yield_now();
            }
            let error = table
                .setlk_blocking(
                    LockRequest::write(FileLockKind::Posix, owner_b, 202, range(0, 9)),
                    waiter_id("m1", 2),
                )
                .unwrap_err();
            assert_eq!(error, LockError::Deadlock);
            table.cancel_waiter(waiter_id("m1", 1)).unwrap();
            assert_eq!(waiter.join().unwrap().unwrap_err(), LockError::Interrupted);
        });
    }

    #[test]
    fn detects_transitive_deadlock_cycle() {
        let table = LockTable::default();
        let owner_a = owner("a", 1);
        let owner_b = owner("b", 2);
        let owner_c = owner("c", 3);
        table
            .setlk_nonblocking(LockRequest::write(
                FileLockKind::Posix,
                owner_a.clone(),
                101,
                range(0, 9),
            ))
            .unwrap();
        table
            .setlk_nonblocking(LockRequest::write(
                FileLockKind::Posix,
                owner_b.clone(),
                202,
                range(10, 19),
            ))
            .unwrap();
        table
            .setlk_nonblocking(LockRequest::write(
                FileLockKind::Posix,
                owner_c.clone(),
                303,
                range(20, 29),
            ))
            .unwrap();

        std::thread::scope(|scope| {
            let waiter_a = scope.spawn(|| {
                table.setlk_blocking(
                    LockRequest::write(FileLockKind::Posix, owner_a, 101, range(10, 19)),
                    waiter_id("m1", 10),
                )
            });
            let waiter_b = scope.spawn(|| {
                table.setlk_blocking(
                    LockRequest::write(FileLockKind::Posix, owner_b, 202, range(20, 29)),
                    waiter_id("m1", 11),
                )
            });
            while table.state.lock().unwrap().waiters.len() < 2 {
                std::thread::yield_now();
            }
            let error = table
                .setlk_blocking(
                    LockRequest::write(FileLockKind::Posix, owner_c, 303, range(0, 9)),
                    waiter_id("m1", 12),
                )
                .unwrap_err();
            assert_eq!(error, LockError::Deadlock);
            table.cancel_waiter(waiter_id("m1", 10)).unwrap();
            table.cancel_waiter(waiter_id("m1", 11)).unwrap();
            assert_eq!(
                waiter_a.join().unwrap().unwrap_err(),
                LockError::Interrupted
            );
            assert_eq!(
                waiter_b.join().unwrap().unwrap_err(),
                LockError::Interrupted
            );
        });
    }

    #[test]
    fn cancelled_waiter_capacity_does_not_drop_existing_marker() {
        let table = LockTable::new(LockTableLimits {
            max_locks: 16,
            max_waiters: 16,
            max_cancelled_waiters: 1,
        });
        table.cancel_waiter(waiter_id("m1", 1)).unwrap();
        let error = table.cancel_waiter(waiter_id("m2", 1)).unwrap_err();
        assert_eq!(error, LockError::Capacity);

        let interrupted = table
            .setlk_blocking(
                LockRequest::write(FileLockKind::Posix, owner("a", 1), 101, range(0, 9)),
                waiter_id("m1", 1),
            )
            .unwrap_err();
        assert_eq!(interrupted, LockError::Interrupted);
        assert_eq!(
            table.acknowledge_waiter(&waiter_id("m1", 1)).unwrap(),
            Some(LockWaiterOutcome::Cancelled)
        );

        let capacity_after_ack = table
            .setlk_blocking(
                LockRequest::write(FileLockKind::Posix, owner("b", 2), 202, range(10, 19)),
                waiter_id("m2", 2),
            )
            .unwrap_err();
        assert_eq!(capacity_after_ack, LockError::Capacity);
        table.release_session("m1").unwrap();
        table
            .setlk_blocking(
                LockRequest::write(FileLockKind::Posix, owner("m2", 2), 202, range(10, 19)),
                waiter_id("m2", 2),
            )
            .unwrap();
    }

    #[test]
    fn waiter_id_includes_ingress_session() {
        let table = LockTable::default();
        table.cancel_waiter(waiter_id("m1", 7)).unwrap();
        table
            .setlk_blocking(
                LockRequest::write(FileLockKind::Posix, owner("m2", 2), 202, range(0, 9)),
                waiter_id("m2", 7),
            )
            .unwrap();
        let interrupted = table
            .setlk_blocking(
                LockRequest::write(FileLockKind::Posix, owner("m1", 1), 101, range(10, 19)),
                waiter_id("m1", 7),
            )
            .unwrap_err();
        assert_eq!(interrupted, LockError::Interrupted);
    }

    #[test]
    fn duplicate_live_waiter_id_is_rejected() {
        let table = LockTable::default();
        table
            .setlk_nonblocking(LockRequest::write(
                FileLockKind::Posix,
                owner("holder", 1),
                101,
                range(0, 9),
            ))
            .unwrap();
        std::thread::scope(|scope| {
            let blocking = scope.spawn(|| {
                table.setlk_blocking(
                    LockRequest::write(FileLockKind::Posix, owner("waiter", 2), 202, range(0, 9)),
                    waiter_id("m1", 42),
                )
            });
            while table.state.lock().unwrap().waiters.is_empty() {
                std::thread::yield_now();
            }
            let duplicate = table
                .setlk_blocking(
                    LockRequest::write(FileLockKind::Posix, owner("other", 3), 303, range(0, 9)),
                    waiter_id("m1", 42),
                )
                .unwrap_err();
            assert_eq!(duplicate, LockError::DuplicateWaiter);
            let nonconflicting_duplicate = table
                .setlk_blocking(
                    LockRequest::write(FileLockKind::Posix, owner("other", 3), 303, range(20, 29)),
                    waiter_id("m1", 42),
                )
                .unwrap_err();
            assert_eq!(nonconflicting_duplicate, LockError::DuplicateWaiter);
            table.cancel_waiter(waiter_id("m1", 42)).unwrap();
            assert_eq!(
                blocking.join().unwrap().unwrap_err(),
                LockError::Interrupted
            );
        });
    }

    #[test]
    fn bounded_waiters_fail_closed() {
        let table = LockTable::new(LockTableLimits {
            max_locks: 16,
            max_waiters: 0,
            max_cancelled_waiters: 16,
        });
        table
            .setlk_nonblocking(LockRequest::write(
                FileLockKind::Posix,
                owner("a", 1),
                101,
                range(0, 9),
            ))
            .unwrap();
        let error = table
            .setlk_blocking(
                LockRequest::write(FileLockKind::Posix, owner("b", 2), 202, range(0, 9)),
                waiter_id("m1", 1),
            )
            .unwrap_err();
        assert_eq!(error, LockError::Capacity);
        assert_eq!(error.errno(), libc::ENOLCK);
    }

    #[test]
    fn cancel_after_blocking_grant_reports_granted_without_releasing_owner() {
        let table = LockTable::default();
        let waiter = waiter_id("m1", 77);
        let lock_owner = owner("m1", 7);
        table
            .setlk_blocking(
                LockRequest::write(FileLockKind::Posix, lock_owner.clone(), 707, range(0, 9)),
                waiter.clone(),
            )
            .unwrap();

        assert_eq!(
            table.cancel_waiter_with_outcome(waiter.clone()).unwrap(),
            LockWaiterOutcome::Granted
        );
        assert_eq!(
            table.cancel_waiter_with_outcome(waiter.clone()).unwrap(),
            LockWaiterOutcome::Granted
        );
        assert!(
            table
                .has_owner_activity(&lock_owner, Some(FileLockKind::Posix))
                .unwrap()
        );
        assert_eq!(table.active_locks().len(), 1);

        assert_eq!(
            table.acknowledge_waiter(&waiter).unwrap(),
            Some(LockWaiterOutcome::Granted)
        );
        assert_eq!(
            table.cancel_waiter_with_outcome(waiter.clone()).unwrap(),
            LockWaiterOutcome::Unknown
        );
        assert!(table.active_locks().len() == 1);

        table.release_session("m1").unwrap();
        assert_eq!(
            table.cancel_waiter_with_outcome(waiter).unwrap(),
            LockWaiterOutcome::Unknown
        );
        assert!(table.active_locks().is_empty());
    }

    #[test]
    fn cancel_live_waiter_reports_cancelled_and_does_not_grant() {
        let table = LockTable::default();
        table
            .setlk_nonblocking(LockRequest::write(
                FileLockKind::Posix,
                owner("holder", 1),
                101,
                range(0, 9),
            ))
            .unwrap();

        std::thread::scope(|scope| {
            let waiter_id = waiter_id("m1", 78);
            let waiting_owner = owner("waiter", 2);
            let waiter = scope.spawn({
                let table_ref = &table;
                let waiter_id = waiter_id.clone();
                let waiting_owner = waiting_owner.clone();
                move || {
                    table_ref.setlk_blocking(
                        LockRequest::write(FileLockKind::Posix, waiting_owner, 202, range(0, 9)),
                        waiter_id,
                    )
                }
            });
            while table.state.lock().unwrap().waiters.is_empty() {
                std::thread::yield_now();
            }

            assert_eq!(
                table.cancel_waiter_with_outcome(waiter_id.clone()).unwrap(),
                LockWaiterOutcome::Cancelled
            );
            assert_eq!(waiter.join().unwrap().unwrap_err(), LockError::Interrupted);
            assert!(
                !table
                    .has_owner_activity(&waiting_owner, Some(FileLockKind::Posix))
                    .unwrap()
            );

            let replay = table
                .setlk_blocking(
                    LockRequest::write(
                        FileLockKind::Posix,
                        waiting_owner.clone(),
                        202,
                        range(20, 29),
                    ),
                    waiter_id.clone(),
                )
                .unwrap_err();
            assert_eq!(replay, LockError::Interrupted);
            assert_eq!(
                table.acknowledge_waiter(&waiter_id).unwrap(),
                Some(LockWaiterOutcome::Cancelled)
            );
            let old_replay = table
                .setlk_blocking(
                    LockRequest::write(
                        FileLockKind::Posix,
                        waiting_owner.clone(),
                        202,
                        range(20, 29),
                    ),
                    waiter_id.clone(),
                )
                .unwrap_err();
            assert_eq!(old_replay, LockError::Interrupted);

            let next_waiter_id = LockWaiterId {
                ingress_session_id: "m1".into(),
                request_id: 79,
            };
            table
                .setlk_blocking(
                    LockRequest::write(FileLockKind::Posix, waiting_owner, 202, range(20, 29)),
                    next_waiter_id.clone(),
                )
                .unwrap();
            assert_eq!(
                table.acknowledge_waiter(&next_waiter_id).unwrap(),
                Some(LockWaiterOutcome::Granted)
            );
        });
    }

    #[test]
    fn acknowledged_terminal_outcome_retains_replay_capacity_until_session_close() {
        let table = LockTable::new(LockTableLimits {
            max_locks: 16,
            max_waiters: 16,
            max_cancelled_waiters: 1,
        });
        let first = waiter_id("m1", 79);
        let first_owner = owner("m1", 79);
        let second = waiter_id("m1", 81);
        table
            .setlk_blocking(
                LockRequest::write(FileLockKind::Posix, first_owner.clone(), 790, range(0, 9)),
                first.clone(),
            )
            .unwrap();
        let error = table
            .setlk_blocking(
                LockRequest::write(FileLockKind::Posix, owner("m1", 80), 800, range(20, 29)),
                second.clone(),
            )
            .unwrap_err();
        assert_eq!(error, LockError::Capacity);
        assert_eq!(
            table.acknowledge_waiter(&first).unwrap(),
            Some(LockWaiterOutcome::Granted)
        );
        table.release_posix_owner(&first_owner).unwrap();
        let error = table
            .setlk_blocking(
                LockRequest::write(FileLockKind::Posix, owner("m1", 80), 800, range(20, 29)),
                second.clone(),
            )
            .unwrap_err();
        assert_eq!(error, LockError::Capacity);
        table.release_session("m1").unwrap();
        let second = waiter_id("m2", 81);
        table
            .setlk_blocking(
                LockRequest::write(FileLockKind::Posix, owner("m2", 80), 800, range(20, 29)),
                second.clone(),
            )
            .unwrap();
        assert_eq!(
            table.acknowledge_waiter(&second).unwrap(),
            Some(LockWaiterOutcome::Granted)
        );
    }

    #[test]
    fn acknowledged_waiter_id_cannot_regrant() {
        let table = LockTable::default();
        let waiter = waiter_id("m1", 84);
        let lock_owner = owner("m1", 84);
        table
            .setlk_blocking(
                LockRequest::write(FileLockKind::Posix, lock_owner, 840, range(0, 9)),
                waiter.clone(),
            )
            .unwrap();
        assert_eq!(
            table.acknowledge_waiter(&waiter).unwrap(),
            Some(LockWaiterOutcome::Granted)
        );
        assert_eq!(
            table.cancel_waiter_with_outcome(waiter.clone()).unwrap(),
            LockWaiterOutcome::Unknown
        );
        let replay = table
            .setlk_blocking(
                LockRequest::write(FileLockKind::Posix, owner("m1", 85), 850, range(20, 29)),
                waiter,
            )
            .unwrap_err();
        assert_eq!(replay, LockError::Interrupted);
    }

    #[test]
    fn ack_higher_waiter_does_not_retire_delayed_lower_waiter() {
        let table = LockTable::default();
        let higher = waiter_id("m1", 90);
        let lower = waiter_id("m1", 89);
        table
            .setlk_blocking(
                LockRequest::write(FileLockKind::Posix, owner("m1", 90), 900, range(20, 29)),
                higher.clone(),
            )
            .unwrap();
        assert_eq!(
            table.acknowledge_waiter(&higher).unwrap(),
            Some(LockWaiterOutcome::Granted)
        );

        table
            .setlk_blocking(
                LockRequest::write(FileLockKind::Posix, owner("m1", 89), 890, range(0, 9)),
                lower.clone(),
            )
            .unwrap();
        assert_eq!(
            table.acknowledge_waiter(&lower).unwrap(),
            Some(LockWaiterOutcome::Granted)
        );
    }

    #[test]
    fn pending_lower_waiter_survives_newer_ack() {
        let table = LockTable::default();
        let holder = owner("holder", 1);
        table
            .setlk_nonblocking(LockRequest::write(
                FileLockKind::Posix,
                holder.clone(),
                101,
                range(0, 9),
            ))
            .unwrap();

        std::thread::scope(|scope| {
            let lower = waiter_id("m1", 93);
            let lower_owner = owner("m1", 93);
            let waiter = scope.spawn({
                let table_ref = &table;
                let lower = lower.clone();
                let lower_owner = lower_owner.clone();
                move || {
                    table_ref.setlk_blocking(
                        LockRequest::write(FileLockKind::Posix, lower_owner, 930, range(0, 9)),
                        lower,
                    )
                }
            });
            while table.state.lock().unwrap().waiters.is_empty() {
                std::thread::yield_now();
            }

            let higher = waiter_id("m1", 94);
            table
                .setlk_blocking(
                    LockRequest::write(FileLockKind::Posix, owner("m1", 94), 940, range(20, 29)),
                    higher.clone(),
                )
                .unwrap();
            assert_eq!(
                table.acknowledge_waiter(&higher).unwrap(),
                Some(LockWaiterOutcome::Granted)
            );
            table.release_posix_owner(&holder).unwrap();
            waiter.join().unwrap().unwrap();
            assert_eq!(
                table.acknowledge_waiter(&lower).unwrap(),
                Some(LockWaiterOutcome::Granted)
            );
        });
    }

    #[test]
    fn retired_identity_capacity_is_bounded() {
        let table = LockTable::new(LockTableLimits {
            max_locks: 16,
            max_waiters: 16,
            max_cancelled_waiters: 1,
        });
        let first = waiter_id("m1", 91);
        let second = waiter_id("m1", 92);
        table
            .setlk_blocking(
                LockRequest::write(FileLockKind::Posix, owner("m1", 91), 910, range(0, 9)),
                first.clone(),
            )
            .unwrap();
        assert_eq!(
            table.acknowledge_waiter(&first).unwrap(),
            Some(LockWaiterOutcome::Granted)
        );
        let error = table
            .setlk_blocking(
                LockRequest::write(FileLockKind::Posix, owner("m1", 92), 920, range(20, 29)),
                second,
            )
            .unwrap_err();
        assert_eq!(error, LockError::Capacity);
    }

    #[test]
    fn live_cancel_at_outcome_capacity_keeps_waiter_registered() {
        let table = LockTable::new(LockTableLimits {
            max_locks: 16,
            max_waiters: 16,
            max_cancelled_waiters: 1,
        });
        table
            .setlk_nonblocking(LockRequest::write(
                FileLockKind::Posix,
                owner("holder", 1),
                101,
                range(0, 9),
            ))
            .unwrap();

        std::thread::scope(|scope| {
            let waiter_id = waiter_id("m1", 83);
            let (tx, rx) = mpsc::channel();
            let waiter = scope.spawn({
                let table_ref = &table;
                let waiter_id = waiter_id.clone();
                move || {
                    let result = table_ref.setlk_blocking(
                        LockRequest::write(
                            FileLockKind::Posix,
                            owner("waiter", 2),
                            202,
                            range(0, 9),
                        ),
                        waiter_id,
                    );
                    tx.send(result.clone()).unwrap();
                    result
                }
            });
            let deadline = Instant::now() + Duration::from_secs(1);
            loop {
                if table.state.lock().unwrap().waiters.contains_key(&waiter_id) {
                    break;
                }
                if let Ok(result) = rx.try_recv() {
                    panic!("waiter returned before registration: {result:?}");
                }
                if Instant::now() >= deadline {
                    table.release_session("m1").unwrap();
                    let _ = rx.recv_timeout(Duration::from_secs(1));
                    panic!("waiter did not register before deadline");
                }
                std::thread::yield_now();
            }

            let retained = self::waiter_id("m1", 82);
            table
                .setlk_blocking(
                    LockRequest::write(FileLockKind::Posix, owner("m1", 82), 820, range(20, 29)),
                    retained,
                )
                .unwrap();

            let error = table
                .cancel_waiter_with_outcome(waiter_id.clone())
                .unwrap_err();
            assert_eq!(error, LockError::Capacity);
            assert!(table.state.lock().unwrap().waiters.contains_key(&waiter_id));

            table.release_session("m1").unwrap();
            let result = rx
                .recv_timeout(Duration::from_secs(1))
                .expect("waiter did not finish after session cleanup");
            assert_eq!(result.unwrap_err(), LockError::Interrupted);
            assert_eq!(waiter.join().unwrap().unwrap_err(), LockError::Interrupted);
            assert!(!table.state.lock().unwrap().waiters.contains_key(&waiter_id));
        });
    }

    #[test]
    fn owner_release_reclaims_matching_granted_outcomes() {
        let table = LockTable::default();
        let waiter = waiter_id("m1", 81);
        let lock_owner = owner("m1", 81);
        table
            .setlk_blocking(
                LockRequest::write(FileLockKind::Posix, lock_owner.clone(), 810, range(0, 9)),
                waiter.clone(),
            )
            .unwrap();
        assert_eq!(
            table.cancel_waiter_with_outcome(waiter.clone()).unwrap(),
            LockWaiterOutcome::Granted
        );
        table.release_posix_owner(&lock_owner).unwrap();
        assert_eq!(
            table.cancel_waiter_with_outcome(waiter).unwrap(),
            LockWaiterOutcome::Cancelled
        );
        assert!(table.active_locks().is_empty());
    }

    #[test]
    fn owner_release_does_not_reclaim_cancelled_outcomes_before_ack() {
        let table = LockTable::default();
        table
            .setlk_nonblocking(LockRequest::write(
                FileLockKind::Posix,
                owner("holder", 1),
                101,
                range(0, 9),
            ))
            .unwrap();

        std::thread::scope(|scope| {
            let waiter_id = waiter_id("m1", 86);
            let waiting_owner = owner("waiter", 86);
            let waiter = scope.spawn({
                let table_ref = &table;
                let waiter_id = waiter_id.clone();
                let waiting_owner = waiting_owner.clone();
                move || {
                    table_ref.setlk_blocking(
                        LockRequest::write(FileLockKind::Posix, waiting_owner, 202, range(0, 9)),
                        waiter_id,
                    )
                }
            });
            while table.state.lock().unwrap().waiters.is_empty() {
                std::thread::yield_now();
            }
            assert_eq!(
                table.cancel_waiter_with_outcome(waiter_id.clone()).unwrap(),
                LockWaiterOutcome::Cancelled
            );
            table.release_posix_owner(&waiting_owner).unwrap();
            let replay = table
                .setlk_blocking(
                    LockRequest::write(FileLockKind::Posix, waiting_owner, 202, range(20, 29)),
                    waiter_id.clone(),
                )
                .unwrap_err();
            assert_eq!(replay, LockError::Interrupted);
            assert_eq!(waiter.join().unwrap().unwrap_err(), LockError::Interrupted);
            assert_eq!(
                table.acknowledge_waiter(&waiter_id).unwrap(),
                Some(LockWaiterOutcome::Cancelled)
            );
        });
    }

    #[test]
    fn owner_release_converts_unacked_grant_to_cancelled_terminal() {
        let table = LockTable::default();
        let waiter = waiter_id("m1", 89);
        let lock_owner = owner("m1", 89);
        table
            .setlk_blocking(
                LockRequest::write(FileLockKind::Posix, lock_owner.clone(), 890, range(0, 9)),
                waiter.clone(),
            )
            .unwrap();
        assert_eq!(
            table.cancel_waiter_with_outcome(waiter.clone()).unwrap(),
            LockWaiterOutcome::Granted
        );
        table.release_posix_owner(&lock_owner).unwrap();
        assert_eq!(
            table.cancel_waiter_with_outcome(waiter.clone()).unwrap(),
            LockWaiterOutcome::Cancelled
        );
        let replay = table
            .setlk_blocking(
                LockRequest::write(FileLockKind::Posix, lock_owner, 890, range(20, 29)),
                waiter.clone(),
            )
            .unwrap_err();
        assert_eq!(replay, LockError::Interrupted);
        assert_eq!(
            table.acknowledge_waiter(&waiter).unwrap(),
            Some(LockWaiterOutcome::Cancelled)
        );
    }

    #[test]
    fn owner_release_does_not_reclaim_retired_replay_fence() {
        let table = LockTable::default();
        let waiter = waiter_id("m1", 87);
        let lock_owner = owner("m1", 87);
        table
            .setlk_blocking(
                LockRequest::write(FileLockKind::Posix, lock_owner.clone(), 870, range(0, 9)),
                waiter.clone(),
            )
            .unwrap();
        assert_eq!(
            table.acknowledge_waiter(&waiter).unwrap(),
            Some(LockWaiterOutcome::Granted)
        );
        table.release_posix_owner(&lock_owner).unwrap();
        let replay = table
            .setlk_blocking(
                LockRequest::write(FileLockKind::Posix, lock_owner, 870, range(20, 29)),
                waiter,
            )
            .unwrap_err();
        assert_eq!(replay, LockError::Interrupted);
    }

    #[test]
    fn release_session_reclaims_retired_capacity() {
        let table = LockTable::new(LockTableLimits {
            max_locks: 16,
            max_waiters: 16,
            max_cancelled_waiters: 1,
        });
        let first = waiter_id("m1", 88);
        table
            .setlk_blocking(
                LockRequest::write(FileLockKind::Posix, owner("m1", 88), 880, range(0, 9)),
                first.clone(),
            )
            .unwrap();
        assert_eq!(
            table.acknowledge_waiter(&first).unwrap(),
            Some(LockWaiterOutcome::Granted)
        );
        assert!(!table.is_idle().unwrap());
        table.release_session("m1").unwrap();
        assert!(table.is_idle().unwrap());

        let second = waiter_id("m2", 1);
        table
            .setlk_blocking(
                LockRequest::write(FileLockKind::Posix, owner("m2", 1), 201, range(20, 29)),
                second.clone(),
            )
            .unwrap();
        assert_eq!(
            table.acknowledge_waiter(&second).unwrap(),
            Some(LockWaiterOutcome::Granted)
        );
    }

    #[test]
    fn release_session_cancels_blocked_waiters_and_drops_active_locks() {
        let table = LockTable::default();
        table
            .setlk_nonblocking(LockRequest::write(
                FileLockKind::Posix,
                owner("m1", 1),
                101,
                range(0, 9),
            ))
            .unwrap();

        std::thread::scope(|scope| {
            let waiter = scope.spawn(|| {
                table.setlk_blocking(
                    LockRequest::write(FileLockKind::Posix, owner("m1", 2), 202, range(0, 9)),
                    waiter_id("m1", 42),
                )
            });
            while table.state.lock().unwrap().waiters.is_empty() {
                std::thread::yield_now();
            }

            table.release_session("m1").unwrap();
            assert_eq!(waiter.join().unwrap().unwrap_err(), LockError::Interrupted);
            assert!(table.active_locks().is_empty());
        });
    }

    #[test]
    fn release_session_prevents_late_waiter_from_recreating_lock() {
        let table = LockTable::default();
        table.release_session("m1").unwrap();

        let error = table
            .setlk_blocking(
                LockRequest::write(FileLockKind::Posix, owner("m1", 2), 202, range(0, 9)),
                waiter_id("m1", 43),
            )
            .unwrap_err();
        assert_eq!(error, LockError::Interrupted);

        let error = table
            .setlk_nonblocking(LockRequest::write(
                FileLockKind::Flock,
                owner("m1", 3),
                303,
                range(0, u64::MAX),
            ))
            .unwrap_err();
        assert_eq!(error, LockError::Interrupted);
        assert!(table.active_locks().is_empty());
    }

    #[test]
    fn closed_session_capacity_preserves_other_session_locks() {
        let table = LockTable::new(LockTableLimits {
            max_locks: 16,
            max_waiters: 16,
            max_cancelled_waiters: 1,
        });
        table
            .setlk_nonblocking(LockRequest::write(
                FileLockKind::Posix,
                owner("a", 1),
                101,
                range(0, 9),
            ))
            .unwrap();

        table.release_session("b1").unwrap();
        table.release_session("b2").unwrap();

        let conflict = table
            .getlk(&LockRequest::write(
                FileLockKind::Posix,
                owner("c", 1),
                301,
                range(0, 9),
            ))
            .unwrap()
            .unwrap();
        assert_eq!(conflict.owner, owner("a", 1));

        let error = table
            .setlk_nonblocking(LockRequest::write(
                FileLockKind::Posix,
                owner("c", 1),
                301,
                range(20, 29),
            ))
            .unwrap_err();
        assert_eq!(error, LockError::Capacity);

        table
            .setlk_nonblocking(LockRequest::unlock(
                FileLockKind::Posix,
                owner("a", 1),
                101,
                range(0, 9),
            ))
            .unwrap();
        assert!(table.active_locks().is_empty());
    }
}
