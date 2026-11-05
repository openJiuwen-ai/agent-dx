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
                    LockRequest::write(FileLockKind::Posix, owner("waiter", 2), 202, range(0, 9)),
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
