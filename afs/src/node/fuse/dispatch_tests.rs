#[test]
fn cached_open_incompatible_versions_bypass_shared_pages_until_retired() {
    let old = super::PageCacheIdentity {
        version_id: Some("v1".into()),
        length: 8,
    };
    let new = super::PageCacheIdentity {
        version_id: Some("v2".into()),
        length: 4,
    };
    let mut views = super::ActiveCachedViews::default();
    assert!(views.admit(2, 10, &old));
    assert!(views.admit(2, 11, &old));
    assert!(!views.admit(2, 12, &new));
    views.release(2, 10);
    assert!(!views.admit(2, 12, &new));
    views.release(2, 11);
    assert!(views.admit(2, 12, &new));
    views.release(2, 12);
    assert!(views.inodes.is_empty());
    assert_eq!(views.handles, 0);
    assert_eq!(views.version_bytes, 0);
}

#[test]
fn cached_open_live_view_metadata_is_bounded_and_never_evicted() {
    let identity = super::PageCacheIdentity {
        version_id: Some("v".into()),
        length: 1,
    };
    let mut views = super::ActiveCachedViews {
        version_bytes: super::PAGE_CACHE_VERSION_BYTES,
        ..Default::default()
    };
    assert!(!views.admit(2, 10, &identity));
    views.version_bytes = 0;
    views.handles = super::PAGE_CACHE_IDENTITIES;
    assert!(!views.admit(2, 10, &identity));
    assert!(views.inodes.is_empty());
}
#[test]
fn cached_open_fixed_view_may_use_kernel_readahead() {
    assert!(
        !super::native_open_reply_flags(true, true, libc::O_RDONLY, true)
            .contains(super::DIRECT_IO)
    );
}

#[test]
fn cached_open_direct_and_unidentified_views_stay_direct() {
    for (cached, flags, admitted) in [
        (false, libc::O_RDONLY, true),
        (true, libc::O_RDONLY | libc::O_DIRECT, true),
        (true, libc::O_RDONLY, false),
    ] {
        assert!(
            super::native_open_reply_flags(cached, true, flags, admitted)
                .contains(super::DIRECT_IO)
        );
    }
}

#[test]
fn page_cache_reuse_requires_exact_version_and_length() {
    let identity = |version: Option<&str>, length| super::PageCacheIdentity {
        version_id: version.map(str::to_owned),
        length,
    };
    let mut cache = super::PageCacheIdentities::default();
    assert!(!cache.observe(2, Some(identity(Some("same-prefix-old"), 4))));
    assert!(cache.observe(2, Some(identity(Some("same-prefix-old"), 4))));
    assert!(!cache.observe(2, Some(identity(Some("same-prefix-new"), 4))));
    assert!(!cache.observe(2, Some(identity(Some("same-prefix-new"), 8))));
    assert!(!cache.observe(2, None));
    assert!(!cache.observe(2, Some(identity(Some("same-prefix-new"), 8))));
    assert!(!cache.observe(3, Some(identity(None, 0))));
    assert!(cache.observe(3, Some(identity(None, 0))));
    assert!(!cache.observe(3, Some(identity(None, 1))));
    cache.remove(3);
    assert!(!cache.observe(3, Some(identity(None, 0))));
}

#[test]
fn page_cache_identity_metadata_has_entry_and_version_byte_limits() {
    let mut cache = super::PageCacheIdentities::default();
    for ino in 0..=super::PAGE_CACHE_IDENTITIES {
        assert!(!cache.observe(
            ino as u64,
            Some(super::PageCacheIdentity {
                version_id: None,
                length: 0
            })
        ));
    }
    assert_eq!(cache.entries.len(), super::PAGE_CACHE_IDENTITIES);
    assert!(!cache.entries.contains_key(&0));
    let oversized = super::PageCacheIdentity {
        version_id: Some("x".repeat(super::PAGE_CACHE_VERSION_BYTES + 1)),
        length: 1,
    };
    assert!(!cache.observe(1, Some(oversized)));
    assert!(!cache.entries.contains_key(&1));
    let large = |value| super::PageCacheIdentity {
        version_id: Some(std::iter::repeat_n(value, super::PAGE_CACHE_VERSION_BYTES / 2).collect()),
        length: 1,
    };
    assert!(!cache.observe(1, Some(large('a'))));
    assert!(!cache.observe(2, Some(large('b'))));
    assert!(cache.observe(1, Some(large('a'))));
    assert!(!cache.observe(3, Some(large('c'))));
    assert!(!cache.entries.contains_key(&2));
    assert!(cache.entries.contains_key(&1));
    assert!(cache.version_bytes <= super::PAGE_CACHE_VERSION_BYTES);
}

#[test]
fn fuse_mount_options_wire_threads_clone_and_zero_ttls() {
    let options = super::FuseExecOptions {
        fuse_threads: 2,
        fuse_clone_fd: true,
        fuse_workers: 1,
        ..Default::default()
    };
    let config = super::configured_mount("afs-unit", false, options);
    assert_eq!(config.n_threads, Some(2));
    assert!(config.clone_fd);
    let single = super::configured_mount(
        "afs-unit",
        false,
        super::FuseExecOptions {
            fuse_threads: 1,
            ..options
        },
    );
    assert!(!single.clone_fd);
    let mut fs = super::AfsFuse::with_mount_options(Arc::new(OwnedReadBackend::default()), options);
    assert_eq!(fs.dispatch.capacity, 8);
    assert_eq!(fs.entry_ttls(), (Duration::ZERO, Duration::ZERO));
    fs.cached_io = true;
    assert_eq!(fs.entry_ttls(), (Duration::ZERO, Duration::ZERO));
    let mut dfs_ttl = None;
    super::policy::with_cache_policy(
        &super::DfsFusePolicy,
        crate::node::vfs::types::BackendInode { value: 7 },
        |ttl, private| dfs_ttl = Some((ttl, private)),
    );
    assert_eq!(dfs_ttl, Some((Duration::ZERO, false)));
    for invalid in [
        super::FuseExecOptions {
            fuse_threads: 0,
            ..options
        },
        super::FuseExecOptions {
            fuse_workers: 0,
            ..options
        },
    ] {
        assert!(invalid.validate().is_err());
        assert!(
            super::FuseMountOptions {
                exec: invalid,
                ..Default::default()
            }
            .validate()
            .is_err()
        );
    }
}

#[test]
fn fuse_readahead_uses_the_supported_public_api_limit() {
    for (requested, ceiling, expected) in [
        (1024 * 1024, 128 * 1024, 128 * 1024),
        (4096, 128 * 1024, 4096),
        (0, 128 * 1024, 1),
        (4096, 0, 0),
        (0, 0, 0),
    ] {
        let mut effective = ceiling;
        super::configure_readahead(requested, |value| {
            if value == 0 {
                return Err(1);
            }
            if value > ceiling {
                return Err(ceiling);
            }
            let old = effective;
            effective = value;
            Ok(old)
        })
        .unwrap();
        assert_eq!(effective, expected);
    }
}

#[test]
fn inode_cache_gates_allow_distinct_inodes_in_one_shard() {
    // Exercise the current gate lookup with two distinct inode identities.
    // The held read is the barrier; a different inode's OPEN must acquire
    // its write side before this read is released, including shard peers.
    let fs = super::AfsFuse::new(Arc::new(OwnedReadBackend::default()));
    let gates = &fs.inode_cache_gates;
    let first_ino = 7;
    let other_ino = first_ino + super::PAGE_CACHE_INODE_GATES as u64;
    let first_gate = gates.acquire(first_ino).unwrap();
    let first_read = first_gate.read().unwrap();
    let other_gate = gates.acquire(other_ino).unwrap();
    let other_open = other_gate.gate.as_ref().unwrap().try_write();
    assert!(
        other_open.is_ok(),
        "another inode in the same shard was fenced by this READ"
    );
    drop(other_open);
    drop(first_read);
}

fn inode_cache_gate_entries(gates: &super::InodeCacheGates) -> usize {
    gates
        .shards
        .iter()
        .map(|shard| shard.lock().unwrap().len())
        .sum()
}

#[test]
fn inode_cache_gate_concurrent_acquisition_keeps_live_identity() {
    let fs = super::AfsFuse::new(Arc::new(OwnedReadBackend::default()));
    let gates = &fs.inode_cache_gates;
    let anchor = gates.acquire(17).unwrap();
    let writer = anchor.write().unwrap();
    let expected = Arc::as_ptr(anchor.gate.as_ref().unwrap()) as usize;
    std::thread::scope(|scope| {
        for _ in 0..8 {
            scope.spawn(|| {
                for _ in 0..128 {
                    let lease = gates.acquire(17).unwrap();
                    assert_eq!(Arc::as_ptr(lease.gate.as_ref().unwrap()) as usize, expected);
                    assert!(lease.gate.as_ref().unwrap().try_read().is_err());
                }
            });
        }
    });
    assert_eq!(inode_cache_gate_entries(gates), 1);
    drop(writer);
    drop(anchor);
    assert_eq!(inode_cache_gate_entries(gates), 0);
}

#[test]
fn inode_cache_gate_concurrent_last_release_does_not_accumulate() {
    let fs = super::AfsFuse::new(Arc::new(OwnedReadBackend::default()));
    let gates = &fs.inode_cache_gates;
    let barrier = std::sync::Barrier::new(9);
    let mut leaked_entries = 0;
    std::thread::scope(|scope| {
        for _ in 0..8 {
            let barrier = &barrier;
            scope.spawn(move || {
                for round in 0..64 {
                    // A different inode each round would accumulate leaked
                    // table-only entries if simultaneous last drops race.
                    let lease = gates.acquire(round * 64 + 3).unwrap();
                    let read = lease.read().unwrap();
                    barrier.wait();
                    drop(read);
                    drop(lease);
                    barrier.wait();
                    barrier.wait();
                }
            });
        }
        for _ in 0..64 {
            barrier.wait();
            barrier.wait();
            leaked_entries += inode_cache_gate_entries(gates);
            barrier.wait();
        }
    });
    assert_eq!(leaked_entries, 0);
    assert_eq!(inode_cache_gate_entries(gates), 0);
}

#[test]
fn inode_cache_gate_error_returns_retire_leases() {
    fn failed_read(gates: &super::InodeCacheGates, ino: u64) -> std::result::Result<(), i32> {
        let lease = gates.acquire(ino)?;
        let _read = lease.read()?;
        Err(libc::ESTALE)
    }
    fn failed_open(gates: &super::InodeCacheGates, ino: u64) -> std::result::Result<(), i32> {
        let lease = gates.acquire(ino)?;
        let _open = lease.write()?;
        Err(libc::EACCES)
    }
    let fs = super::AfsFuse::new(Arc::new(OwnedReadBackend::default()));
    for ino in 0..128 {
        assert_eq!(failed_read(&fs.inode_cache_gates, ino), Err(libc::ESTALE));
        assert_eq!(failed_open(&fs.inode_cache_gates, ino), Err(libc::EACCES));
        assert_eq!(inode_cache_gate_entries(&fs.inode_cache_gates), 0);
    }
}

#[test]
fn inode_cache_open_fence_drains_with_small_and_production_worker_counts() {
    for workers in [
        1,
        2,
        usize::from(super::FuseExecOptions::default().fuse_workers),
    ] {
        let dispatch = Arc::new(super::FuseDispatch::new(workers));
        let fs = super::AfsFuse::new(Arc::new(OwnedReadBackend::default()));
        let gates = fs.inode_cache_gates.clone();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let first_gate = gates.clone();
        dispatch.submit_keyed(61, move || {
            let lease = first_gate.acquire(61).unwrap();
            let _read = lease.read().unwrap();
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        });
        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        let (opened_tx, opened_rx) = std::sync::mpsc::channel();
        let open_gate = gates.clone();
        dispatch.submit(move || {
            let lease = open_gate.acquire(61).unwrap();
            let _open = lease.write().unwrap();
            opened_tx.send(()).unwrap();
        });
        let (second_tx, second_rx) = std::sync::mpsc::channel();
        dispatch.submit_keyed(62, move || {
            let lease = gates.acquire(61).unwrap();
            let _read = lease.read().unwrap();
            second_tx.send(()).unwrap();
        });
        let (drained_tx, drained_rx) = std::sync::mpsc::channel();
        let drain_dispatch = dispatch.clone();
        let drainer = std::thread::spawn(move || {
            drain_dispatch.close_and_drain();
            drained_tx.send(()).unwrap();
        });
        let early_drain = drained_rx.recv_timeout(Duration::from_millis(50));
        release_tx.send(()).unwrap();
        opened_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        second_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        drained_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        drainer.join().unwrap();
        assert!(early_drain.is_err());
        assert_eq!(inode_cache_gate_entries(&fs.inode_cache_gates), 0);
    }
}

#[test]
fn read_concurrency_ready_async_starter_does_not_wait_for_unrelated_worker() {
    let dispatch = super::FuseDispatch::with_capacity(1, 3);
    let (busy_tx, busy_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    dispatch.submit_keyed(101, move || {
        busy_tx.send(()).unwrap();
        release_rx.recv().unwrap();
    });
    busy_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    let (ticket_tx, ticket_rx) = std::sync::mpsc::channel();
    dispatch.submit_read_deferred(102, move |ticket| ticket_tx.send(ticket).unwrap());
    let early = ticket_rx.recv_timeout(Duration::from_millis(100));
    let started_while_busy = early.is_ok();
    release_tx.send(()).unwrap();
    let ticket = early
        .or_else(|_| ticket_rx.recv_timeout(Duration::from_secs(1)))
        .unwrap();
    drop(ticket);
    dispatch.close_and_drain();
    assert!(
        started_while_busy,
        "eligible async starter must not await an unrelated synchronous worker"
    );
    assert_eq!(dispatch.shared.queue.lock().unwrap().jobs, 0);
}

#[test]
fn read_concurrency_async_starter_preserves_prior_exclusive_turn() {
    let dispatch = super::FuseDispatch::with_capacity(1, 3);
    let (busy_tx, busy_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    dispatch.submit_keyed(103, move || {
        busy_tx.send(()).unwrap();
        release_rx.recv().unwrap();
    });
    busy_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    let (ticket_tx, ticket_rx) = std::sync::mpsc::channel();
    dispatch.submit_read_deferred(103, move |ticket| ticket_tx.send(ticket).unwrap());
    let early = ticket_rx.recv_timeout(Duration::from_millis(20));
    let incorrectly_started = early.is_ok();
    release_tx.send(()).unwrap();
    let ticket = early
        .or_else(|_| ticket_rx.recv_timeout(Duration::from_secs(1)))
        .unwrap();
    assert_eq!(dispatch.shared.queue.lock().unwrap().jobs, 1);
    drop(ticket);
    dispatch.close_and_drain();
    assert!(
        !incorrectly_started,
        "async start must retain the same-fh exclusive barrier"
    );
}

#[test]
fn read_concurrency_deferred_completion_releases_worker_but_keeps_fh_barrier() {
    let dispatch = super::FuseDispatch::with_capacity(1, 3);
    let (ticket_tx, ticket_rx) = std::sync::mpsc::channel();
    dispatch.submit_read_deferred(71, move |ticket| ticket_tx.send(ticket).unwrap());
    let ticket = ticket_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    let (independent_tx, independent_rx) = std::sync::mpsc::channel();
    dispatch.submit_keyed(72, move || independent_tx.send(()).unwrap());
    independent_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    let (exclusive_tx, exclusive_rx) = std::sync::mpsc::channel();
    dispatch.submit_keyed(71, move || exclusive_tx.send(()).unwrap());
    assert!(
        exclusive_rx
            .recv_timeout(Duration::from_millis(20))
            .is_err()
    );
    assert_eq!(dispatch.shared.queue.lock().unwrap().jobs, 2);
    drop(ticket);
    exclusive_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    dispatch.close_and_drain();
    assert_eq!(dispatch.shared.queue.lock().unwrap().jobs, 0);
}

#[test]
fn read_concurrency_child_work_keeps_task_turn_after_parent_drop() {
    let dispatch = super::FuseDispatch::with_capacity(1, 3);
    let (ticket_tx, ticket_rx) = std::sync::mpsc::channel();
    dispatch.submit_read_deferred(75, move |ticket| ticket_tx.send(ticket).unwrap());
    let ticket = ticket_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    let child = ticket.hold();
    drop(ticket);
    let (exclusive_tx, exclusive_rx) = std::sync::mpsc::channel();
    dispatch.submit_keyed(75, move || exclusive_tx.send(()).unwrap());
    assert!(
        exclusive_rx
            .recv_timeout(Duration::from_millis(20))
            .is_err()
    );
    assert_eq!(dispatch.shared.queue.lock().unwrap().jobs, 2);
    drop(child);
    exclusive_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    dispatch.close_and_drain();
    assert_eq!(dispatch.shared.queue.lock().unwrap().jobs, 0);
}

#[test]
fn read_concurrency_explicit_capacity_can_be_lower_than_worker_count() {
    let dispatch = Arc::new(super::FuseDispatch::with_capacity(4, 1));
    let (ticket_tx, ticket_rx) = std::sync::mpsc::channel();
    dispatch.submit_read_deferred(73, move |ticket| ticket_tx.send(ticket).unwrap());
    let ticket = ticket_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (submitted_tx, submitted_rx) = std::sync::mpsc::channel();
    let submitting = dispatch.clone();
    let submitter = std::thread::spawn(move || {
        submitting.submit_keyed(74, move || entered_tx.send(()).unwrap());
        submitted_tx.send(()).unwrap();
    });
    assert!(
        submitted_rx
            .recv_timeout(Duration::from_millis(20))
            .is_err()
    );
    assert!(entered_rx.try_recv().is_err());
    drop(ticket);
    submitted_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    submitter.join().unwrap();
    dispatch.close_and_drain();
}

#[test]
fn read_batch_overlaps_but_a_queued_exclusive_turn_fences_later_reads() {
    let dispatch = super::FuseDispatch::new(3);
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let mut releases = Vec::new();
    for _ in 0..2 {
        let started = started_tx.clone();
        let (release, wait) = std::sync::mpsc::channel();
        releases.push(release);
        dispatch.submit_read_keyed(41, move || {
            started.send(()).unwrap();
            wait.recv().unwrap();
        });
    }
    let first = started_rx.recv_timeout(Duration::from_secs(1));
    let second = started_rx.recv_timeout(Duration::from_secs(1));
    let (exclusive_tx, exclusive_rx) = std::sync::mpsc::channel();
    let (release_exclusive, wait_exclusive) = std::sync::mpsc::channel();
    dispatch.submit_keyed(41, move || {
        exclusive_tx.send(()).unwrap();
        wait_exclusive.recv().unwrap();
    });
    let (late_tx, late_rx) = std::sync::mpsc::channel();
    dispatch.submit_read_keyed(41, move || late_tx.send(()).unwrap());
    let early_exclusive = exclusive_rx.try_recv();
    let early_read = late_rx.try_recv();
    for release in releases {
        release.send(()).unwrap();
    }
    let exclusive = exclusive_rx.recv_timeout(Duration::from_secs(1));
    let read_crossed_barrier = late_rx.recv_timeout(Duration::from_millis(20));
    release_exclusive.send(()).unwrap();
    let late = late_rx.recv_timeout(Duration::from_secs(1));
    dispatch.close_and_drain();
    assert!(
        first.is_ok() && second.is_ok(),
        "adjacent readers must both run"
    );
    assert!(early_exclusive.is_err() && early_read.is_err());
    assert!(exclusive.is_ok() && read_crossed_barrier.is_err() && late.is_ok());
}

#[test]
fn queued_read_batch_panic_retires_only_its_own_turn() {
    let dispatch = super::FuseDispatch::new(3);
    let (initial_tx, initial_rx) = std::sync::mpsc::channel();
    let (release_initial, wait_initial) = std::sync::mpsc::channel();
    dispatch.submit_keyed(42, move || {
        initial_tx.send(()).unwrap();
        wait_initial.recv().unwrap();
    });
    initial_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let panic_started = started_tx.clone();
    dispatch.submit_read_keyed(42, move || {
        panic_started.send(()).unwrap();
        panic!("injected read failure");
    });
    let (release_read, wait_read) = std::sync::mpsc::channel();
    dispatch.submit_read_keyed(42, move || {
        started_tx.send(()).unwrap();
        wait_read.recv().unwrap();
    });
    let (tail_tx, tail_rx) = std::sync::mpsc::channel();
    dispatch.submit_keyed(42, move || tail_tx.send(()).unwrap());
    release_initial.send(()).unwrap();
    let first = started_rx.recv_timeout(Duration::from_secs(1));
    let second = started_rx.recv_timeout(Duration::from_secs(1));
    let premature_tail = tail_rx.recv_timeout(Duration::from_millis(20));
    release_read.send(()).unwrap();
    let tail = tail_rx.recv_timeout(Duration::from_secs(1));
    dispatch.close_and_drain();
    assert!(first.is_ok() && second.is_ok());
    assert!(premature_tail.is_err() && tail.is_ok());
}

#[test]
fn borrowed_write_and_release_follow_the_read_batch_with_production_workers() {
    for workers in [
        1,
        2,
        usize::from(super::FuseExecOptions::default().fuse_workers),
    ] {
        let dispatch = super::FuseDispatch::new(workers);
        let events = Arc::new(std::sync::Mutex::new(Vec::new()));
        for value in [1, 2] {
            let events = events.clone();
            dispatch.submit_read_keyed(43, move || events.lock().unwrap().push(value));
        }
        let input = [7_u8; 8];
        let address = input.as_ptr() as usize;
        dispatch.run_borrowed_keyed(43, || {
            assert_eq!(input.as_ptr() as usize, address);
            let mut seen = events.lock().unwrap();
            assert_eq!(seen.len(), 2);
            seen.push(3);
        });
        let tail = events.clone();
        dispatch.submit_read_keyed(43, move || tail.lock().unwrap().push(4));
        let released = events.clone();
        dispatch.submit_keyed(43, move || released.lock().unwrap().push(5));
        dispatch.close_and_drain();
        let seen = events.lock().unwrap();
        assert!(seen[..2].contains(&1) && seen[..2].contains(&2));
        assert_eq!(&seen[2..], &[3, 4, 5]);
    }
}

#[test]
fn read_diagnostic_disabled_keeps_reply_behavior() {
    let registry = afs_metrics::Registry::new();
    let metrics = crate::node::read_work::ReadWorkMetrics::register(&registry).unwrap();
    let span = metrics.diagnostic(crate::node::read_work::ReadDiagnosticStage::FuseRead);
    assert!(span.is_none());
    let mut replied = false;
    super::complete_read_reply(span, false, || replied = true);
    assert!(replied);
    assert!(metrics.diagnostics_snapshot().is_none());
}

#[test]
fn read_diagnostic_covers_async_queue_and_actual_reply_completion() {
    use crate::node::read_work::{ReadDiagnosticStage, ReadWorkMetrics};
    let registry = afs_metrics::Registry::new();
    let metrics = ReadWorkMetrics::register(&registry)
        .unwrap()
        .with_read_diagnostics(true);
    let dispatch = super::FuseDispatch::new(1);
    let (reply_entered_tx, reply_entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let span = metrics.diagnostic(ReadDiagnosticStage::FuseRead);
    dispatch.submit_keyed(1, move || {
        super::complete_read_reply(span, true, || {
            reply_entered_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        });
    });
    reply_entered_rx
        .recv_timeout(std::time::Duration::from_secs(1))
        .unwrap();
    let before_reply_return = metrics.diagnostics_snapshot().unwrap();
    // submit_keyed has returned, but the actual reply is still blocked.
    let prematurely_completed = before_reply_return
        .records
        .iter()
        .any(|record| record.completed);
    release_tx.send(()).unwrap();
    drop(dispatch);
    assert!(!prematurely_completed);
    let snapshot = metrics.diagnostics_snapshot().unwrap();
    assert_eq!(snapshot.records.len(), 1);
    let record = &snapshot.records[0];
    assert_eq!(record.stage, ReadDiagnosticStage::FuseRead);
    assert!(record.completed && record.success);
    assert!(record.end_ns >= record.start_ns);
}

#[test]
fn read_diagnostic_keeps_error_and_unwind_reply_outcomes() {
    use crate::node::read_work::{ReadDiagnosticStage, ReadWorkMetrics};
    let registry = afs_metrics::Registry::new();
    let metrics = ReadWorkMetrics::register(&registry)
        .unwrap()
        .with_read_diagnostics(true);
    let mut reply_errno = 0;
    super::complete_read_reply(
        metrics.diagnostic(ReadDiagnosticStage::FuseRead),
        false,
        || {
            reply_errno = libc::ESTALE;
        },
    );
    assert_eq!(reply_errno, libc::ESTALE);
    let span = metrics.diagnostic(ReadDiagnosticStage::FuseRead);
    let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        super::complete_read_reply(span, true, || panic!("test reply unwind"));
    }));
    assert!(unwound.is_err());
    let snapshot = metrics.diagnostics_snapshot().unwrap();
    assert_eq!(snapshot.records.len(), 2);
    assert!(
        snapshot
            .records
            .iter()
            .all(|record| record.completed && !record.success)
    );
}

#[test]
fn inline_local_read_data_observes_fresh_short_smaller_zero_and_eof_reads() {
    use std::os::unix::fs::FileExt;

    let file = tempfile::tempfile().unwrap();
    let mut scratch = Vec::new();
    let mut read = |size, offset| {
        super::inline_local_read_data(&mut scratch, size, |out| {
            file.read_at(out, offset)
                .map_err(|error| error.raw_os_error().unwrap())
        })
        .unwrap()
        .into_owned()
    };
    file.write_at(b"abcdefgh", 0).unwrap();
    assert_eq!(read(8, 0), b"abcdefgh");
    file.set_len(0).unwrap();
    file.write_at(b"XY", 0).unwrap();
    assert_eq!(read(8, 0), b"XY");
    assert_eq!(read(1, 0), b"X");
    file.write_at(b"ZQ", 0).unwrap();
    assert_eq!(read(8, 0), b"ZQ");
    assert!(read(0, 0).is_empty());
    assert!(read(8, 2).is_empty());
    file.set_len(0).unwrap();
    assert!(read(8, 0).is_empty());
}

#[test]
fn inline_local_read_data_error_after_success_never_returns_prior_bytes() {
    let mut scratch = Vec::new();
    let data = super::inline_local_read_data(&mut scratch, 8, |out| {
        out.copy_from_slice(b"abcdefgh");
        Ok(8)
    })
    .unwrap();
    assert_eq!(data.as_ref(), b"abcdefgh");
    let result = super::inline_local_read_data(&mut scratch, 8, |out| {
        out[0] = b'!';
        Err(libc::EACCES)
    });
    assert_eq!(result.unwrap_err(), libc::EACCES);
    let data = super::inline_local_read_data(&mut scratch, 8, |out| {
        out[0] = b'z';
        Ok(1)
    })
    .unwrap();
    assert_eq!(data.as_ref(), b"z");
}

#[test]
fn inline_local_read_data_clamps_reported_length_to_requested_size() {
    let mut scratch = Vec::new();
    let data = super::inline_local_read_data(&mut scratch, 3, |out| {
        out.copy_from_slice(b"abc");
        Ok(usize::MAX)
    })
    .unwrap();
    assert_eq!(data.as_ref(), b"abc");
}

#[test]
fn inline_local_read_data_reuses_initialized_storage_with_bounded_growth() {
    let mut scratch = Vec::new();
    let mut initialized = 0;
    for size in [
        17,
        64 * 1024,
        700 * 1024,
        super::MAX_INLINE_LOCAL_READ_SCRATCH,
    ] {
        let data = super::inline_local_read_data(&mut scratch, size, |out| {
            assert_eq!(out.len(), size);
            assert!(out[initialized..].iter().all(|byte| *byte == 0));
            out.fill(b'a');
            Ok(size)
        })
        .unwrap();
        assert!(matches!(data, std::borrow::Cow::Borrowed(_)));
        assert_eq!(data.len(), size);
        let pointer = data.as_ptr();
        let capacity = scratch.capacity();
        assert!(capacity >= size);
        assert!(capacity <= super::MAX_INLINE_LOCAL_READ_SCRATCH);
        let data = super::inline_local_read_data(&mut scratch, size / 2, |out| {
            out.fill(b'b');
            Ok(out.len())
        })
        .unwrap();
        assert_eq!(data.as_ptr(), pointer);
        assert!(data.iter().all(|byte| *byte == b'b'));
        assert_eq!(scratch.capacity(), capacity);
        initialized = size;
    }
}

#[test]
fn inline_local_read_data_oversize_success_and_error_leave_scratch_unchanged() {
    let mut scratch = Vec::new();
    let oversized = super::MAX_INLINE_LOCAL_READ_SCRATCH + 1;
    for primed in [false, true] {
        if primed {
            super::inline_local_read_data(&mut scratch, 8, |out| {
                out.copy_from_slice(b"retained");
                Ok(8)
            })
            .unwrap();
        }
        let capacity = scratch.capacity();
        let pointer = scratch.as_ptr();
        let contents = scratch.clone();
        let data = super::inline_local_read_data(&mut scratch, oversized, |out| {
            assert_eq!(out.len(), oversized);
            assert!(out.iter().all(|byte| *byte == 0));
            out[..3].copy_from_slice(b"new");
            Ok(3)
        })
        .unwrap();
        assert!(matches!(data, std::borrow::Cow::Owned(_)));
        assert_eq!(data.as_ref(), b"new");
        assert_eq!(scratch.capacity(), capacity);
        assert_eq!(scratch.as_ptr(), pointer);
        assert_eq!(scratch, contents);
        let result = super::inline_local_read_data(&mut scratch, oversized, |out| {
            out[0] = b'!';
            Err(libc::EIO)
        });
        assert_eq!(result.unwrap_err(), libc::EIO);
        assert_eq!(scratch.capacity(), capacity);
        assert_eq!(scratch.as_ptr(), pointer);
        assert_eq!(scratch, contents);
    }
}

#[derive(Default)]
struct OwnedReadBackend {
    owned: Vec<u8>,
    buffered: &'static [u8],
    read_calls: std::sync::atomic::AtomicUsize,
    read_owned_calls: std::sync::atomic::AtomicUsize,
}

impl super::Backend for OwnedReadBackend {
    fn root_inode(&self) -> super::BackendInode {
        super::BackendInode { value: 1 }
    }

    fn read_owned(
        &self,
        _ctx: &super::RequestContext,
        handle: super::FileHandle,
        offset: u64,
        size: usize,
    ) -> super::Result<Vec<u8>> {
        self.read_owned_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        assert_eq!(handle, super::FileHandle(9));
        assert_eq!(offset, 4);
        assert_eq!(size, 32);
        Ok(self.owned.clone())
    }

    fn read(
        &self,
        _ctx: &super::RequestContext,
        handle: super::FileHandle,
        offset: u64,
        out: &mut [u8],
    ) -> super::Result<usize> {
        self.read_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        assert_eq!(handle, super::FileHandle(9));
        assert_eq!(offset, 4);
        out[..self.buffered.len()].copy_from_slice(self.buffered);
        Ok(self.buffered.len())
    }
}

fn test_context() -> super::RequestContext {
    super::RequestContext {
        uid: 1000,
        gid: 1000,
        pid: 1234,
        umask: 0,
        supplementary_gids: Vec::new(),
    }
}

#[test]
fn buffer_reuse_backend_reply_holds_pool_owner_until_send_returns() {
    struct PooledBackend {
        pool: std::sync::Arc<crate::node::read_buffer::ReadBufferPool>,
    }
    impl super::Backend for PooledBackend {
        fn root_inode(&self) -> super::BackendInode {
            super::BackendInode { value: 1 }
        }
        fn read_reply(
            &self,
            _: &super::RequestContext,
            _: super::FileHandle,
            _: u64,
            size: usize,
        ) -> super::Result<prost::bytes::Bytes> {
            let mut out = self.pool.take(size, None)?;
            out.fill(23);
            Ok(prost::bytes::Bytes::from_owner(out))
        }
    }
    let backend = PooledBackend {
        pool: crate::node::read_buffer::ReadBufferPool::new("reply"),
    };
    let reply =
        super::backend_read_data(&backend, &test_context(), super::FileHandle(9), 0, 1024).unwrap();
    let pointer = reply.as_ptr();
    let held = reply.clone();
    drop(reply);
    let other = backend.pool.take(1024, None).unwrap();
    assert_ne!(other.as_ptr(), pointer);
    assert_eq!(&held[..], &[23; 1024]);
    drop(held);
    assert_eq!(backend.pool.take(1024, None).unwrap().as_ptr(), pointer);
}

#[test]
fn backend_read_data_prefers_owned_response_without_buffered_read() {
    let backend = OwnedReadBackend {
        owned: b"private".to_vec(),
        buffered: b"fallback",
        read_calls: std::sync::atomic::AtomicUsize::new(0),
        read_owned_calls: std::sync::atomic::AtomicUsize::new(0),
    };
    let data =
        super::backend_read_data(&backend, &test_context(), super::FileHandle(9), 4, 32).unwrap();
    assert_eq!(data.as_ref(), b"private");
    assert_eq!(
        backend
            .read_owned_calls
            .load(std::sync::atomic::Ordering::SeqCst),
        1
    );
    assert_eq!(
        backend.read_calls.load(std::sync::atomic::Ordering::SeqCst),
        0
    );
}

#[test]
fn backend_read_data_uses_backend_default_owned_fallback() {
    struct BufferedOnlyBackend {
        read_calls: std::sync::atomic::AtomicUsize,
    }

    impl super::Backend for BufferedOnlyBackend {
        fn root_inode(&self) -> super::BackendInode {
            super::BackendInode { value: 1 }
        }

        fn read(
            &self,
            _ctx: &super::RequestContext,
            handle: super::FileHandle,
            offset: u64,
            out: &mut [u8],
        ) -> super::Result<usize> {
            self.read_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            assert_eq!(handle, super::FileHandle(9));
            assert_eq!(offset, 4);
            out[..8].copy_from_slice(b"fallback");
            Ok(8)
        }
    }

    let backend = BufferedOnlyBackend {
        read_calls: std::sync::atomic::AtomicUsize::new(0),
    };
    let data =
        super::backend_read_data(&backend, &test_context(), super::FileHandle(9), 4, 32).unwrap();
    assert_eq!(data.as_ref(), b"fallback");
    assert_eq!(
        backend.read_calls.load(std::sync::atomic::Ordering::SeqCst),
        1
    );
}

#[test]
fn fuse_callback_metrics_share_registry_without_reset_or_cross_registry_leak() {
    let registry = afs_metrics::Registry::new();
    let first = super::FuseRequestMetrics::register(&registry).unwrap();
    let same = super::FuseRequestMetrics::register(&registry.clone()).unwrap();
    let other_registry = afs_metrics::Registry::new();
    let other = super::FuseRequestMetrics::register(&other_registry).unwrap();
    first.record("ownerfs", "read");
    same.record("ownerfs", "read");
    first.record("dfs", "read");
    assert_eq!(
        first.requests.with_label_values(&["ownerfs", "read"]).get(),
        2
    );
    assert_eq!(
        other.requests.with_label_values(&["ownerfs", "read"]).get(),
        0
    );
    let text = afs_metrics::encode_text(&registry).unwrap();
    assert!(text.contains("afs_fuse_callbacks_total{filesystem=\"ownerfs\",operation=\"read\"} 2"));
    assert!(text.contains("afs_fuse_callbacks_total{filesystem=\"dfs\",operation=\"read\"} 1"));
    assert_eq!(text, afs_metrics::encode_text(&registry).unwrap());
    assert_eq!(
        first.requests.with_label_values(&["ownerfs", "read"]).get(),
        2
    );
}

#[test]
fn fuse_callback_metrics_export_zeros_for_every_implemented_operation() {
    let registry = afs_metrics::Registry::new();
    super::FuseRequestMetrics::register(&registry).unwrap();
    let text = afs_metrics::encode_text(&registry).unwrap();
    for filesystem in ["ownerfs", "dfs"] {
        for operation in super::FUSE_CALLBACKS {
            assert!(text.contains(&format!("afs_fuse_callbacks_total{{filesystem=\"{filesystem}\",operation=\"{operation}\"}} 0")));
        }
    }
}

#[test]
fn fuse_io_size_metrics_count_callback_bytes_and_cumulative_boundaries() {
    let registry = afs_metrics::Registry::new();
    let metrics = super::FuseRequestMetrics::register(&registry).unwrap();
    let sizes = [
        4096, 4097, 65_536, 65_537, 262_144, 262_145, 1_048_576, 1_048_577,
    ];
    for size in sizes {
        metrics.record_io("dfs", "read", size);
    }
    metrics.record_io("ownerfs", "write", 0);
    metrics.record_io("ownerfs", "write", 131_072);
    let read = metrics.io_size_bytes.with_label_values(&["dfs", "read"]);
    assert_eq!(read.get_sample_count(), sizes.len() as u64);
    assert_eq!(read.get_sample_sum(), sizes.iter().sum::<usize>() as f64);
    assert_eq!(
        metrics.requests.with_label_values(&["dfs", "read"]).get(),
        8
    );
    let write = metrics
        .io_size_bytes
        .with_label_values(&["ownerfs", "write"]);
    assert_eq!(write.get_sample_count(), 2);
    assert_eq!(write.get_sample_sum(), 131_072.0);
    assert_eq!(
        metrics
            .requests
            .with_label_values(&["ownerfs", "write"])
            .get(),
        2
    );
    let text = afs_metrics::encode_text(&registry).unwrap();
    for (bound, count) in [
        ("4096", 1),
        ("65536", 3),
        ("262144", 5),
        ("1048576", 7),
        ("+Inf", 8),
    ] {
        assert!(
            text.lines().any(|line| {
                line.starts_with("afs_fuse_callback_size_bytes_bucket{")
                    && line.contains("filesystem=\"dfs\"")
                    && line.contains("operation=\"read\"")
                    && line.contains(&format!("le=\"{bound}\""))
                    && line.ends_with(&format!(" {count}"))
            }),
            "missing cumulative bucket le={bound}, count={count}: {text}"
        );
    }
}

#[test]
fn fuse_io_size_metrics_share_registry_and_export_only_bounded_io_labels() {
    let registry = afs_metrics::Registry::new();
    let first = super::FuseRequestMetrics::register(&registry).unwrap();
    let same = super::FuseRequestMetrics::register(&registry.clone()).unwrap();
    let other_registry = afs_metrics::Registry::new();
    let other = super::FuseRequestMetrics::register(&other_registry).unwrap();
    first.record_io("dfs", "write", 4096);
    same.record_io("dfs", "write", 65_536);
    assert_eq!(
        first
            .io_size_bytes
            .with_label_values(&["dfs", "write"])
            .get_sample_count(),
        2
    );
    assert_eq!(
        other
            .io_size_bytes
            .with_label_values(&["dfs", "write"])
            .get_sample_count(),
        0
    );
    let text = afs_metrics::encode_text(&registry).unwrap();
    for filesystem in ["ownerfs", "dfs"] {
        for operation in ["read", "write"] {
            assert!(text.lines().any(|line| {
                line.starts_with("afs_fuse_callback_size_bytes_count{")
                    && line.contains(&format!("filesystem=\"{filesystem}\""))
                    && line.contains(&format!("operation=\"{operation}\""))
            }));
        }
    }
    assert_eq!(
        text.lines()
            .filter(|line| line.starts_with("afs_fuse_callback_size_bytes_count{"))
            .count(),
        4
    );
    assert_eq!(
        text.lines()
            .filter(|line| line.starts_with("afs_fuse_callback_size_bytes_bucket{"))
            .count(),
        20
    );
}

#[test]
fn metadata_groups_require_matching_kernel_identity() {
    let status = "Uid:\t1000 1000 1000 1000\nGid:\t100 100 100 100\nGroups:\t200 100 200\n";
    assert_eq!(
        super::status_groups(status, 1000, 100),
        Some(vec![100, 200])
    );
    assert_eq!(super::status_groups(status, 1001, 100), None);
    assert_eq!(super::status_groups(status, 1000, 101), None);
    assert_eq!(super::status_groups("Uid: 1000\n", 1000, 100), None);
}

#[test]
fn process_start_ignores_spaces_and_parentheses_in_command_name() {
    let stat = format!(
        "123 (a name ) b) {}",
        (3..=22)
            .map(|x| x.to_string())
            .collect::<Vec<_>>()
            .join(" ")
    );
    assert_eq!(super::process_start(&stat), Some("22"));
    assert_eq!(super::process_start("123 (dead) Z"), None);
}

use super::FuseDispatch;
use std::{
    sync::{Arc, Mutex, mpsc},
    time::Duration,
};

#[test]
fn xattr_size_query_short_buffer_and_empty_value_are_distinct() {
    assert_eq!(super::xattr_response_size(4, 0), Ok(Some(4)));
    assert_eq!(super::xattr_response_size(4, 3), Err(libc::ERANGE));
    assert_eq!(super::xattr_response_size(4, 4), Ok(None));
    assert_eq!(super::xattr_response_size(4, 64), Ok(None));
    assert_eq!(super::xattr_response_size(0, 0), Ok(Some(0)));
    assert_eq!(super::xattr_response_size(0, 1), Ok(None));
    assert_eq!(super::xattr_response_size(usize::MAX, 0), Err(libc::E2BIG));
}

#[test]
fn xattr_create_replace_and_invalid_flags_keep_linux_contract() {
    for flags in [0, libc::XATTR_CREATE, libc::XATTR_REPLACE] {
        assert_eq!(super::validate_xattr_set(flags, 0), Ok(()));
    }
    for flags in [libc::XATTR_CREATE | libc::XATTR_REPLACE, 4, -1] {
        assert_eq!(super::validate_xattr_set(flags, 0), Err(libc::EINVAL));
    }
    assert_eq!(super::validate_xattr_set(0, 1), Err(libc::EINVAL));
}

#[test]
fn sparse_file_attributes_keep_allocated_blocks_separate_from_eof() {
    let attributes = super::FileAttributes {
        kind: super::FileKind::Regular,
        size: 8 * 1024 * 1024 * 1024,
        blocks: 8,
        mode: 0o600,
        uid: 1000,
        gid: 1000,
        nlink: 1,
        atime: super::UNIX_EPOCH,
        mtime: super::UNIX_EPOCH,
        ctime: super::UNIX_EPOCH,
    };
    let attr = super::file_attr(2, &attributes);
    assert_eq!(attr.size, attributes.size);
    assert_eq!(attr.blocks, 8);
}

#[test]
fn dropping_fuse_dispatch_waits_for_admitted_jobs() {
    let dispatch = FuseDispatch::new(2);
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let (second_tx, second_rx) = mpsc::channel();
    dispatch.submit_keyed(7, move || {
        started_tx.send(()).unwrap();
        release_rx.recv().unwrap();
    });
    dispatch.submit_keyed(7, move || {
        second_tx.send(()).unwrap();
    });
    started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    let (dropped_tx, dropped_rx) = mpsc::channel();
    let dropper = std::thread::spawn(move || {
        drop(dispatch);
        dropped_tx.send(()).unwrap();
    });
    let early = dropped_rx.recv_timeout(Duration::from_millis(100));
    release_tx.send(()).unwrap();
    second_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    dropper.join().unwrap();
    assert!(
        early.is_err(),
        "dispatch teardown returned while accepted write remained active"
    );
    dropped_rx.recv_timeout(Duration::from_secs(1)).unwrap();
}

#[test]
fn full_fuse_queue_releases_blocked_producer_without_ready_successor() {
    let dispatch = Arc::new(FuseDispatch::new(2));
    let (started_tx, started_rx) = mpsc::channel();
    let (first_release_tx, first_release_rx) = mpsc::channel();
    let (second_release_tx, second_release_rx) = mpsc::channel();
    let (finished_tx, finished_rx) = mpsc::channel();
    for (key, release_rx) in [(31, first_release_rx), (32, second_release_rx)] {
        let started_tx = started_tx.clone();
        let finished_tx = finished_tx.clone();
        dispatch.submit_keyed(key, move || {
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            finished_tx.send(()).unwrap();
        });
    }
    for _ in 0..2 {
        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    }
    // Only key 31 has queued successors. Completing key 32 must wake the
    // capacity producer even though it makes no worker job ready.
    for _ in 2..dispatch.capacity {
        let finished_tx = finished_tx.clone();
        dispatch.submit_keyed(31, move || finished_tx.send(()).unwrap());
    }
    let (submitting_tx, submitting_rx) = mpsc::channel();
    let (admitted_tx, admitted_rx) = mpsc::channel();
    let producer_dispatch = dispatch.clone();
    let producer = std::thread::spawn(move || {
        submitting_tx.send(()).unwrap();
        producer_dispatch.submit_keyed(33, move || finished_tx.send(()).unwrap());
        admitted_tx.send(()).unwrap();
    });
    submitting_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    let early_admission = admitted_rx.recv_timeout(Duration::from_millis(100));
    second_release_tx.send(()).unwrap();
    let capacity_admission = admitted_rx.recv_timeout(Duration::from_secs(1));
    // Release all callbacks before asserting, including on a missed wake.
    first_release_tx.send(()).unwrap();
    for _ in 0..=dispatch.capacity {
        finished_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    }
    producer.join().unwrap();
    dispatch.close_and_drain();
    assert!(early_admission.is_err(), "full queue admitted another job");
    assert!(
        capacity_admission.is_ok(),
        "capacity-only completion lost a wake"
    );
    assert_eq!(dispatch.shared.queue.lock().unwrap().jobs, 0);
}

#[test]
fn full_unkeyed_queue_never_executes_on_the_submitting_thread() {
    let dispatch = Arc::new(FuseDispatch::new(1));
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    dispatch.submit(move || {
        started_tx.send(()).unwrap();
        release_rx.recv().unwrap();
    });
    started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    for _ in 1..dispatch.capacity {
        dispatch.submit(|| {});
    }
    let (submitting_tx, submitting_rx) = mpsc::channel();
    let (admitted_tx, admitted_rx) = mpsc::channel();
    let (executed_tx, executed_rx) = mpsc::channel();
    let producer_dispatch = dispatch.clone();
    let producer = std::thread::spawn(move || {
        let producer_id = std::thread::current().id();
        submitting_tx.send(producer_id).unwrap();
        producer_dispatch.submit(move || {
            executed_tx.send(std::thread::current().id()).unwrap();
        });
        admitted_tx.send(()).unwrap();
    });
    let producer_id = submitting_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    let early_admission = admitted_rx.recv_timeout(Duration::from_millis(100));
    // Release every accepted callback before asserting, including on RED.
    release_tx.send(()).unwrap();
    let executed_id = executed_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    producer.join().unwrap();
    dispatch.close_and_drain();
    assert!(
        early_admission.is_err(),
        "full queue did not backpressure ingress"
    );
    assert_ne!(executed_id, producer_id, "full queue executed on ingress");
}

struct RejectedCallbackCapture(mpsc::Sender<()>);

impl Drop for RejectedCallbackCapture {
    fn drop(&mut self) {
        // ReplyRaw is not publicly constructible in fuser. This probe locks
        // the closure ownership boundary: rejecting a callback must drop
        // its captured reply, whose official Drop sends the kernel EIO.
        let _ = self.0.send(());
    }
}

#[test]
fn closing_dispatch_drops_a_blocked_callback_without_running_its_body() {
    for workers in [
        1,
        usize::from(super::FuseExecOptions::default().fuse_workers),
    ] {
        for access in [super::FuseAccess::Exclusive, super::FuseAccess::Read] {
            let dispatch = Arc::new(FuseDispatch::new(workers));
            let (started_tx, started_rx) = mpsc::channel();
            let (release_tx, release_rx) = mpsc::channel();
            // An exclusive predecessor keeps both queued access kinds behind
            // the same handle barrier even with production worker concurrency.
            dispatch.submit_access(51, super::FuseAccess::Exclusive, move || {
                started_tx.send(()).unwrap();
                release_rx.recv().unwrap();
            });
            started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
            for _ in 1..dispatch.capacity {
                dispatch.submit_access(51, access, || {});
            }
            let (submitting_tx, submitting_rx) = mpsc::channel();
            let (returned_tx, returned_rx) = mpsc::channel();
            let (body_tx, body_rx) = mpsc::channel();
            let (dropped_tx, dropped_rx) = mpsc::channel();
            let producer_dispatch = dispatch.clone();
            let producer = std::thread::spawn(move || {
                let captured_reply = RejectedCallbackCapture(dropped_tx);
                submitting_tx.send(()).unwrap();
                producer_dispatch.submit_access(52, access, move || {
                    let _captured_reply = captured_reply;
                    body_tx.send(()).unwrap();
                });
                returned_tx.send(()).unwrap();
            });
            submitting_rx.recv_timeout(Duration::from_secs(1)).unwrap();
            let early_admission = returned_rx.recv_timeout(Duration::from_millis(100));
            let drain_dispatch = dispatch.clone();
            let drainer = std::thread::spawn(move || drain_dispatch.close_and_drain());
            let rejected = returned_rx.recv_timeout(Duration::from_secs(1));
            let captured_reply_dropped = dropped_rx.recv_timeout(Duration::from_secs(1));
            release_tx.send(()).unwrap();
            producer.join().unwrap();
            drainer.join().unwrap();
            assert!(early_admission.is_err(), "full queue admitted a callback");
            assert!(rejected.is_ok(), "closing left an ingress waiter blocked");
            assert!(
                captured_reply_dropped.is_ok(),
                "rejected reply was retained"
            );
            assert!(
                body_rx.try_recv().is_err(),
                "closing executed an unaccepted callback"
            );
            assert_eq!(dispatch.shared.queue.lock().unwrap().jobs, 0);
        }
    }
}

#[test]
fn owned_callback_panic_retires_slot_and_runs_its_fifo_successor() {
    let dispatch = FuseDispatch::new(1);
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    dispatch.submit_keyed(53, move || {
        started_tx.send(()).unwrap();
        release_rx.recv().unwrap();
        panic!("injected owned callback panic");
    });
    started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    let (successor_tx, successor_rx) = mpsc::channel();
    dispatch.submit_keyed(53, move || successor_tx.send(()).unwrap());
    release_tx.send(()).unwrap();
    successor_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    dispatch.close_and_drain();
    assert_eq!(dispatch.shared.queue.lock().unwrap().jobs, 0);
}

#[test]
fn independent_fuse_jobs_overlap_before_either_finishes() {
    let dispatch = FuseDispatch::new(2);
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Arc::new(Mutex::new(release_rx));
    for _ in 0..2 {
        let started_tx = started_tx.clone();
        let release_rx = release_rx.clone();
        dispatch.submit(move || {
            started_tx.send(()).unwrap();
            release_rx.lock().unwrap().recv().unwrap();
        });
    }
    let first = started_rx.recv_timeout(Duration::from_secs(1));
    let second = started_rx.recv_timeout(Duration::from_secs(1));
    release_tx.send(()).unwrap();
    release_tx.send(()).unwrap();
    assert!(
        first.is_ok() && second.is_ok(),
        "jobs remained serial at FUSE ingress"
    );
}

fn wait_for_admitted_jobs(dispatch: &FuseDispatch, count: usize) {
    // Test observation must not share a production worker/capacity wakeup.
    let deadline = std::time::Instant::now() + Duration::from_secs(1);
    loop {
        let jobs = dispatch.shared.queue.lock().unwrap().jobs;
        if jobs >= count {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "expected {count} admitted jobs, got {jobs}"
        );
        std::thread::yield_now();
    }
}

#[test]
fn borrowed_write_keeps_fifo_and_input_address_without_occupying_worker() {
    let dispatch = Arc::new(FuseDispatch::new(1));
    let (events_tx, events_rx) = mpsc::channel();
    let (first_release_tx, first_release_rx) = mpsc::channel();
    let (borrowed_release_tx, borrowed_release_rx) = mpsc::channel();
    let first_tx = events_tx.clone();
    dispatch.submit_keyed(41, move || {
        first_tx.send("write:start").unwrap();
        first_release_rx.recv().unwrap();
        first_tx.send("write:finish").unwrap();
    });
    assert_eq!(
        events_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        "write:start"
    );
    let setattr_tx = events_tx.clone();
    dispatch.submit_keyed(41, move || setattr_tx.send("resize").unwrap());
    let input = [7_u8; 32];
    let input_pointer = input.as_ptr() as usize;
    std::thread::scope(|scope| {
        let borrowed_dispatch = dispatch.clone();
        let borrowed_tx = events_tx.clone();
        let input = &input;
        scope.spawn(move || {
            borrowed_dispatch.run_borrowed_keyed(41, || {
                assert_eq!(input.as_ptr() as usize, input_pointer);
                assert_eq!(input, &[7_u8; 32]);
                borrowed_tx.send("borrowed:start").unwrap();
                borrowed_release_rx.recv().unwrap();
                borrowed_tx.send("borrowed:finish").unwrap();
            });
        });
        wait_for_admitted_jobs(&dispatch, 3);
        let sync_tx = events_tx.clone();
        dispatch.submit_keyed(41, move || sync_tx.send("fsync").unwrap());
        let close_tx = events_tx.clone();
        dispatch.submit_keyed(41, move || close_tx.send("close").unwrap());
        first_release_tx.send(()).unwrap();
        for expected in ["write:finish", "resize", "borrowed:start"] {
            assert_eq!(
                events_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
                expected
            );
        }
        let other_tx = events_tx.clone();
        dispatch.submit_keyed(42, move || other_tx.send("other-handle").unwrap());
        assert_eq!(
            events_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            "other-handle"
        );
        borrowed_release_tx.send(()).unwrap();
        for expected in ["borrowed:finish", "fsync", "close"] {
            assert_eq!(
                events_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
                expected
            );
        }
    });
    dispatch.close_and_drain();
    assert_eq!(dispatch.shared.queue.lock().unwrap().jobs, 0);
}

#[test]
fn kernel_writeback_does_not_hold_the_fuse_receiver() {
    use crate::node::vfs::WriteAdmissionMode;
    let dispatch = Arc::new(FuseDispatch::new(2));
    let receiver = dispatch.clone();
    let (release, blocked) = std::sync::mpsc::channel();
    let (progress, observed) = std::sync::mpsc::channel();
    let (tail, tail_rx) = std::sync::mpsc::channel();
    let receive = std::thread::spawn(move || {
        let write = move || {
            blocked.recv().unwrap();
        };
        match super::fuse_write_admission(
            WriteAdmissionMode::Borrowed,
            fuser::WriteFlags::FUSE_WRITE_CACHE,
        ) {
            WriteAdmissionMode::Borrowed => receiver.run_borrowed_keyed(51, write),
            WriteAdmissionMode::Queued => receiver.submit_keyed(51, write),
        }
        receiver.submit_keyed(52, move || {
            progress.send(()).unwrap();
        });
        receiver.submit_keyed(51, move || {
            tail.send(()).unwrap();
        });
    });
    let independent = observed.recv_timeout(Duration::from_secs(1));
    let premature_sync = tail_rx.try_recv();
    release.send(()).unwrap();
    receive.join().unwrap();
    let sync = tail_rx.recv_timeout(Duration::from_secs(1));
    dispatch.close_and_drain();
    assert!(
        independent.is_ok(),
        "kernel page writeback must not stop admission of another inode's page fault"
    );
    assert!(
        premature_sync.is_err() && sync.is_ok(),
        "same-handle fsync must remain behind the accepted write"
    );
}

#[test]
fn owned_write_input_waits_for_bytes_before_copying() {
    let dispatch = Arc::new(FuseDispatch::new(2));
    let held = dispatch
        .copy_write_input(&vec![7; super::MAX_OWNED_WRITE_INPUT_BYTES])
        .unwrap();
    let other = dispatch.clone();
    let (progress, observed) = std::sync::mpsc::channel();
    let writer = std::thread::spawn(move || {
        let input = other.copy_write_input(&[9]).unwrap();
        progress.send(()).unwrap();
        assert_eq!(input.as_slice(), &[9]);
    });
    let early = observed.recv_timeout(Duration::from_millis(100));
    drop(held);
    if early.is_err() {
        observed.recv_timeout(Duration::from_secs(1)).unwrap();
    }
    writer.join().unwrap();
    dispatch.close_and_drain();
    assert!(
        early.is_err(),
        "owned write inputs must not exceed the byte budget"
    );
    assert_eq!(dispatch.shared.queue.lock().unwrap().write_input_bytes, 0);
}

#[test]
fn owned_write_input_shutdown_wakes_waiters_and_drops_unadmitted_body() {
    let dispatch = Arc::new(FuseDispatch::new(2));
    let held = dispatch
        .copy_write_input(&vec![7; super::MAX_OWNED_WRITE_INPUT_BYTES])
        .unwrap();
    let other = dispatch.clone();
    let (result, observed) = std::sync::mpsc::channel();
    let writer = std::thread::spawn(move || {
        result.send(other.copy_write_input(&[9]).err()).unwrap();
    });
    let early = observed.recv_timeout(Duration::from_millis(100));
    dispatch.close_and_drain();
    let error = match early {
        Ok(error) => error,
        Err(_) => observed.recv_timeout(Duration::from_secs(1)).unwrap(),
    };
    drop(held);
    writer.join().unwrap();
    assert_eq!(error, Some(libc::EIO));
    assert_eq!(dispatch.shared.queue.lock().unwrap().write_input_bytes, 0);
    let fresh = FuseDispatch::new(1);
    let input = fresh.copy_write_input(&[1; 8]).unwrap();
    fresh.close_and_drain();
    fresh.submit_keyed(1, move || {
        drop(input);
        panic!("closed dispatcher ran backend work");
    });
    assert_eq!(fresh.shared.queue.lock().unwrap().write_input_bytes, 0);
}

#[test]
fn borrowed_callback_panic_retires_fifo_slot() {
    let dispatch = FuseDispatch::new(1);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        dispatch.run_borrowed_keyed(43, || panic!("injected callback panic"));
    }));
    assert!(result.is_err());
    let (finished_tx, finished_rx) = mpsc::channel();
    dispatch.submit_keyed(43, move || finished_tx.send(()).unwrap());
    finished_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    dispatch.close_and_drain();
    assert_eq!(dispatch.shared.queue.lock().unwrap().jobs, 0);
}

#[test]
fn drain_waits_for_borrowed_owner_and_queued_successor() {
    let dispatch = Arc::new(FuseDispatch::new(1));
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let borrowed_dispatch = dispatch.clone();
    let borrower = std::thread::spawn(move || {
        borrowed_dispatch.run_borrowed_keyed(44, || {
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        });
    });
    started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    let (successor_tx, successor_rx) = mpsc::channel();
    dispatch.submit_keyed(44, move || successor_tx.send(()).unwrap());
    let drain_dispatch = dispatch.clone();
    let (drained_tx, drained_rx) = mpsc::channel();
    let drainer = std::thread::spawn(move || {
        drain_dispatch.close_and_drain();
        drained_tx.send(()).unwrap();
    });
    let early_drain = drained_rx.recv_timeout(Duration::from_millis(50));
    release_tx.send(()).unwrap();
    successor_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    drained_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    borrower.join().unwrap();
    drainer.join().unwrap();
    assert!(early_drain.is_err());
    assert_eq!(dispatch.shared.queue.lock().unwrap().jobs, 0);
}

#[test]
fn same_handle_callbacks_keep_order_while_other_handles_progress() {
    let dispatch = FuseDispatch::new(2);
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let first_tx = started_tx.clone();
    dispatch.submit_keyed(11, move || {
        first_tx.send("first").unwrap();
        release_rx.recv().unwrap();
    });
    assert_eq!(
        started_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        "first"
    );
    let second_tx = started_tx.clone();
    dispatch.submit_keyed(11, move || {
        second_tx.send("second").unwrap();
    });
    dispatch.submit_keyed(12, move || {
        started_tx.send("other").unwrap();
    });
    assert_eq!(
        started_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        "other"
    );
    release_tx.send(()).unwrap();
    assert_eq!(
        started_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        "second"
    );
}

#[test]
fn queued_same_handle_attribute_callbacks_do_not_block_unrelated_handles() {
    let dispatch = FuseDispatch::new(2);
    let (events_tx, events_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();

    let first_tx = events_tx.clone();
    dispatch.submit_keyed(21, move || {
        first_tx.send("write:start").unwrap();
        release_rx.recv().unwrap();
        first_tx.send("write:finish").unwrap();
    });
    assert_eq!(
        events_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        "write:start"
    );

    let setattr_tx = events_tx.clone();
    let before_setattr_enqueue = std::time::Instant::now();
    dispatch.submit_keyed(21, move || {
        setattr_tx.send("setattr").unwrap();
    });
    assert!(
        before_setattr_enqueue.elapsed() < Duration::from_millis(100),
        "same-handle setattr enqueue blocked the FUSE receive path"
    );

    let getattr_tx = events_tx.clone();
    dispatch.submit_keyed(22, move || {
        getattr_tx.send("getattr:other-handle").unwrap();
    });
    assert_eq!(
        events_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        "getattr:other-handle",
        "queued setattr for one fh blocked another fh"
    );

    release_tx.send(()).unwrap();
    assert_eq!(
        events_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        "write:finish"
    );
    assert_eq!(
        events_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        "setattr"
    );
}

#[test]
fn official_fuser_delivery_does_not_request_lock_or_killpriv_capabilities() {
    let flags = super::requested_init_flags();
    assert!(flags.contains(fuser::InitFlags::FUSE_PARALLEL_DIROPS));
    assert!(flags.contains(fuser::InitFlags::FUSE_DIRECT_IO_ALLOW_MMAP));
    assert!(!flags.contains(fuser::InitFlags::FUSE_POSIX_LOCKS));
    assert!(!flags.contains(fuser::InitFlags::FUSE_FLOCK_LOCKS));
    assert!(!flags.contains(fuser::InitFlags::FUSE_HANDLE_KILLPRIV));
    assert!(!flags.contains(fuser::InitFlags::FUSE_HANDLE_KILLPRIV_V2));
}

#[test]
fn init_capabilities_are_requested_independently() {
    let available = fuser::InitFlags::FUSE_DIRECT_IO_ALLOW_MMAP;
    let negotiated = super::negotiated_init_flags(available)
        .fold(fuser::InitFlags::empty(), |flags, capability| {
            flags | capability
        });
    assert!(negotiated.contains(fuser::InitFlags::FUSE_DIRECT_IO_ALLOW_MMAP));
    assert!(!negotiated.contains(fuser::InitFlags::FUSE_PARALLEL_DIROPS));
}

#[test]
fn fuser_lock_callbacks_reject_user_space_locking_explicitly() {
    assert_eq!(
        super::unsupported_lock_errno(0, 99, libc::F_RDLCK),
        libc::EOPNOTSUPP
    );
    assert_eq!(
        super::unsupported_lock_errno(10, 9, libc::F_WRLCK),
        libc::EINVAL
    );
    assert_eq!(super::unsupported_lock_errno(0, 9, -1), libc::EINVAL);
}
