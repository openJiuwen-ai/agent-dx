use super::*;
use crate::{dfs::OperationId, node::chunk::StagedChunk};
use std::os::unix::fs::{PermissionsExt, symlink};

fn options(directory: PathBuf) -> PeerCacheOptions {
    PeerCacheOptions {
        directory,
        capacity_bytes: 256 * 1024,
        namespace_id: NamespaceId::new("test-namespace"),
    }
}

fn pending_files(cache: &DiskPeerCache) -> Vec<PathBuf> {
    fs::read_dir(cache.directory())
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(valid_pending_name)
        })
        .collect()
}

fn fill(cache: &DiskPeerCache, staged: &StagedChunk) -> Result<bool> {
    cache
        .operation()?
        .store_verified(&VerifiedPeerChunk::verify(
            &staged.chunk,
            staged.bytes(),
            cache.proof_budget(),
        )?)
}

#[test]
fn r7_background_cache_admission_does_not_wait_or_evict_for_space() {
    let temp = tempfile::tempdir().unwrap();
    let cache = Arc::new(DiskPeerCache::open(options(temp.path().join("cache"))).unwrap());
    let staged = StagedChunk::new(OperationId::new("r7-background"), vec![17; 64 * 1024]);
    let mut operation = cache.operation().unwrap();
    let state = cache.state.lock().unwrap();
    assert!(cache.try_operation().unwrap().is_none());
    assert!(cache.try_fill_flight(&staged.chunk).unwrap().is_none());
    assert!(!operation.try_acquire_stream_fill().unwrap());
    drop(state);
    let waiting = FillWaiter::new(&cache).unwrap();
    assert!(cache.try_operation().unwrap().is_none());
    assert!(cache.try_fill_flight(&staged.chunk).unwrap().is_none());
    assert!(!operation.try_acquire_stream_fill().unwrap());
    drop(waiting);
    drop(operation);
    assert!(fill(&cache, &staged).unwrap());
    let used = cache.state.lock().unwrap().used_bytes;
    // A different object cannot evict this healthy entry to run ahead.
    let next = StagedChunk::new(OperationId::new("r7-next"), vec![29; 256 * 1024]);
    let mut operation = cache.try_operation().unwrap().unwrap();
    let sink = operation.try_begin_stream(&next.chunk).unwrap().unwrap();
    assert!(!sink.is_caching());
    drop(sink);
    drop(operation);
    assert_eq!(cache.try_contains_ready(&staged.chunk).unwrap(), Some(true));
    assert_eq!(cache.state.lock().unwrap().used_bytes, used);
    assert!(pending_files(&cache).is_empty());
}

#[test]
fn disk_peer_cache_different_object_stream_fills_overlap_without_sharing_pending() {
    use std::{sync::mpsc, thread, time::Duration};

    let temp = tempfile::tempdir().unwrap();
    let mut config = options(temp.path().join("cache"));
    config.capacity_bytes = 1024 * 1024;
    let cache = DiskPeerCache::open(config).unwrap();
    let first = StagedChunk::new(OperationId::new("f2-first"), vec![17; 64 * 1024]);
    let second = StagedChunk::new(OperationId::new("f2-second"), vec![29; 64 * 1024]);
    let (first_ready_tx, first_ready_rx) = mpsc::channel();
    let (release_first_tx, release_first_rx) = mpsc::channel();
    let (second_ready_tx, second_ready_rx) = mpsc::channel();
    let (release_second_tx, release_second_rx) = mpsc::channel();
    let deadline = Duration::from_secs(5);
    let overlapped = thread::scope(|scope| {
        let cache_ref = &cache;
        let first = &first;
        let first_task = scope.spawn(move || {
            let mut operation = cache_ref.operation().unwrap();
            let mut sink = operation.begin_stream(&first.chunk).unwrap();
            assert!(sink.file.is_some());
            first_ready_tx.send(()).unwrap();
            release_first_rx.recv_timeout(deadline).unwrap();
            let mut out = [0; 8];
            let proof = crate::node::rpc::peer::complete_peer_disk_read_for_test(
                &first.chunk,
                first.bytes(),
                0,
                &mut out,
                &mut sink,
            )
            .unwrap();
            assert!(sink.commit(proof).unwrap());
        });
        first_ready_rx.recv_timeout(deadline).unwrap();
        let cache_ref = &cache;
        let second = &second;
        let second_task = scope.spawn(move || {
            let mut operation = cache_ref.operation().unwrap();
            let mut sink = operation.begin_stream(&second.chunk).unwrap();
            assert!(sink.file.is_some());
            second_ready_tx.send(()).unwrap();
            release_second_rx.recv_timeout(deadline).unwrap();
            let mut out = [0; 8];
            let proof = crate::node::rpc::peer::complete_peer_disk_read_for_test(
                &second.chunk,
                second.bytes(),
                0,
                &mut out,
                &mut sink,
            )
            .unwrap();
            assert!(sink.commit(proof).unwrap());
        });
        // Always release and join before asserting: the old single allowance
        // fails this progress contract without leaving a blocked test thread.
        let overlapped = second_ready_rx.recv_timeout(Duration::from_secs(2)).is_ok();
        release_first_tx.send(()).unwrap();
        if !overlapped {
            second_ready_rx.recv_timeout(deadline).unwrap();
        }
        release_second_tx.send(()).unwrap();
        first_task.join().unwrap();
        second_task.join().unwrap();
        overlapped
    });
    assert_eq!(cache.stats().unwrap().entries, 2);
    assert!(
        overlapped,
        "different-object stream fills waited for one global fill"
    );
    for (staged, byte) in [(&first, 17), (&second, 29)] {
        let mut out = [0; 8];
        assert!(
            cache
                .operation()
                .unwrap()
                .read_range(&staged.chunk, 0, &mut out)
                .unwrap()
        );
        assert_eq!(out, [byte; 8]);
    }
}

#[test]
fn disk_peer_cache_same_object_follower_waits_for_its_single_flight() {
    use std::{sync::mpsc, thread, time::Duration};

    let temp = tempfile::tempdir().unwrap();
    let cache = DiskPeerCache::open(options(temp.path().join("cache"))).unwrap();
    let staged = StagedChunk::new(OperationId::new("f2-same"), vec![17; 64 * 1024]);
    let FillRole::Leader(mut leader) = cache.fill_flight(&staged.chunk, || Ok(())).unwrap() else {
        panic!("first request must lead");
    };
    let (entered_tx, entered_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    thread::scope(|scope| {
        let follower = scope.spawn(|| {
            let outcome = cache
                .fill_flight(&staged.chunk, || {
                    let _ = entered_tx.send(());
                    Ok(())
                })
                .unwrap();
            assert!(matches!(outcome, FillRole::Finished(FillOutcome::Uncached)));
            done_tx.send(()).unwrap();
        });
        entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert!(done_rx.try_recv().is_err());
        assert_eq!(cache.state.lock().unwrap().flight_live, 1);
        leader.finish(FillOutcome::Uncached);
        done_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        follower.join().unwrap();
    });
    drop(leader);
    assert_eq!(cache.state.lock().unwrap().flight_live, 0);
}

#[test]
fn disk_peer_cache_stream_limit_cancel_and_legacy_exclusivity() {
    use std::{sync::mpsc, thread};
    let temp = tempfile::tempdir().unwrap();
    let cache = DiskPeerCache::open(options(temp.path().join("cache"))).unwrap();
    let mut first = cache.operation().unwrap();
    let mut second = cache.operation().unwrap();
    first.acquire_stream_fill(|| Ok(())).unwrap();
    second.acquire_stream_fill(|| Ok(())).unwrap();
    let deadline = Instant::now() + Duration::from_millis(30);
    let mut third = cache.operation().unwrap();
    assert!(
        third
            .acquire_stream_fill(|| {
                if Instant::now() >= deadline {
                    Err(unavailable("cancelled third stream"))
                } else {
                    Ok(())
                }
            })
            .is_err()
    );
    assert_eq!(cache.state.lock().unwrap().stream_active, 2);
    let (checked_tx, checked_rx) = mpsc::channel();
    let (admitted_tx, admitted_rx) = mpsc::channel();
    thread::scope(|scope| {
        let cache_ref = &cache;
        let legacy = scope.spawn(move || {
            let mut operation = cache_ref.operation().unwrap();
            operation
                .acquire_fill(|| {
                    let _ = checked_tx.send(());
                    Ok(())
                })
                .unwrap();
            admitted_tx.send(()).unwrap();
        });
        checked_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        drop(first);
        assert!(
            admitted_rx.recv_timeout(Duration::from_millis(50)).is_err(),
            "legacy must wait for the final streaming owner"
        );
        drop(second);
        admitted_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        legacy.join().unwrap();
    });
    assert_eq!(cache.state.lock().unwrap().stream_active, 0);
    let mut legacy = cache.operation().unwrap();
    legacy.acquire_fill(|| Ok(())).unwrap();
    let deadline = Instant::now() + Duration::from_millis(30);
    assert!(
        third
            .acquire_stream_fill(|| {
                if Instant::now() >= deadline {
                    Err(unavailable("cancelled behind legacy"))
                } else {
                    Ok(())
                }
            })
            .is_err()
    );
    assert!(cache.state.lock().unwrap().fill_active);
    cache.close_admission();
    assert!(third.acquire_stream_fill(|| Ok(())).is_err());
    drop(legacy);
    assert!(!cache.state.lock().unwrap().fill_active);
}

#[test]
fn disk_peer_cache_pending_generations_keep_failed_charge_and_other_fill_private() {
    let temp = tempfile::tempdir().unwrap();
    let mut config = options(temp.path().join("cache"));
    config.capacity_bytes = 512 * 1024;
    let cache = DiskPeerCache::open(config).unwrap();
    let first = StagedChunk::new(OperationId::new("f2-retained"), vec![17; 64 * 1024]);
    let second = StagedChunk::new(OperationId::new("f2-isolated"), vec![29; 64 * 1024]);
    let mut first_op = cache.operation().unwrap();
    let first_sink = first_op.begin_stream(&first.chunk).unwrap();
    let first_path = cache.pending_path(first_sink.pending_generation);
    let first_charge = cache.state.lock().unwrap().pending[&first_sink.pending_generation].charge;
    let mut second_op = cache.operation().unwrap();
    let mut second_sink = second_op.begin_stream(&second.chunk).unwrap();
    let second_path = cache.pending_path(second_sink.pending_generation);
    assert_ne!(first_path, second_path);
    assert!(first_path.exists() && second_path.exists());
    assert_eq!(cache.state.lock().unwrap().pending.len(), 2);
    assert!(cache.stats().unwrap().disk_bytes <= cache.options.capacity_bytes);
    cache.state.lock().unwrap().fail_remove = true;
    drop(first_sink);
    drop(first_op);
    cache.state.lock().unwrap().fail_remove = false;
    assert!(first_path.exists());
    let mut out = [0; 8];
    let proof = crate::node::rpc::peer::complete_peer_disk_read_for_test(
        &second.chunk,
        second.bytes(),
        0,
        &mut out,
        &mut second_sink,
    )
    .unwrap();
    assert!(second_sink.commit(proof).unwrap());
    drop(second_sink);
    drop(second_op);
    let state = cache.state.lock().unwrap();
    assert_eq!(state.pending_charge, first_charge);
    assert_eq!(state.pending.len(), 1);
    assert!(
        state.pending_metadata_charge > 0,
        "failed deletion retains its metadata lease"
    );
    assert_eq!(
        state.used_bytes,
        first_charge
            + state
                .entries
                .values()
                .map(|entry| entry.charge)
                .sum::<u64>()
    );
    drop(state);
    assert!(first_path.exists());
    assert!(!second_path.exists());
    cache.cleanup_pending().unwrap();
    assert!(!first_path.exists());
    assert_eq!(cache.state.lock().unwrap().pending_charge, 0);
}

#[test]
fn disk_peer_cache_replaced_pending_is_preserved_without_touching_another_fill() {
    let temp = tempfile::tempdir().unwrap();
    let mut config = options(temp.path().join("cache"));
    config.capacity_bytes = 512 * 1024;
    let cache = DiskPeerCache::open(config).unwrap();
    let first = StagedChunk::new(OperationId::new("f2-replaced"), vec![17; 64 * 1024]);
    let second = StagedChunk::new(OperationId::new("f2-preserved"), vec![29; 64 * 1024]);
    let mut first_op = cache.operation().unwrap();
    let first_sink = first_op.begin_stream(&first.chunk).unwrap();
    let first_path = cache.pending_path(first_sink.pending_generation);
    let mut second_op = cache.operation().unwrap();
    let second_sink = second_op.begin_stream(&second.chunk).unwrap();
    let second_path = cache.pending_path(second_sink.pending_generation);
    fs::remove_file(&first_path).unwrap();
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&first_path)
        .unwrap()
        .write_all(b"foreign replacement")
        .unwrap();
    drop(first_sink);
    assert_eq!(fs::read(&first_path).unwrap(), b"foreign replacement");
    assert!(second_path.exists());
    assert!(cache.state.lock().unwrap().pending_charge > 0);
    drop(second_sink);
    assert!(!second_path.exists());
    assert_eq!(fs::read(&first_path).unwrap(), b"foreign replacement");
}

#[test]
fn disk_peer_cache_blocked_fill_does_not_block_another_cached_hit() {
    use std::{sync::mpsc, thread, time::Duration};

    let temp = tempfile::tempdir().unwrap();
    let mut config = options(temp.path().join("cache"));
    config.capacity_bytes = 1024 * 1024;
    let cache = DiskPeerCache::open(config).unwrap();
    let cached = StagedChunk::new(OperationId::new("r5-cached-hit"), vec![73; 64 * 1024]);
    let incoming = StagedChunk::new(OperationId::new("r5-blocked-fill"), vec![91; 64 * 1024]);
    assert!(fill(&cache, &cached).unwrap());
    let (fill_ready_tx, fill_ready_rx) = mpsc::channel();
    let (release_fill_tx, release_fill_rx) = mpsc::channel();
    let (hit_started_tx, hit_started_rx) = mpsc::channel();
    let (hit_finished_tx, hit_finished_rx) = mpsc::channel();
    let deadline = Duration::from_secs(5);

    let hit_completed_before_release = thread::scope(|scope| {
        let cache_ref = &cache;
        let incoming_ref = &incoming;
        let fill_thread = scope.spawn(move || {
            let mut operation = cache_ref.operation().unwrap();
            let mut sink = operation.begin_stream(&incoming_ref.chunk).unwrap();
            assert!(
                sink.file.is_some(),
                "fixture must own an actual pending file"
            );
            fill_ready_tx.send(()).unwrap();
            // Model a Peer response paused before its body arrives. Keep the
            // real prepared sink alive, rather than duplicating cache locks.
            release_fill_rx.recv_timeout(deadline).unwrap();
            let mut output = [0; 8];
            let proof = crate::node::rpc::peer::complete_peer_disk_read_for_test(
                &incoming_ref.chunk,
                incoming_ref.bytes(),
                0,
                &mut output,
                &mut sink,
            )
            .unwrap();
            assert_eq!(proof.private_output(), &[91; 8]);
            assert!(sink.commit(proof).unwrap());
        });
        fill_ready_rx.recv_timeout(deadline).unwrap();

        let cache_ref = &cache;
        let cached_ref = &cached;
        let hit_thread = scope.spawn(move || {
            hit_started_tx.send(()).unwrap();
            let mut output = [0; 4096];
            assert!(
                cache_ref
                    .operation()
                    .unwrap()
                    .read_range(&cached_ref.chunk, 4096, &mut output)
                    .unwrap()
            );
            assert_eq!(output, [73; 4096]);
            hit_finished_tx.send(()).unwrap();
        });
        hit_started_rx.recv_timeout(deadline).unwrap();
        // This timeout is a bounded progress assertion, not a latency gate.
        // Release and join both threads before reporting the old lock bug.
        let completed = hit_finished_rx.recv_timeout(Duration::from_secs(2)).is_ok();
        release_fill_tx.send(()).unwrap();
        fill_thread.join().unwrap();
        hit_thread.join().unwrap();
        completed
    });
    assert_eq!(cache.stats().unwrap().entries, 2);
    assert!(
        hit_completed_before_release,
        "another object's cached hit waited for an unrelated in-flight fill"
    );
}

#[test]
fn disk_peer_cache_retired_pin_keeps_disk_charge_and_blocks_same_key_replacement() {
    let temp = tempfile::tempdir().unwrap();
    let cache = DiskPeerCache::open(options(temp.path().join("cache"))).unwrap();
    let staged = StagedChunk::new(OperationId::new("r5-retired-pin"), vec![73; 64 * 1024]);
    assert!(fill(&cache, &staged).unwrap());
    let key = cache.key(&staged.chunk).unwrap();
    let generation = cache
        .state
        .lock()
        .unwrap()
        .entries
        .get(&key)
        .unwrap()
        .generation
        .clone();
    let pin = EntryPin {
        cache: &cache,
        generation: Some(generation),
    };
    let (mut file, before) = open_private(&cache.path(&key)).unwrap();
    let (_, body_start) = read_header(&mut file).unwrap();
    let charged = cache.stats().unwrap().disk_bytes;
    cache.retire(&key, pin.generation.as_ref(), true).unwrap();
    assert_eq!(cache.stats().unwrap().entries, 0);
    assert_eq!(cache.stats().unwrap().disk_bytes, charged);
    assert!(
        !fill(&cache, &staged).unwrap(),
        "cannot reuse a pinned generation's ready path"
    );
    let mut out = [0; 4096];
    scan_body(
        &mut file,
        body_start,
        &staged.chunk,
        0,
        &mut out,
        &mut PeerCacheStats::default(),
        None,
    )
    .unwrap();
    assert_eq!(out, [73; 4096]);
    assert_eq!(
        file_identity(&before),
        file_identity(&file.metadata().unwrap())
    );
    drop(file);
    drop(pin);
    assert!(!cache.path(&key).exists());
    assert_eq!(cache.stats().unwrap().disk_bytes, 0);
    assert!(cache.state.lock().unwrap().retired.is_empty());
    assert!(fill(&cache, &staged).unwrap());
}

#[test]
fn disk_peer_cache_close_wakes_fill_waiter_without_refunding_active_fill() {
    use std::{sync::mpsc, thread, time::Duration};
    let temp = tempfile::tempdir().unwrap();
    let cache = DiskPeerCache::open(options(temp.path().join("cache"))).unwrap();
    let mut owner = cache.operation().unwrap();
    owner.acquire_fill(|| Ok(())).unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    thread::scope(|scope| {
        let waiter = scope.spawn(|| {
            let mut operation = cache.operation().unwrap();
            started_tx.send(()).unwrap();
            operation.acquire_fill(|| Ok(()))
        });
        started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        cache.close_admission();
        assert!(waiter.join().unwrap().is_err());
        assert!(cache.state.lock().unwrap().fill_active);
    });
    drop(owner);
    assert!(!cache.state.lock().unwrap().fill_active);
    cache.shutdown().unwrap();
    let state = cache.state.lock().unwrap();
    assert_eq!(state.operations, 0);
    assert!(state.flights.is_empty());
}

#[test]
fn disk_peer_cache_flight_drop_and_repeated_rounds_leave_no_slots() {
    let temp = tempfile::tempdir().unwrap();
    let cache = DiskPeerCache::open(options(temp.path().join("cache"))).unwrap();
    let staged = StagedChunk::new(OperationId::new("r5-flight-drop"), vec![73; 64 * 1024]);
    for _ in 0..32 {
        let FillRole::Leader(mut leader) = cache.fill_flight(&staged.chunk, || Ok(())).unwrap()
        else {
            panic!("fresh flight must have one leader");
        };
        leader.finish(FillOutcome::Failed(unavailable("bounded shared failure")));
        let mut checks = 0;
        // Same-thread follower observes the terminal data outcome, while
        // retaining a separate authorization callback.
        let check_count = std::cell::Cell::new(0);
        match cache
            .fill_flight(&staged.chunk, || {
                check_count.set(check_count.get() + 1);
                Ok(())
            })
            .unwrap()
        {
            FillRole::Finished(FillOutcome::Failed(error)) => {
                assert_eq!(error, unavailable("bounded shared failure"));
                checks += check_count.get();
            }
            _ => panic!("follower must observe the same shared data failure"),
        }
        assert!(checks >= 2);
        drop(leader);
        let state = cache.state.lock().unwrap();
        assert_eq!(state.flight_live, 0);
        assert!(state.flights.is_empty());
    }
    let leader = cache.fill_flight(&staged.chunk, || Ok(())).unwrap();
    drop(leader);
    assert_eq!(cache.state.lock().unwrap().flight_live, 0);
}

#[test]
fn disk_peer_cache_failed_retired_delete_preserves_slots_and_index_until_retry() {
    let temp = tempfile::tempdir().unwrap();
    let cache = DiskPeerCache::open(options(temp.path().join("cache"))).unwrap();
    let staged = StagedChunk::new(
        OperationId::new("r5-stop-delete-failure"),
        vec![73; 64 * 1024],
    );
    assert!(fill(&cache, &staged).unwrap());
    let key = cache.key(&staged.chunk).unwrap();
    let before = cache.stats().unwrap().disk_bytes;
    cache.state.lock().unwrap().fail_remove = true;
    assert!(cache.retire(&key, None, true).is_err());
    assert!(cache.shutdown().is_err());
    {
        let state = cache.state.lock().unwrap();
        assert!(state.index_lease.is_some());
        assert_eq!(state.retired.len(), 1);
        assert_eq!(state.retired_disk_bytes, before);
        assert_eq!(state.used_bytes, before);
    }
    cache.state.lock().unwrap().fail_remove = false;
    cache.shutdown().unwrap();
    let state = cache.state.lock().unwrap();
    assert!(state.retired.is_empty());
    assert!(state.index_lease.is_none());
    assert_eq!(state.retired_disk_bytes, 0);
    assert_eq!(state.entry_metadata_bytes, 0);
    assert_eq!(state.used_bytes, 0);
}

#[test]
fn disk_peer_cache_pinned_foreign_replacement_is_never_unlinked_or_refunded() {
    let temp = tempfile::tempdir().unwrap();
    let cache = DiskPeerCache::open(options(temp.path().join("cache"))).unwrap();
    let staged = StagedChunk::new(OperationId::new("r5-foreign-retired"), vec![73; 64 * 1024]);
    assert!(fill(&cache, &staged).unwrap());
    let key = cache.key(&staged.chunk).unwrap();
    let generation = cache
        .state
        .lock()
        .unwrap()
        .entries
        .get(&key)
        .unwrap()
        .generation
        .clone();
    let pin = EntryPin {
        cache: &cache,
        generation: Some(generation),
    };
    let before = cache.stats().unwrap().disk_bytes;
    cache.retire(&key, pin.generation.as_ref(), true).unwrap();
    let original = temp.path().join("retired-original");
    fs::rename(cache.path(&key), &original).unwrap();
    fs::write(cache.path(&key), b"foreign replacement").unwrap();
    drop(pin);
    assert_eq!(fs::read(cache.path(&key)).unwrap(), b"foreign replacement");
    assert_eq!(cache.stats().unwrap().disk_bytes, before);
    assert_eq!(cache.state.lock().unwrap().retired.len(), 1);
    fs::remove_file(cache.path(&key)).unwrap();
    fs::rename(original, cache.path(&key)).unwrap();
    cache.cleanup_retired().unwrap();
    assert_eq!(cache.stats().unwrap().disk_bytes, 0);
}

#[test]
fn disk_peer_cache_flights_are_transient_bounded_and_close_wakes_admission() {
    use std::{sync::mpsc, thread, time::Duration};
    let temp = tempfile::tempdir().unwrap();
    let cache = DiskPeerCache::open(options(temp.path().join("cache"))).unwrap();
    let resident_before = cache.proof_budget.resident_used();
    let mut leaders = Vec::new();
    for index in 0..8 {
        let staged = StagedChunk::new(
            OperationId::new(format!("r5-flight-{index}")),
            vec![index; 64],
        );
        leaders.push(cache.fill_flight(&staged.chunk, || Ok(())).unwrap());
    }
    assert_eq!(cache.state.lock().unwrap().flight_live, 8);
    assert_eq!(cache.proof_budget.resident_used(), resident_before);
    let ninth = StagedChunk::new(OperationId::new("r5-flight-ninth"), vec![91; 64]);
    let (started_tx, started_rx) = mpsc::channel();
    thread::scope(|scope| {
        let waiting = scope.spawn(|| {
            cache.fill_flight(&ninth.chunk, || {
                started_tx.send(()).unwrap();
                Ok(())
            })
        });
        started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        cache.close_admission();
        assert!(waiting.join().unwrap().is_err());
    });
    drop(leaders);
    cache.shutdown().unwrap();
    assert_eq!(cache.proof_budget.used(), 0);
}

#[test]
fn disk_peer_cache_small_hit_verifies_only_32k_coverage_group() {
    let temp = tempfile::tempdir().unwrap();
    let mut config = options(temp.path().join("cache"));
    config.capacity_bytes = 8 * 1024 * 1024;
    let cache = DiskPeerCache::open(config).unwrap();
    let staged = StagedChunk::new(
        OperationId::new("outboard-small-hit"),
        vec![73; 4 * 1024 * 1024],
    );
    assert!(fill(&cache, &staged).unwrap());
    let before = cache.stats().unwrap();
    let mut out = [0; 4096];
    assert!(
        cache
            .operation()
            .unwrap()
            .read_range(&staged.chunk, 4096, &mut out)
            .unwrap()
    );
    let after = cache.stats().unwrap();
    assert_eq!(out, [73; 4096]);
    assert_eq!(after.hash_bytes_total - before.hash_bytes_total, 32 * 1024);
    assert_eq!(
        after.hash_work.cache_fullscan_bytes,
        before.hash_work.cache_fullscan_bytes
    );
}

#[test]
fn disk_peer_cache_v2_body_has_fixed_offset_and_trailer() {
    let temp = tempfile::tempdir().unwrap();
    let cache = DiskPeerCache::open(options(temp.path().join("cache"))).unwrap();
    let staged = StagedChunk::new(OperationId::new("outboard-format"), vec![39; 64 * 1024]);
    assert!(fill(&cache, &staged).unwrap());
    let bytes = fs::read(cache.path(&cache.key(&staged.chunk).unwrap())).unwrap();
    assert_eq!(&bytes[..8], b"AFSPC002");
    assert_eq!(&bytes[4096..4096 + staged.bytes().len()], staged.bytes());
    assert!(bytes.len() >= 4096 + staged.bytes().len() + 64);
}

#[test]
fn disk_peer_cache_rejects_half_or_forged_outboard_trailer() {
    for truncated in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let cache = DiskPeerCache::open(options(temp.path().join("cache"))).unwrap();
        let staged = StagedChunk::new(OperationId::new("outboard-corrupt"), vec![47; 64 * 1024]);
        assert!(fill(&cache, &staged).unwrap());
        let path = cache.path(&cache.key(&staged.chunk).unwrap());
        let before = file_identity(&fs::metadata(&path).unwrap());
        let mut bytes = fs::read(&path).unwrap();
        if truncated {
            bytes.truncate(bytes.len() - 16);
        } else {
            *bytes.last_mut().unwrap() ^= 1;
        }
        fs::write(&path, bytes).unwrap();
        // Cached proof reuse is valid until file identity changes. Make
        // trailer revalidation explicit instead of relying on tick timing.
        File::open(&path)
            .unwrap()
            .set_modified(std::time::SystemTime::UNIX_EPOCH)
            .unwrap();
        assert_ne!(before, file_identity(&fs::metadata(&path).unwrap()));
        let mut out = [0; 19];
        assert!(
            !cache
                .operation()
                .unwrap()
                .read_range(&staged.chunk, 0, &mut out)
                .unwrap()
        );
        assert!(!path.exists());
        assert_eq!(cache.stats().unwrap().entries, 0);
    }
}

#[test]
fn disk_peer_cache_outboard_never_replaces_trusted_meta_root() {
    let temp = tempfile::tempdir().unwrap();
    let cache = DiskPeerCache::open(options(temp.path().join("cache"))).unwrap();
    let staged = StagedChunk::new(OperationId::new("outboard-meta-root"), vec![49; 64 * 1024]);
    assert!(fill(&cache, &staged).unwrap());
    let held = cache
        .state
        .lock()
        .unwrap()
        .entries
        .values()
        .next()
        .unwrap()
        .proof
        .as_ref()
        .unwrap()
        .outboard
        .clone();
    let mut changed = staged.chunk.clone();
    changed.content_digest.bytes[0] ^= 1;
    assert!(validate_outboard(&changed, &held).is_err());
    let mut output = [0; 19];
    assert!(
        !cache
            .operation()
            .unwrap()
            .read_range(&changed, 0, &mut output)
            .unwrap()
    );
    assert_eq!(output, [0; 19]);
}

#[test]
fn disk_peer_cache_drop_releases_entries_before_index_reservation() {
    let temp = tempfile::tempdir().unwrap();
    let budget = OutboardBudget::new(MAX_PEER_CACHE_PROOF_BYTES as usize);
    let observed = Arc::new(std::sync::atomic::AtomicUsize::new(usize::MAX));
    let cache = DiskPeerCache::open_with_outboard_budget(
        options(temp.path().join("cache")),
        budget.clone(),
    )
    .unwrap();
    let staged = StagedChunk::new(OperationId::new("outboard-drop-order"), vec![79; 64 * 1024]);
    assert!(fill(&cache, &staged).unwrap());
    cache
        .state
        .lock()
        .unwrap()
        .entries
        .values_mut()
        .next()
        .unwrap()
        .drop_observation = Some(DropObservation {
        budget: budget.clone(),
        observed: observed.clone(),
    });
    drop(cache);
    let pointers =
        MAX_PEER_CACHE_ENTRIES * std::mem::size_of::<Option<Box<VerifiedDiskOutboard>>>();
    assert_eq!(
        observed.load(std::sync::atomic::Ordering::SeqCst),
        pointers,
        "entry destructor must still observe its pointer-index reservation"
    );
    assert_eq!(budget.used(), 0);
}

#[test]
fn disk_peer_cache_shared_budget_pressure_falls_back_without_changing_read_result() {
    let temp = tempfile::tempdir().unwrap();
    let budget = OutboardBudget::new(MAX_PEER_CACHE_PROOF_BYTES as usize);
    let cache = DiskPeerCache::open_with_outboard_budget(
        options(temp.path().join("cache")),
        budget.clone(),
    )
    .unwrap();
    let staged = StagedChunk::new(OperationId::new("outboard-pressure"), vec![67; 64 * 1024]);
    assert!(fill(&cache, &staged).unwrap());
    let held = budget.try_reserve(budget.limit() - budget.used()).unwrap();
    let mut out = [0; 19];
    assert!(
        cache
            .operation()
            .unwrap()
            .read_range(&staged.chunk, 0, &mut out)
            .unwrap()
    );
    assert_eq!(out, [67; 19]);
    assert_eq!(
        cache.stats().unwrap().hash_work.cache_fullscan_bytes,
        staged.chunk.length
    );
    assert_eq!(budget.used(), budget.limit());
    drop(held);
}

#[test]
fn pr60_natural_resident_pressure_preserves_shared_working_headroom() {
    use std::sync::{
        Barrier,
        atomic::{AtomicUsize, Ordering},
    };
    let temp = tempfile::tempdir().unwrap();
    let budget = OutboardBudget::new(MAX_PEER_CACHE_PROOF_BYTES as usize);
    let mut config = options(temp.path().join("cache"));
    config.capacity_bytes = 64 * 1024 * 1024;
    let cache = DiskPeerCache::open_with_outboard_budget(config, budget.clone()).unwrap();
    let index_charge = budget.used();
    let first = StagedChunk::new(OperationId::new("resident-first"), vec![0; 4096]);
    assert!(fill(&cache, &first).unwrap());
    let one_proof = budget.used() - index_charge;
    assert!(one_proof > 0);
    // Use actual natural proof ownership, not an injected global reservation.
    // Unique 4KiB objects keep real body data below 16MiB / 4096 entries.
    let objects = (budget.limit() / 2) / one_proof + 16;
    assert!(objects < MAX_PEER_CACHE_ENTRIES);
    for index in 1..objects {
        let mut bytes = vec![0; 4096];
        bytes[..8].copy_from_slice(&(index as u64).to_le_bytes());
        let staged = StagedChunk::new(OperationId::new(format!("resident-{index}")), bytes);
        assert!(fill(&cache, &staged).unwrap());
    }
    assert_eq!(
        cache.stats().unwrap().entries,
        objects,
        "proof retention may retire metadata, never healthy body entries"
    );
    assert!(
        budget.used() <= budget.limit() / 2,
        "natural resident proofs consumed the working half: used={} total={}",
        budget.used(),
        budget.limit()
    );

    let ready = Arc::new(Barrier::new(9));
    let release = Arc::new(Barrier::new(9));
    let peak = Arc::new(AtomicUsize::new(budget.used()));
    let mut threads = Vec::new();
    for _ in 0..8 {
        let (budget, ready, release, peak) =
            (budget.clone(), ready.clone(), release.clone(), peak.clone());
        threads.push(std::thread::spawn(move || {
            let work = budget.try_reserve(64 * 1024);
            peak.fetch_max(budget.used(), Ordering::AcqRel);
            ready.wait();
            release.wait();
            work.is_some()
        }));
    }
    ready.wait();
    release.wait();
    let admitted: Vec<_> = threads
        .into_iter()
        .map(|thread| thread.join().unwrap())
        .collect();
    assert!(
        admitted.into_iter().all(|value| value),
        "bounded 512KiB working admission must remain available after resident pressure"
    );
    assert!(peak.load(Ordering::Acquire) <= budget.limit());
    assert!(budget.used() <= budget.limit() / 2);
    let mut output = [99; 19];
    assert!(
        cache
            .operation()
            .unwrap()
            .read_range(&first.chunk, 0, &mut output)
            .unwrap()
    );
    assert_eq!(output, [0; 19]);
    cache.shutdown().unwrap();
    assert_eq!(
        budget.used(),
        0,
        "resident and working ownership must both retire"
    );
}

#[test]
fn disk_peer_cache_only_retires_recognized_legacy_disposable_file() {
    let temp = tempfile::tempdir().unwrap();
    let config = options(temp.path().join("cache"));
    let staged = StagedChunk::new(OperationId::new("legacy-cache"), vec![51; 64 * 1024]);
    let cache = DiskPeerCache::open(config.clone()).unwrap();
    let key = cache.key(&staged.chunk).unwrap();
    let path = cache.path(&key);
    let header = serde_json::to_vec(&Header {
        version: 1,
        namespace_id: config.namespace_id.clone(),
        chunk: staged.chunk.clone(),
        outboard_length: 0,
    })
    .unwrap();
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(&path)
        .unwrap();
    file.write_all(OLD_MAGIC).unwrap();
    file.write_all(&(header.len() as u32).to_le_bytes())
        .unwrap();
    file.write_all(&header).unwrap();
    file.write_all(staged.bytes()).unwrap();
    drop(file);
    let persistent = temp.path().join("authoritative-chunk");
    fs::write(&persistent, staged.bytes()).unwrap();
    drop(cache);
    let cache = DiskPeerCache::open(config).unwrap();
    assert_eq!(cache.stats().unwrap().entries, 0);
    assert!(!cache.path(&key).exists());
    assert_eq!(fs::read(persistent).unwrap(), staged.bytes());
}

#[test]
fn disk_peer_cache_foreign_ledger_proof_cannot_bypass_node_budget() {
    let temp = tempfile::tempdir().unwrap();
    let budget = OutboardBudget::new(MAX_PEER_CACHE_PROOF_BYTES as usize);
    let cache = DiskPeerCache::open_with_outboard_budget(
        options(temp.path().join("cache")),
        budget.clone(),
    )
    .unwrap();
    let staged = StagedChunk::new(
        OperationId::new("outboard-other-ledger"),
        vec![41; 64 * 1024],
    );
    let foreign = OutboardBudget::new(MAX_PEER_CACHE_PROOF_BYTES as usize);
    let verified = VerifiedPeerChunk::verify(&staged.chunk, staged.bytes(), foreign).unwrap();
    assert!(
        cache
            .operation()
            .unwrap()
            .store_verified(&verified)
            .unwrap()
    );
    assert!(
        cache
            .state
            .lock()
            .unwrap()
            .entries
            .values()
            .all(|entry| entry.proof.is_none())
    );
    let mut out = [0; 9];
    assert!(
        cache
            .operation()
            .unwrap()
            .read_range(&staged.chunk, 0, &mut out)
            .unwrap()
    );
    assert_eq!(out, [41; 9]);
    assert_eq!(
        cache.stats().unwrap().hash_work.cache_fullscan_bytes,
        staged.chunk.length
    );
    assert!(budget.used() <= budget.limit());
}

#[test]
fn peer_stream_sink_stays_private_and_charges_pending_before_body() {
    let temp = tempfile::tempdir().unwrap();
    let cache = DiskPeerCache::open(options(temp.path().join("cache"))).unwrap();
    let staged = StagedChunk::new(OperationId::new("stream-pending"), vec![37; 64 * 1024]);
    let mut operation = cache.operation().unwrap();
    let mut sink = operation.begin_stream(&staged.chunk).unwrap();
    assert!(
        cache.state.lock().unwrap().pending_charge > 0,
        "private stream file must hold its disk reservation before body receipt"
    );
    sink.write(0, staged.bytes()).unwrap();
    assert!(
        cache.state.lock().unwrap().entries.is_empty(),
        "body completion alone cannot publish Ready"
    );
}

#[test]
fn peer_stream_sink_complete_proof_stays_private_until_commit() {
    let temp = tempfile::tempdir().unwrap();
    let cache = DiskPeerCache::open(options(temp.path().join("cache"))).unwrap();
    let staged = StagedChunk::new(OperationId::new("stream-commit"), vec![37; 64 * 1024]);
    let mut out = [0; 19];
    {
        let mut operation = cache.operation().unwrap();
        let mut sink = operation.begin_stream(&staged.chunk).unwrap();
        let proof = crate::node::rpc::peer::complete_peer_disk_read_for_test(
            &staged.chunk,
            staged.bytes(),
            63,
            &mut out,
            &mut sink,
        )
        .unwrap();
        assert_eq!(proof.private_output(), &[37; 19]);
        assert!(cache.state.lock().unwrap().entries.is_empty());
        assert!(sink.commit(proof).unwrap());
    }
    assert_eq!(out, [37; 19]);
    assert_eq!(cache.stats().unwrap().entries, 1);
    assert!(
        cache
            .operation()
            .unwrap()
            .read_range(&staged.chunk, 63, &mut out)
            .unwrap()
    );
}

#[test]
fn peer_stream_completed_disk_receipt_cannot_commit_another_sink() {
    let temp = tempfile::tempdir().unwrap();
    let cache_a = DiskPeerCache::open(options(temp.path().join("a"))).unwrap();
    let cache_b = DiskPeerCache::open(options(temp.path().join("b"))).unwrap();
    let staged = StagedChunk::new(OperationId::new("receipt-replay"), vec![19; 64 * 1024]);
    let mut operation_a = cache_a.operation().unwrap();
    let mut operation_b = cache_b.operation().unwrap();
    let mut sink_a = operation_a.begin_stream(&staged.chunk).unwrap();
    let mut sink_b = operation_b.begin_stream(&staged.chunk).unwrap();
    let mut out = [0; 8];
    let proof = crate::node::rpc::peer::complete_peer_disk_read_for_test(
        &staged.chunk,
        staged.bytes(),
        0,
        &mut out,
        &mut sink_a,
    )
    .unwrap();
    sink_b.write(0, staged.bytes()).unwrap();
    assert!(sink_b.commit(proof).is_err());
    assert!(cache_b.state.lock().unwrap().entries.is_empty());
}

#[test]
fn peer_stream_sink_quota_or_write_failure_keeps_verified_output_without_ready() {
    for write_failure in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let mut config = options(temp.path().join("cache"));
        if !write_failure {
            config.capacity_bytes = 4096;
        }
        let cache = DiskPeerCache::open(config).unwrap();
        let staged = StagedChunk::new(OperationId::new("stream-degraded"), vec![19; 64 * 1024]);
        let mut out = [0; 31];
        {
            let mut operation = cache.operation().unwrap();
            cache.state.lock().unwrap().fail_stream_write = write_failure;
            let mut sink = operation.begin_stream(&staged.chunk).unwrap();
            let proof = crate::node::rpc::peer::complete_peer_disk_read_for_test(
                &staged.chunk,
                staged.bytes(),
                2,
                &mut out,
                &mut sink,
            )
            .unwrap();
            assert_eq!(proof.private_output(), &[19; 31]);
            assert!(!sink.commit(proof).unwrap());
        }
        assert_eq!(out, [19; 31]);
        assert_eq!(cache.stats().unwrap().entries, 0);
        assert_eq!(cache.state.lock().unwrap().pending_charge, 0);
    }
}

#[test]
fn peer_stream_sink_unknown_created_file_and_failed_cleanup_remain_charged() {
    let temp = tempfile::tempdir().unwrap();
    let cache = DiskPeerCache::open(options(temp.path().join("cache"))).unwrap();
    let staged = StagedChunk::new(OperationId::new("stream-unidentified"), vec![19; 64 * 1024]);
    {
        let mut operation = cache.operation().unwrap();
        cache.state.lock().unwrap().fail_stream_metadata = true;
        let sink = operation.begin_stream(&staged.chunk).unwrap();
        assert!(sink.file.is_none());
        assert!(cache.state.lock().unwrap().pending_charge > 0);
    }
    let state = cache.state.lock().unwrap();
    assert!(state.pending_charge > 0);
    assert_eq!(state.used_bytes, state.pending_charge);
    assert_eq!(pending_files(&cache).len(), 1);
}

#[test]
fn peer_stream_sink_observed_extra_blocks_stay_charged_after_cleanup_failure() {
    let temp = tempfile::tempdir().unwrap();
    let cache = DiskPeerCache::open(options(temp.path().join("cache"))).unwrap();
    let staged = StagedChunk::new(OperationId::new("stream-charge"), vec![19; 64 * 1024]);
    let actual;
    {
        let mut operation = cache.operation().unwrap();
        let mut sink = operation.begin_stream(&staged.chunk).unwrap();
        let mut out = [0; 2];
        let proof = crate::node::rpc::peer::complete_peer_disk_read_for_test(
            &staged.chunk,
            staged.bytes(),
            2,
            &mut out,
            &mut sink,
        )
        .unwrap();
        actual = allocated(&sink.file.as_ref().unwrap().metadata().unwrap()).unwrap();
        cache.state.lock().unwrap().used_bytes = actual - 512;
        {
            let mut state = cache.state.lock().unwrap();
            state.pending_charge = actual - 512;
            state
                .pending
                .get_mut(&sink.pending_generation)
                .unwrap()
                .charge = actual - 512;
        }
        cache.state.lock().unwrap().fail_remove = true;
        assert!(sink.commit(proof).is_err());
        assert_eq!(cache.state.lock().unwrap().used_bytes, actual);
        assert_eq!(cache.state.lock().unwrap().pending_charge, actual);
    }
    assert_eq!(cache.state.lock().unwrap().used_bytes, actual);
    assert_eq!(pending_files(&cache).len(), 1);
}

#[test]
fn coarse_cache_diagnostics_separate_misses_from_verified_hit_attempts() {
    let temp = tempfile::tempdir().unwrap();
    let registry = afs_metrics::Registry::new();
    let metrics = super::super::read_work::ReadWorkMetrics::register(&registry)
        .unwrap()
        .with_read_diagnostics(true);
    let cache = DiskPeerCache::open(options(temp.path().join("cache")))
        .unwrap()
        .with_read_work_metrics(metrics.clone());
    let staged = StagedChunk::new(OperationId::new("diagnostic-hit"), vec![37; 64 * 1024]);
    let mut out = [9; 19];
    assert!(
        !cache
            .operation()
            .unwrap()
            .read_range(&staged.chunk, 63, &mut out)
            .unwrap()
    );
    assert_eq!(out, [9; 19]);
    assert!(fill(&cache, &staged).unwrap());
    assert!(
        cache
            .operation()
            .unwrap()
            .read_range(&staged.chunk, 63, &mut out)
            .unwrap()
    );
    assert_eq!(out, [37; 19]);
    let snapshot = metrics.diagnostics_snapshot().unwrap();
    assert!(snapshot.records.is_empty());
    assert_eq!(snapshot.overflow, 0);
    let lock = snapshot
        .coarse_phases
        .iter()
        .find(|phase| phase.phase == "cache_lock_wait")
        .unwrap();
    assert_eq!(lock.calls, 3);
    let hit = snapshot
        .coarse_phases
        .iter()
        .find(|phase| phase.phase == "cache_hit_read_verify")
        .unwrap();
    assert_eq!(hit.calls, 1);
    assert_eq!(hit.bytes, out.len() as u64);
    // The earlier miss consumes no compact object slot.
    assert_eq!(snapshot.object_records.len(), 1);
    assert_eq!(snapshot.object_overflow, 0);
    assert!(snapshot.object_records[0].completed);
    assert!(snapshot.object_records[0].success);
    assert_eq!(
        snapshot.object_records[0].stage,
        super::super::read_work::ReadObjectDiagnosticStage::CacheHitReadVerify
    );
}

#[test]
fn disk_peer_cache_admits_verified_object_and_reads_requested_range() {
    let temp = tempfile::tempdir().unwrap();
    let cache = DiskPeerCache::open(options(temp.path().join("cache"))).unwrap();
    let staged = StagedChunk::new(OperationId::new("cache-read"), vec![37; 64 * 1024]);
    assert!(fill(&cache, &staged).unwrap());
    let mut out = [0; 19];
    assert!(
        cache
            .operation()
            .unwrap()
            .read_range(&staged.chunk, 63, &mut out)
            .unwrap()
    );
    assert_eq!(out, [37; 19]);
    let stats = cache.stats().unwrap();
    assert_eq!(stats.hash_bytes_total, COVERAGE_GROUP_BYTES as u64);
    assert!(stats.scratch_peak_bytes <= (2 * COVERAGE_GROUP_BYTES + MAX_OUTBOARD_BYTES) as u64);
    assert_eq!(stats.entries, 1);
    assert!(stats.disk_bytes >= staged.chunk.length);
}

#[test]
fn disk_peer_cache_aligned_range_reports_private_range_validator_work() {
    let temp = tempfile::tempdir().unwrap();
    let mut config = options(temp.path().join("cache"));
    config.capacity_bytes = 8 * 1024 * 1024;
    let cache = DiskPeerCache::open(config).unwrap();
    let staged = StagedChunk::new(
        OperationId::new("direct-private-segment"),
        vec![73; 4 * PEER_CACHE_SEGMENT_BYTES],
    );
    assert!(fill(&cache, &staged).unwrap());
    let before = cache.stats().unwrap();
    let mut out = vec![0; PEER_CACHE_SEGMENT_BYTES];
    assert!(
        cache
            .operation()
            .unwrap()
            .read_range(&staged.chunk, PEER_CACHE_SEGMENT_BYTES as u64, &mut out)
            .unwrap()
    );
    assert!(out.iter().all(|byte| *byte == 73));
    let after = cache.stats().unwrap();
    assert_eq!(
        (
            after.copied_bytes_total - before.copied_bytes_total,
            after.data_read_calls - before.data_read_calls
        ),
        // One aligned body read plus one copy into the official validator's
        // scratch; unaligned ranges retain the separate codec fallback.
        (PEER_CACHE_SEGMENT_BYTES as u64, 1),
    );
    assert_eq!(
        after.hash_bytes_total - before.hash_bytes_total,
        PEER_CACHE_SEGMENT_BYTES as u64
    );
    assert!(after.read_stages.prepare_ns > before.read_stages.prepare_ns);
    assert!(after.read_stages.data_read_ns > before.read_stages.data_read_ns);
    assert!(after.read_stages.segment_verify_ns > before.read_stages.segment_verify_ns);
    assert!(after.read_stages.finalize_ns > before.read_stages.finalize_ns);
    assert!(after.read_stages.stream_read_verify_ns > before.read_stages.stream_read_verify_ns);
}

#[test]
fn disk_peer_cache_short_file_fails_preflight_without_body_io_or_hashing() {
    let mut file = tempfile::tempfile().unwrap();
    file.write_all(&[79; 17]).unwrap();
    let mut private = vec![0; PEER_CACHE_SEGMENT_BYTES];
    let mut stats = PeerCacheStats::default();

    let staged = StagedChunk::new(
        OperationId::new("outboard-short-file"),
        vec![79; PEER_CACHE_SEGMENT_BYTES],
    );
    let proof = build_borrowed(
        staged.bytes(),
        &OutboardBudget::new(MAX_PEER_CACHE_PROOF_BYTES as usize),
    )
    .outboard
    .unwrap();
    let error =
        scan_outboard(&file, 0, &staged.chunk, 0, &mut private, &proof, &mut stats).unwrap_err();

    assert_eq!(error.code(), afs_error::NODE_TRANSFER_CORRUPT_DATA);
    // Metadata proves the body shorter than the trusted object before the
    // official codec is entered. No positioned body-read attempt is made.
    assert_eq!(stats.data_read_calls, 0);
    assert_eq!(stats.copied_bytes_total, 0);
    assert_eq!(private, vec![0; PEER_CACHE_SEGMENT_BYTES]);
    assert_eq!(stats.hash_bytes_total, 0);
    assert_eq!(stats.hash_work.cache_segment_bytes, 0);
    assert_eq!(stats.read_stages.segment_verify_ns, 0);
}

#[test]
fn disk_peer_cache_direct_private_digest_failure_retires_file_without_success() {
    let temp = tempfile::tempdir().unwrap();
    let mut config = options(temp.path().join("cache"));
    config.capacity_bytes = 8 * 1024 * 1024;
    let cache = DiskPeerCache::open(config).unwrap();
    let staged = StagedChunk::new(
        OperationId::new("direct-private-failure"),
        vec![74; 4 * PEER_CACHE_SEGMENT_BYTES],
    );
    assert!(fill(&cache, &staged).unwrap());
    let key = cache.key(&staged.chunk).unwrap();
    let path = cache.path(&key);
    let mut bytes = fs::read(&path).unwrap();
    let body_start = MAX_HEADER_BYTES;
    bytes[body_start + PEER_CACHE_SEGMENT_BYTES + 17] ^= 1;
    fs::write(&path, bytes).unwrap();
    // Preserve trusted old digests while simulating an unobserved stat
    // change, so the failing current segment takes the direct-read path.
    cache
        .state
        .lock()
        .unwrap()
        .entries
        .get_mut(&key)
        .unwrap()
        .proof
        .as_mut()
        .map(|proof| Arc::get_mut(proof).unwrap())
        .unwrap()
        .identity = file_identity(&fs::metadata(&path).unwrap());
    let before = cache.stats().unwrap();
    let mut private = vec![0; PEER_CACHE_SEGMENT_BYTES];
    assert!(
        !cache
            .operation()
            .unwrap()
            .read_range(&staged.chunk, PEER_CACHE_SEGMENT_BYTES as u64, &mut private,)
            .unwrap()
    );
    assert_ne!(private[17], 74);
    let after = cache.stats().unwrap();
    assert!(
        after.copied_bytes_total - before.copied_bytes_total <= 3 * COVERAGE_GROUP_BYTES as u64
    );
    assert_eq!(after.data_read_calls - before.data_read_calls, 1);
    assert_eq!(
        after.hash_bytes_total - before.hash_bytes_total,
        COVERAGE_GROUP_BYTES as u64
    );
    assert_eq!(after.corruptions, 1);
    assert_eq!(after.proof_metadata_bytes, 0);
    assert!(!path.exists());
}

#[test]
fn disk_peer_cache_stable_one_mib_hit_hashes_only_its_verified_segment() {
    let temp = tempfile::tempdir().unwrap();
    let mut config = options(temp.path().join("cache"));
    config.capacity_bytes = 8 * 1024 * 1024;
    let cache = DiskPeerCache::open(config).unwrap();
    let staged = StagedChunk::new(OperationId::new("segment-read"), vec![71; 4 * 1024 * 1024]);
    assert!(fill(&cache, &staged).unwrap());
    let before = cache.stats().unwrap().hash_bytes_total;
    let mut out = vec![0; 1024 * 1024];
    assert!(
        cache
            .operation()
            .unwrap()
            .read_range(&staged.chunk, 1024 * 1024, &mut out)
            .unwrap()
    );
    assert!(out.iter().all(|byte| *byte == 71));
    assert_eq!(
        cache.stats().unwrap().hash_bytes_total - before,
        1024 * 1024
    );
}

#[test]
fn disk_peer_cache_reopens_disk_objects_without_retained_payloads() {
    let temp = tempfile::tempdir().unwrap();
    let config = options(temp.path().join("cache"));
    let staged = StagedChunk::new(OperationId::new("cache-reopen"), vec![17; 64 * 1024]);
    {
        let cache = DiskPeerCache::open(config.clone()).unwrap();
        assert!(fill(&cache, &staged).unwrap());
    }
    let cache = DiskPeerCache::open(config).unwrap();
    let mut out = [0; 31];
    assert!(
        cache
            .operation()
            .unwrap()
            .read_range(&staged.chunk, 2, &mut out)
            .unwrap()
    );
    assert_eq!(out, [17; 31]);
    assert_eq!(cache.stats().unwrap().entries, 1);
    assert_eq!(
        cache.stats().unwrap().hash_bytes_total,
        staged.chunk.length + COVERAGE_GROUP_BYTES as u64
    );
}

#[test]
fn disk_peer_cache_corrupt_or_truncated_body_is_retired_and_refillable() {
    for truncate in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let cache = DiskPeerCache::open(options(temp.path().join("cache"))).unwrap();
        let staged = StagedChunk::new(OperationId::new("cache-corrupt"), vec![33; 64 * 1024]);
        assert!(fill(&cache, &staged).unwrap());
        let path = cache.path(&cache.key(&staged.chunk).unwrap());
        let previous_identity = file_identity(&fs::metadata(&path).unwrap());
        let mut bytes = fs::read(&path).unwrap();
        if truncate {
            bytes.pop();
        } else {
            bytes[MAX_HEADER_BYTES + staged.bytes().len() - 1] ^= 1;
        }
        fs::write(&path, bytes).unwrap();
        // Timestamp granularity can make a same-jiffy write appear unchanged.
        // This test is specifically the changed-identity full-root contract;
        // the separate unobserved-damage test covers an unchanged identity.
        File::open(&path)
            .unwrap()
            .set_modified(std::time::UNIX_EPOCH)
            .unwrap();
        assert_ne!(
            previous_identity,
            file_identity(&fs::metadata(&path).unwrap())
        );
        let mut private = [0; 19];
        assert!(
            !cache
                .operation()
                .unwrap()
                .read_range(&staged.chunk, 0, &mut private)
                .unwrap()
        );
        assert!(!path.exists());
        assert_eq!(cache.stats().unwrap().disk_bytes, 0);
        assert!(fill(&cache, &staged).unwrap());
    }
}

#[test]
fn disk_peer_cache_refuses_bad_proof_and_oversized_admission() {
    let temp = tempfile::tempdir().unwrap();
    let cache = DiskPeerCache::open(options(temp.path().join("cache"))).unwrap();
    let staged = StagedChunk::new(OperationId::new("bad-proof"), vec![21; 64 * 1024]);
    assert!(
        VerifiedPeerChunk::verify(&staged.chunk, &[0; 64 * 1024], cache.proof_budget()).is_err()
    );
    assert!(VerifiedPeerChunk::verify(&staged.chunk, &[], cache.proof_budget()).is_err());
    let large = StagedChunk::new(
        OperationId::new("oversize"),
        vec![44; MAX_OBJECT_BYTES as usize + 1],
    );
    assert!(!fill(&cache, &large).unwrap());
    assert_eq!(cache.stats().unwrap().disk_bytes, 0);
}

#[test]
fn disk_peer_cache_evicts_with_actual_blocks_and_failed_delete_stays_charged() {
    let temp = tempfile::tempdir().unwrap();
    let mut config = options(temp.path().join("cache"));
    config.capacity_bytes = 96 * 1024;
    let cache = DiskPeerCache::open(config.clone()).unwrap();
    let first = StagedChunk::new(OperationId::new("first"), vec![1; 64 * 1024]);
    let second = StagedChunk::new(OperationId::new("second"), vec![2; 64 * 1024]);
    assert!(fill(&cache, &first).unwrap());
    let before = cache.stats().unwrap().disk_bytes;
    cache.state.lock().unwrap().fail_remove = true;
    assert!(fill(&cache, &second).is_err());
    assert_eq!(cache.stats().unwrap().disk_bytes, before);
    cache.state.lock().unwrap().fail_remove = false;
    assert!(fill(&cache, &second).unwrap());
    assert!(cache.stats().unwrap().disk_bytes <= config.capacity_bytes);
    assert_eq!(cache.stats().unwrap().evictions, 1);
    assert!(
        !cache
            .operation()
            .unwrap()
            .read_range(&first.chunk, 0, &mut [0; 1])
            .unwrap()
    );
}

#[test]
fn disk_peer_cache_preserves_foreign_files_and_rejects_symlinks() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("cache");
    fs::create_dir(&root).unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
    fs::write(root.join("foreign"), b"preserve").unwrap();
    assert!(DiskPeerCache::open(options(root.clone())).is_err());
    assert_eq!(fs::read(root.join("foreign")).unwrap(), b"preserve");
    let link = temp.path().join("link");
    symlink(&root, &link).unwrap();
    assert!(DiskPeerCache::open(options(link)).is_err());
}

#[test]
fn disk_peer_cache_rejects_linked_ready_files_without_deleting_target() {
    let temp = tempfile::tempdir().unwrap();
    let cache = DiskPeerCache::open(options(temp.path().join("cache"))).unwrap();
    let staged = StagedChunk::new(OperationId::new("cache-link"), vec![27; 1024]);
    assert!(fill(&cache, &staged).unwrap());
    let path = cache.path(&cache.key(&staged.chunk).unwrap());
    let target = temp.path().join("target");
    fs::write(&target, b"preserve").unwrap();
    fs::remove_file(&path).unwrap();
    symlink(&target, &path).unwrap();
    assert!(
        cache
            .operation()
            .unwrap()
            .read_range(&staged.chunk, 0, &mut [0; 1])
            .is_err()
    );
    assert_eq!(fs::read(target).unwrap(), b"preserve");
}

#[test]
fn disk_peer_cache_recovery_retires_body_corruption_and_stale_private_temp() {
    let temp = tempfile::tempdir().unwrap();
    let config = options(temp.path().join("cache"));
    let staged = StagedChunk::new(OperationId::new("restart-corrupt"), vec![18; 64 * 1024]);
    let path;
    {
        let cache = DiskPeerCache::open(config.clone()).unwrap();
        assert!(fill(&cache, &staged).unwrap());
        path = config
            .directory
            .join(format!("{}.ready", cache.key(&staged.chunk).unwrap()));
    }
    let mut bytes = fs::read(&path).unwrap();
    bytes[MAX_HEADER_BYTES + staged.bytes().len() - 1] ^= 1;
    fs::write(&path, bytes).unwrap();
    let pending = config.directory.join(PENDING);
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&pending)
        .unwrap()
        .write_all(b"interrupted fill")
        .unwrap();
    let cache = DiskPeerCache::open(config).unwrap();
    assert!(!pending.exists());
    assert!(!path.exists());
    assert_eq!(cache.stats().unwrap().entries, 0);
    assert_eq!(cache.stats().unwrap().disk_bytes, 0);
}

#[test]
fn disk_peer_cache_namespace_identity_and_single_owner_are_enforced() {
    let temp = tempfile::tempdir().unwrap();
    let config = options(temp.path().join("cache"));
    let staged = StagedChunk::new(OperationId::new("namespace"), vec![15; 1024]);
    {
        let cache = DiskPeerCache::open(config.clone()).unwrap();
        assert!(fill(&cache, &staged).unwrap());
        assert!(DiskPeerCache::open(config.clone()).is_err());
    }
    let mut wrong = config;
    wrong.namespace_id = NamespaceId::new("other");
    assert!(DiskPeerCache::open(wrong).is_err());
}

#[test]
fn disk_peer_cache_shutdown_stops_new_operations() {
    let temp = tempfile::tempdir().unwrap();
    let cache = DiskPeerCache::open(options(temp.path().join("cache"))).unwrap();
    cache.shutdown().unwrap();
    assert!(cache.operation().is_err());
}

#[test]
fn disk_peer_cache_failed_pending_cleanup_keeps_reserved_blocks_charged() {
    let temp = tempfile::tempdir().unwrap();
    let cache = DiskPeerCache::open(options(temp.path().join("cache"))).unwrap();
    let staged = StagedChunk::new(OperationId::new("pending-failure"), vec![11; 64 * 1024]);
    let foreign = cache.path(&cache.key(&staged.chunk).unwrap());
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&foreign)
        .unwrap()
        .write_all(b"preserve foreign publication target")
        .unwrap();
    cache.state.lock().unwrap().fail_remove = true;
    assert!(fill(&cache, &staged).is_err());
    let stats = cache.stats().unwrap();
    assert!(stats.disk_bytes >= staged.chunk.length);
    assert!(stats.disk_bytes <= cache.options.capacity_bytes);
    assert_eq!(pending_files(&cache).len(), 1);
    assert_eq!(
        fs::read(&foreign).unwrap(),
        b"preserve foreign publication target"
    );
    cache.state.lock().unwrap().fail_remove = false;
    fs::remove_file(foreign).unwrap();
    assert!(fill(&cache, &staged).unwrap());
    assert!(pending_files(&cache).is_empty());
}

#[test]
fn disk_peer_cache_startup_limits_index_before_reading_unbounded_metadata() {
    let temp = tempfile::tempdir().unwrap();
    let mut config = options(temp.path().join("cache"));
    config.capacity_bytes = 64 * 1024 * 1024;
    drop(DiskPeerCache::open(config.clone()).unwrap());
    for index in 0..=MAX_PEER_CACHE_ENTRIES {
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(config.directory.join(format!("{index:064x}.ready")))
            .unwrap()
            .write_all(b"x")
            .unwrap();
    }
    let error = DiskPeerCache::open(config.clone()).unwrap_err();
    assert!(error.to_string().contains("index budget"));
    assert_eq!(
        fs::read_dir(config.directory).unwrap().count(),
        MAX_PEER_CACHE_ENTRIES + 2
    );
}

#[test]
fn disk_peer_cache_pins_root_and_preserves_foreign_replacement() {
    let temp = tempfile::tempdir().unwrap();
    let config = options(temp.path().join("cache"));
    let cache = DiskPeerCache::open(config.clone()).unwrap();
    let staged = StagedChunk::new(OperationId::new("root-replacement"), vec![23; 1024]);
    assert!(fill(&cache, &staged).unwrap());
    let key = cache.key(&staged.chunk).unwrap();
    let anchored = cache.path(&key);
    let original = fs::read(&anchored).unwrap();
    let mut operation = cache.operation().unwrap();
    fs::rename(&config.directory, temp.path().join("old-root")).unwrap();
    DirBuilder::new()
        .mode(0o700)
        .create(&config.directory)
        .unwrap();
    let foreign = config.directory.join(format!("{key}.ready"));
    fs::write(&foreign, b"foreign replacement must survive").unwrap();
    // The checked-before-rename operation cannot touch the new root.
    assert!(operation.read_range(&staged.chunk, 0, &mut [0; 1]).is_err());
    assert!(
        operation
            .store_verified(
                &VerifiedPeerChunk::verify(&staged.chunk, staged.bytes(), cache.proof_budget())
                    .unwrap()
            )
            .is_err()
    );
    // Even stale child paths still resolve to the originally pinned root.
    assert_eq!(fs::read(anchored).unwrap(), original);
    assert_eq!(
        fs::read(foreign).unwrap(),
        b"foreign replacement must survive"
    );
    assert!(!config.directory.join(MARKER).exists());
}

#[test]
fn disk_peer_cache_segments_cover_cross_boundary_and_short_tail() {
    let temp = tempfile::tempdir().unwrap();
    let mut config = options(temp.path().join("cache"));
    config.capacity_bytes = 8 * 1024 * 1024;
    let cache = DiskPeerCache::open(config).unwrap();
    let bytes: Vec<_> = (0..2 * PEER_CACHE_SEGMENT_BYTES + 17)
        .map(|index| (index % 251) as u8)
        .collect();
    let staged = StagedChunk::new(OperationId::new("segment-boundaries"), bytes.clone());
    assert!(fill(&cache, &staged).unwrap());
    let mut cross = [0; 19];
    let start = PEER_CACHE_SEGMENT_BYTES - 7;
    assert!(
        cache
            .operation()
            .unwrap()
            .read_range(&staged.chunk, start as u64, &mut cross)
            .unwrap()
    );
    assert_eq!(cross, bytes[start..start + cross.len()]);
    let mut tail = [0; 14];
    let start = 2 * PEER_CACHE_SEGMENT_BYTES + 3;
    assert!(
        cache
            .operation()
            .unwrap()
            .read_range(&staged.chunk, start as u64, &mut tail)
            .unwrap()
    );
    assert_eq!(tail, bytes[start..]);
    let stats = cache.stats().unwrap();
    assert_eq!(
        stats.hash_work.cache_segment_bytes,
        (2 * COVERAGE_GROUP_BYTES + 17) as u64
    );
    assert_eq!(stats.hash_work.cache_fullscan_bytes, 0);
    assert_eq!(stats.hash_work.segment_index_bytes, 0);
    assert_eq!(
        stats.copied_bytes_total,
        2 * (2 * COVERAGE_GROUP_BYTES as u64 + 17) + 33
    );
    assert_eq!(stats.data_read_calls, 3);
    assert!(stats.read_stages.stream_read_verify_ns > 0);
    assert!(stats.read_stages.data_read_ns > 0);
    assert!(stats.read_stages.segment_verify_ns > 0);
}

#[test]
fn disk_peer_cache_changed_metadata_rechecks_entire_root_before_range_return() {
    let temp = tempfile::tempdir().unwrap();
    let mut config = options(temp.path().join("cache"));
    config.capacity_bytes = 8 * 1024 * 1024;
    let cache = DiskPeerCache::open(config).unwrap();
    let staged = StagedChunk::new(
        OperationId::new("changed-tail"),
        vec![51; 4 * PEER_CACHE_SEGMENT_BYTES],
    );
    assert!(fill(&cache, &staged).unwrap());
    let path = cache.path(&cache.key(&staged.chunk).unwrap());
    let mut bytes = fs::read(&path).unwrap();
    bytes[MAX_HEADER_BYTES + staged.bytes().len() - 1] ^= 1;
    fs::write(&path, bytes).unwrap();
    File::open(&path)
        .unwrap()
        .set_modified(std::time::UNIX_EPOCH)
        .unwrap();
    let mut private = [0; 19];
    assert!(
        !cache
            .operation()
            .unwrap()
            .read_range(&staged.chunk, 0, &mut private)
            .unwrap()
    );
    assert_eq!(
        cache.stats().unwrap().hash_work.cache_fullscan_bytes,
        staged.chunk.length
    );
    assert_eq!(cache.stats().unwrap().proof_metadata_bytes, 0);
}

#[test]
fn disk_peer_cache_unobserved_damage_is_detected_when_its_segment_is_read() {
    let temp = tempfile::tempdir().unwrap();
    let mut config = options(temp.path().join("cache"));
    config.capacity_bytes = 8 * 1024 * 1024;
    let cache = DiskPeerCache::open(config).unwrap();
    let staged = StagedChunk::new(
        OperationId::new("silent-tail"),
        vec![52; 4 * PEER_CACHE_SEGMENT_BYTES],
    );
    assert!(fill(&cache, &staged).unwrap());
    let key = cache.key(&staged.chunk).unwrap();
    let path = cache.path(&key);
    let mut bytes = fs::read(&path).unwrap();
    bytes[MAX_HEADER_BYTES + staged.bytes().len() - 1] ^= 1;
    fs::write(&path, bytes).unwrap();
    // Fault injection: model a disk-sector change with no observable stat
    // change. Preserve the trusted old digests, adjust only the observation.
    cache
        .state
        .lock()
        .unwrap()
        .entries
        .get_mut(&key)
        .unwrap()
        .proof
        .as_mut()
        .map(|proof| Arc::get_mut(proof).unwrap())
        .unwrap()
        .identity = file_identity(&fs::metadata(&path).unwrap());
    let mut first = [0; 19];
    assert!(
        cache
            .operation()
            .unwrap()
            .read_range(&staged.chunk, 0, &mut first)
            .unwrap()
    );
    assert_eq!(first, [52; 19]);
    let mut private_tail = [0; 1];
    assert!(
        !cache
            .operation()
            .unwrap()
            .read_range(&staged.chunk, staged.chunk.length - 1, &mut private_tail)
            .unwrap()
    );
    assert_eq!(cache.stats().unwrap().corruptions, 1);
    assert_eq!(cache.stats().unwrap().proof_metadata_bytes, 0);
}

#[test]
fn disk_peer_cache_metadata_quota_falls_back_to_complete_verification() {
    let temp = tempfile::tempdir().unwrap();
    let mut config = options(temp.path().join("cache"));
    config.capacity_bytes = 8 * 1024 * 1024;
    let cache = DiskPeerCache::open(config).unwrap();
    let limit = std::mem::size_of::<Option<Box<VerifiedDiskOutboard>>>() as u64;
    cache.state.lock().unwrap().proof_limit = Some(limit);
    let staged = StagedChunk::new(
        OperationId::new("metadata-quota"),
        vec![53; 4 * PEER_CACHE_SEGMENT_BYTES],
    );
    assert!(fill(&cache, &staged).unwrap());
    let mut out = [0; 19];
    assert!(
        cache
            .operation()
            .unwrap()
            .read_range(&staged.chunk, 0, &mut out)
            .unwrap()
    );
    assert_eq!(out, [53; 19]);
    let stats = cache.stats().unwrap();
    assert!(stats.proof_metadata_bytes <= limit);
    assert!(stats.proof_metadata_peak_bytes <= MAX_PEER_CACHE_PROOF_BYTES);
    assert_eq!(stats.hash_work.cache_fullscan_bytes, staged.chunk.length);
    assert_eq!(stats.hash_work.cache_segment_bytes, 0);
    assert_eq!(stats.hash_work.segment_index_bytes, 0);
}

#[test]
fn disk_peer_cache_restart_rebuilds_root_bound_segments_without_disk_digest_trust() {
    let temp = tempfile::tempdir().unwrap();
    let mut config = options(temp.path().join("cache"));
    config.capacity_bytes = 8 * 1024 * 1024;
    let staged = StagedChunk::new(
        OperationId::new("segment-restart"),
        vec![54; 4 * PEER_CACHE_SEGMENT_BYTES],
    );
    {
        let cache = DiskPeerCache::open(config.clone()).unwrap();
        assert!(fill(&cache, &staged).unwrap());
    }
    let cache = DiskPeerCache::open(config).unwrap();
    let recovered = cache.stats().unwrap();
    assert_eq!(
        recovered.hash_work.cache_fullscan_bytes,
        staged.chunk.length
    );
    assert_eq!(recovered.hash_work.segment_index_bytes, 0);
    let mut out = [0; 19];
    assert!(
        cache
            .operation()
            .unwrap()
            .read_range(&staged.chunk, 0, &mut out)
            .unwrap()
    );
    assert_eq!(
        cache.stats().unwrap().hash_work.cache_fullscan_bytes,
        staged.chunk.length
    );
    assert_eq!(
        cache.stats().unwrap().hash_work.cache_segment_bytes,
        COVERAGE_GROUP_BYTES as u64
    );
    assert!(cache.stats().unwrap().proof_metadata_bytes <= MAX_PEER_CACHE_PROOF_BYTES);
}

#[test]
fn disk_peer_cache_metadata_retires_on_replacement_eviction_and_shutdown() {
    let temp = tempfile::tempdir().unwrap();
    let mut config = options(temp.path().join("cache"));
    config.capacity_bytes = 96 * 1024;
    let cache = DiskPeerCache::open(config).unwrap();
    let first = StagedChunk::new(OperationId::new("metadata-first"), vec![55; 64 * 1024]);
    let second = StagedChunk::new(OperationId::new("metadata-second"), vec![56; 64 * 1024]);
    assert!(fill(&cache, &first).unwrap());
    let one = cache.stats().unwrap().proof_metadata_bytes;
    assert!(one > 0 && one <= MAX_PEER_CACHE_PROOF_BYTES);
    assert!(fill(&cache, &first).unwrap());
    assert_eq!(cache.stats().unwrap().proof_metadata_bytes, one);
    assert!(fill(&cache, &second).unwrap());
    assert_eq!(cache.stats().unwrap().proof_metadata_bytes, one);
    assert_eq!(cache.stats().unwrap().evictions, 1);
    cache.shutdown().unwrap();
    assert_eq!(cache.stats().unwrap().proof_metadata_bytes, 0);
    assert_eq!(cache.stats().unwrap().proof_metadata_peak_bytes, one);
}

#[test]
fn disk_peer_cache_proof_quota_reserves_future_entry_pointers() {
    let temp = tempfile::tempdir().unwrap();
    let budget = OutboardBudget::new(MAX_PEER_CACHE_PROOF_BYTES as usize);
    let cache = DiskPeerCache::open_with_outboard_budget(
        options(temp.path().join("cache")),
        budget.clone(),
    )
    .unwrap();
    let pointers =
        MAX_PEER_CACHE_ENTRIES * std::mem::size_of::<Option<Box<VerifiedDiskOutboard>>>();
    assert_eq!(budget.used(), pointers);
    let staged = StagedChunk::new(OperationId::new("ledger-proof"), vec![3; 64 * 1024]);
    assert!(fill(&cache, &staged).unwrap());
    let held = cache
        .state
        .lock()
        .unwrap()
        .entries
        .values()
        .next()
        .unwrap()
        .proof
        .as_ref()
        .unwrap()
        .outboard
        .clone();
    let charged = held.charged_bytes();
    cache.shutdown().unwrap();
    assert_eq!(cache.stats().unwrap().proof_metadata_bytes, 0);
    assert_eq!(
        budget.used(),
        charged,
        "eviction must retain the shared allocation charge until the final Arc drops"
    );
    drop(held);
    assert_eq!(budget.used(), 0);
}

#[test]
fn disk_peer_cache_reports_actual_segment_work_and_metadata_until_shutdown() {
    let temp = tempfile::tempdir().unwrap();
    let registry = afs_metrics::Registry::new();
    let mut config = options(temp.path().join("cache"));
    config.capacity_bytes = 8 * 1024 * 1024;
    let cache = DiskPeerCache::open(config).unwrap().with_read_work_metrics(
        super::super::read_work::ReadWorkMetrics::register(&registry).unwrap(),
    );
    let staged = StagedChunk::new(
        OperationId::new("metadata-metrics"),
        vec![57; 4 * PEER_CACHE_SEGMENT_BYTES],
    );
    assert!(fill(&cache, &staged).unwrap());
    let mut out = [0; 19];
    assert!(
        cache
            .operation()
            .unwrap()
            .read_range(&staged.chunk, 0, &mut out)
            .unwrap()
    );
    let encoded = afs_metrics::encode_text(&registry).unwrap();
    assert!(encoded.contains("afs_dfs_read_hash_bytes_total{stage=\"cache_segment_hash\"} 32768"));
    assert!(encoded.contains("afs_dfs_read_hash_bytes_total{stage=\"cache_full_hash\"} 0"));
    assert!(encoded.contains("afs_dfs_peer_disk_cache_data_read_calls_total 1"));
    assert!(encoded.contains("afs_dfs_read_copy_bytes_total{stage=\"cache_to_output\"} 65555"));
    assert!(encoded.contains(
        "afs_dfs_peer_disk_cache_stage_observations_total{stage=\"stream_read_verify\"} 1"
    ));
    let mut direct = vec![0; PEER_CACHE_SEGMENT_BYTES];
    assert!(
        cache
            .operation()
            .unwrap()
            .read_range(&staged.chunk, PEER_CACHE_SEGMENT_BYTES as u64, &mut direct,)
            .unwrap()
    );
    let encoded = afs_metrics::encode_text(&registry).unwrap();
    assert!(encoded.contains("afs_dfs_peer_disk_cache_data_read_calls_total 2"));
    assert!(encoded.contains("afs_dfs_read_copy_bytes_total{stage=\"cache_to_output\"} 1114131"));
    assert!(
        encoded.contains("afs_dfs_peer_disk_cache_stage_observations_total{stage=\"data_read\"} 2")
    );
    assert!(
        encoded.contains(
            "afs_dfs_peer_disk_cache_stage_observations_total{stage=\"segment_verify\"} 2"
        )
    );
    let current = cache.stats().unwrap().proof_metadata_bytes;
    assert!(encoded.contains(&format!("afs_dfs_peer_disk_cache_metadata_bytes {current}")));
    cache.shutdown().unwrap();
    assert_eq!(
        cache.proof_budget.used(),
        0,
        "shutdown must release actual metadata leases"
    );
    assert_eq!(cache.proof_budget.resident_used(), 0);
    {
        let state = cache.state.lock().unwrap();
        assert!(state.entries.is_empty() && state.retired.is_empty());
        assert!(state.flights.is_empty() && state.index_lease.is_none());
        assert_eq!(
            state.operations + state.flight_live + state.deleting_slots,
            0
        );
        assert!(!state.fill_active);
        assert_eq!(
            state.operation_metadata_bytes
                + state.entry_metadata_bytes
                + state.flight_metadata_bytes
                + state.pending_metadata_charge,
            0
        );
    }
    let encoded = afs_metrics::encode_text(&registry).unwrap();
    assert!(encoded.contains("afs_dfs_peer_disk_cache_metadata_bytes 0"));
    assert!(encoded.contains("afs_dfs_peer_disk_cache_resources{resource=\"control_bytes\"} 0"));
    assert!(encoded.contains(&format!(
        "afs_dfs_peer_disk_cache_metadata_peak_bytes {current}"
    )));
}
#[tokio::test(flavor = "current_thread")]
async fn unified_cancelled_fill_follower_releases_last_flight_owner() {
    let temp = tempfile::tempdir().unwrap();
    let cache = Arc::new(
        DiskPeerCache::open(PeerCacheOptions {
            directory: temp.path().join("cache"),
            capacity_bytes: 1024 * 1024,
            namespace_id: crate::dfs::NamespaceId::new("default"),
        })
        .unwrap(),
    );
    let chunk = StagedChunk::new(
        crate::dfs::OperationId::new("cancel-follower"),
        vec![7; 4096],
    )
    .chunk;
    let leader = match cache.fill_flight_async(&chunk, || Ok(())).await.unwrap() {
        FillRole::Leader(leader) => leader,
        _ => panic!("expected leader"),
    };
    let other = cache.clone();
    let object = chunk.clone();
    let waiting = tokio::spawn(async move {
        let _ = other.fill_flight_async(&object, || Ok(())).await;
    });
    while Arc::strong_count(leader.flight.as_ref().unwrap()) < 3 {
        tokio::task::yield_now().await;
    }
    waiting.abort();
    drop(leader);
    assert!(waiting.await.unwrap_err().is_cancelled());
    assert_eq!(cache.lock_state().unwrap().flight_live, 0);
    assert!(cache.lock_state().unwrap().flights.is_empty());
}
