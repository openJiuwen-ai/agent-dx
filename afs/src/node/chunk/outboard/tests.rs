use super::*;
use bao_tree::{BlockSize, io::outboard::PreOrderMemOutboard};

#[test]
fn p2_w0_sidecar_headroom_is_instantaneous_and_uses_largest_work_ticket() {
    let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
    let proof = build_borrowed(&vec![37; 128 * 1024], &budget)
        .outboard
        .unwrap();
    let pressure = budget
        .try_reserve(budget.limit() - budget.used() - 4 * RANGE_WORKING_BYTES)
        .unwrap();
    let before = budget.used();
    let pin = budget.pin_sidecar(&proof).unwrap();
    assert_eq!(
        budget.used(),
        before,
        "Arc pin cannot add another proof byte charge"
    );
    assert!(std::ptr::eq(
        proof.encoded().as_ptr(),
        pin.proof().encoded().as_ptr()
    ));
    // Original builder admission remains nonblocking and can consume this
    // headroom later; the queue never claims a permanent new partition.
    let extra = budget.try_reserve(1).unwrap();
    assert!(budget.pin_sidecar(&proof).is_none());
    drop(extra);
    assert!(budget.pin_sidecar(&proof).is_some());
    drop(pin);
    drop(pressure);
    drop(proof);
    assert_eq!(budget.used(), 0);
}

#[test]
fn p2_w0_sidecar_pins_keep_shared_allocation_transient_until_last_pin() {
    let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
    let proof = build_borrowed(&vec![31; 128 * 1024], &budget)
        .outboard
        .unwrap();
    let charge = proof.charged_bytes();
    let first = budget.pin_sidecar(&proof).unwrap();
    let second = budget.pin_sidecar(&proof).unwrap();
    assert!(!proof.try_mark_resident());
    assert_eq!((budget.used(), budget.resident_used()), (charge, 0));
    drop(first);
    assert!(!proof.try_mark_resident());
    drop(second);
    assert!(proof.try_mark_resident());
    assert!(
        budget.pin_sidecar(&proof).is_none(),
        "resident ownership must never be demoted for the queue"
    );
    assert_eq!((budget.used(), budget.resident_used()), (charge, charge));
    drop(proof);
    assert_eq!((budget.used(), budget.resident_used()), (0, 0));
}

#[test]
fn p2_w0_sidecar_rejects_foreign_ledger_and_retains_original_last_owner() {
    let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
    let foreign = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
    let proof = build_borrowed(&vec![13; 128 * 1024], &budget)
        .outboard
        .unwrap();
    assert!(foreign.pin_sidecar(&proof).is_none());
    let pin = budget.pin_sidecar(&proof).unwrap();
    let independent = proof.clone();
    drop(proof);
    drop(pin);
    assert!(budget.used() > 0);
    assert_eq!(foreign.used(), 0);
    drop(independent);
    assert_eq!(budget.used(), 0);
}

#[test]
fn p2_r3_header_tree_eligibility_includes_local_tree_without_peer_proof() {
    let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
    let mut local = OutboardBuilder::new(128 * 1024, budget);
    assert!(local.has_tree());
    assert!(!local.has_peer_tree());
    local.update(&[37]).unwrap();
    assert!(!local.has_tree());
    assert!(!OutboardBuilder::new(128 * 1024, OutboardBudget::new(0)).has_tree());
}

#[test]
fn p2_c1a_default_range_admission_waits_until_original_deadline() {
    let budget = OutboardBudget::new(RANGE_WORKING_BYTES);
    let occupied = budget.try_reserve(RANGE_WORKING_BYTES).unwrap();
    let entered = Instant::now();
    assert!(budget.try_reserve_range_work().is_none());
    let returned_after = entered.elapsed();
    eprintln!("range-work default exhausted return delay: {returned_after:?}");
    assert!(
        returned_after >= std::time::Duration::from_millis(2),
        "default range-work wait must use the original 2ms deadline"
    );
    assert_eq!(budget.used(), RANGE_WORKING_BYTES);
    drop(occupied);
    assert_eq!(budget.used(), 0);
}

#[test]
fn p2_c1a_last_holder_release_wakes_range_admission_without_early_refund() {
    use std::{sync::mpsc, thread};
    let budget = OutboardBudget::create(RANGE_WORKING_BYTES, None, Duration::from_millis(50));
    let occupied = Arc::new(budget.try_reserve(RANGE_WORKING_BYTES).unwrap());
    let last = occupied.clone();
    drop(occupied);
    assert_eq!(budget.used(), RANGE_WORKING_BYTES);
    let (checked, checked_rx) = mpsc::sync_channel(0);
    let (resume, resume_rx) = mpsc::sync_channel(0);
    budget.work_state.lock().unwrap().test_hooks.before_wait = Some((checked, resume_rx));
    let waiting_budget = budget.clone();
    let waiter = thread::spawn(move || waiting_budget.try_reserve_range_work());
    checked_rx.recv().unwrap();
    // Capacity was checked while holding work_state. Start the final
    // release at that exact check/wait boundary; its lock must serialize
    // with the waiter's atomic unlock-and-wait, so no notification is lost.
    let (releasing, releasing_rx) = mpsc::sync_channel(0);
    let releaser = thread::spawn(move || {
        releasing.send(()).unwrap();
        drop(last);
    });
    releasing_rx.recv().unwrap();
    assert_eq!(budget.used(), RANGE_WORKING_BYTES);
    resume.send(()).unwrap();
    let ticket = waiter.join().unwrap();
    releaser.join().unwrap();
    assert!(
        ticket.is_some(),
        "boundary release must wake range admission"
    );
    assert_eq!(budget.used(), RANGE_WORKING_BYTES);
    drop(ticket);
    assert_eq!(budget.used(), 0);
}

fn c1a_wait_for_waiters(budget: &OutboardBudget, count: usize) {
    let guard = Instant::now() + Duration::from_secs(2);
    loop {
        if budget.work_state.lock().unwrap().test_hooks.waiters == count {
            return;
        }
        assert!(
            Instant::now() < guard,
            "expected {count} parked range waiters"
        );
        std::thread::yield_now();
    }
}

#[test]
fn p2_c1a_zero_wait_and_builder_resident_remain_nonblocking() {
    let budget = OutboardBudget::create(RANGE_WORKING_BYTES, None, Duration::ZERO);
    let held = budget.try_reserve(RANGE_WORKING_BYTES).unwrap();
    assert!(budget.try_reserve_range_work().is_none());
    assert_eq!(budget.work_state.lock().unwrap().test_hooks.waiters, 0);
    assert!(budget.try_reserve(1).is_none());
    assert!(budget.try_reserve_resident(1).is_none());
    drop(held);
    assert!(budget.try_reserve_range_work().is_some());
}

#[test]
fn p2_c1a_stop_wakes_all_waiters_without_refunding_held_tickets() {
    let budget = OutboardBudget::create(RANGE_WORKING_BYTES, None, Duration::from_millis(50));
    let held = budget.try_reserve(RANGE_WORKING_BYTES).unwrap();
    let waiters: Vec<_> = (0..8)
        .map(|_| {
            let budget = budget.clone();
            std::thread::spawn(move || budget.try_reserve_range_work().is_none())
        })
        .collect();
    c1a_wait_for_waiters(&budget, 8);
    budget.stop_range_work();
    for waiter in waiters {
        assert!(waiter.join().unwrap());
    }
    assert_eq!(budget.used(), RANGE_WORKING_BYTES);
    drop(held);
    assert_eq!(budget.used(), 0);
    assert!(budget.try_reserve_range_work().is_none());
    // Stop affects range workers, not the builder/resident admission API.
    assert!(budget.try_reserve(1).is_some());
}

#[test]
fn p2_c1a_spurious_notifications_do_not_extend_entry_deadline() {
    let budget = OutboardBudget::create(RANGE_WORKING_BYTES, None, Duration::from_millis(5));
    let held = budget.try_reserve(RANGE_WORKING_BYTES).unwrap();
    let waiting = budget.clone();
    let waiter = std::thread::spawn(move || {
        let entered = Instant::now();
        let result = waiting.try_reserve_range_work();
        (result.is_none(), entered.elapsed())
    });
    c1a_wait_for_waiters(&budget, 1);
    // Keep waking while capacity remains unavailable. Completion must not
    // depend on the notifications ending or the held allocation retiring.
    let notify_until = Instant::now() + Duration::from_millis(500);
    while !waiter.is_finished() && Instant::now() < notify_until {
        budget.work_changed.notify_all();
        std::thread::yield_now();
    }
    assert!(
        waiter.is_finished(),
        "spurious wakeups renewed the waiting interval"
    );
    let (rejected, elapsed) = waiter.join().unwrap();
    assert!(rejected);
    eprintln!("5ms entry deadline under spurious wakeups: returned after {elapsed:?}");
    assert_eq!(budget.used(), RANGE_WORKING_BYTES);
    drop(held);
}

#[test]
fn p2_c1a_expired_check_wait_boundary_does_not_start_another_wait() {
    use std::sync::mpsc;
    let budget = OutboardBudget::create(RANGE_WORKING_BYTES, None, Duration::from_millis(2));
    let held = budget.try_reserve(RANGE_WORKING_BYTES).unwrap();
    let (checked, checked_rx) = mpsc::sync_channel(0);
    let (resume, resume_rx) = mpsc::sync_channel(0);
    budget.work_state.lock().unwrap().test_hooks.before_wait = Some((checked, resume_rx));
    let waiting = budget.clone();
    let waiter = std::thread::spawn(move || waiting.try_reserve_range_work().is_none());
    checked_rx.recv().unwrap();
    std::thread::sleep(Duration::from_millis(5));
    resume.send(()).unwrap();
    assert!(waiter.join().unwrap());
    assert_eq!(budget.work_state.lock().unwrap().test_hooks.waiters, 0);
    assert_eq!(budget.used(), RANGE_WORKING_BYTES);
    drop(held);
}

#[test]
fn p2_c1a_aligned_and_partial_work_account_their_real_lifetimes() {
    let aligned = [VerifiedRangeCopy {
        chunk_offset: 0,
        length: 128 * 1024,
        output_offset: 0,
    }];
    let partial = [VerifiedRangeCopy {
        chunk_offset: 1,
        length: 128 * 1024,
        output_offset: 0,
    }];
    assert_eq!(
        range_working_bytes(&aligned, 4 * 1024 * 1024),
        ALIGNED_RANGE_WORKING_BYTES
    );
    assert_eq!(
        range_working_bytes(&partial, 4 * 1024 * 1024),
        RANGE_WORKING_BYTES
    );
    let budget = OutboardBudget::new(ALIGNED_RANGE_WORKING_BYTES);
    let lease = budget
        .reserve_range_work(&aligned, 4 * 1024 * 1024)
        .unwrap();
    assert_eq!(budget.used(), ALIGNED_RANGE_WORKING_BYTES);
    drop(lease);
    assert_eq!(budget.used(), 0);
    assert!(
        budget
            .reserve_range_work(&partial, 4 * 1024 * 1024)
            .is_none()
    );
}

#[test]
#[ignore = "fixed resource acceptance: run with --release and --ignored; debug timings are not performance criteria"]
fn p2_c1a_32_threads_hot_object_with_resident_sublimit_full() {
    use std::{
        os::unix::fs::FileExt,
        sync::{Barrier, atomic::AtomicUsize},
    };
    let body = vec![59; 4 * 1024 * 1024];
    let chunk = expected(&body);
    let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
    let proofs: Vec<_> = (0..60)
        .map(|_| {
            let proof = build_borrowed(&body, &budget).outboard.unwrap();
            assert!(proof.try_mark_resident());
            proof
        })
        .collect();
    let remaining = budget.resident_limit() - budget.resident_used();
    let resident_padding = budget.try_reserve_resident(remaining).unwrap();
    assert_eq!(budget.resident_used(), budget.resident_limit());
    let file = tempfile::tempfile().unwrap();
    file.write_all_at(&body, 0).unwrap();
    let start = Arc::new(Barrier::new(32));
    let fallbacks = AtomicUsize::new(0);
    let started = Instant::now();
    std::thread::scope(|scope| {
        for worker in 0..32 {
            let (proof, file, chunk, start, fallbacks) =
                (&proofs[0], &file, &chunk, &start, &fallbacks);
            scope.spawn(move || {
                let mut out = vec![0; 128 * 1024];
                start.wait();
                for iteration in 0..1000 {
                    let offset = ((worker + iteration) % 32) * out.len();
                    let result = proof.read_ranges_into_with_work(
                        file,
                        0,
                        chunk,
                        &[VerifiedRangeCopy {
                            chunk_offset: offset as u64,
                            length: out.len(),
                            output_offset: 0,
                        }],
                        &mut out,
                        &mut OutboardReadWork::default(),
                    );
                    match result {
                        Ok(counts) => {
                            assert_eq!(counts, vec![out.len()]);
                            assert!(out.iter().all(|byte| *byte == 59));
                        }
                        Err(error) => {
                            assert_eq!(error.code(), afs_error::IO_CAPACITY);
                            fallbacks.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            });
        }
    });
    let count = fallbacks.load(Ordering::Relaxed);
    eprintln!(
        "C-1a fixed hot range load: reads=32000 capacity_fallbacks={count} elapsed={:?} resident={} total={}",
        started.elapsed(),
        budget.resident_used(),
        budget.used()
    );
    assert!(
        count * 100 < 32000 * 5,
        "range full-scan fallback must be below 5%"
    );
    assert_eq!(budget.used(), budget.resident_used());
    drop(resident_padding);
    drop(proofs);
    assert_eq!((budget.used(), budget.resident_used()), (0, 0));
}

#[test]
fn wi6_borrowed_builder_matches_official_standard_root_and_outboard() {
    for length in [
        0,
        1,
        1023,
        1024,
        32767,
        32768,
        32769,
        65536,
        4 * 1024 * 1024,
    ] {
        let body = (0..length).map(|n| (n % 251) as u8).collect::<Vec<_>>();
        let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
        let built = build_borrowed(&body, &budget);
        let official = PreOrderMemOutboard::create(&body, BlockSize::from_chunk_log(5));
        assert_eq!(built.digest.bytes, *blake3::hash(&body).as_bytes());
        assert_eq!(built.digest.bytes, *official.root.as_bytes());
        assert_eq!(
            built
                .outboard
                .as_ref()
                .expect("outboard required within budget")
                .raw_cv_bytes(),
            official.data
        );
        drop(built);
        assert_eq!(budget.used(), 0);
    }
}

#[test]
fn wi6_last_owner_releases_encoded_allocation_before_lease() {
    let body = vec![37; 150_001];
    let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
    let proof = build_borrowed(&body, &budget).outboard.unwrap();
    let held = proof.clone();
    OUTBOARD_DROP_EVENTS.with(|events| events.borrow_mut().clear());
    drop(proof);
    assert!(OUTBOARD_DROP_EVENTS.with(|events| events.borrow().is_empty()));
    let charge = held.charged_bytes();
    drop(held);
    let events = OUTBOARD_DROP_EVENTS.with(|events| events.borrow().clone());
    assert_eq!(
        events,
        vec![("encoded_freed", charge), ("lease_released", 0)]
    );
    assert_eq!(budget.used(), 0);
}

#[test]
fn wi6_rejected_encoded_input_frees_allocation_before_refund() {
    let body = vec![37; 150_001];
    let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
    let proof = build_borrowed(&body, &budget).outboard.unwrap();
    let mut encoded = proof.encoded().to_vec();
    encoded[0] ^= 1;
    let charge = ChunkOutboard::resident_charge(encoded.capacity()).unwrap();
    let lease = budget.try_reserve(charge).unwrap();
    drop(proof);
    OUTBOARD_DROP_EVENTS.with(|events| events.borrow_mut().clear());
    let error = ChunkOutboard::from_encoded(&expected(&body), encoded, lease).unwrap_err();
    assert_eq!(error.code(), afs_error::NODE_TRANSFER_CORRUPT_DATA);
    assert_eq!(
        OUTBOARD_DROP_EVENTS.with(|events| events.borrow().clone()),
        vec![("encoded_freed", charge), ("lease_released", 0)]
    );
    assert_eq!(budget.used(), 0);
}

fn expected(body: &[u8]) -> ChunkObject {
    super::super::StagedChunk::new(crate::dfs::OperationId::new("outboard-test"), body.to_vec())
        .chunk
}

#[test]
fn wi6_aligned_range_reads_file_once_and_copies_only_into_official_scratch() {
    use std::io::Write;
    let body = (0..4 * 1024 * 1024)
        .map(|n| (n % 251) as u8)
        .collect::<Vec<_>>();
    let chunk = expected(&body);
    let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
    let proof = build_borrowed(&body, &budget).outboard.unwrap();
    let before = budget.used();
    let mut file = tempfile::tempfile().unwrap();
    file.write_all(&[0xa5; 4096]).unwrap();
    file.write_all(&body).unwrap();
    file.write_all(b"trailer").unwrap();
    for offset in [0, 1024 * 1024, 3 * 1024 * 1024] {
        let length = 1024 * 1024;
        let mut out = vec![0xa5; length + 14];
        let mut work = OutboardReadWork::default();
        let counts = proof
            .read_ranges_into_with_work(
                &file,
                4096,
                &chunk,
                &[VerifiedRangeCopy {
                    chunk_offset: offset as u64,
                    length,
                    output_offset: 7,
                }],
                &mut out,
                &mut work,
            )
            .unwrap();
        assert_eq!(counts, vec![length]);
        assert_eq!(&out[7..7 + length], &body[offset..offset + length]);
        assert!(
            out[..7]
                .iter()
                .chain(out[7 + length..].iter())
                .all(|byte| *byte == 0xa5)
        );
        assert_eq!(
            work.read_calls, 1,
            "one contiguous file read, not one per group"
        );
        assert_eq!(work.hash_bytes, length as u64);
        assert_eq!(
            work.copied_bytes, length as u64,
            "only borrowed output into Bao validation scratch"
        );
        assert!(work.scratch_peak <= OUTBOARD_GROUP_BYTES);
        assert!(work.parent_hash_bytes > 0);
        assert_eq!(budget.used(), before);
    }
}

#[test]
fn wi6_aligned_range_validates_a_short_last_group_and_rejects_tampering() {
    use std::io::Write;
    use std::os::unix::fs::FileExt;
    let length = 3 * OUTBOARD_GROUP_BYTES + 37;
    let body = vec![37; length];
    let chunk = expected(&body);
    let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
    let proof = build_borrowed(&body, &budget).outboard.unwrap();
    let mut file = tempfile::tempfile().unwrap();
    file.write_all(&body).unwrap();
    let range = VerifiedRangeCopy {
        chunk_offset: OUTBOARD_GROUP_BYTES as u64,
        length,
        output_offset: 3,
    };
    let allowed = length - OUTBOARD_GROUP_BYTES;
    let mut out = vec![0xa5; length + 3];
    let mut work = OutboardReadWork::default();
    assert_eq!(
        proof
            .read_ranges_into_with_work(&file, 0, &chunk, &[range], &mut out, &mut work)
            .unwrap(),
        vec![allowed]
    );
    assert_eq!(&out[3..3 + allowed], &body[OUTBOARD_GROUP_BYTES..]);
    assert_eq!(work.read_calls, 1);
    assert_eq!(work.hash_bytes, allowed as u64);
    assert_eq!(work.copied_bytes, allowed as u64);
    // Output is caller-private until successful validation. On corruption
    // it may contain untrusted bytes, but must never be returned as success.
    file.write_at(b"X", (length - 1) as u64).unwrap();
    assert_eq!(
        proof
            .read_ranges_into(
                &file,
                0,
                &chunk,
                &[VerifiedRangeCopy {
                    chunk_offset: 0,
                    length,
                    output_offset: 0
                }],
                &mut out
            )
            .unwrap_err()
            .code(),
        afs_error::NODE_TRANSFER_CORRUPT_DATA
    );
    file.write_at(&[37], (length - 1) as u64).unwrap();
    let mut encoded = proof.encoded().to_vec();
    encoded[HEADER] ^= 1;
    let lease = budget
        .try_reserve(ChunkOutboard::resident_charge(encoded.capacity()).unwrap())
        .unwrap();
    let damaged = ChunkOutboard::from_encoded(&chunk, encoded, lease).unwrap();
    assert_eq!(
        damaged
            .read_ranges_into(
                &file,
                0,
                &chunk,
                &[VerifiedRangeCopy {
                    chunk_offset: 0,
                    length,
                    output_offset: 0
                }],
                &mut out
            )
            .unwrap_err()
            .code(),
        afs_error::NODE_TRANSFER_CORRUPT_DATA
    );
}

#[test]
fn wi6_aligned_range_rejects_bad_bounds_and_exhausted_budget_before_read() {
    use std::io::Write;
    let body = vec![19; 2 * OUTBOARD_GROUP_BYTES];
    let chunk = expected(&body);
    let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
    let proof = build_borrowed(&body, &budget).outboard.unwrap();
    let mut file = tempfile::tempfile().unwrap();
    file.write_all(&body).unwrap();
    let mut out = vec![0xa5; body.len()];
    let mut work = OutboardReadWork::default();
    assert!(
        proof
            .read_ranges_into_with_work(
                &file,
                0,
                &chunk,
                &[VerifiedRangeCopy {
                    chunk_offset: 0,
                    length: body.len(),
                    output_offset: 1
                }],
                &mut out,
                &mut work
            )
            .is_err()
    );
    assert_eq!(work.read_calls, 0);
    assert!(out.iter().all(|byte| *byte == 0xa5));
    let held = budget.try_reserve(budget.limit() - budget.used()).unwrap();
    assert_eq!(
        proof
            .read_ranges_into_with_work(
                &file,
                0,
                &chunk,
                &[VerifiedRangeCopy {
                    chunk_offset: 0,
                    length: body.len(),
                    output_offset: 0
                }],
                &mut out,
                &mut work
            )
            .unwrap_err()
            .code(),
        afs_error::IO_CAPACITY
    );
    assert_eq!(work.read_calls, 0);
    assert!(out.iter().all(|byte| *byte == 0xa5));
    drop(held);
    file.set_len(body.len() as u64 - 1).unwrap();
    assert_eq!(
        proof
            .read_ranges_into(
                &file,
                0,
                &chunk,
                &[VerifiedRangeCopy {
                    chunk_offset: 0,
                    length: body.len(),
                    output_offset: 0
                }],
                &mut out
            )
            .unwrap_err()
            .code(),
        afs_error::NODE_TRANSFER_CORRUPT_DATA
    );
}

#[test]
fn wi6_thousand_random_ranges_match_body_with_nonzero_file_offset() {
    use std::io::Write;
    let body = (0..150_001).map(|n| (n % 251) as u8).collect::<Vec<_>>();
    let chunk = expected(&body);
    let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
    let proof = build_borrowed(&body, &budget).outboard.unwrap();
    let mut file = tempfile::tempfile().unwrap();
    file.write_all(&vec![0xa5; 4096]).unwrap();
    file.write_all(&body).unwrap();
    file.write_all(b"trailer").unwrap();
    let mut seed = 0x3ce6_72d9_018a_c45b_u64;
    for _ in 0..1000 {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        let offset = seed % (body.len() as u64 + 100);
        let length = ((seed >> 19) % 8192) as usize;
        let mut out = vec![0xa5; length + 11];
        let counts = proof
            .read_ranges_into(
                &file,
                4096,
                &chunk,
                &[VerifiedRangeCopy {
                    chunk_offset: offset,
                    length,
                    output_offset: 7,
                }],
                &mut out,
            )
            .unwrap();
        let allowed = (body.len() as u64)
            .saturating_sub(offset)
            .min(length as u64) as usize;
        assert_eq!(counts, vec![allowed]);
        if allowed != 0 {
            assert_eq!(
                &out[7..7 + allowed],
                &body[offset as usize..offset as usize + allowed]
            );
        }
        assert_eq!(&out[..7], &[0xa5; 7]);
        assert!(out[7 + allowed..].iter().all(|byte| *byte == 0xa5));
    }
}

#[test]
fn wi6_shape_body_and_cv_tampering_report_structured_corruption() {
    use std::io::Write;
    let body = vec![37; 150_001];
    let chunk = expected(&body);
    let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
    let proof = build_borrowed(&body, &budget).outboard.unwrap();
    for byte in [0, 8, 10, 11, 16, 24, 56, 60] {
        let mut raw = proof.encoded().to_vec();
        raw[byte] ^= 1;
        assert_eq!(
            ChunkOutboard::validate_encoded_shape(&chunk, &raw)
                .unwrap_err()
                .code(),
            afs_error::NODE_TRANSFER_CORRUPT_DATA
        );
    }
    let mut file = tempfile::tempfile().unwrap();
    file.write_all(&body).unwrap();
    let mut damaged = proof.encoded().to_vec();
    damaged[64] ^= 1;
    let lease = budget
        .try_reserve(ChunkOutboard::resident_charge(damaged.capacity()).unwrap())
        .unwrap();
    let damaged = ChunkOutboard::from_encoded(&chunk, damaged, lease).unwrap();
    let mut out = [0xa5; 17];
    assert_eq!(
        damaged
            .read_ranges_into(
                &file,
                0,
                &chunk,
                &[VerifiedRangeCopy {
                    chunk_offset: 3,
                    length: 17,
                    output_offset: 0
                }],
                &mut out
            )
            .unwrap_err()
            .code(),
        afs_error::NODE_TRANSFER_CORRUPT_DATA
    );
    assert_eq!(out, [0xa5; 17]);
    #[cfg(unix)]
    std::os::unix::fs::FileExt::write_at(&file, b"X", 3).unwrap();
    let mut work = OutboardReadWork::default();
    assert_eq!(
        proof
            .read_ranges_into_with_work(
                &file,
                0,
                &chunk,
                &[VerifiedRangeCopy {
                    chunk_offset: 3,
                    length: 17,
                    output_offset: 0
                }],
                &mut out,
                &mut work
            )
            .unwrap_err()
            .code(),
        afs_error::NODE_TRANSFER_CORRUPT_DATA
    );
    assert_eq!(work.hash_bytes, OUTBOARD_GROUP_BYTES as u64);
    assert!(work.parent_hash_bytes > 0);
    assert_eq!(out, [0xa5; 17]);
    let mut validating = OutboardBuilder::validating(chunk, damaged, budget);
    let result = validating.update(&body).and_then(|_| validating.finish());
    assert_eq!(
        result.unwrap_err().code(),
        afs_error::NODE_TRANSFER_CORRUPT_DATA
    );
}

#[test]
fn wi6_budget_zero_large_objects_and_failed_streams_keep_safe_root_fallback() {
    for length in [0, 100_001, 4 * 1024 * 1024 + 1] {
        let body = vec![13; length];
        let budget = OutboardBudget::new(0);
        let mut builder = OutboardBuilder::new(length as u64, budget.clone());
        for frame in body.chunks(4099) {
            builder.update(frame).unwrap();
        }
        let result = builder.finish().unwrap();
        assert_eq!(result.digest.bytes, *blake3::hash(&body).as_bytes());
        assert!(result.outboard.is_none());
        assert_eq!(budget.used(), 0);
    }
    let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
    let mut short = OutboardBuilder::new(4, budget.clone());
    short.update(b"abc").unwrap();
    assert!(short.finish().is_err());
    let mut long = OutboardBuilder::new(3, budget.clone());
    long.update(b"abc").unwrap();
    assert!(long.update(b"d").is_err());
    assert!(long.finish().is_err());
    assert_eq!(budget.used(), 0);
}

#[test]
fn wi6_noncontiguous_and_unaligned_ranges_keep_verified_fallback() {
    use std::io::Write;
    let body = (0..3 * OUTBOARD_GROUP_BYTES + 17)
        .map(|n| (n % 251) as u8)
        .collect::<Vec<_>>();
    let chunk = expected(&body);
    let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
    let proof = build_borrowed(&body, &budget).outboard.unwrap();
    let mut file = tempfile::tempfile().unwrap();
    file.write_all(&body).unwrap();
    let ranges = [
        VerifiedRangeCopy {
            chunk_offset: 3,
            length: 21,
            output_offset: 2,
        },
        VerifiedRangeCopy {
            chunk_offset: (2 * OUTBOARD_GROUP_BYTES + 3) as u64,
            length: 17,
            output_offset: 40,
        },
    ];
    let mut out = [0xa5; 64];
    let mut work = OutboardReadWork::default();
    assert_eq!(
        proof
            .read_ranges_into_with_work(&file, 0, &chunk, &ranges, &mut out, &mut work)
            .unwrap(),
        vec![21, 17]
    );
    assert_eq!(&out[2..23], &body[3..24]);
    assert_eq!(
        &out[40..57],
        &body[2 * OUTBOARD_GROUP_BYTES + 3..2 * OUTBOARD_GROUP_BYTES + 20]
    );
    assert!(
        out[..2]
            .iter()
            .chain(out[23..40].iter())
            .chain(out[57..].iter())
            .all(|byte| *byte == 0xa5)
    );
    assert_eq!(work.hash_bytes, (2 * OUTBOARD_GROUP_BYTES) as u64);
    assert_eq!(work.read_calls, 2);
}

#[test]
fn r8_unaligned_mebibyte_reads_middle_once_with_bounded_edges() {
    use std::io::Write;
    let body: Vec<u8> = (0..4 * 1024 * 1024).map(|n| (n % 251) as u8).collect();
    let chunk = expected(&body);
    let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
    let proof = build_borrowed(&body, &budget).outboard.unwrap();
    let retained = budget.used();
    let mut file = tempfile::tempfile().unwrap();
    file.write_all(&[0xa5; 4096]).unwrap();
    file.write_all(&body).unwrap();
    let length = 1024 * 1024;
    let mut out = vec![0xa5; length + 14];
    let mut work = OutboardReadWork::default();
    assert_eq!(
        proof
            .read_ranges_into_with_work(
                &file,
                4096,
                &chunk,
                &[VerifiedRangeCopy {
                    chunk_offset: 7,
                    length,
                    output_offset: 7
                }],
                &mut out,
                &mut work,
            )
            .unwrap(),
        vec![length]
    );
    assert_eq!(&out[7..7 + length], &body[7..7 + length]);
    assert!(
        out[..7]
            .iter()
            .chain(out[7 + length..].iter())
            .all(|byte| *byte == 0xa5)
    );
    let covered = (length + OUTBOARD_GROUP_BYTES) as u64;
    assert_eq!(
        work.hash_bytes, covered,
        "both partial edge groups still authenticate completely"
    );
    assert_eq!(
        budget.used(),
        retained,
        "range scratch retires before return"
    );
    eprintln!(
        "R8 unaligned: reads={} hash={} copy={} scratch={}",
        work.read_calls, work.hash_bytes, work.copied_bytes, work.scratch_peak
    );
    assert!(
        work.read_calls <= 3,
        "one middle read plus at most two edge reads"
    );
    assert!(
        work.copied_bytes <= covered + (2 * OUTBOARD_GROUP_BYTES) as u64,
        "codec encode/decode cannot add two complete coverage copies"
    );
    assert!(work.scratch_peak <= RANGE_WORKING_BYTES);
}

#[test]
fn r8_noncontiguous_large_interiors_scale_reads_with_ranges_not_groups() {
    use std::io::Write;
    let body: Vec<u8> = (0..4 * 1024 * 1024).map(|n| (n % 251) as u8).collect();
    let chunk = expected(&body);
    let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
    let proof = build_borrowed(&body, &budget).outboard.unwrap();
    let retained = budget.used();
    let mut file = tempfile::tempfile().unwrap();
    file.write_all(&body).unwrap();
    let length = 512 * 1024;
    let second_source = 2 * 1024 * 1024 + 11;
    let second_output = length + 19;
    let ranges = [
        VerifiedRangeCopy {
            chunk_offset: 7,
            length,
            output_offset: 7,
        },
        VerifiedRangeCopy {
            chunk_offset: second_source as u64,
            length,
            output_offset: second_output,
        },
    ];
    let mut out = vec![0xa5; second_output + length + 7];
    let mut work = OutboardReadWork::default();
    assert_eq!(
        proof
            .read_ranges_into_with_work(&file, 0, &chunk, &ranges, &mut out, &mut work,)
            .unwrap(),
        vec![length, length]
    );
    assert_eq!(&out[7..7 + length], &body[7..7 + length]);
    assert_eq!(
        &out[second_output..second_output + length],
        &body[second_source..second_source + length]
    );
    assert!(
        out[..7]
            .iter()
            .chain(out[7 + length..second_output].iter())
            .chain(out[second_output + length..].iter())
            .all(|byte| *byte == 0xa5)
    );
    let covered = (2 * (length + OUTBOARD_GROUP_BYTES)) as u64;
    assert_eq!(work.hash_bytes, covered);
    assert_eq!(budget.used(), retained);
    eprintln!(
        "R8 multi-range: reads={} hash={} copy={} scratch={}",
        work.read_calls, work.hash_bytes, work.copied_bytes, work.scratch_peak
    );
    assert!(
        work.read_calls <= 6,
        "two middle reads plus at most four edge reads"
    );
    assert!(work.copied_bytes <= covered + (4 * OUTBOARD_GROUP_BYTES) as u64);
    assert!(work.scratch_peak <= RANGE_WORKING_BYTES);
}

#[test]
fn r8_aliasing_outputs_preserve_coverage_group_write_order() {
    use std::io::Write;
    let body: Vec<u8> = (0..4 * OUTBOARD_GROUP_BYTES + 17)
        .map(|n| (n % 251) as u8)
        .collect();
    let chunk = expected(&body);
    let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
    let proof = build_borrowed(&body, &budget).outboard.unwrap();
    let mut file = tempfile::tempfile().unwrap();
    file.write_all(&body).unwrap();
    let ranges = [
        VerifiedRangeCopy {
            chunk_offset: (OUTBOARD_GROUP_BYTES + 1) as u64,
            length: OUTBOARD_GROUP_BYTES,
            output_offset: 7,
        },
        VerifiedRangeCopy {
            chunk_offset: 3,
            length: OUTBOARD_GROUP_BYTES / 2,
            output_offset: 7,
        },
    ];
    let mut out = vec![0xa5; OUTBOARD_GROUP_BYTES + 14];
    let mut work = OutboardReadWork::default();
    assert_eq!(
        proof
            .read_ranges_into_with_work(&file, 0, &chunk, &ranges, &mut out, &mut work,)
            .unwrap(),
        vec![OUTBOARD_GROUP_BYTES, OUTBOARD_GROUP_BYTES / 2]
    );
    // Group 0 writes the second mapping first. Groups 1/2 overwrite it
    // with the first mapping, despite the second mapping's later position.
    assert_eq!(
        &out[7..7 + OUTBOARD_GROUP_BYTES],
        &body[OUTBOARD_GROUP_BYTES + 1..2 * OUTBOARD_GROUP_BYTES + 1]
    );
    assert!(
        out[..7]
            .iter()
            .chain(out[7 + OUTBOARD_GROUP_BYTES..].iter())
            .all(|byte| *byte == 0xa5)
    );
    assert_eq!(work.hash_bytes, (3 * OUTBOARD_GROUP_BYTES) as u64);
    assert!(work.scratch_peak <= RANGE_WORKING_BYTES);
}

#[test]
fn r8_private_edges_authenticate_padding_and_bound_short_tail_before_delivery() {
    use std::{io::Write, os::unix::fs::FileExt};
    let body: Vec<u8> = (0..3 * OUTBOARD_GROUP_BYTES + 37)
        .map(|n| (n % 251) as u8)
        .collect();
    let chunk = expected(&body);
    let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
    let proof = build_borrowed(&body, &budget).outboard.unwrap();
    let retained = budget.used();
    let mut file = tempfile::tempfile().unwrap();
    file.write_all(&body).unwrap();
    let length = 2 * OUTBOARD_GROUP_BYTES + 23;
    for corrupt_offset in [0, OUTBOARD_GROUP_BYTES + 17, 2 * OUTBOARD_GROUP_BYTES + 31] {
        file.write_all_at(&body, 0).unwrap();
        file.write_all_at(&[body[corrupt_offset] ^ 1], corrupt_offset as u64)
            .unwrap();
        let mut private = vec![0xa5; length + 14];
        let error = proof
            .read_ranges_into_with_work(
                &file,
                0,
                &chunk,
                &[VerifiedRangeCopy {
                    chunk_offset: 7,
                    length,
                    output_offset: 7,
                }],
                &mut private,
                &mut OutboardReadWork::default(),
            )
            .unwrap_err();
        assert_eq!(error.code(), afs_error::NODE_TRANSFER_CORRUPT_DATA);
        assert!(
            private[..7]
                .iter()
                .chain(private[7 + length..].iter())
                .all(|byte| *byte == 0xa5)
        );
        assert_eq!(budget.used(), retained);
    }
    file.write_all_at(&body, 0).unwrap();
    let tail_offset = 3;
    let allowed = body.len() - tail_offset;
    let mut private = vec![0xa5; allowed + 14];
    let mut work = OutboardReadWork::default();
    assert_eq!(
        proof
            .read_ranges_into_with_work(
                &file,
                0,
                &chunk,
                &[VerifiedRangeCopy {
                    chunk_offset: tail_offset as u64,
                    length: 4 * OUTBOARD_GROUP_BYTES,
                    output_offset: 7
                }],
                &mut private,
                &mut work,
            )
            .unwrap(),
        vec![allowed]
    );
    assert_eq!(&private[7..7 + allowed], &body[tail_offset..]);
    assert!(
        private[..7]
            .iter()
            .chain(private[7 + allowed..].iter())
            .all(|byte| *byte == 0xa5)
    );
    assert_eq!(
        work.hash_bytes,
        body.len() as u64,
        "the short final coverage group remains complete after a long interior"
    );
    assert!(work.read_calls <= 2);
    assert!(work.scratch_peak <= RANGE_WORKING_BYTES);
    let mut rejected = vec![0xa5; allowed];
    let mut work = OutboardReadWork::default();
    assert!(
        proof
            .read_ranges_into_with_work(
                &file,
                0,
                &chunk,
                &[VerifiedRangeCopy {
                    chunk_offset: tail_offset as u64,
                    length: 4 * OUTBOARD_GROUP_BYTES,
                    output_offset: 1
                }],
                &mut rejected,
                &mut work,
            )
            .is_err()
    );
    assert_eq!(work.read_calls, 0);
    assert!(rejected.iter().all(|byte| *byte == 0xa5));
    assert_eq!(budget.used(), retained);
}

#[test]
fn r8_source_aliases_keep_codec_verification_for_each_destination() {
    use std::io::Write;
    let body: Vec<u8> = (0..4 * OUTBOARD_GROUP_BYTES)
        .map(|n| (n % 251) as u8)
        .collect();
    let chunk = expected(&body);
    let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
    let proof = build_borrowed(&body, &budget).outboard.unwrap();
    let mut file = tempfile::tempfile().unwrap();
    file.write_all(&body).unwrap();
    let second_output = OUTBOARD_GROUP_BYTES + 32;
    let ranges = [
        VerifiedRangeCopy {
            chunk_offset: 7,
            length: OUTBOARD_GROUP_BYTES + 17,
            output_offset: 0,
        },
        VerifiedRangeCopy {
            chunk_offset: (OUTBOARD_GROUP_BYTES / 2) as u64,
            length: OUTBOARD_GROUP_BYTES,
            output_offset: second_output,
        },
    ];
    let mut out = vec![0xa5; second_output + OUTBOARD_GROUP_BYTES + 7];
    let mut work = OutboardReadWork::default();
    assert_eq!(
        proof
            .read_ranges_into_with_work(&file, 0, &chunk, &ranges, &mut out, &mut work,)
            .unwrap(),
        vec![OUTBOARD_GROUP_BYTES + 17, OUTBOARD_GROUP_BYTES]
    );
    assert_eq!(
        &out[..OUTBOARD_GROUP_BYTES + 17],
        &body[7..OUTBOARD_GROUP_BYTES + 24]
    );
    assert_eq!(
        &out[second_output..second_output + OUTBOARD_GROUP_BYTES],
        &body[OUTBOARD_GROUP_BYTES / 2..3 * OUTBOARD_GROUP_BYTES / 2]
    );
    assert!(
        out[OUTBOARD_GROUP_BYTES + 17..second_output]
            .iter()
            .chain(out[second_output + OUTBOARD_GROUP_BYTES..].iter())
            .all(|byte| *byte == 0xa5)
    );
    assert_eq!(
        work.read_calls, 2,
        "shared source groups remain deduplicated"
    );
    assert_eq!(work.hash_bytes, (2 * OUTBOARD_GROUP_BYTES) as u64);
    assert!(work.scratch_peak <= RANGE_WORKING_BYTES);
}

#[test]
fn wi6_peer_tree_eligibility_is_frozen_before_any_body_and_rejects_root_fallback() {
    let body = vec![37; 2 * OUTBOARD_GROUP_BYTES];
    let chunk = expected(&body);
    let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
    let proof = build_borrowed(&body, &budget).outboard.unwrap();
    let mut builder = OutboardBuilder::validating(chunk.clone(), proof.clone(), budget.clone());
    assert!(builder.has_peer_tree());
    builder.update(&body[..1]).unwrap();
    assert!(!builder.has_peer_tree());
    builder.update(&body[1..]).unwrap();
    assert_eq!(builder.finish().unwrap().digest, chunk.content_digest);
    let fallback =
        OutboardBuilder::validating(chunk.clone(), proof.clone(), OutboardBudget::new(0));
    assert!(!fallback.has_peer_tree());
    assert!(!OutboardBuilder::new(body.len() as u64, budget.clone()).has_peer_tree());
    let mut wrong = chunk.clone();
    wrong.content_digest.bytes[0] ^= 1;
    assert!(!OutboardBuilder::validating(wrong, proof.clone(), budget.clone()).has_peer_tree());
    let mut failed = OutboardBuilder::validating(chunk, proof, budget);
    assert!(failed.update(&vec![0; body.len() + 1]).is_err());
    assert!(!failed.has_peer_tree());
}

#[test]
fn wi6_incremental_builder_accepts_borrowed_frames_without_rescan() {
    let body = vec![37; 150_001];
    let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
    let mut builder = OutboardBuilder::new(body.len() as u64, budget.clone());
    for frame in body.chunks(173) {
        builder.update(frame).unwrap();
    }
    let built = builder.finish().unwrap();
    assert_eq!(built.digest.bytes, *blake3::hash(&body).as_bytes());
    assert!(built.outboard.is_some());
}
#[test]
fn pr60_resident_mark_is_shared_until_last_owner_and_not_a_second_pool() {
    let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
    let body = vec![31; 64 * 1024];
    let proof = build_borrowed(&body, &budget).outboard.unwrap();
    let charge = proof.charged_bytes();
    assert_eq!(budget.resident_used(), 0);
    assert_eq!(budget.used(), charge);
    let barrier = Arc::new(std::sync::Barrier::new(17));
    let mut threads = Vec::new();
    for _ in 0..16 {
        let proof = proof.clone();
        let barrier = barrier.clone();
        threads.push(std::thread::spawn(move || {
            barrier.wait();
            proof.try_mark_resident()
        }));
    }
    barrier.wait();
    let admitted: Vec<_> = threads
        .into_iter()
        .map(|thread| thread.join().unwrap())
        .collect();
    assert!(admitted.into_iter().all(|value| value));
    assert_eq!(
        budget.resident_used(),
        charge,
        "one allocation, one resident admission"
    );
    assert_eq!(
        budget.used(),
        charge,
        "promotion cannot double-charge total"
    );
    let last = proof.clone();
    drop(proof);
    assert_eq!(budget.resident_used(), charge);
    drop(last);
    assert_eq!(budget.resident_used(), 0);
    assert_eq!(budget.used(), 0);
}

#[test]
fn pr60_resident_lease_resize_and_rejection_keep_one_exact_total() {
    let budget = OutboardBudget::new(1024);
    let mut resident = budget.try_reserve_resident(512).unwrap();
    assert_eq!((budget.used(), budget.resident_used()), (512, 512));
    assert!(budget.try_reserve_resident(1).is_none());
    assert_eq!((budget.used(), budget.resident_used()), (512, 512));
    let working = budget.try_reserve(512).unwrap();
    assert_eq!((budget.used(), budget.resident_used()), (1024, 512));
    drop(working);
    resident.retain_capacity(256).unwrap();
    assert_eq!((budget.used(), budget.resident_used()), (256, 256));
    resident.retain_capacity(384).unwrap();
    assert_eq!((budget.used(), budget.resident_used()), (384, 384));
    assert!(resident.retain_capacity(513).is_err());
    assert_eq!((budget.used(), budget.resident_used()), (384, 384));
    drop(resident);
    assert_eq!((budget.used(), budget.resident_used()), (0, 0));
}

#[test]
fn pr60_range_work_ticket_keeps_ledger_identity_and_is_not_reserved_twice() {
    let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
    let body = vec![43; 64 * 1024];
    let chunk = expected(&body);
    let proof = build_borrowed(&body, &budget).outboard.unwrap();
    let file = tempfile::tempfile().unwrap();
    use std::os::unix::fs::FileExt;
    file.write_all_at(&body, 0).unwrap();
    let range = [VerifiedRangeCopy {
        chunk_offset: 0,
        output_offset: 0,
        length: OUTBOARD_GROUP_BYTES,
    }];
    let mut out = vec![0; OUTBOARD_GROUP_BYTES];
    let foreign = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
    let work = foreign.reserve_range_work(&range, chunk.length).unwrap();
    let mut observed = OutboardReadWork::default();
    assert!(
        proof
            .read_ranges_into_with_reserved_work(
                &file,
                0,
                &chunk,
                &range,
                &mut out,
                OutboardReadContext {
                    work: &mut observed,
                    working: work
                },
            )
            .is_err()
    );
    assert_eq!(observed.read_calls, 0);
    assert_eq!(foreign.used(), 0);
    let undersized = budget.try_reserve(1).unwrap();
    assert!(
        proof
            .read_ranges_into_with_reserved_work(
                &file,
                0,
                &chunk,
                &range,
                &mut out,
                OutboardReadContext {
                    work: &mut observed,
                    working: undersized
                },
            )
            .is_err()
    );
    let ticket = budget.reserve_range_work(&range, chunk.length).unwrap();
    assert_eq!(ticket.bytes, ALIGNED_RANGE_WORKING_BYTES);
    let pressure = budget.try_reserve(budget.limit() - budget.used()).unwrap();
    assert_eq!(budget.used(), budget.limit());
    proof
        .read_ranges_into_with_reserved_work(
            &file,
            0,
            &chunk,
            &range,
            &mut out,
            OutboardReadContext {
                work: &mut observed,
                working: ticket,
            },
        )
        .unwrap();
    assert_eq!(out, body[..OUTBOARD_GROUP_BYTES]);
    assert!(observed.hash_bytes > 0);
    drop(pressure);
    drop(proof);
    assert_eq!(budget.used(), 0);
}

#[test]
fn pr60_ledger_metrics_do_not_call_read_builder_fallback_a_native_write() {
    let registry = afs_metrics::Registry::new();
    let metrics = super::super::super::metrics::work::ReadWorkMetrics::register(&registry).unwrap();
    let budget = OutboardBudget::new_with_metrics(1024, metrics);
    let resident = budget.try_reserve_resident(512).unwrap();
    let working = budget.try_reserve(256).unwrap();
    let built = build_borrowed(&[11; 4096], &budget);
    assert!(built.outboard.is_none());
    let text = afs_metrics::encode_text(&registry).unwrap();
    assert!(text.contains("afs_dfs_outboard_events_total{event=\"write_without_proof\"} 0\n"));
    assert!(text.contains("afs_dfs_outboard_metadata_bytes 768\n"));
    assert!(text.contains("afs_dfs_outboard_resident_bytes 512\n"));
    assert!(budget.try_reserve_resident(1).is_none());
    assert!(budget.try_reserve(1024).is_none());
    budget.record_write_without_proof();
    budget.record_range_fullscan_fallback();
    budget.record_range_fullscan_unretained();
    drop(working);
    drop(resident);
    let text = afs_metrics::encode_text(&registry).unwrap();
    assert!(text.contains("afs_dfs_outboard_metadata_bytes 0\n"));
    assert!(text.contains("afs_dfs_outboard_resident_bytes 0\n"));
    for event in [
        "reserve_resident_failed",
        "write_without_proof",
        "range_fullscan_fallback",
        "range_fullscan_unretained",
    ] {
        assert!(
            text.contains(&format!(
                "afs_dfs_outboard_events_total{{event=\"{event}\"}} 1\n"
            )),
            "{text}"
        );
    }
}
