use super::*;

fn lease(budget: &Arc<NativeWriteBudget>, bytes: usize) -> NativeWriteLease {
    match budget.try_reserve(bytes).unwrap() {
        NativeWriteAdmission::Reserved(lease) => lease,
        NativeWriteAdmission::Wait { .. } => panic!("test budget must have capacity"),
    }
}

#[test]
fn p2_w2_native_last_slice_keeps_actual_charge_and_freeze_address() {
    let pool = WriteBufferPool::new(8192);
    let budget = NativeWriteBudget::new(4096);
    let mut builder =
        ChunkBuilder::with_native_lease(pool.clone(), 4096, lease(&budget, 4096)).unwrap();
    builder.write_at(0, &[7; 4096]).unwrap();
    let pointer = builder.data_ptr();
    let allocation = builder.allocation_id();
    let body = builder.freeze();
    assert_eq!(body.as_ptr(), pointer);
    assert_eq!(body.allocation_id(), allocation);
    let slow_transport = body.slice(4095..);
    let staged = StagedChunk::from_shared(OperationId::new("native-lease-owner"), body.clone());
    drop(body);
    drop(staged);
    assert_eq!(budget.snapshot().live_bytes, 4096);
    assert_eq!(budget.snapshot().leases, 1);
    assert_eq!(pool.snapshot().idle_bytes, 0);
    assert_eq!(slow_transport.as_ref(), &[7]);
    assert!(matches!(
        budget.try_reserve(1).unwrap(),
        NativeWriteAdmission::Wait { .. }
    ));
    drop(slow_transport);
    assert_eq!(budget.snapshot().live_bytes, 0);
    assert_eq!(budget.snapshot().leases, 0);
    assert_eq!(pool.snapshot().idle_bytes, 4096);
}

#[test]
fn p2_w2_native_failed_allocation_refunds_admission_without_body_or_copy() {
    let pool = WriteBufferPool::new(0);
    let budget = NativeWriteBudget::new(4096);
    pool.fail_next_allocation();
    let error =
        ChunkBuilder::with_native_lease(pool.clone(), 4096, lease(&budget, 4096)).unwrap_err();
    assert_eq!(error.code(), afs_error::IO_OUT_OF_MEMORY);
    assert_eq!(budget.snapshot().live_bytes, 0);
    assert_eq!(budget.snapshot().leases, 0);
    assert_eq!(pool.snapshot().live_bytes, 0);
    assert_eq!(pool.snapshot().copied_bytes, 0);
    drop(ChunkBuilder::with_native_lease(pool, 4096, lease(&budget, 4096)).unwrap());
    assert_eq!(budget.snapshot().live_bytes, 0);
}

#[test]
fn p2_w2_native_idle_actual_capacity_is_never_borrowed_without_charge() {
    let pool = WriteBufferPool::new(8192);
    let large = ChunkBuilder::with_pool(pool.clone(), 4096).unwrap();
    let pointer = large.data_ptr();
    drop(large);
    let small_budget = NativeWriteBudget::new(3000);
    let small =
        ChunkBuilder::with_native_lease(pool.clone(), 3000, lease(&small_budget, 3000)).unwrap();
    assert_eq!(small.capacity(), 3000);
    assert_ne!(small.data_ptr(), pointer);
    assert_eq!(small_budget.snapshot().live_bytes, small.capacity());
    assert_eq!(pool.snapshot().idle_bytes, 4096);
    drop(small);
    let reuse_pool = WriteBufferPool::new(4096);
    drop(ChunkBuilder::with_pool(reuse_pool.clone(), 4096).unwrap());
    let large_budget = NativeWriteBudget::new(4096);
    let reused =
        ChunkBuilder::with_native_lease(reuse_pool, 3000, lease(&large_budget, 4096)).unwrap();
    assert_eq!(reused.capacity(), 4096);
    assert_eq!(large_budget.snapshot().live_bytes, 4096);
    assert_eq!(reused.writable_capacity(), 3000);
    drop(reused);
    assert_eq!(large_budget.snapshot().live_bytes, 0);
    assert_eq!(pool.snapshot().copied_bytes, 0);
}

#[test]
fn p2_w2_native_stop_keeps_admitted_checkout_and_live_owner_until_last_drop() {
    let pool = WriteBufferPool::new(4096);
    let budget = NativeWriteBudget::new(4096);
    let mut admitted = lease(&budget, 3000);
    budget.stop();
    assert_eq!(
        budget.try_reserve(1).unwrap_err().code(),
        afs_error::IO_UNAVAILABLE
    );
    assert!(
        admitted.try_grow_to(4096).unwrap(),
        "stop must not revoke an accepted owner"
    );
    drop(ChunkBuilder::with_pool(pool.clone(), 4096).unwrap());
    let mut builder = ChunkBuilder::with_native_lease(pool.clone(), 3000, admitted).unwrap();
    assert_eq!(builder.capacity(), 4096);
    assert_eq!(budget.snapshot().live_bytes, 4096);
    builder.write_at(0, b"ok").unwrap();
    let body = builder.freeze();
    let retained = body.slice(..);
    drop(body);
    pool.stop();
    assert_eq!(budget.snapshot().live_bytes, 4096);
    drop(retained);
    assert_eq!(budget.snapshot().live_bytes, 0);
    assert_eq!(pool.snapshot().idle_bytes, 0);
}

#[test]
fn p2_w2_native_failed_grow_keeps_original_charge_until_owner_retires() {
    let budget = NativeWriteBudget::new(4096);
    let mut first = lease(&budget, 3000);
    let second = lease(&budget, 1096);
    assert!(!first.try_grow_to(4096).unwrap());
    assert_eq!(first.charged_bytes(), 3000);
    assert_eq!(budget.snapshot().live_bytes, 4096);
    drop(second);
    assert!(first.try_grow_to(4096).unwrap());
    assert_eq!(budget.snapshot().live_bytes, 4096);
    drop(first);
    assert_eq!(budget.snapshot().live_bytes, 0);
}

#[test]
fn p2_w2_native_release_between_failed_admission_and_wait_is_not_lost() {
    let budget = NativeWriteBudget::new(4096);
    let owner = lease(&budget, 4096);
    let NativeWriteAdmission::Wait { epoch } = budget.try_reserve(1).unwrap() else {
        panic!("full ledger must return its observed epoch");
    };
    // Deterministically release before the waiter enters Condvar::wait.
    drop(owner);
    let (tx, rx) = std::sync::mpsc::channel();
    let waiting = budget.clone();
    let join = std::thread::spawn(move || {
        tx.send(waiting.wait_for_change(epoch, Duration::from_secs(10)))
            .unwrap();
    });
    let state = rx
        .recv_timeout(Duration::from_secs(1))
        .expect("must observe prior release without waiting again");
    join.join().unwrap();
    assert_ne!(state.epoch, epoch);
    assert_eq!(state.live_bytes, 0);
}

#[test]
fn p2_w2_native_stop_wakes_waiter_without_refunding_existing_body() {
    let budget = NativeWriteBudget::new(4096);
    let owner = lease(&budget, 4096);
    let NativeWriteAdmission::Wait { epoch } = budget.try_reserve(1).unwrap() else {
        panic!("full ledger must wait");
    };
    let (tx, rx) = std::sync::mpsc::channel();
    let waiting = budget.clone();
    let join = std::thread::spawn(move || {
        tx.send(waiting.wait_for_change(epoch, Duration::from_secs(10)))
            .unwrap();
    });
    let deadline = Instant::now() + Duration::from_secs(5);
    while budget.snapshot().waiters == 0 {
        assert!(
            Instant::now() < deadline,
            "waiter must actually enter the Condvar wait"
        );
        std::thread::yield_now();
    }
    budget.stop();
    let stopped = rx.recv_timeout(Duration::from_secs(1)).unwrap();
    join.join().unwrap();
    assert!(stopped.stopped);
    assert_eq!(stopped.live_bytes, 4096);
    drop(owner);
    assert_eq!(budget.snapshot().live_bytes, 0);
}

#[test]
fn p2_w2_native_unbudgeted_ownerfs_and_existing_idle_pool_semantics_remain() {
    let pool = WriteBufferPool::new(4096);
    let first = ChunkBuilder::with_pool(pool.clone(), 4096).unwrap();
    let second = ChunkBuilder::with_pool(pool.clone(), 4096).unwrap();
    assert_eq!(pool.snapshot().live_bytes, 8192);
    pool.stop();
    drop(first);
    drop(second);
    let after_stop = ChunkBuilder::with_pool(pool.clone(), 4096).unwrap();
    drop(after_stop);
    assert_eq!(pool.snapshot().live_bytes, 0);
    assert_eq!(pool.snapshot().idle_bytes, 0);
}

#[test]
fn p2_w2_native_checkout_completes_actual_capacity_after_stop_or_releases_failure() {
    let pool = WriteBufferPool::new(0);
    let budget = NativeWriteBudget::new(4096);
    let admitted = lease(&budget, 3000);
    budget.stop();
    pool.state.lock().unwrap().next_allocation_capacity = Some(4096);
    let builder = ChunkBuilder::with_native_lease(pool.clone(), 3000, admitted).unwrap();
    assert_eq!(builder.capacity(), 4096);
    assert_eq!(budget.snapshot().live_bytes, 4096);
    drop(builder);
    assert_eq!(budget.snapshot().live_bytes, 0);
    assert_eq!(pool.snapshot().live_bytes, 0);

    let limited = NativeWriteBudget::new(3000);
    pool.state.lock().unwrap().next_allocation_capacity = Some(4096);
    let error =
        ChunkBuilder::with_native_lease(pool.clone(), 3000, lease(&limited, 3000)).unwrap_err();
    assert_eq!(error.code(), afs_error::IO_CAPACITY);
    assert_eq!(limited.snapshot().live_bytes, 0);
    assert_eq!(limited.snapshot().leases, 0);
    assert_eq!(pool.snapshot().live_bytes, 0);
}

#[test]
fn p2_w2_native_pool_destruction_does_not_refund_a_shared_allocation() {
    let pool = WriteBufferPool::new(4096);
    let budget = NativeWriteBudget::new(4096);
    assert_eq!(budget.snapshot().limit, 4096);
    let mut builder =
        ChunkBuilder::with_native_lease(pool.clone(), 4096, lease(&budget, 4096)).unwrap();
    builder.write_at(0, b"retained").unwrap();
    let body = builder.freeze();
    let slow = body.slice(..);
    drop(pool);
    drop(body);
    assert_eq!(budget.snapshot().live_bytes, 4096);
    assert_eq!(slow.as_ref(), b"retained");
    drop(slow);
    assert_eq!(budget.snapshot().live_bytes, 0);
}

#[test]
fn p2_w2_native_checkout_capacity_pressure_is_retryable_and_not_enomem() {
    let pool = WriteBufferPool::new(0);
    let budget = NativeWriteBudget::new(4096);
    let admitted = lease(&budget, 3000);
    let other = lease(&budget, 1096);
    pool.state.lock().unwrap().next_allocation_capacity = Some(4096);
    let error = ChunkBuilder::with_native_lease(pool.clone(), 3000, admitted).unwrap_err();
    assert_eq!(error.code(), afs_error::IO_WOULD_BLOCK);
    assert_eq!(budget.snapshot().live_bytes, 1096);
    assert_eq!(budget.snapshot().leases, 1);
    assert_eq!(pool.snapshot().live_bytes, 0);
    drop(other);
    assert_eq!(budget.snapshot().live_bytes, 0);
}

fn owner_lease(
    budget: &Arc<NativeWriteBudget>,
    owner: &Arc<NativeWriteOwner>,
    bytes: usize,
    owner_limit: usize,
) -> NativeWriteLease {
    match budget
        .try_reserve_for(owner.clone(), bytes, owner_limit)
        .unwrap()
    {
        NativeWriteAdmission::Reserved(lease) => lease,
        NativeWriteAdmission::Wait { .. } => panic!("owner test requires available capacity"),
    }
}

#[test]
fn p2_w2_native_inode_charge_follows_last_shared_slice_and_actual_capacity() {
    let budget = NativeWriteBudget::new(8192);
    let owner = NativeWriteOwner::new();
    let pool = WriteBufferPool::new(4096);
    pool.state.lock().unwrap().next_allocation_capacity = Some(4096);
    let mut builder = ChunkBuilder::with_native_lease(
        pool.clone(),
        3000,
        owner_lease(&budget, &owner, 3000, 4096),
    )
    .unwrap();
    builder.write_at(0, b"shared-owner").unwrap();
    assert_eq!(owner.snapshot().live_bytes, 4096);
    assert_eq!(owner.snapshot().live_peak_bytes, 4096);
    assert_eq!(budget.snapshot().live_bytes, 4096);
    let frozen = builder.freeze();
    let last = frozen.slice(..);
    drop(frozen);
    assert_eq!(owner.snapshot().live_bytes, 4096);
    assert!(matches!(
        budget.try_reserve_for(owner.clone(), 1, 4096).unwrap(),
        NativeWriteAdmission::Wait { .. }
    ));
    drop(last);
    assert_eq!(owner.snapshot().live_bytes, 0);
    assert_eq!(budget.snapshot().live_bytes, 0);
    assert_eq!(pool.snapshot().idle_bytes, 4096);
}

#[test]
fn p2_w2_native_inode_cap_and_node_cap_are_checked_without_cross_owner_refund() {
    let budget = NativeWriteBudget::new(8192);
    let first = NativeWriteOwner::new();
    let second = NativeWriteOwner::new();
    let mut a = owner_lease(&budget, &first, 3000, 4096);
    let b = owner_lease(&budget, &second, 4096, 4096);
    assert!(matches!(
        budget.try_reserve_for(first.clone(), 1097, 4096).unwrap(),
        NativeWriteAdmission::Wait { .. }
    ));
    assert!(a.try_grow_to(4096).unwrap());
    assert_eq!(first.snapshot().live_bytes, 4096);
    assert_eq!(second.snapshot().live_bytes, 4096);
    assert!(matches!(
        budget
            .try_reserve_for(NativeWriteOwner::new(), 1, 4096)
            .unwrap(),
        NativeWriteAdmission::Wait { .. }
    ));
    assert_eq!(budget.snapshot().live_bytes, 8192);
    drop(a);
    assert_eq!(first.snapshot().live_bytes, 0);
    assert_eq!(second.snapshot().live_bytes, 4096);
    assert_eq!(budget.snapshot().live_bytes, 4096);
    drop(b);
    assert_eq!(budget.snapshot().live_bytes, 0);
}

#[test]
fn p2_w2_native_owner_cap_grow_failure_preserves_both_ledgers() {
    let budget = NativeWriteBudget::new(8192);
    let owner = NativeWriteOwner::new();
    let first = owner_lease(&budget, &owner, 3000, 4096);
    let second = owner_lease(&budget, &owner, 1096, 4096);
    let pool = WriteBufferPool::new(0);
    pool.state.lock().unwrap().next_allocation_capacity = Some(4096);
    let error = ChunkBuilder::with_native_lease(pool, 3000, first).unwrap_err();
    assert_eq!(error.code(), afs_error::IO_WOULD_BLOCK);
    assert_eq!(budget.snapshot().live_bytes, 1096);
    assert_eq!(owner.snapshot().live_bytes, 1096);
    drop(second);
    assert_eq!(owner.snapshot().live_bytes, 0);
    assert_eq!(budget.snapshot().live_bytes, 0);
}

#[test]
fn p2_w2_native_inode_idle_reuse_refunds_only_spare_admission() {
    let pool = WriteBufferPool::new(4096);
    drop(ChunkBuilder::with_pool(pool.clone(), 3000).unwrap());
    let budget = NativeWriteBudget::new(4096);
    let owner = NativeWriteOwner::new();
    let builder = ChunkBuilder::with_native_lease(
        pool.clone(),
        3000,
        owner_lease(&budget, &owner, 4096, 4096),
    )
    .unwrap();
    assert_eq!(builder.capacity(), 3000);
    assert_eq!(budget.snapshot().live_bytes, 3000);
    assert_eq!(owner.snapshot().live_bytes, 3000);
    assert_eq!(pool.snapshot().reuses, 1);
    let body = builder.freeze();
    assert_eq!(budget.snapshot().live_bytes, 3000);
    drop(body);
    assert_eq!(budget.snapshot().live_bytes, 0);
    assert_eq!(owner.snapshot().live_bytes, 0);
}

#[test]
fn p2_w2_native_inode_accepted_capacity_completion_survives_stop() {
    let pool = WriteBufferPool::new(0);
    let budget = NativeWriteBudget::new(4096);
    let owner = NativeWriteOwner::new();
    let admitted = owner_lease(&budget, &owner, 3000, 4096);
    budget.stop();
    pool.state.lock().unwrap().next_allocation_capacity = Some(4096);
    let builder = ChunkBuilder::with_native_lease(pool.clone(), 3000, admitted).unwrap();
    assert_eq!(owner.snapshot().live_bytes, 4096);
    assert_eq!(budget.snapshot().live_bytes, 4096);
    let frozen = builder.freeze();
    let last = frozen.slice(..);
    drop(frozen);
    assert_eq!(owner.snapshot().live_bytes, 4096);
    drop(last);
    assert_eq!(owner.snapshot().live_bytes, 0);
    assert_eq!(budget.snapshot().live_bytes, 0);
}

#[test]
fn p2_w2_native_default_receipts_never_claim_required_local_release() {
    let temp = tempfile::tempdir().unwrap();
    let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
    let staged = StagedChunk::new(
        OperationId::new("native-default-receipt"),
        b"durable".to_vec(),
    );
    assert_eq!(
        store
            .native_descriptor_reservation_bytes(&staged.operation_id)
            .unwrap(),
        16 * 1024
    );
    let receipts = store.put_native_batch(vec![staged.clone()]).unwrap();
    assert_eq!(receipts.len(), 1);
    assert_eq!(receipts[0].receipt.chunk, staged.chunk);
    assert_eq!(receipts[0].receipt.operation_id, staged.operation_id);
    assert!(!receipts[0].local_required_and_durable);
    let mut output = [9; 3];
    assert_eq!(
        store
            .read_native_uploaded_at(&receipts[0].receipt, 0, &mut output)
            .unwrap_err()
            .code(),
        afs_error::NODE_VFS_UNIMPLEMENTED
    );
    assert_eq!(output, [9; 3]);
}

#[test]
fn p2_w2_native_custom_descriptor_allowance_rejects_oversized_actual_operation_capacity() {
    let temp = tempfile::tempdir().unwrap();
    let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
    let mut large = String::with_capacity(8192);
    large.push('x');
    let operation = OperationId::new(large);
    assert_eq!(operation.0.len(), 1);
    assert_eq!(
        store
            .native_descriptor_reservation_bytes(&operation)
            .unwrap_err()
            .code(),
        afs_error::IO_CAPACITY
    );
    assert!(store.catalog.lock().unwrap().records.is_empty());
}

#[cfg(feature = "dfs")]
#[test]
fn p2_w2_native_staged_metadata_counts_actual_capacity_without_body_or_proof() {
    let mut operation = String::with_capacity(4096);
    operation.push_str("metadata");
    let staged = StagedChunk::new(OperationId::new(operation), vec![37; 65536]);
    let expected = staged.operation_id.0.capacity()
        + staged.chunk.id.0.capacity()
        + staged.sealed_chunk.id.0.capacity();
    assert_eq!(staged.native_retained_metadata_bytes(), expected);
    assert!(staged.native_retained_metadata_bytes() < staged.bytes().len());
    let small = StagedChunk::new(staged.operation_id.clone(), vec![37; 1]);
    assert_eq!(
        small.native_retained_metadata_bytes(),
        small.operation_id.0.capacity()
            + small.chunk.id.0.capacity()
            + small.sealed_chunk.id.0.capacity()
    );
}

#[test]
fn p2_w2_native_receipt_reader_checks_exact_object_without_eager_scan() {
    let temp = tempfile::tempdir().unwrap();
    let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
    let staged = StagedChunk::new(OperationId::new("native-receipt-pin"), vec![57; 65537]);
    store.put(staged.clone()).unwrap();
    VERIFIED_SCAN_BYTES.with(|count| count.set(0));
    let reader = store.open_receipt_reader(&staged.chunk).unwrap();
    assert_eq!(VERIFIED_SCAN_BYTES.with(|count| count.get()), 0);
    let mut wrong = staged.chunk.clone();
    wrong.length -= 1;
    assert_eq!(
        store.open_receipt_reader(&wrong).unwrap_err().code(),
        afs_error::NODE_TRANSFER_CORRUPT_DATA
    );
    assert!(store.quarantined_chunks().unwrap().is_empty());
    let mut output = [9; 17];
    assert_eq!(reader.read_at(3, &mut output).unwrap(), 17);
    assert_eq!(output, [57; 17]);
    let file = OpenOptions::new()
        .write(true)
        .open(temp.path().join("chunks").join(&staged.chunk.id.0))
        .unwrap();
    file.write_all_at(&[91], 7).unwrap();
    output.fill(9);
    assert_eq!(
        reader.read_at(3, &mut output).unwrap_err().code(),
        afs_error::NODE_TRANSFER_CORRUPT_DATA
    );
    assert_eq!(
        output, [9; 17],
        "private pinned verification must not expose damaged bytes"
    );
}
