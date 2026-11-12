use super::*;

#[test]
fn writable_capacity_keeps_requested_limit_separate_from_reused_allocation() {
    let pool = WriteBufferPool::new(8192);
    drop(ChunkBuilder::with_pool(pool.clone(), 4096).unwrap());
    let mut builder = ChunkBuilder::with_pool(pool.clone(), 3000).unwrap();
    assert_eq!(builder.capacity(), 4096);
    assert_eq!(builder.writable_capacity(), 3000);
    builder.write_at(2999, &[7]).unwrap();
    assert_eq!(
        builder.write_at(3000, &[8]).unwrap_err().code(),
        afs_error::IO_OUT_OF_MEMORY
    );
    assert_eq!(pool.snapshot().live_bytes, 4096);
}

#[test]
fn hot_large_allocation_cannot_inflate_a_small_patch() {
    let pool = WriteBufferPool::new(8 * 1024 * 1024);
    let large = ChunkBuilder::with_pool(pool.clone(), 4 * 1024 * 1024).unwrap();
    let large_pointer = large.data_ptr();
    drop(large);
    let mut patch = ChunkBuilder::with_pool(pool.clone(), 4096).unwrap();
    patch.write_at(0, &[3; 4096]).unwrap();
    assert_eq!(patch.capacity(), 4096);
    assert_ne!(patch.data_ptr(), large_pointer);
    assert_eq!(pool.snapshot().live_bytes, 4096);
    assert_eq!(pool.snapshot().idle_bytes, 4 * 1024 * 1024);
    assert_eq!(pool.snapshot().allocations, 2);
    assert_eq!(pool.snapshot().reuses, 0);
}

#[test]
fn six_mebibyte_body_uses_two_final_allocations_without_freeze_copy() {
    const MIB: usize = 1024 * 1024;
    let pool = WriteBufferPool::new(8 * MIB);
    let mut first = ChunkBuilder::with_pool(pool.clone(), 4 * MIB).unwrap();
    let mut tail = ChunkBuilder::with_pool(pool.clone(), 4 * MIB).unwrap();
    let input = vec![37; MIB];
    for index in 0..6 {
        let (builder, offset) = if index < 4 {
            (&mut first, index * MIB)
        } else {
            (&mut tail, (index - 4) * MIB)
        };
        builder.write_at(offset as u64, &input).unwrap();
    }
    let first_pointer = first.data_ptr();
    let tail_pointer = tail.data_ptr();
    let first_generation = first.allocation_id();
    let tail_generation = tail.allocation_id();
    let first = first.stage(OperationId::new("four-mib"));
    let tail = tail.stage(OperationId::new("two-mib"));
    assert_eq!(first.bytes().len(), 4 * MIB);
    assert_eq!(tail.bytes().len(), 2 * MIB);
    assert_eq!(first.bytes().as_ptr(), first_pointer);
    assert_eq!(tail.bytes().as_ptr(), tail_pointer);
    assert_eq!(first.bytes.allocation_id(), first_generation);
    assert_eq!(tail.bytes.allocation_id(), tail_generation);
    assert!(
        first
            .bytes()
            .iter()
            .chain(tail.bytes())
            .all(|byte| *byte == 37)
    );
    let stats = pool.snapshot();
    assert_eq!(stats.live_bytes, 8 * MIB);
    assert_eq!(stats.copied_bytes, 6 * MIB as u64);
    assert_eq!(stats.copy_operations, 6);
    assert_eq!(stats.allocations, 2);
    drop(first);
    drop(tail);
    assert_eq!(pool.snapshot().live_bytes, 0);
    assert_eq!(pool.snapshot().idle_bytes, 8 * MIB);
}

#[test]
fn actual_copy_entries_count_overlap_extension_and_zeroed_holes_separately() {
    let pool = WriteBufferPool::new(32);
    let mut builder = ChunkBuilder::with_pool(pool.clone(), 16).unwrap();
    builder.write_at(0, b"abc").unwrap();
    builder.write_at(2, b"def").unwrap();
    builder.write_at(8, b"g").unwrap();
    assert_eq!(builder.as_bytes(), b"abdef\0\0\0g");
    assert_eq!(pool.snapshot().copied_bytes, 7);
    // The extension crossing the initialized tail copies one existing and
    // one new range; sparse zero initialization is not input copying.
    assert_eq!(pool.snapshot().copy_operations, 4);
}

#[test]
fn exact_metrics_track_last_owner_reuse_and_stop() {
    let registry = afs_metrics::Registry::new();
    let pool = WriteBufferPool::new(4096);
    pool.register_metrics(&registry).unwrap();
    let mut builder = ChunkBuilder::with_pool(pool.clone(), 4096).unwrap();
    builder.write_at(0, b"data").unwrap();
    let frozen = builder.freeze();
    let delayed = frozen.slice(..);
    drop(frozen);
    let before = afs_metrics::encode_text(&registry).unwrap();
    assert!(
        before.contains("afs_dfs_write_buffer_live_bytes 4096\n"),
        "{before}"
    );
    assert!(
        before.contains("afs_dfs_write_buffer_idle_bytes 0\n"),
        "{before}"
    );
    drop(delayed);
    let reused = ChunkBuilder::with_pool(pool.clone(), 4096).unwrap();
    drop(reused);
    pool.stop();
    let after = afs_metrics::encode_text(&registry).unwrap();
    for expected in [
        "afs_dfs_write_buffer_live_bytes 0\n",
        "afs_dfs_write_buffer_idle_bytes 0\n",
        "afs_dfs_write_buffer_live_peak_bytes 4096\n",
        "afs_dfs_write_buffer_allocations_total 1\n",
        "afs_dfs_write_buffer_reuses_total 1\n",
        "afs_dfs_write_buffer_released_bytes_total 4096\n",
        "afs_dfs_write_buffer_input_copy_bytes_total 4\n",
        "afs_dfs_write_buffer_input_copies_total 1\n",
    ] {
        assert!(after.contains(expected), "missing {expected} in {after}");
    }
}

#[test]
fn freeze_and_shared_ranges_keep_allocation_until_last_owner() {
    let pool = WriteBufferPool::new(8192);
    let mut builder = ChunkBuilder::with_pool(pool.clone(), 4096).unwrap();
    builder.write_at(0, b"abcdef").unwrap();
    let pointer = builder.data_ptr();
    let generation = builder.allocation_id();
    let capacity = builder.capacity();
    let frozen = builder.freeze();
    assert_eq!(frozen.as_ptr(), pointer);
    assert_eq!(frozen.allocation_id(), generation);
    assert_eq!(frozen.capacity(), capacity);
    let range = frozen.slice(1..4);
    let staged = StagedChunk::from_shared(OperationId::new("shared-freeze"), range.clone());
    assert_eq!(staged.bytes(), b"bcd");
    assert_eq!(staged.bytes().as_ptr(), pointer.wrapping_add(1));
    assert_eq!(staged.chunk.length, 3);
    assert_eq!(pool.snapshot().copied_bytes, 6);
    drop(frozen);
    drop(range);
    assert_eq!(pool.snapshot().live_buffers, 1);
    assert_eq!(pool.snapshot().idle_bytes, 0);
    drop(staged);
    assert_eq!(pool.snapshot().live_bytes, 0);
    assert_eq!(pool.snapshot().idle_bytes, capacity);
}

#[test]
fn reuse_has_new_generation_and_never_exposes_old_data() {
    let pool = WriteBufferPool::new(8192);
    let mut first = ChunkBuilder::with_pool(pool.clone(), 4096).unwrap();
    first.write_at(0, &[0xa5; 4096]).unwrap();
    let pointer = first.data_ptr();
    let generation = first.allocation_id();
    drop(first);
    let mut next = ChunkBuilder::with_pool(pool.clone(), 4096).unwrap();
    assert_eq!(next.data_ptr(), pointer);
    assert_ne!(next.allocation_id(), generation);
    assert_eq!(next.len(), 0);
    next.write_at(8, &[7]).unwrap();
    assert_eq!(next.as_bytes(), &[0, 0, 0, 0, 0, 0, 0, 0, 7]);
    next.truncate(3);
    next.write_at(8, &[9]).unwrap();
    assert_eq!(next.as_bytes(), &[0, 0, 0, 0, 0, 0, 0, 0, 9]);
    assert_eq!(pool.snapshot().allocations, 1);
    assert_eq!(pool.snapshot().reuses, 1);
}

#[test]
fn idle_baseline_does_not_limit_live_and_stop_retires_late_owners() {
    let pool = WriteBufferPool::new(4096);
    let mut builders = Vec::new();
    for _ in 0..3 {
        let mut builder = ChunkBuilder::with_pool(pool.clone(), 4096).unwrap();
        builder.write_at(0, &[1; 4096]).unwrap();
        builders.push(builder);
    }
    assert_eq!(pool.snapshot().live_bytes, 3 * 4096);
    let retained = builders.pop().unwrap().freeze();
    drop(builders);
    assert_eq!(pool.snapshot().live_bytes, 4096);
    assert_eq!(pool.snapshot().idle_bytes, 4096);
    assert_eq!(pool.snapshot().released_bytes, 4096);
    pool.stop();
    assert_eq!(pool.snapshot().idle_bytes, 0);
    assert_eq!(pool.snapshot().live_bytes, 4096);
    drop(retained);
    assert_eq!(pool.snapshot().live_bytes, 0);
    assert_eq!(pool.snapshot().released_bytes, 3 * 4096);
    let after_stop = ChunkBuilder::with_pool(pool.clone(), 4096).unwrap();
    drop(after_stop);
    assert_eq!(pool.snapshot().idle_bytes, 0);
}

#[test]
fn fixed_capacity_failure_keeps_previously_accepted_bytes() {
    let pool = WriteBufferPool::new(0);
    let mut builder = ChunkBuilder::with_pool(pool.clone(), 4).unwrap();
    builder.write_at(0, b"abcd").unwrap();
    let pointer = builder.data_ptr();
    let error = builder.write_at(3, b"ef").unwrap_err();
    assert_eq!(error.code(), afs_error::IO_OUT_OF_MEMORY);
    assert_eq!(builder.as_bytes(), b"abcd");
    assert_eq!(builder.data_ptr(), pointer);
    assert_eq!(pool.snapshot().copied_bytes, 4);
    assert_eq!(pool.snapshot().copy_operations, 1);
    pool.fail_next_allocation();
    assert_eq!(
        ChunkBuilder::with_pool(pool.clone(), 8).unwrap_err().code(),
        afs_error::IO_OUT_OF_MEMORY
    );
    assert_eq!(pool.snapshot().live_bytes, 4);
}

#[test]
fn pooled_builder_cannot_keep_pool_alive_or_modify_a_frozen_chunk() {
    let pool = WriteBufferPool::new(4096);
    let weak = Arc::downgrade(&pool);
    let mut builder = ChunkBuilder::with_pool(pool.clone(), 4096).unwrap();
    builder.write_at(0, b"original").unwrap();
    let frozen = builder.freeze();
    drop(pool);
    assert!(weak.upgrade().is_none());
    assert_eq!(&*frozen, b"original");
    drop(frozen);
}

#[test]
fn staged_vec_and_default_builder_keep_legacy_content_without_vec_to_arc_copy() {
    let bytes = b"owned".to_vec();
    let pointer = bytes.as_ptr();
    let staged = StagedChunk::new(OperationId::new("vec-owner"), bytes);
    assert_eq!(staged.bytes().as_ptr(), pointer);
    let mut builder = ChunkBuilder::default();
    builder.write_at(3, b"data").unwrap();
    assert_eq!(builder.as_bytes(), b"\0\0\0data");
    builder.replace(b"replacement".to_vec());
    assert_eq!(
        builder.stage(OperationId::new("legacy")).bytes(),
        b"replacement"
    );
}

#[test]
fn mutable_overwrite_is_in_place_and_frozen_identity_is_immutable() {
    let pool = WriteBufferPool::new(8192);
    let mut builder = ChunkBuilder::with_pool(pool.clone(), 4096).unwrap();
    builder.write_at(0, b"abcdef").unwrap();
    let pointer = builder.data_ptr();
    builder.write_at(2, b"XY").unwrap();
    assert_eq!(builder.data_ptr(), pointer);
    let staged = builder.stage(OperationId::new("first"));
    assert_eq!(staged.bytes(), b"abXYef");
    let mut patch = ChunkBuilder::with_pool(pool.clone(), 2).unwrap();
    patch.write_at(0, b"zz").unwrap();
    assert_eq!(patch.stage(OperationId::new("patch")).bytes(), b"zz");
    assert_eq!(staged.bytes(), b"abXYef");
    assert_eq!(pool.snapshot().copied_bytes, 10);
}
