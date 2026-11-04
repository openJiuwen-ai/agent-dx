use super::*;

#[test]
fn killpriv_positive_write_clears_suid_and_executable_sgid_until_commit() {
    let (_temp, meta, fs) = test_fs();
    meta.inode.lock().unwrap().attributes.mode = 0o6777;
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("killpriv-write.bin"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();

    assert_eq!(
        fs.write_with_options(
            &context(),
            created.handle,
            0,
            b"x",
            WriteOptions { kill_suidgid: true },
        )
        .unwrap(),
        1
    );
    assert_eq!(
        fs.getattr(&context(), created.entry.inode, Some(created.handle))
            .unwrap()
            .mode,
        0o0777
    );

    let killpriv_ctime = system_time_ms(
        fs.getattr(&context(), created.entry.inode, Some(created.handle))
            .unwrap()
            .ctime,
    );
    fs.flush(&context(), created.handle).unwrap();
    let commits = meta.commits.lock().unwrap();
    assert_eq!(commits.len(), 1);
    assert!(commits[0].metadata_delta.kill_suidgid);
    assert_eq!(
        commits[0].metadata_delta.ctime_unix_ms,
        Some(killpriv_ctime)
    );
    assert_eq!(meta.inode.lock().unwrap().attributes.mode, 0o0777);
}

#[test]
fn killpriv_zero_write_does_not_clear_mode() {
    let (_temp, meta, fs) = test_fs();
    meta.inode.lock().unwrap().attributes.mode = 0o6777;
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("killpriv-zero.bin"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();

    assert_eq!(
        fs.write_with_options(
            &context(),
            created.handle,
            0,
            b"",
            WriteOptions { kill_suidgid: true },
        )
        .unwrap(),
        0
    );
    assert_eq!(
        fs.getattr(&context(), created.entry.inode, Some(created.handle))
            .unwrap()
            .mode,
        0o6777
    );
    assert_eq!(meta.commit_count(), 0);
}

#[test]
fn killpriv_pending_clear_commits_before_plain_chmod() {
    let (_temp, meta, fs) = test_fs();
    meta.inode.lock().unwrap().attributes.mode = 0o6777;
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("killpriv-chmod.bin"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();

    fs.write_with_options(
        &context(),
        created.handle,
        0,
        b"x",
        WriteOptions { kill_suidgid: true },
    )
    .unwrap();
    let attrs = fs
        .setattr_with_options(
            &context(),
            created.entry.inode,
            Some(created.handle),
            &AttributeChange {
                mode: Some(0o6755),
                ..AttributeChange::default()
            },
            SetAttrOptions::default(),
        )
        .unwrap();
    assert_eq!(attrs.mode, 0o6755);
    fs.fsync(&context(), created.handle, SyncMode::Full)
        .unwrap();

    let commits = meta.commits.lock().unwrap();
    assert_eq!(commits.len(), 1);
    assert!(commits[0].metadata_delta.kill_suidgid);
    assert_eq!(meta.inode.lock().unwrap().attributes.mode, 0o6755);
}

#[test]
fn killpriv_resize_clears_mode_on_explicit_cause() {
    let (_temp, meta, fs) = test_fs();
    meta.inode.lock().unwrap().attributes.mode = 0o6777;
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("killpriv-resize.bin"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();

    let attrs = fs
        .setattr_with_options(
            &context(),
            created.entry.inode,
            Some(created.handle),
            &AttributeChange {
                size: Some(4),
                ..AttributeChange::default()
            },
            SetAttrOptions {
                kill_suidgid: true,
                timestamps_now: false,
            },
        )
        .unwrap();
    assert_eq!(attrs.mode, 0o0777);
    fs.flush(&context(), created.handle).unwrap();
    assert!(meta.commits.lock().unwrap()[0].metadata_delta.kill_suidgid);
}

#[test]
fn legacy_kernel_mode_clear_rechecks_current_dfs_state() {
    let (_temp, meta, fs) = test_fs();
    meta.inode.lock().unwrap().attributes.mode = 0o6777;
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("legacy-clear.bin"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    let options = SetAttrOptions {
        kill_suidgid: true,
        timestamps_now: false,
    };
    let invalid = AttributeChange {
        mode: Some(0o666),
        ..Default::default()
    };
    assert!(
        fs.setattr_with_options(
            &context(),
            created.entry.inode,
            Some(created.handle),
            &invalid,
            options
        )
        .is_err()
    );
    assert_eq!(
        fs.getattr(&context(), created.entry.inode, Some(created.handle))
            .unwrap()
            .mode,
        0o6777
    );
    let clear = AttributeChange {
        mode: Some(0o777),
        ..Default::default()
    };
    let attrs = fs
        .setattr_with_options(
            &context(),
            created.entry.inode,
            Some(created.handle),
            &clear,
            options,
        )
        .unwrap();
    assert_eq!(attrs.mode, 0o777);
    assert!(
        fs.setattr_with_options(
            &context(),
            created.entry.inode,
            Some(created.handle),
            &clear,
            options
        )
        .is_err()
    );
    fs.flush(&context(), created.handle).unwrap();
    assert!(
        meta.metadata_syncs
            .lock()
            .unwrap()
            .iter()
            .any(|sync| sync.metadata_delta.kill_suidgid)
    );
}

#[test]
fn killpriv_setattr_on_special_inode_uses_normal_meta_attribute_path() {
    let (_temp, meta, fs) = test_fs();
    let entry = fs
        .mknod(
            &context(),
            fs.root_inode(),
            OsStr::new("killpriv-fifo"),
            SpecialFileKind::Fifo,
            0o666,
        )
        .unwrap();

    let attrs = fs
        .setattr_with_options(
            &context(),
            entry.inode,
            None,
            &AttributeChange {
                mode: Some(0o600),
                uid: Some(4242),
                gid: Some(4343),
                ..AttributeChange::default()
            },
            SetAttrOptions {
                kill_suidgid: true,
                timestamps_now: false,
            },
        )
        .unwrap();

    assert!(matches!(
        attrs.kind,
        FileKind::Special(SpecialFileKind::Fifo)
    ));
    assert_eq!(attrs.mode & 0o777, 0o600);
    assert_eq!(attrs.uid, 4242);
    assert_eq!(attrs.gid, 4343);
    assert_eq!(
        meta.commit_count(),
        0,
        "non-regular setattr killpriv cause must not enter DFS chunk/write-lease commit path"
    );
}

#[test]
fn native_writeback_buffers_streaming_writes_until_an_explicit_barrier() {
    let (_temp, meta, fs) = test_fs();
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("budget-stream.bin"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();

    assert_eq!(
        fs.write(&context(), created.handle, 0, b"abcdefghijklmnop")
            .unwrap(),
        16
    );
    assert_eq!(
        meta.commit_count(),
        0,
        "ordinary write no longer publishes legacy dirty-budget windows"
    );
    fs.fsync(&context(), created.handle, SyncMode::DataOnly)
        .unwrap();
    let commits = meta.commits.lock().unwrap();
    assert_eq!(commits.len(), 1);
    assert_eq!(commits[0].metadata_delta.mode, CommitMetadataMode::DataOnly);
    assert_eq!(commits[0].file_version.length, 16);
    drop(commits);

    let mut out = [0; 16];
    assert_eq!(
        fs.read(&context(), created.handle, 0, &mut out).unwrap(),
        16
    );
    assert_eq!(&out, b"abcdefghijklmnop");
}

#[test]
/// Content addressing (one receipt per identical chunk) is S3's BLAKE3
/// object model; native CRC32C objects carry per-generation ids (F6).
fn s3_repeated_identical_chunks_share_receipt_but_keep_layout_references() {
    let (_temp, meta, fs, _store) = test_fs_on_fake_s3();
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("budget-repeated-chunks.bin"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    let payload = vec![b'w'; 8192];
    let mut offset = 0u64;

    for size in (1..=8192).rev() {
        let size = size.min((DEFAULT_DIRTY_DATA_BUDGET_BYTES - offset) as usize);
        if size == 0 {
            break;
        }
        assert_eq!(
            fs.write(&context(), created.handle, offset, &payload[..size])
                .unwrap(),
            size
        );
        offset += size as u64;
    }
    assert_eq!(offset, DEFAULT_DIRTY_DATA_BUDGET_BYTES);
    assert_eq!(meta.commit_count(), 0);
    fs.fsync(&context(), created.handle, SyncMode::Full)
        .unwrap();

    let commits = meta.commits.lock().unwrap();
    assert_eq!(commits.len(), 1);
    let commit = &commits[0];
    assert_eq!(commit.file_version.length, DEFAULT_DIRTY_DATA_BUDGET_BYTES);
    assert_eq!(
        commit.layout_root.file_length,
        DEFAULT_DIRTY_DATA_BUDGET_BYTES
    );
    assert_eq!(
        commit.layout_root.inline_extents.len(),
        (DEFAULT_DIRTY_DATA_BUDGET_BYTES / COMMIT_CHUNK_BYTES) as usize
    );
    assert_eq!(
        commit.chunk_receipts.len(),
        1,
        "identical content-addressed chunks need one proof receipt per chunk id"
    );
    let repeated_chunk = commit.layout_root.inline_extents[0].chunk_id.clone();
    assert_eq!(commit.chunk_receipts[0].chunk.id, repeated_chunk);
    for (index, extent) in commit.layout_root.inline_extents.iter().enumerate() {
        assert_eq!(extent.file_offset, index as u64 * COMMIT_CHUNK_BYTES);
        assert_eq!(extent.length, COMMIT_CHUNK_BYTES);
        assert_eq!(extent.chunk_offset, 0);
        assert_eq!(extent.chunk_id, repeated_chunk);
    }
    drop(commits);

    let mut checked = 0u64;
    let mut out = vec![0; 64 * 1024];
    while checked < DEFAULT_DIRTY_DATA_BUDGET_BYTES {
        let read = fs
            .read(&context(), created.handle, checked, &mut out)
            .unwrap();
        assert_eq!(read, out.len());
        assert!(out[..read].iter().all(|byte| *byte == b'w'));
        checked += read as u64;
    }
}

#[test]
fn dirty_budget_counts_payload_not_sparse_holes() {
    let (_temp, meta, fs) = test_fs();
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("budget-sparse.bin"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    let offset = 8 * 1024 * 1024 * 1024u64;

    assert_eq!(
        fs.write(&context(), created.handle, offset, b"tail")
            .unwrap(),
        4
    );
    assert_eq!(
        meta.commit_count(),
        0,
        "large logical hole must not count as dirty payload memory"
    );
    fs.flush(&context(), created.handle).unwrap();
    let commits = meta.commits.lock().unwrap();
    assert_eq!(commits.len(), 1);
    assert_eq!(commits[0].file_version.length, offset + 4);
    assert_eq!(commits[0].layout_root.inline_extents.len(), 1);
    assert_eq!(commits[0].layout_root.inline_extents[0].file_offset, offset);
    assert_eq!(commits[0].layout_root.inline_extents[0].length, 4);
    assert_eq!(commits[0].chunk_receipts.len(), 1);
}

#[test]
fn explicit_publication_unknown_commit_ack_preserves_exact_request_and_backpressures_next_write() {
    // The inline-body tail path is S3's; native CRC tails are covered by
    // p2_w2_unknown_request_keeps_original_identity_and_body_until_exact_retry.
    let (_temp, meta, fs, _store) = test_fs_on_fake_s3();
    let pool = WriteBufferPool::new(0);
    let fs = fs.with_write_buffer_pool(pool.clone());
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("budget-retry.bin"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();

    meta.fail_next_commit();
    assert_eq!(
        fs.write(&context(), created.handle, 0, b"data").unwrap(),
        4,
        "buffered write reports bytes admitted into the exact pending request"
    );
    assert!(
        fs.fsync(&context(), created.handle, SyncMode::Full)
            .is_err()
    );
    let failed = meta.failed_commits.lock().unwrap()[0].clone();
    let inode_id = fs.inode_id(created.entry.inode).unwrap();
    let cell = fs.write_state(&inode_id).unwrap().unwrap();
    let retained_body = {
        let state = cell.lock().unwrap();
        let InFlightCommit::File(pending) = state.in_flight.as_ref().unwrap() else {
            panic!("unknown Meta outcome must retain the exact prepared commit");
        };
        assert_eq!(
            serde_json::to_vec(pending.batch.commit.as_ref()).unwrap(),
            serde_json::to_vec(&failed).unwrap()
        );
        pending
            .frozen
            .dirty_extents
            .iter_extents()
            .next()
            .unwrap()
            .data
            .as_ref()
            .unwrap()
            .clone()
    };
    let pointer = retained_body.as_ptr();
    let generation = retained_body.allocation_id();
    let capacity = retained_body.capacity();
    let last_holder = retained_body.clone();
    assert_eq!(retained_body.as_ref(), b"data");
    assert_eq!(pool.snapshot().live_bytes, capacity);
    assert_eq!(pool.snapshot().idle_bytes, 0);
    assert_eq!(pool.snapshot().released_bytes, 0);

    meta.fail_next_commit();
    assert_eq!(
        fs.write(&context(), created.handle, 4, b"!")
            .unwrap_err()
            .code(),
        afs_error::NODE_VFS_UNAVAILABLE,
        "the unknown commit is reported before admitting new mutation"
    );
    {
        let state = cell.lock().unwrap();
        let InFlightCommit::File(pending) = state.in_flight.as_ref().unwrap() else {
            panic!("unobserved retry must not retire pending identity");
        };
        let body = pending
            .frozen
            .dirty_extents
            .iter_extents()
            .next()
            .unwrap()
            .data
            .as_ref()
            .unwrap();
        assert_eq!(body.as_ptr(), pointer);
        assert_eq!(body.allocation_id(), generation);
        assert_eq!(
            serde_json::to_vec(pending.batch.commit.as_ref()).unwrap(),
            serde_json::to_vec(&failed).unwrap()
        );
    }
    assert_eq!(pool.snapshot().live_bytes, capacity);
    assert_eq!(fs.write(&context(), created.handle, 4, b"!").unwrap(), 1);
    let commits = meta.commits.lock().unwrap();
    assert_eq!(commits.len(), 1);
    assert_eq!(
        serde_json::to_vec(&failed).unwrap(),
        serde_json::to_vec(&commits[0]).unwrap()
    );
    assert_eq!(failed.operation_id, commits[0].operation_id);
    assert_eq!(failed.file_version.id, commits[0].file_version.id);
    assert_eq!(failed.layout_root.id, commits[0].layout_root.id);
    assert_eq!(commits[0].file_version.length, 4);
    drop(commits);

    fs.flush(&context(), created.handle).unwrap();
    let commits = meta.commits.lock().unwrap();
    assert_eq!(commits.len(), 2);
    assert_eq!(commits[1].file_version.length, 5);
    drop(commits);
    assert!(cell.lock().unwrap().in_flight.is_none());
    assert_eq!(
        pool.snapshot().live_bytes,
        capacity,
        "an ACK and inode retirement cannot release an independent body holder"
    );
    assert_eq!(pool.snapshot().idle_bytes, 0);
    assert_eq!(retained_body.as_ptr(), pointer);
    assert_eq!(retained_body.allocation_id(), generation);
    assert_eq!(retained_body.as_ref(), b"data");
    drop(retained_body);
    assert_eq!(pool.snapshot().live_bytes, capacity);
    drop(last_holder);
    assert_eq!(pool.snapshot().live_bytes, 0);
    assert_eq!(pool.snapshot().idle_bytes, 0);
    assert_eq!(pool.snapshot().copied_bytes, 5);
}

#[test]
fn native_builder_slow_put_and_transport_retry_keep_pool_live_until_last_owner() {
    struct SlowStore {
        inner: Arc<LocalChunkStore>,
        pause: Arc<CommitPause>,
        held: Mutex<Option<StagedChunk>>,
    }
    impl ChunkStore for SlowStore {
        fn put_chunks(&self, staged: Vec<StagedChunk>, _: PutIntent) -> Result<Vec<StoredChunk>> {
            *self.held.lock().unwrap() = staged.first().cloned();
            self.pause.block();
            self.inner.put_chunks(staged, PutIntent::Commit)
        }
        fn read_at(
            &self,
            chunk_id: &crate::dfs::ChunkId,
            offset: u64,
            out: &mut [u8],
        ) -> Result<usize> {
            self.inner.read_at(chunk_id, offset, out)
        }
    }
    let meta = Arc::new(RecordingMeta::new());
    let temp = tempfile::tempdir().unwrap();
    let local = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
    let pause = Arc::new(CommitPause::new(0));
    let store = Arc::new(SlowStore {
        inner: local.clone(),
        pause: pause.clone(),
        held: Mutex::new(None),
    });
    let pool = WriteBufferPool::new(0);
    let fs = Arc::new(
        distributed_fs_with_chunk_store(meta.clone(), local, store.clone())
            .with_write_buffer_pool(pool.clone()),
    );
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("native-slow-put.bin"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    let payload = vec![23; 64 * 1024];
    fs.write(&context(), created.handle, 0, &payload).unwrap();
    let inode_id = fs.inode_id(created.entry.inode).unwrap();
    let cell = fs.write_state(&inode_id).unwrap().unwrap();
    let (pointer, generation, capacity) = {
        let state = cell.lock().unwrap();
        let builder = &state.dirty_extents.builders[0].builder;
        (
            builder.data_ptr(),
            builder.allocation_id(),
            builder.capacity(),
        )
    };
    let writer_fs = fs.clone();
    let writer =
        std::thread::spawn(move || writer_fs.fsync(&context(), created.handle, SyncMode::Full));
    pause.wait_until_reached();
    let during_put = pool.snapshot();
    let commit_count_during_put = meta.commit_count();
    let retained = store.held.lock().unwrap().take().unwrap();
    let body = retained.shared_bytes();
    // Always unblock the test store before assertions, including a failed
    // invariant, so the runtime thread cannot be stranded by this test.
    pause.release();
    writer.join().unwrap().unwrap();
    assert_eq!(during_put.live_bytes, capacity);
    assert_eq!(during_put.idle_bytes, 0);
    assert_eq!(during_put.released_bytes, 0);
    assert_eq!(commit_count_during_put, 0);
    assert_eq!(body.as_ptr(), pointer);
    assert_eq!(body.allocation_id(), generation);
    assert_eq!(body.as_ref(), payload.as_slice());
    assert_eq!(pool.snapshot().live_bytes, capacity);
    assert_eq!(pool.snapshot().idle_bytes, 0);
    let transport = axum::body::Bytes::from_owner(retained);
    let retry = transport.clone();
    drop(body);
    drop(transport);
    assert_eq!(
        pool.snapshot().live_bytes,
        capacity,
        "a slow/retry transport clone retains the same allocation after Meta ACK"
    );
    assert_eq!(retry.as_ref(), payload.as_slice());
    drop(retry);
    assert_eq!(pool.snapshot().live_bytes, 0);
    assert_eq!(pool.snapshot().idle_bytes, 0);
    assert_eq!(pool.snapshot().released_bytes, capacity as u64);
    assert_eq!(pool.snapshot().copied_bytes, payload.len() as u64);
}

#[test]
fn readonly_fsync_drains_writer_but_flush_does_not() {
    let (_temp, meta, fs) = test_fs();
    let entry = fs
        .lookup(&context(), fs.root_inode(), OsStr::new("readonly-sync.bin"))
        .unwrap();
    let only_reader = fs.open(&context(), entry.inode, libc::O_RDONLY).unwrap();
    fs.fsync(&context(), only_reader, SyncMode::Full).unwrap();
    assert_eq!(
        meta.commit_count(),
        0,
        "read-only inode without local writes has nothing to publish"
    );
    fs.release(&context(), only_reader).unwrap();
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("readonly-sync.bin"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    fs.write(&context(), created.handle, 0, b"first").unwrap();
    let reader = fs
        .open(&context(), created.entry.inode, libc::O_RDONLY)
        .unwrap();
    fs.fsync(&context(), reader, SyncMode::DataOnly).unwrap();
    assert_eq!(
        meta.commits
            .lock()
            .unwrap()
            .last()
            .unwrap()
            .file_version
            .length,
        5
    );
    fs.write(&context(), created.handle, 5, b"second").unwrap();
    let before = meta.commit_count();
    fs.flush(&context(), reader).unwrap();
    fs.release(&context(), reader).unwrap();
    assert_eq!(
        meta.commit_count(),
        before,
        "readonly close must not publish another writer's pending data"
    );
    fs.fsync(&context(), created.handle, SyncMode::Full)
        .unwrap();
    assert_eq!(
        meta.commits
            .lock()
            .unwrap()
            .last()
            .unwrap()
            .file_version
            .length,
        11
    );
}

#[test]
fn publication_failure_preserves_admitted_bytes_and_blocks_later_mutation() {
    let (_temp, meta, fs) = test_fs();
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("budget-partial.bin"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();

    meta.fail_next_commit();
    assert_eq!(
        fs.write(&context(), created.handle, 0, b"abcdefgh")
            .unwrap(),
        8,
        "buffered write reports every byte admitted into dirty/pending state"
    );
    assert!(
        fs.fsync(&context(), created.handle, SyncMode::Full)
            .is_err()
    );
    assert_eq!(meta.commit_count(), 0);
    assert_eq!(meta.failed_commits.lock().unwrap().len(), 1);
    assert_eq!(
        fs.getattr(&context(), created.entry.inode, Some(created.handle))
            .unwrap()
            .size,
        8,
        "visible length follows the returned admitted byte count"
    );
    meta.fail_next_commit();
    assert_eq!(
        fs.write(&context(), created.handle, 8, b"!")
            .unwrap_err()
            .code(),
        afs_error::NODE_VFS_UNAVAILABLE,
        "the uncertain later window is reported on the next operation"
    );
}

#[test]
fn native_sync_write_error_does_not_report_unconfirmed_bytes() {
    let (_temp, meta, fs) = test_fs();
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("budget-sync-error.bin"),
            0o640,
            libc::O_RDWR | libc::O_SYNC,
        )
        .unwrap();

    meta.fail_next_commit();
    assert_eq!(
        fs.write(&context(), created.handle, 0, b"abcdefgh")
            .unwrap_err()
            .code(),
        afs_error::NODE_VFS_UNAVAILABLE,
        "O_SYNC cannot report bytes whose publication result is unknown"
    );
    assert_eq!(meta.commit_count(), 0);
    assert_eq!(meta.failed_commits.lock().unwrap().len(), 1);
}

#[test]
fn native_sync_append_write_serializes_concurrent_resize() {
    let (_temp, meta, fs) = test_fs();
    let fs = Arc::new(fs);
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("append-resize.bin"),
            0o640,
            libc::O_RDWR | libc::O_APPEND | libc::O_SYNC,
        )
        .unwrap();
    let pause = meta.pause_commit_after_successes(0);

    let writer_fs = fs.clone();
    let writer_handle = created.handle;
    let inode = created.entry.inode;
    let handle = created.handle;
    let writer =
        std::thread::spawn(move || writer_fs.write(&context(), writer_handle, 0, b"abcdefgh"));
    pause.wait_until_reached();

    let resizer_fs = fs.clone();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let resizer = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        resizer_fs.setattr(
            &context(),
            inode,
            Some(handle),
            &AttributeChange {
                size: Some(1),
                ..AttributeChange::default()
            },
        )
    });
    started_rx.recv().unwrap();
    pause.release();

    assert_eq!(writer.join().unwrap().unwrap(), 8);
    assert_eq!(resizer.join().unwrap().unwrap().size, 1);
    assert_eq!(
        fs.getattr(&context(), inode, Some(handle)).unwrap().size,
        1,
        "resize runs after the complete append write, not before its accepted sync barrier"
    );
    assert_eq!(meta.commits.lock().unwrap().len(), 1);
    assert_eq!(meta.commits.lock().unwrap()[0].file_version.length, 8);
    fs.fsync(&context(), handle, SyncMode::Full).unwrap();
    assert_eq!(
        meta.commits
            .lock()
            .unwrap()
            .last()
            .unwrap()
            .file_version
            .length,
        1
    );
}

#[test]
fn native_datasync_append_write_serializes_concurrent_full_sync() {
    let (_temp, meta, fs) = test_fs();
    let fs = Arc::new(fs);
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("append-sync.bin"),
            0o640,
            libc::O_RDWR | libc::O_APPEND | libc::O_DSYNC,
        )
        .unwrap();
    let pause = meta.pause_commit_after_successes(0);

    let writer_fs = fs.clone();
    let writer_handle = created.handle;
    let sync_handle = created.handle;
    let writer =
        std::thread::spawn(move || writer_fs.write(&context(), writer_handle, 0, b"abcdefgh"));
    pause.wait_until_reached();

    let sync_fs = fs.clone();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let syncer = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        sync_fs.fsync(&context(), sync_handle, SyncMode::Full)
    });
    started_rx.recv().unwrap();
    pause.release();

    assert_eq!(writer.join().unwrap().unwrap(), 8);
    syncer.join().unwrap().unwrap();

    let commits = meta.commits.lock().unwrap();
    assert_eq!(commits.len(), 1);
    assert_eq!(commits[0].file_version.length, 8);
    let final_head = Some(commits[0].file_version.id.clone());
    drop(commits);

    let metadata_syncs = meta.metadata_syncs.lock().unwrap();
    assert_eq!(metadata_syncs.len(), 1);
    assert_eq!(
        metadata_syncs[0].expected_head_version, final_head,
        "full fsync must synchronize the completed append version, after its completed data-sync barrier"
    );
}
