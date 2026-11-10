use super::*;

#[test]
fn failed_metadata_only_sync_keeps_request_identity_and_blocks_writes() {
    let (_temp, meta, fs) = test_fs();
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("metadata-retry.bin"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    fs.write(&context(), created.handle, 0, b"data").unwrap();
    fs.fsync(&context(), created.handle, SyncMode::DataOnly)
        .unwrap();
    assert_eq!(meta.commit_count(), 1);
    assert!(meta.metadata_syncs.lock().unwrap().is_empty());

    meta.fail_next_metadata_sync();
    fs.fsync(&context(), created.handle, SyncMode::Full)
        .unwrap_err();
    let failed = meta.failed_metadata_syncs.lock().unwrap()[0].clone();
    assert_eq!(
        fs.write(&context(), created.handle, 4, b"!").unwrap(),
        1,
        "write must wait for the pending metadata sync retry before creating new dirty state"
    );
    let metadata_syncs = meta.metadata_syncs.lock().unwrap();
    assert_eq!(metadata_syncs.len(), 1);
    assert_eq!(failed.operation_id, metadata_syncs[0].operation_id);
    assert_eq!(
        failed.expected_inode_revision,
        metadata_syncs[0].expected_inode_revision
    );
    assert_eq!(
        failed.expected_head_version,
        metadata_syncs[0].expected_head_version
    );
}

#[test]
fn full_sync_after_pending_data_only_commit_also_syncs_metadata() {
    let (_temp, meta, fs) = test_fs();
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("full-after-data-pending.bin"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    fs.write(&context(), created.handle, 0, b"data").unwrap();
    let dirty_mtime = {
        let state = fs
            .write_state(&meta.inode.lock().unwrap().inode_id)
            .unwrap()
            .unwrap();
        state.lock().unwrap().inode.attributes.mtime_unix_ms
    };

    meta.fail_next_commit();
    fs.fsync(&context(), created.handle, SyncMode::DataOnly)
        .unwrap_err();
    let failed = meta.failed_commits.lock().unwrap()[0].clone();

    fs.fsync(&context(), created.handle, SyncMode::Full)
        .unwrap();
    let commits = meta.commits.lock().unwrap();
    assert_eq!(commits.len(), 1);
    assert_eq!(failed.operation_id, commits[0].operation_id);
    assert_eq!(failed.file_version.id, commits[0].file_version.id);
    assert_eq!(commits[0].metadata_delta.mode, CommitMetadataMode::DataOnly);
    drop(commits);
    let metadata_syncs = meta.metadata_syncs.lock().unwrap();
    assert_eq!(metadata_syncs.len(), 1);
    assert_eq!(
        metadata_syncs[0].metadata_delta.mtime_unix_ms,
        Some(dirty_mtime)
    );
    assert_eq!(
        metadata_syncs[0].expected_head_version,
        meta.inode.lock().unwrap().head_version
    );
}

#[test]
fn background_writeback_does_not_hold_table_behind_locked_inode() {
    let (_temp, meta, fs) = test_fs();
    let fs = Arc::new(fs);
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("busy-table.bin"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    fs.write(&context(), created.handle, 0, b"data").unwrap();
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    let cell = fs.write_state(&inode_id).unwrap().unwrap();
    let held = cell.lock().unwrap();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let writer_fs = fs.clone();
    let writer = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        writer_fs.writeback_pending()
    });
    started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    // Give the original blocking scan time to acquire the table mutex.
    std::thread::sleep(Duration::from_millis(50));
    let (lookup_tx, lookup_rx) = std::sync::mpsc::channel();
    let lookup_fs = fs.clone();
    let lookup = std::thread::spawn(move || {
        lookup_tx
            .send(lookup_fs.write_state(&inode_id).unwrap().is_some())
            .unwrap();
    });
    let result = lookup_rx.recv_timeout(Duration::from_millis(150));
    drop(held);
    writer.join().unwrap().unwrap();
    lookup.join().unwrap();
    assert!(
        result.unwrap(),
        "busy inode must not block the shared table"
    );
}

#[test]
fn background_writeback_skips_active_commit_and_preserves_exact_request() {
    let (_temp, meta, fs) = test_fs();
    let fs = Arc::new(fs);
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("busy-commit.bin"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    fs.write(&context(), created.handle, 0, b"data").unwrap();
    let pause = meta.pause_commit_after_successes(0);
    let commit_fs = fs.clone();
    let commit =
        std::thread::spawn(move || commit_fs.fsync(&context(), created.handle, SyncMode::DataOnly));
    pause.wait_until_reached();
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    let cell = fs.write_state(&inode_id).unwrap().unwrap();
    let exact = {
        let state = cell.lock().unwrap();
        match state.in_flight.as_ref().unwrap() {
            InFlightCommit::File(pending) => pending.batch.commit.clone(),
            _ => panic!("expected exact prepared file request"),
        }
    };
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let maintenance_fs = fs.clone();
    let maintenance = std::thread::spawn(move || {
        done_tx.send(maintenance_fs.writeback_pending()).unwrap();
    });
    let result = done_rx.recv_timeout(Duration::from_millis(150));
    // Release both workers even on the original regression failure.
    pause.release();
    commit.join().unwrap().unwrap();
    maintenance.join().unwrap();
    assert_eq!(result.unwrap().unwrap(), 0);
    let committed = meta.commits.lock().unwrap();
    assert_eq!(committed.len(), 1);
    assert_eq!(
        serde_json::to_value(exact.as_ref()).unwrap(),
        serde_json::to_value(&committed[0]).unwrap()
    );
}

fn synthetic_write_state(
    inode_id: InodeId,
    lease: WriteLease,
    dirty: bool,
    metadata_dirty: bool,
    kill_suidgid_dirty: bool,
    operation_busy: bool,
) -> SharedInodeWriteState {
    let mut inode = InodeRecord {
        namespace_id: NamespaceId::new("default"),
        inode_id: inode_id.clone(),
        kind: InodeKind::Regular,
        attributes: InodeAttributes {
            mode: 0o640,
            uid: 1000,
            gid: 1000,
            nlink: 1,
            atime_unix_ms: 1,
            mtime_unix_ms: 1,
            ctime_unix_ms: 1,
        },
        head_version: None,
        symlink_target: None,
        xattrs: std::collections::BTreeMap::new(),
        revision: 1,
    };
    let mut write_lease = lease;
    write_lease.inode_id = inode_id.clone();
    inode.inode_id = inode_id;
    Arc::new(InodeWriteStateCell::new(InodeWriteState {
        inode,
        write_lease,
        base_version: None,
        base_layout: LayoutRoot {
            id: LayoutRootId::new("synthetic-empty-layout"),
            file_length: 0,
            inline_extents: Vec::new(),
        },
        logical_length: 0,
        metadata_dirty,
        kill_suidgid_dirty,
        dirty_extents: DirtyExtentMap::default(),
        in_flight: None,
        commit_busy: false,
        operation_busy,
        native_queue: MutationTicketQueue::default(),
        native_thread: None,
        native_owner: DirtyOwner::new(),
        native_due: false,
        native_due_through: 0,
        dirty,
        next_write_seq: 0,
        visible_write_seq: 0,
        durable_write_seq: 0,
        committed_write_seq: 0,
        open_writers: 0,
        last_writer_background_requested: false,
        background_error: None,
        terminal_error: None,
    }))
}

#[test]
fn closed_metadata_only_writer_stops_pinning_old_versions() {
    let (_temp, meta, fs) = test_fs();
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("closed-metadata.bin"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    fs.write(&context(), created.handle, 0, b"data").unwrap();
    fs.fsync(&context(), created.handle, SyncMode::DataOnly)
        .unwrap();
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    let cell = fs.write_state(&inode_id).unwrap().unwrap();
    {
        let state = cell.lock().unwrap();
        assert!(!state.dirty);
        assert!(state.metadata_dirty);
        assert_eq!(state.visible_write_seq, state.committed_write_seq);
    }
    fs.release(&context(), created.handle).unwrap();
    assert_eq!(fs.writeback_pending().unwrap(), 1);
    {
        let mut state = cell.lock().unwrap();
        assert!(!state.metadata_dirty);
        assert!(!state.last_writer_background_requested);
        assert!(state.in_flight.is_none());
        state.write_lease.expires_at_unix_ms = now_unix_ms() + 10_000;
    }
    assert_eq!(fs.renew_active_write_leases().unwrap(), 0);
    assert_eq!(meta.metadata_syncs.lock().unwrap().len(), 1);
}

#[test]
fn active_writer_lease_renewal_runs_without_mutating_pending_commit() {
    let (_temp, meta, fs) = test_fs();
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("idle-writer.bin"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    fs.write(&context(), created.handle, 0, b"pending").unwrap();
    meta.fail_next_commit_with(Error::coded(
        afs_error::CLIENT_DEADLINE_EXCEEDED,
        "unknown commit",
    ));
    fs.fsync(&context(), created.handle, SyncMode::Full)
        .unwrap_err();
    let pending = meta.failed_commits.lock().unwrap()[0].clone();
    let cell = fs.write_state(&pending.inode_id).unwrap().unwrap();
    cell.lock().unwrap().write_lease.expires_at_unix_ms = now_unix_ms() + 10_000;
    let renewed_expiry = now_unix_ms() + 30_000;
    meta.queue_renew_result(Ok(meta.lease_with_expiry(renewed_expiry)));
    assert_eq!(fs.renew_active_write_leases().unwrap(), 1);
    assert_eq!(
        cell.lock().unwrap().write_lease.expires_at_unix_ms,
        renewed_expiry
    );
    let renewal_budgets = meta.renew_timeouts();
    assert_eq!(renewal_budgets.len(), 1);
    assert!(renewal_budgets[0] <= Duration::from_secs(2));
    fs.fsync(&context(), created.handle, SyncMode::Full)
        .unwrap();
    assert_eq!(meta.commits.lock().unwrap()[0], pending);
}

#[test]
fn active_writer_lease_renewal_skips_closed_clean_states_and_rejects_fencing() {
    let (_temp, meta, fs) = test_fs();
    let mut lease = meta.lease.lock().unwrap().clone();
    lease.expires_at_unix_ms = now_unix_ms() + 10_000;
    let cell = synthetic_write_state(
        lease.inode_id.clone(),
        lease.clone(),
        false,
        false,
        false,
        false,
    );
    fs.writes
        .insert_cell(lease.inode_id.clone(), cell.clone())
        .unwrap();
    assert_eq!(fs.renew_active_write_leases().unwrap(), 0);
    assert_eq!(meta.renew_call_count(), 0);
    cell.lock().unwrap().open_writers = 1;
    let mut fenced = lease.clone();
    fenced.lease_epoch += 1;
    meta.queue_renew_result(Ok(fenced));
    assert!(fs.renew_active_write_leases().is_err());
    assert_eq!(cell.lock().unwrap().write_lease, lease);
    meta.queue_renew_result(Err(unavailable("transient Meta outage")));
    assert!(fs.renew_active_write_leases().is_err());
    assert!(cell.lock().unwrap().terminal_error.is_none());
}

#[test]
fn active_writer_lease_renewal_late_reply_cannot_replace_newer_authority() {
    let (_temp, meta, fs) = test_fs();
    let fs = Arc::new(fs);
    let mut lease = meta.lease.lock().unwrap().clone();
    lease.expires_at_unix_ms = now_unix_ms() + 10_000;
    let cell = synthetic_write_state(
        lease.inode_id.clone(),
        lease.clone(),
        true,
        false,
        false,
        false,
    );
    fs.writes
        .insert_cell(lease.inode_id.clone(), cell.clone())
        .unwrap();
    let pause = meta.pause_next_renew();
    meta.queue_renew_result(Ok(meta.lease_with_expiry(now_unix_ms() + 30_000)));
    let worker_fs = fs.clone();
    let worker = std::thread::spawn(move || worker_fs.renew_active_write_leases());
    let deadline = Instant::now() + Duration::from_secs(5);
    while !pause.was_reached() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(pause.was_reached());
    lease.lease_epoch += 1;
    cell.lock().unwrap().write_lease = lease.clone();
    pause.release();
    assert_eq!(worker.join().unwrap().unwrap(), 0);
    assert_eq!(cell.lock().unwrap().write_lease, lease);
}

#[test]
fn foreground_sync_does_not_use_writeback_meta_timeout_slice() {
    let (_temp, meta, fs) = test_fs();
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("foreground-no-budget.bin"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    fs.write(&context(), created.handle, 0, b"data").unwrap();
    fs.fsync(&context(), created.handle, SyncMode::DataOnly)
        .unwrap();
    assert!(meta.commit_timeouts().is_empty());
    assert!(meta.metadata_timeouts().is_empty());
    assert!(meta.renew_timeouts().is_empty());
}

#[test]
fn deadline_commit_error_preserves_exact_batch_for_retry() {
    let (_temp, meta, fs) = test_fs();
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("deadline-retry.bin"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    fs.write(&context(), created.handle, 0, b"data").unwrap();

    meta.fail_next_commit_with(Error::coded(
        afs_error::CLIENT_DEADLINE_EXCEEDED,
        "deadline while committing file version",
    ));
    fs.fsync(&context(), created.handle, SyncMode::Full)
        .unwrap_err();
    let failed = meta.failed_commits.lock().unwrap()[0].clone();

    fs.fsync(&context(), created.handle, SyncMode::Full)
        .unwrap();
    let commits = meta.commits.lock().unwrap();
    assert_eq!(commits.len(), 1);
    assert_eq!(failed.operation_id, commits[0].operation_id);
    assert_eq!(failed.file_version.id, commits[0].file_version.id);
    assert_eq!(failed.layout_root.id, commits[0].layout_root.id);
}

#[test]
fn remote_status_metadata_error_preserves_exact_request_for_retry() {
    let (_temp, meta, fs) = test_fs();
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("remote-status-metadata-retry.bin"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    fs.write(&context(), created.handle, 0, b"data").unwrap();
    fs.fsync(&context(), created.handle, SyncMode::DataOnly)
        .unwrap();

    meta.fail_next_metadata_sync_with(Error::new(
        afs_error::CLIENT_REMOTE_STATUS,
        afs_error::ErrorKind::Unavailable,
        "remote status without AFS detail",
    ));
    fs.fsync(&context(), created.handle, SyncMode::Full)
        .unwrap_err();
    let failed = meta.failed_metadata_syncs.lock().unwrap()[0].clone();

    fs.fsync(&context(), created.handle, SyncMode::Full)
        .unwrap();
    let metadata_syncs = meta.metadata_syncs.lock().unwrap();
    assert_eq!(metadata_syncs.len(), 1);
    assert_eq!(failed.operation_id, metadata_syncs[0].operation_id);
    assert_eq!(
        failed.expected_head_version,
        metadata_syncs[0].expected_head_version
    );
}

#[test]
fn definite_meta_invalid_blocks_subsequent_mutation() {
    let (_temp, meta, fs) = test_fs();
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("terminal-invalid.bin"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    fs.write(&context(), created.handle, 0, b"data").unwrap();

    meta.fail_next_commit_with(Error::coded(
        afs_error::META_CATALOG_INVALID_REQUEST,
        "definite invalid commit",
    ));
    let error = fs
        .fsync(&context(), created.handle, SyncMode::Full)
        .unwrap_err();
    assert_eq!(error.code(), afs_error::META_CATALOG_INVALID_REQUEST);

    let mut visible = [0; 4];
    assert_eq!(
        fs.read(&context(), created.handle, 0, &mut visible)
            .unwrap(),
        4
    );
    assert_eq!(&visible, b"data");
    assert_eq!(
        fs.write(&context(), created.handle, 4, b"!")
            .unwrap_err()
            .code(),
        afs_error::META_CATALOG_INVALID_REQUEST
    );
    assert_eq!(
        fs.fsync(&context(), created.handle, SyncMode::Full)
            .unwrap_err()
            .code(),
        afs_error::META_CATALOG_INVALID_REQUEST
    );
    assert_eq!(meta.commit_count(), 0);
}

#[test]
fn failed_resize_commit_keeps_batch_identity_and_blocks_new_dirty_for_retry() {
    let (_temp, meta, fs) = test_fs();
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("retry-resize.bin"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    fs.write(&context(), created.handle, 0, b"abcdefgh")
        .unwrap();
    fs.fsync(&context(), created.handle, SyncMode::Full)
        .unwrap();
    fs.setattr(
        &context(),
        created.entry.inode,
        Some(created.handle),
        &AttributeChange {
            size: Some(4),
            ..AttributeChange::default()
        },
    )
    .unwrap();

    meta.fail_next_commit();
    fs.fsync(&context(), created.handle, SyncMode::Full)
        .unwrap_err();
    let failed = meta.failed_commits.lock().unwrap()[0].clone();
    let attrs = fs
        .getattr(&context(), created.entry.inode, Some(created.handle))
        .unwrap();
    assert_eq!(attrs.size, 4);
    let mut visible = [0; 8];
    assert_eq!(
        fs.read(&context(), created.handle, 0, &mut visible)
            .unwrap(),
        4
    );
    assert_eq!(&visible[..4], b"abcd");
    assert_eq!(
        fs.write(&context(), created.handle, 4, b"Z").unwrap(),
        1,
        "write must wait for the pending FileVersion retry before appending new dirty state"
    );
    assert_eq!(meta.commit_count(), 2);
    let commits = meta.commits.lock().unwrap();
    let retried = commits.last().unwrap();
    assert_eq!(failed.operation_id, retried.operation_id);
    assert_eq!(failed.file_version.id, retried.file_version.id);
    assert_eq!(failed.layout_root.id, retried.layout_root.id);
    assert_eq!(retried.file_version.length, 4);
    assert_eq!(retried.layout_root.file_length, 4);
    assert!(retried.chunk_receipts.is_empty());
    drop(commits);
    let attrs = fs
        .setattr(
            &context(),
            created.entry.inode,
            Some(created.handle),
            &AttributeChange {
                size: Some(6),
                ..AttributeChange::default()
            },
        )
        .unwrap();
    assert_eq!(attrs.size, 6);
}

#[test]
fn unknown_commit_ack_replays_original_request_after_namespace_revision_change() {
    let (_temp, meta, fs) = test_fs();
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("unknown-ack-revision.bin"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    fs.write(&context(), created.handle, 0, b"data").unwrap();

    meta.fail_next_commit();
    fs.fsync(&context(), created.handle, SyncMode::Full)
        .unwrap_err();
    let failed = meta.failed_commits.lock().unwrap()[0].clone();

    meta.advance_namespace_revision();
    let error = fs.write(&context(), created.handle, 4, b"!").unwrap_err();
    assert_eq!(error.code(), afs_error::NODE_VFS_UNAVAILABLE);

    let cas_failed = meta.cas_failed_commits.lock().unwrap();
    assert_eq!(cas_failed.len(), 1);
    assert_eq!(failed.operation_id, cas_failed[0].operation_id);
    assert_eq!(
        failed.expected_inode_revision,
        cas_failed[0].expected_inode_revision
    );
    assert_eq!(
        failed.expected_head_version,
        cas_failed[0].expected_head_version
    );
    assert_eq!(failed.file_version.id, cas_failed[0].file_version.id);
    assert_eq!(failed.layout_root.id, cas_failed[0].layout_root.id);
}
