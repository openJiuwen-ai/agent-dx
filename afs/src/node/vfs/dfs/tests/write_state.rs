use super::*;

#[test]
fn mknod_creates_special_inode_without_write_session() {
    let (_temp, _meta, fs) = test_fs();
    let entry = fs
        .mknod(
            &context(),
            fs.root_inode(),
            OsStr::new("pipe"),
            SpecialFileKind::Fifo,
            0o666,
        )
        .unwrap();
    assert_eq!(
        entry.attributes.kind,
        FileKind::Special(SpecialFileKind::Fifo)
    );
    assert_eq!(entry.attributes.size, 0);
    assert_eq!(entry.attributes.blocks, 0);
    assert_eq!(entry.attributes.mode, 0o666);
    assert_eq!(
        fs.open(&context(), entry.inode, libc::O_RDONLY)
            .unwrap_err()
            .code(),
        afs_error::IO_NOT_SUPPORTED
    );
}

#[test]
fn close_flush_commits_dirty_data_without_an_explicit_sync() {
    let (_temp, meta, fs) = test_fs();
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("close.txt"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    fs.write(&context(), created.handle, 0, b"closed").unwrap();
    assert_eq!(meta.commit_count(), 0, "write does not create a version");
    fs.flush(&context(), created.handle).unwrap();
    assert_eq!(meta.commit_count(), 1, "successful close flush must commit");
    assert_eq!(meta.commits.lock().unwrap()[0].file_version.length, 6);
    fs.flush(&context(), created.handle).unwrap();
    assert_eq!(
        meta.commit_count(),
        1,
        "unchanged flush must not create a version"
    );
    fs.release(&context(), created.handle).unwrap();
    let reader = fs
        .open(&context(), created.entry.inode, libc::O_RDONLY)
        .unwrap();
    let mut out = [0; 6];
    assert_eq!(fs.read(&context(), reader, 0, &mut out).unwrap(), 6);
    assert_eq!(&out, b"closed");
    fs.release(&context(), reader).unwrap();
}

#[test]
fn close_flush_reports_a_commit_error_instead_of_success() {
    let (_temp, meta, fs) = test_fs();
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("failed-close.txt"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    fs.write(&context(), created.handle, 0, b"pending").unwrap();
    *meta.next_commit_error.lock().unwrap() = Some(unavailable("injected ACK loss"));
    assert!(fs.flush(&context(), created.handle).is_err());
    assert_eq!(meta.commit_count(), 0);
    fs.flush(&context(), created.handle).unwrap();
    assert_eq!(meta.commit_count(), 1);
    let failed = meta.failed_commits.lock().unwrap();
    let applied = meta.commits.lock().unwrap();
    assert_eq!(failed[0].operation_id, applied[0].operation_id);
    assert_eq!(failed[0].file_version, applied[0].file_version);
    assert_eq!(failed[0].layout_root, applied[0].layout_root);
}

#[test]
fn readonly_close_does_not_commit_another_handles_dirty_writes() {
    let (_temp, meta, fs) = test_fs();
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("read-close.txt"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    let reader = fs
        .open(&context(), created.entry.inode, libc::O_RDONLY)
        .unwrap();
    fs.write(&context(), created.handle, 0, b"dirty").unwrap();
    fs.flush(&context(), reader).unwrap();
    fs.release(&context(), reader).unwrap();
    assert_eq!(meta.commit_count(), 0);
    fs.flush(&context(), created.handle).unwrap();
    assert_eq!(meta.commit_count(), 1);
    assert_eq!(
        meta.commits.lock().unwrap()[0].metadata_delta.mode,
        CommitMetadataMode::DataOnly
    );
}

#[test]
fn dsync_write_commits_data_without_promoting_to_full_inode_sync() {
    let (_temp, meta, fs) = test_fs();
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("dsync.txt"),
            0o640,
            libc::O_RDWR | libc::O_DSYNC,
        )
        .unwrap();
    fs.write(&context(), created.handle, 0, b"data").unwrap();
    assert_eq!(meta.commit_count(), 1);
    assert_eq!(
        meta.commits.lock().unwrap()[0].metadata_delta.mode,
        CommitMetadataMode::DataOnly
    );
    fs.flush(&context(), created.handle).unwrap();
    assert_eq!(meta.commit_count(), 1);
}

#[test]
fn readonly_handles_share_the_current_mount_dirty_and_committed_view() {
    let (_temp, meta, fs) = test_fs();
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("hello.txt"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();

    assert_eq!(
        fs.write(&context(), created.handle, 0, b"hello").unwrap(),
        5
    );
    assert_eq!(
        meta.commit_count(),
        0,
        "ordinary write does not create FileVersion"
    );

    let reader = fs
        .open(&context(), created.entry.inode, libc::O_RDONLY)
        .unwrap();
    let mut out = [0; 5];
    assert_eq!(
        fs.read(&context(), reader, 0, &mut out).unwrap(),
        5,
        "ordinary readonly handles share the mount's accepted dirty view"
    );
    assert_eq!(&out, b"hello");
    assert_eq!(
        fs.getattr(&context(), created.entry.inode, Some(reader))
            .unwrap()
            .size,
        5
    );

    let writer = fs
        .open(&context(), created.entry.inode, libc::O_RDWR)
        .unwrap();
    let mut writer_view = [0; 5];
    assert_eq!(fs.read(&context(), writer, 0, &mut writer_view).unwrap(), 5);
    assert_eq!(
        &writer_view, b"hello",
        "writer must keep seeing owner dirty"
    );
    fs.release(&context(), created.handle).unwrap();
    assert_eq!(
        meta.commit_count(),
        0,
        "release must not synchronously commit"
    );

    fs.fsync(&context(), writer, SyncMode::DataOnly).unwrap();
    assert_eq!(meta.commit_count(), 1);
    assert_eq!(
        meta.commits.lock().unwrap()[0].metadata_delta.mode,
        CommitMetadataMode::DataOnly
    );
    let data_version = meta.inode.lock().unwrap().head_version.clone();
    fs.fsync(&context(), writer, SyncMode::Full).unwrap();
    assert_eq!(
        meta.commit_count(),
        1,
        "fsync after fdatasync must sync inode metadata without inventing another FileVersion"
    );
    assert_eq!(meta.inode.lock().unwrap().head_version, data_version);
    let committed_reader = fs
        .open(&context(), created.entry.inode, libc::O_RDONLY)
        .unwrap();
    let mut committed = [0; 5];
    assert_eq!(
        fs.read(&context(), committed_reader, 0, &mut committed)
            .unwrap(),
        5
    );
    assert_eq!(&committed, b"hello");

    fs.write(&context(), writer, 0, b"H").unwrap();
    assert_eq!(fs.read(&context(), reader, 0, &mut out).unwrap(), 5);
    assert_eq!(
        &out, b"Hello",
        "accepted overwrites are visible before sync"
    );
    fs.fsync(&context(), writer, SyncMode::Full).unwrap();
    let commits = meta.commits.lock().unwrap();
    assert_eq!(commits.len(), 2);
    assert_eq!(commits[1].metadata_delta.mode, CommitMetadataMode::Full);
    assert_eq!(
        commits[1].file_version.parent_version,
        Some(commits[0].file_version.id.clone())
    );
    drop(commits);

    let mut latest = [0; 5];
    assert_eq!(fs.read(&context(), reader, 0, &mut latest).unwrap(), 5);
    assert_eq!(&latest, b"Hello");
    assert_eq!(
        fs.read(&context(), committed_reader, 0, &mut latest)
            .unwrap(),
        5
    );
    assert_eq!(&latest, b"Hello");
    let fresh_reader = fs
        .open(&context(), created.entry.inode, libc::O_RDONLY)
        .unwrap();
    assert_eq!(
        fs.read(&context(), fresh_reader, 0, &mut latest).unwrap(),
        5
    );
    assert_eq!(&latest, b"Hello");
    fs.release(&context(), writer).unwrap();
    fs.release(&context(), reader).unwrap();
    fs.release(&context(), committed_reader).unwrap();
    fs.release(&context(), fresh_reader).unwrap();
    assert_eq!(fs.writeback_pending().unwrap(), 0);
}

#[test]
fn previously_open_readonly_handle_tracks_append_and_shrink_grow() {
    let (_temp, meta, fs) = test_fs();
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("view.bin"),
            0o600,
            libc::O_RDWR,
        )
        .unwrap();
    let reader = fs
        .open(&context(), created.entry.inode, libc::O_RDONLY)
        .unwrap();
    let mut out = [0; 8];
    assert_eq!(fs.read(&context(), reader, 0, &mut out).unwrap(), 0);
    fs.write(&context(), created.handle, 0, b"abcd").unwrap();
    fs.write(&context(), created.handle, 4, b"efgh").unwrap();
    assert_eq!(fs.read(&context(), reader, 0, &mut out).unwrap(), 8);
    assert_eq!(&out, b"abcdefgh");
    for length in [3, 8] {
        fs.setattr(
            &context(),
            created.entry.inode,
            Some(created.handle),
            &AttributeChange {
                size: Some(length),
                ..AttributeChange::default()
            },
        )
        .unwrap();
    }
    assert_eq!(meta.commit_count(), 0);
    assert_eq!(fs.read(&context(), reader, 0, &mut out).unwrap(), 8);
    assert_eq!(&out, b"abc\0\0\0\0\0");
    assert_eq!(
        fs.getattr(&context(), created.entry.inode, Some(reader))
            .unwrap()
            .size,
        8
    );
    fs.flush(&context(), created.handle).unwrap();
    assert_eq!(fs.read(&context(), reader, 0, &mut out).unwrap(), 8);
    assert_eq!(&out, b"abc\0\0\0\0\0");
}

#[test]
fn writeonly_handle_cannot_read_the_inode_view() {
    let (_temp, _meta, fs) = test_fs();
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("writeonly.bin"),
            0o600,
            libc::O_WRONLY,
        )
        .unwrap();
    fs.write(&context(), created.handle, 0, b"secret").unwrap();
    assert_eq!(
        fs.read(&context(), created.handle, 0, &mut [0; 8])
            .unwrap_err()
            .code(),
        afs_error::IO_BAD_FILE_DESCRIPTOR
    );
}

#[test]
fn metadata_changes_refresh_active_write_state_after_close() {
    let (_temp, _meta, fs) = test_fs();
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("attrs.bin"),
            0o644,
            libc::O_RDWR,
        )
        .unwrap();
    fs.write(&context(), created.handle, 0, b"mode").unwrap();
    fs.flush(&context(), created.handle).unwrap();
    fs.release(&context(), created.handle).unwrap();

    let changed = AttributeChange {
        mode: Some(libc::S_IFREG | 0o640),
        uid: Some(4242),
        gid: Some(60002),
        ..AttributeChange::default()
    };
    fs.setattr(&context(), created.entry.inode, None, &changed)
        .unwrap();
    let attrs = fs.getattr(&context(), created.entry.inode, None).unwrap();
    assert_eq!(attrs.mode & 0o777, 0o640);
    assert_eq!(attrs.uid, 4242);
    assert_eq!(attrs.gid, 60002);
}

#[test]
fn hardlink_refreshes_active_write_state_nlink() {
    let (_temp, _meta, fs) = test_fs();
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("link-a"),
            0o644,
            libc::O_RDWR,
        )
        .unwrap();
    fs.write(&context(), created.handle, 0, b"hardlink-data")
        .unwrap();
    fs.flush(&context(), created.handle).unwrap();

    let linked = fs
        .link(
            &context(),
            created.entry.inode,
            fs.root_inode(),
            OsStr::new("link-b"),
        )
        .unwrap();
    assert_eq!(linked.attributes.nlink, 2);
    assert_eq!(
        fs.getattr(&context(), created.entry.inode, Some(created.handle))
            .unwrap()
            .nlink,
        2
    );
    assert_eq!(
        fs.getattr(&context(), created.entry.inode, None)
            .unwrap()
            .nlink,
        2
    );
}

#[test]
fn namespace_and_xattr_refresh_preserve_dirty_write_mtime_for_full_sync() {
    let (_temp, meta, fs) = test_fs();
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("dirty-mtime.bin"),
            0o644,
            libc::O_RDWR,
        )
        .unwrap();
    fs.write(&context(), created.handle, 0, b"mtime").unwrap();
    let dirty_mtime = {
        let state = fs
            .write_state(&meta.inode.lock().unwrap().inode_id)
            .unwrap()
            .unwrap();
        state.lock().unwrap().inode.attributes.mtime_unix_ms
    };

    fs.link(
        &context(),
        created.entry.inode,
        fs.root_inode(),
        OsStr::new("dirty-mtime-link"),
    )
    .unwrap();
    fs.setxattr(
        &context(),
        created.entry.inode,
        OsStr::new("user.dirty_mtime"),
        b"value",
        0,
    )
    .unwrap();
    fs.setattr(
        &context(),
        created.entry.inode,
        Some(created.handle),
        &AttributeChange {
            mode: Some(libc::S_IFREG | 0o640),
            ..AttributeChange::default()
        },
    )
    .unwrap();
    fs.fsync(&context(), created.handle, SyncMode::Full)
        .unwrap();

    let commits = meta.commits.lock().unwrap();
    assert_eq!(commits.len(), 1);
    assert_eq!(commits[0].metadata_delta.mode, CommitMetadataMode::Full);
    assert_eq!(commits[0].metadata_delta.mtime_unix_ms, Some(dirty_mtime));
}

#[test]
fn explicit_mtime_refresh_overrides_dirty_write_mtime_for_full_sync() {
    let (_temp, meta, fs) = test_fs();
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("explicit-mtime.bin"),
            0o644,
            libc::O_RDWR,
        )
        .unwrap();
    fs.write(&context(), created.handle, 0, b"mtime").unwrap();
    let explicit = UNIX_EPOCH + std::time::Duration::from_millis(42);
    fs.setattr(
        &context(),
        created.entry.inode,
        Some(created.handle),
        &AttributeChange {
            mtime: Some(explicit),
            ..AttributeChange::default()
        },
    )
    .unwrap();
    fs.fsync(&context(), created.handle, SyncMode::Full)
        .unwrap();

    let commits = meta.commits.lock().unwrap();
    assert_eq!(commits.len(), 1);
    assert_eq!(commits[0].metadata_delta.mode, CommitMetadataMode::Full);
    assert_eq!(commits[0].metadata_delta.mtime_unix_ms, Some(42));
}

#[test]
fn getattr_refreshes_active_write_state_from_meta_without_losing_dirty_mtime() {
    let (_temp, meta, fs) = test_fs();
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("remote-fresh.bin"),
            0o644,
            libc::O_RDWR,
        )
        .unwrap();
    fs.write(&context(), created.handle, 0, b"fresh").unwrap();
    let dirty_mtime = {
        let state = fs
            .write_state(&meta.inode.lock().unwrap().inode_id)
            .unwrap()
            .unwrap();
        state.lock().unwrap().inode.attributes.mtime_unix_ms
    };
    {
        let mut inode = meta.inode.lock().unwrap();
        inode.attributes.mode = libc::S_IFREG | 0o660;
        inode.attributes.gid = 60002;
        inode.attributes.nlink = 2;
        inode.attributes.mtime_unix_ms = 7;
        inode.attributes.ctime_unix_ms = inode.attributes.ctime_unix_ms.saturating_add(100);
        inode.revision = inode.revision.saturating_add(1);
    }

    let attrs = fs
        .getattr(&context(), created.entry.inode, Some(created.handle))
        .unwrap();
    assert_eq!(attrs.mode & 0o777, 0o660);
    assert_eq!(attrs.gid, 60002);
    assert_eq!(attrs.nlink, 2);
    assert_eq!(system_time_ms(attrs.mtime), dirty_mtime);
}

#[test]
fn sparse_write_materializes_only_payload_chunk() {
    let (_temp, meta, fs) = test_fs();
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("sparse.bin"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    let offset = 1024 * 1024;
    assert_eq!(
        fs.write(&context(), created.handle, offset, b"tail")
            .unwrap(),
        4
    );

    let attrs = fs
        .getattr(&context(), created.entry.inode, Some(created.handle))
        .unwrap();
    assert_eq!(attrs.size, offset + 4);
    let mut head = [1; 16];
    assert_eq!(
        fs.read(&context(), created.handle, 0, &mut head).unwrap(),
        head.len()
    );
    assert_eq!(head, [0; 16]);

    fs.fsync(&context(), created.handle, SyncMode::Full)
        .unwrap();
    let commits = meta.commits.lock().unwrap();
    let commit = commits.last().unwrap();
    assert_eq!(commit.file_version.length, offset + 4);
    assert_eq!(commit.layout_root.file_length, offset + 4);
    assert_eq!(commit.layout_root.inline_extents.len(), 1);
    assert_eq!(commit.layout_root.inline_extents[0].file_offset, offset);
    assert_eq!(commit.layout_root.inline_extents[0].length, 4);
    assert_eq!(commit.chunk_receipts.len(), 1);
}

#[test]
fn handle_resize_shrink_then_grow_masks_old_tail_without_new_chunk() {
    let (_temp, meta, fs) = test_fs();
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("resize.bin"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    fs.write(&context(), created.handle, 0, b"abcdefgh")
        .unwrap();
    fs.fsync(&context(), created.handle, SyncMode::Full)
        .unwrap();
    let before_seq = fs
        .handle_snapshot(created.handle)
        .unwrap()
        .write_session
        .unwrap()
        .local
        .last_accepted_seq;

    let shrink = AttributeChange {
        size: Some(4),
        ..AttributeChange::default()
    };
    let grow = AttributeChange {
        size: Some(8),
        ..AttributeChange::default()
    };
    fs.setattr(
        &context(),
        created.entry.inode,
        Some(created.handle),
        &shrink,
    )
    .unwrap();
    let attrs = fs
        .setattr(&context(), created.entry.inode, Some(created.handle), &grow)
        .unwrap();
    assert_eq!(attrs.size, 8);
    let after_seq = fs
        .handle_snapshot(created.handle)
        .unwrap()
        .write_session
        .unwrap()
        .local
        .last_accepted_seq;
    assert!(after_seq > before_seq);

    let mut visible = [1; 8];
    assert_eq!(
        fs.read(&context(), created.handle, 0, &mut visible)
            .unwrap(),
        visible.len()
    );
    assert_eq!(&visible, b"abcd\0\0\0\0");
    fs.fsync(&context(), created.handle, SyncMode::Full)
        .unwrap();

    let commits = meta.commits.lock().unwrap();
    assert_eq!(commits.len(), 2);
    let resized = commits.last().unwrap();
    assert_eq!(resized.file_version.length, 8);
    assert!(resized.chunk_receipts.is_empty());
    assert_eq!(resized.layout_root.inline_extents.len(), 1);
    assert_eq!(resized.layout_root.inline_extents[0].length, 4);
    drop(commits);

    fs.setattr(&context(), created.entry.inode, Some(created.handle), &grow)
        .unwrap();
    fs.fsync(&context(), created.handle, SyncMode::Full)
        .unwrap();
    assert_eq!(
        meta.commit_count(),
        2,
        "same-length resize must not create another FileVersion"
    );
}

#[test]
fn path_resize_uses_inode_state_without_opening_a_writer() {
    let (_temp, meta, fs) = test_fs();
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("path-resize.bin"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    fs.write(&context(), created.handle, 0, b"abcdefgh")
        .unwrap();
    fs.fsync(&context(), created.handle, SyncMode::Full)
        .unwrap();
    fs.release(&context(), created.handle).unwrap();

    let resize = AttributeChange {
        size: Some(3),
        ..AttributeChange::default()
    };
    let attrs = fs
        .setattr(&context(), created.entry.inode, None, &resize)
        .unwrap();
    assert_eq!(attrs.size, 3);
    let state = fs
        .write_state(&meta.inode.lock().unwrap().inode_id)
        .unwrap()
        .unwrap();
    let state = state.lock().unwrap();
    assert_eq!(state.open_writers, 0);
    assert!(state.last_writer_background_requested);
    drop(state);

    let reader = fs
        .open(&context(), created.entry.inode, libc::O_RDONLY)
        .unwrap();
    let readonly_error = fs
        .setattr(
            &context(),
            created.entry.inode,
            Some(reader),
            &AttributeChange {
                size: Some(2),
                ..AttributeChange::default()
            },
        )
        .unwrap_err();
    assert_eq!(readonly_error.code(), afs_error::IO_BAD_FILE_DESCRIPTOR);
    let mut visible = [0; 8];
    assert_eq!(fs.read(&context(), reader, 0, &mut visible).unwrap(), 3);
    assert_eq!(&visible[..3], b"abc");
    assert_eq!(
        fs.getattr(&context(), created.entry.inode, Some(reader))
            .unwrap()
            .size,
        3
    );

    assert_eq!(fs.writeback_pending().unwrap(), 1);
    assert_eq!(meta.commit_count(), 2);
    let commits = meta.commits.lock().unwrap();
    let resized = commits.last().unwrap();
    assert_eq!(resized.file_version.length, 3);
    assert!(resized.chunk_receipts.is_empty());
}

#[test]
fn closed_metadata_dirty_write_state_reopen_adopts_fresh_epoch_without_old_renewal() {
    let (_temp, meta, fs) = test_fs();
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("clean-reopen-fresh-lease.bin"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    assert_eq!(fs.write(&context(), created.handle, 0, b"base").unwrap(), 4);
    fs.flush(&context(), created.handle).unwrap();
    fs.release(&context(), created.handle).unwrap();
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    let state = fs.write_state(&inode_id).unwrap().unwrap();
    {
        let mut state = state.lock().unwrap();
        state.write_lease.expires_at_unix_ms = now_unix_ms().saturating_sub(1);
        assert!(!DistributedFs::write_state_can_adopt_fresh_lease(&state));
        assert!(DistributedFs::write_state_can_rebind_fresh_lease_preserving_metadata(&state));
    }
    let fresh_epoch = {
        let mut lease = meta.lease.lock().unwrap();
        lease.lease_epoch = lease.lease_epoch.saturating_add(1);
        lease.expires_at_unix_ms = now_unix_ms().saturating_add(30_000);
        lease.lease_epoch
    };
    meta.fail_next_renew_with(Error::coded(
        afs_error::META_DFS_CONFLICT,
        "old clean lease must be replaced by fresh Meta open before renewal",
    ));
    let renews_before = meta.renew_call_count();

    let handle = fs
        .open(&context(), created.entry.inode, libc::O_RDWR)
        .unwrap();

    assert_eq!(meta.renew_call_count(), renews_before);
    let state = fs.write_state(&inode_id).unwrap().unwrap();
    let state = state.lock().unwrap();
    assert_eq!(state.write_lease.lease_epoch, fresh_epoch);
    assert!(state.write_lease.expires_at_unix_ms > now_unix_ms());
    drop(state);
    assert_eq!(fs.write(&context(), handle, 4, b"x").unwrap(), 1);
}

#[test]
fn clean_retained_write_state_path_resize_reacquires_fresh_epoch() {
    let (_temp, meta, fs) = test_fs();
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("clean-resize-fresh-lease.bin"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    fs.release(&context(), created.handle).unwrap();
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    let state = fs.write_state(&inode_id).unwrap().unwrap();
    {
        let mut state = state.lock().unwrap();
        state.write_lease.expires_at_unix_ms = now_unix_ms().saturating_sub(1);
        assert!(DistributedFs::write_state_can_adopt_fresh_lease(&state));
    }
    let fresh_epoch = {
        let mut lease = meta.lease.lock().unwrap();
        lease.lease_epoch = lease.lease_epoch.saturating_add(1);
        lease.expires_at_unix_ms = now_unix_ms().saturating_add(30_000);
        lease.lease_epoch
    };
    meta.fail_next_renew_with(Error::coded(
        afs_error::META_DFS_CONFLICT,
        "path resize must not renew the old clean lease",
    ));
    let renews_before = meta.renew_call_count();

    let attrs = fs
        .setattr(
            &context(),
            created.entry.inode,
            None,
            &AttributeChange {
                size: Some(5),
                ..AttributeChange::default()
            },
        )
        .unwrap();

    assert_eq!(attrs.size, 5);
    assert_eq!(meta.renew_call_count(), renews_before);
    let state = fs.write_state(&inode_id).unwrap().unwrap();
    let state = state.lock().unwrap();
    assert_eq!(state.write_lease.lease_epoch, fresh_epoch);
    assert_eq!(state.logical_length, 5);
    assert!(state.dirty);
}

#[test]
fn metadata_dirty_write_state_rejects_fresh_epoch_after_head_change() {
    let (_temp, meta, fs) = test_fs();
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("metadata-dirty-reject-changed-head.bin"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    assert_eq!(fs.write(&context(), created.handle, 0, b"base").unwrap(), 4);
    fs.flush(&context(), created.handle).unwrap();
    fs.release(&context(), created.handle).unwrap();
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    let state = fs.write_state(&inode_id).unwrap().unwrap();
    {
        let mut state = state.lock().unwrap();
        state.write_lease.expires_at_unix_ms = now_unix_ms().saturating_sub(1);
        assert!(DistributedFs::write_state_can_rebind_fresh_lease_preserving_metadata(&state));
    }
    {
        let mut lease = meta.lease.lock().unwrap();
        lease.lease_epoch = lease.lease_epoch.saturating_add(1);
        lease.expires_at_unix_ms = now_unix_ms().saturating_add(30_000);
    }
    meta.advance_namespace_revision();

    let error = fs
        .open(&context(), created.entry.inode, libc::O_RDWR)
        .expect_err("metadata-dirty state must not rebind across a changed Meta inode");

    assert_eq!(error.code(), afs_error::NODE_DFS_STALE_HANDLE);
    let state = state.lock().unwrap();
    assert!(state.metadata_dirty);
    assert_eq!(state.write_lease.lease_epoch, 1);
}

#[test]
fn clean_retained_write_state_rejects_delayed_older_epoch() {
    let (_temp, meta, fs) = test_fs();
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("clean-reject-old-epoch.bin"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    fs.release(&context(), created.handle).unwrap();
    let inode = meta.inode.lock().unwrap().clone();
    let older_lease = meta.lease.lock().unwrap().clone();
    let mut newer_lease = older_lease.clone();
    newer_lease.lease_epoch = newer_lease.lease_epoch.saturating_add(1);
    newer_lease.expires_at_unix_ms = now_unix_ms().saturating_add(30_000);
    fs.install_write_state(inode.clone(), newer_lease.clone())
        .unwrap();

    let error = fs
        .install_write_state(inode, older_lease)
        .expect_err("a delayed old open response must not downgrade a newer clean state");

    assert_eq!(error.code(), afs_error::NODE_DFS_STALE_HANDLE);
    let state = fs.write_state(&newer_lease.inode_id).unwrap().unwrap();
    assert_eq!(
        state.lock().unwrap().write_lease.lease_epoch,
        newer_lease.lease_epoch
    );
}

#[test]
fn dirty_write_state_rejects_fresh_epoch_without_losing_dirty_data() {
    let (_temp, meta, fs) = test_fs();
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("dirty-reject-fresh-epoch.bin"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    assert_eq!(
        fs.write(&context(), created.handle, 0, b"dirty").unwrap(),
        5
    );
    let inode = meta.inode.lock().unwrap().clone();
    let inode_id = inode.inode_id.clone();
    let mut fresh_lease = meta.lease.lock().unwrap().clone();
    fresh_lease.lease_epoch = fresh_lease.lease_epoch.saturating_add(1);
    fresh_lease.expires_at_unix_ms = now_unix_ms().saturating_add(30_000);

    let error = fs
        .install_write_state(inode, fresh_lease)
        .expect_err("dirty state must not be silently reattached to a new lease epoch");

    assert_eq!(error.code(), afs_error::NODE_DFS_STALE_HANDLE);
    let state = fs.write_state(&inode_id).unwrap().unwrap();
    let state = state.lock().unwrap();
    assert_eq!(state.write_lease.lease_epoch, 1);
    assert!(state.dirty);
    assert_eq!(state.dirty_extents.dirty_data_bytes(), 5);
}
