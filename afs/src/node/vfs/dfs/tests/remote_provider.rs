use super::*;

#[test]
fn remote_owner_write_after_local_close_rebinds_metadata_dirty_state() {
    let (_temp, meta, fs) = test_fs();
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("remote-after-close.bin"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    assert_eq!(
        fs.write(&context(), created.handle, 0, b"base-data")
            .unwrap(),
        9
    );
    fs.flush(&context(), created.handle).unwrap();
    fs.release(&context(), created.handle).unwrap();
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    let state = fs.write_state(&inode_id).unwrap().unwrap();
    {
        let mut state = state.lock().unwrap();
        state.write_lease.expires_at_unix_ms = now_unix_ms().saturating_sub(1);
        assert!(state.metadata_dirty);
        assert!(DistributedFs::write_state_can_rebind_fresh_lease_preserving_metadata(&state));
    }
    let fresh_epoch = {
        let mut lease = meta.lease.lock().unwrap();
        lease.lease_epoch = lease.lease_epoch.saturating_add(1);
        lease.expires_at_unix_ms = now_unix_ms().saturating_add(30_000);
        lease.lease_epoch
    };

    let open = <DistributedFs as crate::node::vfs::dfs::owner_api::DfsOwnerLifecycleHandler>::open(
        &fs,
        "node-b",
        afs_protocol::node_control::DfsOwnerOpenRequest {
            namespace_id: "default".into(),
            inode_id: "inode:test".into(),
            owner_node_id: "node-a".into(),
            owner_session_id: "session-a".into(),
            lease_epoch: fresh_epoch,
            caller_session_id: "session-b".into(),
            open_flags: libc::O_RDWR,
            kill_suidgid: false,
            open_seq: 1,
        },
    )
    .unwrap();
    let handle = open.handle.expect("owner open returns handle");
    let data_handle = data_owner_handle(&handle);
    let write = <DistributedFs as crate::node::vfs::dfs::owner_api::DfsOwnerFilesHandler>::write(
        &fs,
        "node-b",
        afs_protocol::node_data::DfsOwnerWriteRequest {
            handle: Some(data_handle.clone()),
            operation_id: "remote-after-close-write".into(),
            offset: 0,
            data: b"remote-final".to_vec(),
            append: false,
            kill_suidgid: false,
        },
    )
    .unwrap();
    assert_eq!(write.written, 12);
    <DistributedFs as crate::node::vfs::dfs::owner_api::DfsOwnerFilesHandler>::resize(
        &fs,
        "node-b",
        afs_protocol::node_data::DfsOwnerResizeRequest {
            handle: Some(data_handle.clone()),
            operation_id: "remote-after-close-resize".into(),
            length: 12,
            kill_suidgid: false,
        },
    )
    .unwrap();
    <DistributedFs as crate::node::vfs::dfs::owner_api::DfsOwnerFilesHandler>::sync(
        &fs,
        "node-b",
        afs_protocol::node_data::DfsOwnerSyncRequest {
            handle: Some(data_handle),
            operation_id: "remote-after-close-sync".into(),
            through_write_seq: write.accepted_write_seq,
            data_only: false,
        },
    )
    .unwrap();
    <DistributedFs as crate::node::vfs::dfs::owner_api::DfsOwnerLifecycleHandler>::release(
        &fs,
        "node-b",
        afs_protocol::node_control::DfsOwnerReleaseRequest {
            handle: Some(handle),
        },
    )
    .unwrap();

    let state = state.lock().unwrap();
    assert_eq!(state.write_lease.lease_epoch, fresh_epoch);
    assert_eq!(state.logical_length, 12);
    assert!(!state.metadata_dirty);
}

#[test]
fn path_resize_to_remote_owner_uses_owner_rpc_not_local_lease() {
    let (_temp_a, meta, fs_a_raw) = test_fs();
    let fs_a = Arc::new(fs_a_raw);
    let created = fs_a
        .create(
            &context(),
            fs_a.root_inode(),
            OsStr::new("remote-path-resize.bin"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    assert_eq!(
        fs_a.write(&context(), created.handle, 0, b"abcdef")
            .unwrap(),
        6
    );
    fs_a.flush(&context(), created.handle).unwrap();
    fs_a.release(&context(), created.handle).unwrap();
    assert_eq!(meta.commit_count(), 1);

    let temp_b = tempfile::tempdir().unwrap();
    let chunks_b = Arc::new(LocalChunkStore::open(temp_b.path(), "node-b").unwrap());
    let read_engine_b = Arc::new(crate::node::read::engine::DfsReadEngine::new(
        NamespaceId::new("default"),
        "node-b".into(),
        chunks_b.clone(),
        Arc::new(crate::node::read::engine::UnimplementedReadSourceProvider),
        Arc::new(crate::node::read::engine::UnimplementedChunkTransfer),
        crate::node::read::engine::DfsReadConfig::default(),
    ));
    let remote_owner = Arc::new(LoopbackRemoteDfsOwner {
        owner: fs_a.clone(),
        peer: "node-b".into(),
    }) as Arc<dyn RemoteDfsOwner>;
    let fs_b = test_dfs_new(
        NamespaceId::new("default"),
        "node-b",
        "session-b",
        meta.clone(),
        chunks_b,
        read_engine_b,
    )
    .with_remote_owner_factory(Arc::new(StaticRemoteOwnerFactory {
        owner: remote_owner,
    }));
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    let inode_b = fs_b.backend_inode(&inode_id).unwrap();

    let attrs = fs_b
        .setattr(
            &context(),
            inode_b,
            None,
            &AttributeChange {
                size: Some(3),
                ..AttributeChange::default()
            },
        )
        .unwrap();

    assert_eq!(attrs.size, 3);
    assert_eq!(meta.commit_count(), 2);
    let state = fs_a.write_state(&inode_id).unwrap().unwrap();
    let state = state.lock().unwrap();
    assert_eq!(state.logical_length, 3);
    assert!(!state.dirty);
}

#[test]
fn temporary_remote_resize_preserves_existing_remote_provider() {
    let (_temp_a, meta, fs_a_raw) = test_fs();
    let fs_a = Arc::new(fs_a_raw);
    let created = fs_a
        .create(
            &context(),
            fs_a.root_inode(),
            OsStr::new("remote-provider-preserve.bin"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    assert_eq!(
        fs_a.write(&context(), created.handle, 0, b"abcdef")
            .unwrap(),
        6
    );
    fs_a.fsync(&context(), created.handle, SyncMode::Full)
        .unwrap();
    fs_a.release(&context(), created.handle).unwrap();

    let temp_b = tempfile::tempdir().unwrap();
    let chunks_b = Arc::new(LocalChunkStore::open(temp_b.path(), "node-b").unwrap());
    let read_engine_b = Arc::new(crate::node::read::engine::DfsReadEngine::new(
        NamespaceId::new("default"),
        "node-b".into(),
        chunks_b.clone(),
        Arc::new(crate::node::read::engine::UnimplementedReadSourceProvider),
        Arc::new(crate::node::read::engine::UnimplementedChunkTransfer),
        crate::node::read::engine::DfsReadConfig::default(),
    ));
    let remote_owner = Arc::new(LoopbackRemoteDfsOwner {
        owner: fs_a.clone(),
        peer: "node-b".into(),
    }) as Arc<dyn RemoteDfsOwner>;
    let fs_b = test_dfs_new(
        NamespaceId::new("default"),
        "node-b",
        "session-b",
        meta.clone(),
        chunks_b,
        read_engine_b,
    )
    .with_remote_owner_factory(Arc::new(StaticRemoteOwnerFactory {
        owner: remote_owner,
    }));
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    let inode_b = fs_b.backend_inode(&inode_id).unwrap();
    let handle = fs_b.open(&context(), inode_b, libc::O_RDWR).unwrap();
    let provider_before = fs_b
        .write_coordinator
        .current_remote_provider(&inode_id)
        .unwrap()
        .expect("remote open installs provider");

    let attrs = fs_b
        .setattr(
            &context(),
            inode_b,
            None,
            &AttributeChange {
                size: Some(3),
                ..AttributeChange::default()
            },
        )
        .unwrap();
    assert_eq!(attrs.size, 3);
    let provider_after = fs_b
        .write_coordinator
        .current_remote_provider(&inode_id)
        .unwrap()
        .expect("temporary resize restores existing provider");
    assert!(same_remote_provider(&provider_after, &provider_before));

    assert_eq!(fs_b.write(&context(), handle, 0, b"XYZ").unwrap(), 3);
    assert_eq!(fs_b.getattr(&context(), inode_b, None).unwrap().size, 3);
    let reader = fs_b.open(&context(), inode_b, libc::O_RDONLY).unwrap();
    assert_eq!(
        fs_b.getattr(&context(), inode_b, Some(reader))
            .unwrap()
            .size,
        3
    );
    let mut path_data = [0; 3];
    assert_eq!(fs_b.read(&context(), reader, 0, &mut path_data).unwrap(), 3);
    assert_eq!(&path_data, b"XYZ");
    fs_b.release(&context(), reader).unwrap();

    let mut data = [0; 3];
    assert_eq!(fs_b.read(&context(), handle, 0, &mut data).unwrap(), 3);
    assert_eq!(&data, b"XYZ");

    fs_b.release(&context(), handle).unwrap();
    assert!(
        fs_b.write_coordinator
            .current_remote_provider(&inode_id)
            .unwrap()
            .is_none()
    );
}

#[test]
fn remote_release_failure_after_companion_close_still_removes_provider() {
    let (_temp_a, meta, fs_a_raw) = test_fs();
    let fs_a = Arc::new(fs_a_raw);
    let created = fs_a
        .create(
            &context(),
            fs_a.root_inode(),
            OsStr::new("remote-release-failure-cleans-provider.bin"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    fs_a.write(&context(), created.handle, 0, b"abcdef")
        .unwrap();
    fs_a.fsync(&context(), created.handle, SyncMode::Full)
        .unwrap();
    fs_a.release(&context(), created.handle).unwrap();

    let temp_b = tempfile::tempdir().unwrap();
    let chunks_b = Arc::new(LocalChunkStore::open(temp_b.path(), "node-b").unwrap());
    let read_engine_b = Arc::new(crate::node::read::engine::DfsReadEngine::new(
        NamespaceId::new("default"),
        "node-b".into(),
        chunks_b.clone(),
        Arc::new(crate::node::read::engine::UnimplementedReadSourceProvider),
        Arc::new(crate::node::read::engine::UnimplementedChunkTransfer),
        crate::node::read::engine::DfsReadConfig::default(),
    ));
    let remote_owner = Arc::new(LoopbackRemoteDfsOwner {
        owner: fs_a.clone(),
        peer: "node-b".into(),
    }) as Arc<dyn RemoteDfsOwner>;
    let fs_b = test_dfs_new(
        NamespaceId::new("default"),
        "node-b",
        "session-b",
        meta.clone(),
        chunks_b,
        read_engine_b,
    )
    .with_remote_owner_factory(Arc::new(StaticRemoteOwnerFactory {
        owner: remote_owner,
    }));
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    let inode_b = fs_b.backend_inode(&inode_id).unwrap();
    let writer = fs_b.open(&context(), inode_b, libc::O_WRONLY).unwrap();
    let provider = fs_b
        .write_coordinator
        .current_remote_provider(&inode_id)
        .unwrap()
        .expect("remote open installs provider");
    assert!(provider.read_handle.is_some());
    let owner_writer = file_handle_from_owner(&provider.handle).unwrap();
    fs_a.handles.remove(owner_writer.0).unwrap().unwrap();

    fs_b.release(&context(), writer).unwrap();
    assert!(
        fs_b.write_coordinator
            .current_remote_provider(&inode_id)
            .unwrap()
            .is_none()
    );
    assert!(fs_a.handles.is_empty());
}

#[test]
fn paused_companion_release_retires_provider_before_fresh_read() {
    let (_temp_a, meta, fs_a_raw) = test_fs();
    let fs_a = Arc::new(fs_a_raw);
    let created = fs_a
        .create(
            &context(),
            fs_a.root_inode(),
            OsStr::new("remote-release-race.bin"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    fs_a.write(&context(), created.handle, 0, b"abcdef")
        .unwrap();
    fs_a.fsync(&context(), created.handle, SyncMode::Full)
        .unwrap();
    fs_a.release(&context(), created.handle).unwrap();

    let chunks_b = fs_a.chunk_store.clone();
    let read_engine_b = fs_a.read_engine.clone();
    let pause = Arc::new(OwnerOpenFinishPause::new());
    let loopback = Arc::new(LoopbackRemoteDfsOwner {
        owner: fs_a.clone(),
        peer: "node-b".into(),
    }) as Arc<dyn RemoteDfsOwner>;
    let scripted = Arc::new(ScriptedReleaseRemoteDfsOwner::new(
        loopback,
        vec![ScriptedReleaseAction::DelegateThenPause(pause.clone())],
    ));
    let remote_owner = scripted.clone() as Arc<dyn RemoteDfsOwner>;
    let fs_b = Arc::new(
        test_dfs_new(
            NamespaceId::new("default"),
            "node-b",
            "session-b",
            meta.clone(),
            chunks_b,
            read_engine_b,
        )
        .with_remote_owner_factory(Arc::new(StaticRemoteOwnerFactory {
            owner: remote_owner,
        })),
    );
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    let inode_b = fs_b.backend_inode(&inode_id).unwrap();
    let writer = fs_b
        .open(&context(), inode_b, libc::O_WRONLY | libc::O_TRUNC)
        .unwrap();

    assert_eq!(fs_b.write(&context(), writer, 0, b"XYZ").unwrap(), 3);
    fs_b.fsync(&context(), writer, SyncMode::Full).unwrap();

    let release_fs = fs_b.clone();
    let releaser = std::thread::spawn(move || release_fs.release(&context(), writer));
    pause.wait_until_reached();

    assert!(
        fs_b.write_coordinator
            .current_remote_provider(&inode_id)
            .unwrap()
            .is_none(),
        "provider must be retired before the owner release RPC can expose a closed handle"
    );
    let reader = fs_b.open(&context(), inode_b, libc::O_RDONLY).unwrap();
    let mut data = [0; 3];
    assert_eq!(fs_b.read(&context(), reader, 0, &mut data).unwrap(), 3);
    assert_eq!(&data, b"XYZ");
    fs_b.release(&context(), reader).unwrap();

    pause.resume();
    releaser.join().unwrap().unwrap();
    assert_eq!(scripted.release_call_count(), 2);
    assert!(fs_a.handles.is_empty());
}

#[test]
fn retiring_provider_waits_for_inflight_fresh_read_before_owner_release() {
    let (_temp_a, meta, fs_a_raw) = test_fs();
    let fs_a = Arc::new(fs_a_raw);
    let created = fs_a
        .create(
            &context(),
            fs_a.root_inode(),
            OsStr::new("remote-provider-inflight-read.bin"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    fs_a.write(&context(), created.handle, 0, b"abcdef")
        .unwrap();
    fs_a.fsync(&context(), created.handle, SyncMode::Full)
        .unwrap();
    fs_a.release(&context(), created.handle).unwrap();

    let chunks_b = fs_a.chunk_store.clone();
    let read_engine_b = fs_a.read_engine.clone();
    let loopback = Arc::new(LoopbackRemoteDfsOwner {
        owner: fs_a.clone(),
        peer: "node-b".into(),
    }) as Arc<dyn RemoteDfsOwner>;
    let scripted = Arc::new(ScriptedReleaseRemoteDfsOwner::new(loopback, Vec::new()));
    let read_pause = Arc::new(OwnerOpenFinishPause::new());
    scripted.set_read_pause(read_pause.clone());
    let remote_owner = scripted.clone() as Arc<dyn RemoteDfsOwner>;
    let fs_b = Arc::new(
        test_dfs_new(
            NamespaceId::new("default"),
            "node-b",
            "session-b",
            meta.clone(),
            chunks_b,
            read_engine_b,
        )
        .with_remote_owner_factory(Arc::new(StaticRemoteOwnerFactory {
            owner: remote_owner,
        })),
    );
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    let inode_b = fs_b.backend_inode(&inode_id).unwrap();
    let writer = fs_b
        .open(&context(), inode_b, libc::O_WRONLY | libc::O_TRUNC)
        .unwrap();
    fs_b.write(&context(), writer, 0, b"XYZ").unwrap();
    fs_b.fsync(&context(), writer, SyncMode::Full).unwrap();

    let reader = fs_b.open(&context(), inode_b, libc::O_RDONLY).unwrap();
    let reader_fs = fs_b.clone();
    let reader_thread = std::thread::spawn(move || {
        let mut data = [0; 3];
        let read = reader_fs.read(&context(), reader, 0, &mut data);
        (read, data)
    });
    read_pause.wait_until_reached();

    let release_fs = fs_b.clone();
    let (release_done_tx, release_done_rx) = std::sync::mpsc::channel();
    let releaser = std::thread::spawn(move || {
        let result = release_fs.release(&context(), writer);
        release_done_tx.send(()).unwrap();
        result
    });
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
    while fs_b
        .write_coordinator
        .current_remote_provider(&inode_id)
        .unwrap()
        .is_some()
    {
        assert!(
            std::time::Instant::now() < deadline,
            "provider was not retired while an admitted read was paused"
        );
        std::thread::yield_now();
    }
    assert_eq!(
        release_done_rx.recv_timeout(std::time::Duration::from_millis(100)),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout),
        "provider release must wait for an already admitted provider read"
    );

    read_pause.resume();
    let (read, data) = reader_thread.join().unwrap();
    assert_eq!(read.unwrap(), 3);
    assert_eq!(&data, b"XYZ");
    releaser.join().unwrap().unwrap();
    assert!(release_done_rx.recv().is_ok());
    assert_eq!(scripted.release_call_count(), 2);
    assert!(fs_a.handles.is_empty());
}

#[test]
fn remote_provider_lifecycle_wait_idle_times_out_with_admitted_io() {
    let lifecycle = RemoteProviderLifecycle::live();
    let guard = lifecycle
        .begin_io()
        .unwrap()
        .expect("live lifecycle admits provider IO");
    lifecycle.retire_new_io().unwrap();
    assert!(
        lifecycle.begin_io().unwrap().is_none(),
        "retired provider must reject new IO while admitted IO drains"
    );

    let started = Instant::now();
    let result = lifecycle.wait_idle(Duration::from_millis(50));
    assert!(result.is_err());
    assert!(
        started.elapsed() >= Duration::from_millis(40),
        "wait_idle should wait for its budget before timing out"
    );
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "wait_idle timeout must be bounded"
    );

    drop(guard);
    lifecycle.wait_idle(Duration::from_secs(1)).unwrap();
}

#[test]
fn temporary_remote_release_failure_still_restores_previous_provider() {
    let (_temp_a, meta, fs_a_raw) = test_fs();
    let fs_a = Arc::new(fs_a_raw);
    let created = fs_a
        .create(
            &context(),
            fs_a.root_inode(),
            OsStr::new("remote-temp-release-failure-restores.bin"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    fs_a.write(&context(), created.handle, 0, b"abcdef")
        .unwrap();
    fs_a.fsync(&context(), created.handle, SyncMode::Full)
        .unwrap();
    fs_a.release(&context(), created.handle).unwrap();

    let temp_b = tempfile::tempdir().unwrap();
    let chunks_b = Arc::new(LocalChunkStore::open(temp_b.path(), "node-b").unwrap());
    let read_engine_b = Arc::new(crate::node::read::engine::DfsReadEngine::new(
        NamespaceId::new("default"),
        "node-b".into(),
        chunks_b.clone(),
        Arc::new(crate::node::read::engine::UnimplementedReadSourceProvider),
        Arc::new(crate::node::read::engine::UnimplementedChunkTransfer),
        crate::node::read::engine::DfsReadConfig::default(),
    ));
    let remote_owner = Arc::new(LoopbackRemoteDfsOwner {
        owner: fs_a.clone(),
        peer: "node-b".into(),
    }) as Arc<dyn RemoteDfsOwner>;
    let fs_b = test_dfs_new(
        NamespaceId::new("default"),
        "node-b",
        "session-b",
        meta.clone(),
        chunks_b,
        read_engine_b,
    )
    .with_remote_owner_factory(Arc::new(StaticRemoteOwnerFactory {
        owner: remote_owner,
    }));
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    let inode_b = fs_b.backend_inode(&inode_id).unwrap();
    let writer = fs_b.open(&context(), inode_b, libc::O_RDWR).unwrap();
    let previous = fs_b
        .write_coordinator
        .current_remote_provider(&inode_id)
        .unwrap()
        .expect("remote open installs provider");
    let (record, lease) = meta.open_write(&inode_id).unwrap();
    let record = fs_b.validate_inode(record).unwrap();
    let temp = fs_b
        .open_remote_write_session(&record, &lease, libc::O_WRONLY, OpenOptions::default())
        .unwrap();
    let temp_remote = temp.remote.as_ref().unwrap().clone();
    let owner_writer = file_handle_from_owner(&temp_remote.handle).unwrap();
    fs_a.handles.remove(owner_writer.0).unwrap().unwrap();

    fs_b.close_temporary_remote_write_session(&inode_id, &temp, Some(previous.clone()))
        .unwrap();
    let current = fs_b
        .write_coordinator
        .current_remote_provider(&inode_id)
        .unwrap()
        .expect("previous provider is restored after failed temp close");
    assert!(same_remote_provider(&current, &previous));
    fs_b.release(&context(), writer).unwrap();
}

#[test]
fn remote_release_ack_loss_is_retained_and_drained_idempotently() {
    let (_temp_a, meta, fs_a_raw) = test_fs();
    let fs_a = Arc::new(fs_a_raw);
    let created = fs_a
        .create(
            &context(),
            fs_a.root_inode(),
            OsStr::new("release-ack-loss.bin"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    fs_a.release(&context(), created.handle).unwrap();
    let temp_b = tempfile::tempdir().unwrap();
    let chunks_b = Arc::new(LocalChunkStore::open(temp_b.path(), "node-b").unwrap());
    let read_engine_b = Arc::new(crate::node::read::engine::DfsReadEngine::new(
        NamespaceId::new("default"),
        "node-b".into(),
        chunks_b.clone(),
        Arc::new(crate::node::read::engine::UnimplementedReadSourceProvider),
        Arc::new(crate::node::read::engine::UnimplementedChunkTransfer),
        crate::node::read::engine::DfsReadConfig::default(),
    ));
    let loopback = Arc::new(LoopbackRemoteDfsOwner {
        owner: fs_a.clone(),
        peer: "node-b".into(),
    }) as Arc<dyn RemoteDfsOwner>;
    let scripted = Arc::new(ScriptedReleaseRemoteDfsOwner::new(
        loopback,
        vec![ScriptedReleaseAction::DelegateThenErr(unavailable(
            "injected ACK loss",
        ))],
    ));
    let remote_owner = scripted.clone() as Arc<dyn RemoteDfsOwner>;
    let fs_b = test_dfs_new(
        NamespaceId::new("default"),
        "node-b",
        "session-b",
        meta.clone(),
        chunks_b,
        read_engine_b,
    )
    .with_remote_owner_factory(Arc::new(StaticRemoteOwnerFactory {
        owner: remote_owner,
    }));
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    let inode_b = fs_b.backend_inode(&inode_id).unwrap();
    let writer = fs_b.open(&context(), inode_b, libc::O_RDWR).unwrap();

    fs_b.release(&context(), writer)
        .expect_err("lost release ACK is reported once");
    assert_eq!(fs_b.pending_remote_release_count().unwrap(), 1);
    assert!(fs_a.handles.is_empty());
    assert_eq!(fs_b.retry_pending_remote_releases().unwrap(), 1);
    assert_eq!(fs_b.pending_remote_release_count().unwrap(), 0);
    assert_eq!(scripted.release_call_count(), 2);
}

#[test]
fn remote_release_before_apply_failure_is_retained_and_later_drained() {
    let (_temp_a, meta, fs_a_raw) = test_fs();
    let fs_a = Arc::new(fs_a_raw);
    let created = fs_a
        .create(
            &context(),
            fs_a.root_inode(),
            OsStr::new("release-before-apply.bin"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    fs_a.release(&context(), created.handle).unwrap();
    let temp_b = tempfile::tempdir().unwrap();
    let chunks_b = Arc::new(LocalChunkStore::open(temp_b.path(), "node-b").unwrap());
    let read_engine_b = Arc::new(crate::node::read::engine::DfsReadEngine::new(
        NamespaceId::new("default"),
        "node-b".into(),
        chunks_b.clone(),
        Arc::new(crate::node::read::engine::UnimplementedReadSourceProvider),
        Arc::new(crate::node::read::engine::UnimplementedChunkTransfer),
        crate::node::read::engine::DfsReadConfig::default(),
    ));
    let loopback = Arc::new(LoopbackRemoteDfsOwner {
        owner: fs_a.clone(),
        peer: "node-b".into(),
    }) as Arc<dyn RemoteDfsOwner>;
    let scripted = Arc::new(ScriptedReleaseRemoteDfsOwner::new(
        loopback,
        vec![ScriptedReleaseAction::ErrWithoutDelegate(unavailable(
            "injected before apply",
        ))],
    ));
    let remote_owner = scripted.clone() as Arc<dyn RemoteDfsOwner>;
    let fs_b = test_dfs_new(
        NamespaceId::new("default"),
        "node-b",
        "session-b",
        meta.clone(),
        chunks_b,
        read_engine_b,
    )
    .with_remote_owner_factory(Arc::new(StaticRemoteOwnerFactory {
        owner: remote_owner,
    }));
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    let inode_b = fs_b.backend_inode(&inode_id).unwrap();
    let writer = fs_b.open(&context(), inode_b, libc::O_RDWR).unwrap();

    fs_b.release(&context(), writer)
        .expect_err("pre-apply release failure is reported");
    assert_eq!(fs_b.pending_remote_release_count().unwrap(), 1);
    assert_eq!(fs_a.handles.len(), 1);
    assert_eq!(fs_b.retry_pending_remote_releases().unwrap(), 1);
    assert_eq!(fs_b.pending_remote_release_count().unwrap(), 0);
    assert!(fs_a.handles.is_empty());
}

#[test]
fn pending_remote_release_retry_pass_attempts_each_existing_handle_once() {
    let (_temp, meta, fs) = test_fs();
    meta.set_node_session("node-b", Some("owner-session-b"));
    let scripted = Arc::new(ScriptedReleaseRemoteDfsOwner::new(
        Arc::new(NoopRemoteDfsOwner),
        vec![
            ScriptedReleaseAction::ErrWithoutDelegate(unavailable("release one fails")),
            ScriptedReleaseAction::ErrWithoutDelegate(unavailable("release two fails")),
        ],
    ));
    let owner = scripted.clone() as Arc<dyn RemoteDfsOwner>;
    let first = afs_protocol::node_control::DfsOwnerHandle {
        namespace_id: "default".into(),
        inode_id: "inode:pending-release-one".into(),
        owner_node_id: "node-b".into(),
        owner_session_id: "owner-session-b".into(),
        lease_epoch: 7,
        caller_node_id: "node-a".into(),
        caller_session_id: "caller-session-a".into(),
        open_seq: 1,
        opaque_handle: b"release-one".to_vec(),
    };
    let second = afs_protocol::node_control::DfsOwnerHandle {
        namespace_id: "default".into(),
        inode_id: "inode:pending-release-two".into(),
        owner_node_id: "node-b".into(),
        owner_session_id: "owner-session-b".into(),
        lease_epoch: 7,
        caller_node_id: "node-a".into(),
        caller_session_id: "caller-session-a".into(),
        open_seq: 2,
        opaque_handle: b"release-two".to_vec(),
    };
    fs.write_coordinator
        .queue_pending_remote_release(owner.clone(), first.clone(), false)
        .unwrap();
    fs.write_coordinator
        .queue_pending_remote_release(owner, second.clone(), false)
        .unwrap();

    fs.retry_pending_remote_releases()
        .expect_err("retryable release failures remain queued");

    assert_eq!(
        scripted.release_call_count(),
        2,
        "one maintenance pass must not retry the same queued release more than once"
    );
    assert_eq!(fs.pending_remote_release_count().unwrap(), 2);
    let state = fs.write_coordinator.pending_remote_releases.lock().unwrap();
    assert!(state.entries.contains_key(&remote_release_key(&first)));
    assert!(state.entries.contains_key(&remote_release_key(&second)));
}

#[test]
fn pending_remote_release_retry_pass_caps_attempts_and_rotates_fairly() {
    let (_temp, meta, fs) = test_fs();
    meta.set_node_session("node-b", Some("owner-session-b"));
    let scripted = Arc::new(ScriptedReleaseRemoteDfsOwner::new(
        Arc::new(NoopRemoteDfsOwner),
        (0..65)
            .map(|index| {
                ScriptedReleaseAction::ErrWithoutDelegate(unavailable(format!(
                    "release {index} fails"
                )))
            })
            .collect(),
    ));
    let owner = scripted.clone() as Arc<dyn RemoteDfsOwner>;
    let handles: Vec<_> = (0..65)
        .map(|index| afs_protocol::node_control::DfsOwnerHandle {
            namespace_id: "default".into(),
            inode_id: format!("inode:pending-release-{index:02}"),
            owner_node_id: "node-b".into(),
            owner_session_id: "owner-session-b".into(),
            lease_epoch: 7,
            caller_node_id: "node-a".into(),
            caller_session_id: "caller-session-a".into(),
            open_seq: index + 1,
            opaque_handle: vec![index as u8],
        })
        .collect();
    for handle in &handles {
        fs.write_coordinator
            .queue_pending_remote_release(owner.clone(), handle.clone(), false)
            .unwrap();
    }

    fs.retry_pending_remote_releases()
        .expect_err("retryable release failures remain queued");

    let first_pass = scripted.release_opaque_handles();
    assert_eq!(
        first_pass,
        (0..64).map(|index| vec![index as u8]).collect::<Vec<_>>(),
        "one maintenance pass tries only the first 64 queued handles once"
    );
    assert_eq!(fs.pending_remote_release_count().unwrap(), 65);

    fs.retry_pending_remote_releases()
        .expect_err("retryable release failures remain queued");

    let calls = scripted.release_opaque_handles();
    assert_eq!(
        calls.get(64),
        Some(&vec![64]),
        "the next pass starts with the previous pass's unattempted tail handle"
    );
    assert_eq!(fs.pending_remote_release_count().unwrap(), 65);
}

#[test]
fn pending_remote_release_retires_replaced_or_absent_owner_session() {
    let (_temp, meta, fs) = test_fs();
    meta.set_node_session("node-b", Some("session-new"));
    meta.set_node_session("node-c", None);
    let scripted = Arc::new(ScriptedReleaseRemoteDfsOwner::new(
        Arc::new(NoopRemoteDfsOwner),
        Vec::new(),
    ));
    let owner = scripted.clone() as Arc<dyn RemoteDfsOwner>;
    let replaced = afs_protocol::node_control::DfsOwnerHandle {
        namespace_id: "default".into(),
        inode_id: "inode:pending-retired-replaced".into(),
        owner_node_id: "node-b".into(),
        owner_session_id: "session-old".into(),
        lease_epoch: 7,
        caller_node_id: "node-a".into(),
        caller_session_id: "caller-session-a".into(),
        open_seq: 1,
        opaque_handle: b"retired-replaced".to_vec(),
    };
    let absent = afs_protocol::node_control::DfsOwnerHandle {
        namespace_id: "default".into(),
        inode_id: "inode:pending-retired-absent".into(),
        owner_node_id: "node-c".into(),
        owner_session_id: "session-old".into(),
        lease_epoch: 7,
        caller_node_id: "node-a".into(),
        caller_session_id: "caller-session-a".into(),
        open_seq: 2,
        opaque_handle: b"retired-absent".to_vec(),
    };
    fs.write_coordinator
        .queue_pending_remote_release(owner.clone(), replaced, false)
        .unwrap();
    fs.write_coordinator
        .queue_pending_remote_release(owner, absent, false)
        .unwrap();

    assert_eq!(fs.retry_pending_remote_releases().unwrap(), 2);

    assert_eq!(fs.pending_remote_release_count().unwrap(), 0);
    assert_eq!(scripted.release_call_count(), 0);
    assert_eq!(meta.current_session_call_count(), 2);
}

#[test]
fn pending_remote_release_unknown_owner_session_retains_debt() {
    let (_temp, meta, fs) = test_fs();
    meta.fail_next_current_session_with(unavailable("injected session lookup failure"));
    let scripted = Arc::new(ScriptedReleaseRemoteDfsOwner::new(
        Arc::new(NoopRemoteDfsOwner),
        Vec::new(),
    ));
    let owner = scripted.clone() as Arc<dyn RemoteDfsOwner>;
    let handle = afs_protocol::node_control::DfsOwnerHandle {
        namespace_id: "default".into(),
        inode_id: "inode:pending-unknown-session".into(),
        owner_node_id: "node-b".into(),
        owner_session_id: "session-b".into(),
        lease_epoch: 7,
        caller_node_id: "node-a".into(),
        caller_session_id: "caller-session-a".into(),
        open_seq: 1,
        opaque_handle: b"unknown-session".to_vec(),
    };
    fs.write_coordinator
        .queue_pending_remote_release(owner, handle.clone(), false)
        .unwrap();

    fs.retry_pending_remote_releases()
        .expect_err("unknown owner process session keeps exact cleanup debt");

    assert_eq!(fs.pending_remote_release_count().unwrap(), 1);
    assert_eq!(scripted.release_call_count(), 0);
    assert_eq!(meta.current_session_call_count(), 1);
    let state = fs.write_coordinator.pending_remote_releases.lock().unwrap();
    assert!(state.entries.contains_key(&remote_release_key(&handle)));
}

#[test]
fn pending_remote_release_mixed_sessions_retire_old_and_release_same_session() {
    let (_temp, meta, fs) = test_fs();
    meta.set_node_session("node-b", Some("session-live"));
    let scripted = Arc::new(ScriptedReleaseRemoteDfsOwner::new(
        Arc::new(NoopRemoteDfsOwner),
        vec![ScriptedReleaseAction::OkWithoutDelegate],
    ));
    let owner = scripted.clone() as Arc<dyn RemoteDfsOwner>;
    let old = afs_protocol::node_control::DfsOwnerHandle {
        namespace_id: "default".into(),
        inode_id: "inode:pending-old-session".into(),
        owner_node_id: "node-b".into(),
        owner_session_id: "session-old".into(),
        lease_epoch: 7,
        caller_node_id: "node-a".into(),
        caller_session_id: "caller-session-a".into(),
        open_seq: 1,
        opaque_handle: b"old-session".to_vec(),
    };
    let live = afs_protocol::node_control::DfsOwnerHandle {
        namespace_id: "default".into(),
        inode_id: "inode:pending-live-session".into(),
        owner_node_id: "node-b".into(),
        owner_session_id: "session-live".into(),
        lease_epoch: 7,
        caller_node_id: "node-a".into(),
        caller_session_id: "caller-session-a".into(),
        open_seq: 2,
        opaque_handle: b"live-session".to_vec(),
    };
    fs.write_coordinator
        .queue_pending_remote_release(owner.clone(), old, false)
        .unwrap();
    fs.write_coordinator
        .queue_pending_remote_release(owner, live, false)
        .unwrap();

    assert_eq!(fs.retry_pending_remote_releases().unwrap(), 2);

    assert_eq!(fs.pending_remote_release_count().unwrap(), 0);
    assert_eq!(scripted.release_call_count(), 1);
    assert_eq!(
        scripted.release_opaque_handles(),
        vec![b"live-session".to_vec()]
    );
    assert_eq!(meta.current_session_call_count(), 1);
}

#[test]
fn pending_remote_release_groups_retired_session_lookup_and_preserves_fair_cap() {
    let (_temp, meta, fs) = test_fs();
    meta.set_node_session("node-b", Some("session-new"));
    let scripted = Arc::new(ScriptedReleaseRemoteDfsOwner::new(
        Arc::new(NoopRemoteDfsOwner),
        Vec::new(),
    ));
    let owner = scripted.clone() as Arc<dyn RemoteDfsOwner>;
    for index in 0..65 {
        let handle = afs_protocol::node_control::DfsOwnerHandle {
            namespace_id: "default".into(),
            inode_id: format!("inode:pending-retired-group-{index:02}"),
            owner_node_id: "node-b".into(),
            owner_session_id: "session-old".into(),
            lease_epoch: 7,
            caller_node_id: "node-a".into(),
            caller_session_id: "caller-session-a".into(),
            open_seq: index + 1,
            opaque_handle: vec![index as u8],
        };
        fs.write_coordinator
            .queue_pending_remote_release(owner.clone(), handle, false)
            .unwrap();
    }

    fs.retry_pending_remote_releases()
        .expect_err("one fair pass retires only the first 64 queued entries");

    assert_eq!(fs.pending_remote_release_count().unwrap(), 1);
    assert_eq!(scripted.release_call_count(), 0);
    assert_eq!(meta.current_session_call_count(), 1);

    assert_eq!(fs.retry_pending_remote_releases().unwrap(), 1);
    assert_eq!(fs.pending_remote_release_count().unwrap(), 0);
    assert_eq!(scripted.release_call_count(), 0);
    assert_eq!(meta.current_session_call_count(), 2);
}

#[test]
fn remote_release_capacity_rejects_new_admission_without_owner_open() {
    let (_temp_a, meta, fs_a_raw) = test_fs();
    let fs_a = Arc::new(fs_a_raw);
    let created = fs_a
        .create(
            &context(),
            fs_a.root_inode(),
            OsStr::new("release-capacity.bin"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    fs_a.release(&context(), created.handle).unwrap();
    let temp_b = tempfile::tempdir().unwrap();
    let chunks_b = Arc::new(LocalChunkStore::open(temp_b.path(), "node-b").unwrap());
    let read_engine_b = Arc::new(crate::node::read::engine::DfsReadEngine::new(
        NamespaceId::new("default"),
        "node-b".into(),
        chunks_b.clone(),
        Arc::new(crate::node::read::engine::UnimplementedReadSourceProvider),
        Arc::new(crate::node::read::engine::UnimplementedChunkTransfer),
        crate::node::read::engine::DfsReadConfig::default(),
    ));
    let remote_owner = Arc::new(LoopbackRemoteDfsOwner {
        owner: fs_a.clone(),
        peer: "node-b".into(),
    }) as Arc<dyn RemoteDfsOwner>;
    let fs_b = test_dfs_new(
        NamespaceId::new("default"),
        "node-b",
        "session-b",
        meta.clone(),
        chunks_b,
        read_engine_b,
    )
    .with_remote_owner_factory(Arc::new(StaticRemoteOwnerFactory {
        owner: remote_owner,
    }));
    fs_b.write_coordinator
        .pending_remote_releases
        .lock()
        .unwrap()
        .reserved = MAX_PENDING_REMOTE_RELEASES;
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    let inode_b = fs_b.backend_inode(&inode_id).unwrap();
    let error = fs_b.open(&context(), inode_b, libc::O_RDWR).unwrap_err();
    assert_eq!(error.code(), afs_error::NODE_VFS_UNAVAILABLE);
    assert!(fs_a.handles.is_empty());
}
