use super::*;

#[test]
fn owner_open_open_write_failure_clears_inflight_admission() {
    let (_temp, meta, fs) = test_fs();
    meta.fail_next_open_write_with(unavailable("injected open_write failure"));
    let error = <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::open(
        &fs,
        "node-b",
        dfs_owner_open_request(1, libc::O_RDWR),
    )
    .unwrap_err();
    assert_eq!(error.code(), afs_error::NODE_VFS_UNAVAILABLE);
    let routes = fs.owner_open_routes.lock().unwrap();
    let state = routes.values().next().unwrap();
    assert_eq!(state.highwater, 1);
    assert!(state.inflight.is_empty());
    assert!(state.cancelled.is_empty());
    assert!(state.active.is_empty());
}

#[test]
fn owner_open_resolves_fresh_authority_without_legacy_open_write() {
    let (_temp, meta, fs) = test_fs();
    let opens_before = meta.open_write_calls.load(Ordering::SeqCst);
    let resolve_before = meta.resolve_write_calls.load(Ordering::SeqCst);

    let reply = <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::open(
        &fs,
        "node-b",
        dfs_owner_open_request(1, libc::O_RDWR),
    )
    .unwrap();
    assert!(reply.handle.is_some());

    assert_eq!(
        meta.open_write_calls.load(Ordering::SeqCst),
        opens_before,
        "remote owner admission must not invoke legacy OpenWrite"
    );
    assert_eq!(
        meta.resolve_write_calls.load(Ordering::SeqCst),
        resolve_before + 1,
        "remote owner admission must resolve fresh write authority"
    );
}

#[test]
fn owner_open_rejects_stale_request_epoch_after_fresh_write_authority_reacquire() {
    let (_temp, meta, fs) = test_fs();
    {
        let mut lease = meta.lease.lock().unwrap();
        lease.lease_epoch = lease.lease_epoch.saturating_add(1);
        lease.expires_at_unix_ms = now_unix_ms().saturating_add(30_000);
    }

    let error = <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::open(
            &fs,
            "node-b",
            dfs_owner_open_request(1, libc::O_RDWR),
        )
        .expect_err("stale remote owner open request must not succeed after Meta reacquires a newer lease epoch");

    assert_eq!(error.code(), afs_error::NODE_DFS_STALE_HANDLE);
    let routes = fs.owner_open_routes.lock().unwrap();
    let state = routes.values().next().unwrap();
    assert_eq!(state.highwater, 1);
    assert!(state.inflight.is_empty());
    assert!(state.cancelled.is_empty());
    assert!(state.active.is_empty());
    assert!(fs.handles.is_empty());
}

#[test]
fn owner_open_rejects_epoch_changed_by_legacy_reacquire_after_resolver_reply() {
    let (_temp, meta, fs) = test_fs();
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    let near_expiry = now_unix_ms().saturating_add(1_000);
    let inode = meta.inode.lock().unwrap().clone();
    let mut epoch_two = {
        let mut lease = meta.lease.lock().unwrap();
        lease.lease_epoch = 1;
        lease.expires_at_unix_ms = near_expiry;
        lease.clone()
    };
    epoch_two.lease_epoch = 2;
    epoch_two.expires_at_unix_ms = now_unix_ms().saturating_add(30_000);
    meta.queue_open_write_result(Ok((inode, epoch_two)));

    let error = <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::open(
            &fs,
            "node-b",
            dfs_owner_open_request(1, libc::O_RDWR),
        )
        .expect_err("owner open must not succeed after legacy OpenWrite changes the lease epoch behind the resolver reply");

    assert_eq!(error.code(), afs_error::NODE_DFS_STALE_HANDLE);
    assert_eq!(meta.resolve_write_calls.load(Ordering::SeqCst), 1);
    assert_eq!(meta.open_write_calls.load(Ordering::SeqCst), 1);
    let routes = fs.owner_open_routes.lock().unwrap();
    let state = routes.values().next().unwrap();
    assert_eq!(state.highwater, 1);
    assert!(state.inflight.is_empty());
    assert!(state.cancelled.is_empty());
    assert!(state.active.is_empty());
    drop(routes);
    assert!(fs.handles.is_empty());
    let state = fs.write_state(&inode_id).unwrap().unwrap();
    assert_eq!(state.lock().unwrap().open_writers, 0);

    let replay = <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::open(
        &fs,
        "node-b",
        dfs_owner_open_request(1, libc::O_RDWR),
    )
    .expect_err("failed owner admission sequence must be retired");
    assert_eq!(replay.code(), afs_error::NODE_DFS_STALE_HANDLE);
    assert!(fs.handles.is_empty());
}

#[test]
fn owner_open_validate_inode_failure_clears_inflight_admission() {
    let (_temp, meta, fs) = test_fs();
    meta.return_bad_namespace_on_next_open_write();
    let error = <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::open(
        &fs,
        "node-b",
        dfs_owner_open_request(1, libc::O_RDWR),
    )
    .unwrap_err();
    assert_eq!(error.code(), afs_error::NODE_VFS_INVALID);
    let routes = fs.owner_open_routes.lock().unwrap();
    let state = routes.values().next().unwrap();
    assert_eq!(state.highwater, 1);
    assert!(state.inflight.is_empty());
    assert!(state.cancelled.is_empty());
    assert!(state.active.is_empty());
}

#[test]
fn owner_open_replay_returns_same_handle_and_changed_fingerprint_is_rejected() {
    let (_temp, _meta, fs) = test_fs();
    let request = dfs_owner_open_request(1, libc::O_RDWR);
    let first = <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::open(
        &fs,
        "node-b",
        request.clone(),
    )
    .unwrap()
    .handle
    .unwrap();
    let replay = <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::open(
        &fs, "node-b", request,
    )
    .unwrap()
    .handle
    .unwrap();
    assert_eq!(replay.opaque_handle, first.opaque_handle);
    assert_eq!(fs.handles.len(), 1);

    let changed = <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::open(
        &fs,
        "node-b",
        dfs_owner_open_request(1, libc::O_WRONLY),
    )
    .unwrap_err();
    assert_eq!(changed.code(), afs_error::NODE_DFS_STALE_HANDLE);
    assert_eq!(fs.handles.len(), 1);
}

#[test]
fn owner_open_cleanup_identity_cancels_late_open_without_allocation() {
    let (_temp, _meta, fs) = test_fs();
    <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::release(
        &fs,
        "node-b",
        afs_protocol::node_control::DfsOwnerReleaseRequest {
            handle: Some(dfs_owner_cleanup_handle(1)),
        },
    )
    .unwrap();

    let late = <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::open(
        &fs,
        "node-b",
        dfs_owner_open_request(1, libc::O_RDWR),
    )
    .unwrap_err();
    assert_eq!(late.code(), afs_error::NODE_DFS_STALE_HANDLE);
    assert!(fs.handles.is_empty());

    let phantom = DistributedFs::data_owner_handle(&dfs_owner_cleanup_handle(2));
    let read = <DistributedFs as crate::node::rpc::data::DfsOwnerFilesHandler>::read(
        &fs,
        "node-b",
        afs_protocol::node_data::DfsOwnerReadRequest {
            handle: Some(phantom),
            offset: 0,
            length: 1,
        },
    )
    .unwrap_err();
    assert_eq!(read.code(), afs_error::NODE_DFS_STALE_HANDLE);
}

#[test]
fn long_lived_low_open_sequence_does_not_block_later_admissions() {
    let (_temp, _meta, fs) = test_fs();
    for seq in 1..=5000 {
        <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::open(
            &fs,
            "node-b",
            dfs_owner_open_request(seq, libc::O_RDWR),
        )
        .unwrap();
    }
    assert_eq!(fs.handles.len(), 5000);
}

#[test]
fn owner_open_unknown_cleanup_advances_highwater_without_tombstone_growth() {
    let (_temp, _meta, fs) = test_fs();
    for seq in 1..=MAX_PENDING_REMOTE_RELEASES as u64 + 10 {
        <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::release(
            &fs,
            "node-b",
            afs_protocol::node_control::DfsOwnerReleaseRequest {
                handle: Some(dfs_owner_cleanup_handle(seq)),
            },
        )
        .unwrap();
    }
    let routes = fs.owner_open_routes.lock().unwrap();
    let state = routes.values().next().unwrap();
    assert_eq!(state.cancelled.len(), 0);
    assert_eq!(state.inflight.len(), 0);
    assert_eq!(state.highwater, MAX_PENDING_REMOTE_RELEASES as u64 + 10);
    drop(routes);
    let old = <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::open(
        &fs,
        "node-b",
        dfs_owner_open_request(1, libc::O_RDWR),
    )
    .unwrap_err();
    assert_eq!(old.code(), afs_error::NODE_DFS_STALE_HANDLE);
    <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::open(
        &fs,
        "node-b",
        dfs_owner_open_request(MAX_PENDING_REMOTE_RELEASES as u64 + 11, libc::O_RDWR),
    )
    .unwrap();
}

#[test]
fn owner_open_inflight_cleanup_wrong_scope_does_not_cancel() {
    let (_temp, _meta, fs) = test_fs();
    let request = dfs_owner_open_request(1, libc::O_RDWR);
    let admission = fs.start_owner_open_admission("node-b", &request).unwrap();
    assert!(matches!(
        admission,
        DfsOwnerOpenAdmissionStart::Apply { .. }
    ));
    let mut wrong = dfs_owner_cleanup_handle(1);
    wrong.inode_id = "inode:wrong".into();
    let error = <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::release(
        &fs,
        "node-b",
        afs_protocol::node_control::DfsOwnerReleaseRequest {
            handle: Some(wrong),
        },
    )
    .unwrap_err();
    assert_eq!(error.code(), afs_error::NODE_DFS_STALE_HANDLE);
    let routes = fs.owner_open_routes.lock().unwrap();
    let state = routes.values().next().unwrap();
    assert_eq!(state.cancelled.len(), 0);
    assert_eq!(state.inflight.len(), 1);
    drop(routes);
    <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::release(
        &fs,
        "node-b",
        afs_protocol::node_control::DfsOwnerReleaseRequest {
            handle: Some(dfs_owner_cleanup_handle(1)),
        },
    )
    .unwrap();
    let routes = fs.owner_open_routes.lock().unwrap();
    let state = routes.values().next().unwrap();
    assert_eq!(state.cancelled.len(), 1);
}

#[test]
fn owner_open_cleanup_wrong_scope_does_not_close_active_handle() {
    let (_temp, _meta, fs) = test_fs();
    let request = dfs_owner_open_request(1, libc::O_RDWR);
    let handle = <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::open(
        &fs,
        "node-b",
        request.clone(),
    )
    .unwrap()
    .handle
    .unwrap();
    let mut wrong = dfs_owner_cleanup_handle(1);
    wrong.inode_id = "inode:wrong".into();
    let error = <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::release(
        &fs,
        "node-b",
        afs_protocol::node_control::DfsOwnerReleaseRequest {
            handle: Some(wrong),
        },
    )
    .unwrap_err();
    assert_eq!(error.code(), afs_error::NODE_DFS_STALE_HANDLE);
    assert_eq!(fs.handles.len(), 1);
    let replay = <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::open(
        &fs, "node-b", request,
    )
    .unwrap()
    .handle
    .unwrap();
    assert_eq!(replay.opaque_handle, handle.opaque_handle);
}

#[test]
fn owner_open_cleanup_release_failure_retains_active_identity() {
    let (_temp, _meta, fs) = test_fs();
    let request = dfs_owner_open_request(1, libc::O_RDWR);
    let handle = <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::open(
        &fs,
        "node-b",
        request.clone(),
    )
    .unwrap()
    .handle
    .unwrap();
    fs.handles.clear();
    let error = <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::release(
        &fs,
        "node-b",
        afs_protocol::node_control::DfsOwnerReleaseRequest {
            handle: Some(dfs_owner_cleanup_handle(1)),
        },
    )
    .unwrap_err();
    assert_eq!(error.code(), afs_error::NODE_DFS_STALE_HANDLE);
    let replay = <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::open(
        &fs, "node-b", request,
    )
    .unwrap()
    .handle
    .unwrap();
    assert_eq!(replay.opaque_handle, handle.opaque_handle);
}

#[test]
fn owner_open_replay_state_retires_with_authoritative_caller_session() {
    let (_temp, meta, fs) = test_fs();
    <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::release(
        &fs,
        "node-b",
        afs_protocol::node_control::DfsOwnerReleaseRequest {
            handle: Some(dfs_owner_cleanup_handle(1)),
        },
    )
    .unwrap();
    assert_eq!(fs.owner_open_routes.lock().unwrap().len(), 1);
    meta.set_node_session("node-b", Some("session-b2"));
    assert_eq!(fs.reap_expired_peer_owner_handles().unwrap(), 0);
    assert!(fs.owner_open_routes.lock().unwrap().is_empty());
    let stale = <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::release(
        &fs,
        "node-b",
        afs_protocol::node_control::DfsOwnerReleaseRequest {
            handle: Some(dfs_owner_cleanup_handle(2)),
        },
    )
    .unwrap_err();
    assert_eq!(stale.code(), afs_error::NODE_DFS_STALE_HANDLE);
    assert!(fs.owner_open_routes.lock().unwrap().is_empty());
}

#[test]
fn remote_open_admission_lock_cleanup_keeps_reused_lock_with_users() {
    let (_temp, _meta, fs) = test_fs();
    let route = DfsOwnerOpenRouteKey {
        caller_node_id: "node-b".into(),
        caller_session_id: "session-b".into(),
        owner_node_id: "node-a".into(),
        owner_session_id: "session-a".into(),
    };
    let lock = Arc::new(DfsRemoteOpenAdmissionLock {
        gate: Mutex::new(()),
        users: AtomicUsize::new(1),
    });
    fs.remote_open_admission_locks
        .lock()
        .unwrap()
        .insert(route.clone(), lock.clone());
    fs.cleanup_remote_open_admission_lock(&route, &lock)
        .unwrap();
    assert!(
        fs.remote_open_admission_locks
            .lock()
            .unwrap()
            .contains_key(&route)
    );
    lock.users.store(0, Ordering::Release);
    fs.cleanup_remote_open_admission_lock(&route, &lock)
        .unwrap();
    assert!(fs.remote_open_admission_locks.lock().unwrap().is_empty());
}

#[test]
fn caller_serializes_same_route_open_admission_until_ack() {
    let (_temp_a, meta, fs_a_raw) = test_fs();
    let fs_a = Arc::new(fs_a_raw);
    let created = fs_a
        .create(
            &context(),
            fs_a.root_inode(),
            OsStr::new("serialized-open-admission.bin"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    fs_a.release(&context(), created.handle).unwrap();
    let temp_b = tempfile::tempdir().unwrap();
    let chunks_b = Arc::new(LocalChunkStore::open(temp_b.path(), "node-b").unwrap());
    let read_engine_b = Arc::new(crate::node::dfs_read::DfsReadEngine::new(
        NamespaceId::new("default"),
        "node-b".into(),
        chunks_b.clone(),
        Arc::new(crate::node::dfs_read::UnimplementedReadSourceProvider),
        Arc::new(crate::node::dfs_read::UnimplementedChunkTransfer),
        crate::node::dfs_read::DfsReadConfig::default(),
    ));
    let loopback = Arc::new(LoopbackRemoteDfsOwner {
        owner: fs_a.clone(),
        peer: "node-b".into(),
    }) as Arc<dyn RemoteDfsOwner>;
    let (first_entered_tx, first_entered_rx) = std::sync::mpsc::channel();
    let (release_first_tx, release_first_rx) = std::sync::mpsc::channel();
    let (second_entered_tx, second_entered_rx) = std::sync::mpsc::channel();
    let blocking = Arc::new(BlockingFirstOpenRemoteDfsOwner::new(
        loopback,
        first_entered_tx,
        release_first_rx,
        second_entered_tx,
    ));
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
            owner: blocking.clone() as Arc<dyn RemoteDfsOwner>,
        })),
    );
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    let inode_b = fs_b.backend_inode(&inode_id).unwrap();
    let fs_first = fs_b.clone();
    let first_inode = inode_b;
    let first = std::thread::spawn(move || fs_first.open(&context(), first_inode, libc::O_RDWR));
    first_entered_rx.recv().unwrap();
    let fs_second = fs_b.clone();
    let second_inode = inode_b;
    let second = std::thread::spawn(move || fs_second.open(&context(), second_inode, libc::O_RDWR));
    assert!(
        second_entered_rx
            .recv_timeout(std::time::Duration::from_millis(50))
            .is_err()
    );
    release_first_tx.send(()).unwrap();
    first.join().unwrap().unwrap();
    second.join().unwrap().unwrap();
    let requests = blocking.open_requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].open_seq, 1);
    assert_eq!(requests[1].open_seq, 2);
    assert!(fs_b.remote_open_admission_locks.lock().unwrap().is_empty());
}

#[test]
fn owner_open_finish_after_session_reaper_cleans_allocated_handle() {
    let (_temp, meta, fs_raw) = test_fs();
    let fs = Arc::new(fs_raw);
    let pause = Arc::new(OwnerOpenFinishPause::new());
    fs.set_owner_open_finish_pause(Some(pause.clone()));
    let fs_open = fs.clone();
    let open = std::thread::spawn(move || {
        <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::open(
            fs_open.as_ref(),
            "node-b",
            dfs_owner_open_request(1, libc::O_RDWR),
        )
    });
    pause.wait_until_reached();
    assert_eq!(fs.handles.len(), 1);
    meta.set_node_session("node-b", Some("session-b2"));
    assert_eq!(fs.reap_expired_peer_owner_handles().unwrap(), 1);
    assert!(fs.handles.is_empty());
    pause.resume();
    let error = open.join().unwrap().unwrap_err();
    assert_eq!(error.code(), afs_error::NODE_DFS_STALE_HANDLE);
    assert!(fs.handles.is_empty());
    assert!(fs.owner_open_routes.lock().unwrap().is_empty());
    fs.set_owner_open_finish_pause(None);
}

#[test]
fn remote_open_ack_loss_queues_cleanup_identity_and_late_replay_does_not_reopen() {
    let (_temp_a, meta, fs_a_raw) = test_fs();
    let fs_a = Arc::new(fs_a_raw);
    let created = fs_a
        .create(
            &context(),
            fs_a.root_inode(),
            OsStr::new("open-ack-loss.bin"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    fs_a.release(&context(), created.handle).unwrap();
    let temp_b = tempfile::tempdir().unwrap();
    let chunks_b = Arc::new(LocalChunkStore::open(temp_b.path(), "node-b").unwrap());
    let read_engine_b = Arc::new(crate::node::dfs_read::DfsReadEngine::new(
        NamespaceId::new("default"),
        "node-b".into(),
        chunks_b.clone(),
        Arc::new(crate::node::dfs_read::UnimplementedReadSourceProvider),
        Arc::new(crate::node::dfs_read::UnimplementedChunkTransfer),
        crate::node::dfs_read::DfsReadConfig::default(),
    ));
    let loopback = Arc::new(LoopbackRemoteDfsOwner {
        owner: fs_a.clone(),
        peer: "node-b".into(),
    }) as Arc<dyn RemoteDfsOwner>;
    let scripted = Arc::new(ScriptedOpenRemoteDfsOwner::new(
        loopback,
        vec![ScriptedOpenAction::DelegateThenErr(unavailable(
            "injected open ACK loss",
        ))],
    ));
    let fs_b = test_dfs_new(
        NamespaceId::new("default"),
        "node-b",
        "session-b",
        meta.clone(),
        chunks_b,
        read_engine_b,
    )
    .with_remote_owner_factory(Arc::new(StaticRemoteOwnerFactory {
        owner: scripted.clone() as Arc<dyn RemoteDfsOwner>,
    }));
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    let inode_b = fs_b.backend_inode(&inode_id).unwrap();

    let error = fs_b.open(&context(), inode_b, libc::O_RDWR).unwrap_err();
    assert_eq!(error.code(), afs_error::NODE_VFS_UNAVAILABLE);
    assert_eq!(fs_b.pending_remote_release_count().unwrap(), 1);
    assert_eq!(fs_a.handles.len(), 1);
    assert_eq!(fs_b.retry_pending_remote_releases().unwrap(), 1);
    assert!(fs_a.handles.is_empty());

    let late = scripted.open_requests()[0].clone();
    let late = scripted.open(late).unwrap_err();
    assert_eq!(late.code(), afs_error::NODE_DFS_STALE_HANDLE);
    assert!(fs_a.handles.is_empty());
}

#[test]
fn writeonly_read_companion_ack_loss_has_independent_cleanup_identity() {
    let (_temp_a, meta, fs_a_raw) = test_fs();
    let fs_a = Arc::new(fs_a_raw);
    let created = fs_a
        .create(
            &context(),
            fs_a.root_inode(),
            OsStr::new("writeonly-companion-ack-loss.bin"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    fs_a.release(&context(), created.handle).unwrap();
    let temp_b = tempfile::tempdir().unwrap();
    let chunks_b = Arc::new(LocalChunkStore::open(temp_b.path(), "node-b").unwrap());
    let read_engine_b = Arc::new(crate::node::dfs_read::DfsReadEngine::new(
        NamespaceId::new("default"),
        "node-b".into(),
        chunks_b.clone(),
        Arc::new(crate::node::dfs_read::UnimplementedReadSourceProvider),
        Arc::new(crate::node::dfs_read::UnimplementedChunkTransfer),
        crate::node::dfs_read::DfsReadConfig::default(),
    ));
    let loopback = Arc::new(LoopbackRemoteDfsOwner {
        owner: fs_a.clone(),
        peer: "node-b".into(),
    }) as Arc<dyn RemoteDfsOwner>;
    let scripted = Arc::new(ScriptedOpenRemoteDfsOwner::new(
        loopback,
        vec![
            ScriptedOpenAction::Delegate,
            ScriptedOpenAction::DelegateThenErr(unavailable("injected companion ACK loss")),
        ],
    ));
    let fs_b = test_dfs_new(
        NamespaceId::new("default"),
        "node-b",
        "session-b",
        meta.clone(),
        chunks_b,
        read_engine_b,
    )
    .with_remote_owner_factory(Arc::new(StaticRemoteOwnerFactory {
        owner: scripted.clone() as Arc<dyn RemoteDfsOwner>,
    }));
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    let inode_b = fs_b.backend_inode(&inode_id).unwrap();

    let error = fs_b.open(&context(), inode_b, libc::O_WRONLY).unwrap_err();
    assert_eq!(error.code(), afs_error::NODE_VFS_UNAVAILABLE);
    assert_eq!(scripted.open_requests().len(), 2);
    assert_eq!(fs_b.pending_remote_release_count().unwrap(), 1);
    assert_eq!(fs_a.handles.len(), 1);
    assert_eq!(fs_b.retry_pending_remote_releases().unwrap(), 1);
    assert!(fs_a.handles.is_empty());
}

#[test]
fn old_owner_lease_data_ops_are_denied_but_release_cleans_resource() {
    let (_temp, meta, fs) = test_fs();
    let open = <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::open(
        &fs,
        "node-b",
        afs_protocol::node_control::DfsOwnerOpenRequest {
            namespace_id: "default".into(),
            inode_id: "inode:test".into(),
            owner_node_id: "node-a".into(),
            owner_session_id: "session-a".into(),
            lease_epoch: 1,
            caller_session_id: "session-b".into(),
            open_flags: libc::O_RDWR,
            kill_suidgid: false,
            open_seq: 1,
        },
    )
    .unwrap();
    let handle = open.handle.unwrap();
    let inode_id = InodeId::new("inode:test");
    let state = fs.write_state(&inode_id).unwrap().unwrap();
    state.lock().unwrap().write_lease.lease_epoch = 2;
    meta.lease.lock().unwrap().lease_epoch = 2;
    let data_handle = DistributedFs::data_owner_handle(&handle);
    let error = <DistributedFs as crate::node::rpc::data::DfsOwnerFilesHandler>::write(
        &fs,
        "node-b",
        afs_protocol::node_data::DfsOwnerWriteRequest {
            handle: Some(data_handle),
            operation_id: "old-lease-write".into(),
            offset: 0,
            data: b"x".to_vec(),
            append: false,
            kill_suidgid: false,
        },
    )
    .unwrap_err();
    assert_eq!(error.code(), afs_error::NODE_DFS_STALE_HANDLE);
    <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::release(
        &fs,
        "node-b",
        afs_protocol::node_control::DfsOwnerReleaseRequest {
            handle: Some(handle),
        },
    )
    .unwrap();
    assert!(fs.handles.is_empty());
}

#[test]
fn owner_handle_reap_requires_definite_caller_session_replacement() {
    let (_temp, meta, fs) = test_fs();
    let open = <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::open(
        &fs,
        "node-b",
        afs_protocol::node_control::DfsOwnerOpenRequest {
            namespace_id: "default".into(),
            inode_id: "inode:test".into(),
            owner_node_id: "node-a".into(),
            owner_session_id: "session-a".into(),
            lease_epoch: 1,
            caller_session_id: "session-b".into(),
            open_flags: libc::O_RDWR,
            kill_suidgid: false,
            open_seq: 1,
        },
    )
    .unwrap();
    assert!(open.handle.is_some());
    meta.fail_next_current_session_with(unavailable("injected Meta session lookup failure"));
    assert!(fs.reap_expired_peer_owner_handles().is_err());
    assert_eq!(fs.handles.len(), 1);
    meta.set_node_session("node-b", Some("session-b2"));
    assert_eq!(fs.reap_expired_peer_owner_handles().unwrap(), 1);
    assert!(fs.handles.is_empty());
}

#[test]
fn wrong_peer_cannot_release_live_owner_handle() {
    let (_temp, _meta, fs) = test_fs();
    let handle = <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::open(
        &fs,
        "node-b",
        afs_protocol::node_control::DfsOwnerOpenRequest {
            namespace_id: "default".into(),
            inode_id: "inode:test".into(),
            owner_node_id: "node-a".into(),
            owner_session_id: "session-a".into(),
            lease_epoch: 1,
            caller_session_id: "session-b".into(),
            open_flags: libc::O_RDWR,
            kill_suidgid: false,
            open_seq: 1,
        },
    )
    .unwrap()
    .handle
    .unwrap();
    let error = <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::release(
        &fs,
        "node-c",
        afs_protocol::node_control::DfsOwnerReleaseRequest {
            handle: Some(handle.clone()),
        },
    )
    .unwrap_err();
    assert_eq!(error.code(), afs_error::NODE_DFS_STALE_HANDLE);
    assert_eq!(fs.handles.len(), 1);
    <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::release(
        &fs,
        "node-b",
        afs_protocol::node_control::DfsOwnerReleaseRequest {
            handle: Some(handle),
        },
    )
    .unwrap();
}

#[test]
fn writeonly_remote_provider_supports_fresh_readonly_reader() {
    let (_temp_a, meta, fs_a_raw) = test_fs();
    let fs_a = Arc::new(fs_a_raw);
    let created = fs_a
        .create(
            &context(),
            fs_a.root_inode(),
            OsStr::new("remote-provider-readonly-reader.bin"),
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
    let read_engine_b = Arc::new(crate::node::dfs_read::DfsReadEngine::new(
        NamespaceId::new("default"),
        "node-b".into(),
        chunks_b.clone(),
        Arc::new(crate::node::dfs_read::UnimplementedReadSourceProvider),
        Arc::new(crate::node::dfs_read::UnimplementedChunkTransfer),
        crate::node::dfs_read::DfsReadConfig::default(),
    ));
    let remote_owner = Arc::new(LoopbackRemoteDfsOwner {
        owner: fs_a.clone(),
        peer: "node-b".into(),
    }) as Arc<dyn RemoteDfsOwner>;
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
    let reader = fs_b.open(&context(), inode_b, libc::O_RDONLY).unwrap();
    assert!(fs_b.page_cache_identity(reader).is_none());
    assert!(!fs_b.supports_parallel_reads(reader));
    assert_eq!(
        fs_b.getattr(&context(), inode_b, Some(reader))
            .unwrap()
            .size,
        3
    );
    let mut data = [0; 3];
    assert_eq!(fs_b.read(&context(), reader, 0, &mut data).unwrap(), 3);
    assert_eq!(&data, b"XYZ");
    assert!(fs_b.read_owned(&context(), reader, 0, usize::MAX).is_err());
    for with_handle in [false, true] {
        let denial_reader = fs_b.open(&context(), inode_b, libc::O_RDONLY).unwrap();
        let before = fs_b
            .handle_snapshot(denial_reader)
            .unwrap()
            .native_denial_serial;
        meta.fail_next_current_session_with(Error::coded(
            afs_error::IO_PERMISSION_DENIED,
            "owner getattr rejected",
        ));
        assert!(
            fs_b.getattr(&context(), inode_b, with_handle.then_some(denial_reader))
                .is_err()
        );
        let rejected = fs_b.handle_snapshot(denial_reader).unwrap();
        assert!(rejected.native_read_view.is_none());
        assert!(rejected.native_denial_serial > before);
        // Re-entering the proxy checks the owner again; a rejected local
        // view is never used as authority for a later result.
        assert_eq!(
            fs_b.read_owned(&context(), denial_reader, 0, 3).unwrap(),
            b"XYZ"
        );
        fs_b.release(&context(), denial_reader).unwrap();
    }
    let hook_fs = fs_b.clone();
    let hook_meta = meta.clone();
    *fs_b.native_read_before_delivery.lock().unwrap() = Some(Box::new(move || {
        hook_meta.fail_next_current_session_with(Error::coded(
            afs_error::IO_PERMISSION_DENIED,
            "proxy revoked",
        ));
        assert!(hook_fs.getattr(&context(), inode_b, Some(reader)).is_err());
    }));
    let mut unchanged = [9; 3];
    assert!(fs_b.read(&context(), reader, 0, &mut unchanged).is_err());
    assert_eq!(unchanged, [9; 3]);
    assert_eq!(fs_b.read_owned(&context(), reader, 0, 3).unwrap(), b"XYZ");

    assert!(fs_b.read(&context(), writer, 0, &mut data).is_err());
    fs_b.release(&context(), reader).unwrap();
    fs_b.release(&context(), writer).unwrap();
    assert!(fs_b.current_remote_provider(&inode_id).unwrap().is_none());
    assert!(fs_a.handles.is_empty());
}

#[test]
fn remote_provider_retire_timeout_keeps_release_debt_until_io_guard_drops() {
    let (_temp_a, meta, fs_a_raw) = test_fs();
    meta.set_node_session("node-a", Some("session-a"));
    let fs_a = Arc::new(fs_a_raw);
    let created = fs_a
        .create(
            &context(),
            fs_a.root_inode(),
            OsStr::new("remote-provider-retire-timeout.bin"),
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
    let read_engine_b = Arc::new(crate::node::dfs_read::DfsReadEngine::new(
        NamespaceId::new("default"),
        "node-b".into(),
        chunks_b.clone(),
        Arc::new(crate::node::dfs_read::UnimplementedReadSourceProvider),
        Arc::new(crate::node::dfs_read::UnimplementedChunkTransfer),
        crate::node::dfs_read::DfsReadConfig::default(),
    ));
    let loopback = Arc::new(LoopbackRemoteDfsOwner {
        owner: fs_a.clone(),
        peer: "node-b".into(),
    }) as Arc<dyn RemoteDfsOwner>;
    let scripted = Arc::new(ScriptedReleaseRemoteDfsOwner::new(loopback, Vec::new()));
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
    let writer = fs_b.open(&context(), inode_b, libc::O_WRONLY).unwrap();
    let snapshot = fs_b.handle_snapshot(writer).unwrap();
    let remote = snapshot
        .write_session
        .as_ref()
        .and_then(|session| session.remote.as_ref())
        .expect("remote write-only open installs a provider")
        .clone();
    let guard = remote
        .lifecycle
        .begin_io()
        .unwrap()
        .expect("live provider admits read IO");

    let error = fs_b
        .release(&context(), writer)
        .expect_err("retire waits for admitted IO and reports timeout");
    assert_eq!(error.code(), afs_error::NODE_VFS_UNAVAILABLE);
    assert_eq!(scripted.release_call_count(), 0);
    assert_eq!(fs_b.pending_remote_release_count().unwrap(), 2);
    assert_eq!(fs_a.handles.len(), 2);
    assert_eq!(fs_b.pending_remote_releases.lock().unwrap().reserved, 0);

    fs_b.retry_pending_remote_releases()
        .expect_err("maintenance must not release while provider IO is admitted");
    assert_eq!(scripted.release_call_count(), 0);
    assert_eq!(fs_b.pending_remote_release_count().unwrap(), 2);
    assert_eq!(fs_a.handles.len(), 2);

    drop(guard);

    assert_eq!(fs_b.retry_pending_remote_releases().unwrap(), 2);
    assert_eq!(scripted.release_call_count(), 2);
    assert_eq!(fs_b.pending_remote_release_count().unwrap(), 0);
    assert_eq!(fs_b.pending_remote_releases.lock().unwrap().reserved, 0);
    assert!(fs_a.handles.is_empty());
}

#[test]
fn former_local_clean_state_does_not_shadow_remote_provider() {
    let (_temp_a, meta, fs_a_raw) = test_fs();
    let created = fs_a_raw
        .create(
            &context(),
            fs_a_raw.root_inode(),
            OsStr::new("former-local-remote-provider.bin"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    fs_a_raw
        .write(&context(), created.handle, 0, b"abcdef")
        .unwrap();
    fs_a_raw
        .fsync(&context(), created.handle, SyncMode::Full)
        .unwrap();
    fs_a_raw.release(&context(), created.handle).unwrap();
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    let retained = fs_a_raw
        .write_state(&inode_id)
        .unwrap()
        .expect("local owner keeps a clean retained write state");
    assert!(DistributedFs::write_state_can_refresh_committed_view(
        &retained.lock().unwrap()
    ));

    {
        let mut lease = meta.lease.lock().unwrap();
        lease.owner_node_id = "node-b".into();
        lease.owner_session_id = "session-b".into();
        lease.lease_epoch = lease.lease_epoch.saturating_add(1);
        lease.expires_at_unix_ms = u64::MAX;
    }

    // The routing fixture gives the new owner a recovered committed base.
    let chunks_b = Arc::new(LocalChunkStore::open(_temp_a.path(), "node-b").unwrap());
    let read_engine_b = Arc::new(crate::node::dfs_read::DfsReadEngine::new(
        NamespaceId::new("default"),
        "node-b".into(),
        chunks_b.clone(),
        Arc::new(crate::node::dfs_read::UnimplementedReadSourceProvider),
        Arc::new(crate::node::dfs_read::UnimplementedChunkTransfer),
        crate::node::dfs_read::DfsReadConfig::default(),
    ));
    let fs_b = Arc::new(test_dfs_new(
        NamespaceId::new("default"),
        "node-b",
        "session-b",
        meta.clone(),
        chunks_b,
        read_engine_b,
    ));
    let remote_owner = Arc::new(LoopbackRemoteDfsOwner {
        owner: fs_b,
        peer: "node-a".into(),
    }) as Arc<dyn RemoteDfsOwner>;
    let fs_a = fs_a_raw.with_remote_owner_factory(Arc::new(StaticRemoteOwnerFactory {
        owner: remote_owner,
    }));
    let inode_a = fs_a.backend_inode(&inode_id).unwrap();
    let writer = fs_a
        .open(&context(), inode_a, libc::O_WRONLY | libc::O_TRUNC)
        .unwrap();

    assert_eq!(fs_a.write(&context(), writer, 0, b"XYZ").unwrap(), 3);
    let reader = fs_a.open(&context(), inode_a, libc::O_RDONLY).unwrap();
    assert_eq!(
        fs_a.getattr(&context(), inode_a, Some(reader))
            .unwrap()
            .size,
        3
    );
    let mut data = [0; 3];
    assert_eq!(fs_a.read(&context(), reader, 0, &mut data).unwrap(), 3);
    assert_eq!(&data, b"XYZ");
    fs_a.release(&context(), reader).unwrap();
    fs_a.release(&context(), writer).unwrap();
    assert!(fs_a.current_remote_provider(&inode_id).unwrap().is_none());
}

#[test]
fn temporary_remote_close_does_not_restore_released_previous_provider() {
    let (_temp_a, meta, fs_a_raw) = test_fs();
    let fs_a = Arc::new(fs_a_raw);
    let created = fs_a
        .create(
            &context(),
            fs_a.root_inode(),
            OsStr::new("remote-provider-stale-restore.bin"),
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
    let read_engine_b = Arc::new(crate::node::dfs_read::DfsReadEngine::new(
        NamespaceId::new("default"),
        "node-b".into(),
        chunks_b.clone(),
        Arc::new(crate::node::dfs_read::UnimplementedReadSourceProvider),
        Arc::new(crate::node::dfs_read::UnimplementedChunkTransfer),
        crate::node::dfs_read::DfsReadConfig::default(),
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
    let previous = fs_b
        .current_remote_provider(&inode_id)
        .unwrap()
        .expect("remote open installs provider");
    let (record, lease) = meta.open_write(&inode_id).unwrap();
    let record = fs_b.validate_inode(record).unwrap();
    let temp = fs_b
        .open_remote_write_session(&record, &lease, libc::O_WRONLY, OpenOptions::default())
        .unwrap();
    let current = fs_b
        .current_remote_provider(&inode_id)
        .unwrap()
        .expect("temporary session becomes provider");
    assert!(!DistributedFs::same_remote_provider(&current, &previous));

    fs_b.release(&context(), handle).unwrap();
    let current = fs_b
        .current_remote_provider(&inode_id)
        .unwrap()
        .expect("ordinary release must not remove temporary provider");
    assert!(!DistributedFs::same_remote_provider(&current, &previous));

    fs_b.close_temporary_remote_write_session(&inode_id, &temp, Some(previous))
        .unwrap();
    assert!(fs_b.current_remote_provider(&inode_id).unwrap().is_none());
}

fn dfs_owner_open_request(
    open_seq: u64,
    open_flags: i32,
) -> afs_protocol::node_control::DfsOwnerOpenRequest {
    afs_protocol::node_control::DfsOwnerOpenRequest {
        namespace_id: "default".into(),
        inode_id: "inode:test".into(),
        owner_node_id: "node-a".into(),
        owner_session_id: "session-a".into(),
        lease_epoch: 1,
        caller_session_id: "session-b".into(),
        open_flags,
        kill_suidgid: false,
        open_seq,
    }
}

fn dfs_owner_cleanup_handle(open_seq: u64) -> afs_protocol::node_control::DfsOwnerHandle {
    afs_protocol::node_control::DfsOwnerHandle {
        namespace_id: "default".into(),
        inode_id: "inode:test".into(),
        owner_node_id: "node-a".into(),
        owner_session_id: "session-a".into(),
        lease_epoch: 1,
        caller_node_id: "node-b".into(),
        caller_session_id: "session-b".into(),
        opaque_handle: Vec::new(),
        open_seq,
    }
}

struct BlockingFirstOpenRemoteDfsOwner {
    inner: Arc<dyn RemoteDfsOwner>,
    calls: Mutex<Vec<afs_protocol::node_control::DfsOwnerOpenRequest>>,
    first_entered: Mutex<Option<std::sync::mpsc::Sender<()>>>,
    release_first: Mutex<std::sync::mpsc::Receiver<()>>,
    second_entered: Mutex<Option<std::sync::mpsc::Sender<()>>>,
}

impl BlockingFirstOpenRemoteDfsOwner {
    fn new(
        inner: Arc<dyn RemoteDfsOwner>,
        first_entered: std::sync::mpsc::Sender<()>,
        release_first: std::sync::mpsc::Receiver<()>,
        second_entered: std::sync::mpsc::Sender<()>,
    ) -> Self {
        Self {
            inner,
            calls: Mutex::new(Vec::new()),
            first_entered: Mutex::new(Some(first_entered)),
            release_first: Mutex::new(release_first),
            second_entered: Mutex::new(Some(second_entered)),
        }
    }

    fn open_requests(&self) -> Vec<afs_protocol::node_control::DfsOwnerOpenRequest> {
        self.calls.lock().unwrap().clone()
    }
}

impl RemoteDfsOwner for BlockingFirstOpenRemoteDfsOwner {
    fn open(
        &self,
        request: afs_protocol::node_control::DfsOwnerOpenRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerOpenReply> {
        let call_index = {
            let mut calls = self.calls.lock().unwrap();
            let call_index = calls.len();
            calls.push(request.clone());
            call_index
        };
        if call_index == 0 {
            if let Some(sender) = self.first_entered.lock().unwrap().take() {
                sender.send(()).unwrap();
            }
            self.release_first.lock().unwrap().recv().unwrap();
        } else if call_index == 1
            && let Some(sender) = self.second_entered.lock().unwrap().take()
        {
            sender.send(()).unwrap();
        }
        self.inner.open(request)
    }

    fn getattr(
        &self,
        request: afs_protocol::node_control::DfsOwnerGetAttrRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerGetAttrReply> {
        self.inner.getattr(request)
    }

    fn release(
        &self,
        request: afs_protocol::node_control::DfsOwnerReleaseRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerReleaseReply> {
        self.inner.release(request)
    }

    fn get_lock(
        &self,
        request: afs_protocol::node_control::DfsOwnerGetLockRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerGetLockReply> {
        self.inner.get_lock(request)
    }

    fn set_lock(
        &self,
        request: afs_protocol::node_control::DfsOwnerSetLockRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerSetLockReply> {
        self.inner.set_lock(request)
    }

    fn cancel_lock_wait(
        &self,
        request: afs_protocol::node_control::DfsOwnerCancelLockWaitRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerCancelLockWaitReply> {
        self.inner.cancel_lock_wait(request)
    }

    fn acknowledge_lock_wait(
        &self,
        request: afs_protocol::node_control::DfsOwnerAcknowledgeLockWaitRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerAcknowledgeLockWaitReply> {
        self.inner.acknowledge_lock_wait(request)
    }

    fn release_locks(
        &self,
        request: afs_protocol::node_control::DfsOwnerReleaseLocksRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerReleaseLocksReply> {
        self.inner.release_locks(request)
    }

    fn release_lock_session(
        &self,
        request: afs_protocol::node_control::DfsOwnerReleaseLockSessionRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerReleaseLockSessionReply> {
        self.inner.release_lock_session(request)
    }

    fn read(
        &self,
        request: afs_protocol::node_data::DfsOwnerReadRequest,
    ) -> Result<afs_protocol::node_data::DfsOwnerReadReply> {
        self.inner.read(request)
    }

    fn write(
        &self,
        request: afs_protocol::node_data::DfsOwnerWriteRequest,
    ) -> Result<afs_protocol::node_data::DfsOwnerWriteReply> {
        self.inner.write(request)
    }

    fn resize(
        &self,
        request: afs_protocol::node_data::DfsOwnerResizeRequest,
    ) -> Result<afs_protocol::node_data::DfsOwnerResizeReply> {
        self.inner.resize(request)
    }

    fn sync(
        &self,
        request: afs_protocol::node_data::DfsOwnerSyncRequest,
    ) -> Result<afs_protocol::node_data::DfsOwnerSyncReply> {
        self.inner.sync(request)
    }
}

enum ScriptedOpenAction {
    Delegate,
    DelegateThenErr(Error),
}

struct ScriptedOpenRemoteDfsOwner {
    inner: Arc<dyn RemoteDfsOwner>,
    opens: Mutex<std::collections::VecDeque<ScriptedOpenAction>>,
    calls: Mutex<Vec<afs_protocol::node_control::DfsOwnerOpenRequest>>,
}

impl ScriptedOpenRemoteDfsOwner {
    fn new(inner: Arc<dyn RemoteDfsOwner>, actions: Vec<ScriptedOpenAction>) -> Self {
        Self {
            inner,
            opens: Mutex::new(actions.into_iter().collect()),
            calls: Mutex::new(Vec::new()),
        }
    }

    fn open_requests(&self) -> Vec<afs_protocol::node_control::DfsOwnerOpenRequest> {
        self.calls.lock().unwrap().clone()
    }
}

impl RemoteDfsOwner for ScriptedOpenRemoteDfsOwner {
    fn open(
        &self,
        request: afs_protocol::node_control::DfsOwnerOpenRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerOpenReply> {
        self.calls.lock().unwrap().push(request.clone());
        match self.opens.lock().unwrap().pop_front() {
            None | Some(ScriptedOpenAction::Delegate) => self.inner.open(request),
            Some(ScriptedOpenAction::DelegateThenErr(error)) => {
                self.inner.open(request)?;
                Err(error)
            }
        }
    }

    fn getattr(
        &self,
        request: afs_protocol::node_control::DfsOwnerGetAttrRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerGetAttrReply> {
        self.inner.getattr(request)
    }

    fn release(
        &self,
        request: afs_protocol::node_control::DfsOwnerReleaseRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerReleaseReply> {
        self.inner.release(request)
    }

    fn get_lock(
        &self,
        request: afs_protocol::node_control::DfsOwnerGetLockRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerGetLockReply> {
        self.inner.get_lock(request)
    }

    fn set_lock(
        &self,
        request: afs_protocol::node_control::DfsOwnerSetLockRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerSetLockReply> {
        self.inner.set_lock(request)
    }

    fn cancel_lock_wait(
        &self,
        request: afs_protocol::node_control::DfsOwnerCancelLockWaitRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerCancelLockWaitReply> {
        self.inner.cancel_lock_wait(request)
    }

    fn acknowledge_lock_wait(
        &self,
        request: afs_protocol::node_control::DfsOwnerAcknowledgeLockWaitRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerAcknowledgeLockWaitReply> {
        self.inner.acknowledge_lock_wait(request)
    }

    fn release_locks(
        &self,
        request: afs_protocol::node_control::DfsOwnerReleaseLocksRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerReleaseLocksReply> {
        self.inner.release_locks(request)
    }

    fn release_lock_session(
        &self,
        request: afs_protocol::node_control::DfsOwnerReleaseLockSessionRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerReleaseLockSessionReply> {
        self.inner.release_lock_session(request)
    }

    fn read(
        &self,
        request: afs_protocol::node_data::DfsOwnerReadRequest,
    ) -> Result<afs_protocol::node_data::DfsOwnerReadReply> {
        self.inner.read(request)
    }

    fn write(
        &self,
        request: afs_protocol::node_data::DfsOwnerWriteRequest,
    ) -> Result<afs_protocol::node_data::DfsOwnerWriteReply> {
        self.inner.write(request)
    }

    fn resize(
        &self,
        request: afs_protocol::node_data::DfsOwnerResizeRequest,
    ) -> Result<afs_protocol::node_data::DfsOwnerResizeReply> {
        self.inner.resize(request)
    }

    fn sync(
        &self,
        request: afs_protocol::node_data::DfsOwnerSyncRequest,
    ) -> Result<afs_protocol::node_data::DfsOwnerSyncReply> {
        self.inner.sync(request)
    }
}

#[test]
fn remote_owner_append_retry_is_idempotent_and_dirty_read_is_visible() {
    let (_temp, _meta, fs) = test_fs();
    let open = <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::open(
        &fs,
        "node-b",
        afs_protocol::node_control::DfsOwnerOpenRequest {
            namespace_id: "default".into(),
            inode_id: "inode:test".into(),
            owner_node_id: "node-a".into(),
            owner_session_id: "session-a".into(),
            lease_epoch: 1,
            caller_session_id: "session-b".into(),
            open_flags: libc::O_RDWR | libc::O_APPEND,
            kill_suidgid: false,
            open_seq: 1,
        },
    )
    .unwrap();
    let handle = open.handle.expect("owner open returns handle");
    let data_handle = DistributedFs::data_owner_handle(&handle);

    let write = afs_protocol::node_data::DfsOwnerWriteRequest {
        handle: Some(data_handle.clone()),
        operation_id: "node-b-op-1".into(),
        offset: 0,
        data: b"hi".to_vec(),
        append: true,
        kill_suidgid: false,
    };
    let first = <DistributedFs as crate::node::rpc::data::DfsOwnerFilesHandler>::write(
        &fs,
        "node-b",
        write.clone(),
    )
    .unwrap();
    let retry = <DistributedFs as crate::node::rpc::data::DfsOwnerFilesHandler>::write(
        &fs,
        "node-b",
        write.clone(),
    )
    .unwrap();
    assert_eq!(first, retry);
    assert_eq!(first.written, 2);
    let mut changed = afs_protocol::node_data::DfsOwnerWriteRequest {
        data: b"changed".to_vec(),
        ..write.clone()
    };
    changed.operation_id = "node-b-op-1".into();
    assert!(
        <DistributedFs as crate::node::rpc::data::DfsOwnerFilesHandler>::write(
            &fs, "node-b", changed
        )
        .unwrap_err()
        .message()
        .contains("reused with a different request body")
    );

    let reply = <DistributedFs as crate::node::rpc::data::DfsOwnerFilesHandler>::read(
        &fs,
        "node-b",
        afs_protocol::node_data::DfsOwnerReadRequest {
            handle: Some(data_handle.clone()),
            offset: 0,
            length: 8,
        },
    )
    .unwrap();
    assert_eq!(reply.data, b"hi");

    <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::release(
        &fs,
        "node-b",
        afs_protocol::node_control::DfsOwnerReleaseRequest {
            handle: Some(handle),
        },
    )
    .unwrap();
}

/// S0 characterization for ledger Q01: the owner-side idempotency table for
/// proxied writes has a fixed capacity and no eviction, so a long-lived owner
/// rejects new proxied writes once it has remembered that many operations.
#[test]
fn s0_q01_owner_operation_table_fills_and_rejects_new_proxied_writes() {
    let (_temp, _meta, fs) = test_fs();
    let open = <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::open(
        &fs,
        "node-b",
        afs_protocol::node_control::DfsOwnerOpenRequest {
            namespace_id: "default".into(),
            inode_id: "inode:test".into(),
            owner_node_id: "node-a".into(),
            owner_session_id: "session-a".into(),
            lease_epoch: 1,
            caller_session_id: "session-b".into(),
            open_flags: libc::O_RDWR,
            kill_suidgid: false,
            open_seq: 1,
        },
    )
    .unwrap();
    let handle = open.handle.expect("owner open returns handle");
    let data_handle = DistributedFs::data_owner_handle(&handle);
    let write = |index: usize| afs_protocol::node_data::DfsOwnerWriteRequest {
        handle: Some(data_handle.clone()),
        operation_id: format!("node-b-op-{index}"),
        offset: 0,
        data: vec![b'x'],
        append: false,
        kill_suidgid: false,
    };
    for index in 0..MAX_REMOTE_OPERATION_RESULTS {
        <DistributedFs as crate::node::rpc::data::DfsOwnerFilesHandler>::write(
            &fs,
            "node-b",
            write(index),
        )
        .unwrap();
    }
    let error = <DistributedFs as crate::node::rpc::data::DfsOwnerFilesHandler>::write(
        &fs,
        "node-b",
        write(MAX_REMOTE_OPERATION_RESULTS),
    )
    .unwrap_err();
    assert!(
        error.message().contains("remote operation table is full"),
        "{error:?}"
    );
    <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::release(
        &fs,
        "node-b",
        afs_protocol::node_control::DfsOwnerReleaseRequest {
            handle: Some(handle),
        },
    )
    .unwrap();
}
