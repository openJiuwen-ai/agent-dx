use super::*;

fn dfs_lock_authority(
    caller_session_id: &str,
) -> afs_protocol::node_control::DfsOwnerLockAuthority {
    afs_protocol::node_control::DfsOwnerLockAuthority {
        namespace_id: "default".into(),
        inode_id: "inode:test".into(),
        owner_node_id: "node-a".into(),
        owner_session_id: "session-a".into(),
        lease_epoch: 1,
        lease_expires_at_unix_ms: u64::MAX,
        caller_node_id: "node-b".into(),
        caller_session_id: caller_session_id.into(),
    }
}

struct InterruptedSetLockRemoteDfsOwner {
    cancel_calls: AtomicUsize,
    acknowledge_calls: AtomicUsize,
}

impl InterruptedSetLockRemoteDfsOwner {
    fn cancel_call_count(&self) -> usize {
        self.cancel_calls.load(Ordering::SeqCst)
    }

    fn acknowledge_call_count(&self) -> usize {
        self.acknowledge_calls.load(Ordering::SeqCst)
    }
}

struct CancelAckFailsOnceRemoteDfsOwner {
    cancel_calls: AtomicUsize,
    acknowledge_calls: AtomicUsize,
}

impl CancelAckFailsOnceRemoteDfsOwner {
    fn cancel_call_count(&self) -> usize {
        self.cancel_calls.load(Ordering::SeqCst)
    }

    fn acknowledge_call_count(&self) -> usize {
        self.acknowledge_calls.load(Ordering::SeqCst)
    }
}

impl RemoteDfsOwner for CancelAckFailsOnceRemoteDfsOwner {
    noop_remote_owner_method!(
        open,
        afs_protocol::node_control::DfsOwnerOpenRequest,
        afs_protocol::node_control::DfsOwnerOpenReply
    );
    noop_remote_owner_method!(
        getattr,
        afs_protocol::node_control::DfsOwnerGetAttrRequest,
        afs_protocol::node_control::DfsOwnerGetAttrReply
    );
    noop_remote_owner_method!(
        release,
        afs_protocol::node_control::DfsOwnerReleaseRequest,
        afs_protocol::node_control::DfsOwnerReleaseReply
    );
    noop_remote_owner_method!(
        get_lock,
        afs_protocol::node_control::DfsOwnerGetLockRequest,
        afs_protocol::node_control::DfsOwnerGetLockReply
    );
    fn set_lock(
        &self,
        _: afs_protocol::node_control::DfsOwnerSetLockRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerSetLockReply> {
        Err(unavailable("injected remote lock failure"))
    }
    fn cancel_lock_wait(
        &self,
        _: afs_protocol::node_control::DfsOwnerCancelLockWaitRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerCancelLockWaitReply> {
        self.cancel_calls.fetch_add(1, Ordering::SeqCst);
        Ok(afs_protocol::node_control::DfsOwnerCancelLockWaitReply { outcome: 1 })
    }
    fn acknowledge_lock_wait(
        &self,
        _: afs_protocol::node_control::DfsOwnerAcknowledgeLockWaitRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerAcknowledgeLockWaitReply> {
        if self.acknowledge_calls.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err(unavailable("injected remote lock ack failure"));
        }
        Ok(afs_protocol::node_control::DfsOwnerAcknowledgeLockWaitReply {})
    }
    noop_remote_owner_method!(
        release_locks,
        afs_protocol::node_control::DfsOwnerReleaseLocksRequest,
        afs_protocol::node_control::DfsOwnerReleaseLocksReply
    );
    noop_remote_owner_method!(
        release_lock_session,
        afs_protocol::node_control::DfsOwnerReleaseLockSessionRequest,
        afs_protocol::node_control::DfsOwnerReleaseLockSessionReply
    );
    noop_remote_owner_method!(
        read,
        afs_protocol::node_data::DfsOwnerReadRequest,
        afs_protocol::node_data::DfsOwnerReadReply
    );
    noop_remote_owner_method!(
        write,
        afs_protocol::node_data::DfsOwnerWriteRequest,
        afs_protocol::node_data::DfsOwnerWriteReply
    );
    noop_remote_owner_method!(
        resize,
        afs_protocol::node_data::DfsOwnerResizeRequest,
        afs_protocol::node_data::DfsOwnerResizeReply
    );
    noop_remote_owner_method!(
        sync,
        afs_protocol::node_data::DfsOwnerSyncRequest,
        afs_protocol::node_data::DfsOwnerSyncReply
    );
}

impl RemoteDfsOwner for InterruptedSetLockRemoteDfsOwner {
    noop_remote_owner_method!(
        open,
        afs_protocol::node_control::DfsOwnerOpenRequest,
        afs_protocol::node_control::DfsOwnerOpenReply
    );
    noop_remote_owner_method!(
        getattr,
        afs_protocol::node_control::DfsOwnerGetAttrRequest,
        afs_protocol::node_control::DfsOwnerGetAttrReply
    );
    noop_remote_owner_method!(
        release,
        afs_protocol::node_control::DfsOwnerReleaseRequest,
        afs_protocol::node_control::DfsOwnerReleaseReply
    );
    noop_remote_owner_method!(
        get_lock,
        afs_protocol::node_control::DfsOwnerGetLockRequest,
        afs_protocol::node_control::DfsOwnerGetLockReply
    );
    fn set_lock(
        &self,
        _: afs_protocol::node_control::DfsOwnerSetLockRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerSetLockReply> {
        Err(lock_error(LockError::Interrupted))
    }
    fn cancel_lock_wait(
        &self,
        _: afs_protocol::node_control::DfsOwnerCancelLockWaitRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerCancelLockWaitReply> {
        self.cancel_calls.fetch_add(1, Ordering::SeqCst);
        Ok(afs_protocol::node_control::DfsOwnerCancelLockWaitReply { outcome: 1 })
    }
    fn acknowledge_lock_wait(
        &self,
        _: afs_protocol::node_control::DfsOwnerAcknowledgeLockWaitRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerAcknowledgeLockWaitReply> {
        self.acknowledge_calls.fetch_add(1, Ordering::SeqCst);
        Ok(afs_protocol::node_control::DfsOwnerAcknowledgeLockWaitReply {})
    }
    noop_remote_owner_method!(
        release_locks,
        afs_protocol::node_control::DfsOwnerReleaseLocksRequest,
        afs_protocol::node_control::DfsOwnerReleaseLocksReply
    );
    noop_remote_owner_method!(
        release_lock_session,
        afs_protocol::node_control::DfsOwnerReleaseLockSessionRequest,
        afs_protocol::node_control::DfsOwnerReleaseLockSessionReply
    );
    noop_remote_owner_method!(
        read,
        afs_protocol::node_data::DfsOwnerReadRequest,
        afs_protocol::node_data::DfsOwnerReadReply
    );
    noop_remote_owner_method!(
        write,
        afs_protocol::node_data::DfsOwnerWriteRequest,
        afs_protocol::node_data::DfsOwnerWriteReply
    );
    noop_remote_owner_method!(
        resize,
        afs_protocol::node_data::DfsOwnerResizeRequest,
        afs_protocol::node_data::DfsOwnerResizeReply
    );
    noop_remote_owner_method!(
        sync,
        afs_protocol::node_data::DfsOwnerSyncRequest,
        afs_protocol::node_data::DfsOwnerSyncReply
    );
}

struct FailOnceReleaseRemoteDfsOwner {
    calls: Mutex<Vec<String>>,
}

impl FailOnceReleaseRemoteDfsOwner {
    fn new() -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
        }
    }

    fn call_count(&self) -> usize {
        self.calls.lock().unwrap().len()
    }
}

impl RemoteDfsOwner for FailOnceReleaseRemoteDfsOwner {
    noop_remote_owner_method!(
        open,
        afs_protocol::node_control::DfsOwnerOpenRequest,
        afs_protocol::node_control::DfsOwnerOpenReply
    );
    noop_remote_owner_method!(
        getattr,
        afs_protocol::node_control::DfsOwnerGetAttrRequest,
        afs_protocol::node_control::DfsOwnerGetAttrReply
    );
    noop_remote_owner_method!(
        release,
        afs_protocol::node_control::DfsOwnerReleaseRequest,
        afs_protocol::node_control::DfsOwnerReleaseReply
    );
    noop_remote_owner_method!(
        get_lock,
        afs_protocol::node_control::DfsOwnerGetLockRequest,
        afs_protocol::node_control::DfsOwnerGetLockReply
    );
    noop_remote_owner_method!(
        set_lock,
        afs_protocol::node_control::DfsOwnerSetLockRequest,
        afs_protocol::node_control::DfsOwnerSetLockReply
    );
    noop_remote_owner_method!(
        cancel_lock_wait,
        afs_protocol::node_control::DfsOwnerCancelLockWaitRequest,
        afs_protocol::node_control::DfsOwnerCancelLockWaitReply
    );
    noop_remote_owner_method!(
        acknowledge_lock_wait,
        afs_protocol::node_control::DfsOwnerAcknowledgeLockWaitRequest,
        afs_protocol::node_control::DfsOwnerAcknowledgeLockWaitReply
    );
    noop_remote_owner_method!(
        release_locks,
        afs_protocol::node_control::DfsOwnerReleaseLocksRequest,
        afs_protocol::node_control::DfsOwnerReleaseLocksReply
    );

    fn release_lock_session(
        &self,
        request: afs_protocol::node_control::DfsOwnerReleaseLockSessionRequest,
    ) -> afs_error::Result<afs_protocol::node_control::DfsOwnerReleaseLockSessionReply> {
        let mut calls = self.calls.lock().unwrap();
        calls.push(request.ingress_session_id);
        if calls.len() == 1 {
            return Err(unavailable("injected remote release failure"));
        }
        Ok(afs_protocol::node_control::DfsOwnerReleaseLockSessionReply {})
    }

    noop_remote_owner_method!(
        read,
        afs_protocol::node_data::DfsOwnerReadRequest,
        afs_protocol::node_data::DfsOwnerReadReply
    );
    noop_remote_owner_method!(
        write,
        afs_protocol::node_data::DfsOwnerWriteRequest,
        afs_protocol::node_data::DfsOwnerWriteReply
    );
    noop_remote_owner_method!(
        resize,
        afs_protocol::node_data::DfsOwnerResizeRequest,
        afs_protocol::node_data::DfsOwnerResizeReply
    );
    noop_remote_owner_method!(
        sync,
        afs_protocol::node_data::DfsOwnerSyncRequest,
        afs_protocol::node_data::DfsOwnerSyncReply
    );
}

#[test]
fn dfs_reaps_lock_scopes_for_expired_peer_process_session() {
    let (_temp, _meta, fs) = test_fs();
    let inode_id = InodeId::new("inode:reap-peer-locks");
    let authority = fs
        .local_lock_authority(
            &inode_id,
            WriteLease {
                inode_id: inode_id.clone(),
                owner_node_id: "node-a".into(),
                owner_session_id: "session-a".into(),
                lease_epoch: 1,
                expires_at_unix_ms: u64::MAX,
            },
        )
        .unwrap();
    let owner = FileLockOwner {
        ingress_session_id: DistributedFs::lock_scope("node-z", "old-session", "mount-z").unwrap(),
        kernel_owner: 77,
    };
    let lock = LockRequest::write(FileLockKind::Posix, owner.clone(), 77, lock_range(0, 99));
    authority.table.setlk_nonblocking(lock.clone()).unwrap();
    fs.register_local_lock_success(&authority, &lock).unwrap();
    assert!(
        authority
            .table
            .getlk(&LockRequest::write(
                FileLockKind::Posix,
                FileLockOwner {
                    ingress_session_id: DistributedFs::lock_scope("node-a", "session-a", "mount-a")
                        .unwrap(),
                    kernel_owner: 78,
                },
                78,
                lock_range(0, 99),
            ))
            .unwrap()
            .is_some()
    );

    assert_eq!(fs.reap_expired_peer_lock_sessions().unwrap(), 1);
    assert!(
        authority
            .table
            .getlk(&LockRequest::write(
                FileLockKind::Posix,
                FileLockOwner {
                    ingress_session_id: DistributedFs::lock_scope("node-a", "session-a", "mount-a")
                        .unwrap(),
                    kernel_owner: 78,
                },
                78,
                lock_range(0, 99),
            ))
            .unwrap()
            .is_none()
    );
    assert!(authority.state.lock().unwrap().pinned_owners.is_empty());
}

#[test]
fn dfs_release_scoped_session_cleans_scope_when_closed_tombstones_are_full() {
    let (_temp, _meta, fs) = test_fs();
    let inode_id = InodeId::new("inode:closed-capacity");
    let authority = fs
        .local_lock_authority(
            &inode_id,
            WriteLease {
                inode_id: inode_id.clone(),
                owner_node_id: "node-a".into(),
                owner_session_id: "session-a".into(),
                lease_epoch: 1,
                expires_at_unix_ms: u64::MAX,
            },
        )
        .unwrap();
    for index in 0..MAX_CLOSED_LOCK_SESSIONS {
        fs.locks
            .closed_sessions
            .lock()
            .unwrap()
            .insert(format!("closed-{index}"));
    }
    let released_scope = DistributedFs::lock_scope("node-z", "old-session", "mount-z").unwrap();
    let retained_scope = DistributedFs::lock_scope("node-y", "live-session", "mount-y").unwrap();
    let released = FileLockOwner {
        ingress_session_id: released_scope.clone(),
        kernel_owner: 10,
    };
    let retained = FileLockOwner {
        ingress_session_id: retained_scope.clone(),
        kernel_owner: 11,
    };
    let released_lock =
        LockRequest::write(FileLockKind::Posix, released.clone(), 10, lock_range(0, 9));
    let retained_lock = LockRequest::write(
        FileLockKind::Posix,
        retained.clone(),
        11,
        lock_range(100, 109),
    );
    authority
        .table
        .setlk_nonblocking(released_lock.clone())
        .unwrap();
    fs.register_local_lock_success(&authority, &released_lock)
        .unwrap();
    authority
        .table
        .setlk_nonblocking(retained_lock.clone())
        .unwrap();
    fs.register_local_lock_success(&authority, &retained_lock)
        .unwrap();

    let mut scopes = HashSet::new();
    scopes.insert(released_scope.clone());
    assert_eq!(fs.release_scoped_lock_sessions(scopes).unwrap(), 1);

    let closed = fs.locks.closed_sessions.lock().unwrap();
    assert_eq!(closed.len(), MAX_CLOSED_LOCK_SESSIONS);
    assert!(!closed.contains(&released_scope));
    drop(closed);
    assert!(
        fs.locks
            .closed_session_admission_closed
            .load(Ordering::Acquire)
    );
    assert!(fs.ensure_lock_session_open("new-mount").is_err());
    assert!(
        authority
            .table
            .getlk(&LockRequest::write(
                FileLockKind::Posix,
                lock_owner("probe-a", 20),
                20,
                lock_range(0, 9),
            ))
            .unwrap()
            .is_none()
    );
    assert!(
        authority
            .table
            .getlk(&LockRequest::write(
                FileLockKind::Posix,
                lock_owner("probe-b", 21),
                21,
                lock_range(100, 109),
            ))
            .unwrap()
            .is_some()
    );
    assert!(matches!(
        authority.table.setlk_nonblocking(LockRequest::write(
            FileLockKind::Posix,
            released,
            10,
            lock_range(0, 9),
        )),
        Err(LockError::Interrupted)
    ));
    assert!(
        authority
            .state
            .lock()
            .unwrap()
            .pinned_owners
            .contains(&retained)
    );
}

#[test]
fn dfs_remote_lock_session_release_failure_retains_identity_for_retry() {
    let (_temp, _meta, fs) = test_fs();
    let inode_id = InodeId::new("inode:remote-release-retry");
    let owner = Arc::new(FailOnceReleaseRemoteDfsOwner::new());
    let remote = DfsRemoteLockAuthority {
        owner: owner.clone(),
        authority: afs_protocol::node_control::DfsOwnerLockAuthority {
            namespace_id: "default".into(),
            inode_id: inode_id.0.clone(),
            owner_node_id: "node-b".into(),
            owner_session_id: "session-b".into(),
            lease_epoch: 1,
            lease_expires_at_unix_ms: u64::MAX,
            caller_node_id: "node-a".into(),
            caller_session_id: "session-a".into(),
        },
    };
    let scoped_session = DistributedFs::lock_scope("node-a", "session-a", "mount-r").unwrap();
    let waiter = LockWaiterId {
        ingress_session_id: scoped_session.clone(),
        request_id: 7,
    };
    fs.locks
        .proxy
        .authorities
        .lock()
        .unwrap()
        .insert(inode_id.clone(), remote.clone());
    fs.note_remote_lock_session(&inode_id, &scoped_session)
        .unwrap();
    for index in 0..MAX_CLOSED_LOCK_SESSIONS {
        fs.locks
            .closed_sessions
            .lock()
            .unwrap()
            .insert(format!("closed-remote-{index}"));
    }
    fs.register_remote_lock_waiter_route(&waiter, remote)
        .unwrap();

    assert_eq!(
        fs.release_lock_session("mount-r").unwrap_err().code(),
        afs_error::NODE_VFS_UNAVAILABLE
    );
    assert_eq!(owner.call_count(), 1);
    let closed = fs.locks.closed_sessions.lock().unwrap();
    assert_eq!(closed.len(), MAX_CLOSED_LOCK_SESSIONS);
    assert!(!closed.contains(&scoped_session));
    drop(closed);
    assert_eq!(
        fs.locks
            .proxy
            .authority_sessions
            .lock()
            .unwrap()
            .get(&inode_id)
            .unwrap()
            .get(&scoped_session),
        Some(&REMOTE_LOCK_SESSION_PENDING_CLOSE)
    );
    assert_eq!(
        fs.note_remote_lock_session(&inode_id, &scoped_session)
            .unwrap_err()
            .code(),
        afs_error::IO_INTERRUPTED
    );
    assert!(fs.locks.proxy.waiters.lock().unwrap().contains_key(&waiter));
    assert!(
        fs.locks
            .proxy
            .authorities
            .lock()
            .unwrap()
            .contains_key(&inode_id)
    );

    assert_eq!(fs.reap_expired_peer_lock_sessions().unwrap(), 1);
    assert_eq!(owner.call_count(), 2);
    assert!(
        !fs.locks
            .proxy
            .authority_sessions
            .lock()
            .unwrap()
            .contains_key(&inode_id)
    );
    assert!(!fs.locks.proxy.waiters.lock().unwrap().contains_key(&waiter));
    assert!(
        !fs.locks
            .proxy
            .authorities
            .lock()
            .unwrap()
            .contains_key(&inode_id)
    );
}

#[test]
fn dfs_remote_lock_precancel_survives_replayed_waiter_identity() {
    let (_temp, _meta, fs) = test_fs();
    let remote = DfsRemoteLockAuthority {
        owner: Arc::new(NoopRemoteDfsOwner),
        authority: afs_protocol::node_control::DfsOwnerLockAuthority {
            namespace_id: "default".into(),
            inode_id: "inode:test".into(),
            owner_node_id: "node-b".into(),
            owner_session_id: "session-b".into(),
            lease_epoch: 1,
            lease_expires_at_unix_ms: u64::MAX,
            caller_node_id: "node-a".into(),
            caller_session_id: "session-a".into(),
        },
    };
    let waiter = LockWaiterId {
        ingress_session_id: "mount-a".into(),
        request_id: 43,
    };

    assert!(
        fs.take_remote_lock_waiter_route_or_precancel(waiter.clone())
            .unwrap()
            .is_none()
    );
    assert_eq!(
        fs.register_remote_lock_waiter_route(&waiter, remote.clone())
            .unwrap_err()
            .code(),
        afs_error::IO_INTERRUPTED
    );
    assert_eq!(
        fs.register_remote_lock_waiter_route(&waiter, remote)
            .unwrap_err()
            .code(),
        afs_error::IO_INTERRUPTED
    );
    assert!(fs.locks.proxy.waiters.lock().unwrap().is_empty());
}

#[test]
fn dfs_remote_lock_waiter_route_rejects_duplicate_identity() {
    let (_temp, _meta, fs) = test_fs();
    let remote = DfsRemoteLockAuthority {
        owner: Arc::new(NoopRemoteDfsOwner),
        authority: afs_protocol::node_control::DfsOwnerLockAuthority {
            namespace_id: "default".into(),
            inode_id: "inode:test".into(),
            owner_node_id: "node-b".into(),
            owner_session_id: "session-b".into(),
            lease_epoch: 1,
            lease_expires_at_unix_ms: u64::MAX,
            caller_node_id: "node-a".into(),
            caller_session_id: "session-a".into(),
        },
    };
    let waiter = LockWaiterId {
        ingress_session_id: "mount-a".into(),
        request_id: 42,
    };

    fs.register_remote_lock_waiter_route(&waiter, remote.clone())
        .unwrap();
    let error = fs
        .register_remote_lock_waiter_route(&waiter, remote)
        .unwrap_err();

    assert_eq!(error.code(), afs_error::IO_OTHER);
    assert_eq!(fs.locks.proxy.waiters.lock().unwrap().len(), 1);
}

#[test]
fn dfs_local_posix_locks_conflict_and_release_by_owner() {
    let (_temp, _meta, fs) = test_fs();
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("locks-local.bin"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    let second = fs
        .open(&context(), created.entry.inode, libc::O_RDWR)
        .unwrap();
    let owner_a = lock_owner("mount-a", 10);
    let owner_b = lock_owner("mount-a", 11);

    fs.setlk(
        &context(),
        created.entry.inode,
        created.handle,
        LockRequest::read(FileLockKind::Posix, owner_a.clone(), 100, lock_range(0, 9)),
        None,
    )
    .unwrap();
    let conflict = fs
        .getlk(
            &context(),
            created.entry.inode,
            second,
            LockRequest::write(FileLockKind::Posix, owner_b.clone(), 200, lock_range(0, 9)),
        )
        .unwrap()
        .expect("write lock must observe read-lock conflict");
    assert_eq!(conflict.owner, owner_a);
    assert_eq!(conflict.lock_type, FileLockType::Read);

    fs.setlk(
        &context(),
        created.entry.inode,
        second,
        LockRequest::write(FileLockKind::Posix, owner_b.clone(), 200, lock_range(0, 9)),
        None,
    )
    .unwrap_err();
    let authority = fs
        .locks
        .authorities
        .read(&InodeId::new("inode:test"))
        .unwrap()
        .get(&InodeId::new("inode:test"))
        .cloned()
        .expect("local lock authority exists");
    assert!(
        !authority
            .state
            .lock()
            .unwrap()
            .pinned_owners
            .contains(&owner_b),
        "failed nonblocking lock must not pin owner or renew activity"
    );
    fs.release_locks(
        &context(),
        created.entry.inode,
        created.handle,
        owner_a,
        ReleaseKind::PosixOwner,
    )
    .unwrap();
    fs.setlk(
        &context(),
        created.entry.inode,
        second,
        LockRequest::write(FileLockKind::Posix, owner_b, 200, lock_range(0, 9)),
        None,
    )
    .unwrap();
}

#[test]
fn dfs_owner_lock_authority_is_bound_to_authenticated_peer() {
    let (_temp, _meta, fs) = test_fs();
    let authority = afs_protocol::node_control::DfsOwnerLockAuthority {
        namespace_id: "default".into(),
        inode_id: "inode:test".into(),
        owner_node_id: "node-a".into(),
        owner_session_id: "session-a".into(),
        lease_epoch: 1,
        lease_expires_at_unix_ms: u64::MAX,
        caller_node_id: "node-b".into(),
        caller_session_id: "session-b".into(),
    };
    let request = afs_protocol::node_control::DfsOwnerGetLockRequest {
        authority: Some(authority.clone()),
        lock: Some(DistributedFs::wire_lock_request(&LockRequest::read(
            FileLockKind::Posix,
            lock_owner("mount-b", 7),
            77,
            lock_range(0, 0),
        ))),
    };
    <DistributedFs as crate::node::vfs::dfs::owner_api::DfsOwnerLifecycleHandler>::get_lock(
        &fs,
        "node-b",
        request.clone(),
    )
    .unwrap();
    assert_eq!(
        <DistributedFs as crate::node::vfs::dfs::owner_api::DfsOwnerLifecycleHandler>::get_lock(
            &fs, "node-c", request,
        )
        .unwrap_err()
        .code(),
        afs_error::NODE_DFS_STALE_HANDLE
    );
}

#[test]
fn dfs_owner_lock_release_is_scoped_by_authenticated_process_session() {
    let (_temp, _meta, fs) = test_fs();
    let authority = dfs_lock_authority("session-b");
    let lock = LockRequest::write(
        FileLockKind::Posix,
        lock_owner("mount-b", 70),
        70,
        lock_range(0, 99),
    );
    <DistributedFs as crate::node::vfs::dfs::owner_api::DfsOwnerLifecycleHandler>::set_lock(
        &fs,
        "node-b",
        afs_protocol::node_control::DfsOwnerSetLockRequest {
            authority: Some(authority.clone()),
            lock: Some(DistributedFs::wire_lock_request(&lock)),
            waiter: None,
        },
    )
    .unwrap();

    let mut forged_authority = authority.clone();
    forged_authority.caller_session_id = "session-other".into();
    <DistributedFs as crate::node::vfs::dfs::owner_api::DfsOwnerLifecycleHandler>::release_locks(
        &fs,
        "node-b",
        afs_protocol::node_control::DfsOwnerReleaseLocksRequest {
            authority: Some(forged_authority),
            owner: Some(DistributedFs::wire_lock_owner(&lock.owner)),
            release_kind: DistributedFs::wire_lock_release_kind(ReleaseKind::PosixOwner) as i32,
        },
    )
    .unwrap();

    let conflict =
        <DistributedFs as crate::node::vfs::dfs::owner_api::DfsOwnerLifecycleHandler>::get_lock(
            &fs,
            "node-b",
            afs_protocol::node_control::DfsOwnerGetLockRequest {
                authority: Some(authority.clone()),
                lock: Some(DistributedFs::wire_lock_request(&LockRequest::write(
                    FileLockKind::Posix,
                    lock_owner("mount-c", 71),
                    71,
                    lock_range(0, 99),
                ))),
            },
        )
        .unwrap()
        .conflict
        .map(DistributedFs::file_lock_conflict_from_wire)
        .transpose()
        .unwrap()
        .expect("forged release must not drop another process session's lock");
    assert_eq!(conflict.owner, lock.owner);

    <DistributedFs as crate::node::vfs::dfs::owner_api::DfsOwnerLifecycleHandler>::release_locks(
        &fs,
        "node-b",
        afs_protocol::node_control::DfsOwnerReleaseLocksRequest {
            authority: Some(authority.clone()),
            owner: Some(DistributedFs::wire_lock_owner(&lock.owner)),
            release_kind: DistributedFs::wire_lock_release_kind(ReleaseKind::PosixOwner) as i32,
        },
    )
    .unwrap();
    assert!(
        <DistributedFs as crate::node::vfs::dfs::owner_api::DfsOwnerLifecycleHandler>::get_lock(
            &fs,
            "node-b",
            afs_protocol::node_control::DfsOwnerGetLockRequest {
                authority: Some(authority),
                lock: Some(DistributedFs::wire_lock_request(&LockRequest::write(
                    FileLockKind::Posix,
                    lock_owner("mount-c", 71),
                    71,
                    lock_range(0, 99),
                ))),
            },
        )
        .unwrap()
        .conflict
        .is_none()
    );
}

#[test]
fn dfs_remote_lock_session_presence_is_not_refcounted_by_lock_count() {
    let (_temp, _meta, fs) = test_fs();
    let inode_id = InodeId::new("inode:remote-presence");
    assert!(fs.note_remote_lock_session(&inode_id, "mount-a").unwrap());
    assert!(!fs.note_remote_lock_session(&inode_id, "mount-a").unwrap());
    let sessions = fs.locks.proxy.authority_sessions.lock().unwrap();
    assert_eq!(sessions.get(&inode_id).unwrap().get("mount-a"), Some(&1));
    drop(sessions);
    fs.forget_remote_lock_session(&inode_id, "mount-a").unwrap();
    assert!(
        !fs.locks
            .proxy
            .authority_sessions
            .lock()
            .unwrap()
            .contains_key(&inode_id)
    );
}

#[test]
fn remote_interrupted_lock_wait_does_not_issue_redundant_cancel_ack() {
    let (_temp, meta, fs_raw) = test_fs();
    {
        let mut lease = meta.lease.lock().unwrap();
        lease.owner_node_id = "node-c".into();
        lease.owner_session_id = "session-c".into();
        lease.expires_at_unix_ms = u64::MAX;
    }
    let remote = Arc::new(InterruptedSetLockRemoteDfsOwner {
        cancel_calls: AtomicUsize::new(0),
        acknowledge_calls: AtomicUsize::new(0),
    });
    let fs = fs_raw.with_remote_owner_factory(Arc::new(StaticRemoteOwnerFactory {
        owner: remote.clone() as Arc<dyn RemoteDfsOwner>,
    }));
    let inode = fs.backend_inode(&InodeId::new("inode:test")).unwrap();
    let handle = fs.open(&context(), inode, libc::O_RDONLY).unwrap();
    let error = fs
        .setlk(
            &context(),
            inode,
            handle,
            LockRequest::read(
                FileLockKind::Posix,
                lock_owner("mount-b", 94),
                94,
                lock_range(0, 99),
            ),
            Some(LockWaiterId {
                ingress_session_id: "mount-b".into(),
                request_id: 94,
            }),
        )
        .unwrap_err();
    assert_eq!(error.code(), afs_error::IO_INTERRUPTED);
    assert_eq!(remote.cancel_call_count(), 0);
    assert_eq!(remote.acknowledge_call_count(), 0);
    let scoped_waiter = LockWaiterId {
        ingress_session_id: DistributedFs::lock_scope("node-a", "session-a", "mount-b").unwrap(),
        request_id: 94,
    };
    assert!(
        fs.locks
            .proxy
            .waiters
            .lock()
            .unwrap()
            .contains_key(&scoped_waiter)
    );
    assert!(
        fs.locks
            .proxy
            .cancelled_waiters
            .lock()
            .unwrap()
            .contains(&scoped_waiter)
    );

    assert_eq!(fs.reap_expired_peer_lock_sessions().unwrap(), 1);
    assert_eq!(remote.acknowledge_call_count(), 1);
    assert!(
        !fs.locks
            .proxy
            .waiters
            .lock()
            .unwrap()
            .contains_key(&scoped_waiter)
    );
    assert!(
        !fs.locks
            .proxy
            .cancelled_waiters
            .lock()
            .unwrap()
            .contains(&scoped_waiter)
    );
}

#[test]
fn remote_cancel_ack_failure_retains_waiter_for_background_retry() {
    let (_temp, meta, fs_raw) = test_fs();
    {
        let mut lease = meta.lease.lock().unwrap();
        lease.owner_node_id = "node-c".into();
        lease.owner_session_id = "session-c".into();
        lease.expires_at_unix_ms = u64::MAX;
    }
    let remote = Arc::new(CancelAckFailsOnceRemoteDfsOwner {
        cancel_calls: AtomicUsize::new(0),
        acknowledge_calls: AtomicUsize::new(0),
    });
    let fs = fs_raw.with_remote_owner_factory(Arc::new(StaticRemoteOwnerFactory {
        owner: remote.clone() as Arc<dyn RemoteDfsOwner>,
    }));
    let inode = fs.backend_inode(&InodeId::new("inode:test")).unwrap();
    let handle = fs.open(&context(), inode, libc::O_RDONLY).unwrap();
    let error = fs
        .setlk(
            &context(),
            inode,
            handle,
            LockRequest::read(
                FileLockKind::Posix,
                lock_owner("mount-b", 95),
                95,
                lock_range(0, 99),
            ),
            Some(LockWaiterId {
                ingress_session_id: "mount-b".into(),
                request_id: 95,
            }),
        )
        .unwrap_err();
    assert_eq!(error.code(), afs_error::NODE_VFS_UNAVAILABLE);
    assert_eq!(remote.cancel_call_count(), 1);
    assert_eq!(remote.acknowledge_call_count(), 1);
    let scoped_waiter = LockWaiterId {
        ingress_session_id: DistributedFs::lock_scope("node-a", "session-a", "mount-b").unwrap(),
        request_id: 95,
    };
    assert!(
        fs.locks
            .proxy
            .waiters
            .lock()
            .unwrap()
            .contains_key(&scoped_waiter)
    );
    assert!(
        fs.locks
            .proxy
            .cancelled_waiters
            .lock()
            .unwrap()
            .contains(&scoped_waiter)
    );

    assert_eq!(fs.reap_expired_peer_lock_sessions().unwrap(), 1);
    assert_eq!(remote.acknowledge_call_count(), 2);
    assert!(
        !fs.locks
            .proxy
            .waiters
            .lock()
            .unwrap()
            .contains_key(&scoped_waiter)
    );
    assert!(
        !fs.locks
            .proxy
            .cancelled_waiters
            .lock()
            .unwrap()
            .contains(&scoped_waiter)
    );
}

#[test]
fn dfs_owner_lock_session_release_is_scoped_by_authenticated_process_session() {
    let (_temp, _meta, fs) = test_fs();
    let authority = dfs_lock_authority("session-b");
    let lock = LockRequest::write(
        FileLockKind::Posix,
        lock_owner("mount-b", 80),
        80,
        lock_range(0, 99),
    );
    <DistributedFs as crate::node::vfs::dfs::owner_api::DfsOwnerLifecycleHandler>::set_lock(
        &fs,
        "node-b",
        afs_protocol::node_control::DfsOwnerSetLockRequest {
            authority: Some(authority.clone()),
            lock: Some(DistributedFs::wire_lock_request(&lock)),
            waiter: None,
        },
    )
    .unwrap();

    let mut forged_authority = authority.clone();
    forged_authority.caller_session_id = "session-other".into();
    <DistributedFs as crate::node::vfs::dfs::owner_api::DfsOwnerLifecycleHandler>::release_lock_session(
        &fs,
        "node-b",
        afs_protocol::node_control::DfsOwnerReleaseLockSessionRequest {
            authority: Some(forged_authority),
            ingress_session_id: "mount-b".into(),
        },
    )
    .unwrap();
    assert!(
        <DistributedFs as crate::node::vfs::dfs::owner_api::DfsOwnerLifecycleHandler>::get_lock(
            &fs,
            "node-b",
            afs_protocol::node_control::DfsOwnerGetLockRequest {
                authority: Some(authority.clone()),
                lock: Some(DistributedFs::wire_lock_request(&LockRequest::write(
                    FileLockKind::Posix,
                    lock_owner("mount-c", 81),
                    81,
                    lock_range(0, 99),
                ))),
            },
        )
        .unwrap()
        .conflict
        .is_some()
    );

    <DistributedFs as crate::node::vfs::dfs::owner_api::DfsOwnerLifecycleHandler>::release_lock_session(
        &fs,
        "node-b",
        afs_protocol::node_control::DfsOwnerReleaseLockSessionRequest {
            authority: Some(authority.clone()),
            ingress_session_id: "mount-b".into(),
        },
    )
    .unwrap();
    assert!(
        <DistributedFs as crate::node::vfs::dfs::owner_api::DfsOwnerLifecycleHandler>::get_lock(
            &fs,
            "node-b",
            afs_protocol::node_control::DfsOwnerGetLockRequest {
                authority: Some(authority),
                lock: Some(DistributedFs::wire_lock_request(&LockRequest::write(
                    FileLockKind::Posix,
                    lock_owner("mount-c", 81),
                    81,
                    lock_range(0, 99),
                ))),
            },
        )
        .unwrap()
        .conflict
        .is_none()
    );
}

#[test]
fn dfs_owner_cancel_before_authority_precancels_replayed_waiter() {
    let (_temp, _meta, fs) = test_fs();
    let authority = dfs_lock_authority("session-b");
    let waiter = afs_protocol::node_control::DfsOwnerLockWaiter {
        ingress_session_id: "mount-b".into(),
        request_id: 91,
    };
    let cancel =
        <DistributedFs as crate::node::vfs::dfs::owner_api::DfsOwnerLifecycleHandler>::cancel_lock_wait(
            &fs,
            "node-b",
            afs_protocol::node_control::DfsOwnerCancelLockWaitRequest {
                authority: Some(authority.clone()),
                waiter: Some(waiter.clone()),
            },
        )
        .unwrap();
    assert_eq!(cancel.outcome, 1);

    let lock = LockRequest::write(
        FileLockKind::Posix,
        lock_owner("mount-b", 91),
        91,
        lock_range(0, 99),
    );
    let error =
        <DistributedFs as crate::node::vfs::dfs::owner_api::DfsOwnerLifecycleHandler>::set_lock(
            &fs,
            "node-b",
            afs_protocol::node_control::DfsOwnerSetLockRequest {
                authority: Some(authority),
                lock: Some(DistributedFs::wire_lock_request(&lock)),
                waiter: Some(waiter),
            },
        )
        .unwrap_err();
    assert_eq!(error.code(), afs_error::IO_INTERRUPTED);
}

#[test]
fn interrupted_owner_lock_wait_does_not_run_post_mutation_renewal() {
    let (_temp, meta, fs_raw) = test_fs();
    let fs = Arc::new(fs_raw);
    let authority = dfs_lock_authority("session-b");
    let inode_id = InodeId::new("inode:test");
    let held = LockRequest::write(
        FileLockKind::Posix,
        lock_owner("mount-b", 92),
        92,
        lock_range(0, 99),
    );
    <DistributedFs as crate::node::vfs::dfs::owner_api::DfsOwnerLifecycleHandler>::set_lock(
        fs.as_ref(),
        "node-b",
        afs_protocol::node_control::DfsOwnerSetLockRequest {
            authority: Some(authority.clone()),
            lock: Some(DistributedFs::wire_lock_request(&held)),
            waiter: None,
        },
    )
    .unwrap();
    let renews_after_grant = meta.renew_call_count();
    let waiter = afs_protocol::node_control::DfsOwnerLockWaiter {
        ingress_session_id: "mount-b".into(),
        request_id: 93,
    };
    let blocked = LockRequest::write(
        FileLockKind::Posix,
        lock_owner("mount-b", 93),
        93,
        lock_range(0, 99),
    );
    let fs_for_waiter = fs.clone();
    let authority_for_waiter = authority.clone();
    let waiter_for_thread = waiter.clone();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let join = std::thread::spawn(move || {
        let result =
            <DistributedFs as crate::node::vfs::dfs::owner_api::DfsOwnerLifecycleHandler>::set_lock(
                fs_for_waiter.as_ref(),
                "node-b",
                afs_protocol::node_control::DfsOwnerSetLockRequest {
                    authority: Some(authority_for_waiter),
                    lock: Some(DistributedFs::wire_lock_request(&blocked)),
                    waiter: Some(waiter_for_thread),
                },
            )
            .map(|_| ());
        let _ = done_tx.send(result);
    });
    let mut registered = false;
    for _ in 0..100 {
        registered = fs
            .locks
            .authorities
            .values_snapshot()
            .unwrap()
            .iter()
            .any(|authority| !authority.state.lock().unwrap().waiters.is_empty());
        if registered {
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(
        registered,
        "waiter should be registered before cancellation"
    );
    let local_authority = fs
        .locks
        .authorities
        .read(&inode_id)
        .unwrap()
        .get(&inode_id)
        .cloned()
        .expect("owner lock authority must exist");
    local_authority
        .state
        .lock()
        .unwrap()
        .lease
        .expires_at_unix_ms = now_unix_ms().saturating_add(4_000);
    let renews_after_waiter_registered = meta.renew_call_count();
    let pause = meta.pause_next_renew();
    <DistributedFs as crate::node::vfs::dfs::owner_api::DfsOwnerLifecycleHandler>::cancel_lock_wait(
        fs.as_ref(),
        "node-b",
        afs_protocol::node_control::DfsOwnerCancelLockWaitRequest {
            authority: Some(authority),
            waiter: Some(waiter),
        },
    )
    .unwrap();
    let result = match done_rx.recv_timeout(Duration::from_millis(100)) {
        Ok(result) => result,
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            pause.release();
            join.join().unwrap();
            panic!("interrupted waiter was blocked behind post-mutation lock renewal");
        }
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            panic!("interrupted waiter thread exited without reporting a result");
        }
    };
    join.join().unwrap();
    let error = result.expect_err("interrupted waiter must return an error");
    assert_eq!(error.code(), afs_error::IO_INTERRUPTED);
    assert_eq!(meta.renew_call_count(), renews_after_waiter_registered);
    assert!(!pause.was_reached());
    assert!(meta.renew_call_count() >= renews_after_grant);
}

#[test]
fn dfs_owner_lock_waiter_must_share_owner_ingress_scope() {
    let (_temp, _meta, fs) = test_fs();
    let authority = dfs_lock_authority("session-b");
    let lock = LockRequest::write(
        FileLockKind::Posix,
        lock_owner("mount-b", 90),
        90,
        lock_range(0, 99),
    );
    let error =
        <DistributedFs as crate::node::vfs::dfs::owner_api::DfsOwnerLifecycleHandler>::set_lock(
            &fs,
            "node-b",
            afs_protocol::node_control::DfsOwnerSetLockRequest {
                authority: Some(authority),
                lock: Some(DistributedFs::wire_lock_request(&lock)),
                waiter: Some(afs_protocol::node_control::DfsOwnerLockWaiter {
                    ingress_session_id: "mount-other".into(),
                    request_id: 90,
                }),
            },
        )
        .unwrap_err();
    assert_eq!(error.code(), afs_error::NODE_VFS_INVALID);
}

#[test]
fn dfs_writable_reopen_uses_fresh_authority_without_legacy_open_write() {
    let (_temp, meta, fs) = test_fs();
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("write-authority-reopen.bin"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    fs.release(&context(), created.handle).unwrap();

    let opens_before = meta.open_write_calls.load(Ordering::SeqCst);
    let reopened = fs
        .open(&context(), created.entry.inode, libc::O_RDWR)
        .unwrap();
    fs.release(&context(), reopened).unwrap();

    assert_eq!(
        meta.open_write_calls.load(Ordering::SeqCst),
        opens_before,
        "new writable opens must resolve fresh write authority without invoking legacy OpenWrite"
    );
    assert_eq!(meta.resolve_write_calls.load(Ordering::SeqCst), 1);
}

#[test]
fn dfs_writable_open_near_expiry_clean_state_reacquires_and_renews() {
    let (_temp, meta, fs) = test_fs();
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("near-expiry-clean-reacquire.bin"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    fs.release(&context(), created.handle).unwrap();
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    let near_expiry = now_unix_ms().saturating_add(1_000);
    {
        let mut lease = meta.lease.lock().unwrap();
        lease.expires_at_unix_ms = near_expiry;
    }
    {
        let state = fs.write_state(&inode_id).unwrap().unwrap();
        {
            let mut locked = state.lock().unwrap();
            locked.write_lease.expires_at_unix_ms = near_expiry;
            locked.last_writer_background_requested = false;
            assert!(DistributedFs::write_state_can_adopt_fresh_lease(&locked));
        }
        assert!(DistributedFs::write_state_should_reacquire_clean_lease(&state).unwrap());
    }
    let renewed_expiry = now_unix_ms().saturating_add(30_000);
    meta.queue_renew_result(Ok(meta.lease_with_expiry(renewed_expiry)));
    let opens_before = meta.open_write_calls.load(Ordering::SeqCst);
    let resolves_before = meta.resolve_write_calls.load(Ordering::SeqCst);
    let renews_before = meta.renew_call_count();

    let reopened = fs
        .open(&context(), created.entry.inode, libc::O_RDWR)
        .unwrap();

    assert_eq!(
        meta.resolve_write_calls.load(Ordering::SeqCst),
        resolves_before + 1
    );
    assert_eq!(
        meta.open_write_calls.load(Ordering::SeqCst),
        opens_before + 1,
        "clean near-expiry writable open must preserve existing ensure_inode_write_state reacquire"
    );
    assert_eq!(meta.renew_call_count(), renews_before + 1);
    let state = fs.write_state(&inode_id).unwrap().unwrap();
    assert!(state.lock().unwrap().write_lease.expires_at_unix_ms >= renewed_expiry);
    fs.release(&context(), reopened).unwrap();
}

#[test]
fn dfs_writable_open_near_expiry_protected_state_renews_without_reacquire() {
    let (_temp, meta, fs) = test_fs();
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("near-expiry-protected-renew.bin"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    let near_expiry = now_unix_ms().saturating_add(1_000);
    {
        let mut lease = meta.lease.lock().unwrap();
        lease.expires_at_unix_ms = near_expiry;
    }
    {
        let state = fs.write_state(&inode_id).unwrap().unwrap();
        state.lock().unwrap().write_lease.expires_at_unix_ms = near_expiry;
    }
    let renewed_expiry = now_unix_ms().saturating_add(30_000);
    meta.queue_renew_result(Ok(meta.lease_with_expiry(renewed_expiry)));
    let opens_before = meta.open_write_calls.load(Ordering::SeqCst);
    let resolves_before = meta.resolve_write_calls.load(Ordering::SeqCst);
    let renews_before = meta.renew_call_count();

    let reopened = fs
        .open(&context(), created.entry.inode, libc::O_RDWR)
        .unwrap();

    assert_eq!(
        meta.resolve_write_calls.load(Ordering::SeqCst),
        resolves_before + 1
    );
    assert_eq!(
        meta.open_write_calls.load(Ordering::SeqCst),
        opens_before,
        "protected near-expiry writable state must renew current state instead of reacquiring"
    );
    assert_eq!(meta.renew_call_count(), renews_before + 1);
    let state = fs.write_state(&inode_id).unwrap().unwrap();
    let state = state.lock().unwrap();
    assert_eq!(state.write_lease.lease_epoch, 1);
    assert!(state.write_lease.expires_at_unix_ms >= renewed_expiry);
    drop(state);
    fs.release(&context(), reopened).unwrap();
    fs.release(&context(), created.handle).unwrap();
}

#[test]
fn dfs_readonly_lock_handle_resolves_fresh_authority_without_writable_open() {
    let (_temp, meta, fs) = test_fs();
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("readonly-lock.bin"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    let readonly = fs
        .open(&context(), created.entry.inode, libc::O_RDONLY)
        .unwrap();
    let opens_before = meta.open_write_calls.load(Ordering::SeqCst);
    fs.setlk(
        &context(),
        created.entry.inode,
        readonly,
        LockRequest::read(
            FileLockKind::Posix,
            lock_owner("mount-a", 201),
            201,
            lock_range(0, 99),
        ),
        None,
    )
    .unwrap();
    for _ in 0..64 {
        assert!(
            fs.getlk(
                &context(),
                created.entry.inode,
                created.handle,
                LockRequest::write(
                    FileLockKind::Posix,
                    lock_owner("mount-b", 202),
                    202,
                    lock_range(0, 99)
                )
            )
            .unwrap()
            .is_some()
        );
    }
    assert_eq!(meta.open_write_calls.load(Ordering::SeqCst), opens_before);
    assert_eq!(
        meta.resolve_lock_calls.load(Ordering::SeqCst),
        65,
        "every lock operation must still resolve current Meta authority"
    );
}

#[test]
fn dfs_lock_authority_epoch_replaces_old_table() {
    let (_temp, meta, fs) = test_fs();
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("lock-epoch.bin"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    let owner_a = lock_owner("mount-a", 30);
    fs.setlk(
        &context(),
        created.entry.inode,
        created.handle,
        LockRequest::write(FileLockKind::Posix, owner_a, 30, lock_range(0, 99)),
        None,
    )
    .unwrap();
    {
        let mut lease = meta.lease.lock().unwrap();
        lease.lease_epoch = lease.lease_epoch.saturating_add(1);
    }
    let conflict = fs
        .getlk(
            &context(),
            created.entry.inode,
            created.handle,
            LockRequest::write(
                FileLockKind::Posix,
                lock_owner("mount-b", 31),
                31,
                lock_range(0, 99),
            ),
        )
        .unwrap();
    assert!(
        conflict.is_none(),
        "old epoch locks must not survive fencing"
    );
}

#[test]
fn dfs_lock_renewal_transient_error_preserves_locks_and_waiters() {
    let (_temp, meta, fs) = test_fs();
    let inode_id = InodeId::new("inode:test");
    {
        let mut lease = meta.lease.lock().unwrap();
        lease.expires_at_unix_ms = now_unix_ms().saturating_add(4_000);
    }
    let authority = fs
        .local_lock_authority(&inode_id, meta.lease.lock().unwrap().clone())
        .unwrap();
    let owner = lock_owner("mount-a", 40);
    let lock = LockRequest::write(FileLockKind::Posix, owner.clone(), 40, lock_range(0, 99));
    let waiter = LockWaiterId {
        ingress_session_id: "mount-b".into(),
        request_id: 41,
    };
    authority.table.setlk_nonblocking(lock).unwrap();
    {
        let mut state = authority.state.lock().unwrap();
        state.pinned_owners.insert(owner.clone());
        state.waiters.insert(waiter.clone());
    }

    meta.fail_next_renew_with(Error::coded(
        afs_error::META_DFS_LEASE_RETRY,
        "injected retryable renewal contention",
    ));
    assert_eq!(
        fs.check_lock_renewal(&authority).unwrap_err().code(),
        afs_error::META_DFS_LEASE_RETRY
    );

    let state = authority.state.lock().unwrap();
    assert!(state.last_error.is_none());
    assert!(state.pinned_owners.contains(&owner));
    assert!(state.waiters.contains(&waiter));
    drop(state);
    assert!(
        authority
            .table
            .getlk(&LockRequest::write(
                FileLockKind::Posix,
                lock_owner("mount-c", 42),
                42,
                lock_range(0, 99),
            ))
            .unwrap()
            .is_some(),
        "retryable renewal errors must not clear the active lock table"
    );
}

#[test]
fn dfs_lock_authority_refresh_waits_for_renewal_guard_and_preserves_terminal_state() {
    let (_temp, meta, fs) = test_fs();
    let fs = Arc::new(fs);
    let inode_id = InodeId::new("inode:test");
    let authority = fs
        .local_lock_authority(&inode_id, meta.lease.lock().unwrap().clone())
        .unwrap();
    let renewal = lock_authority_renewal_guard(&authority).unwrap();
    let worker_fs = fs.clone();
    let worker_inode = inode_id.clone();
    let worker_lease = meta.lease_with_expiry(now_unix_ms().saturating_add(30_000));
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        done_tx
            .send(worker_fs.local_lock_authority(&worker_inode, worker_lease))
            .unwrap();
    });
    started_rx.recv().unwrap();
    assert!(
        done_rx.recv_timeout(Duration::from_millis(50)).is_err(),
        "same-epoch authority refresh must serialize behind an in-flight renewal decision"
    );
    {
        let mut state = authority.state.lock().unwrap();
        state.last_error = Some(stale("foreground renewal fenced authority"));
    }
    let _ = authority.table.invalidate();
    drop(renewal);
    assert_eq!(
        done_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .err()
            .expect("same-epoch refresh must remain fenced")
            .code(),
        afs_error::NODE_DFS_STALE_HANDLE,
        "same-epoch refresh must not revive a terminal authority once the serialized renewal finishes"
    );
    worker.join().unwrap();
    assert!(authority.state.lock().unwrap().last_error.is_some());
    assert!(matches!(
        authority.table.getlk(&LockRequest::write(
            FileLockKind::Posix,
            lock_owner("mount-b", 39),
            39,
            lock_range(0, 99),
        )),
        Err(LockError::Interrupted)
    ));
}

#[test]
fn dfs_lock_renewal_retry_after_expiry_records_local_stale_error() {
    let (_temp, meta, fs) = test_fs();
    let inode_id = InodeId::new("inode:test");
    let attempted = meta.lease.lock().unwrap().clone();
    let authority = fs
        .local_lock_authority(&inode_id, attempted.clone())
        .unwrap();
    let expired_at = now_unix_ms().saturating_sub(1);
    {
        let mut state = authority.state.lock().unwrap();
        state.lease.expires_at_unix_ms = expired_at;
        assert_eq!(state.lease.expires_at_unix_ms, expired_at);
    }
    let mut attempted = attempted;
    attempted.expires_at_unix_ms = expired_at;

    assert_eq!(
        apply_lock_renewal_failure(
            &authority,
            &attempted,
            Error::coded(
                afs_error::META_DFS_LEASE_RETRY,
                "retry exhausted after expiry"
            ),
        )
        .unwrap()
        .unwrap()
        .code(),
        afs_error::NODE_DFS_STALE_HANDLE
    );
    assert_eq!(
        authority
            .state
            .lock()
            .unwrap()
            .last_error
            .as_ref()
            .unwrap()
            .code(),
        afs_error::NODE_DFS_STALE_HANDLE,
        "local expiry is the terminal cause; a retryable Meta error must not become the terminal lock state"
    );
}

#[test]
fn dfs_lock_expiry_waits_for_inflight_renewal_completion() {
    let (_temp, meta, fs) = test_fs();
    let inode_id = InodeId::new("inode:test");
    let authority = fs
        .local_lock_authority(&inode_id, meta.lease.lock().unwrap().clone())
        .unwrap();
    let expired_at = now_unix_ms().saturating_sub(1);
    {
        let mut state = authority.state.lock().unwrap();
        state.lease.expires_at_unix_ms = expired_at;
        assert_eq!(state.lease.expires_at_unix_ms, expired_at);
    }
    let renewal = lock_authority_renewal_guard(&authority).unwrap();
    let worker_authority = authority.clone();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        let _renewal = lock_authority_renewal_guard(&worker_authority).unwrap();
        done_tx
            .send(expire_lock_authority_if_due_locked(&worker_authority).unwrap())
            .unwrap();
    });
    started_rx.recv().unwrap();
    assert!(
        done_rx.recv_timeout(Duration::from_millis(50)).is_err(),
        "background expiry must not complete while a foreground renewal owns the authority guard"
    );
    authority.state.lock().unwrap().lease.expires_at_unix_ms = now_unix_ms().saturating_add(30_000);
    drop(renewal);
    assert!(
        !done_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        "a foreground renewal that extends the same authority before releasing the guard prevents stale expiry"
    );
    worker.join().unwrap();
    assert!(authority.state.lock().unwrap().last_error.is_none());
}

#[test]
fn dfs_lock_renewal_stale_retry_error_does_not_fence_extended_state() {
    let (_temp, meta, fs) = test_fs();
    let inode_id = InodeId::new("inode:test");
    let attempted = {
        let mut lease = meta.lease.lock().unwrap();
        lease.expires_at_unix_ms = now_unix_ms().saturating_add(1_000);
        lease.clone()
    };
    let authority = fs
        .local_lock_authority(&inode_id, attempted.clone())
        .unwrap();
    let owner = lock_owner("mount-a", 45);
    let lock = LockRequest::write(FileLockKind::Posix, owner.clone(), 45, lock_range(0, 99));
    authority.table.setlk_nonblocking(lock).unwrap();
    {
        let mut state = authority.state.lock().unwrap();
        state.pinned_owners.insert(owner.clone());
        state.lease.expires_at_unix_ms = now_unix_ms().saturating_add(30_000);
    }

    assert!(
        apply_lock_renewal_failure(
            &authority,
            &attempted,
            Error::coded(afs_error::META_DFS_LEASE_RETRY, "stale renewal retry"),
        )
        .unwrap()
        .is_none()
    );

    let state = authority.state.lock().unwrap();
    assert!(state.last_error.is_none());
    assert!(state.pinned_owners.contains(&owner));
    drop(state);
    assert!(
        authority
            .table
            .getlk(&LockRequest::write(
                FileLockKind::Posix,
                lock_owner("mount-c", 46),
                46,
                lock_range(0, 99),
            ))
            .unwrap()
            .is_some(),
        "a stale renewal response must not invalidate a fresher local authority"
    );
}

#[test]
fn dfs_lock_renewal_confirmed_fence_invalidates_same_identity_extended_state() {
    let (_temp, meta, fs) = test_fs();
    let inode_id = InodeId::new("inode:test");
    let attempted = {
        let mut lease = meta.lease.lock().unwrap();
        lease.expires_at_unix_ms = now_unix_ms().saturating_add(1_000);
        lease.clone()
    };
    let authority = fs
        .local_lock_authority(&inode_id, attempted.clone())
        .unwrap();
    {
        let mut state = authority.state.lock().unwrap();
        state.lease.expires_at_unix_ms = now_unix_ms().saturating_add(30_000);
    }

    assert_eq!(
        apply_lock_renewal_failure(
            &authority,
            &attempted,
            Error::coded(afs_error::META_DFS_CONFLICT, "confirmed lease fencing"),
        )
        .unwrap()
        .unwrap()
        .code(),
        afs_error::META_DFS_CONFLICT
    );

    assert_eq!(
        authority
            .state
            .lock()
            .unwrap()
            .last_error
            .as_ref()
            .unwrap()
            .code(),
        afs_error::META_DFS_CONFLICT
    );
    assert!(matches!(
        authority.table.getlk(&LockRequest::write(
            FileLockKind::Posix,
            lock_owner("mount-c", 48),
            48,
            lock_range(0, 99),
        )),
        Err(LockError::Interrupted)
    ));
}

#[test]
fn dfs_local_setlk_postgrant_transient_renewal_keeps_lock() {
    let (_temp, meta, fs) = test_fs();
    let near_expiry = now_unix_ms().saturating_add(1_000);
    {
        let mut lease = meta.lease.lock().unwrap();
        lease.expires_at_unix_ms = near_expiry;
    }
    meta.queue_renew_result(Ok(meta.lease_with_expiry(near_expiry)));
    meta.queue_renew_result(Ok(meta.lease_with_expiry(near_expiry)));
    meta.queue_renew_result(Err(Error::coded(
        afs_error::META_DFS_LEASE_RETRY,
        "post-grant renewal retry",
    )));

    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("postgrant-local-lock.bin"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    fs.setlk(
        &context(),
        created.entry.inode,
        created.handle,
        LockRequest::write(
            FileLockKind::Posix,
            lock_owner("mount-a", 48),
            48,
            lock_range(0, 99),
        ),
        None,
    )
    .unwrap();

    let authority = fs
        .locks
        .authorities
        .read(&InodeId::new("inode:test"))
        .unwrap()
        .get(&InodeId::new("inode:test"))
        .cloned()
        .expect("successful lock must keep a local authority");
    assert_eq!(
        meta.renew_call_count(),
        3,
        "local lock regression must inject the retryable error after the lock table mutation"
    );
    assert!(authority.state.lock().unwrap().last_error.is_none());
    assert!(
        authority
            .table
            .getlk(&LockRequest::write(
                FileLockKind::Posix,
                lock_owner("mount-b", 49),
                49,
                lock_range(0, 99),
            ))
            .unwrap()
            .is_some(),
        "post-grant transient renewal errors must not leave a granted lock reported as failed"
    );
}

#[test]
fn dfs_owner_set_lock_postgrant_transient_renewal_keeps_lock() {
    let (_temp, meta, fs) = test_fs();
    let near_expiry = now_unix_ms().saturating_add(1_000);
    {
        let mut lease = meta.lease.lock().unwrap();
        lease.expires_at_unix_ms = near_expiry;
    }
    meta.queue_renew_result(Ok(meta.lease_with_expiry(near_expiry)));
    meta.queue_renew_result(Ok(meta.lease_with_expiry(near_expiry)));
    meta.queue_renew_result(Err(Error::coded(
        afs_error::META_DFS_LEASE_RETRY,
        "remote post-grant renewal retry",
    )));

    let authority = dfs_lock_authority("session-b");
    let lock = LockRequest::write(
        FileLockKind::Posix,
        lock_owner("mount-b", 50),
        50,
        lock_range(0, 99),
    );
    <DistributedFs as crate::node::vfs::dfs::owner_api::DfsOwnerLifecycleHandler>::set_lock(
        &fs,
        "node-b",
        afs_protocol::node_control::DfsOwnerSetLockRequest {
            authority: Some(authority),
            lock: Some(DistributedFs::wire_lock_request(&lock)),
            waiter: None,
        },
    )
    .unwrap();

    let authority = fs
        .locks
        .authorities
        .read(&InodeId::new("inode:test"))
        .unwrap()
        .get(&InodeId::new("inode:test"))
        .cloned()
        .expect("successful owner lock must keep a local authority");
    assert_eq!(
        meta.renew_call_count(),
        3,
        "remote owner lock regression must inject the retryable error after the owner table mutation"
    );
    assert!(authority.state.lock().unwrap().last_error.is_none());
    assert!(
        authority
            .table
            .getlk(&LockRequest::write(
                FileLockKind::Posix,
                lock_owner("mount-c", 51),
                51,
                lock_range(0, 99),
            ))
            .unwrap()
            .is_some(),
        "remote post-grant transient renewal errors must preserve the installed owner lock"
    );
}

#[test]
fn dfs_lock_renewal_success_accepts_live_response_after_old_expiry() {
    let (_temp, meta, fs) = test_fs();
    let inode_id = InodeId::new("inode:test");
    let expired = {
        let mut lease = meta.lease.lock().unwrap();
        lease.expires_at_unix_ms = now_unix_ms().saturating_sub(1);
        lease.clone()
    };
    let authority = fs.local_lock_authority(&inode_id, expired).unwrap();
    let renewed_expiry = now_unix_ms().saturating_add(30_000);

    assert!(
        apply_lock_renewal_success(&authority, meta.lease_with_expiry(renewed_expiry))
            .unwrap()
            .is_none(),
        "a same-identity live renewal proves continuous authority even if the old local snapshot expired"
    );
    let state = authority.state.lock().unwrap();
    assert!(state.last_error.is_none());
    assert!(state.lease.expires_at_unix_ms >= renewed_expiry);
}

#[test]
fn dfs_lock_renewal_old_success_hint_does_not_shorten_newer_state() {
    let (_temp, meta, fs) = test_fs();
    let inode_id = InodeId::new("inode:test");
    let current_expiry = now_unix_ms().saturating_add(30_000);
    let authority = fs
        .local_lock_authority(&inode_id, meta.lease_with_expiry(current_expiry))
        .unwrap();
    let stale_success_expiry = now_unix_ms().saturating_sub(1);

    assert!(
        apply_lock_renewal_success(&authority, meta.lease_with_expiry(stale_success_expiry))
            .unwrap()
            .is_none()
    );
    let state = authority.state.lock().unwrap();
    assert!(state.last_error.is_none());
    assert_eq!(state.lease.expires_at_unix_ms, current_expiry);
}

#[test]
fn dfs_lock_renewal_success_does_not_resurrect_terminal_authority() {
    let (_temp, meta, fs) = test_fs();
    let inode_id = InodeId::new("inode:test");
    let attempted = meta.lease_with_expiry(now_unix_ms().saturating_add(1_000));
    let authority = fs
        .local_lock_authority(&inode_id, attempted.clone())
        .unwrap();
    authority
        .table
        .setlk_nonblocking(LockRequest::write(
            FileLockKind::Posix,
            lock_owner("mount-a", 52),
            52,
            lock_range(0, 99),
        ))
        .unwrap();
    apply_lock_renewal_failure(
        &authority,
        &attempted,
        Error::coded(afs_error::META_DFS_CONFLICT, "previous confirmed fencing"),
    )
    .unwrap()
    .unwrap();
    let original_expiry = authority.state.lock().unwrap().lease.expires_at_unix_ms;
    let renewed_expiry = now_unix_ms().saturating_add(30_000);

    assert_eq!(
        apply_lock_renewal_success(&authority, meta.lease_with_expiry(renewed_expiry))
            .unwrap()
            .unwrap()
            .code(),
        afs_error::META_DFS_CONFLICT
    );
    let state = authority.state.lock().unwrap();
    assert_eq!(state.lease.expires_at_unix_ms, original_expiry);
    assert_eq!(
        state.last_error.as_ref().unwrap().code(),
        afs_error::META_DFS_CONFLICT
    );
    assert!(state.pinned_owners.is_empty());
    drop(state);
    assert!(matches!(
        authority.table.getlk(&LockRequest::write(
            FileLockKind::Posix,
            lock_owner("mount-b", 53),
            53,
            lock_range(0, 99),
        )),
        Err(LockError::Interrupted)
    ));
}

#[test]
fn dfs_lock_renewal_terminal_fence_invalidates_locks_and_waiters() {
    let (_temp, meta, fs) = test_fs();
    let inode_id = InodeId::new("inode:test");
    {
        let mut lease = meta.lease.lock().unwrap();
        lease.expires_at_unix_ms = now_unix_ms().saturating_add(4_000);
    }
    let authority = fs
        .local_lock_authority(&inode_id, meta.lease.lock().unwrap().clone())
        .unwrap();
    let owner = lock_owner("mount-a", 50);
    let lock = LockRequest::write(FileLockKind::Posix, owner.clone(), 50, lock_range(0, 99));
    let waiter = LockWaiterId {
        ingress_session_id: "mount-b".into(),
        request_id: 51,
    };
    authority.table.setlk_nonblocking(lock).unwrap();
    {
        let mut state = authority.state.lock().unwrap();
        state.pinned_owners.insert(owner);
        state.waiters.insert(waiter);
    }

    meta.fail_next_renew_with(Error::coded(
        afs_error::META_DFS_CONFLICT,
        "injected lease fencing",
    ));
    assert_eq!(
        fs.check_lock_renewal(&authority).unwrap_err().code(),
        afs_error::META_DFS_CONFLICT
    );

    let state = authority.state.lock().unwrap();
    assert!(state.last_error.is_some());
    assert!(state.pinned_owners.is_empty());
    assert!(state.waiters.is_empty());
    drop(state);
    assert!(matches!(
        authority.table.getlk(&LockRequest::write(
            FileLockKind::Posix,
            lock_owner("mount-c", 52),
            52,
            lock_range(0, 99),
        )),
        Err(LockError::Interrupted)
    ));
}

#[test]
fn dfs_lock_background_renewal_starts_with_retry_budget() {
    let mut lease = WriteLease {
        inode_id: InodeId::new("inode:test"),
        owner_node_id: "node-a".into(),
        owner_session_id: "session-a".into(),
        lease_epoch: 1,
        expires_at_unix_ms: now_unix_ms().saturating_add(21_000),
    };
    assert!(!should_background_renew(&lease));
    assert!(!should_renew(&lease));

    lease.expires_at_unix_ms = now_unix_ms().saturating_add(19_000);
    assert!(should_background_renew(&lease));
    assert!(!should_renew(&lease));
}

#[test]
fn dfs_release_locks_without_authority_does_not_open_write() {
    let (_temp, _meta, fs) = test_fs();
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("release-no-lock.bin"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    fs.release_locks(
        &context(),
        created.entry.inode,
        created.handle,
        lock_owner("mount-a", 20),
        ReleaseKind::PosixOwner,
    )
    .unwrap();
    assert!(fs.locks.authorities.is_empty());
    assert!(fs.locks.proxy.authorities.lock().unwrap().is_empty());
}

#[test]
fn dfs_posix_lock_checks_file_access() {
    let (_temp, _meta, fs) = test_fs();
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("lock-access.bin"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    let readonly = fs
        .open(&context(), created.entry.inode, libc::O_RDONLY)
        .unwrap();
    let writeonly = fs
        .open(&context(), created.entry.inode, libc::O_WRONLY)
        .unwrap();
    assert_eq!(
        fs.setlk(
            &context(),
            created.entry.inode,
            readonly,
            LockRequest::write(
                FileLockKind::Posix,
                lock_owner("mount-a", 21),
                21,
                lock_range(0, 0),
            ),
            None,
        )
        .unwrap_err()
        .code(),
        afs_error::IO_BAD_FILE_DESCRIPTOR
    );
    assert_eq!(
        fs.getlk(
            &context(),
            created.entry.inode,
            writeonly,
            LockRequest::read(
                FileLockKind::Posix,
                lock_owner("mount-a", 22),
                22,
                lock_range(0, 0),
            ),
        )
        .unwrap_err()
        .code(),
        afs_error::IO_BAD_FILE_DESCRIPTOR
    );
}

#[test]
fn dfs_closed_lock_session_rejects_late_waiter() {
    let (_temp, _meta, fs) = test_fs();
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("closed-lock-session.bin"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    fs.release_lock_session("mount-closed").unwrap();
    assert_eq!(
        fs.setlk(
            &context(),
            created.entry.inode,
            created.handle,
            LockRequest::write(
                FileLockKind::Posix,
                lock_owner("mount-closed", 23),
                23,
                lock_range(0, 0),
            ),
            Some(LockWaiterId {
                ingress_session_id: "mount-closed".into(),
                request_id: 1,
            }),
        )
        .unwrap_err()
        .code(),
        afs_error::IO_INTERRUPTED
    );
}
