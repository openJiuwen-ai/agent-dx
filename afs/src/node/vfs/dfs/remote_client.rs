//! Proxy side of the DFS write path: sessions against a remote inode owner.
//!
//! `WriteCoordinator` owns the remote-owner connection state, the per-inode
//! provider table and the pending release queue. It does not see local write
//! state or handles: callers pass whether local state protects a provider, and
//! `RemoteProviderHost` lists live sessions and applies handle-side effects
//! when a provider is retired.
use super::*;

/// Local handle state consulted while a remote provider is being retired.
pub(super) trait RemoteProviderHost {
    /// Remote sessions of the handles still open on the inode, in handle order.
    fn live_remote_sessions(&self, inode_id: &InodeId) -> Result<Vec<RemoteDfsWriteSession>>;
    /// Called after the provider table changed and before waiting for the
    /// retired provider's in-flight IO; `last_provider` means none remains.
    fn remote_provider_replaced(&self, inode_id: &InodeId, last_provider: bool) -> Result<()>;
}

/// Connection to remote inode owners, shared with the lock proxy client.
pub(super) struct OwnerChannel {
    identity: Arc<NodeIdentity>,
    meta: Arc<dyn DfsMeta>,
    pub(super) remote_owner_factory: Option<Arc<dyn DfsRemoteOwnerFactory>>,
    pub(super) remote_open_admission_locks:
        Mutex<HashMap<DfsOwnerOpenRouteKey, Arc<DfsRemoteOpenAdmissionLock>>>,
    next_owner_open_seq: AtomicU64,
}

pub(super) struct DfsRemoteOpenAdmissionLock {
    pub(super) gate: Mutex<()>,
    pub(super) users: AtomicUsize,
}

/// Client side of proxied writes for inodes whose lease another node holds.
pub(super) struct WriteCoordinator {
    identity: Arc<NodeIdentity>,
    meta: Arc<dyn DfsMeta>,
    pub(super) channel: OwnerChannel,
    remote_inode_providers: ShardedMap<InodeId, RemoteDfsWriteSession>,
    pub(super) pending_remote_releases: Mutex<PendingRemoteReleaseState>,
    /// The version the owner reported for this Node's latest proxied sync of
    /// each inode, consumed when the inode's last proxy session ends (R94).
    proxied_versions: Mutex<HashMap<InodeId, FileVersionId>>,
}

#[derive(Default)]
pub(super) struct RemoteProviderLifecycle {
    state: Mutex<RemoteProviderLifecycleState>,
    cv: Condvar,
}

#[derive(Default)]
struct RemoteProviderLifecycleState {
    live: bool,
    inflight: usize,
}

pub(super) struct RemoteProviderIoGuard {
    lifecycle: Arc<RemoteProviderLifecycle>,
}

impl RemoteProviderLifecycle {
    pub(super) fn live() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(RemoteProviderLifecycleState {
                live: true,
                inflight: 0,
            }),
            cv: Condvar::new(),
        })
    }

    pub(super) fn begin_io(self: &Arc<Self>) -> Result<Option<RemoteProviderIoGuard>> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| unavailable("DFS remote provider lifecycle is poisoned"))?;
        if !state.live {
            return Ok(None);
        }
        state.inflight = state.inflight.saturating_add(1);
        Ok(Some(RemoteProviderIoGuard {
            lifecycle: self.clone(),
        }))
    }

    pub(super) fn retire_new_io(&self) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| unavailable("DFS remote provider lifecycle is poisoned"))?;
        state.live = false;
        Ok(())
    }

    pub(super) fn wait_idle(&self, budget: Duration) -> Result<()> {
        let deadline = Instant::now() + budget;
        let mut state = self
            .state
            .lock()
            .map_err(|_| unavailable("DFS remote provider lifecycle is poisoned"))?;
        while state.inflight != 0 {
            let now = Instant::now();
            if now >= deadline {
                return Err(unavailable("DFS remote provider idle wait timed out"));
            }
            let remaining = deadline.saturating_duration_since(now);
            let (next_state, timeout) = self
                .cv
                .wait_timeout(state, remaining)
                .map_err(|_| unavailable("DFS remote provider lifecycle is poisoned"))?;
            state = next_state;
            if timeout.timed_out() && state.inflight != 0 {
                return Err(unavailable("DFS remote provider idle wait timed out"));
            }
        }
        Ok(())
    }

    fn is_live(&self) -> Result<bool> {
        Ok(self
            .state
            .lock()
            .map_err(|_| unavailable("DFS remote provider lifecycle is poisoned"))?
            .live)
    }

    fn is_idle(&self) -> Result<bool> {
        Ok(self
            .state
            .lock()
            .map_err(|_| unavailable("DFS remote provider lifecycle is poisoned"))?
            .inflight
            == 0)
    }
}

impl Drop for RemoteProviderIoGuard {
    fn drop(&mut self) {
        if let Ok(mut state) = self.lifecycle.state.lock() {
            state.inflight = state.inflight.saturating_sub(1);
            self.lifecycle.cv.notify_all();
        }
    }
}

#[derive(Clone)]
pub(super) struct RemoteDfsWriteSession {
    pub(super) owner: Arc<dyn RemoteDfsOwner>,
    pub(super) handle: afs_protocol::node_control::DfsOwnerHandle,
    pub(super) read_handle: Option<afs_protocol::node_control::DfsOwnerHandle>,
    release_slots: usize,
    pub(super) lifecycle: Arc<RemoteProviderLifecycle>,
}

pub(super) struct RemoteDfsProviderIo {
    pub(super) remote: RemoteDfsWriteSession,
    pub(super) _guard: RemoteProviderIoGuard,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct PendingRemoteReleaseKey {
    namespace_id: String,
    inode_id: String,
    owner_node_id: String,
    owner_session_id: String,
    lease_epoch: u64,
    caller_node_id: String,
    caller_session_id: String,
    open_seq: u64,
    opaque_handle: Vec<u8>,
}

#[derive(Clone)]
pub(super) struct PendingRemoteRelease {
    owner: Arc<dyn RemoteDfsOwner>,
    handle: afs_protocol::node_control::DfsOwnerHandle,
    lifecycle: Option<Arc<RemoteProviderLifecycle>>,
}

#[derive(Default)]
pub(super) struct PendingRemoteReleaseState {
    pub(super) entries: HashMap<PendingRemoteReleaseKey, PendingRemoteRelease>,
    order: VecDeque<PendingRemoteReleaseKey>,
    pub(super) reserved: usize,
}

#[derive(Clone)]
pub(super) struct DfsRemoteWriteSession {
    pub(super) local: DfsWriteSession,
    pub(super) remote: Option<RemoteDfsWriteSession>,
}

impl WriteCoordinator {
    pub(super) fn new(identity: Arc<NodeIdentity>, meta: Arc<dyn DfsMeta>) -> Self {
        Self {
            channel: OwnerChannel::new(identity.clone(), meta.clone()),
            identity,
            meta,
            remote_inode_providers: ShardedMap::default(),
            pending_remote_releases: Mutex::new(PendingRemoteReleaseState::default()),
            proxied_versions: Mutex::new(HashMap::new()),
        }
    }

    /// Takes the version the owner reported for the inode's latest proxied
    /// sync. Replies of concurrent syncs may arrive out of order: an older
    /// version recorded last is no longer the head, and the caller then keeps
    /// its handles where they are.
    pub(super) fn take_proxied_version(&self, inode_id: &InodeId) -> Result<Option<FileVersionId>> {
        Ok(self
            .proxied_versions
            .lock()
            .map_err(|_| unavailable("DFS proxied version table is poisoned"))?
            .remove(inode_id))
    }

    pub(super) fn open_remote_write_session(
        &self,
        inode: &InodeRecord,
        lease: &WriteLease,
        open_flags: i32,
        options: OpenOptions,
    ) -> Result<DfsRemoteWriteSession> {
        let release_slots = Self::remote_release_slot_count(open_flags);
        let owner = self.channel.remote_owner(lease)?;
        self.reserve_remote_release_capacity(release_slots)?;
        let open_request = afs_protocol::node_control::DfsOwnerOpenRequest {
            namespace_id: self.identity.namespace_id.0.clone(),
            inode_id: inode.inode_id.0.clone(),
            owner_node_id: lease.owner_node_id.clone(),
            owner_session_id: lease.owner_session_id.clone(),
            lease_epoch: lease.lease_epoch,
            caller_session_id: self.identity.session_id.clone(),
            open_flags,
            kill_suidgid: options.kill_suidgid,
            open_seq: 0,
        };
        let (_open_request, handle) = self.open_remote_owner_admitted(
            &owner,
            open_request,
            release_slots,
            "DFS owner open returned no handle",
        )?;
        let read_handle = if open_flags & libc::O_ACCMODE == libc::O_WRONLY {
            let read_open_request = afs_protocol::node_control::DfsOwnerOpenRequest {
                namespace_id: self.identity.namespace_id.0.clone(),
                inode_id: inode.inode_id.0.clone(),
                owner_node_id: lease.owner_node_id.clone(),
                owner_session_id: lease.owner_session_id.clone(),
                lease_epoch: lease.lease_epoch,
                caller_session_id: self.identity.session_id.clone(),
                open_flags: libc::O_RDONLY,
                kill_suidgid: false,
                open_seq: 0,
            };
            match self.open_remote_owner_admitted(
                &owner,
                read_open_request,
                1,
                "DFS owner read companion open returned no handle",
            ) {
                Ok((_request, read_handle)) => Some(read_handle),
                Err(error) => {
                    let release =
                        self.remote_release_handle_or_queue_reserved(&owner, handle, true);
                    release?;
                    return Err(error);
                }
            }
        } else {
            None
        };
        let remote = RemoteDfsWriteSession {
            owner,
            handle,
            read_handle,
            release_slots,
            lifecycle: RemoteProviderLifecycle::live(),
        };
        self.remote_inode_providers
            .insert(inode.inode_id.clone(), remote.clone())?;
        Ok(DfsRemoteWriteSession {
            local: DfsWriteSession {
                id: DfsWriteSessionId::new(format!(
                    "{}-remote-write-session-{}",
                    self.identity.session_id,
                    self.identity.next_operation.fetch_add(1, Ordering::Relaxed)
                )),
                inode_id: inode.inode_id.clone(),
                open_flags,
                lease_epoch: lease.lease_epoch,
                last_accepted_seq: 0,
                last_synced_seq: 0,
                error_cursor: 0,
            },
            remote: Some(remote),
        })
    }

    fn open_remote_owner_admitted(
        &self,
        owner: &Arc<dyn RemoteDfsOwner>,
        request_template: afs_protocol::node_control::DfsOwnerOpenRequest,
        reserved_slots: usize,
        missing_handle_message: &'static str,
    ) -> Result<(
        afs_protocol::node_control::DfsOwnerOpenRequest,
        afs_protocol::node_control::DfsOwnerHandle,
    )> {
        let entered = std::cell::Cell::new(false);
        let result = self.channel.with_remote_open_admission(
            &request_template.owner_node_id,
            &request_template.owner_session_id,
            || {
                entered.set(true);
                let open_seq = match self.channel.next_remote_open_seq() {
                    Ok(open_seq) => open_seq,
                    Err(error) => {
                        self.release_remote_release_capacity(reserved_slots)?;
                        return Err(error);
                    }
                };
                let mut request = request_template.clone();
                request.open_seq = open_seq;
                let reply = match owner.open(request.clone()) {
                    Ok(reply) => reply,
                    Err(error) => {
                        if Self::remote_release_error_is_retryable(&error) {
                            self.queue_pending_remote_release(
                                owner.clone(),
                                cleanup_handle_for_open_request(&self.identity.node_id, &request)?,
                                true,
                            )?;
                            if reserved_slots > 1 {
                                self.release_remote_release_capacity(reserved_slots - 1)?;
                            }
                        } else {
                            self.release_remote_release_capacity(reserved_slots)?;
                        }
                        return Err(error);
                    }
                };
                let Some(handle) = reply.handle else {
                    self.queue_pending_remote_release(
                        owner.clone(),
                        cleanup_handle_for_open_request(&self.identity.node_id, &request)?,
                        true,
                    )?;
                    if reserved_slots > 1 {
                        self.release_remote_release_capacity(reserved_slots - 1)?;
                    }
                    return Err(unavailable(missing_handle_message));
                };
                if let Err(error) = self.validate_remote_open_reply_handle(&request, &handle) {
                    self.queue_pending_remote_release(
                        owner.clone(),
                        cleanup_handle_for_open_request(&self.identity.node_id, &request)?,
                        true,
                    )?;
                    if reserved_slots > 1 {
                        self.release_remote_release_capacity(reserved_slots - 1)?;
                    }
                    return Err(error);
                }
                Ok((request, handle))
            },
        );
        if result.is_err() && !entered.get() {
            self.release_remote_release_capacity(reserved_slots)?;
        }
        result
    }

    fn validate_remote_open_reply_handle(
        &self,
        request: &afs_protocol::node_control::DfsOwnerOpenRequest,
        handle: &afs_protocol::node_control::DfsOwnerHandle,
    ) -> Result<()> {
        if handle.namespace_id != request.namespace_id
            || handle.inode_id != request.inode_id
            || handle.owner_node_id != request.owner_node_id
            || handle.owner_session_id != request.owner_session_id
            || handle.lease_epoch != request.lease_epoch
            || handle.caller_node_id != self.identity.node_id
            || handle.caller_session_id != self.identity.session_id
            || handle.open_seq != request.open_seq
            || handle.opaque_handle.is_empty()
        {
            return Err(stale("DFS owner open returned a mismatched handle"));
        }
        Ok(())
    }

    pub(super) fn remote_read(
        &self,
        provider: &RemoteDfsProviderIo,
        offset: u64,
        out: &mut [u8],
    ) -> Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }
        let remote = &provider.remote;
        let handle = remote.read_handle.as_ref().unwrap_or(&remote.handle);
        let reply = remote
            .owner
            .read(afs_protocol::node_data::DfsOwnerReadRequest {
                handle: Some(data_owner_handle(handle)),
                offset,
                length: out.len() as u64,
            })?;
        let count = reply.data.len().min(out.len());
        out[..count].copy_from_slice(&reply.data[..count]);
        Ok(count)
    }

    pub(super) fn remote_getattr(&self, provider: &RemoteDfsProviderIo) -> Result<FileAttributes> {
        let remote = &provider.remote;
        let reply = remote
            .owner
            .getattr(afs_protocol::node_control::DfsOwnerGetAttrRequest {
                handle: Some(remote.handle.clone()),
            })?;
        let attr = reply
            .attr
            .ok_or_else(|| unavailable("DFS owner getattr returned no attributes"))?;
        file_attributes_from_owner_attr(attr)
    }

    pub(super) fn remote_write(
        &self,
        session: &DfsRemoteWriteSession,
        flags: i32,
        offset: u64,
        data: &[u8],
        options: WriteOptions,
    ) -> Result<(usize, u64)> {
        let remote = session
            .remote
            .as_ref()
            .ok_or_else(|| invalid("DFS write session is local"))?;
        let operation_id = self.identity.operation_id("remote-write");
        let reply = remote
            .owner
            .write(afs_protocol::node_data::DfsOwnerWriteRequest {
                handle: Some(data_owner_handle(&remote.handle)),
                operation_id: operation_id.0,
                offset,
                data: data.to_vec(),
                append: flags & libc::O_APPEND != 0,
                kill_suidgid: options.kill_suidgid,
            })?;
        Ok((reply.written as usize, reply.accepted_write_seq))
    }

    pub(super) fn remote_resize(
        &self,
        session: &DfsRemoteWriteSession,
        length: u64,
        options: SetAttrOptions,
    ) -> Result<u64> {
        let remote = session
            .remote
            .as_ref()
            .ok_or_else(|| invalid("DFS write session is local"))?;
        let reply = remote
            .owner
            .resize(afs_protocol::node_data::DfsOwnerResizeRequest {
                handle: Some(data_owner_handle(&remote.handle)),
                operation_id: self.identity.operation_id("remote-resize").0,
                length,
                kill_suidgid: options.kill_suidgid,
            })?;
        Ok(reply.accepted_write_seq)
    }

    pub(super) fn remote_sync(
        &self,
        session: &DfsRemoteWriteSession,
        mode: SyncMode,
    ) -> Result<afs_protocol::node_data::DfsOwnerSyncReply> {
        let remote = session
            .remote
            .as_ref()
            .ok_or_else(|| invalid("DFS write session is local"))?;
        let reply = remote
            .owner
            .sync(afs_protocol::node_data::DfsOwnerSyncRequest {
                handle: Some(data_owner_handle(&remote.handle)),
                operation_id: self.identity.operation_id("remote-sync").0,
                through_write_seq: session.local.last_accepted_seq,
                data_only: matches!(mode, SyncMode::DataOnly),
            })?;
        if !reply.file_version_id.is_empty() {
            self.proxied_versions
                .lock()
                .map_err(|_| unavailable("DFS proxied version table is poisoned"))?
                .insert(
                    session.local.inode_id.clone(),
                    FileVersionId::new(reply.file_version_id.clone()),
                );
        }
        Ok(reply)
    }

    fn remote_release(&self, session: &DfsRemoteWriteSession) -> Result<()> {
        let remote = session
            .remote
            .as_ref()
            .ok_or_else(|| invalid("DFS write session is local"))?;
        let mut first_error = None;
        let mut released = 0usize;
        if let Some(read_handle) = remote.read_handle.clone() {
            released = released.saturating_add(1);
            if let Err(error) =
                self.remote_release_handle_or_queue_reserved(&remote.owner, read_handle, true)
            {
                first_error = Some(error);
            }
        }
        released = released.saturating_add(1);
        if let Err(error) =
            self.remote_release_handle_or_queue_reserved(&remote.owner, remote.handle.clone(), true)
            && first_error.is_none()
        {
            first_error = Some(error);
        }
        if remote.release_slots > released {
            self.release_remote_release_capacity(remote.release_slots - released)?;
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        Ok(())
    }

    fn remote_release_slot_count(open_flags: i32) -> usize {
        if open_flags & libc::O_ACCMODE == libc::O_WRONLY {
            2
        } else {
            1
        }
    }

    fn reserve_remote_release_capacity(&self, slots: usize) -> Result<()> {
        let mut state = self
            .pending_remote_releases
            .lock()
            .map_err(|_| unavailable("DFS pending remote release table is poisoned"))?;
        if state
            .entries
            .len()
            .saturating_add(state.reserved)
            .saturating_add(slots)
            > MAX_PENDING_REMOTE_RELEASES
        {
            return Err(Error::coded(
                afs_error::NODE_VFS_UNAVAILABLE,
                "DFS pending remote release table is full",
            ));
        }
        state.reserved = state.reserved.saturating_add(slots);
        Ok(())
    }

    fn release_remote_release_capacity(&self, slots: usize) -> Result<()> {
        if slots == 0 {
            return Ok(());
        }
        let mut state = self
            .pending_remote_releases
            .lock()
            .map_err(|_| unavailable("DFS pending remote release table is poisoned"))?;
        state.reserved = state.reserved.saturating_sub(slots);
        Ok(())
    }

    fn remote_release_handle_or_queue_reserved(
        &self,
        owner: &Arc<dyn RemoteDfsOwner>,
        handle: afs_protocol::node_control::DfsOwnerHandle,
        consume_reserved: bool,
    ) -> Result<()> {
        match Self::remote_release_handle(owner, &handle) {
            Ok(()) => {
                if consume_reserved {
                    self.release_remote_release_capacity(1)?;
                }
                Ok(())
            }
            Err(error) if Self::remote_release_error_is_retryable(&error) => {
                self.queue_pending_remote_release(owner.clone(), handle, consume_reserved)?;
                Err(error)
            }
            Err(error) => {
                if consume_reserved {
                    self.release_remote_release_capacity(1)?;
                }
                Err(error)
            }
        }
    }

    pub(super) fn queue_pending_remote_release(
        &self,
        owner: Arc<dyn RemoteDfsOwner>,
        handle: afs_protocol::node_control::DfsOwnerHandle,
        consume_reserved: bool,
    ) -> Result<()> {
        self.queue_pending_remote_release_with_lifecycle(owner, handle, consume_reserved, None)
    }

    fn queue_pending_remote_release_with_lifecycle(
        &self,
        owner: Arc<dyn RemoteDfsOwner>,
        handle: afs_protocol::node_control::DfsOwnerHandle,
        consume_reserved: bool,
        lifecycle: Option<Arc<RemoteProviderLifecycle>>,
    ) -> Result<()> {
        let key = remote_release_key(&handle);
        let mut state = self
            .pending_remote_releases
            .lock()
            .map_err(|_| unavailable("DFS pending remote release table is poisoned"))?;
        if consume_reserved {
            state.reserved = state.reserved.saturating_sub(1);
        } else if !state.entries.contains_key(&key)
            && state.entries.len().saturating_add(state.reserved) >= MAX_PENDING_REMOTE_RELEASES
        {
            return Err(Error::coded(
                afs_error::NODE_VFS_UNAVAILABLE,
                "DFS pending remote release table is full",
            ));
        }
        if !state.entries.contains_key(&key) {
            state.order.push_back(key.clone());
        }
        state.entries.entry(key).or_insert(PendingRemoteRelease {
            owner,
            handle,
            lifecycle,
        });
        Ok(())
    }

    fn queue_remote_session_release_with_lifecycle(
        &self,
        remote: &RemoteDfsWriteSession,
    ) -> Result<()> {
        let mut handles = Vec::with_capacity(remote.release_slots);
        if let Some(read_handle) = remote.read_handle.clone() {
            handles.push(read_handle);
        }
        handles.push(remote.handle.clone());

        let mut state = self
            .pending_remote_releases
            .lock()
            .map_err(|_| unavailable("DFS pending remote release table is poisoned"))?;
        let new_entries = handles
            .iter()
            .filter(|handle| {
                let key = remote_release_key(handle);
                !state.entries.contains_key(&key)
            })
            .count();
        let remaining_reserved = state.reserved.saturating_sub(handles.len());
        if state
            .entries
            .len()
            .saturating_add(new_entries)
            .saturating_add(remaining_reserved)
            > MAX_PENDING_REMOTE_RELEASES
        {
            return Err(Error::coded(
                afs_error::NODE_VFS_UNAVAILABLE,
                "DFS pending remote release table is full",
            ));
        }
        state.reserved = state.reserved.saturating_sub(handles.len());
        for handle in handles {
            let key = remote_release_key(&handle);
            if !state.entries.contains_key(&key) {
                state.order.push_back(key.clone());
            }
            state
                .entries
                .entry(key)
                .or_insert_with(|| PendingRemoteRelease {
                    owner: remote.owner.clone(),
                    handle,
                    lifecycle: Some(remote.lifecycle.clone()),
                });
        }
        Ok(())
    }

    pub(super) fn retry_pending_remote_releases(&self) -> Result<usize> {
        self.retry_pending_remote_releases_with_budget(REMOTE_RELEASE_MAINTENANCE_BUDGET)
    }

    pub(super) fn drain_pending_remote_releases(&self) -> Result<usize> {
        self.retry_pending_remote_releases_with_budget(REMOTE_RELEASE_SHUTDOWN_BUDGET)
    }

    fn retry_pending_remote_releases_with_budget(
        &self,
        budget: std::time::Duration,
    ) -> Result<usize> {
        let deadline = std::time::Instant::now() + budget;
        let mut released = 0usize;
        let mut first_error = None;
        let mut session_cache = HashMap::new();
        for (attempted, (key, pending)) in self
            .pending_remote_release_snapshot()?
            .into_iter()
            .enumerate()
        {
            if attempted >= REMOTE_RELEASE_RETRY_MAX_OPS || std::time::Instant::now() >= deadline {
                break;
            }
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            let session_key = key.owner_node_id.clone();
            let current_session = if let Some(cached) = session_cache.get(&session_key).cloned() {
                cached
            } else {
                let timeout = remaining.min(REMOTE_RELEASE_RPC_BUDGET);
                let result = crate::node::call_deadline::within_timeout(timeout, || {
                    self.meta.current_node_session(&key.owner_node_id)
                });
                session_cache.insert(session_key, result.clone());
                result
            };
            match current_session {
                Ok(Some(current)) if current == key.owner_session_id => {}
                Ok(_) => {
                    if self.remove_pending_remote_release_if_current(&key, &pending)? {
                        released = released.saturating_add(1);
                    }
                    continue;
                }
                Err(error) => {
                    first_error.get_or_insert(error);
                    self.rotate_pending_remote_release(&key)?;
                    continue;
                }
            }
            if let Some(lifecycle) = pending.lifecycle.as_ref()
                && !lifecycle.is_idle()?
            {
                self.rotate_pending_remote_release(&key)?;
                first_error.get_or_insert_with(|| {
                    unavailable("DFS remote provider release is waiting for admitted IO")
                });
                continue;
            }

            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            let timeout = remaining.min(REMOTE_RELEASE_RPC_BUDGET);
            let release = crate::node::call_deadline::within_timeout(timeout, || {
                Self::remote_release_handle(&pending.owner, &pending.handle)
            });
            match release {
                Ok(()) => {
                    if self.remove_pending_remote_release_if_current(&key, &pending)? {
                        released = released.saturating_add(1);
                    }
                }
                Err(error) => {
                    first_error.get_or_insert(error);
                    self.rotate_pending_remote_release(&key)?;
                }
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        let remaining = self.pending_remote_release_count()?;
        if remaining != 0 {
            return Err(unavailable(format!(
                "DFS pending remote release drain left {remaining} handle(s) queued"
            )));
        }
        Ok(released)
    }

    fn pending_remote_release_snapshot(
        &self,
    ) -> Result<Vec<(PendingRemoteReleaseKey, PendingRemoteRelease)>> {
        let state = self
            .pending_remote_releases
            .lock()
            .map_err(|_| unavailable("DFS pending remote release table is poisoned"))?;
        Ok(state
            .order
            .iter()
            .filter_map(|key| {
                state
                    .entries
                    .get(key)
                    .map(|pending| (key.clone(), pending.clone()))
            })
            .take(REMOTE_RELEASE_RETRY_MAX_OPS)
            .collect())
    }

    fn rotate_pending_remote_release(&self, key: &PendingRemoteReleaseKey) -> Result<()> {
        let mut state = self
            .pending_remote_releases
            .lock()
            .map_err(|_| unavailable("DFS pending remote release table is poisoned"))?;
        if let Some(position) = state.order.iter().position(|candidate| candidate == key)
            && let Some(key) = state.order.remove(position)
        {
            state.order.push_back(key);
        }
        Ok(())
    }

    fn remove_pending_remote_release_if_current(
        &self,
        key: &PendingRemoteReleaseKey,
        pending: &PendingRemoteRelease,
    ) -> Result<bool> {
        let mut state = self
            .pending_remote_releases
            .lock()
            .map_err(|_| unavailable("DFS pending remote release table is poisoned"))?;
        let current_matches = state.entries.get(key).is_some_and(|current| {
            current.handle == pending.handle
                && Arc::ptr_eq(&current.owner, &pending.owner)
                && Self::same_pending_release_lifecycle(&current.lifecycle, &pending.lifecycle)
        });
        if current_matches {
            state.entries.remove(key);
            state.order.retain(|candidate| candidate != key);
        }
        Ok(current_matches)
    }

    fn same_pending_release_lifecycle(
        left: &Option<Arc<RemoteProviderLifecycle>>,
        right: &Option<Arc<RemoteProviderLifecycle>>,
    ) -> bool {
        match (left, right) {
            (Some(left), Some(right)) => Arc::ptr_eq(left, right),
            (None, None) => true,
            _ => false,
        }
    }

    pub(super) fn pending_remote_release_count(&self) -> Result<usize> {
        Ok(self
            .pending_remote_releases
            .lock()
            .map_err(|_| unavailable("DFS pending remote release table is poisoned"))?
            .entries
            .len())
    }

    fn remote_release_error_is_retryable(error: &Error) -> bool {
        matches!(
            error.kind(),
            afs_error::ErrorKind::Unavailable
                | afs_error::ErrorKind::DeadlineExceeded
                | afs_error::ErrorKind::ResourceExhausted
                | afs_error::ErrorKind::Aborted
        )
    }

    fn remote_release_handle(
        owner: &Arc<dyn RemoteDfsOwner>,
        handle: &afs_protocol::node_control::DfsOwnerHandle,
    ) -> Result<()> {
        owner.release(afs_protocol::node_control::DfsOwnerReleaseRequest {
            handle: Some(handle.clone()),
        })?;
        Ok(())
    }

    pub(super) fn current_remote_provider(
        &self,
        inode_id: &InodeId,
    ) -> Result<Option<RemoteDfsWriteSession>> {
        self.remote_inode_providers
            .read(inode_id)
            .map_err(|_| unavailable("DFS remote owner table is poisoned"))
            .map(|providers| providers.get(inode_id).cloned())
    }

    /// A session may serve reads unless `local_state_protects` says this
    /// node's own write state must keep answering them.
    pub(super) fn remote_provider_visible_for_reads(
        &self,
        inode_id: &InodeId,
        local_state_protects: bool,
    ) -> Result<Option<RemoteDfsProviderIo>> {
        if local_state_protects {
            return Ok(None);
        }
        for _ in 0..2 {
            let Some(remote) = self.current_remote_provider(inode_id)? else {
                return Ok(None);
            };
            if let Some(guard) = remote.lifecycle.begin_io()? {
                return Ok(Some(RemoteDfsProviderIo {
                    remote,
                    _guard: guard,
                }));
            }
        }
        Ok(None)
    }

    fn replacement_remote_provider(
        &self,
        host: &dyn RemoteProviderHost,
        inode_id: &InodeId,
        released: &RemoteDfsWriteSession,
        preferred: Option<&RemoteDfsWriteSession>,
    ) -> Result<Option<RemoteDfsWriteSession>> {
        let live = host.live_remote_sessions(inode_id)?;
        if let Some(preferred) = preferred
            && !same_remote_provider(preferred, released)
            && preferred.lifecycle.is_live()?
            && live
                .iter()
                .any(|remote| same_remote_provider(remote, preferred))
        {
            return Ok(Some(preferred.clone()));
        }
        for remote in live {
            if !same_remote_provider(&remote, released) && remote.lifecycle.is_live()? {
                return Ok(Some(remote));
            }
        }
        Ok(None)
    }

    fn replace_released_remote_provider(
        &self,
        host: &dyn RemoteProviderHost,
        inode_id: &InodeId,
        released: &RemoteDfsWriteSession,
        preferred: Option<&RemoteDfsWriteSession>,
    ) -> Result<()> {
        let replacement = self.replacement_remote_provider(host, inode_id, released, preferred)?;
        let mut providers = self
            .remote_inode_providers
            .write(inode_id)
            .map_err(|_| unavailable("DFS remote owner table is poisoned"))?;
        if providers
            .get(inode_id)
            .is_some_and(|current| same_remote_provider(current, released))
        {
            if let Some(replacement) = replacement.filter(|candidate| {
                candidate.lifecycle.is_live().unwrap_or(false)
                    && !same_remote_provider(candidate, released)
            }) {
                providers.insert(inode_id.clone(), replacement);
            } else {
                providers.remove(inode_id);
            }
        }
        let last_provider = !providers.contains_key(inode_id);
        drop(providers);
        host.remote_provider_replaced(inode_id, last_provider)
    }

    fn retire_released_remote_provider(
        &self,
        host: &dyn RemoteProviderHost,
        inode_id: &InodeId,
        released: &RemoteDfsWriteSession,
        preferred: Option<&RemoteDfsWriteSession>,
    ) -> Result<()> {
        released.lifecycle.retire_new_io()?;
        let replace_result =
            self.replace_released_remote_provider(host, inode_id, released, preferred);
        let wait_result = released
            .lifecycle
            .wait_idle(REMOTE_PROVIDER_IDLE_WAIT_BUDGET);
        replace_result.and(wait_result)
    }

    /// Retires the session's provider (restoring `previous_provider` when it
    /// is still live) and then releases its owner handles, keeping the release
    /// as debt if retirement or the release RPC does not finish.
    pub(super) fn close_remote_write_session(
        &self,
        host: &dyn RemoteProviderHost,
        inode_id: &InodeId,
        session: &DfsRemoteWriteSession,
        previous_provider: Option<&RemoteDfsWriteSession>,
    ) -> Result<()> {
        let Some(remote) = session.remote.as_ref() else {
            return Err(invalid("DFS write session is local"));
        };
        if let Err(error) =
            self.retire_released_remote_provider(host, inode_id, remote, previous_provider)
        {
            self.queue_remote_session_release_with_lifecycle(remote)?;
            return Err(error);
        }
        self.remote_release(session)
    }
}

impl OwnerChannel {
    fn new(identity: Arc<NodeIdentity>, meta: Arc<dyn DfsMeta>) -> Self {
        Self {
            identity,
            meta,
            remote_owner_factory: None,
            remote_open_admission_locks: Mutex::new(HashMap::new()),
            next_owner_open_seq: AtomicU64::new(1),
        }
    }

    pub(super) fn remote_owner(&self, lease: &WriteLease) -> Result<Arc<dyn RemoteDfsOwner>> {
        let factory = self.remote_owner_factory.as_ref().ok_or_else(|| {
            Error::coded(
                afs_error::NODE_VFS_UNIMPLEMENTED,
                "DFS remote write owner connector is not configured",
            )
        })?;
        let location = self
            .meta
            .lookup_node_location(&lease.owner_node_id)?
            .ok_or_else(|| stale("DFS write owner node is not registered"))?;
        if location.node_id != lease.owner_node_id {
            return Err(stale("DFS owner lookup returned another node"));
        }
        factory.connect(location)
    }

    fn with_remote_open_admission<T>(
        &self,
        owner_node_id: &str,
        owner_session_id: &str,
        action: impl FnOnce() -> Result<T>,
    ) -> Result<T> {
        let route = DfsOwnerOpenRouteKey {
            caller_node_id: self.identity.node_id.clone(),
            caller_session_id: self.identity.session_id.clone(),
            owner_node_id: owner_node_id.to_owned(),
            owner_session_id: owner_session_id.to_owned(),
        };
        let lock = {
            let mut locks = self
                .remote_open_admission_locks
                .lock()
                .map_err(|_| unavailable("DFS remote open admission lock table is poisoned"))?;
            if !locks.contains_key(&route) && locks.len() >= MAX_PENDING_REMOTE_RELEASES {
                return Err(Error::coded(
                    afs_error::NODE_VFS_UNAVAILABLE,
                    "DFS remote open admission lock table is full",
                ));
            }
            let lock = locks
                .entry(route.clone())
                .or_insert_with(|| {
                    Arc::new(DfsRemoteOpenAdmissionLock {
                        gate: Mutex::new(()),
                        users: AtomicUsize::new(0),
                    })
                })
                .clone();
            lock.users.fetch_add(1, Ordering::AcqRel);
            lock
        };
        let result = match lock.gate.lock() {
            Ok(_guard) => action(),
            Err(_) => Err(unavailable("DFS remote open admission lock is poisoned")),
        };
        if lock.users.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.cleanup_remote_open_admission_lock(&route, &lock)?;
        }
        result
    }

    pub(super) fn cleanup_remote_open_admission_lock(
        &self,
        route: &DfsOwnerOpenRouteKey,
        lock: &Arc<DfsRemoteOpenAdmissionLock>,
    ) -> Result<()> {
        let mut locks = self
            .remote_open_admission_locks
            .lock()
            .map_err(|_| unavailable("DFS remote open admission lock table is poisoned"))?;
        if locks
            .get(route)
            .is_some_and(|current| Arc::ptr_eq(current, lock))
            && lock.users.load(Ordering::Acquire) == 0
        {
            locks.remove(route);
        }
        Ok(())
    }

    fn next_remote_open_seq(&self) -> Result<u64> {
        let seq = self
            .next_owner_open_seq
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_add(1)
            })
            .map_err(|_| {
                Error::coded(
                    afs_error::NODE_VFS_UNAVAILABLE,
                    "DFS owner open sequence counter overflowed",
                )
            })?;
        if seq == 0 {
            return Err(Error::coded(
                afs_error::NODE_VFS_UNAVAILABLE,
                "DFS owner open sequence counter overflowed",
            ));
        }
        Ok(seq)
    }
}

impl DistributedFs {
    pub(super) fn ensure_local_write_owner(&self, lease: &WriteLease) -> Result<()> {
        if lease.owner_node_id != self.identity.node_id
            || lease.owner_session_id != self.identity.session_id
        {
            return Err(stale("DFS write lease is owned by another node session"));
        }
        Ok(())
    }

    pub(super) fn make_local_write_session(session: DfsWriteSession) -> DfsRemoteWriteSession {
        DfsRemoteWriteSession {
            local: session,
            remote: None,
        }
    }

    pub(super) fn open_remote_write_session(
        &self,
        inode: &InodeRecord,
        lease: &WriteLease,
        open_flags: i32,
        options: OpenOptions,
    ) -> Result<DfsRemoteWriteSession> {
        let session = self
            .write_coordinator
            .open_remote_write_session(inode, lease, open_flags, options)?;
        self.handles.mark_dynamic(&inode.inode_id)?;
        Ok(session)
    }

    pub(super) fn close_remote_write_session(
        &self,
        inode_id: &InodeId,
        session: &DfsRemoteWriteSession,
    ) -> Result<()> {
        self.write_coordinator
            .close_remote_write_session(self, inode_id, session, None)
    }

    pub(super) fn close_temporary_remote_write_session(
        &self,
        inode_id: &InodeId,
        session: &DfsRemoteWriteSession,
        previous_provider: Option<RemoteDfsWriteSession>,
    ) -> Result<()> {
        self.write_coordinator.close_remote_write_session(
            self,
            inode_id,
            session,
            previous_provider.as_ref(),
        )
    }

    pub fn retry_pending_remote_releases(&self) -> Result<usize> {
        self.write_coordinator.retry_pending_remote_releases()
    }

    pub fn drain_pending_remote_releases(&self) -> Result<usize> {
        self.write_coordinator.drain_pending_remote_releases()
    }

    pub fn pending_remote_release_count(&self) -> Result<usize> {
        self.write_coordinator.pending_remote_release_count()
    }
}

impl RemoteProviderHost for DistributedFs {
    fn live_remote_sessions(&self, inode_id: &InodeId) -> Result<Vec<RemoteDfsWriteSession>> {
        let handles = self.handles.for_inode(inode_id)?;
        let snapshots = handles
            .iter()
            .filter_map(|handle| match DfsFileHandleSnapshot::from_handle(handle) {
                Ok(snapshot) => Some(Ok(snapshot)),
                Err(error) if error.code() == afs_error::NODE_DFS_STALE_HANDLE => None,
                Err(error) => Some(Err(error)),
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(snapshots
            .into_iter()
            .filter(|handle| handle.inode_id == *inode_id)
            .filter_map(|handle| handle.write_session.and_then(|session| session.remote))
            .collect())
    }

    fn remote_provider_replaced(&self, inode_id: &InodeId, last_provider: bool) -> Result<()> {
        self.handles.mark_dynamic(inode_id)?;
        if !last_provider {
            return Ok(());
        }
        let proxied = self.write_coordinator.take_proxied_version(inode_id)?;
        let handles = self.handles.for_inode(inode_id)?;
        if let Some(version) = proxied
            && !handles.is_empty()
        {
            // Writes this Node proxied are visible to its handles (4a): once
            // the last proxy session ends they move to the version the owner
            // reported for them, and only to it: another Node's later commit
            // never reaches an open handle. If that version is no longer the
            // head the handles keep their views. The owner has already synced
            // the writes; failing to look the version up is not a write error
            // of the closing writer, so the handles keep their protected views
            // and the next read looks again for the same version.
            match self.proxied_view(inode_id, &version) {
                Ok(Some(inode)) => self.advance_to_local_view(&inode)?,
                Ok(None) => {}
                Err(error) => {
                    afs_logging::warn!("dfs.handle_view_refresh_failed";
                        "inode" => inode_id.0.as_str(),
                        "handles" => handles.len(),
                        "error" => error.to_string());
                    for handle in handles {
                        handle
                            .state
                            .lock()
                            .map_err(|_| unavailable("DFS handle state is poisoned"))?
                            .refresh_view = Some(version.clone());
                    }
                }
            }
        }
        Ok(())
    }
}

pub(super) fn remote_release_key(
    handle: &afs_protocol::node_control::DfsOwnerHandle,
) -> PendingRemoteReleaseKey {
    PendingRemoteReleaseKey {
        namespace_id: handle.namespace_id.clone(),
        inode_id: handle.inode_id.clone(),
        owner_node_id: handle.owner_node_id.clone(),
        owner_session_id: handle.owner_session_id.clone(),
        lease_epoch: handle.lease_epoch,
        caller_node_id: handle.caller_node_id.clone(),
        caller_session_id: handle.caller_session_id.clone(),
        open_seq: handle.open_seq,
        opaque_handle: handle.opaque_handle.clone(),
    }
}

pub(super) fn same_remote_provider(
    left: &RemoteDfsWriteSession,
    right: &RemoteDfsWriteSession,
) -> bool {
    let left = &left.handle;
    let right = &right.handle;
    left.namespace_id == right.namespace_id
        && left.inode_id == right.inode_id
        && left.owner_node_id == right.owner_node_id
        && left.owner_session_id == right.owner_session_id
        && left.lease_epoch == right.lease_epoch
        && left.caller_node_id == right.caller_node_id
        && left.caller_session_id == right.caller_session_id
        && left.open_seq == right.open_seq
        && left.opaque_handle == right.opaque_handle
}

pub(super) fn data_owner_handle(
    handle: &afs_protocol::node_control::DfsOwnerHandle,
) -> afs_protocol::node_data::DfsOwnerHandle {
    afs_protocol::node_data::DfsOwnerHandle {
        namespace_id: handle.namespace_id.clone(),
        inode_id: handle.inode_id.clone(),
        owner_node_id: handle.owner_node_id.clone(),
        owner_session_id: handle.owner_session_id.clone(),
        lease_epoch: handle.lease_epoch,
        caller_node_id: handle.caller_node_id.clone(),
        caller_session_id: handle.caller_session_id.clone(),
        opaque_handle: handle.opaque_handle.clone(),
        open_seq: handle.open_seq,
    }
}
