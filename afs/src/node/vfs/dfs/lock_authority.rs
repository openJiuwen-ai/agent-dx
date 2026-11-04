use super::*;

pub(super) struct DfsLockAuthority {
    pub(super) table: LockTable,
    pub(super) state: Mutex<DfsLockAuthorityState>,
    renewal: Mutex<()>,
}

pub(super) struct DfsLockAuthorityState {
    pub(super) lease: WriteLease,
    pub(super) pinned_owners: HashSet<FileLockOwner>,
    pub(super) waiters: HashSet<LockWaiterId>,
    pub(super) last_error: Option<Error>,
}

pub(super) struct DfsLockRenewal {
    authorities: Arc<Mutex<HashMap<InodeId, Arc<DfsLockAuthority>>>>,
    running: Arc<AtomicBool>,
}

#[derive(Clone)]
pub(super) struct DfsRemoteLockAuthority {
    pub(super) owner: Arc<dyn RemoteDfsOwner>,
    pub(super) authority: afs_protocol::node_control::DfsOwnerLockAuthority,
}

pub(super) enum DfsLockTarget {
    Local(Arc<DfsLockAuthority>),
    Remote(DfsRemoteLockAuthority),
}

impl DfsLockAuthority {
    fn new(lease: WriteLease) -> Self {
        Self {
            table: LockTable::default(),
            state: Mutex::new(DfsLockAuthorityState {
                lease,
                pinned_owners: HashSet::new(),
                waiters: HashSet::new(),
                last_error: None,
            }),
            renewal: Mutex::new(()),
        }
    }
}

pub(super) fn same_lease_identity(left: &WriteLease, right: &WriteLease) -> bool {
    left.inode_id == right.inode_id
        && left.owner_node_id == right.owner_node_id
        && left.owner_session_id == right.owner_session_id
        && left.lease_epoch == right.lease_epoch
}

pub(super) fn merge_same_identity_lease(current: &WriteLease, incoming: WriteLease) -> WriteLease {
    let mut merged = incoming;
    merged.expires_at_unix_ms = merged.expires_at_unix_ms.max(current.expires_at_unix_ms);
    merged
}

pub(super) fn lock_authority_renewal_guard(
    authority: &Arc<DfsLockAuthority>,
) -> Result<std::sync::MutexGuard<'_, ()>> {
    authority
        .renewal
        .lock()
        .map_err(|_| unavailable("DFS lock authority renewal guard is poisoned"))
}

fn refresh_same_epoch_lock_authority(
    authority: &Arc<DfsLockAuthority>,
    lease: WriteLease,
) -> Result<()> {
    let mut state = authority
        .state
        .lock()
        .map_err(|_| unavailable("DFS lock authority state is poisoned"))?;
    if let Some(error) = state.last_error.clone() {
        return Err(error);
    }
    if same_lease_identity(&state.lease, &lease) {
        state.lease = merge_same_identity_lease(&state.lease, lease);
    }
    Ok(())
}

pub(super) fn apply_lock_renewal_success(
    authority: &Arc<DfsLockAuthority>,
    renewed: WriteLease,
) -> Result<Option<Error>> {
    let terminal_error = {
        let mut state = authority
            .state
            .lock()
            .map_err(|_| unavailable("DFS lock authority state is poisoned"))?;
        if let Some(error) = state.last_error.clone() {
            return Ok(Some(error));
        }
        let terminal_error = if !same_lease_identity(&state.lease, &renewed) {
            Some(stale(
                "DFS lock authority was renewed with a different lease identity",
            ))
        } else if state
            .lease
            .expires_at_unix_ms
            .max(renewed.expires_at_unix_ms)
            <= now_unix_ms()
        {
            Some(stale("DFS lock authority lease expired"))
        } else {
            None
        };
        if let Some(error) = terminal_error.clone() {
            state.last_error = Some(error);
            state.pinned_owners.clear();
            state.waiters.clear();
        } else {
            state.lease = merge_same_identity_lease(&state.lease, renewed);
        }
        terminal_error
    };
    if terminal_error.is_some() {
        let _ = authority.table.invalidate();
    }
    Ok(terminal_error)
}

pub(super) fn apply_lock_renewal_failure(
    authority: &Arc<DfsLockAuthority>,
    attempted: &WriteLease,
    error: Error,
) -> Result<Option<Error>> {
    let terminal_error = {
        let mut state = authority
            .state
            .lock()
            .map_err(|_| unavailable("DFS lock authority state is poisoned"))?;
        if let Some(error) = state.last_error.clone() {
            return Ok(Some(error));
        }
        if !same_lease_identity(&state.lease, attempted) {
            return Ok(None);
        }
        let terminal_error = if is_confirmed_lock_renewal_fence(&error) {
            Some(error)
        } else if state.lease.expires_at_unix_ms <= now_unix_ms() {
            Some(stale("DFS lock authority lease expired"))
        } else {
            None
        };
        if let Some(error) = terminal_error.clone() {
            state.last_error = Some(error.clone());
            state.pinned_owners.clear();
            state.waiters.clear();
            Some(error)
        } else {
            None
        }
    };
    if terminal_error.is_some() {
        let _ = authority.table.invalidate();
    }
    Ok(terminal_error)
}

pub(super) fn expire_lock_authority_if_due_locked(
    authority: &Arc<DfsLockAuthority>,
) -> Result<bool> {
    let expired = {
        let mut state = authority
            .state
            .lock()
            .map_err(|_| unavailable("DFS lock authority state is poisoned"))?;
        let expired = state.last_error.is_none() && state.lease.expires_at_unix_ms <= now_unix_ms();
        if expired {
            state.last_error = Some(stale("DFS lock authority lease expired"));
            state.pinned_owners.clear();
            state.waiters.clear();
        }
        expired
    };
    if expired {
        let _ = authority.table.invalidate();
        return Ok(true);
    }
    Ok(false)
}

fn is_confirmed_lock_renewal_fence(error: &Error) -> bool {
    matches!(
        error.code(),
        afs_error::META_DFS_CONFLICT | afs_error::NODE_DFS_STALE_HANDLE
    )
}

impl DfsLockRenewal {
    pub(super) fn new() -> Self {
        Self {
            authorities: Arc::new(Mutex::new(HashMap::new())),
            running: Arc::new(AtomicBool::new(false)),
        }
    }

    fn track(&self, inode_id: InodeId, authority: Arc<DfsLockAuthority>) -> Result<()> {
        let mut authorities = self
            .authorities
            .lock()
            .map_err(|_| unavailable("DFS lock renewal registry is poisoned"))?;
        authorities.insert(inode_id, authority);
        Ok(())
    }

    fn untrack_if_current(
        &self,
        inode_id: &InodeId,
        authority: &Arc<DfsLockAuthority>,
    ) -> Result<bool> {
        let mut authorities = self
            .authorities
            .lock()
            .map_err(|_| unavailable("DFS lock renewal registry is poisoned"))?;
        let should_remove = authorities
            .get(inode_id)
            .is_some_and(|current| Arc::ptr_eq(current, authority));
        if should_remove {
            authorities.remove(inode_id);
        }
        Ok(should_remove)
    }

    pub(super) fn stop(&self) {
        self.running.store(false, Ordering::Release);
        if let Ok(mut authorities) = self.authorities.lock() {
            for authority in authorities.values() {
                let _ = authority.table.invalidate();
                if let Ok(mut state) = authority.state.lock() {
                    state.last_error = Some(stale("DFS lock renewal stopped"));
                    state.pinned_owners.clear();
                    state.waiters.clear();
                }
            }
            authorities.clear();
        }
    }

    pub(super) fn start(&self, meta: Arc<dyn DfsMeta>) {
        if self.running.swap(true, Ordering::AcqRel) {
            return;
        }
        let authorities = self.authorities.clone();
        let running = self.running.clone();
        // The thread is intentionally one per DistributedFs instance, not one per inode.
        std::thread::spawn(move || {
            while running.load(Ordering::Acquire) {
                std::thread::sleep(Duration::from_secs(1));
                if !running.load(Ordering::Acquire) {
                    break;
                }
                let entries = match authorities.lock() {
                    Ok(map) => map
                        .iter()
                        .map(|(inode_id, authority)| (inode_id.clone(), authority.clone()))
                        .collect::<Vec<_>>(),
                    Err(_) => return,
                };
                let mut idle = Vec::new();
                for (inode_id, authority) in entries {
                    let Ok(_renewal) = lock_authority_renewal_guard(&authority) else {
                        return;
                    };
                    let (active, lease) = match authority.state.lock() {
                        Ok(state) => {
                            let active =
                                !state.pinned_owners.is_empty() || !state.waiters.is_empty();
                            (active, state.lease.clone())
                        }
                        Err(_) => return,
                    };
                    if expire_lock_authority_if_due_locked(&authority).unwrap_or(true) {
                        continue;
                    }
                    if !active && authority.table.is_idle().unwrap_or(false) {
                        drop(_renewal);
                        idle.push((inode_id, authority));
                        continue;
                    }
                    if !should_background_renew(&lease) {
                        continue;
                    }
                    match meta.renew_write_lease(lease.clone()) {
                        Ok(renewed) => {
                            if apply_lock_renewal_success(&authority, renewed).is_err() {
                                return;
                            }
                        }
                        Err(error) => {
                            if apply_lock_renewal_failure(&authority, &lease, error).is_err() {
                                return;
                            }
                        }
                    }
                }
                if !idle.is_empty() {
                    if let Ok(mut map) = authorities.lock() {
                        for (inode_id, authority) in idle {
                            let current_is_idle = map.get(&inode_id).is_some_and(|current| {
                                Arc::ptr_eq(current, &authority)
                                    && Arc::strong_count(current) <= 2
                                    && current.table.is_idle().unwrap_or(false)
                            });
                            if current_is_idle {
                                map.remove(&inode_id);
                            }
                        }
                    } else {
                        running.store(false, Ordering::Release);
                        return;
                    }
                }
            }
            running.store(false, Ordering::Release);
        });
    }
}

impl DistributedFs {
    pub(super) fn lock_authority_for(&self, inode_id: &InodeId) -> Result<DfsLockTarget> {
        let (inode, lease) = self.meta.resolve_lock_authority(inode_id)?;
        let inode = self.validate_inode(inode)?;
        if inode.inode_id != *inode_id {
            return Err(stale("DFS lock authority returned another inode"));
        }
        if lease.inode_id != *inode_id
            || lease.lease_epoch == 0
            || lease.owner_node_id.is_empty()
            || lease.owner_session_id.is_empty()
        {
            return Err(stale(
                "DFS lock authority returned an invalid lease identity",
            ));
        }
        if lease.owner_node_id == self.node_id && lease.owner_session_id == self.session_id {
            self.ensure_local_write_owner(&lease)?;
            let authority = self.local_lock_authority(&inode.inode_id, lease)?;
            Ok(DfsLockTarget::Local(authority))
        } else {
            Ok(DfsLockTarget::Remote(
                self.remote_lock_authority(&inode.inode_id, &lease)?,
            ))
        }
    }

    pub(super) fn existing_lock_authority_for(
        &self,
        inode_id: &InodeId,
    ) -> Result<Option<DfsLockTarget>> {
        if let Some(authority) = self
            .lock_authorities
            .read(inode_id)
            .map_err(|_| unavailable("DFS lock authority table is poisoned"))?
            .get(inode_id)
            .cloned()
        {
            return Ok(Some(DfsLockTarget::Local(authority)));
        }
        Ok(self
            .remote_lock_authorities
            .lock()
            .map_err(|_| unavailable("DFS remote lock authority table is poisoned"))?
            .get(inode_id)
            .cloned()
            .map(DfsLockTarget::Remote))
    }

    fn reclaim_idle_lock_authorities(&self) -> Result<()> {
        let idle = self
            .lock_authorities
            .snapshot()?
            .into_iter()
            .filter(|(_, authority)| {
                // The snapshot adds one metadata reference to table + renewal.
                authority.table.is_idle().unwrap_or(false) && Arc::strong_count(authority) <= 3
            })
            .collect::<Vec<_>>();
        for (inode_id, authority) in idle {
            let mut authorities = self
                .lock_authorities
                .write(&inode_id)
                .map_err(|_| unavailable("DFS lock authority table is poisoned"))?;
            let current_is_idle = authorities.get(&inode_id).is_some_and(|current| {
                Arc::ptr_eq(current, &authority)
                    && Arc::strong_count(current) <= 3
                    && current.table.is_idle().unwrap_or(false)
            });
            if current_is_idle {
                authorities.remove(&inode_id);
            }
            drop(authorities);
            if current_is_idle {
                let _ = self.lock_renewal.untrack_if_current(&inode_id, &authority);
            }
        }
        Ok(())
    }

    fn replace_stale_lock_authority(
        &self,
        inode_id: &InodeId,
        old: &Arc<DfsLockAuthority>,
        lease: WriteLease,
    ) -> Result<Arc<DfsLockAuthority>> {
        let _ = old.table.invalidate();
        if let Ok(mut state) = old.state.lock() {
            state.last_error = Some(stale(
                "DFS lock authority was fenced by a newer lease epoch",
            ));
            state.pinned_owners.clear();
            state.waiters.clear();
        }
        let authority = Arc::new(DfsLockAuthority::new(lease));
        self.lock_renewal
            .track(inode_id.clone(), authority.clone())?;
        Ok(authority)
    }

    pub(super) fn local_lock_authority(
        &self,
        inode_id: &InodeId,
        lease: WriteLease,
    ) -> Result<Arc<DfsLockAuthority>> {
        let admission = self
            .lock_authority_admission
            .lock()
            .map_err(|_| unavailable("DFS lock authority admission is poisoned"))?;
        self.reclaim_idle_lock_authorities()?;
        let count = self.lock_authorities.len()?;
        let mut authorities = self
            .lock_authorities
            .write(inode_id)
            .map_err(|_| unavailable("DFS lock authority table is poisoned"))?;
        if let Some(authority) = authorities.get(inode_id).cloned() {
            let epoch = authority
                .state
                .lock()
                .map_err(|_| unavailable("DFS lock authority state is poisoned"))?
                .lease
                .lease_epoch;
            if epoch == lease.lease_epoch {
                drop(authorities);
                drop(admission);
                let _renewal = lock_authority_renewal_guard(&authority)?;
                refresh_same_epoch_lock_authority(&authority, lease)?;
                self.check_lock_renewal_locked(&authority)?;
                drop(_renewal);
                return Ok(authority);
            }
            let replacement = self.replace_stale_lock_authority(inode_id, &authority, lease)?;
            authorities.insert(inode_id.clone(), replacement.clone());
            return Ok(replacement);
        }
        if count >= MAX_LOCK_AUTHORITIES {
            return Err(Error::from(std::io::Error::from_raw_os_error(libc::ENOLCK)));
        }
        let authority = Arc::new(DfsLockAuthority::new(lease));
        self.lock_renewal
            .track(inode_id.clone(), authority.clone())?;
        authorities.insert(inode_id.clone(), authority.clone());
        drop(authorities);
        Ok(authority)
    }

    fn reclaim_idle_remote_lock_authorities(&self) -> Result<()> {
        let active_session_inodes = self
            .remote_lock_authority_sessions
            .lock()
            .map_err(|_| unavailable("DFS remote lock authority session table is poisoned"))?
            .iter()
            .filter(|&(_inode_id, sessions)| !sessions.is_empty())
            .map(|(inode_id, _sessions)| inode_id.clone())
            .collect::<HashSet<_>>();
        let active_waiter_inodes = self
            .remote_lock_waiters
            .lock()
            .map_err(|_| unavailable("DFS remote lock waiter table is poisoned"))?
            .values()
            .map(|remote| InodeId::new(remote.authority.inode_id.clone()))
            .collect::<HashSet<_>>();
        self.remote_lock_authorities
            .lock()
            .map_err(|_| unavailable("DFS remote lock authority table is poisoned"))?
            .retain(|inode_id, _| {
                active_session_inodes.contains(inode_id) || active_waiter_inodes.contains(inode_id)
            });
        Ok(())
    }

    fn remote_lock_authority(
        &self,
        inode_id: &InodeId,
        lease: &WriteLease,
    ) -> Result<DfsRemoteLockAuthority> {
        self.reclaim_idle_remote_lock_authorities()?;
        let owner = self.remote_owner(lease)?;
        let remote = DfsRemoteLockAuthority {
            owner,
            authority: self.lock_authority_wire(inode_id, lease),
        };
        let mut authorities = self
            .remote_lock_authorities
            .lock()
            .map_err(|_| unavailable("DFS remote lock authority table is poisoned"))?;
        if !authorities.contains_key(inode_id) && authorities.len() >= MAX_REMOTE_LOCK_AUTHORITIES {
            return Err(Error::from(std::io::Error::from_raw_os_error(libc::ENOLCK)));
        }
        authorities.insert(inode_id.clone(), remote.clone());
        Ok(remote)
    }

    fn lock_authority_wire(
        &self,
        inode_id: &InodeId,
        lease: &WriteLease,
    ) -> afs_protocol::node_control::DfsOwnerLockAuthority {
        afs_protocol::node_control::DfsOwnerLockAuthority {
            namespace_id: self.namespace_id.0.clone(),
            inode_id: inode_id.0.clone(),
            owner_node_id: lease.owner_node_id.clone(),
            owner_session_id: lease.owner_session_id.clone(),
            lease_epoch: lease.lease_epoch,
            lease_expires_at_unix_ms: lease.expires_at_unix_ms,
            caller_node_id: self.node_id.clone(),
            caller_session_id: self.session_id.clone(),
        }
    }

    pub(super) fn lock_scope(
        node_id: &str,
        caller_session_id: &str,
        ingress_session_id: &str,
    ) -> Result<String> {
        if node_id.is_empty() || caller_session_id.is_empty() || ingress_session_id.is_empty() {
            return Err(invalid("DFS lock scope is incomplete"));
        }
        if node_id.contains('\0')
            || caller_session_id.contains('\0')
            || ingress_session_id.contains('\0')
        {
            return Err(invalid("DFS lock scope contains an invalid NUL byte"));
        }
        Ok(format!(
            "{node_id}\0{caller_session_id}\0{ingress_session_id}"
        ))
    }

    pub(super) fn raw_lock_session(scoped: &str) -> &str {
        scoped.rsplit('\0').next().unwrap_or(scoped)
    }

    fn scoped_lock_process_session(scoped: &str) -> Option<(String, String)> {
        let mut parts = scoped.splitn(3, '\0');
        let node_id = parts.next()?;
        let session_id = parts.next()?;
        let raw_session = parts.next()?;
        (!node_id.is_empty() && !session_id.is_empty() && !raw_session.is_empty())
            .then(|| (node_id.to_owned(), session_id.to_owned()))
    }

    pub(super) fn scoped_local_lock_owner(&self, owner: FileLockOwner) -> Result<FileLockOwner> {
        Ok(FileLockOwner {
            ingress_session_id: Self::lock_scope(
                &self.node_id,
                &self.session_id,
                &owner.ingress_session_id,
            )?,
            kernel_owner: owner.kernel_owner,
        })
    }

    pub(super) fn scoped_local_lock_waiter(&self, waiter: LockWaiterId) -> Result<LockWaiterId> {
        Ok(LockWaiterId {
            ingress_session_id: Self::lock_scope(
                &self.node_id,
                &self.session_id,
                &waiter.ingress_session_id,
            )?,
            request_id: waiter.request_id,
        })
    }

    pub(super) fn scoped_remote_lock_owner(
        peer: &str,
        caller_session_id: &str,
        owner: afs_protocol::node_control::DfsOwnerLockOwner,
    ) -> Result<FileLockOwner> {
        if owner.ingress_session_id.is_empty() {
            return Err(invalid("DFS lock owner session is empty"));
        }
        Ok(FileLockOwner {
            ingress_session_id: Self::lock_scope(
                peer,
                caller_session_id,
                &owner.ingress_session_id,
            )?,
            kernel_owner: owner.kernel_owner,
        })
    }

    pub(super) fn scoped_remote_lock_waiter(
        peer: &str,
        caller_session_id: &str,
        waiter: afs_protocol::node_control::DfsOwnerLockWaiter,
    ) -> Result<LockWaiterId> {
        if waiter.ingress_session_id.is_empty() {
            return Err(invalid("DFS lock waiter session is empty"));
        }
        Ok(LockWaiterId {
            ingress_session_id: Self::lock_scope(
                peer,
                caller_session_id,
                &waiter.ingress_session_id,
            )?,
            request_id: waiter.request_id,
        })
    }

    pub(super) fn scoped_local_lock_request(
        &self,
        mut request: LockRequest,
    ) -> Result<LockRequest> {
        request.owner = self.scoped_local_lock_owner(request.owner)?;
        Ok(request)
    }

    pub(super) fn scoped_remote_lock_request(
        peer: &str,
        caller_session_id: &str,
        request: afs_protocol::node_control::DfsOwnerLockRequest,
    ) -> Result<LockRequest> {
        Ok(LockRequest {
            kind: Self::file_lock_kind_from_wire(request.kind)?,
            owner: Self::scoped_remote_lock_owner(
                peer,
                caller_session_id,
                request
                    .owner
                    .ok_or_else(|| invalid("DFS lock owner is missing"))?,
            )?,
            pid: request.pid,
            range: Self::file_lock_range_from_wire(
                request
                    .range
                    .ok_or_else(|| invalid("DFS lock range is missing"))?,
            )?,
            lock_type: Self::file_lock_type_from_wire(request.lock_type)?,
        })
    }

    fn validate_lock_authority(
        &self,
        peer: &str,
        authority: &afs_protocol::node_control::DfsOwnerLockAuthority,
    ) -> Result<WriteLease> {
        if authority.namespace_id != self.namespace_id.0
            || authority.owner_node_id != self.node_id
            || authority.owner_session_id != self.session_id
            || authority.caller_node_id != peer
            || authority.caller_session_id.is_empty()
            || authority.lease_epoch == 0
        {
            return Err(stale(
                "DFS lock authority identity does not match this owner",
            ));
        }
        if peer.contains('\0') || authority.caller_session_id.contains('\0') {
            return Err(invalid("DFS lock authority contains an invalid NUL byte"));
        }
        match self.meta.current_node_session(peer)? {
            Some(session_id) if session_id == authority.caller_session_id => {}
            _ => {
                return Err(stale(
                    "DFS lock authority caller session is no longer current",
                ));
            }
        }
        let lease = WriteLease {
            inode_id: InodeId::new(authority.inode_id.clone()),
            owner_node_id: authority.owner_node_id.clone(),
            owner_session_id: authority.owner_session_id.clone(),
            lease_epoch: authority.lease_epoch,
            expires_at_unix_ms: authority.lease_expires_at_unix_ms,
        };
        let renewed = self.meta.renew_write_lease(lease)?;
        if renewed.owner_node_id != self.node_id
            || renewed.owner_session_id != self.session_id
            || renewed.lease_epoch != authority.lease_epoch
        {
            return Err(stale("DFS lock authority lease is no longer current"));
        }
        Ok(renewed)
    }

    pub(super) fn cached_local_authority_from_wire(
        &self,
        peer: &str,
        authority: afs_protocol::node_control::DfsOwnerLockAuthority,
    ) -> Result<Option<(InodeId, Arc<DfsLockAuthority>)>> {
        if authority.namespace_id != self.namespace_id.0
            || authority.owner_node_id != self.node_id
            || authority.owner_session_id != self.session_id
            || authority.caller_node_id != peer
            || authority.caller_session_id.is_empty()
            || authority.lease_epoch == 0
        {
            return Err(stale(
                "DFS lock authority identity does not match this owner",
            ));
        }
        if peer.contains('\0') || authority.caller_session_id.contains('\0') {
            return Err(invalid("DFS lock authority contains an invalid NUL byte"));
        }
        let inode_id = InodeId::new(authority.inode_id);
        let existing = self
            .lock_authorities
            .read(&inode_id)
            .map_err(|_| unavailable("DFS lock authority table is poisoned"))?
            .get(&inode_id)
            .cloned();
        let Some(existing) = existing else {
            return Ok(None);
        };
        let epoch = existing
            .state
            .lock()
            .map_err(|_| unavailable("DFS lock authority state is poisoned"))?
            .lease
            .lease_epoch;
        if epoch != authority.lease_epoch {
            return Ok(None);
        }
        Ok(Some((inode_id, existing)))
    }

    pub(super) fn local_authority_from_wire(
        &self,
        peer: &str,
        authority: afs_protocol::node_control::DfsOwnerLockAuthority,
    ) -> Result<(InodeId, Arc<DfsLockAuthority>)> {
        let lease = self.validate_lock_authority(peer, &authority)?;
        let inode_id = InodeId::new(authority.inode_id);
        Ok((
            inode_id.clone(),
            self.local_lock_authority(&inode_id, lease)?,
        ))
    }

    fn collect_peer_lock_scopes(&self) -> Result<HashMap<(String, String), HashSet<String>>> {
        let authorities = self.lock_authorities.values_snapshot()?;
        let mut scopes: HashMap<(String, String), HashSet<String>> = HashMap::new();
        for authority in authorities {
            let state = authority
                .state
                .lock()
                .map_err(|_| unavailable("DFS lock authority state is poisoned"))?;
            for scope in state
                .pinned_owners
                .iter()
                .map(|owner| owner.ingress_session_id.as_str())
                .chain(
                    state
                        .waiters
                        .iter()
                        .map(|waiter| waiter.ingress_session_id.as_str()),
                )
            {
                if let Some(peer_session) = Self::scoped_lock_process_session(scope) {
                    scopes
                        .entry(peer_session)
                        .or_default()
                        .insert(scope.to_owned());
                }
            }
        }
        Ok(scopes)
    }

    pub(super) fn release_scoped_lock_sessions(&self, scopes: HashSet<String>) -> Result<usize> {
        if scopes.is_empty() {
            return Ok(0);
        }
        for scope in &scopes {
            self.note_closed_lock_session(scope)?;
        }
        let authorities = self.lock_authorities.values_snapshot()?;
        let mut first_error = None;
        for authority in authorities {
            for scope in &scopes {
                if let Err(error) = authority.table.release_session(scope).map_err(lock_error) {
                    first_error.get_or_insert(error);
                }
            }
            let mut state = authority
                .state
                .lock()
                .map_err(|_| unavailable("DFS lock authority state is poisoned"))?;
            state
                .pinned_owners
                .retain(|owner| !scopes.contains(&owner.ingress_session_id));
            state
                .waiters
                .retain(|waiter| !scopes.contains(&waiter.ingress_session_id));
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        Ok(scopes.len())
    }

    pub fn reap_expired_peer_lock_sessions(&self) -> Result<usize> {
        self.reclaim_idle_lock_authorities()?;
        let deadline = std::time::Instant::now() + LOCK_SESSION_REAP_BUDGET;
        let mut scoped_by_peer = self
            .collect_peer_lock_scopes()?
            .into_iter()
            .collect::<Vec<_>>();
        scoped_by_peer.sort_by(|left, right| left.0.cmp(&right.0));
        if !scoped_by_peer.is_empty() {
            let start = (self
                .lock_session_reap_cursor
                .fetch_add(1, Ordering::Relaxed) as usize)
                % scoped_by_peer.len();
            scoped_by_peer.rotate_left(start);
        }
        let mut reclaimed = 0usize;
        let mut first_error = None;
        let mut checked = 0usize;
        for ((node_id, session_id), scopes) in scoped_by_peer {
            if checked >= LOCK_SESSION_REAP_MAX_OPS || std::time::Instant::now() >= deadline {
                break;
            }
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            let timeout = remaining.min(REMOTE_RELEASE_RPC_BUDGET);
            if timeout.is_zero() {
                break;
            }
            checked = checked.saturating_add(1);
            match self
                .meta
                .current_node_session_with_timeout(&node_id, timeout)
            {
                Ok(current) if current.as_deref() != Some(session_id.as_str()) => {
                    match self.release_scoped_lock_sessions(scopes) {
                        Ok(count) => reclaimed = reclaimed.saturating_add(count),
                        Err(error) => {
                            first_error.get_or_insert(error);
                        }
                    }
                }
                Ok(_) => {}
                Err(error) => {
                    first_error.get_or_insert(error);
                }
            }
        }
        match self.retry_closed_remote_lock_sessions_until(
            deadline,
            LOCK_SESSION_REAP_MAX_OPS.saturating_sub(checked),
        ) {
            Ok(count) => reclaimed = reclaimed.saturating_add(count),
            Err(error) => {
                first_error.get_or_insert(error);
            }
        }
        match self
            .retry_cancelled_remote_lock_waiter_acks_until(deadline, LOCK_SESSION_REAP_MAX_OPS)
        {
            Ok(count) => reclaimed = reclaimed.saturating_add(count),
            Err(error) => {
                first_error.get_or_insert(error);
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        Ok(reclaimed)
    }
}
