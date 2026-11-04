use super::*;

impl DistributedFs {
    fn start_lock_renewal_if_needed(&self) {
        self.lock_renewal.start(self.meta.clone());
    }

    fn renewal_lease_snapshot(
        &self,
        authority: &Arc<DfsLockAuthority>,
    ) -> Result<Option<WriteLease>> {
        let state = authority
            .state
            .lock()
            .map_err(|_| unavailable("DFS lock authority state is poisoned"))?;
        if let Some(error) = state.last_error.clone() {
            return Err(error);
        }
        if !should_renew(&state.lease) {
            return Ok(None);
        }
        Ok(Some(state.lease.clone()))
    }

    pub(super) fn check_lock_renewal_locked(
        &self,
        authority: &Arc<DfsLockAuthority>,
    ) -> Result<()> {
        let Some(lease) = self.renewal_lease_snapshot(authority)? else {
            return Ok(());
        };
        match self.meta.renew_write_lease(lease.clone()) {
            Ok(renewed) => match apply_lock_renewal_success(authority, renewed)? {
                Some(error) => Err(error),
                None => Ok(()),
            },
            Err(error) => match apply_lock_renewal_failure(authority, &lease, error.clone())? {
                Some(terminal) => Err(terminal),
                None => Err(error),
            },
        }
    }

    pub(super) fn check_lock_renewal(&self, authority: &Arc<DfsLockAuthority>) -> Result<()> {
        let _renewal = lock_authority_renewal_guard(authority)?;
        self.check_lock_renewal_locked(authority)
    }

    pub(super) fn check_lock_renewal_after_mutation(
        &self,
        authority: &Arc<DfsLockAuthority>,
    ) -> Result<()> {
        let _renewal = lock_authority_renewal_guard(authority)?;
        let Some(lease) = self.renewal_lease_snapshot(authority)? else {
            return Ok(());
        };
        match self.meta.renew_write_lease(lease.clone()) {
            Ok(renewed) => match apply_lock_renewal_success(authority, renewed)? {
                Some(error) => Err(error),
                None => Ok(()),
            },
            Err(error) => match apply_lock_renewal_failure(authority, &lease, error)? {
                Some(terminal) => Err(terminal),
                None => Ok(()),
            },
        }
    }

    pub(super) fn register_local_lock_success(
        &self,
        authority: &Arc<DfsLockAuthority>,
        request: &LockRequest,
    ) -> Result<()> {
        if request.lock_type == FileLockType::Unlock {
            self.unpin_owner_if_inactive(authority, &request.owner, Some(request.kind))?;
            return Ok(());
        }
        let mut state = authority
            .state
            .lock()
            .map_err(|_| unavailable("DFS lock authority state is poisoned"))?;
        if let Some(error) = state.last_error.clone() {
            return Err(error);
        }
        state.pinned_owners.insert(request.owner.clone());
        drop(state);
        self.start_lock_renewal_if_needed();
        Ok(())
    }

    pub(super) fn register_lock_waiter(
        &self,
        authority: &Arc<DfsLockAuthority>,
        waiter: &LockWaiterId,
    ) -> Result<()> {
        if self.consume_precancelled_lock_waiter(waiter)? {
            return Err(lock_error(LockError::Interrupted));
        }
        let mut state = authority
            .state
            .lock()
            .map_err(|_| unavailable("DFS lock authority state is poisoned"))?;
        if let Some(error) = state.last_error.clone() {
            return Err(error);
        }
        state.waiters.insert(waiter.clone());
        drop(state);
        self.start_lock_renewal_if_needed();
        Ok(())
    }

    pub(super) fn unpin_lock_waiter(
        &self,
        authority: &Arc<DfsLockAuthority>,
        waiter: Option<&LockWaiterId>,
    ) -> Result<()> {
        let Some(waiter) = waiter else {
            return Ok(());
        };
        let mut state = authority
            .state
            .lock()
            .map_err(|_| unavailable("DFS lock authority state is poisoned"))?;
        state.waiters.remove(waiter);
        Ok(())
    }

    pub(super) fn unpin_owner_if_inactive(
        &self,
        authority: &Arc<DfsLockAuthority>,
        owner: &FileLockOwner,
        kind: Option<FileLockKind>,
    ) -> Result<()> {
        if authority
            .table
            .has_owner_activity(owner, kind)
            .map_err(lock_error)?
        {
            return Ok(());
        }
        authority
            .state
            .lock()
            .map_err(|_| unavailable("DFS lock authority state is poisoned"))?
            .pinned_owners
            .remove(owner);
        Ok(())
    }

    pub(super) fn note_closed_lock_session(&self, ingress_session_id: &str) -> Result<()> {
        let mut sessions = self
            .closed_lock_sessions
            .lock()
            .map_err(|_| unavailable("DFS closed lock session table is poisoned"))?;
        // Closing a concrete session is cleanup, not new lock admission. Keep
        // this admission tombstone table bounded: when it is full, cleanup still
        // proceeds through the concrete lock tables and remote pending-close
        // state, but this global fast-path set is not expanded.
        if sessions.len() < MAX_CLOSED_LOCK_SESSIONS || sessions.contains(ingress_session_id) {
            sessions.insert(ingress_session_id.to_owned());
        } else {
            self.closed_lock_session_admission_closed
                .store(true, Ordering::Release);
        }
        Ok(())
    }

    pub(super) fn ensure_lock_session_open(&self, ingress_session_id: &str) -> Result<()> {
        if self
            .closed_lock_sessions
            .lock()
            .map_err(|_| unavailable("DFS closed lock session table is poisoned"))?
            .contains(ingress_session_id)
        {
            return Err(lock_error(LockError::Interrupted));
        }
        if self
            .closed_lock_session_admission_closed
            .load(Ordering::Acquire)
        {
            return Err(lock_error(LockError::Capacity));
        }
        Ok(())
    }

    pub(super) fn register_remote_lock_waiter_route(
        &self,
        waiter_id: &LockWaiterId,
        remote: DfsRemoteLockAuthority,
    ) -> Result<()> {
        let cancelled = self
            .cancelled_remote_lock_waiters
            .lock()
            .map_err(|_| unavailable("DFS remote lock cancellation table is poisoned"))?;
        if cancelled.contains(waiter_id) {
            return Err(lock_error(LockError::Interrupted));
        }
        let mut waiters = self
            .remote_lock_waiters
            .lock()
            .map_err(|_| unavailable("DFS remote lock waiter table is poisoned"))?;
        if waiters.contains_key(waiter_id) {
            return Err(Error::from(std::io::Error::from_raw_os_error(libc::EBUSY)));
        }
        if waiters.len() >= MAX_REMOTE_LOCK_WAITERS {
            return Err(Error::from(std::io::Error::from_raw_os_error(libc::ENOLCK)));
        }
        waiters.insert(waiter_id.clone(), remote);
        Ok(())
    }

    fn remove_remote_lock_waiter_route(&self, waiter_id: &LockWaiterId) -> Result<()> {
        self.remote_lock_waiters
            .lock()
            .map_err(|_| unavailable("DFS remote lock waiter table is poisoned"))?
            .remove(waiter_id);
        Ok(())
    }

    pub(super) fn mark_cancelled_remote_lock_waiter(&self, waiter: &LockWaiterId) -> Result<()> {
        let mut cancelled = self
            .cancelled_remote_lock_waiters
            .lock()
            .map_err(|_| unavailable("DFS remote lock cancellation table is poisoned"))?;
        if cancelled.len() >= MAX_REMOTE_LOCK_WAITERS && !cancelled.contains(waiter) {
            return Err(Error::from(std::io::Error::from_raw_os_error(libc::ENOLCK)));
        }
        cancelled.insert(waiter.clone());
        Ok(())
    }

    fn clear_cancelled_remote_lock_waiter(&self, waiter: &LockWaiterId) -> Result<()> {
        self.cancelled_remote_lock_waiters
            .lock()
            .map_err(|_| unavailable("DFS remote lock cancellation table is poisoned"))?
            .remove(waiter);
        Ok(())
    }

    pub(super) fn cleanup_acknowledged_remote_lock_waiter(
        &self,
        waiter: &LockWaiterId,
    ) -> Result<()> {
        self.remove_remote_lock_waiter_route(waiter)?;
        self.clear_cancelled_remote_lock_waiter(waiter)
    }

    pub(super) fn retry_cancelled_remote_lock_waiter_acks_until(
        &self,
        deadline: std::time::Instant,
        max_ops: usize,
    ) -> Result<usize> {
        let mut cancelled = self
            .cancelled_remote_lock_waiters
            .lock()
            .map_err(|_| unavailable("DFS remote lock cancellation table is poisoned"))?
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        cancelled.sort_by(|left, right| {
            left.ingress_session_id
                .cmp(&right.ingress_session_id)
                .then_with(|| left.request_id.cmp(&right.request_id))
        });
        if !cancelled.is_empty() {
            let start = (self
                .lock_session_reap_cursor
                .fetch_add(1, Ordering::Relaxed) as usize)
                % cancelled.len();
            cancelled.rotate_left(start);
        }
        let waiters = self
            .remote_lock_waiters
            .lock()
            .map_err(|_| unavailable("DFS remote lock waiter table is poisoned"))?
            .clone();
        let mut acknowledged = 0usize;
        let mut first_error = None;
        let mut attempted = 0usize;
        for waiter in cancelled {
            if attempted >= max_ops || std::time::Instant::now() >= deadline {
                break;
            }
            attempted = attempted.saturating_add(1);
            let Some(remote) = waiters.get(&waiter) else {
                continue;
            };
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            let timeout = remaining.min(REMOTE_RELEASE_RPC_BUDGET);
            if timeout.is_zero() {
                break;
            }
            match remote.owner.acknowledge_lock_wait_with_timeout(
                afs_protocol::node_control::DfsOwnerAcknowledgeLockWaitRequest {
                    authority: Some(remote.authority.clone()),
                    waiter: Some(Self::wire_lock_waiter(&waiter)),
                },
                timeout,
            ) {
                Ok(_) => {
                    let inode_id = InodeId::new(remote.authority.inode_id.clone());
                    self.cleanup_acknowledged_remote_lock_waiter(&waiter)?;
                    self.cleanup_remote_lock_authority_if_idle(&inode_id)?;
                    acknowledged = acknowledged.saturating_add(1);
                }
                Err(error) => {
                    first_error.get_or_insert(error);
                }
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        Ok(acknowledged)
    }

    fn consume_precancelled_lock_waiter(&self, waiter: &LockWaiterId) -> Result<bool> {
        Ok(self
            .cancelled_remote_lock_waiters
            .lock()
            .map_err(|_| unavailable("DFS lock cancellation table is poisoned"))?
            .contains(waiter))
    }

    pub(super) fn note_precancelled_lock_waiter(&self, waiter: LockWaiterId) -> Result<()> {
        let mut cancelled = self
            .cancelled_remote_lock_waiters
            .lock()
            .map_err(|_| unavailable("DFS lock cancellation table is poisoned"))?;
        if cancelled.len() >= MAX_REMOTE_LOCK_WAITERS && !cancelled.contains(&waiter) {
            return Err(Error::from(std::io::Error::from_raw_os_error(libc::ENOLCK)));
        }
        cancelled.insert(waiter);
        Ok(())
    }

    pub(super) fn note_remote_lock_session(
        &self,
        inode_id: &InodeId,
        session_id: &str,
    ) -> Result<bool> {
        let mut sessions = self
            .remote_lock_authority_sessions
            .lock()
            .map_err(|_| unavailable("DFS remote lock authority session table is poisoned"))?;
        if let Some(active_sessions) = sessions.get_mut(inode_id) {
            if let Some(state) = active_sessions.get(session_id) {
                if *state == REMOTE_LOCK_SESSION_PENDING_CLOSE {
                    return Err(lock_error(LockError::Interrupted));
                }
                return Ok(false);
            }
            if self
                .closed_lock_session_admission_closed
                .load(Ordering::Acquire)
            {
                return Err(lock_error(LockError::Capacity));
            }
            if active_sessions.len() >= MAX_REMOTE_LOCK_WAITERS {
                return Err(Error::from(std::io::Error::from_raw_os_error(libc::ENOLCK)));
            }
            active_sessions.insert(session_id.to_owned(), REMOTE_LOCK_SESSION_ACTIVE);
            return Ok(true);
        }
        if self
            .closed_lock_session_admission_closed
            .load(Ordering::Acquire)
        {
            return Err(lock_error(LockError::Capacity));
        }
        if sessions.len() >= MAX_REMOTE_LOCK_AUTHORITIES {
            return Err(Error::from(std::io::Error::from_raw_os_error(libc::ENOLCK)));
        }
        sessions.insert(
            inode_id.clone(),
            HashMap::from([(session_id.to_owned(), REMOTE_LOCK_SESSION_ACTIVE)]),
        );
        Ok(true)
    }

    pub(super) fn mark_remote_lock_session_pending_close(
        &self,
        inode_id: &InodeId,
        session_id: &str,
    ) -> Result<bool> {
        let mut sessions = self
            .remote_lock_authority_sessions
            .lock()
            .map_err(|_| unavailable("DFS remote lock authority session table is poisoned"))?;
        let Some(active_sessions) = sessions.get_mut(inode_id) else {
            return Ok(false);
        };
        let Some(state) = active_sessions.get_mut(session_id) else {
            return Ok(false);
        };
        *state = REMOTE_LOCK_SESSION_PENDING_CLOSE;
        Ok(true)
    }

    pub(super) fn forget_remote_lock_session(
        &self,
        inode_id: &InodeId,
        session_id: &str,
    ) -> Result<()> {
        let mut sessions = self
            .remote_lock_authority_sessions
            .lock()
            .map_err(|_| unavailable("DFS remote lock authority session table is poisoned"))?;
        if let Some(active_sessions) = sessions.get_mut(inode_id) {
            active_sessions.remove(session_id);
            if active_sessions.is_empty() {
                sessions.remove(inode_id);
            }
        }
        Ok(())
    }

    fn remote_waiter_inodes(&self) -> Result<HashSet<InodeId>> {
        Ok(self
            .remote_lock_waiters
            .lock()
            .map_err(|_| unavailable("DFS remote lock waiter table is poisoned"))?
            .values()
            .map(|remote| InodeId::new(remote.authority.inode_id.clone()))
            .collect())
    }

    fn cleanup_remote_lock_authority_if_idle(&self, inode_id: &InodeId) -> Result<()> {
        let has_sessions = self
            .remote_lock_authority_sessions
            .lock()
            .map_err(|_| unavailable("DFS remote lock authority session table is poisoned"))?
            .get(inode_id)
            .is_some_and(|sessions| !sessions.is_empty());
        if has_sessions {
            return Ok(());
        }
        if self.remote_waiter_inodes()?.contains(inode_id) {
            return Ok(());
        }
        self.remote_lock_authorities
            .lock()
            .map_err(|_| unavailable("DFS remote lock authority table is poisoned"))?
            .remove(inode_id);
        Ok(())
    }

    fn cleanup_successful_remote_lock_session(
        &self,
        inode_id: &InodeId,
        scoped_session: &str,
    ) -> Result<()> {
        self.forget_remote_lock_session(inode_id, scoped_session)?;
        self.remote_lock_waiters
            .lock()
            .map_err(|_| unavailable("DFS remote lock waiter table is poisoned"))?
            .retain(|waiter, remote| {
                waiter.ingress_session_id != scoped_session
                    || remote.authority.inode_id != inode_id.0
            });
        self.cancelled_remote_lock_waiters
            .lock()
            .map_err(|_| unavailable("DFS remote lock cancellation table is poisoned"))?
            .retain(|waiter| waiter.ingress_session_id != scoped_session);
        self.cleanup_remote_lock_authority_if_idle(inode_id)
    }

    pub(super) fn release_remote_lock_session_once(
        &self,
        inode_id: &InodeId,
        remote: &DfsRemoteLockAuthority,
        scoped_session: &str,
        timeout: Duration,
    ) -> Result<()> {
        remote.owner.release_lock_session_with_timeout(
            afs_protocol::node_control::DfsOwnerReleaseLockSessionRequest {
                authority: Some(remote.authority.clone()),
                ingress_session_id: Self::raw_lock_session(scoped_session).to_owned(),
            },
            timeout,
        )?;
        self.cleanup_successful_remote_lock_session(inode_id, scoped_session)
    }

    pub(super) fn retry_closed_remote_lock_sessions_until(
        &self,
        deadline: std::time::Instant,
        max_ops: usize,
    ) -> Result<usize> {
        let closed = self
            .closed_lock_sessions
            .lock()
            .map_err(|_| unavailable("DFS closed lock session table is poisoned"))?
            .clone();
        let mut pending = self
            .remote_lock_authority_sessions
            .lock()
            .map_err(|_| unavailable("DFS remote lock authority session table is poisoned"))?
            .iter()
            .flat_map(|(inode_id, sessions)| {
                sessions
                    .iter()
                    .filter(|&(session, state)| {
                        *state == REMOTE_LOCK_SESSION_PENDING_CLOSE || closed.contains(session)
                    })
                    .map(|(session, _state)| (inode_id.clone(), session.clone()))
            })
            .collect::<Vec<_>>();
        pending.sort_by(|left, right| left.0.0.cmp(&right.0.0).then_with(|| left.1.cmp(&right.1)));
        if !pending.is_empty() {
            let start = (self
                .lock_session_reap_cursor
                .fetch_add(1, Ordering::Relaxed) as usize)
                % pending.len();
            pending.rotate_left(start);
        }
        let remotes = self
            .remote_lock_authorities
            .lock()
            .map_err(|_| unavailable("DFS remote lock authority table is poisoned"))?
            .clone();
        let mut released = 0usize;
        let mut first_error = None;
        let mut attempted = 0usize;
        for (inode_id, scoped_session) in pending {
            if attempted >= max_ops || std::time::Instant::now() >= deadline {
                break;
            }
            attempted = attempted.saturating_add(1);
            let Some(remote) = remotes.get(&inode_id) else {
                if let Err(error) =
                    self.cleanup_successful_remote_lock_session(&inode_id, &scoped_session)
                {
                    first_error.get_or_insert(error);
                } else {
                    released = released.saturating_add(1);
                }
                continue;
            };
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            let timeout = remaining.min(REMOTE_RELEASE_RPC_BUDGET);
            if timeout.is_zero() {
                break;
            }
            match self.release_remote_lock_session_once(&inode_id, remote, &scoped_session, timeout)
            {
                Ok(()) => released = released.saturating_add(1),
                Err(error) => {
                    first_error.get_or_insert(error);
                }
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        Ok(released)
    }

    pub(super) fn take_remote_lock_waiter_route_or_precancel(
        &self,
        waiter: LockWaiterId,
    ) -> Result<Option<DfsRemoteLockAuthority>> {
        let mut cancelled = self
            .cancelled_remote_lock_waiters
            .lock()
            .map_err(|_| unavailable("DFS remote lock cancellation table is poisoned"))?;
        let waiters = self
            .remote_lock_waiters
            .lock()
            .map_err(|_| unavailable("DFS remote lock waiter table is poisoned"))?;
        if let Some(remote) = waiters.get(&waiter).cloned() {
            return Ok(Some(remote));
        }
        if cancelled.len() >= MAX_REMOTE_LOCK_WAITERS && !cancelled.contains(&waiter) {
            return Err(Error::from(std::io::Error::from_raw_os_error(libc::ENOLCK)));
        }
        cancelled.insert(waiter);
        Ok(None)
    }

    pub(super) fn validate_lock_handle(
        &self,
        inode: BackendInode,
        handle: FileHandle,
        request: Option<&LockRequest>,
    ) -> Result<(InodeId, DfsFileHandleSnapshot)> {
        let inode_id = self.inode_id(inode)?;
        let snapshot = self.handle_snapshot(handle)?;
        if snapshot.inode_id != inode_id {
            return Err(bad_file_descriptor(
                "DFS lock handle belongs to another inode",
            ));
        }
        if let Some(request) = request
            && request.kind == FileLockKind::Posix
        {
            match request.lock_type {
                FileLockType::Read if snapshot.flags & libc::O_ACCMODE == libc::O_WRONLY => {
                    return Err(bad_file_descriptor(
                        "POSIX read lock requires a readable handle",
                    ));
                }
                FileLockType::Write if snapshot.flags & libc::O_ACCMODE == libc::O_RDONLY => {
                    return Err(bad_file_descriptor(
                        "POSIX write lock requires a writable handle",
                    ));
                }
                _ => {}
            }
        }
        Ok((inode_id, snapshot))
    }
}
