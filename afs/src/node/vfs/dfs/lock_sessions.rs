use super::*;

impl DistributedFs {
    fn start_lock_renewal_if_needed(&self) {
        self.locks.renewal.start(self.meta.clone());
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
            .locks
            .closed_sessions
            .lock()
            .map_err(|_| unavailable("DFS closed lock session table is poisoned"))?;
        // Closing a concrete session is cleanup, not new lock admission. Keep
        // this admission tombstone table bounded: when it is full, cleanup still
        // proceeds through the concrete lock tables and remote pending-close
        // state, but this global fast-path set is not expanded.
        if sessions.len() < MAX_CLOSED_LOCK_SESSIONS || sessions.contains(ingress_session_id) {
            sessions.insert(ingress_session_id.to_owned());
        } else {
            self.locks
                .closed_session_admission_closed
                .store(true, Ordering::Release);
        }
        Ok(())
    }

    pub(super) fn ensure_lock_session_open(&self, ingress_session_id: &str) -> Result<()> {
        if self
            .locks
            .closed_sessions
            .lock()
            .map_err(|_| unavailable("DFS closed lock session table is poisoned"))?
            .contains(ingress_session_id)
        {
            return Err(lock_error(LockError::Interrupted));
        }
        if self
            .locks
            .closed_session_admission_closed
            .load(Ordering::Acquire)
        {
            return Err(lock_error(LockError::Capacity));
        }
        Ok(())
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
