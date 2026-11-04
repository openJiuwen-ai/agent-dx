//! Remote-lock proxy, owner side (frozen per S1b H1).
//!
//! Serves lock RPCs from peers that hold no lock authority themselves and
//! reaps lock sessions of peers whose node session ended. Moved verbatim
//! from the lock modules and the owner lifecycle handler; not redesigned
//! until the POSIX-boundary work (S4 R20) decides the proxy's fate.
use super::*;

impl DistributedFs {
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

    pub(super) fn validate_lock_authority(
        &self,
        peer: &str,
        authority: &afs_protocol::node_control::DfsOwnerLockAuthority,
    ) -> Result<WriteLease> {
        if authority.namespace_id != self.identity.namespace_id.0
            || authority.owner_node_id != self.identity.node_id
            || authority.owner_session_id != self.identity.session_id
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
        if renewed.owner_node_id != self.identity.node_id
            || renewed.owner_session_id != self.identity.session_id
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
        if authority.namespace_id != self.identity.namespace_id.0
            || authority.owner_node_id != self.identity.node_id
            || authority.owner_session_id != self.identity.session_id
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
            .locks
            .authorities
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

    pub(super) fn collect_peer_lock_scopes(
        &self,
    ) -> Result<HashMap<(String, String), HashSet<String>>> {
        let authorities = self.locks.authorities.values_snapshot()?;
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
        let authorities = self.locks.authorities.values_snapshot()?;
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
                .locks
                .session_reap_cursor
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

    pub(super) fn consume_precancelled_lock_waiter(&self, waiter: &LockWaiterId) -> Result<bool> {
        Ok(self
            .locks
            .proxy
            .cancelled_waiters
            .lock()
            .map_err(|_| unavailable("DFS lock cancellation table is poisoned"))?
            .contains(waiter))
    }

    pub(super) fn note_precancelled_lock_waiter(&self, waiter: LockWaiterId) -> Result<()> {
        let mut cancelled = self
            .locks
            .proxy
            .cancelled_waiters
            .lock()
            .map_err(|_| unavailable("DFS lock cancellation table is poisoned"))?;
        if cancelled.len() >= MAX_REMOTE_LOCK_WAITERS && !cancelled.contains(&waiter) {
            return Err(Error::from(std::io::Error::from_raw_os_error(libc::ENOLCK)));
        }
        cancelled.insert(waiter);
        Ok(())
    }

    pub(super) fn owner_get_lock(
        &self,
        peer: &str,
        request: afs_protocol::node_control::DfsOwnerGetLockRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerGetLockReply> {
        let authority = request
            .authority
            .ok_or_else(|| stale("DFS owner lock authority is missing"))?;
        let lock = request
            .lock
            .ok_or_else(|| invalid("DFS owner lock request is missing"))?;
        let caller_session_id = authority.caller_session_id.clone();
        let (_, authority) = self.local_authority_from_wire(peer, authority)?;
        let lock = Self::scoped_remote_lock_request(peer, &caller_session_id, lock)?;
        self.check_lock_renewal(&authority)?;
        let conflict = authority
            .table
            .getlk(&lock)
            .map_err(lock_error)?
            .map(Self::wire_lock_conflict);
        Ok(afs_protocol::node_control::DfsOwnerGetLockReply { conflict })
    }

    pub(super) fn owner_set_lock(
        &self,
        peer: &str,
        request: afs_protocol::node_control::DfsOwnerSetLockRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerSetLockReply> {
        let authority_wire = request
            .authority
            .ok_or_else(|| stale("DFS owner lock authority is missing"))?;
        let lock = request
            .lock
            .ok_or_else(|| invalid("DFS owner lock request is missing"))?;
        let caller_session_id = authority_wire.caller_session_id.clone();
        let waiter = request
            .waiter
            .map(|waiter| Self::scoped_remote_lock_waiter(peer, &caller_session_id, waiter))
            .transpose()?;
        let (_, authority) = self.local_authority_from_wire(peer, authority_wire)?;
        let lock = Self::scoped_remote_lock_request(peer, &caller_session_id, lock)?;
        self.ensure_lock_session_open(&lock.owner.ingress_session_id)?;
        if let Some(waiter_id) = waiter.as_ref() {
            if waiter_id.ingress_session_id != lock.owner.ingress_session_id {
                return Err(invalid("DFS lock waiter scope does not match owner scope"));
            }
            self.ensure_lock_session_open(&waiter_id.ingress_session_id)?;
        }
        self.check_lock_renewal(&authority)?;
        let result = if let Some(waiter_id) = waiter.clone() {
            self.register_lock_waiter(&authority, &waiter_id)?;
            let result = authority
                .table
                .setlk_blocking(lock.clone(), waiter_id.clone())
                .map_err(lock_error);
            self.unpin_lock_waiter(&authority, waiter.as_ref())?;
            result
        } else {
            authority
                .table
                .setlk_nonblocking(lock.clone())
                .map_err(lock_error)
        };
        if result.is_ok() {
            self.register_local_lock_success(&authority, &lock)?;
            self.check_lock_renewal_after_mutation(&authority)?;
        }
        result?;
        Ok(afs_protocol::node_control::DfsOwnerSetLockReply {})
    }

    pub(super) fn owner_cancel_lock_wait(
        &self,
        peer: &str,
        request: afs_protocol::node_control::DfsOwnerCancelLockWaitRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerCancelLockWaitReply> {
        let authority_wire = request
            .authority
            .ok_or_else(|| stale("DFS owner lock authority is missing"))?;
        let caller_session_id = authority_wire.caller_session_id.clone();
        let waiter = Self::scoped_remote_lock_waiter(
            peer,
            &caller_session_id,
            request
                .waiter
                .ok_or_else(|| invalid("DFS owner lock waiter is missing"))?,
        )?;
        let Some((_, authority)) = self.cached_local_authority_from_wire(peer, authority_wire)?
        else {
            self.note_precancelled_lock_waiter(waiter)?;
            return Ok(afs_protocol::node_control::DfsOwnerCancelLockWaitReply { outcome: 1 });
        };
        let outcome = authority
            .table
            .cancel_waiter_with_outcome(waiter.clone())
            .map_err(lock_error)?;
        authority
            .state
            .lock()
            .map_err(|_| unavailable("DFS lock authority state is poisoned"))?
            .waiters
            .remove(&waiter);
        Ok(afs_protocol::node_control::DfsOwnerCancelLockWaitReply {
            outcome: match outcome {
                LockWaiterOutcome::Cancelled => 1,
                LockWaiterOutcome::Granted => 2,
                LockWaiterOutcome::Unknown => 3,
            },
        })
    }

    pub(super) fn owner_acknowledge_lock_wait(
        &self,
        peer: &str,
        request: afs_protocol::node_control::DfsOwnerAcknowledgeLockWaitRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerAcknowledgeLockWaitReply> {
        let authority_wire = request
            .authority
            .ok_or_else(|| stale("DFS owner lock authority is missing"))?;
        let caller_session_id = authority_wire.caller_session_id.clone();
        let waiter = Self::scoped_remote_lock_waiter(
            peer,
            &caller_session_id,
            request
                .waiter
                .ok_or_else(|| invalid("DFS owner lock waiter is missing"))?,
        )?;
        let Some((_, authority)) = self.cached_local_authority_from_wire(peer, authority_wire)?
        else {
            return Ok(afs_protocol::node_control::DfsOwnerAcknowledgeLockWaitReply {});
        };
        authority
            .table
            .acknowledge_waiter(&waiter)
            .map_err(lock_error)?;
        authority
            .state
            .lock()
            .map_err(|_| unavailable("DFS lock authority state is poisoned"))?
            .waiters
            .remove(&waiter);
        Ok(afs_protocol::node_control::DfsOwnerAcknowledgeLockWaitReply {})
    }

    pub(super) fn owner_release_locks(
        &self,
        peer: &str,
        request: afs_protocol::node_control::DfsOwnerReleaseLocksRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerReleaseLocksReply> {
        let authority_wire = request
            .authority
            .ok_or_else(|| stale("DFS owner lock authority is missing"))?;
        let caller_session_id = authority_wire.caller_session_id.clone();
        let owner = Self::scoped_remote_lock_owner(
            peer,
            &caller_session_id,
            request
                .owner
                .ok_or_else(|| invalid("DFS owner lock owner is missing"))?,
        )?;
        let kind = Self::lock_release_kind_from_wire(request.release_kind)?;
        let Some((_, authority)) = self.cached_local_authority_from_wire(peer, authority_wire)?
        else {
            return Ok(afs_protocol::node_control::DfsOwnerReleaseLocksReply {});
        };
        match kind {
            ReleaseKind::PosixOwner => authority.table.release_posix_owner(&owner),
            ReleaseKind::FlockOwner => authority.table.release_flock_owner(&owner),
        }
        .map_err(lock_error)?;
        self.unpin_owner_if_inactive(
            &authority,
            &owner,
            Some(match kind {
                ReleaseKind::PosixOwner => FileLockKind::Posix,
                ReleaseKind::FlockOwner => FileLockKind::Flock,
            }),
        )?;
        Ok(afs_protocol::node_control::DfsOwnerReleaseLocksReply {})
    }

    pub(super) fn owner_release_lock_session(
        &self,
        peer: &str,
        request: afs_protocol::node_control::DfsOwnerReleaseLockSessionRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerReleaseLockSessionReply> {
        let authority_wire = request
            .authority
            .ok_or_else(|| stale("DFS owner lock authority is missing"))?;
        if request.ingress_session_id.is_empty() {
            return Err(invalid("DFS owner release lock session is empty"));
        }
        let caller_session_id = authority_wire.caller_session_id.clone();
        let scoped_session =
            Self::lock_scope(peer, &caller_session_id, &request.ingress_session_id)?;
        self.note_closed_lock_session(&scoped_session)?;
        let Some((_, authority)) = self.cached_local_authority_from_wire(peer, authority_wire)?
        else {
            return Ok(afs_protocol::node_control::DfsOwnerReleaseLockSessionReply {});
        };
        let known_session = {
            let state = authority
                .state
                .lock()
                .map_err(|_| unavailable("DFS lock authority state is poisoned"))?;
            state
                .pinned_owners
                .iter()
                .any(|owner| owner.ingress_session_id == scoped_session)
                || state
                    .waiters
                    .iter()
                    .any(|waiter| waiter.ingress_session_id == scoped_session)
        };
        if !known_session {
            return Ok(afs_protocol::node_control::DfsOwnerReleaseLockSessionReply {});
        }
        authority
            .table
            .release_session(&scoped_session)
            .map_err(lock_error)?;
        let mut state = authority
            .state
            .lock()
            .map_err(|_| unavailable("DFS lock authority state is poisoned"))?;
        state
            .pinned_owners
            .retain(|owner| owner.ingress_session_id != scoped_session);
        state
            .waiters
            .retain(|waiter| waiter.ingress_session_id != scoped_session);
        Ok(afs_protocol::node_control::DfsOwnerReleaseLockSessionReply {})
    }
}
