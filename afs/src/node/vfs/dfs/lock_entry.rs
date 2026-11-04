//! `Backend` advisory-lock entry points (local authority or remote-lock
//! proxy). Moved verbatim out of the `Backend` impl and frozen per S1b H1;
//! the trait methods forward here.
use super::*;

impl DistributedFs {
    pub(super) fn lock_entry_getlk(
        &self,
        _ctx: &RequestContext,
        inode: BackendInode,
        handle: FileHandle,
        request: LockRequest,
    ) -> Result<Option<FileLockConflict>> {
        let (inode_id, _) = self.validate_lock_handle(inode, handle, Some(&request))?;
        let request = self.scoped_local_lock_request(request)?;
        self.ensure_lock_session_open(&request.owner.ingress_session_id)?;
        match self.lock_authority_for(&inode_id)? {
            DfsLockTarget::Local(authority) => {
                self.check_lock_renewal(&authority)?;
                authority
                    .table
                    .getlk(&request)
                    .map_err(lock_error)
                    .map(|conflict| conflict.map(Self::raw_lock_conflict))
            }
            DfsLockTarget::Remote(remote) => {
                let reply =
                    remote
                        .owner
                        .get_lock(afs_protocol::node_control::DfsOwnerGetLockRequest {
                            authority: Some(remote.authority),
                            lock: Some(Self::wire_lock_request(&request)),
                        })?;
                reply
                    .conflict
                    .map(Self::file_lock_conflict_from_wire)
                    .transpose()
            }
        }
    }

    pub(super) fn lock_entry_setlk(
        &self,
        _ctx: &RequestContext,
        inode: BackendInode,
        handle: FileHandle,
        request: LockRequest,
        waiter: Option<LockWaiterId>,
    ) -> Result<()> {
        let (inode_id, _) = self.validate_lock_handle(inode, handle, Some(&request))?;
        let request = self.scoped_local_lock_request(request)?;
        let waiter = waiter
            .map(|waiter| self.scoped_local_lock_waiter(waiter))
            .transpose()?;
        if request.lock_type != FileLockType::Unlock {
            self.ensure_lock_session_open(&request.owner.ingress_session_id)?;
        }
        if let Some(waiter) = waiter.as_ref() {
            if waiter.ingress_session_id != request.owner.ingress_session_id {
                return Err(invalid("DFS lock waiter scope does not match owner scope"));
            }
            if request.lock_type != FileLockType::Unlock {
                self.ensure_lock_session_open(&waiter.ingress_session_id)?;
            }
        }
        match self.lock_authority_for(&inode_id)? {
            DfsLockTarget::Local(authority) => {
                self.check_lock_renewal(&authority)?;
                if let Some(waiter_id) = waiter.clone() {
                    self.register_lock_waiter(&authority, &waiter_id)?;
                    let result = authority
                        .table
                        .setlk_blocking(request.clone(), waiter_id.clone())
                        .map_err(lock_error);
                    self.unpin_lock_waiter(&authority, waiter.as_ref())?;
                    if result.is_ok() {
                        self.register_local_lock_success(&authority, &request)?;
                        authority
                            .table
                            .acknowledge_waiter(&waiter_id)
                            .map_err(lock_error)?;
                        self.check_lock_renewal_after_mutation(&authority)?;
                    } else if result
                        .as_ref()
                        .err()
                        .is_some_and(|error| error.code() == afs_error::IO_INTERRUPTED)
                    {
                        authority
                            .table
                            .acknowledge_waiter(&waiter_id)
                            .map_err(lock_error)?;
                    }
                    result
                } else {
                    let result = authority
                        .table
                        .setlk_nonblocking(request.clone())
                        .map_err(lock_error);
                    if result.is_ok() {
                        self.register_local_lock_success(&authority, &request)?;
                        self.check_lock_renewal_after_mutation(&authority)?;
                    }
                    result
                }
            }
            DfsLockTarget::Remote(remote) => {
                let pins_remote_session = request.lock_type != FileLockType::Unlock;
                let remote_session_was_new = if pins_remote_session {
                    self.note_remote_lock_session(&inode_id, &request.owner.ingress_session_id)?
                } else {
                    false
                };
                if let Some(waiter_id) = waiter.as_ref()
                    && let Err(error) =
                        self.register_remote_lock_waiter_route(waiter_id, remote.clone())
                {
                    if remote_session_was_new {
                        self.forget_remote_lock_session(
                            &inode_id,
                            &request.owner.ingress_session_id,
                        )?;
                    }
                    return Err(error);
                }
                let result =
                    remote
                        .owner
                        .set_lock(afs_protocol::node_control::DfsOwnerSetLockRequest {
                            authority: Some(remote.authority.clone()),
                            lock: Some(Self::wire_lock_request(&request)),
                            waiter: waiter.as_ref().map(Self::wire_lock_waiter),
                        });
                if result
                    .as_ref()
                    .err()
                    .is_some_and(|error| error.code() == afs_error::IO_INTERRUPTED)
                    && let Some(waiter_id) = waiter.as_ref()
                {
                    self.mark_cancelled_remote_lock_waiter(waiter_id)?;
                    if remote_session_was_new {
                        self.forget_remote_lock_session(
                            &inode_id,
                            &request.owner.ingress_session_id,
                        )?;
                    }
                    return Err(lock_error(LockError::Interrupted));
                }
                if result.is_err()
                    && waiter.is_some()
                    && let Some(waiter_id) = waiter.as_ref()
                {
                    match remote.owner.cancel_lock_wait(
                        afs_protocol::node_control::DfsOwnerCancelLockWaitRequest {
                            authority: Some(remote.authority.clone()),
                            waiter: Some(Self::wire_lock_waiter(waiter_id)),
                        },
                    ) {
                        Ok(reply) if reply.outcome == 1 => {
                            let ack = remote.owner.acknowledge_lock_wait(
                                afs_protocol::node_control::DfsOwnerAcknowledgeLockWaitRequest {
                                    authority: Some(remote.authority.clone()),
                                    waiter: Some(Self::wire_lock_waiter(waiter_id)),
                                },
                            );
                            if ack.is_err() {
                                self.mark_cancelled_remote_lock_waiter(waiter_id)?;
                                return Err(unavailable(
                                    "DFS remote lock cancellation could not be acknowledged; waiter identity retained",
                                ));
                            }
                            self.cleanup_acknowledged_remote_lock_waiter(waiter_id)?;
                            if remote_session_was_new {
                                self.forget_remote_lock_session(
                                    &inode_id,
                                    &request.owner.ingress_session_id,
                                )?;
                            }
                            return Err(lock_error(LockError::Interrupted));
                        }
                        Ok(reply) if reply.outcome == 2 => {
                            let ack = remote.owner.acknowledge_lock_wait(
                                afs_protocol::node_control::DfsOwnerAcknowledgeLockWaitRequest {
                                    authority: Some(remote.authority.clone()),
                                    waiter: Some(Self::wire_lock_waiter(waiter_id)),
                                },
                            );
                            if ack.is_ok() {
                                self.cleanup_acknowledged_remote_lock_waiter(waiter_id)?;
                            } else {
                                self.mark_cancelled_remote_lock_waiter(waiter_id)?;
                            }
                            return Ok(());
                        }
                        _ => {
                            return Err(unavailable(
                                "DFS remote lock cancellation outcome is unknown; waiter identity retained",
                            ));
                        }
                    }
                }
                if result.is_ok()
                    && let Some(waiter_id) = waiter.as_ref()
                {
                    let ack = remote.owner.acknowledge_lock_wait(
                        afs_protocol::node_control::DfsOwnerAcknowledgeLockWaitRequest {
                            authority: Some(remote.authority.clone()),
                            waiter: Some(Self::wire_lock_waiter(waiter_id)),
                        },
                    );
                    if ack.is_ok() {
                        self.cleanup_acknowledged_remote_lock_waiter(waiter_id)?;
                    } else {
                        self.mark_cancelled_remote_lock_waiter(waiter_id)?;
                    }
                }
                result.map(|_| ())
            }
        }
    }

    pub(super) fn lock_entry_cancel_lock_wait(&self, waiter: LockWaiterId) -> Result<()> {
        let waiter = self.scoped_local_lock_waiter(waiter)?;
        if let Some(remote) = self.take_remote_lock_waiter_route_or_precancel(waiter.clone())? {
            let authority = remote.authority.clone();
            let reply = remote.owner.cancel_lock_wait(
                afs_protocol::node_control::DfsOwnerCancelLockWaitRequest {
                    authority: Some(authority.clone()),
                    waiter: Some(Self::wire_lock_waiter(&waiter)),
                },
            )?;
            match reply.outcome {
                1 => {
                    match remote.owner.acknowledge_lock_wait(
                        afs_protocol::node_control::DfsOwnerAcknowledgeLockWaitRequest {
                            authority: Some(authority.clone()),
                            waiter: Some(Self::wire_lock_waiter(&waiter)),
                        },
                    ) {
                        Ok(_) => {
                            self.cleanup_acknowledged_remote_lock_waiter(&waiter)?;
                        }
                        Err(error) => {
                            self.mark_cancelled_remote_lock_waiter(&waiter)?;
                            return Err(error);
                        }
                    }
                }
                2 => {
                    let ack = remote.owner.acknowledge_lock_wait(
                        afs_protocol::node_control::DfsOwnerAcknowledgeLockWaitRequest {
                            authority: Some(authority),
                            waiter: Some(Self::wire_lock_waiter(&waiter)),
                        },
                    );
                    if ack.is_ok() {
                        self.cleanup_acknowledged_remote_lock_waiter(&waiter)?;
                    } else {
                        self.mark_cancelled_remote_lock_waiter(&waiter)?;
                    }
                }
                _ => {
                    return Err(Error::coded(
                        afs_error::IO_OTHER,
                        "DFS remote lock cancellation outcome is unknown",
                    ));
                }
            }
            return Ok(());
        }
        let authorities = self.locks.authorities.values_snapshot()?;
        let mut last_error = None;
        for authority in authorities {
            match authority
                .table
                .cancel_waiter_with_outcome(waiter.clone())
                .map_err(lock_error)
            {
                Ok(LockWaiterOutcome::Unknown) => {
                    last_error = Some(Error::coded(
                        afs_error::IO_OTHER,
                        "DFS local lock cancellation outcome is unknown",
                    ));
                }
                Ok(LockWaiterOutcome::Cancelled | LockWaiterOutcome::Granted) => {}
                Err(error) => last_error = Some(error),
            }
            authority
                .state
                .lock()
                .map_err(|_| unavailable("DFS lock authority state is poisoned"))?
                .waiters
                .remove(&waiter);
        }
        if let Some(error) = last_error {
            return Err(error);
        }
        Ok(())
    }

    pub(super) fn lock_entry_release_locks(
        &self,
        _ctx: &RequestContext,
        inode: BackendInode,
        handle: FileHandle,
        owner: FileLockOwner,
        kind: ReleaseKind,
    ) -> Result<()> {
        let (inode_id, _) = self.validate_lock_handle(inode, handle, None)?;
        let owner = self.scoped_local_lock_owner(owner)?;
        match self.existing_lock_authority_for(&inode_id)? {
            Some(DfsLockTarget::Local(authority)) => {
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
                Ok(())
            }
            Some(DfsLockTarget::Remote(remote)) => {
                remote.owner.release_locks(
                    afs_protocol::node_control::DfsOwnerReleaseLocksRequest {
                        authority: Some(remote.authority),
                        owner: Some(Self::wire_lock_owner(&owner)),
                        release_kind: Self::wire_lock_release_kind(kind) as i32,
                    },
                )?;
                // Keep remote session presence until explicit session close. Releasing one
                // kernel owner does not prove that this mount has no remaining locks.
                Ok(())
            }
            None => Ok(()),
        }
    }

    pub(super) fn lock_entry_release_lock_session(&self, ingress_session_id: &str) -> Result<()> {
        let scoped_session = Self::lock_scope(
            &self.identity.node_id,
            &self.identity.session_id,
            ingress_session_id,
        )?;
        let ingress_session_id = scoped_session.as_str();
        self.note_closed_lock_session(ingress_session_id)?;
        let authorities = self.locks.authorities.values_snapshot()?;
        let mut errors = Vec::new();
        for authority in authorities {
            if let Err(error) = authority
                .table
                .release_session(ingress_session_id)
                .map_err(lock_error)
            {
                errors.push(error);
                continue;
            }
            let mut state = authority
                .state
                .lock()
                .map_err(|_| unavailable("DFS lock authority state is poisoned"))?;
            state
                .pinned_owners
                .retain(|owner| owner.ingress_session_id != ingress_session_id);
            state
                .waiters
                .retain(|waiter| waiter.ingress_session_id != ingress_session_id);
        }
        let remotes = self
            .locks
            .proxy
            .authorities
            .lock()
            .map_err(|_| unavailable("DFS remote lock authority table is poisoned"))?
            .iter()
            .map(|(inode_id, remote)| (inode_id.clone(), remote.clone()))
            .collect::<Vec<_>>();
        for (inode_id, remote) in &remotes {
            let has_session = self
                .locks
                .proxy
                .authority_sessions
                .lock()
                .map_err(|_| unavailable("DFS remote lock authority session table is poisoned"))?
                .get(inode_id)
                .is_some_and(|sessions| sessions.contains_key(ingress_session_id));
            if !has_session {
                continue;
            }
            self.mark_remote_lock_session_pending_close(inode_id, ingress_session_id)?;
            if let Err(error) = self.release_remote_lock_session_once(
                inode_id,
                remote,
                ingress_session_id,
                REMOTE_RELEASE_RPC_BUDGET,
            ) {
                errors.push(error);
            }
        }
        if let Some(error) = errors.into_iter().next() {
            return Err(error);
        }
        Ok(())
    }
}
