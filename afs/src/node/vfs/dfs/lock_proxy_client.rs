//! Remote-lock proxy, caller side (frozen per S1b H1).
//!
//! Under decision 11 FUSE does not forward advisory locks, so this code is
//! only reachable from unmount lock-session cleanup and unit tests. It was
//! moved here verbatim from the lock modules; it is intentionally not
//! redesigned until the POSIX-boundary work (S4 R20) decides its fate.
use super::*;

/// Remote-lock proxy tables. Four independent locks, as before the split;
/// they are not merged (no atomic snapshot across them is assumed).
#[derive(Default)]
pub(super) struct LockProxyState {
    pub(super) authorities: Mutex<HashMap<InodeId, DfsRemoteLockAuthority>>,
    pub(super) authority_sessions: Mutex<HashMap<InodeId, HashMap<String, usize>>>,
    pub(super) waiters: Mutex<HashMap<LockWaiterId, DfsRemoteLockAuthority>>,
    pub(super) cancelled_waiters: Mutex<HashSet<LockWaiterId>>,
}

#[derive(Clone)]
pub(super) struct DfsRemoteLockAuthority {
    pub(super) owner: Arc<dyn RemoteDfsOwner>,
    pub(super) authority: afs_protocol::node_control::DfsOwnerLockAuthority,
}

impl DistributedFs {
    pub(super) fn reclaim_idle_remote_lock_authorities(&self) -> Result<()> {
        let active_session_inodes = self
            .locks
            .proxy
            .authority_sessions
            .lock()
            .map_err(|_| unavailable("DFS remote lock authority session table is poisoned"))?
            .iter()
            .filter(|&(_inode_id, sessions)| !sessions.is_empty())
            .map(|(inode_id, _sessions)| inode_id.clone())
            .collect::<HashSet<_>>();
        let active_waiter_inodes = self
            .locks
            .proxy
            .waiters
            .lock()
            .map_err(|_| unavailable("DFS remote lock waiter table is poisoned"))?
            .values()
            .map(|remote| InodeId::new(remote.authority.inode_id.clone()))
            .collect::<HashSet<_>>();
        self.locks
            .proxy
            .authorities
            .lock()
            .map_err(|_| unavailable("DFS remote lock authority table is poisoned"))?
            .retain(|inode_id, _| {
                active_session_inodes.contains(inode_id) || active_waiter_inodes.contains(inode_id)
            });
        Ok(())
    }

    pub(super) fn remote_lock_authority(
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
            .locks
            .proxy
            .authorities
            .lock()
            .map_err(|_| unavailable("DFS remote lock authority table is poisoned"))?;
        if !authorities.contains_key(inode_id) && authorities.len() >= MAX_REMOTE_LOCK_AUTHORITIES {
            return Err(Error::from(std::io::Error::from_raw_os_error(libc::ENOLCK)));
        }
        authorities.insert(inode_id.clone(), remote.clone());
        Ok(remote)
    }

    pub(super) fn lock_authority_wire(
        &self,
        inode_id: &InodeId,
        lease: &WriteLease,
    ) -> afs_protocol::node_control::DfsOwnerLockAuthority {
        afs_protocol::node_control::DfsOwnerLockAuthority {
            namespace_id: self.identity.namespace_id.0.clone(),
            inode_id: inode_id.0.clone(),
            owner_node_id: lease.owner_node_id.clone(),
            owner_session_id: lease.owner_session_id.clone(),
            lease_epoch: lease.lease_epoch,
            lease_expires_at_unix_ms: lease.expires_at_unix_ms,
            caller_node_id: self.identity.node_id.clone(),
            caller_session_id: self.identity.session_id.clone(),
        }
    }

    pub(super) fn register_remote_lock_waiter_route(
        &self,
        waiter_id: &LockWaiterId,
        remote: DfsRemoteLockAuthority,
    ) -> Result<()> {
        let cancelled = self
            .locks
            .proxy
            .cancelled_waiters
            .lock()
            .map_err(|_| unavailable("DFS remote lock cancellation table is poisoned"))?;
        if cancelled.contains(waiter_id) {
            return Err(lock_error(LockError::Interrupted));
        }
        let mut waiters = self
            .locks
            .proxy
            .waiters
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

    pub(super) fn remove_remote_lock_waiter_route(&self, waiter_id: &LockWaiterId) -> Result<()> {
        self.locks
            .proxy
            .waiters
            .lock()
            .map_err(|_| unavailable("DFS remote lock waiter table is poisoned"))?
            .remove(waiter_id);
        Ok(())
    }

    pub(super) fn mark_cancelled_remote_lock_waiter(&self, waiter: &LockWaiterId) -> Result<()> {
        let mut cancelled = self
            .locks
            .proxy
            .cancelled_waiters
            .lock()
            .map_err(|_| unavailable("DFS remote lock cancellation table is poisoned"))?;
        if cancelled.len() >= MAX_REMOTE_LOCK_WAITERS && !cancelled.contains(waiter) {
            return Err(Error::from(std::io::Error::from_raw_os_error(libc::ENOLCK)));
        }
        cancelled.insert(waiter.clone());
        Ok(())
    }

    pub(super) fn clear_cancelled_remote_lock_waiter(&self, waiter: &LockWaiterId) -> Result<()> {
        self.locks
            .proxy
            .cancelled_waiters
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
            .locks
            .proxy
            .cancelled_waiters
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
                .locks
                .session_reap_cursor
                .fetch_add(1, Ordering::Relaxed) as usize)
                % cancelled.len();
            cancelled.rotate_left(start);
        }
        let waiters = self
            .locks
            .proxy
            .waiters
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

    pub(super) fn note_remote_lock_session(
        &self,
        inode_id: &InodeId,
        session_id: &str,
    ) -> Result<bool> {
        let mut sessions = self
            .locks
            .proxy
            .authority_sessions
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
                .locks
                .closed_session_admission_closed
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
            .locks
            .closed_session_admission_closed
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
            .locks
            .proxy
            .authority_sessions
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
            .locks
            .proxy
            .authority_sessions
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

    pub(super) fn remote_waiter_inodes(&self) -> Result<HashSet<InodeId>> {
        Ok(self
            .locks
            .proxy
            .waiters
            .lock()
            .map_err(|_| unavailable("DFS remote lock waiter table is poisoned"))?
            .values()
            .map(|remote| InodeId::new(remote.authority.inode_id.clone()))
            .collect())
    }

    pub(super) fn cleanup_remote_lock_authority_if_idle(&self, inode_id: &InodeId) -> Result<()> {
        let has_sessions = self
            .locks
            .proxy
            .authority_sessions
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
        self.locks
            .proxy
            .authorities
            .lock()
            .map_err(|_| unavailable("DFS remote lock authority table is poisoned"))?
            .remove(inode_id);
        Ok(())
    }

    pub(super) fn cleanup_successful_remote_lock_session(
        &self,
        inode_id: &InodeId,
        scoped_session: &str,
    ) -> Result<()> {
        self.forget_remote_lock_session(inode_id, scoped_session)?;
        self.locks
            .proxy
            .waiters
            .lock()
            .map_err(|_| unavailable("DFS remote lock waiter table is poisoned"))?
            .retain(|waiter, remote| {
                waiter.ingress_session_id != scoped_session
                    || remote.authority.inode_id != inode_id.0
            });
        self.locks
            .proxy
            .cancelled_waiters
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
            .locks
            .closed_sessions
            .lock()
            .map_err(|_| unavailable("DFS closed lock session table is poisoned"))?
            .clone();
        let mut pending = self
            .locks
            .proxy
            .authority_sessions
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
                .locks
                .session_reap_cursor
                .fetch_add(1, Ordering::Relaxed) as usize)
                % pending.len();
            pending.rotate_left(start);
        }
        let remotes = self
            .locks
            .proxy
            .authorities
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
            .locks
            .proxy
            .cancelled_waiters
            .lock()
            .map_err(|_| unavailable("DFS remote lock cancellation table is poisoned"))?;
        let waiters = self
            .locks
            .proxy
            .waiters
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
}
