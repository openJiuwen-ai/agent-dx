use super::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct DfsOwnerHandleScope {
    caller_node_id: String,
    caller_session_id: String,
    lease_epoch: u64,
    open_seq: u64,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct DfsOwnerOpenRouteKey {
    pub(super) caller_node_id: String,
    pub(super) caller_session_id: String,
    pub(super) owner_node_id: String,
    pub(super) owner_session_id: String,
}

pub(super) struct DfsRemoteOpenAdmissionLock {
    pub(super) gate: Mutex<()>,
    pub(super) users: AtomicUsize,
}

#[derive(Clone)]
pub(super) struct DfsOwnerOpenAdmission {
    fingerprint: [u8; 32],
    handle: afs_protocol::node_control::DfsOwnerHandle,
}

pub(super) struct DfsOwnerOpenInflight {
    fingerprint: [u8; 32],
    cleanup_scope: afs_protocol::node_control::DfsOwnerHandle,
}

#[derive(Default)]
pub(super) struct DfsOwnerOpenRouteState {
    pub(super) highwater: u64,
    pub(super) active: HashMap<u64, DfsOwnerOpenAdmission>,
    pub(super) inflight: HashMap<u64, DfsOwnerOpenInflight>,
    pub(super) cancelled: HashSet<u64>,
}

#[cfg(test)]
pub(super) struct OwnerOpenFinishPause {
    reached: (Mutex<bool>, Condvar),
    release: (Mutex<bool>, Condvar),
}

#[cfg(test)]
impl OwnerOpenFinishPause {
    pub(super) fn new() -> Self {
        Self {
            reached: (Mutex::new(false), Condvar::new()),
            release: (Mutex::new(false), Condvar::new()),
        }
    }

    pub(super) fn pause(&self) {
        let (reached_lock, reached_cv) = &self.reached;
        *reached_lock.lock().unwrap() = true;
        reached_cv.notify_all();
        let (release_lock, release_cv) = &self.release;
        let mut released = release_lock.lock().unwrap();
        while !*released {
            released = release_cv.wait(released).unwrap();
        }
    }

    pub(super) fn wait_until_reached(&self) {
        let (lock, cv) = &self.reached;
        let mut reached = lock.lock().unwrap();
        while !*reached {
            reached = cv.wait(reached).unwrap();
        }
    }

    pub(super) fn resume(&self) {
        let (lock, cv) = &self.release;
        *lock.lock().unwrap() = true;
        cv.notify_all();
    }
}

/// Results an owner handle keeps for proxied write/resize/sync replay (R54).
/// The oldest completed result is evicted to make room; in-flight
/// placeholders are never evicted.
pub(super) const OWNER_OPERATION_RESULTS_PER_HANDLE: usize = 64;

#[derive(Clone)]
pub(super) enum RemoteOperationResult {
    Write {
        fingerprint: [u8; 32],
        reply: afs_protocol::node_data::DfsOwnerWriteReply,
    },
    Resize {
        fingerprint: [u8; 32],
        reply: afs_protocol::node_data::DfsOwnerResizeReply,
    },
    Sync {
        fingerprint: [u8; 32],
        reply: afs_protocol::node_data::DfsOwnerSyncReply,
    },
}

impl RemoteOperationResult {
    fn fingerprint(&self) -> [u8; 32] {
        match self {
            Self::Write { fingerprint, .. }
            | Self::Resize { fingerprint, .. }
            | Self::Sync { fingerprint, .. } => *fingerprint,
        }
    }
}

enum OwnerOperationSlot {
    /// The operation is being applied; `token` identifies the claim that owns it.
    InFlight {
        fingerprint: [u8; 32],
        token: u64,
    },
    Done(RemoteOperationResult),
}

#[derive(Default)]
struct OwnerHandleOperations {
    /// Operation ids in arrival order (oldest first); same set as `entries`.
    order: VecDeque<OperationId>,
    entries: HashMap<OperationId, OwnerOperationSlot>,
}

/// Owner-side idempotency table for proxied mutating operations (ledger R54).
///
/// Invariants:
/// - Keyed by the owner-side `FileHandle`. Handle ids are never reused and the
///   table is only consulted after `validate_owner_handle` bound the caller
///   node/session to the handle, so the key is at least as narrow as the old
///   `(peer, caller_session, operation_id)` key.
/// - Each handle keeps at most `OWNER_OPERATION_RESULTS_PER_HANDLE` slots;
///   admitting a new operation evicts that handle's oldest `Done` slot.
/// - The process-wide slot count never exceeds `MAX_REMOTE_OPERATION_RESULTS`;
///   an operation that would grow past it is rejected *before* it is applied.
/// - A slot is `InFlight` from admission until the operation finishes; a
///   duplicate arriving meanwhile gets a retryable error. A failed operation
///   removes its placeholder, so errors are never cached.
/// - All slots of a handle are dropped when the owner handle is released,
///   its open is cancelled, or it is reaped with its caller session.
#[derive(Default)]
pub(super) struct OwnerOperationTable {
    handles: HashMap<FileHandle, OwnerHandleOperations>,
    total: usize,
    next_token: u64,
}

pub(super) enum OwnerOperationBegin {
    Cached(RemoteOperationResult),
    Apply(u64),
}

impl OwnerOperationTable {
    fn begin(
        &mut self,
        file: FileHandle,
        operation_id: &OperationId,
        fingerprint: [u8; 32],
    ) -> Result<OwnerOperationBegin> {
        if let Some(slot) = self
            .handles
            .get(&file)
            .and_then(|ops| ops.entries.get(operation_id))
        {
            let recorded = match slot {
                OwnerOperationSlot::InFlight { fingerprint, .. } => *fingerprint,
                OwnerOperationSlot::Done(result) => result.fingerprint(),
            };
            if recorded != fingerprint {
                return Err(invalid(
                    "DFS remote operation id was reused with a different request body",
                ));
            }
            return match slot {
                OwnerOperationSlot::InFlight { .. } => Err(unavailable(
                    "DFS remote operation is still in progress; retry later",
                )),
                OwnerOperationSlot::Done(result) => Ok(OwnerOperationBegin::Cached(result.clone())),
            };
        }
        let held = self.handles.get(&file).map_or(0, |ops| ops.entries.len());
        if held < OWNER_OPERATION_RESULTS_PER_HANDLE {
            if self.total >= MAX_REMOTE_OPERATION_RESULTS {
                return Err(unavailable(
                    "DFS remote operation table is at capacity; retry later",
                ));
            }
        } else {
            let ops = self.handles.get_mut(&file).expect("held > 0 implies entry");
            let oldest_done = ops
                .order
                .iter()
                .position(|id| matches!(ops.entries.get(id), Some(OwnerOperationSlot::Done(_))));
            let Some(index) = oldest_done else {
                return Err(unavailable(
                    "DFS owner handle has too many operations in progress; retry later",
                ));
            };
            let evicted = ops.order.remove(index).expect("index from position");
            ops.entries.remove(&evicted);
            self.total -= 1;
        }
        self.next_token = self.next_token.wrapping_add(1);
        let token = self.next_token;
        let ops = self.handles.entry(file).or_default();
        ops.order.push_back(operation_id.clone());
        ops.entries.insert(
            operation_id.clone(),
            OwnerOperationSlot::InFlight { fingerprint, token },
        );
        self.total += 1;
        Ok(OwnerOperationBegin::Apply(token))
    }

    fn claimed_slot(
        &mut self,
        file: FileHandle,
        operation_id: &OperationId,
        token: u64,
    ) -> Option<&mut OwnerOperationSlot> {
        self.handles
            .get_mut(&file)?
            .entries
            .get_mut(operation_id)
            .filter(|slot| {
                matches!(slot, OwnerOperationSlot::InFlight { token: owned, .. } if *owned == token)
            })
    }

    fn complete(
        &mut self,
        file: FileHandle,
        operation_id: &OperationId,
        token: u64,
        result: RemoteOperationResult,
    ) {
        // The handle may have been released while the operation ran; its
        // slots are gone then and there is nothing left to replay against.
        if let Some(slot) = self.claimed_slot(file, operation_id, token) {
            *slot = OwnerOperationSlot::Done(result);
        }
    }

    fn abandon(&mut self, file: FileHandle, operation_id: &OperationId, token: u64) {
        if self.claimed_slot(file, operation_id, token).is_none() {
            return;
        }
        let ops = self
            .handles
            .get_mut(&file)
            .expect("claimed slot implies entry");
        ops.entries.remove(operation_id);
        ops.order.retain(|id| id != operation_id);
        self.total -= 1;
        if ops.entries.is_empty() {
            self.handles.remove(&file);
        }
    }

    fn clear_handle(&mut self, file: FileHandle) {
        if let Some(ops) = self.handles.remove(&file) {
            self.total -= ops.entries.len();
        }
    }

    fn handle_ids(&self) -> Vec<FileHandle> {
        self.handles.keys().copied().collect()
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.total
    }
}

/// An admitted operation's in-flight placeholder. Dropping it without
/// `complete` (error or panic while applying) removes the placeholder so the
/// same operation id can be applied again.
struct OwnerOperationClaim<'a> {
    fs: &'a DistributedFs,
    file: FileHandle,
    operation_id: OperationId,
    token: u64,
    done: bool,
}

impl OwnerOperationClaim<'_> {
    fn complete(mut self, result: RemoteOperationResult) {
        self.done = true;
        // A poisoned table already fails every later admission, so dropping
        // the record here cannot turn into a silent second apply.
        if let Ok(mut table) = self.fs.remote_operation_results.lock() {
            table.complete(self.file, &self.operation_id, self.token, result);
        }
    }
}

impl Drop for OwnerOperationClaim<'_> {
    fn drop(&mut self) {
        if self.done {
            return;
        }
        if let Ok(mut table) = self.fs.remote_operation_results.lock() {
            table.abandon(self.file, &self.operation_id, self.token);
        }
    }
}

enum OwnerOperationStart<'a> {
    Cached(RemoteOperationResult),
    Apply(OwnerOperationClaim<'a>),
}

impl DistributedFs {
    /// Admits a proxied mutating operation on a validated owner handle: either
    /// returns the remembered result of a completed duplicate, or installs an
    /// in-flight placeholder that the caller must `complete` after applying.
    fn begin_remote_operation(
        &self,
        file: FileHandle,
        operation_id: OperationId,
        fingerprint: [u8; 32],
    ) -> Result<OwnerOperationStart<'_>> {
        let begin = self
            .remote_operation_results
            .lock()
            .map_err(|_| unavailable("DFS remote operation table is poisoned"))?
            .begin(file, &operation_id, fingerprint)?;
        Ok(match begin {
            OwnerOperationBegin::Cached(result) => OwnerOperationStart::Cached(result),
            OwnerOperationBegin::Apply(token) => OwnerOperationStart::Apply(OwnerOperationClaim {
                fs: self,
                file,
                operation_id,
                token,
                done: false,
            }),
        })
    }

    /// Drops every remembered operation of an owner handle that is gone.
    fn forget_owner_handle_operations(&self, file: FileHandle) -> Result<()> {
        self.remote_operation_results
            .lock()
            .map_err(|_| unavailable("DFS remote operation table is poisoned"))?
            .clear_handle(file);
        Ok(())
    }

    /// Safety net for release paths that do not go through the owner gateway:
    /// drop slots of handles that no longer exist.
    fn prune_released_owner_operations(&self) -> Result<()> {
        let tracked = self
            .remote_operation_results
            .lock()
            .map_err(|_| unavailable("DFS remote operation table is poisoned"))?
            .handle_ids();
        for file in tracked {
            if self.handles.get(file.0)?.is_none() {
                self.forget_owner_handle_operations(file)?;
            }
        }
        Ok(())
    }

    fn fingerprint_remote_write(
        handle: &afs_protocol::node_control::DfsOwnerHandle,
        offset: u64,
        append: bool,
        data: &[u8],
        kill_suidgid: bool,
    ) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        Self::fingerprint_handle(&mut hasher, handle);
        hasher.update(b"write");
        hasher.update(&offset.to_be_bytes());
        hasher.update(&[u8::from(append)]);
        hasher.update(&[u8::from(kill_suidgid)]);
        hasher.update(&(data.len() as u64).to_be_bytes());
        hasher.update(data);
        *hasher.finalize().as_bytes()
    }

    fn fingerprint_remote_resize(
        handle: &afs_protocol::node_control::DfsOwnerHandle,
        length: u64,
        kill_suidgid: bool,
    ) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        Self::fingerprint_handle(&mut hasher, handle);
        hasher.update(b"resize");
        hasher.update(&length.to_be_bytes());
        hasher.update(&[u8::from(kill_suidgid)]);
        *hasher.finalize().as_bytes()
    }

    fn fingerprint_remote_sync(
        handle: &afs_protocol::node_control::DfsOwnerHandle,
        through_write_seq: u64,
        data_only: bool,
    ) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        Self::fingerprint_handle(&mut hasher, handle);
        hasher.update(b"sync");
        hasher.update(&through_write_seq.to_be_bytes());
        hasher.update(&[u8::from(data_only)]);
        *hasher.finalize().as_bytes()
    }

    fn fingerprint_handle(
        hasher: &mut blake3::Hasher,
        handle: &afs_protocol::node_control::DfsOwnerHandle,
    ) {
        for part in [
            handle.namespace_id.as_bytes(),
            handle.inode_id.as_bytes(),
            handle.owner_node_id.as_bytes(),
            handle.owner_session_id.as_bytes(),
            handle.caller_node_id.as_bytes(),
            handle.caller_session_id.as_bytes(),
            handle.opaque_handle.as_slice(),
        ] {
            hasher.update(&(part.len() as u64).to_be_bytes());
            hasher.update(part);
        }
        hasher.update(&handle.lease_epoch.to_be_bytes());
    }
}

fn remote_operation_kind_mismatch() -> Error {
    invalid("DFS remote operation id was reused for a different operation kind")
}

pub(super) enum DfsOwnerOpenAdmissionStart {
    Replay(afs_protocol::node_control::DfsOwnerHandle),
    Cancelled,
    Apply {
        route: DfsOwnerOpenRouteKey,
        open_seq: u64,
        fingerprint: [u8; 32],
    },
}

impl crate::node::rpc::control::DfsOwnerLifecycleHandler for DistributedFs {
    fn open(
        &self,
        peer: &str,
        request: afs_protocol::node_control::DfsOwnerOpenRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerOpenReply> {
        if request.namespace_id != self.namespace_id.0
            || request.owner_node_id != self.node_id
            || request.owner_session_id != self.session_id
        {
            return Err(stale("DFS owner open targets another owner session"));
        }
        let inode_id = InodeId::new(request.inode_id.clone());
        if self.meta.current_node_session(peer)?.as_deref()
            != Some(request.caller_session_id.as_str())
        {
            return Err(stale("DFS owner open caller session is stale"));
        }
        let admission = self.start_owner_open_admission(peer, &request)?;
        let (route, open_seq, fingerprint) = match admission {
            DfsOwnerOpenAdmissionStart::Replay(handle) => {
                return Ok(afs_protocol::node_control::DfsOwnerOpenReply {
                    handle: Some(handle),
                });
            }
            DfsOwnerOpenAdmissionStart::Cancelled => {
                return Err(stale("DFS owner open admission was already retired"));
            }
            DfsOwnerOpenAdmissionStart::Apply {
                route,
                open_seq,
                fingerprint,
            } => (route, open_seq, fingerprint),
        };
        let (inode, lease) = match self.meta.resolve_write_authority(&inode_id) {
            Ok(opened) => opened,
            Err(error) => {
                self.finish_failed_owner_open_admission(&route, open_seq)?;
                return Err(error);
            }
        };
        let inode = match self.validate_inode(inode) {
            Ok(inode) => inode,
            Err(error) => {
                self.finish_failed_owner_open_admission(&route, open_seq)?;
                return Err(error);
            }
        };
        if lease.owner_node_id != self.node_id
            || lease.owner_session_id != self.session_id
            || lease.lease_epoch != request.lease_epoch
        {
            self.finish_failed_owner_open_admission(&route, open_seq)?;
            return Err(stale("DFS owner lease is no longer current"));
        }
        let apply = (|| {
            self.install_write_state(inode.clone(), lease.clone())?;
            let session = self.open_write_session(&inode, request.open_flags)?;
            if session.lease_epoch != request.lease_epoch {
                self.release_writer(&session)?;
                return Err(stale("DFS owner lease changed during open admission"));
            }
            if request.kill_suidgid {
                let state = self
                    .write_state(&inode.inode_id)?
                    .ok_or_else(|| stale("DFS write state is no longer open"))?;
                let mut state = self.lock_for_mutation(&inode.inode_id, &state)?;
                Self::apply_kernel_killpriv_to_state(&mut state);
            }
            self.allocate_handle(DfsFileHandle {
                gc_pin: None,
                inode_id: inode.inode_id.clone(),
                opened_inode: inode.clone(),
                opened_page_identity: None,
                native_read_view: None,
                native_read_generation: 0,
                native_denial_serial: 0,
                write_session: Some(Self::make_local_write_session(session)),
                flags: request.open_flags,
                owner_scope: Some(DfsOwnerHandleScope {
                    caller_node_id: peer.to_owned(),
                    caller_session_id: request.caller_session_id.clone(),
                    lease_epoch: lease.lease_epoch,
                    open_seq,
                }),
            })
        })();
        let handle = match apply {
            Ok(handle) => handle,
            Err(error) => {
                self.finish_failed_owner_open_admission(&route, open_seq)?;
                return Err(error);
            }
        };
        let wire = self.owner_handle_for(
            peer,
            request.caller_session_id,
            &inode,
            &lease,
            handle,
            open_seq,
        );
        #[cfg(test)]
        self.pause_before_owner_open_finish();
        let cancelled =
            match self.finish_owner_open_admission(&route, open_seq, fingerprint, wire.clone()) {
                Ok(cancelled) => cancelled,
                Err(error) => {
                    match <Self as Backend>::release(self, &owner_request_context(), handle) {
                        Ok(()) => {}
                        Err(release_error)
                            if release_error.code() == afs_error::NODE_DFS_STALE_HANDLE => {}
                        Err(release_error) => return Err(release_error),
                    }
                    self.forget_owner_handle_operations(handle)?;
                    return Err(error);
                }
            };
        if cancelled {
            <Self as Backend>::release(self, &owner_request_context(), handle)?;
            self.forget_owner_handle_operations(handle)?;
            self.finish_owner_open_identity_retirement(peer, &wire)?;
            return Err(stale("DFS owner open admission was cancelled"));
        }
        Ok(afs_protocol::node_control::DfsOwnerOpenReply { handle: Some(wire) })
    }

    fn getattr(
        &self,
        peer: &str,
        request: afs_protocol::node_control::DfsOwnerGetAttrRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerGetAttrReply> {
        let handle = request
            .handle
            .ok_or_else(|| stale("DFS owner getattr missing handle"))?;
        let file = self.validate_owner_handle(peer, &handle)?;
        let snapshot = self.handle_snapshot(file)?;
        let attr = self.visible_attributes(&snapshot.opened_inode)?;
        Ok(afs_protocol::node_control::DfsOwnerGetAttrReply {
            attr: Some(Self::wire_attr(attr)),
        })
    }

    fn get_lock(
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

    fn set_lock(
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

    fn cancel_lock_wait(
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

    fn acknowledge_lock_wait(
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

    fn release_locks(
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

    fn release_lock_session(
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

    fn release(
        &self,
        peer: &str,
        request: afs_protocol::node_control::DfsOwnerReleaseRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerReleaseReply> {
        let handle = request
            .handle
            .ok_or_else(|| stale("DFS owner release missing handle"))?;
        if handle.opaque_handle.is_empty() {
            if let Some(active) =
                self.prepare_owner_open_identity_retirement(peer, &handle, true)?
            {
                let file = self.owner_release_file_handle(peer, &active)?;
                let file = file.ok_or_else(|| stale("DFS owner release handle is not active"))?;
                <Self as Backend>::release(self, &owner_request_context(), file)?;
                self.forget_owner_handle_operations(file)?;
                self.finish_owner_open_identity_retirement(peer, &active)?;
            }
            return Ok(afs_protocol::node_control::DfsOwnerReleaseReply {});
        }
        self.validate_owner_release_session_current(peer, &handle)?;
        if let Some(file) = self.owner_release_file_handle(peer, &handle)? {
            <Self as Backend>::release(self, &owner_request_context(), file)?;
            self.forget_owner_handle_operations(file)?;
        }
        self.finish_owner_open_identity_retirement(peer, &handle)?;
        Ok(afs_protocol::node_control::DfsOwnerReleaseReply {})
    }
}

impl crate::node::rpc::data::DfsOwnerFilesHandler for DistributedFs {
    fn read(
        &self,
        peer: &str,
        request: afs_protocol::node_data::DfsOwnerReadRequest,
    ) -> Result<afs_protocol::node_data::DfsOwnerReadReply> {
        let handle = request
            .handle
            .ok_or_else(|| stale("DFS owner read missing handle"))?;
        let handle = Self::control_owner_handle(&handle);
        let file = self.validate_owner_handle(peer, &handle)?;
        let length = usize::try_from(request.length)
            .map_err(|_| invalid("DFS owner read length exceeds platform limits"))?;
        let data = <Self as Backend>::read_owned(
            self,
            &owner_request_context(),
            file,
            request.offset,
            length,
        )?;
        let visible_write_seq = self
            .write_state(&InodeId::new(handle.inode_id))?
            .and_then(|state| state.lock().ok().map(|state| state.visible_write_seq))
            .unwrap_or(0);
        Ok(afs_protocol::node_data::DfsOwnerReadReply {
            data,
            visible_write_seq,
        })
    }

    fn write(
        &self,
        peer: &str,
        request: afs_protocol::node_data::DfsOwnerWriteRequest,
    ) -> Result<afs_protocol::node_data::DfsOwnerWriteReply> {
        let handle = request
            .handle
            .clone()
            .ok_or_else(|| stale("DFS owner write missing handle"))?;
        let handle = Self::control_owner_handle(&handle);
        let file = self.validate_owner_handle(peer, &handle)?;
        let operation_id = OperationId::new(request.operation_id.clone());
        let fingerprint = Self::fingerprint_remote_write(
            &handle,
            request.offset,
            request.append,
            &request.data,
            request.kill_suidgid,
        );
        let claim = match self.begin_remote_operation(file, operation_id, fingerprint)? {
            OwnerOperationStart::Cached(RemoteOperationResult::Write { reply, .. }) => {
                return Ok(reply);
            }
            OwnerOperationStart::Cached(_) => return Err(remote_operation_kind_mismatch()),
            OwnerOperationStart::Apply(claim) => claim,
        };
        #[cfg(test)]
        self.run_remote_operation_before_apply_hook();
        let offset = if request.append {
            let snapshot = self.handle_snapshot(file)?;
            self.visible_attributes(&snapshot.opened_inode)?.size
        } else {
            request.offset
        };
        let written = <Self as Backend>::write_with_options(
            self,
            &owner_request_context(),
            file,
            offset,
            &request.data,
            WriteOptions {
                kill_suidgid: request.kill_suidgid,
            },
        )?;
        let snapshot = self.handle_snapshot(file)?;
        let accepted = snapshot
            .write_session
            .as_ref()
            .map_or(0, |session| session.local.last_accepted_seq);
        let reply = afs_protocol::node_data::DfsOwnerWriteReply {
            written: written as u64,
            accepted_write_seq: accepted,
            effective_offset: offset,
        };
        claim.complete(RemoteOperationResult::Write { fingerprint, reply });
        Ok(reply)
    }

    fn resize(
        &self,
        peer: &str,
        request: afs_protocol::node_data::DfsOwnerResizeRequest,
    ) -> Result<afs_protocol::node_data::DfsOwnerResizeReply> {
        let handle = request
            .handle
            .clone()
            .ok_or_else(|| stale("DFS owner resize missing handle"))?;
        let handle = Self::control_owner_handle(&handle);
        let file = self.validate_owner_handle(peer, &handle)?;
        let operation_id = OperationId::new(request.operation_id.clone());
        let fingerprint =
            Self::fingerprint_remote_resize(&handle, request.length, request.kill_suidgid);
        let claim = match self.begin_remote_operation(file, operation_id, fingerprint)? {
            OwnerOperationStart::Cached(RemoteOperationResult::Resize { reply, .. }) => {
                return Ok(reply);
            }
            OwnerOperationStart::Cached(_) => return Err(remote_operation_kind_mismatch()),
            OwnerOperationStart::Apply(claim) => claim,
        };
        let snapshot = self.handle_snapshot(file)?;
        let accepted =
            self.resize_dirty_inode(&snapshot.inode_id, request.length, request.kill_suidgid)?;
        self.update_handle_write_progress(file, accepted)?;
        let reply = afs_protocol::node_data::DfsOwnerResizeReply {
            accepted_write_seq: accepted,
        };
        claim.complete(RemoteOperationResult::Resize { fingerprint, reply });
        Ok(reply)
    }

    fn sync(
        &self,
        peer: &str,
        request: afs_protocol::node_data::DfsOwnerSyncRequest,
    ) -> Result<afs_protocol::node_data::DfsOwnerSyncReply> {
        let handle = request
            .handle
            .clone()
            .ok_or_else(|| stale("DFS owner sync missing handle"))?;
        let handle = Self::control_owner_handle(&handle);
        let file = self.validate_owner_handle(peer, &handle)?;
        let operation_id = OperationId::new(request.operation_id.clone());
        let fingerprint =
            Self::fingerprint_remote_sync(&handle, request.through_write_seq, request.data_only);
        let claim = match self.begin_remote_operation(file, operation_id, fingerprint)? {
            OwnerOperationStart::Cached(RemoteOperationResult::Sync { reply, .. }) => {
                return Ok(reply);
            }
            OwnerOperationStart::Cached(_) => return Err(remote_operation_kind_mismatch()),
            OwnerOperationStart::Apply(claim) => claim,
        };
        <Self as Backend>::fsync(
            self,
            &owner_request_context(),
            file,
            if request.data_only {
                SyncMode::DataOnly
            } else {
                SyncMode::Full
            },
        )?;
        let snapshot = self.handle_snapshot(file)?;
        let reply = afs_protocol::node_data::DfsOwnerSyncReply {
            durable_write_seq: snapshot
                .write_session
                .as_ref()
                .map_or(0, |session| session.local.last_synced_seq),
            file_version_id: self
                .write_state(&snapshot.inode_id)?
                .and_then(|state| {
                    state.lock().ok().and_then(|state| {
                        state
                            .base_version
                            .as_ref()
                            .map(|version| version.id.0.clone())
                    })
                })
                .unwrap_or_default(),
        };
        claim.complete(RemoteOperationResult::Sync {
            fingerprint,
            reply: reply.clone(),
        });
        Ok(reply)
    }
}

impl DistributedFs {
    #[cfg(test)]
    pub(super) fn set_owner_open_finish_pause(&self, pause: Option<Arc<OwnerOpenFinishPause>>) {
        *self.owner_open_finish_pause.lock().unwrap() = pause;
    }

    #[cfg(test)]
    pub(super) fn set_remote_operation_before_apply(&self, hook: Box<dyn FnOnce() + Send>) {
        *self.remote_operation_before_apply.lock().unwrap() = Some(hook);
    }

    #[cfg(test)]
    fn run_remote_operation_before_apply_hook(&self) {
        let hook = self.remote_operation_before_apply.lock().unwrap().take();
        if let Some(hook) = hook {
            hook();
        }
    }

    #[cfg(test)]
    pub(super) fn remote_operation_entry_count(&self) -> usize {
        self.remote_operation_results.lock().unwrap().len()
    }

    #[cfg(test)]
    fn pause_before_owner_open_finish(&self) {
        let pause = self.owner_open_finish_pause.lock().unwrap().clone();
        if let Some(pause) = pause {
            pause.pause();
        }
    }

    pub(super) fn validate_owner_open_seq(
        request: &afs_protocol::node_control::DfsOwnerOpenRequest,
    ) -> Result<u64> {
        if request.open_seq == 0 {
            return Err(stale("DFS owner open sequence is zero"));
        }
        Ok(request.open_seq)
    }

    fn owner_open_route(
        peer: &str,
        caller_session_id: &str,
        owner_node_id: &str,
        owner_session_id: &str,
    ) -> DfsOwnerOpenRouteKey {
        DfsOwnerOpenRouteKey {
            caller_node_id: peer.to_owned(),
            caller_session_id: caller_session_id.to_owned(),
            owner_node_id: owner_node_id.to_owned(),
            owner_session_id: owner_session_id.to_owned(),
        }
    }

    fn owner_open_fingerprint(
        peer: &str,
        request: &afs_protocol::node_control::DfsOwnerOpenRequest,
    ) -> Result<[u8; 32]> {
        let open_seq = Self::validate_owner_open_seq(request)?;
        let mut hasher = blake3::Hasher::new();
        for part in [
            peer.as_bytes(),
            request.namespace_id.as_bytes(),
            request.inode_id.as_bytes(),
            request.owner_node_id.as_bytes(),
            request.owner_session_id.as_bytes(),
            request.caller_session_id.as_bytes(),
        ] {
            hasher.update(&(part.len() as u64).to_be_bytes());
            hasher.update(part);
        }
        hasher.update(&request.lease_epoch.to_be_bytes());
        hasher.update(&request.open_flags.to_be_bytes());
        hasher.update(&[u8::from(request.kill_suidgid)]);
        hasher.update(&open_seq.to_be_bytes());
        Ok(*hasher.finalize().as_bytes())
    }

    pub(super) fn start_owner_open_admission(
        &self,
        peer: &str,
        request: &afs_protocol::node_control::DfsOwnerOpenRequest,
    ) -> Result<DfsOwnerOpenAdmissionStart> {
        let open_seq = Self::validate_owner_open_seq(request)?;
        let route = Self::owner_open_route(
            peer,
            &request.caller_session_id,
            &request.owner_node_id,
            &request.owner_session_id,
        );
        let fingerprint = Self::owner_open_fingerprint(peer, request)?;
        let mut routes = self
            .owner_open_routes
            .lock()
            .map_err(|_| unavailable("DFS owner open route table is poisoned"))?;
        if !routes.contains_key(&route) && routes.len() >= MAX_PENDING_REMOTE_RELEASES {
            return Err(Error::coded(
                afs_error::NODE_VFS_UNAVAILABLE,
                "DFS owner open route table is full",
            ));
        }
        let state = routes.entry(route.clone()).or_default();
        if let Some(active) = state.active.get(&open_seq) {
            if active.fingerprint == fingerprint {
                return Ok(DfsOwnerOpenAdmissionStart::Replay(active.handle.clone()));
            }
            return Err(stale("DFS owner open replay fingerprint changed"));
        }
        if state.cancelled.contains(&open_seq) {
            return Ok(DfsOwnerOpenAdmissionStart::Cancelled);
        }
        if let Some(existing) = state.inflight.get(&open_seq) {
            if existing.fingerprint == fingerprint {
                return Err(unavailable("DFS owner open admission is still pending"));
            }
            return Err(stale("DFS owner open replay fingerprint changed"));
        }
        if open_seq <= state.highwater {
            return Err(stale("DFS owner open sequence is already retired"));
        }
        if state.cancelled.len().saturating_add(state.inflight.len()) >= MAX_PENDING_REMOTE_RELEASES
        {
            return Err(Error::coded(
                afs_error::NODE_VFS_UNAVAILABLE,
                "DFS owner open admission table is full",
            ));
        }
        state.highwater = open_seq;
        state.inflight.insert(
            open_seq,
            DfsOwnerOpenInflight {
                fingerprint,
                cleanup_scope: Self::cleanup_handle_for_open_request(peer, request)?,
            },
        );
        Ok(DfsOwnerOpenAdmissionStart::Apply {
            route,
            open_seq,
            fingerprint,
        })
    }

    fn finish_failed_owner_open_admission(
        &self,
        route: &DfsOwnerOpenRouteKey,
        open_seq: u64,
    ) -> Result<()> {
        let mut routes = self
            .owner_open_routes
            .lock()
            .map_err(|_| unavailable("DFS owner open route table is poisoned"))?;
        if let Some(state) = routes.get_mut(route) {
            state.inflight.remove(&open_seq);
            state.cancelled.remove(&open_seq);
        }
        Ok(())
    }

    fn finish_owner_open_admission(
        &self,
        route: &DfsOwnerOpenRouteKey,
        open_seq: u64,
        fingerprint: [u8; 32],
        handle: afs_protocol::node_control::DfsOwnerHandle,
    ) -> Result<bool> {
        let mut routes = self
            .owner_open_routes
            .lock()
            .map_err(|_| unavailable("DFS owner open route table is poisoned"))?;
        let state = routes
            .get_mut(route)
            .ok_or_else(|| stale("DFS owner open route was retired"))?;
        state.inflight.remove(&open_seq);
        let cancelled = state.cancelled.remove(&open_seq);
        state.active.insert(
            open_seq,
            DfsOwnerOpenAdmission {
                fingerprint,
                handle,
            },
        );
        Ok(cancelled)
    }

    fn validate_owner_open_release_identity(
        &self,
        peer: &str,
        handle: &afs_protocol::node_control::DfsOwnerHandle,
    ) -> Result<DfsOwnerOpenRouteKey> {
        if handle.open_seq == 0 {
            return Err(stale("DFS owner open sequence is missing"));
        }
        if handle.namespace_id != self.namespace_id.0
            || handle.owner_node_id != self.node_id
            || handle.owner_session_id != self.session_id
            || handle.caller_node_id != peer
            || handle.caller_session_id.is_empty()
            || handle.lease_epoch == 0
        {
            return Err(stale("DFS owner handle identity does not match this owner"));
        }
        let current = self.meta.current_node_session(peer)?;
        if current.as_deref() != Some(handle.caller_session_id.as_str()) {
            return Err(stale("DFS owner release caller session is stale"));
        }
        Ok(Self::owner_open_route(
            peer,
            &handle.caller_session_id,
            &handle.owner_node_id,
            &handle.owner_session_id,
        ))
    }

    fn owner_open_active_scope_matches(
        active: &afs_protocol::node_control::DfsOwnerHandle,
        requested: &afs_protocol::node_control::DfsOwnerHandle,
        cleanup_only: bool,
    ) -> bool {
        active.namespace_id == requested.namespace_id
            && active.inode_id == requested.inode_id
            && active.owner_node_id == requested.owner_node_id
            && active.owner_session_id == requested.owner_session_id
            && active.lease_epoch == requested.lease_epoch
            && active.caller_node_id == requested.caller_node_id
            && active.caller_session_id == requested.caller_session_id
            && active.open_seq == requested.open_seq
            && (cleanup_only || active.opaque_handle == requested.opaque_handle)
    }

    fn prepare_owner_open_identity_retirement(
        &self,
        peer: &str,
        handle: &afs_protocol::node_control::DfsOwnerHandle,
        cleanup_only: bool,
    ) -> Result<Option<afs_protocol::node_control::DfsOwnerHandle>> {
        let route = self.validate_owner_open_release_identity(peer, handle)?;
        let mut routes = self
            .owner_open_routes
            .lock()
            .map_err(|_| unavailable("DFS owner open route table is poisoned"))?;
        if !routes.contains_key(&route) && routes.len() >= MAX_PENDING_REMOTE_RELEASES {
            return Err(Error::coded(
                afs_error::NODE_VFS_UNAVAILABLE,
                "DFS owner open route table is full",
            ));
        }
        let state = routes.entry(route).or_default();
        if let Some(active) = state.active.get(&handle.open_seq) {
            if !Self::owner_open_active_scope_matches(&active.handle, handle, cleanup_only) {
                return Err(stale(
                    "DFS owner open release scope does not match active admission",
                ));
            }
            return Ok(Some(active.handle.clone()));
        }
        if let Some(inflight) = state.inflight.get(&handle.open_seq) {
            if !Self::owner_open_active_scope_matches(&inflight.cleanup_scope, handle, true) {
                return Err(stale(
                    "DFS owner open release scope does not match inflight admission",
                ));
            }
            state.cancelled.insert(handle.open_seq);
            return Ok(None);
        }
        if handle.open_seq > state.highwater {
            state.highwater = handle.open_seq;
        }
        Ok(None)
    }

    fn finish_owner_open_identity_retirement(
        &self,
        peer: &str,
        handle: &afs_protocol::node_control::DfsOwnerHandle,
    ) -> Result<()> {
        let route = self.validate_owner_open_release_identity(peer, handle)?;
        let mut routes = self
            .owner_open_routes
            .lock()
            .map_err(|_| unavailable("DFS owner open route table is poisoned"))?;
        if let Some(state) = routes.get_mut(&route) {
            if let Some(active) = state.active.get(&handle.open_seq)
                && !Self::owner_open_active_scope_matches(&active.handle, handle, false)
            {
                return Err(stale(
                    "DFS owner open release scope does not match active admission",
                ));
            }
            state.active.remove(&handle.open_seq);
            state.cancelled.remove(&handle.open_seq);
            if handle.open_seq > state.highwater {
                state.highwater = handle.open_seq;
            }
        }
        Ok(())
    }

    fn validate_owner_release_session_current(
        &self,
        peer: &str,
        handle: &afs_protocol::node_control::DfsOwnerHandle,
    ) -> Result<()> {
        if handle.namespace_id != self.namespace_id.0
            || handle.owner_node_id != self.node_id
            || handle.owner_session_id != self.session_id
            || handle.caller_node_id != peer
            || handle.caller_session_id.is_empty()
            || handle.lease_epoch == 0
            || handle.open_seq == 0
        {
            return Err(stale("DFS owner handle identity does not match this owner"));
        }
        let current = self.meta.current_node_session(peer)?;
        if current.as_deref() != Some(handle.caller_session_id.as_str()) {
            return Err(stale("DFS owner release caller session is stale"));
        }
        Ok(())
    }

    fn validate_owner_handle(
        &self,
        peer: &str,
        handle: &afs_protocol::node_control::DfsOwnerHandle,
    ) -> Result<FileHandle> {
        let (inode_id, file) = self.validate_owner_handle_basic(peer, handle)?;
        let snapshot = self.handle_snapshot(file)?;
        self.validate_owner_handle_scope(peer, handle, &inode_id, &snapshot)?;
        let current = self.meta.current_node_session(peer)?;
        if current.as_deref() != Some(handle.caller_session_id.as_str()) {
            return Err(stale("DFS owner caller session is stale"));
        }
        let state = self
            .write_state(&inode_id)?
            .ok_or_else(|| stale("DFS owner write state is not open"))?;
        let state = state
            .lock()
            .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
        if state.write_lease.lease_epoch != handle.lease_epoch
            || state.write_lease.owner_node_id != self.node_id
            || state.write_lease.owner_session_id != self.session_id
        {
            return Err(stale("DFS owner lease epoch is stale"));
        }
        Ok(file)
    }

    fn validate_owner_handle_basic(
        &self,
        peer: &str,
        handle: &afs_protocol::node_control::DfsOwnerHandle,
    ) -> Result<(InodeId, FileHandle)> {
        if handle.namespace_id != self.namespace_id.0
            || handle.owner_node_id != self.node_id
            || handle.owner_session_id != self.session_id
            || handle.caller_node_id != peer
            || handle.lease_epoch == 0
            || handle.open_seq == 0
        {
            return Err(stale("DFS owner handle identity does not match this owner"));
        }
        Ok((
            InodeId::new(handle.inode_id.clone()),
            Self::file_handle_from_owner(handle)?,
        ))
    }

    fn validate_owner_handle_scope(
        &self,
        peer: &str,
        handle: &afs_protocol::node_control::DfsOwnerHandle,
        inode_id: &InodeId,
        snapshot: &DfsFileHandleSnapshot,
    ) -> Result<()> {
        if snapshot.inode_id != *inode_id {
            return Err(stale("DFS owner handle names another inode"));
        }
        let Some(scope) = snapshot.owner_scope.as_ref() else {
            return Err(stale("DFS owner handle is not a remote caller handle"));
        };
        if scope.caller_node_id != peer
            || scope.caller_session_id != handle.caller_session_id
            || scope.lease_epoch != handle.lease_epoch
            || scope.open_seq != handle.open_seq
        {
            return Err(stale("DFS owner handle caller scope does not match"));
        }
        Ok(())
    }

    fn owner_release_file_handle(
        &self,
        peer: &str,
        handle: &afs_protocol::node_control::DfsOwnerHandle,
    ) -> Result<Option<FileHandle>> {
        let (inode_id, file) = self.validate_owner_handle_basic(peer, handle)?;
        let Some(opened) = self.handles.get(file.0)? else {
            return Ok(None);
        };
        let snapshot = DfsFileHandleSnapshot::from_handle(&opened)?;
        self.validate_owner_handle_scope(peer, handle, &inode_id, &snapshot)?;
        Ok(Some(file))
    }

    fn peer_owner_handle_sessions(&self) -> Result<HashMap<String, HashSet<String>>> {
        let mut sessions: HashMap<String, HashSet<String>> = HashMap::new();
        for (_, handle) in self.handles.snapshot()? {
            if let Some(scope) = handle.owner_scope.as_ref() {
                sessions
                    .entry(scope.caller_node_id.clone())
                    .or_default()
                    .insert(scope.caller_session_id.clone());
            }
        }
        for route in self
            .owner_open_routes
            .lock()
            .map_err(|_| unavailable("DFS owner open route table is poisoned"))?
            .keys()
        {
            sessions
                .entry(route.caller_node_id.clone())
                .or_default()
                .insert(route.caller_session_id.clone());
        }
        Ok(sessions)
    }

    pub fn reap_expired_peer_owner_handles(&self) -> Result<usize> {
        self.prune_released_owner_operations()?;
        let sessions_by_node = self.peer_owner_handle_sessions()?;
        let mut peers = sessions_by_node.into_iter().collect::<Vec<_>>();
        peers.sort_by(|left, right| left.0.cmp(&right.0));
        if peers.is_empty() {
            return Ok(0);
        }
        let start =
            (self.owner_handle_reap_cursor.fetch_add(1, Ordering::AcqRel) as usize) % peers.len();
        peers.rotate_left(start);
        let deadline = std::time::Instant::now() + OWNER_HANDLE_REAP_BUDGET;
        let mut reaped = 0usize;
        let mut first_error = None;
        let mut checked = 0usize;
        for (node_id, sessions) in peers {
            if checked >= OWNER_HANDLE_REAP_MAX_PEERS || std::time::Instant::now() >= deadline {
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
                Ok(current) => {
                    for session_id in sessions {
                        if current.as_deref() != Some(session_id.as_str()) {
                            match self.reap_peer_owner_session(&node_id, &session_id) {
                                Ok(count) => reaped = reaped.saturating_add(count),
                                Err(error) => {
                                    first_error.get_or_insert(error);
                                }
                            }
                        }
                    }
                }
                Err(error) => {
                    first_error.get_or_insert(error);
                }
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        Ok(reaped)
    }

    fn reap_peer_owner_session(&self, node_id: &str, session_id: &str) -> Result<usize> {
        let handles = self
            .handles
            .snapshot()?
            .into_iter()
            .filter_map(|(id, handle)| {
                handle.owner_scope.as_ref().and_then(|scope| {
                    (scope.caller_node_id == node_id && scope.caller_session_id == session_id)
                        .then_some(FileHandle(id))
                })
            })
            .collect::<Vec<_>>();
        let mut reaped = 0usize;
        let mut first_error = None;
        for file in handles {
            let released = match <Self as Backend>::release(self, &owner_request_context(), file) {
                Ok(()) => {
                    reaped = reaped.saturating_add(1);
                    Ok(())
                }
                Err(error) if error.code() == afs_error::NODE_DFS_STALE_HANDLE => Ok(()),
                Err(error) => Err(error),
            }
            .and_then(|()| self.forget_owner_handle_operations(file));
            match released {
                Ok(()) => {}
                Err(error) => {
                    first_error.get_or_insert(error);
                }
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        self.owner_open_routes
            .lock()
            .map_err(|_| unavailable("DFS owner open route table is poisoned"))?
            .retain(|route, _| {
                !(route.caller_node_id == node_id
                    && route.caller_session_id == session_id
                    && route.owner_session_id == self.session_id)
            });
        Ok(reaped)
    }
}
