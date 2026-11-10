//! Owner side of the DFS write proxy.
//!
//! `OwnerGateway` holds what only the proxy needs (open routes, the replay
//! table, the reap cursor) and reaches local write state, handles and the
//! `Backend` write path exclusively through `OwnerLocalFiles`.
//!
//! Each RPC has two steps: the checks against Meta, and the local apply. The RPC
//! entries (`*_async` on `DistributedFs`) await Meta directly and move the apply
//! to the blocking pool, because the write/commit core and its locks stay
//! synchronous. The sync handler methods run the same steps in order.
use std::ops::ControlFlow;

use super::*;
use crate::node::vfs::dfs::owner_api::{OwnerFuture, offload_handler};

/// What the owner gateway needs from the local file layer.
pub(super) trait OwnerLocalFiles {
    /// Rejects an inode outside this mount's namespace.
    fn validate_owner_inode(&self, inode: InodeRecord) -> Result<InodeRecord>;
    /// Installs the lease's write state, opens a writer and allocates a handle
    /// scoped to the remote caller. Fails stale if the lease moved meanwhile.
    fn open_owner_handle(
        &self,
        inode: &InodeRecord,
        lease: &WriteLease,
        open_flags: i32,
        kill_suidgid: bool,
        scope: DfsOwnerHandleScope,
    ) -> Result<FileHandle>;
    /// The handle's inode and caller scope, `None` once the handle is gone.
    fn owner_handle_view(&self, file: FileHandle) -> Result<Option<OwnerHandleView>>;
    fn owner_handle_exists(&self, file: FileHandle) -> Result<bool>;
    /// Every open handle that serves a remote caller.
    fn owner_scoped_handles(&self) -> Result<Vec<(FileHandle, DfsOwnerHandleScope)>>;
    /// Fails stale unless this node session still holds `lease_epoch` for the
    /// inode's open write state.
    fn ensure_owner_write_lease(&self, inode_id: &InodeId, lease_epoch: u64) -> Result<()>;
    fn owner_visible_attributes(&self, file: FileHandle) -> Result<FileAttributes>;
    /// The bytes read and the inode's visible write sequence after the read.
    fn owner_read(
        &self,
        file: FileHandle,
        inode_id: &InodeId,
        offset: u64,
        length: usize,
    ) -> Result<(Vec<u8>, u64)>;
    /// Bytes written and the handle's accepted write sequence.
    fn owner_write(
        &self,
        file: FileHandle,
        offset: u64,
        data: &[u8],
        options: WriteOptions,
    ) -> Result<(usize, u64)>;
    /// The accepted write sequence of the resize.
    fn owner_resize(&self, file: FileHandle, length: u64, kill_suidgid: bool) -> Result<u64>;
    /// The handle's durable write sequence and the inode's base version id.
    fn owner_sync(
        &self,
        file: FileHandle,
        mode: SyncMode,
    ) -> Result<(SyncReceipt, Option<ProxiedCommit>)>;
    /// Releases the handle. `Ok(Some(_))`: it is gone, and this write error had
    /// not reached any caller yet; the releasing caller is its last receiver.
    fn release_owner_handle(&self, file: FileHandle) -> Result<Option<Error>>;
}

/// The part of an open handle the gateway validates a wire handle against.
pub(super) struct OwnerHandleView {
    inode_id: InodeId,
    owner_scope: Option<DfsOwnerHandleScope>,
}

/// Serves proxied open/IO/release RPCs for inodes this node session owns.
pub(super) struct OwnerGateway {
    identity: Arc<NodeIdentity>,
    meta: Arc<dyn DfsMeta>,
    pub(super) owner_open_routes: Mutex<HashMap<DfsOwnerOpenRouteKey, DfsOwnerOpenRouteState>>,
    owner_handle_reap_cursor: AtomicU64,
    /// Replay table for proxied write/resize/sync (R54).
    remote_operation_results: Mutex<OwnerOperationTable>,
    #[cfg(test)]
    test_hooks: OwnerGatewayTestHooks,
}

#[cfg(test)]
#[derive(Default)]
struct OwnerGatewayTestHooks {
    pub(super) owner_open_finish_pause: Mutex<Option<Arc<OwnerOpenFinishPause>>>,
    pub(super) remote_operation_before_apply: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}

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

impl OwnerGateway {
    pub(super) fn new(identity: Arc<NodeIdentity>, meta: Arc<dyn DfsMeta>) -> Self {
        Self {
            identity,
            meta,
            owner_open_routes: Mutex::new(HashMap::new()),
            owner_handle_reap_cursor: AtomicU64::new(0),
            remote_operation_results: Mutex::new(OwnerOperationTable::default()),
            #[cfg(test)]
            test_hooks: OwnerGatewayTestHooks::default(),
        }
    }

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
            OwnerOperationBegin::Apply(token) => OwnerOperationStart::Apply(
                OwnerOperationClaim::new(&self.remote_operation_results, file, operation_id, token),
            ),
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
    fn prune_released_owner_operations(&self, local: &dyn OwnerLocalFiles) -> Result<()> {
        let tracked = self
            .remote_operation_results
            .lock()
            .map_err(|_| unavailable("DFS remote operation table is poisoned"))?
            .handle_ids();
        for file in tracked {
            if !local.owner_handle_exists(file)? {
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

/// A wire handle that passed the identity and scope checks of this owner.
struct OwnerAdmittedHandle {
    handle: afs_protocol::node_control::DfsOwnerHandle,
    inode_id: InodeId,
    file: FileHandle,
}

impl OwnerAdmittedHandle {
    fn check_caller_session(&self, current: Option<String>) -> Result<()> {
        if current.as_deref() != Some(self.handle.caller_session_id.as_str()) {
            return Err(stale("DFS owner caller session is stale"));
        }
        Ok(())
    }

    fn ensure_lease(&self, local: &dyn OwnerLocalFiles) -> Result<()> {
        local.ensure_owner_write_lease(&self.inode_id, self.handle.lease_epoch)
    }
}

fn owner_read_length(request: &afs_protocol::node_data::DfsOwnerReadRequest) -> Result<usize> {
    usize::try_from(request.length)
        .map_err(|_| invalid("DFS owner read length exceeds platform limits"))
}

/// Route, sequence and fingerprint of an admission waiting for its apply.
type OwnerOpenAdmission = (DfsOwnerOpenRouteKey, u64, [u8; 32]);

pub(super) enum DfsOwnerOpenAdmissionStart {
    Replay(afs_protocol::node_control::DfsOwnerHandle),
    Cancelled,
    Apply {
        route: DfsOwnerOpenRouteKey,
        open_seq: u64,
        fingerprint: [u8; 32],
    },
}

impl OwnerGateway {
    pub(super) fn owner_open(
        &self,
        local: &dyn OwnerLocalFiles,
        peer: &str,
        request: afs_protocol::node_control::DfsOwnerOpenRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerOpenReply> {
        self.check_owner_open_target(&request)?;
        let current = self.meta.current_node_session(peer)?;
        Self::check_owner_open_caller_session(current, &request)?;
        let admission = match self.admit_owner_open(peer, &request)? {
            ControlFlow::Continue(admission) => admission,
            ControlFlow::Break(reply) => return Ok(reply),
        };
        let inode_id = InodeId::new(request.inode_id.clone());
        let authority = self.meta.resolve_write_authority(&inode_id);
        self.apply_owner_open(local, peer, request, admission, authority)
    }

    fn check_owner_open_target(
        &self,
        request: &afs_protocol::node_control::DfsOwnerOpenRequest,
    ) -> Result<()> {
        if request.namespace_id != self.identity.namespace_id.0
            || request.owner_node_id != self.identity.node_id
            || request.owner_session_id != self.identity.session_id
        {
            return Err(stale("DFS owner open targets another owner session"));
        }
        Ok(())
    }

    fn check_owner_open_caller_session(
        current: Option<String>,
        request: &afs_protocol::node_control::DfsOwnerOpenRequest,
    ) -> Result<()> {
        if current.as_deref() != Some(request.caller_session_id.as_str()) {
            return Err(stale("DFS owner open caller session is stale"));
        }
        Ok(())
    }

    /// Starts admission; breaks with the reply of a replay, which needs no apply.
    fn admit_owner_open(
        &self,
        peer: &str,
        request: &afs_protocol::node_control::DfsOwnerOpenRequest,
    ) -> Result<ControlFlow<afs_protocol::node_control::DfsOwnerOpenReply, OwnerOpenAdmission>>
    {
        match self.start_owner_open_admission(peer, request)? {
            DfsOwnerOpenAdmissionStart::Replay(handle) => Ok(ControlFlow::Break(
                afs_protocol::node_control::DfsOwnerOpenReply {
                    handle: Some(handle),
                },
            )),
            DfsOwnerOpenAdmissionStart::Cancelled => {
                Err(stale("DFS owner open admission was already retired"))
            }
            DfsOwnerOpenAdmissionStart::Apply {
                route,
                open_seq,
                fingerprint,
            } => Ok(ControlFlow::Continue((route, open_seq, fingerprint))),
        }
    }

    /// Finishes an admitted open with the authority Meta returned. Always settles
    /// the admission, so it must run even if the original caller went away.
    fn apply_owner_open(
        &self,
        local: &dyn OwnerLocalFiles,
        peer: &str,
        request: afs_protocol::node_control::DfsOwnerOpenRequest,
        (route, open_seq, fingerprint): OwnerOpenAdmission,
        authority: Result<(InodeRecord, WriteLease)>,
    ) -> Result<afs_protocol::node_control::DfsOwnerOpenReply> {
        let (inode, lease) = match authority {
            Ok(opened) => opened,
            Err(error) => {
                self.finish_failed_owner_open_admission(&route, open_seq)?;
                return Err(error);
            }
        };
        let inode = match local.validate_owner_inode(inode) {
            Ok(inode) => inode,
            Err(error) => {
                self.finish_failed_owner_open_admission(&route, open_seq)?;
                return Err(error);
            }
        };
        if lease.owner_node_id != self.identity.node_id
            || lease.owner_session_id != self.identity.session_id
            || lease.lease_epoch != request.lease_epoch
        {
            self.finish_failed_owner_open_admission(&route, open_seq)?;
            return Err(stale("DFS owner lease is no longer current"));
        }
        let apply = local.open_owner_handle(
            &inode,
            &lease,
            request.open_flags,
            request.kill_suidgid,
            DfsOwnerHandleScope {
                caller_node_id: peer.to_owned(),
                caller_session_id: request.caller_session_id.clone(),
                lease_epoch: lease.lease_epoch,
                open_seq,
            },
        );
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
                    match local.release_owner_handle(handle) {
                        Ok(_) => {}
                        Err(release_error)
                            if release_error.code() == afs_error::NODE_DFS_STALE_HANDLE => {}
                        Err(release_error) => return Err(release_error),
                    }
                    self.forget_owner_handle_operations(handle)?;
                    return Err(error);
                }
            };
        if cancelled {
            local.release_owner_handle(handle)?;
            self.forget_owner_handle_operations(handle)?;
            self.finish_owner_open_identity_retirement(peer, &wire)?;
            return Err(stale("DFS owner open admission was cancelled"));
        }
        Ok(afs_protocol::node_control::DfsOwnerOpenReply { handle: Some(wire) })
    }

    pub(super) fn owner_getattr(
        &self,
        local: &dyn OwnerLocalFiles,
        peer: &str,
        request: afs_protocol::node_control::DfsOwnerGetAttrRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerGetAttrReply> {
        let handle = request
            .handle
            .ok_or_else(|| stale("DFS owner getattr missing handle"))?;
        let admitted = self.admit_owner_request(local, peer, handle)?;
        self.apply_owner_getattr(local, &admitted)
    }

    fn apply_owner_getattr(
        &self,
        local: &dyn OwnerLocalFiles,
        admitted: &OwnerAdmittedHandle,
    ) -> Result<afs_protocol::node_control::DfsOwnerGetAttrReply> {
        admitted.ensure_lease(local)?;
        let attr = local.owner_visible_attributes(admitted.file)?;
        Ok(afs_protocol::node_control::DfsOwnerGetAttrReply {
            attr: Some(wire_attr(attr)),
        })
    }

    pub(super) fn owner_release(
        &self,
        local: &dyn OwnerLocalFiles,
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
                self.release_active_owner_open(local, peer, &active)?;
                self.finish_owner_open_identity_retirement(peer, &active)?;
            }
            return Ok(afs_protocol::node_control::DfsOwnerReleaseReply {});
        }
        self.validate_owner_release_session_current(peer, &handle)?;
        let undelivered = self.release_owner_open_if_present(local, peer, &handle)?;
        self.finish_owner_open_identity_retirement(peer, &handle)?;
        undelivered.map_or(Ok(afs_protocol::node_control::DfsOwnerReleaseReply {}), Err)
    }

    /// Cleanup release of an admission that has an active handle.
    fn release_active_owner_open(
        &self,
        local: &dyn OwnerLocalFiles,
        peer: &str,
        active: &afs_protocol::node_control::DfsOwnerHandle,
    ) -> Result<()> {
        let file = self.owner_release_file_handle(local, peer, active)?;
        let file = file.ok_or_else(|| stale("DFS owner release handle is not active"))?;
        local.release_owner_handle(file)?;
        self.forget_owner_handle_operations(file)
    }

    fn release_owner_open_if_present(
        &self,
        local: &dyn OwnerLocalFiles,
        peer: &str,
        handle: &afs_protocol::node_control::DfsOwnerHandle,
    ) -> Result<Option<Error>> {
        let Some(file) = self.owner_release_file_handle(local, peer, handle)? else {
            return Ok(None);
        };
        let undelivered = local.release_owner_handle(file)?;
        self.forget_owner_handle_operations(file)?;
        Ok(undelivered)
    }
}

impl OwnerGateway {
    pub(super) fn owner_read(
        &self,
        local: &dyn OwnerLocalFiles,
        peer: &str,
        request: afs_protocol::node_data::DfsOwnerReadRequest,
    ) -> Result<afs_protocol::node_data::DfsOwnerReadReply> {
        let handle = request
            .handle
            .as_ref()
            .ok_or_else(|| stale("DFS owner read missing handle"))?;
        let admitted = self.admit_owner_request(local, peer, control_owner_handle(handle))?;
        admitted.ensure_lease(local)?;
        let length = owner_read_length(&request)?;
        let (data, visible_write_seq) =
            local.owner_read(admitted.file, &admitted.inode_id, request.offset, length)?;
        Ok(afs_protocol::node_data::DfsOwnerReadReply {
            data,
            visible_write_seq,
        })
    }

    pub(super) fn owner_write(
        &self,
        local: &dyn OwnerLocalFiles,
        peer: &str,
        request: afs_protocol::node_data::DfsOwnerWriteRequest,
    ) -> Result<afs_protocol::node_data::DfsOwnerWriteReply> {
        let handle = request
            .handle
            .clone()
            .ok_or_else(|| stale("DFS owner write missing handle"))?;
        let admitted = self.admit_owner_request(local, peer, control_owner_handle(&handle))?;
        self.apply_owner_write(local, &admitted, request)
    }

    fn apply_owner_write(
        &self,
        local: &dyn OwnerLocalFiles,
        admitted: &OwnerAdmittedHandle,
        request: afs_protocol::node_data::DfsOwnerWriteRequest,
    ) -> Result<afs_protocol::node_data::DfsOwnerWriteReply> {
        admitted.ensure_lease(local)?;
        let (file, handle) = (admitted.file, &admitted.handle);
        let operation_id = OperationId::new(request.operation_id.clone());
        let fingerprint = Self::fingerprint_remote_write(
            handle,
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
            local.owner_visible_attributes(file)?.size
        } else {
            request.offset
        };
        let (written, accepted) = local.owner_write(
            file,
            offset,
            &request.data,
            WriteOptions {
                kill_suidgid: request.kill_suidgid,
            },
        )?;
        let reply = afs_protocol::node_data::DfsOwnerWriteReply {
            written: written as u64,
            accepted_write_seq: accepted,
            effective_offset: offset,
        };
        claim.complete(RemoteOperationResult::Write { fingerprint, reply });
        Ok(reply)
    }

    pub(super) fn owner_resize(
        &self,
        local: &dyn OwnerLocalFiles,
        peer: &str,
        request: afs_protocol::node_data::DfsOwnerResizeRequest,
    ) -> Result<afs_protocol::node_data::DfsOwnerResizeReply> {
        let handle = request
            .handle
            .clone()
            .ok_or_else(|| stale("DFS owner resize missing handle"))?;
        let admitted = self.admit_owner_request(local, peer, control_owner_handle(&handle))?;
        self.apply_owner_resize(local, &admitted, request)
    }

    fn apply_owner_resize(
        &self,
        local: &dyn OwnerLocalFiles,
        admitted: &OwnerAdmittedHandle,
        request: afs_protocol::node_data::DfsOwnerResizeRequest,
    ) -> Result<afs_protocol::node_data::DfsOwnerResizeReply> {
        admitted.ensure_lease(local)?;
        let (file, handle) = (admitted.file, &admitted.handle);
        let operation_id = OperationId::new(request.operation_id.clone());
        let fingerprint =
            Self::fingerprint_remote_resize(handle, request.length, request.kill_suidgid);
        let claim = match self.begin_remote_operation(file, operation_id, fingerprint)? {
            OwnerOperationStart::Cached(RemoteOperationResult::Resize { reply, .. }) => {
                return Ok(reply);
            }
            OwnerOperationStart::Cached(_) => return Err(remote_operation_kind_mismatch()),
            OwnerOperationStart::Apply(claim) => claim,
        };
        let accepted = local.owner_resize(file, request.length, request.kill_suidgid)?;
        let reply = afs_protocol::node_data::DfsOwnerResizeReply {
            accepted_write_seq: accepted,
        };
        claim.complete(RemoteOperationResult::Resize { fingerprint, reply });
        Ok(reply)
    }

    pub(super) fn owner_sync(
        &self,
        local: &dyn OwnerLocalFiles,
        peer: &str,
        request: afs_protocol::node_data::DfsOwnerSyncRequest,
    ) -> Result<afs_protocol::node_data::DfsOwnerSyncReply> {
        let handle = request
            .handle
            .clone()
            .ok_or_else(|| stale("DFS owner sync missing handle"))?;
        let admitted = self.admit_owner_request(local, peer, control_owner_handle(&handle))?;
        self.apply_owner_sync(local, &admitted, request)
    }

    fn apply_owner_sync(
        &self,
        local: &dyn OwnerLocalFiles,
        admitted: &OwnerAdmittedHandle,
        request: afs_protocol::node_data::DfsOwnerSyncRequest,
    ) -> Result<afs_protocol::node_data::DfsOwnerSyncReply> {
        admitted.ensure_lease(local)?;
        let (file, handle) = (admitted.file, &admitted.handle);
        let operation_id = OperationId::new(request.operation_id.clone());
        let fingerprint =
            Self::fingerprint_remote_sync(handle, request.through_write_seq, request.data_only);
        let claim = match self.begin_remote_operation(file, operation_id, fingerprint)? {
            OwnerOperationStart::Cached(RemoteOperationResult::Sync { reply, .. }) => {
                return Ok(reply);
            }
            OwnerOperationStart::Cached(_) => return Err(remote_operation_kind_mismatch()),
            OwnerOperationStart::Apply(claim) => claim,
        };
        let (receipt, proxied_commit) = local.owner_sync(
            file,
            if request.data_only {
                SyncMode::DataOnly
            } else {
                SyncMode::Full
            },
        )?;
        let reply = afs_protocol::node_data::DfsOwnerSyncReply {
            durable_write_seq: receipt.durable_write_seq,
            file_version_id: receipt.file_version_id,
            proxied_commit: proxied_commit.map(|commit| {
                afs_protocol::node_data::DfsOwnerProxiedCommit {
                    file_version_id: commit.version.0,
                    inode_revision: commit.revision,
                }
            }),
        };
        claim.complete(RemoteOperationResult::Sync {
            fingerprint,
            reply: reply.clone(),
        });
        Ok(reply)
    }
}

impl OwnerGateway {
    #[cfg(test)]
    pub(super) fn set_owner_open_finish_pause(&self, pause: Option<Arc<OwnerOpenFinishPause>>) {
        *self.test_hooks.owner_open_finish_pause.lock().unwrap() = pause;
    }

    #[cfg(test)]
    pub(super) fn set_remote_operation_before_apply(&self, hook: Box<dyn FnOnce() + Send>) {
        *self
            .test_hooks
            .remote_operation_before_apply
            .lock()
            .unwrap() = Some(hook);
    }

    #[cfg(test)]
    fn run_remote_operation_before_apply_hook(&self) {
        let hook = self
            .test_hooks
            .remote_operation_before_apply
            .lock()
            .unwrap()
            .take();
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
        let pause = self
            .test_hooks
            .owner_open_finish_pause
            .lock()
            .unwrap()
            .clone();
        if let Some(pause) = pause {
            pause.pause();
        }
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
        let open_seq = validate_owner_open_seq(request)?;
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
        let open_seq = validate_owner_open_seq(request)?;
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
                cleanup_scope: cleanup_handle_for_open_request(peer, request)?,
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
        let route = self.owner_open_release_route(peer, handle)?;
        Self::check_owner_release_caller_session(self.meta.current_node_session(peer)?, handle)?;
        Ok(route)
    }

    fn check_owner_release_caller_session(
        current: Option<String>,
        handle: &afs_protocol::node_control::DfsOwnerHandle,
    ) -> Result<()> {
        if current.as_deref() != Some(handle.caller_session_id.as_str()) {
            return Err(stale("DFS owner release caller session is stale"));
        }
        Ok(())
    }

    fn owner_open_release_route(
        &self,
        peer: &str,
        handle: &afs_protocol::node_control::DfsOwnerHandle,
    ) -> Result<DfsOwnerOpenRouteKey> {
        if handle.open_seq == 0 {
            return Err(stale("DFS owner open sequence is missing"));
        }
        if handle.namespace_id != self.identity.namespace_id.0
            || handle.owner_node_id != self.identity.node_id
            || handle.owner_session_id != self.identity.session_id
            || handle.caller_node_id != peer
            || handle.caller_session_id.is_empty()
            || handle.lease_epoch == 0
        {
            return Err(stale("DFS owner handle identity does not match this owner"));
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
        self.start_owner_open_retirement(route, handle, cleanup_only)
    }

    fn start_owner_open_retirement(
        &self,
        route: DfsOwnerOpenRouteKey,
        handle: &afs_protocol::node_control::DfsOwnerHandle,
        cleanup_only: bool,
    ) -> Result<Option<afs_protocol::node_control::DfsOwnerHandle>> {
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
        self.complete_owner_open_retirement(&route, handle)
    }

    fn complete_owner_open_retirement(
        &self,
        route: &DfsOwnerOpenRouteKey,
        handle: &afs_protocol::node_control::DfsOwnerHandle,
    ) -> Result<()> {
        let mut routes = self
            .owner_open_routes
            .lock()
            .map_err(|_| unavailable("DFS owner open route table is poisoned"))?;
        if let Some(state) = routes.get_mut(route) {
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
        self.check_owner_release_identity(peer, handle)?;
        Self::check_owner_release_caller_session(self.meta.current_node_session(peer)?, handle)
    }

    fn check_owner_release_identity(
        &self,
        peer: &str,
        handle: &afs_protocol::node_control::DfsOwnerHandle,
    ) -> Result<()> {
        if handle.namespace_id != self.identity.namespace_id.0
            || handle.owner_node_id != self.identity.node_id
            || handle.owner_session_id != self.identity.session_id
            || handle.caller_node_id != peer
            || handle.caller_session_id.is_empty()
            || handle.lease_epoch == 0
            || handle.open_seq == 0
        {
            return Err(stale("DFS owner handle identity does not match this owner"));
        }
        Ok(())
    }

    /// Validates a wire handle up to the Meta check. The lease check that follows
    /// it is part of the apply, because it can wait on the inode's write state.
    fn admit_owner_request(
        &self,
        local: &dyn OwnerLocalFiles,
        peer: &str,
        handle: afs_protocol::node_control::DfsOwnerHandle,
    ) -> Result<OwnerAdmittedHandle> {
        let admitted = self.check_owner_handle_locally(local, peer, handle)?;
        admitted.check_caller_session(self.meta.current_node_session(peer)?)?;
        Ok(admitted)
    }

    fn check_owner_handle_locally(
        &self,
        local: &dyn OwnerLocalFiles,
        peer: &str,
        handle: afs_protocol::node_control::DfsOwnerHandle,
    ) -> Result<OwnerAdmittedHandle> {
        let (inode_id, file) = self.validate_owner_handle_basic(peer, &handle)?;
        let view = local
            .owner_handle_view(file)?
            .ok_or_else(|| stale("DFS file handle is no longer open"))?;
        self.validate_owner_handle_scope(peer, &handle, &inode_id, &view)?;
        Ok(OwnerAdmittedHandle {
            handle,
            inode_id,
            file,
        })
    }

    fn validate_owner_handle_basic(
        &self,
        peer: &str,
        handle: &afs_protocol::node_control::DfsOwnerHandle,
    ) -> Result<(InodeId, FileHandle)> {
        if handle.namespace_id != self.identity.namespace_id.0
            || handle.owner_node_id != self.identity.node_id
            || handle.owner_session_id != self.identity.session_id
            || handle.caller_node_id != peer
            || handle.lease_epoch == 0
            || handle.open_seq == 0
        {
            return Err(stale("DFS owner handle identity does not match this owner"));
        }
        Ok((
            InodeId::new(handle.inode_id.clone()),
            file_handle_from_owner(handle)?,
        ))
    }

    fn validate_owner_handle_scope(
        &self,
        peer: &str,
        handle: &afs_protocol::node_control::DfsOwnerHandle,
        inode_id: &InodeId,
        view: &OwnerHandleView,
    ) -> Result<()> {
        if view.inode_id != *inode_id {
            return Err(stale("DFS owner handle names another inode"));
        }
        let Some(scope) = view.owner_scope.as_ref() else {
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
        local: &dyn OwnerLocalFiles,
        peer: &str,
        handle: &afs_protocol::node_control::DfsOwnerHandle,
    ) -> Result<Option<FileHandle>> {
        let (inode_id, file) = self.validate_owner_handle_basic(peer, handle)?;
        let Some(view) = local.owner_handle_view(file)? else {
            return Ok(None);
        };
        self.validate_owner_handle_scope(peer, handle, &inode_id, &view)?;
        Ok(Some(file))
    }

    fn owner_handle_for(
        &self,
        peer: &str,
        caller_session_id: String,
        inode: &InodeRecord,
        lease: &WriteLease,
        handle: FileHandle,
        open_seq: u64,
    ) -> afs_protocol::node_control::DfsOwnerHandle {
        afs_protocol::node_control::DfsOwnerHandle {
            namespace_id: self.identity.namespace_id.0.clone(),
            inode_id: inode.inode_id.0.clone(),
            owner_node_id: self.identity.node_id.clone(),
            owner_session_id: self.identity.session_id.clone(),
            lease_epoch: lease.lease_epoch,
            caller_node_id: peer.to_owned(),
            caller_session_id,
            opaque_handle: handle.0.to_be_bytes().to_vec(),
            open_seq,
        }
    }

    fn peer_owner_handle_sessions(
        &self,
        local: &dyn OwnerLocalFiles,
    ) -> Result<HashMap<String, HashSet<String>>> {
        let mut sessions: HashMap<String, HashSet<String>> = HashMap::new();
        for (_, scope) in local.owner_scoped_handles()? {
            sessions
                .entry(scope.caller_node_id)
                .or_default()
                .insert(scope.caller_session_id);
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

    pub(super) fn reap_expired_peer_owner_handles(
        &self,
        local: &dyn OwnerLocalFiles,
    ) -> Result<usize> {
        self.prune_released_owner_operations(local)?;
        let sessions_by_node = self.peer_owner_handle_sessions(local)?;
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
            let current_session = crate::node::call_deadline::within_timeout(timeout, || {
                self.meta.current_node_session(&node_id)
            });
            match current_session {
                Ok(current) => {
                    for session_id in sessions {
                        if current.as_deref() != Some(session_id.as_str()) {
                            match self.reap_peer_owner_session(local, &node_id, &session_id) {
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

    fn reap_peer_owner_session(
        &self,
        local: &dyn OwnerLocalFiles,
        node_id: &str,
        session_id: &str,
    ) -> Result<usize> {
        let handles = local
            .owner_scoped_handles()?
            .into_iter()
            .filter_map(|(file, scope)| {
                (scope.caller_node_id == node_id && scope.caller_session_id == session_id)
                    .then_some(file)
            })
            .collect::<Vec<_>>();
        let mut reaped = 0usize;
        let mut first_error = None;
        for file in handles {
            let released = match local.release_owner_handle(file) {
                Ok(_) => {
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
                    && route.owner_session_id == self.identity.session_id)
            });
        Ok(reaped)
    }
}

impl owner_api::DfsOwnerLifecycleHandler for DistributedFs {
    fn open(
        &self,
        peer: &str,
        request: afs_protocol::node_control::DfsOwnerOpenRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerOpenReply> {
        self.owner_gateway.owner_open(self, peer, request)
    }
    fn open_async(
        self: Arc<Self>,
        peer: String,
        request: afs_protocol::node_control::DfsOwnerOpenRequest,
    ) -> OwnerFuture<afs_protocol::node_control::DfsOwnerOpenReply> {
        Box::pin(async move {
            let gateway = &self.owner_gateway;
            gateway.check_owner_open_target(&request)?;
            let current = gateway
                .meta
                .current_node_session_async(peer.clone())
                .await?;
            OwnerGateway::check_owner_open_caller_session(current, &request)?;
            let admission = match gateway.admit_owner_open(&peer, &request)? {
                ControlFlow::Continue(admission) => admission,
                ControlFlow::Break(reply) => return Ok(reply),
            };
            let authority = gateway
                .meta
                .resolve_write_authority_async(InodeId::new(request.inode_id.clone()))
                .await;
            let fs = self.clone();
            offload_handler(move || {
                fs.owner_gateway
                    .apply_owner_open(&*fs, &peer, request, admission, authority)
            })
            .await
        })
    }
    fn getattr(
        &self,
        peer: &str,
        request: afs_protocol::node_control::DfsOwnerGetAttrRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerGetAttrReply> {
        self.owner_gateway.owner_getattr(self, peer, request)
    }
    fn getattr_async(
        self: Arc<Self>,
        peer: String,
        request: afs_protocol::node_control::DfsOwnerGetAttrRequest,
    ) -> OwnerFuture<afs_protocol::node_control::DfsOwnerGetAttrReply> {
        Box::pin(async move {
            let handle = request
                .handle
                .ok_or_else(|| stale("DFS owner getattr missing handle"))?;
            let admitted = self.admit_owner_request_async(&peer, handle).await?;
            let fs = self.clone();
            offload_handler(move || fs.owner_gateway.apply_owner_getattr(&*fs, &admitted)).await
        })
    }
    fn get_lock(
        &self,
        peer: &str,
        request: afs_protocol::node_control::DfsOwnerGetLockRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerGetLockReply> {
        self.owner_get_lock(peer, request)
    }
    fn set_lock(
        &self,
        peer: &str,
        request: afs_protocol::node_control::DfsOwnerSetLockRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerSetLockReply> {
        self.owner_set_lock(peer, request)
    }
    fn cancel_lock_wait(
        &self,
        peer: &str,
        request: afs_protocol::node_control::DfsOwnerCancelLockWaitRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerCancelLockWaitReply> {
        self.owner_cancel_lock_wait(peer, request)
    }
    fn acknowledge_lock_wait(
        &self,
        peer: &str,
        request: afs_protocol::node_control::DfsOwnerAcknowledgeLockWaitRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerAcknowledgeLockWaitReply> {
        self.owner_acknowledge_lock_wait(peer, request)
    }
    fn release_locks(
        &self,
        peer: &str,
        request: afs_protocol::node_control::DfsOwnerReleaseLocksRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerReleaseLocksReply> {
        self.owner_release_locks(peer, request)
    }
    fn release_lock_session(
        &self,
        peer: &str,
        request: afs_protocol::node_control::DfsOwnerReleaseLockSessionRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerReleaseLockSessionReply> {
        self.owner_release_lock_session(peer, request)
    }
    fn release(
        &self,
        peer: &str,
        request: afs_protocol::node_control::DfsOwnerReleaseRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerReleaseReply> {
        self.owner_gateway.owner_release(self, peer, request)
    }
    fn release_async(
        self: Arc<Self>,
        peer: String,
        request: afs_protocol::node_control::DfsOwnerReleaseRequest,
    ) -> OwnerFuture<afs_protocol::node_control::DfsOwnerReleaseReply> {
        Box::pin(async move {
            let handle = request
                .handle
                .ok_or_else(|| stale("DFS owner release missing handle"))?;
            let reply = afs_protocol::node_control::DfsOwnerReleaseReply {};
            let gateway = &self.owner_gateway;
            if handle.opaque_handle.is_empty() {
                let route = gateway.owner_open_release_route(&peer, &handle)?;
                let current = gateway
                    .meta
                    .current_node_session_async(peer.clone())
                    .await?;
                OwnerGateway::check_owner_release_caller_session(current, &handle)?;
                if let Some(active) = gateway.start_owner_open_retirement(route, &handle, true)? {
                    let (fs, caller, released) = (self.clone(), peer.clone(), active.clone());
                    offload_handler(move || {
                        fs.owner_gateway
                            .release_active_owner_open(&*fs, &caller, &released)
                    })
                    .await?;
                    self.finish_owner_open_retirement_async(&peer, &active)
                        .await?;
                }
                return Ok(reply);
            }
            gateway.check_owner_release_identity(&peer, &handle)?;
            let current = gateway
                .meta
                .current_node_session_async(peer.clone())
                .await?;
            OwnerGateway::check_owner_release_caller_session(current, &handle)?;
            let (fs, caller, released) = (self.clone(), peer.clone(), handle.clone());
            let undelivered = offload_handler(move || {
                fs.owner_gateway
                    .release_owner_open_if_present(&*fs, &caller, &released)
            })
            .await?;
            self.finish_owner_open_retirement_async(&peer, &handle)
                .await?;
            undelivered.map_or(Ok(reply), Err)
        })
    }
}

impl owner_api::DfsOwnerFilesHandler for DistributedFs {
    fn read(
        &self,
        peer: &str,
        request: afs_protocol::node_data::DfsOwnerReadRequest,
    ) -> Result<afs_protocol::node_data::DfsOwnerReadReply> {
        self.owner_gateway.owner_read(self, peer, request)
    }
    fn read_async(
        self: Arc<Self>,
        peer: String,
        request: afs_protocol::node_data::DfsOwnerReadRequest,
    ) -> OwnerFuture<afs_protocol::node_data::DfsOwnerReadReply> {
        Box::pin(async move {
            let handle = request
                .handle
                .as_ref()
                .ok_or_else(|| stale("DFS owner read missing handle"))?;
            let admitted = self
                .admit_owner_request_async(&peer, control_owner_handle(handle))
                .await?;
            crate::node::bridge::local_io(|| admitted.ensure_lease(&*self))?;
            let length = owner_read_length(&request)?;
            let (data, visible_write_seq) = self
                .owner_read_async(admitted.file, admitted.inode_id, request.offset, length)
                .await?;
            Ok(afs_protocol::node_data::DfsOwnerReadReply {
                data,
                visible_write_seq,
            })
        })
    }
    fn write(
        &self,
        peer: &str,
        request: afs_protocol::node_data::DfsOwnerWriteRequest,
    ) -> Result<afs_protocol::node_data::DfsOwnerWriteReply> {
        self.owner_gateway.owner_write(self, peer, request)
    }
    fn write_async(
        self: Arc<Self>,
        peer: String,
        request: afs_protocol::node_data::DfsOwnerWriteRequest,
    ) -> OwnerFuture<afs_protocol::node_data::DfsOwnerWriteReply> {
        Box::pin(async move {
            let handle = request
                .handle
                .as_ref()
                .ok_or_else(|| stale("DFS owner write missing handle"))?;
            let admitted = self
                .admit_owner_request_async(&peer, control_owner_handle(handle))
                .await?;
            let fs = self.clone();
            offload_handler(move || fs.owner_gateway.apply_owner_write(&*fs, &admitted, request))
                .await
        })
    }
    fn resize(
        &self,
        peer: &str,
        request: afs_protocol::node_data::DfsOwnerResizeRequest,
    ) -> Result<afs_protocol::node_data::DfsOwnerResizeReply> {
        self.owner_gateway.owner_resize(self, peer, request)
    }
    fn resize_async(
        self: Arc<Self>,
        peer: String,
        request: afs_protocol::node_data::DfsOwnerResizeRequest,
    ) -> OwnerFuture<afs_protocol::node_data::DfsOwnerResizeReply> {
        Box::pin(async move {
            let handle = request
                .handle
                .as_ref()
                .ok_or_else(|| stale("DFS owner resize missing handle"))?;
            let admitted = self
                .admit_owner_request_async(&peer, control_owner_handle(handle))
                .await?;
            let fs = self.clone();
            offload_handler(move || {
                fs.owner_gateway
                    .apply_owner_resize(&*fs, &admitted, request)
            })
            .await
        })
    }
    fn sync(
        &self,
        peer: &str,
        request: afs_protocol::node_data::DfsOwnerSyncRequest,
    ) -> Result<afs_protocol::node_data::DfsOwnerSyncReply> {
        self.owner_gateway.owner_sync(self, peer, request)
    }
    fn sync_async(
        self: Arc<Self>,
        peer: String,
        request: afs_protocol::node_data::DfsOwnerSyncRequest,
    ) -> OwnerFuture<afs_protocol::node_data::DfsOwnerSyncReply> {
        Box::pin(async move {
            let handle = request
                .handle
                .as_ref()
                .ok_or_else(|| stale("DFS owner sync missing handle"))?;
            let admitted = self
                .admit_owner_request_async(&peer, control_owner_handle(handle))
                .await?;
            let fs = self.clone();
            offload_handler(move || fs.owner_gateway.apply_owner_sync(&*fs, &admitted, request))
                .await
        })
    }
}

impl OwnerLocalFiles for DistributedFs {
    fn validate_owner_inode(&self, inode: InodeRecord) -> Result<InodeRecord> {
        self.validate_inode(inode)
    }

    fn open_owner_handle(
        &self,
        inode: &InodeRecord,
        lease: &WriteLease,
        open_flags: i32,
        kill_suidgid: bool,
        scope: DfsOwnerHandleScope,
    ) -> Result<FileHandle> {
        self.install_write_state(inode.clone(), lease.clone())?;
        let session = self.open_write_session(inode, open_flags)?;
        if session.lease_epoch != lease.lease_epoch {
            self.abandon_writer(&session)?;
            return Err(stale("DFS owner lease changed during open admission"));
        }
        if kill_suidgid {
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
            flags: open_flags,
            owner_scope: Some(scope),
        })
    }

    fn owner_handle_view(&self, file: FileHandle) -> Result<Option<OwnerHandleView>> {
        let Some(opened) = self.handles.get(file.0)? else {
            return Ok(None);
        };
        let snapshot = DfsFileHandleSnapshot::from_handle(&opened)?;
        Ok(Some(OwnerHandleView {
            inode_id: snapshot.inode_id,
            owner_scope: snapshot.owner_scope,
        }))
    }

    fn owner_handle_exists(&self, file: FileHandle) -> Result<bool> {
        Ok(self.handles.get(file.0)?.is_some())
    }

    fn owner_scoped_handles(&self) -> Result<Vec<(FileHandle, DfsOwnerHandleScope)>> {
        Ok(self
            .handles
            .snapshot()?
            .into_iter()
            .filter_map(|(id, handle)| {
                handle
                    .owner_scope
                    .as_ref()
                    .map(|scope| (FileHandle(id), scope.clone()))
            })
            .collect())
    }

    fn ensure_owner_write_lease(&self, inode_id: &InodeId, lease_epoch: u64) -> Result<()> {
        let state = self
            .write_state(inode_id)?
            .ok_or_else(|| stale("DFS owner write state is not open"))?;
        let state = state
            .lock()
            .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
        if state.write_lease.lease_epoch != lease_epoch
            || state.write_lease.owner_node_id != self.identity.node_id
            || state.write_lease.owner_session_id != self.identity.session_id
        {
            return Err(stale("DFS owner lease epoch is stale"));
        }
        Ok(())
    }

    fn owner_visible_attributes(&self, file: FileHandle) -> Result<FileAttributes> {
        let snapshot = self.handle_snapshot(file)?;
        self.visible_attributes(&snapshot.opened_inode)
    }

    fn owner_read(
        &self,
        file: FileHandle,
        inode_id: &InodeId,
        offset: u64,
        length: usize,
    ) -> Result<(Vec<u8>, u64)> {
        let data =
            <Self as Backend>::read_owned(self, &owner_request_context(), file, offset, length)?;
        Ok((data, self.owner_visible_write_seq(inode_id)?))
    }

    fn owner_write(
        &self,
        file: FileHandle,
        offset: u64,
        data: &[u8],
        options: WriteOptions,
    ) -> Result<(usize, u64)> {
        let written = <Self as Backend>::write_with_options(
            self,
            &owner_request_context(),
            file,
            offset,
            data,
            options,
        )?;
        let snapshot = self.handle_snapshot(file)?;
        let accepted = snapshot
            .write_session
            .as_ref()
            .map_or(0, |session| session.local.last_accepted_seq);
        Ok((written, accepted))
    }

    fn owner_resize(&self, file: FileHandle, length: u64, kill_suidgid: bool) -> Result<u64> {
        let snapshot = self.handle_snapshot(file)?;
        let writer = snapshot
            .write_session
            .as_ref()
            .map(|session| &session.local.id);
        let accepted = self.resize_dirty_inode(&snapshot.inode_id, length, kill_suidgid, writer)?;
        self.update_handle_write_progress(file, accepted)?;
        Ok(accepted)
    }

    fn owner_sync(
        &self,
        file: FileHandle,
        mode: SyncMode,
    ) -> Result<(SyncReceipt, Option<ProxiedCommit>)> {
        <Self as Backend>::fsync(self, &owner_request_context(), file, mode)?;
        self.sync_receipt_with_proxied_commit(file)
    }

    fn release_owner_handle(&self, file: FileHandle) -> Result<Option<Error>> {
        self.release_file_handle(file)
    }
}

impl DistributedFs {
    pub fn reap_expired_peer_owner_handles(&self) -> Result<usize> {
        self.owner_gateway.reap_expired_peer_owner_handles(self)
    }

    /// [`OwnerGateway::admit_owner_request`] with the Meta check awaited.
    async fn admit_owner_request_async(
        &self,
        peer: &str,
        handle: afs_protocol::node_control::DfsOwnerHandle,
    ) -> Result<OwnerAdmittedHandle> {
        let admitted = self
            .owner_gateway
            .check_owner_handle_locally(self, peer, handle)?;
        let current = self
            .owner_gateway
            .meta
            .current_node_session_async(peer.to_owned())
            .await?;
        admitted.check_caller_session(current)?;
        Ok(admitted)
    }

    /// [`OwnerGateway::finish_owner_open_identity_retirement`] with the Meta check
    /// awaited.
    async fn finish_owner_open_retirement_async(
        &self,
        peer: &str,
        handle: &afs_protocol::node_control::DfsOwnerHandle,
    ) -> Result<()> {
        let gateway = &self.owner_gateway;
        let route = gateway.owner_open_release_route(peer, handle)?;
        let current = gateway
            .meta
            .current_node_session_async(peer.to_owned())
            .await?;
        OwnerGateway::check_owner_release_caller_session(current, handle)?;
        gateway.complete_owner_open_retirement(&route, handle)
    }

    /// `OwnerLocalFiles::owner_read` for an async worker: a native read awaits the
    /// read core directly; a read that may need an external backend stays on the
    /// blocking pool with the sync path.
    async fn owner_read_async(
        self: &Arc<Self>,
        file: FileHandle,
        inode_id: InodeId,
        offset: u64,
        length: usize,
    ) -> Result<(Vec<u8>, u64)> {
        if self.read_engine.has_external_backend() {
            let fs = self.clone();
            return offload_handler(move || fs.owner_read(file, &inode_id, offset, length)).await;
        }
        let snapshot = self.handle_snapshot(file)?;
        if snapshot.flags & libc::O_ACCMODE == libc::O_WRONLY {
            return Err(bad_file_descriptor("DFS handle was not opened for reading"));
        }
        let deadline = Instant::now() + self.read_engine.read_pipeline_timeout();
        let data = self
            .read_replicated_bytes_async(file, &snapshot, offset, length, deadline, Arc::new(()))
            .await?
            .to_vec();
        let visible_write_seq =
            crate::node::bridge::local_io(|| self.owner_visible_write_seq(&inode_id))?;
        Ok((data, visible_write_seq))
    }

    fn owner_visible_write_seq(&self, inode_id: &InodeId) -> Result<u64> {
        Ok(self
            .write_state(inode_id)?
            .and_then(|state| state.lock().ok().map(|state| state.visible_write_seq))
            .unwrap_or(0))
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

/// The identity-only wire handle a caller releases to retire an open whose
/// reply it never received; it has no opaque handle.
pub(super) fn cleanup_handle_for_open_request(
    caller_node_id: &str,
    request: &afs_protocol::node_control::DfsOwnerOpenRequest,
) -> Result<afs_protocol::node_control::DfsOwnerHandle> {
    let open_seq = validate_owner_open_seq(request)?;
    Ok(afs_protocol::node_control::DfsOwnerHandle {
        namespace_id: request.namespace_id.clone(),
        inode_id: request.inode_id.clone(),
        owner_node_id: request.owner_node_id.clone(),
        owner_session_id: request.owner_session_id.clone(),
        lease_epoch: request.lease_epoch,
        caller_node_id: caller_node_id.to_owned(),
        caller_session_id: request.caller_session_id.clone(),
        opaque_handle: Vec::new(),
        open_seq,
    })
}

pub(super) fn file_handle_from_owner(
    handle: &afs_protocol::node_control::DfsOwnerHandle,
) -> Result<FileHandle> {
    let bytes: [u8; 8] = handle
        .opaque_handle
        .as_slice()
        .try_into()
        .map_err(|_| stale("DFS owner handle is malformed"))?;
    Ok(FileHandle(u64::from_be_bytes(bytes)))
}

fn control_owner_handle(
    handle: &afs_protocol::node_data::DfsOwnerHandle,
) -> afs_protocol::node_control::DfsOwnerHandle {
    afs_protocol::node_control::DfsOwnerHandle {
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

fn wire_attr(attr: FileAttributes) -> afs_protocol::node_control::DfsOwnerFileAttr {
    let (kind, special_node) = match attr.kind {
        FileKind::Regular => (1, None),
        FileKind::Directory => (2, None),
        FileKind::Symlink => (3, None),
        FileKind::Special(kind) => {
            let special = dfs_owner_special_node(kind);
            (special.kind as u32, Some(special))
        }
    };
    afs_protocol::node_control::DfsOwnerFileAttr {
        size: attr.size,
        mode: attr.mode,
        uid: attr.uid,
        gid: attr.gid,
        nlink: attr.nlink,
        atime_unix_ms: system_time_ms(attr.atime),
        mtime_unix_ms: system_time_ms(attr.mtime),
        ctime_unix_ms: system_time_ms(attr.ctime),
        kind,
        special_node,
    }
}
