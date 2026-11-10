//! Existing POSIX-open identity and lifetime bridge. Payloads stay on USRBIO rings.
use super::*;
use afs_transport::usrbio_fd::{self as wire, Control};
use std::sync::{Condvar, Weak};

const MAX_SESSIONS: usize = 4096;
const MAX_IMPORTS: usize = 4096;
const SESSION_IDLE: Duration = Duration::from_secs(60);

type Key = [u8; 16];
type SessionTable = HashMap<Key, (Arc<FdSession>, Instant)>;
type FinishWork = Box<dyn FnOnce() + Send>;

pub(crate) struct FdSession {
    pub key: Key,
    pub uid: u32,
}
struct Phase {
    state: Mutex<(usize, bool)>,
}
struct PhaseClaim {
    phase: Arc<Phase>,
    write: bool,
}
impl Drop for PhaseClaim {
    fn drop(&mut self) {
        let mut state = self.phase.state.lock().unwrap_or_else(|e| e.into_inner());
        if self.write {
            state.1 = false;
        } else {
            state.0 = state.0.saturating_sub(1);
        }
    }
}
struct OpenState {
    last: HashMap<Key, u64>,
    active: Weak<ImportedFile>,
}
struct OpenFile {
    fh: u64,
    ino: u64,
    handle: types::FileHandle,
    ctx: types::RequestContext,
    flags: i32,
    cached: bool,
    alive: AtomicBool,
    state: Mutex<OpenState>,
    phase: Arc<Phase>,
}
struct ImportState {
    closing: bool,
    scheduled: bool,
    done: bool,
    active: usize,
    error: Option<i32>,
}
pub(super) struct ImportedFile {
    source: Arc<OpenFile>,
    pub session: Arc<FdSession>,
    operation: Key,
    pub id: u32,
    state: Mutex<ImportState>,
    idle: Condvar,
    claim: Mutex<Option<PhaseClaim>>,
}
// If the executor rejects/drops queued work, make the next UNREGISTER retryable.
struct FinishGuard(Arc<ImportedFile>);
impl Drop for FinishGuard {
    fn drop(&mut self) {
        let mut state = self.0.state.lock().unwrap_or_else(|e| e.into_inner());
        if !state.done && state.error.is_none() {
            state.error = Some(libc::EIO);
        }
    }
}
pub(super) struct ImportedUse<'a> {
    import: &'a ImportedFile,
}
impl Drop for ImportedUse<'_> {
    fn drop(&mut self) {
        let mut state = self.import.state.lock().unwrap_or_else(|e| e.into_inner());
        state.active = state.active.saturating_sub(1);
        if state.active == 0 {
            self.import.idle.notify_all();
        }
    }
}
impl ImportedFile {
    pub(super) fn begin(
        &self,
        op: u32,
        session: Option<&Arc<FdSession>>,
    ) -> Result<ImportedUse<'_>, Error> {
        if !session.is_some_and(|s| Arc::ptr_eq(s, &self.session)) {
            return Err(Error::coded(
                afs_error::IO_PERMISSION_DENIED,
                "fd registration belongs to another SDK session",
            ));
        }
        let access = self.source.flags & libc::O_ACCMODE;
        if (op == OP_WRITE && access == libc::O_RDONLY)
            || (op == OP_READ && access == libc::O_WRONLY)
        {
            return Err(Error::coded(
                afs_error::IO_PERMISSION_DENIED,
                "operation exceeds the POSIX fd access mode",
            ));
        }
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.closing || !self.source.alive.load(Ordering::Acquire) {
            return Err(invalid("POSIX fd registration is closing"));
        }
        state.active += 1;
        Ok(ImportedUse { import: self })
    }
    fn close_and_wait(&self) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.closing = true;
        while state.active != 0 {
            state = self.idle.wait(state).unwrap_or_else(|e| e.into_inner());
        }
    }
}

/// Shared by one DFS FUSE mount and its local SDK service. No public raw handle exports.
pub struct FdBridge {
    node: Option<Key>,
    backend: Arc<dyn Backend>,
    hub: Weak<Mutex<HubState>>,
    executor: Arc<BackendThread>,
    owner: u32,
    mounted: AtomicBool,
    notifier: Mutex<Option<Arc<fuser::Notifier>>>,
    opens: Mutex<HashMap<u64, Arc<OpenFile>>>,
    phases: Mutex<HashMap<types::BackendInode, Weak<Phase>>>,
    sessions: Mutex<SessionTable>,
    #[cfg(test)]
    fuse_data: [std::sync::atomic::AtomicU64; 2],
}
impl FdBridge {
    #[cfg(test)]
    pub(crate) fn note_fuse_data(&self, write: bool) {
        self.fuse_data[usize::from(write)].fetch_add(1, Ordering::Relaxed);
    }
    #[cfg(all(test, feature = "dfs"))]
    pub(crate) fn fuse_data_counts(&self) -> [u64; 2] {
        [
            self.fuse_data[0].load(Ordering::Relaxed),
            self.fuse_data[1].load(Ordering::Relaxed),
        ]
    }
    pub(super) fn new(
        backend: Arc<dyn Backend>,
        hub: Weak<Mutex<HubState>>,
        executor: Arc<BackendThread>,
        owner: u32,
    ) -> Self {
        Self {
            node: wire::random_key().ok(),
            backend,
            hub,
            executor,
            owner,
            mounted: AtomicBool::new(false),
            notifier: Mutex::new(None),
            opens: Mutex::new(HashMap::new()),
            phases: Mutex::new(HashMap::new()),
            sessions: Mutex::new(HashMap::new()),
            #[cfg(test)]
            fuse_data: [
                std::sync::atomic::AtomicU64::new(0),
                std::sync::atomic::AtomicU64::new(0),
            ],
        }
    }
    pub(crate) fn begin_session(&self, key: &[u8], uid: u32) -> Result<Key, Error> {
        let key: Key = key
            .try_into()
            .map_err(|_| invalid("invalid SDK session key"))?;
        if key == [0; 16] || uid != self.owner {
            return Err(Error::coded(
                afs_error::IO_PERMISSION_DENIED,
                "local fd session requires the Node owner",
            ));
        }
        let mut sessions = self.sessions.lock().unwrap_or_else(|e| e.into_inner());
        sessions.retain(|_, (session, at)| {
            Arc::strong_count(session) > 1 || at.elapsed() < SESSION_IDLE
        });
        if let Some((session, at)) = sessions.get_mut(&key) {
            if session.uid != uid {
                return Err(invalid("SDK session owner changed"));
            }
            *at = Instant::now();
        } else {
            if sessions.len() >= MAX_SESSIONS {
                return Err(Error::coded(
                    afs_error::NODE_VFS_UNAVAILABLE,
                    "local fd session budget exhausted",
                ));
            }
            sessions.insert(key, (Arc::new(FdSession { key, uid }), Instant::now()));
        }
        self.node.ok_or_else(|| {
            Error::coded(
                afs_error::NODE_VFS_UNAVAILABLE,
                "fd control entropy unavailable",
            )
        })
    }
    pub(crate) fn session(&self, key: &[u8], uid: u32) -> Result<Arc<FdSession>, Error> {
        self.sessions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(key)
            .filter(|(s, _)| s.uid == uid)
            .map(|(s, _)| s.clone())
            .ok_or_else(|| invalid("unknown local SDK session"))
    }
    pub(crate) fn bind(&self, backend: &Arc<dyn Backend>) -> Result<(), Error> {
        if !Arc::ptr_eq(backend, &self.backend) || self.mounted.swap(true, Ordering::AcqRel) {
            return Err(invalid(
                "fd bridge must bind its own Backend and one FUSE mount",
            ));
        }
        Ok(())
    }
    pub(crate) fn set_notifier(&self, notifier: fuser::Notifier) {
        *self.notifier.lock().unwrap_or_else(|e| e.into_inner()) = Some(Arc::new(notifier));
    }
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn opened(
        &self,
        fh: u64,
        ino: u64,
        inode: types::BackendInode,
        handle: types::FileHandle,
        ctx: types::RequestContext,
        flags: i32,
        cached: bool,
    ) {
        let mut phases = self.phases.lock().unwrap_or_else(|e| e.into_inner());
        phases.retain(|_, phase| phase.strong_count() != 0);
        let phase = phases
            .get(&inode)
            .and_then(Weak::upgrade)
            .unwrap_or_else(|| {
                let phase = Arc::new(Phase {
                    state: Mutex::new((0, false)),
                });
                phases.insert(inode, Arc::downgrade(&phase));
                phase
            });
        drop(phases);
        self.opens.lock().unwrap_or_else(|e| e.into_inner()).insert(
            fh,
            Arc::new(OpenFile {
                fh,
                ino,
                handle,
                ctx,
                flags,
                cached,
                alive: AtomicBool::new(true),
                phase,
                state: Mutex::new(OpenState {
                    last: HashMap::new(),
                    active: Weak::new(),
                }),
            }),
        );
    }
    pub(crate) fn check_control(&self, ino: u64, fh: u64) -> Result<(), i32> {
        if self
            .opens
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&fh)
            .is_some_and(|s| s.ino == ino && s.fh == fh)
        {
            Ok(())
        } else {
            Err(libc::EBADF)
        }
    }
    pub(crate) fn control(
        self: &Arc<Self>,
        fh: u64,
        uid: u32,
        command: u32,
        mut value: Control,
    ) -> Result<(Control, Option<FinishWork>), i32> {
        let source = self
            .opens
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&fh)
            .cloned()
            .ok_or(libc::EBADF)?;
        if !source.alive.load(Ordering::Acquire) {
            return Err(libc::ESTALE);
        }
        let node = self.node.ok_or(libc::EIO)?;
        let access = source.flags & libc::O_ACCMODE;
        if command == wire::PROBE {
            value.node = node;
            value.flags = if access != libc::O_WRONLY {
                wire::ACCESS_READ
            } else {
                0
            } | if access != libc::O_RDONLY {
                wire::ACCESS_WRITE
            } else {
                0
            } | if source.cached { wire::CACHED } else { 0 };
            return Ok((value, None));
        }
        if value.node != node {
            return Err(libc::EXDEV);
        }
        let session = self
            .session(&value.session, uid)
            .map_err(|_| libc::EACCES)?;
        let sequence =
            u64::from_le_bytes(value.operation[..8].try_into().map_err(|_| libc::EINVAL)?);
        if sequence == 0 {
            return Err(libc::EINVAL);
        }
        if command == wire::REGISTER {
            if source.flags & (libc::O_PATH | libc::O_APPEND) != 0 {
                return Err(libc::EOPNOTSUPP);
            }
            if source.cached
                && self
                    .notifier
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .is_none()
            {
                return Err(libc::EAGAIN);
            }
            let mut opened = source.state.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(active) = opened.active.upgrade() {
                if active.operation == value.operation && Arc::ptr_eq(&active.session, &session) {
                    value.registration = active.id;
                    return Ok((value, None));
                }
                return Err(libc::EBUSY);
            }
            if opened
                .last
                .get(&session.key)
                .is_some_and(|last| sequence <= *last)
            {
                return Err(libc::ESTALE);
            }
            if opened.last.len() >= 256 && !opened.last.contains_key(&session.key) {
                return Err(libc::EMFILE);
            }
            let write = access != libc::O_RDONLY;
            let mut phase = source.phase.state.lock().unwrap_or_else(|e| e.into_inner());
            if phase.1 || (write && phase.0 != 0) {
                return Err(libc::EBUSY);
            }
            if write {
                phase.1 = true;
            } else {
                phase.0 += 1;
            }
            drop(phase);
            let claim = PhaseClaim {
                phase: source.phase.clone(),
                write,
            };
            let hub = self.hub.upgrade().ok_or(libc::ENOTCONN)?;
            let mut state = hub.lock().unwrap_or_else(|e| e.into_inner());
            if state.registrations.len() >= MAX_IMPORTS {
                return Err(libc::EMFILE);
            }
            let id = state.allocate_registration().map_err(|_| libc::ENOSPC)?;
            let import = Arc::new(ImportedFile {
                source: source.clone(),
                session,
                operation: value.operation,
                id,
                state: Mutex::new(ImportState {
                    closing: false,
                    scheduled: false,
                    done: false,
                    active: 0,
                    error: None,
                }),
                idle: Condvar::new(),
                claim: Mutex::new(Some(claim)),
            });
            state.registrations.insert(
                id,
                RegisteredFile {
                    handle: source.handle,
                    ctx: source.ctx.clone(),
                    opened: Vec::new(),
                    read_cost: Arc::new(ReadCost::default()),
                    imported: Some(import.clone()),
                },
            );
            opened.last.insert(value.session, sequence);
            opened.active = Arc::downgrade(&import);
            value.registration = id;
            value.flags = 0;
            Ok((value, None))
        } else if command == wire::UNREGISTER {
            let mut opened = source.state.lock().unwrap_or_else(|e| e.into_inner());
            // At capacity a new session cannot REGISTER on this open again, so
            // acknowledging its cancellation needs no additional tombstone.
            if opened.last.len() >= 256 && !opened.last.contains_key(&value.session) {
                value.flags = 0;
                return Ok((value, None));
            }
            let last = opened.last.entry(value.session).or_default();
            *last = (*last).max(sequence);
            let Some(import) = opened.active.upgrade().filter(|active| {
                active.operation == value.operation && Arc::ptr_eq(&active.session, &session)
            }) else {
                value.flags = 0;
                return Ok((value, None));
            };
            if value.registration != 0 && value.registration != import.id {
                return Err(libc::EINVAL);
            }
            drop(opened);
            let mut state = import.state.lock().unwrap_or_else(|e| e.into_inner());
            state.closing = true;
            if let Some(error) = state.error.take() {
                state.scheduled = false;
                return Err(error);
            }
            if state.done {
                value.flags = 0;
                return Ok((value, None));
            }
            value.flags = wire::PENDING;
            if state.scheduled {
                return Ok((value, None));
            }
            state.scheduled = true;
            drop(state);
            let bridge = self.clone();
            let guard = FinishGuard(import.clone());
            let work: FinishWork = Box::new(move || {
                let _guard = guard;
                import.close_and_wait();
                let notifier = bridge
                    .notifier
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .clone();
                let result = if import.source.cached
                    && import.source.flags & libc::O_ACCMODE != libc::O_RDONLY
                {
                    notifier.ok_or(libc::ENOTCONN).and_then(|notifier| {
                        notifier
                            .inval_inode(fuser::INodeNo(import.source.ino), 0, 0)
                            .map_err(|e| e.raw_os_error().unwrap_or(libc::EIO))
                    })
                } else {
                    Ok(())
                };
                if let Err(error) = result {
                    import.state.lock().unwrap_or_else(|e| e.into_inner()).error = Some(error);
                    return;
                }
                if let Some(hub) = bridge.hub.upgrade() {
                    hub.lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .registrations
                        .remove(&import.id);
                }
                import
                    .claim
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .take();
                {
                    let mut opened = import
                        .source
                        .state
                        .lock()
                        .unwrap_or_else(|e| e.into_inner());
                    opened.active = Weak::new();
                }
                import.state.lock().unwrap_or_else(|e| e.into_inner()).done = true;
            });
            Ok((value, Some(work)))
        } else {
            Err(libc::ENOTTY)
        }
    }
    /// Queue only after the ioctl reply has been sent: kernel invalidation cannot
    /// wait inside the same FUSE request whose inode locks it may need.
    pub(crate) fn after_reply(&self, work: FinishWork) {
        if let Err(error) = self.executor.enqueue(work) {
            afs_logging::warn!("usrbio.fd_finish_unavailable";"error"=>error.to_string());
        }
    }
    pub(crate) fn release_fuse(&self, fh: u64) {
        let source = self
            .opens
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&fh);
        if let Some(source) = source {
            self.revoke(&source);
        }
    }
    fn revoke(&self, source: &Arc<OpenFile>) {
        source.alive.store(false, Ordering::Release);
        let import = source
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .active
            .upgrade();
        if let Some(import) = import {
            import.close_and_wait();
            if let Some(hub) = self.hub.upgrade() {
                hub.lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .registrations
                    .remove(&import.id);
            }
            import
                .claim
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .take();
        }
    }
    pub(crate) fn unmount(&self) {
        let opens = std::mem::take(&mut *self.opens.lock().unwrap_or_else(|e| e.into_inner()));
        for (_, source) in opens {
            self.revoke(&source);
            if let Err(error) = self.backend.release(&source.ctx, source.handle) {
                afs_logging::warn!("usrbio.fd_unmount_release_failed";"error"=>error.to_string());
            }
        }
        self.notifier
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        self.mounted.store(false, Ordering::Release);
    }
}
