//! POSIX identity handoff. This module never carries data payloads.
use super::*;
use afs_protocol::local_api::DfsFdSessionRequest;
use afs_transport::usrbio_fd::{self as wire, Control};
use std::{
    os::fd::{AsFd, AsRawFd, BorrowedFd},
    sync::{LazyLock, Mutex, atomic::AtomicBool, mpsc},
};

const MAX_REGISTERED_FDS: usize = 64;
static ADMISSION: LazyLock<Arc<Semaphore>> =
    LazyLock::new(|| Arc::new(Semaphore::new(MAX_REGISTERED_FDS)));

pub(super) struct ClientIdentity {
    pub key: [u8; 16],
    next: AtomicU64,
}
impl ClientIdentity {
    pub fn new() -> Result<Self, LocalClientError> {
        Ok(Self {
            key: wire::random_key().map_err(io_error)?,
            next: AtomicU64::new(1),
        })
    }
    fn operation(&self) -> Result<[u8; 16], LocalClientError> {
        let sequence = self
            .next
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
            .map_err(|_| protocol_violation("fd operation sequence exhausted"))?;
        let mut key = wire::random_key().map_err(io_error)?;
        key[..8].copy_from_slice(&sequence.to_le_bytes());
        Ok(key)
    }
}

struct LeaseState {
    fd: OwnedFd,
    control: Control,
}
struct Lease {
    state: Mutex<Option<LeaseState>>,
    done: AtomicBool,
    cleanup: mpsc::SyncSender<Cleanup>,
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
}
struct Cleanup {
    state: LeaseState,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

// Admission covers live leases AND pending cleanup, so cancellation cannot grow
// the queue or spawn a thread per descriptor. Failed cleanup retains its permit
// and fd until handoff is confirmed or the FUSE connection is gone.
static CLEANUP: LazyLock<Result<mpsc::SyncSender<Cleanup>, std::io::Error>> = LazyLock::new(|| {
    let (sender, receiver) = mpsc::sync_channel::<Cleanup>(MAX_REGISTERED_FDS);
    thread::Builder::new()
        .name("afs-fd-cleanup".into())
        .spawn(move || {
            while let Ok(mut job) = receiver.recv() {
                loop {
                    match finish(&mut job.state, Duration::from_secs(5)) {
                        Ok(()) => break,
                        Err(LocalClientError::FdControl(error))
                            if matches!(
                                error.raw_os_error(),
                                Some(libc::ENODEV | libc::ENOTCONN | libc::EBADF)
                            ) =>
                        {
                            break;
                        }
                        Err(_) => thread::sleep(Duration::from_millis(100)),
                    }
                }
            }
        })?;
    Ok(sender)
});

impl Lease {
    fn finish(&self) -> Result<(), LocalClientError> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if self.done.load(Ordering::Acquire) {
            return Ok(());
        }
        if let Some(state) = state.as_mut() {
            finish(state, Duration::from_secs(5))?;
        }
        self.done.store(true, Ordering::Release);
        state.take();
        Ok(())
    }
}
impl Drop for Lease {
    fn drop(&mut self) {
        if self.done.load(Ordering::Acquire) {
            return;
        }
        let state = self
            .state
            .get_mut()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        if let (Some(state), Some(permit)) = (state, self.permit.take()) {
            // At most MAX_REGISTERED_FDS permits exist, including this job. Thus
            // this send has capacity; it never waits for a FUSE syscall in Drop.
            let _ = self.cleanup.try_send(Cleanup {
                state,
                _permit: permit,
            });
        }
    }
}

/// An imported POSIX open-file identity. Keeps a private CLOEXEC duplicate alive.
///
/// Use `registration()` with the ring created by the same client. Before handing
/// a writable file back to POSIX, drain all IO, call `File::sync_all`, then
/// `unregister_fd`. Do not overlap POSIX data IO/mmap or change file status flags
/// during the SDK phase. Drop
/// schedules bounded background cleanup; only explicit unregister confirms it.
pub struct DfsRegisteredFd {
    lease: Option<Arc<Lease>>,
    registration: DfsRegistration,
}
impl DfsRegisteredFd {
    pub fn registration(&self) -> &DfsRegistration {
        &self.registration
    }

    /// Full POSIX barrier on the retained original open-file description. Drain
    /// the ring first. Works even when the application's original fd was closed.
    pub async fn sync_all(&self) -> Result<(), LocalClientError> {
        let lease = self.lease.as_ref().ok_or(LocalClientError::Closed)?.clone();
        tokio::task::spawn_blocking(move || {
            let state = lease.state.lock().unwrap_or_else(|e| e.into_inner());
            let state = state.as_ref().ok_or(LocalClientError::Closed)?;
            let file = std::fs::File::from(state.fd.try_clone().map_err(io_error)?);
            file.sync_all().map_err(io_error)
        })
        .await
        .map_err(|_| LocalClientError::Worker)?
    }
}
impl fmt::Debug for DfsRegisteredFd {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DfsRegisteredFd")
            .field("reg_id", &self.registration.reg_id)
            .field("active", &self.lease.is_some())
            .finish()
    }
}

fn io_error(error: std::io::Error) -> LocalClientError {
    LocalClientError::FdControl(error)
}
fn finish(state: &mut LeaseState, timeout: Duration) -> Result<(), LocalClientError> {
    let until = Instant::now() + timeout;
    loop {
        wire::control(state.fd.as_fd(), wire::UNREGISTER, &mut state.control).map_err(io_error)?;
        if state.control.flags & wire::PENDING == 0 {
            return Ok(());
        }
        if Instant::now() >= until {
            return Err(LocalClientError::Timeout);
        }
        thread::sleep(Duration::from_millis(1));
    }
}

impl DfsLocalClient {
    pub(super) async fn fd_session(&self) -> Result<[u8; 16], LocalClientError> {
        require_non_empty("namespace_id", &self.namespace_id)?;
        let mut client = self.inner.clone();
        let reply = tokio::time::timeout(
            self.request_timeout,
            client.fd_session(request_with_current_context(DfsFdSessionRequest {
                key: self.fd_identity.key.to_vec(),
                namespace_id: self.namespace_id.clone(),
            })),
        )
        .await
        .map_err(|_| LocalClientError::Timeout)??
        .into_inner();
        reply
            .node_id
            .try_into()
            .map_err(|_| protocol_violation("invalid fd session node identity"))
    }

    /// Import the original FUSE open identity, access mode and backend handle.
    /// No path lookup or second backend open occurs. Wrong mounts, O_APPEND,
    /// O_PATH, conflicting imports and unsupported files return explicit errors.
    /// Cancellation retains cleanup ownership until the worker has finished.
    pub async fn register_fd(
        &self,
        fd: BorrowedFd<'_>,
    ) -> Result<DfsRegisteredFd, LocalClientError> {
        let permit = tokio::time::timeout(self.request_timeout, ADMISSION.clone().acquire_owned())
            .await
            .map_err(|_| LocalClientError::Timeout)?
            .map_err(|_| LocalClientError::Closed)?;
        let owned = fd.try_clone_to_owned().map_err(io_error)?;
        // SAFETY: owned retains this descriptor; F_GETFL reads file status bits
        // without pointer arguments or filesystem IO. Access authority is still
        // checked by the Node against the original FUSE open, not these bits.
        let flags = unsafe { libc::fcntl(owned.as_raw_fd(), libc::F_GETFL) };
        if flags < 0 {
            return Err(io_error(std::io::Error::last_os_error()));
        }
        if flags & (libc::O_APPEND | libc::O_PATH) != 0 {
            return Err(io_error(std::io::Error::from_raw_os_error(
                libc::EOPNOTSUPP,
            )));
        }
        let cleanup = CLEANUP
            .as_ref()
            .map_err(|error| io_error(std::io::Error::new(error.kind(), error.to_string())))?
            .clone();
        let node = self.fd_session().await?;
        let mut control = Control {
            node,
            session: self.fd_identity.key,
            operation: self.fd_identity.operation()?,
            ..Control::default()
        };
        let (sender, receiver) = tokio::sync::oneshot::channel();
        // Detached result delivery owns the lease: if this future is cancelled,
        // sending the result drops the guard and enqueues unregister.
        tokio::task::spawn_blocking(move || {
            let result = (|| {
                let mut probe = Control::default();
                wire::control(owned.as_fd(), wire::PROBE, &mut probe).map_err(io_error)?;
                if probe.node != node {
                    return Err(io_error(std::io::Error::from_raw_os_error(libc::EXDEV)));
                }
                if probe.flags & wire::ACCESS_WRITE != 0 {
                    let file = std::fs::File::from(owned.try_clone().map_err(io_error)?);
                    file.sync_all().map_err(io_error)?;
                }
                // Install cleanup BEFORE a possibly committed ioctl result.
                let lease = Arc::new(Lease {
                    state: Mutex::new(Some(LeaseState { fd: owned, control })),
                    done: AtomicBool::new(false),
                    cleanup,
                    permit: Some(permit),
                });
                {
                    let mut state = lease.state.lock().unwrap_or_else(|e| e.into_inner());
                    if let Some(state) = state.as_mut() {
                        wire::control(state.fd.as_fd(), wire::REGISTER, &mut state.control)
                            .map_err(io_error)?;
                        control = state.control;
                    }
                }
                if control.registration == 0 {
                    return Err(protocol_violation("empty fd registration"));
                }
                Ok(DfsRegisteredFd {
                    lease: Some(lease),
                    registration: DfsRegistration {
                        reg_id: control.registration,
                        file_version_id: String::new(),
                    },
                })
            })();
            let _ = sender.send(result);
        });
        tokio::time::timeout(self.request_timeout, receiver)
            .await
            .map_err(|_| LocalClientError::Timeout)?
            .map_err(|_| LocalClientError::Worker)?
    }

    /// Stop new IO, wait for backend users, invalidate cached writable data/attrs,
    /// then release the private duplicate. On error the guard remains retryable.
    pub async fn unregister_fd(
        &self,
        registered: &mut DfsRegisteredFd,
    ) -> Result<(), LocalClientError> {
        if let Some(lease) = registered.lease.as_ref() {
            let lease = lease.clone();
            tokio::task::spawn_blocking(move || lease.finish())
                .await
                .map_err(|_| LocalClientError::Worker)??;
            registered.lease.take();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Seek, Write};
    #[tokio::test]
    async fn append_enabled_after_open_is_rejected_before_any_rpc() {
        use std::os::fd::AsRawFd;
        let file = tempfile::tempfile().unwrap();
        // SAFETY: the File owns a live fd and F_GETFL takes no pointer argument.
        let flags = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GETFL) };
        assert!(flags >= 0);
        assert_eq!(flags & libc::O_APPEND, 0);
        // SAFETY: changing status bits on this exclusively held test file does
        // not change ownership or the immutable access mode.
        let changed =
            unsafe { libc::fcntl(file.as_raw_fd(), libc::F_SETFL, flags | libc::O_APPEND) };
        assert_eq!(changed, 0);
        let client = DfsLocalClient {
            inner: DfsLocalDataClient::new(traced_channel(
                Endpoint::from_static("http://127.0.0.1:1").connect_lazy(),
            )),
            request_timeout: Duration::from_millis(100),
            namespace_id: DEFAULT_NAMESPACE.into(),
            fd_identity: Arc::new(ClientIdentity::new().unwrap()),
        };
        let error = client.register_fd(file.as_fd()).await.unwrap_err();
        assert_eq!(
            error.code(),
            afs_error::IO_NOT_SUPPORTED,
            "must reject the current status flag without contacting a Node"
        );
    }

    #[tokio::test]
    async fn lost_registration_reply_owns_duplicate_and_cleanup_permit() {
        let slots = Arc::new(Semaphore::new(1));
        let permit = slots.clone().acquire_owned().await.unwrap();
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(b"original identity").unwrap();
        file.rewind().unwrap();
        let (cleanup, queue) = mpsc::sync_channel(1);
        let lease = Arc::new(Lease {
            state: Mutex::new(Some(LeaseState {
                fd: file.as_fd().try_clone_to_owned().unwrap(),
                control: Control {
                    registration: 71,
                    ..Control::default()
                },
            })),
            done: AtomicBool::new(false),
            cleanup,
            permit: Some(permit),
        });
        let (reply, answer) = tokio::sync::oneshot::channel();
        drop(answer);
        // Same delivery as register_fd: cancellation before OR after commitment
        // drops the undelivered guard; neither the fd nor permit is forgotten.
        let _ = reply.send(DfsRegisteredFd {
            lease: Some(lease),
            registration: DfsRegistration {
                reg_id: 71,
                file_version_id: String::new(),
            },
        });
        drop(file);
        let job = queue.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(slots.available_permits(), 0);
        assert_eq!(job.state.control.registration, 71);
        let mut file = std::fs::File::from(job.state.fd.try_clone().unwrap());
        let mut value = String::new();
        file.read_to_string(&mut value).unwrap();
        assert_eq!(value, "original identity");
        drop(job);
        assert_eq!(slots.available_permits(), 1);
    }
    #[tokio::test]
    async fn failed_unregister_keeps_retryable_guard_and_original_fd() {
        let slots = Arc::new(Semaphore::new(1));
        let permit = slots.clone().acquire_owned().await.unwrap();
        let (cleanup, queue) = mpsc::sync_channel(1);
        let file = tempfile::tempfile().unwrap();
        let lease = Lease {
            state: Mutex::new(Some(LeaseState {
                fd: file.into(),
                control: Control::default(),
            })),
            done: AtomicBool::new(false),
            cleanup,
            permit: Some(permit),
        };
        assert!(lease.finish().is_err()); // local file deliberately has no AFS ioctl
        assert!(!lease.done.load(Ordering::Acquire));
        assert!(lease.state.lock().unwrap().is_some());
        assert_eq!(slots.available_permits(), 0);
        drop(lease);
        let job = queue.recv_timeout(Duration::from_secs(1)).unwrap();
        drop(job);
        assert_eq!(slots.available_permits(), 1);
    }
}
