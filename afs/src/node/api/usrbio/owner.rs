//! Which client process owns each usrbio resource, and reclaiming them when it
//! exits (R18).
//!
//! The owner is the process that called the local API, taken from the UDS
//! peer credentials (`SO_PEERCRED`). The SDK may reconnect or open several
//! connections, so a connection is not an owner; a process is, the same rule
//! 3FS uses. Each owner gets a pidfd, which becomes readable when the process
//! exits however it ends (`kill -9` included) and cannot be confused with a
//! later process that reuses the pid. One watcher thread polls every pidfd.
//!
//! Reclaim runs on that thread and follows the order the explicit calls
//! require: stop and join each ring (its worker fails what was still queued),
//! then drop the registrations, then flush and release the handles (as the
//! kernel does with FUSE fds when a process dies), then unmap the iovs.
//! Resources created after `fork` belong to the process that created them.

use std::{
    collections::{HashMap, HashSet},
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
    sync::{Arc, Mutex, Weak},
    thread,
};

use afs_error::Error;
use afs_transport::eventfd::EventFd;

use super::HubState;
use crate::node::vfs::Backend;

/// One resource a client process created.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Owned {
    Handle(Vec<u8>),
    Registration(u32),
    Iov(u64),
    Ring(u64),
}

/// Resources of one live owner.
#[derive(Default, Debug)]
pub(super) struct OwnedSet {
    handles: HashSet<Vec<u8>>,
    registrations: HashSet<u32>,
    iovs: HashSet<u64>,
    rings: HashSet<u64>,
}

impl OwnedSet {
    fn insert(&mut self, resource: Owned) {
        match resource {
            Owned::Handle(value) => {
                self.handles.insert(value);
            }
            Owned::Registration(id) => {
                self.registrations.insert(id);
            }
            Owned::Iov(id) => {
                self.iovs.insert(id);
            }
            Owned::Ring(id) => {
                self.rings.insert(id);
            }
        }
    }

    fn remove(&mut self, resource: &Owned) {
        match resource {
            Owned::Handle(value) => {
                self.handles.remove(value);
            }
            Owned::Registration(id) => {
                self.registrations.remove(id);
            }
            Owned::Iov(id) => {
                self.iovs.remove(id);
            }
            Owned::Ring(id) => {
                self.rings.remove(id);
            }
        }
    }
}

/// Owner of every resource, by process id.
#[derive(Default)]
pub(super) struct Owners {
    by_pid: HashMap<i32, OwnedSet>,
}

impl Owners {
    /// Records `resource` for `pid`; true when `pid` is a new owner to watch.
    pub(super) fn adopt(&mut self, pid: i32, resource: Owned) -> bool {
        let new = !self.by_pid.contains_key(&pid);
        self.by_pid.entry(pid).or_default().insert(resource);
        new
    }

    /// An explicit close or destroy: the resource no longer needs reclaiming.
    pub(super) fn forget(&mut self, resource: &Owned) {
        for set in self.by_pid.values_mut() {
            set.remove(resource);
        }
    }

    fn take(&mut self, pid: i32) -> OwnedSet {
        self.by_pid.remove(&pid).unwrap_or_default()
    }
}

/// What one reclaim released.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct Reclaimed {
    pub rings: usize,
    pub registrations: usize,
    pub handles: usize,
    pub iovs: usize,
}

/// Releases everything `pid` still owns, in the safe order.
pub(super) fn reclaim(state: &Mutex<HubState>, backend: &dyn Backend, pid: i32) -> Reclaimed {
    let lock = || {
        state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    };
    let set = lock().owners.take(pid);
    let mut done = Reclaimed::default();
    // 1. Rings first: a joined worker has finished every submission it took,
    //    so no registration or iov below is still in use by it.
    let rings = {
        let mut state = lock();
        set.rings
            .iter()
            .filter_map(|id| state.rings.remove(id).map(|entry| (*id, entry)))
            .collect::<Vec<_>>()
    };
    for (ring_id, entry) in rings {
        super::stop_ring(ring_id, entry);
        done.rings += 1;
    }
    // 2. Registrations. A POSIX-imported one is revoked by the FUSE release
    //    of its fd, which the kernel performs for a dead process.
    let handles = {
        let mut state = lock();
        for id in &set.registrations {
            if state
                .registrations
                .get(id)
                .is_some_and(|registration| registration.imported.is_none())
            {
                state.registrations.remove(id);
                done.registrations += 1;
            }
        }
        // 3. Handles still registered by another owner stay open.
        let unregistered = set
            .handles
            .iter()
            .filter(|value| {
                !state
                    .registrations
                    .values()
                    .any(|registered| &registered.opened == *value)
            })
            .cloned()
            .collect::<Vec<_>>();
        unregistered
            .iter()
            .filter_map(|value| state.opened.remove(value))
            .collect::<Vec<_>>()
    };
    for opened in handles {
        if let Err(error) = backend.flush(&opened.ctx, opened.handle) {
            afs_logging::warn!("usrbio.reclaim_flush_failed"; "pid" => pid,
                "error" => error.to_string());
        }
        if let Err(error) = backend.release(&opened.ctx, opened.handle) {
            afs_logging::warn!("usrbio.reclaim_release_failed"; "pid" => pid,
                "error" => error.to_string());
        }
        done.handles += 1;
    }
    // 4. Iovs last. A ring of another owner keeps its own reference.
    {
        let mut state = lock();
        for id in &set.iovs {
            if state.iovs.remove(id).is_some() {
                done.iovs += 1;
            }
        }
    }
    afs_logging::info!("usrbio.client_reclaimed"; "pid" => pid, "rings" => done.rings,
        "registrations" => done.registrations, "handles" => done.handles, "iovs" => done.iovs);
    done
}

/// Watches owner processes and reclaims each one's resources when it exits.
pub(super) struct ProcessWatch {
    shared: Arc<WatchShared>,
}

struct WatchShared {
    pidfds: Mutex<HashMap<i32, OwnedFd>>,
    wake: EventFd,
}

impl ProcessWatch {
    pub(super) fn start(
        state: Weak<Mutex<HubState>>,
        backend: Arc<dyn Backend>,
        on_reclaim: Option<Box<dyn Fn(Reclaimed) + Send + Sync>>,
    ) -> Result<Self, Error> {
        let shared = Arc::new(WatchShared {
            pidfds: Mutex::new(HashMap::new()),
            wake: EventFd::create().map_err(|error| {
                Error::coded(
                    afs_error::NODE_VFS_UNAVAILABLE,
                    format!("usrbio owner watch eventfd: {error}"),
                )
            })?,
        });
        let watched = shared.clone();
        thread::Builder::new()
            .name("afs-usrbio-owners".into())
            .spawn(move || watch_loop(&watched, &state, backend.as_ref(), on_reclaim))
            .map_err(|error| {
                Error::coded(
                    afs_error::NODE_VFS_UNAVAILABLE,
                    format!("usrbio owner watch thread: {error}"),
                )
            })?;
        Ok(Self { shared })
    }

    /// Starts watching `pid`. A process that already exited is reported as
    /// `ESRCH`; the caller reclaims it at once.
    pub(super) fn watch(&self, pid: i32) -> std::io::Result<()> {
        let fd = pidfd_open(pid)?;
        self.shared
            .pidfds
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(pid, fd);
        self.shared.wake.ring();
        Ok(())
    }
}

fn pidfd_open(pid: i32) -> std::io::Result<OwnedFd> {
    // SAFETY: pidfd_open takes a pid and flags by value and returns a new fd or
    // -1 with errno set; it reads no caller memory.
    let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
    if fd < 0 {
        return Err(std::io::Error::last_os_error());
    }
    let fd = i32::try_from(fd).map_err(|_| std::io::Error::other("pidfd out of range"))?;
    // SAFETY: the kernel just returned `fd` as a new descriptor owned by no one else.
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

fn watch_loop(
    shared: &WatchShared,
    state: &Weak<Mutex<HubState>>,
    backend: &dyn Backend,
    on_reclaim: Option<Box<dyn Fn(Reclaimed) + Send + Sync>>,
) {
    loop {
        let (pids, mut polls) = {
            let pidfds = shared
                .pidfds
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let mut pids = Vec::with_capacity(pidfds.len());
            let mut polls = vec![libc::pollfd {
                fd: shared.wake.raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            }];
            for (pid, fd) in pidfds.iter() {
                pids.push(*pid);
                polls.push(libc::pollfd {
                    fd: fd.as_raw_fd(),
                    events: libc::POLLIN,
                    revents: 0,
                });
            }
            (pids, polls)
        };
        let Ok(count) = libc::nfds_t::try_from(polls.len()) else {
            return;
        };
        // SAFETY: `polls` is a live, exclusively borrowed array of `count`
        // pollfd entries; every fd in it stays open while `shared` lives.
        let ready = unsafe { libc::poll(polls.as_mut_ptr(), count, -1) };
        if ready < 0 {
            if std::io::Error::last_os_error().kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            afs_logging::warn!("usrbio.owner_watch_failed";
                "error" => std::io::Error::last_os_error().to_string());
            return;
        }
        if polls[0].revents != 0 {
            shared.wake.drain();
        }
        let Some(state) = state.upgrade() else {
            // The hub is gone; nothing is left to reclaim.
            return;
        };
        for (pid, poll) in pids.iter().zip(&polls[1..]) {
            if poll.revents == 0 {
                continue;
            }
            shared
                .pidfds
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .remove(pid);
            let done = reclaim(&state, backend, *pid);
            if let Some(on_reclaim) = &on_reclaim {
                on_reclaim(done);
            }
        }
    }
}
