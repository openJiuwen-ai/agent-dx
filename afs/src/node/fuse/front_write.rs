//! Bounded ordinary-write ownership and file-scoped callback barriers.
//! Body execution remains on one worker. Recovery never consumes body capacity.
use libc::{E2BIG, EAGAIN, EIO};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{
    Arc, Condvar, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::thread::{self, JoinHandle};

type Job = Box<dyn FnMut() -> Result<(), i32> + Send>;
type Admission = Box<dyn FnOnce(Result<(), i32>) + Send>;
type RecoveryStart = Box<dyn FnOnce(CallbackGuard) + Send>;
const MAX_WAITING_ENTRIES: usize = 4096;

struct Waiter {
    inode: u64,
    through: u64,
    ready: Admission,
}
struct Entry {
    sequence: u64,
    inode: u64,
    handle: u64,
    bytes: usize,
    job: Job,
    admission: Option<Admission>,
}
#[derive(Default)]
struct State {
    entries: VecDeque<Entry>,
    bytes: usize,
    admitted_bytes: usize,
    peak_bytes: usize,
    peak_pending_bytes: usize,
    peak_entries: usize,
    next_sequence: u64,
    waiters: Vec<Waiter>,
    active: Option<(u64, u64)>,
    errors: HashMap<u64, i32>,
    closing: bool,
    closed: bool,
}
struct Shared {
    state: Mutex<State>,
    changed: Condvar,
    limit: usize,
    admitted_limit: usize,
}
/// How a callback holds the gate. Shared holders of the same file (or of the
/// namespace) overlap; an exclusive holder excludes every other holder of
/// that file (or every holder of the mount). Waiting exclusive holders stop
/// new shared entrants, so a stream of readers cannot starve a writer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum GateAccess {
    Shared,
    Exclusive,
}

#[derive(Default)]
struct GateState {
    /// Files held exclusively (writes, resizes, syncs, recoveries).
    files: HashSet<u64>,
    /// Shared holders per file (reads and attribute queries).
    readers: HashMap<u64, usize>,
    /// Exclusive entrants waiting per file; they block new shared entrants.
    exclusive_waiters: HashMap<u64, usize>,
    /// A namespace change holds the whole mount.
    namespace: bool,
    /// Path lookups in flight; they exclude only namespace changes.
    namespace_readers: usize,
    namespace_waiters: usize,
    recoveries: VecDeque<(u64, RecoveryStart)>,
}

impl GateState {
    fn file_busy(&self, file: u64) -> bool {
        self.files.contains(&file) || self.readers.get(&file).is_some_and(|count| *count != 0)
    }

    fn any_file_holder(&self) -> bool {
        !self.files.is_empty() || !self.readers.is_empty()
    }
}

fn decrement(map: &mut HashMap<u64, usize>, key: u64) {
    if let Some(count) = map.get_mut(&key) {
        *count -= 1;
        if *count == 0 {
            map.remove(&key);
        }
    }
}

enum Held {
    Files { files: Vec<u64>, access: GateAccess },
    Namespace(GateAccess),
}
#[derive(Default)]
struct Gates {
    state: Mutex<GateState>,
    changed: Condvar,
}
struct ControlLease(Arc<AtomicUsize>);
impl Drop for ControlLease {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}
pub(super) struct CallbackGuard {
    gates: Arc<Gates>,
    held: Held,
    _control: Option<ControlLease>,
}
impl Gates {
    fn ready(state: &mut GateState) -> Vec<(u64, RecoveryStart)> {
        let mut ready = Vec::new();
        let mut index = 0;
        while index < state.recoveries.len() {
            let file = state.recoveries[index].0;
            if !state.file_busy(file) {
                if let Some(item) = state.recoveries.remove(index) {
                    state.files.insert(file);
                    ready.push(item);
                }
            } else {
                index += 1;
            }
        }
        ready
    }
    fn start(self: &Arc<Self>, ready: Vec<(u64, RecoveryStart)>) {
        for (inode, start) in ready {
            start(CallbackGuard {
                gates: self.clone(),
                held: Held::Files {
                    files: vec![inode],
                    access: GateAccess::Exclusive,
                },
                _control: None,
            });
        }
    }
    fn recover(self: &Arc<Self>, inode: u64, start: RecoveryStart) -> Result<(), i32> {
        let mut state = self.state.lock().map_err(|_| EIO)?;
        state.recoveries.push_back((inode, start));
        let ready = Self::ready(&mut state);
        drop(state);
        self.start(ready);
        Ok(())
    }
}
impl Drop for CallbackGuard {
    fn drop(&mut self) {
        let mut state = self
            .gates
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match &self.held {
            Held::Files {
                files,
                access: GateAccess::Exclusive,
            } => {
                for file in files {
                    state.files.remove(file);
                }
            }
            Held::Files {
                files,
                access: GateAccess::Shared,
            } => {
                for file in files {
                    decrement(&mut state.readers, *file);
                }
            }
            Held::Namespace(GateAccess::Exclusive) => state.namespace = false,
            Held::Namespace(GateAccess::Shared) => state.namespace_readers -= 1,
        }
        let ready = Gates::ready(&mut state);
        self.gates.changed.notify_all();
        drop(state);
        self.gates.start(ready);
    }
}
pub(super) struct FrontWriteQueue {
    shared: Arc<Shared>,
    gates: Arc<Gates>,
    controls: Arc<AtomicUsize>,
    worker: Mutex<Option<JoinHandle<()>>>,
}
impl FrontWriteQueue {
    pub(super) fn new(admitted_limit: usize) -> Result<Self, i32> {
        // Waiting capacity is additional: never shrink the normal ACK window.
        // Promotion transfers the existing input without another payload copy.
        let reserve = (admitted_limit / 8).min(32 * 1024 * 1024);
        Self::with_limits(admitted_limit, reserve)
    }

    fn with_limits(admitted_limit: usize, reserve: usize) -> Result<Self, i32> {
        if admitted_limit == 0 {
            return Err(E2BIG);
        }
        let limit = admitted_limit.checked_add(reserve).ok_or(E2BIG)?;
        let shared = Arc::new(Shared {
            state: Mutex::new(State::default()),
            changed: Condvar::new(),
            limit,
            admitted_limit,
        });
        let work = shared.clone();
        let worker = thread::Builder::new()
            .name("afs-front-write".into())
            .spawn(move || Self::run(work))
            .map_err(|_| EAGAIN)?;
        Ok(Self {
            shared,
            gates: Arc::new(Gates::default()),
            controls: Arc::new(AtomicUsize::new(0)),
            worker: Mutex::new(Some(worker)),
        })
    }
    /// File callbacks exclude only the same inode: shared callbacks overlap,
    /// an exclusive one excludes every other callback on that inode. A
    /// namespace change excludes every callback on the mount; a shared
    /// namespace callback (path lookup) excludes only namespace changes.
    /// Recovery bypasses the namespace gate.
    pub(super) fn enter(
        &self,
        files: Option<&[u64]>,
        access: GateAccess,
        recovery: bool,
    ) -> Result<CallbackGuard, i32> {
        let mut files = files.map(<[u64]>::to_vec);
        if let Some(files) = &mut files {
            files.sort_unstable();
            files.dedup();
        }
        let mut state = self.gates.state.lock().map_err(|_| EIO)?;
        let mut waiting = true;
        match (&files, access) {
            (None, GateAccess::Exclusive) => state.namespace_waiters += 1,
            (Some(files), GateAccess::Exclusive) => {
                for file in files {
                    *state.exclusive_waiters.entry(*file).or_default() += 1;
                }
            }
            _ => waiting = false,
        }
        loop {
            let namespace_closed = state.namespace || state.namespace_waiters != 0;
            let blocked = match (&files, access) {
                (Some(files), access) => {
                    files.iter().any(|file| {
                        let recovering =
                            state.recoveries.iter().any(|(waiting, _)| waiting == file);
                        match access {
                            GateAccess::Exclusive => state.file_busy(*file) || recovering,
                            GateAccess::Shared => {
                                state.files.contains(file)
                                    || recovering
                                    || state.exclusive_waiters.contains_key(file)
                            }
                        }
                    }) || (!recovery && namespace_closed)
                }
                (None, GateAccess::Exclusive) => {
                    state.namespace || state.namespace_readers != 0 || state.any_file_holder()
                }
                (None, GateAccess::Shared) => namespace_closed,
            };
            if !blocked {
                break;
            }
            state = self.gates.changed.wait(state).map_err(|_| EIO)?;
        }
        let held = match (files, access) {
            (Some(files), GateAccess::Exclusive) => {
                for file in &files {
                    if waiting {
                        decrement(&mut state.exclusive_waiters, *file);
                    }
                    state.files.insert(*file);
                }
                Held::Files { files, access }
            }
            (Some(files), GateAccess::Shared) => {
                for file in &files {
                    *state.readers.entry(*file).or_default() += 1;
                }
                Held::Files { files, access }
            }
            (None, GateAccess::Exclusive) => {
                state.namespace_waiters -= 1;
                state.namespace = true;
                Held::Namespace(access)
            }
            (None, GateAccess::Shared) => {
                state.namespace_readers += 1;
                Held::Namespace(access)
            }
        };
        // Leaving the waiter set can admit blocked shared entrants.
        self.gates.changed.notify_all();
        Ok(CallbackGuard {
            gates: self.gates.clone(),
            held,
            _control: None,
        })
    }
    /// Recovery registers without blocking a FUSE receiver or consuming body quota.
    /// Its callback starts only after the addressed prefix settles (or fails).
    pub(super) fn recover(
        &self,
        inode: u64,
        closing: bool,
        ready: impl FnOnce(Option<CallbackGuard>, Result<(), i32>) + Send + 'static,
    ) {
        // Ordinary controls are bounded separately. One RELEASE per existing
        // FUSE handle has lifecycle capacity even when this ordinary bound is full.
        let previous = self.controls.fetch_add(1, Ordering::AcqRel);
        let lease = ControlLease(self.controls.clone());
        if previous >= 4096 && !closing {
            drop(lease);
            ready(None, Err(EAGAIN));
            return;
        }
        let shared = self.shared.clone();
        let ready = Arc::new(Mutex::new(Some(ready)));
        let on_start = ready.clone();
        let result = self.gates.recover(
            inode,
            Box::new(move |mut guard| {
                guard._control = Some(lease);
                let mut state = shared
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let callback = on_start
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take();
                let Some(callback) = callback else {
                    return;
                };
                if state.closed {
                    drop(state);
                    callback(Some(guard), Err(EIO));
                    return;
                }
                state.errors.remove(&inode);
                let through = state.next_sequence;
                state.waiters.push(Waiter {
                    inode,
                    through,
                    ready: Box::new(move |result| callback(Some(guard), result)),
                });
                let callbacks = Self::ready_waiters(&mut state);
                shared.changed.notify_all();
                drop(state);
                Self::notify_admissions(callbacks);
            }),
        );
        if let Err(error) = result {
            let callback = ready
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
            if let Some(callback) = callback {
                callback(None, Err(error));
            }
        }
    }
    fn ready_waiters(state: &mut State) -> Vec<(Admission, Result<(), i32>)> {
        let mut callbacks = Vec::new();
        let mut index = 0;
        while index < state.waiters.len() {
            let waiter = &state.waiters[index];
            let error = state.errors.get(&waiter.inode).copied();
            let active = state
                .active
                .is_some_and(|(inode, seq)| inode == waiter.inode && seq <= waiter.through);
            let pending = state
                .entries
                .iter()
                .any(|entry| entry.inode == waiter.inode && entry.sequence <= waiter.through);
            if error.is_some() || (!active && !pending) {
                let waiter = state.waiters.remove(index);
                callbacks.push((waiter.ready, error.map_or(Ok(()), Err)));
            } else {
                index += 1;
            }
        }
        callbacks
    }
    fn runnable(state: &State) -> Option<usize> {
        state
            .entries
            .iter()
            .position(|entry| entry.admission.is_none() && !state.errors.contains_key(&entry.inode))
    }
    fn admissions(shared: &Shared, state: &mut State) -> Vec<(Admission, Result<(), i32>)> {
        let mut callbacks = Vec::new();
        let mut index = 0;
        let mut blocked = HashSet::new();
        while index < state.entries.len() {
            let entry = &state.entries[index];
            if entry.admission.is_none() {
                index += 1;
                continue;
            }
            let error = if state.closing {
                Some(EIO)
            } else if let Some(error) = state.errors.get(&entry.inode) {
                Some(*error)
            } else if !state.errors.is_empty() {
                Some(EAGAIN)
            } else {
                None
            };
            if let Some(error) = error {
                if let Some(mut entry) = state.entries.remove(index) {
                    let bytes = entry.bytes;
                    if let Some(callback) = entry.admission.take() {
                        callbacks.push((callback, Err(error)));
                    }
                    drop(entry);
                    state.bytes -= bytes;
                }
                continue;
            }
            let bytes = entry.bytes;
            let inode = entry.inode;
            if !blocked.contains(&inode)
                && bytes <= shared.admitted_limit.saturating_sub(state.admitted_bytes)
            {
                if let Some(callback) = state.entries[index].admission.take() {
                    state.admitted_bytes += bytes;
                    callbacks.push((callback, Ok(())));
                }
            } else {
                blocked.insert(inode);
            }
            index += 1;
        }
        callbacks
    }
    fn notify_admissions(callbacks: Vec<(Admission, Result<(), i32>)>) {
        for (callback, result) in callbacks {
            callback(result);
        }
    }
    fn run(shared: Arc<Shared>) {
        loop {
            let mut state = shared
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let mut callbacks = Self::admissions(&shared, &mut state);
            callbacks.extend(Self::ready_waiters(&mut state));
            if !callbacks.is_empty() {
                shared.changed.notify_all();
                drop(state);
                Self::notify_admissions(callbacks);
                continue;
            }
            if state.closed {
                return;
            }
            let Some(index) = Self::runnable(&state) else {
                drop(
                    shared
                        .changed
                        .wait(state)
                        .unwrap_or_else(std::sync::PoisonError::into_inner),
                );
                continue;
            };
            let Some(mut entry) = state.entries.remove(index) else {
                continue;
            };
            state.active = Some((entry.inode, entry.sequence));
            drop(state);
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(&mut entry.job))
                .unwrap_or(Err(EIO));
            let mut state = shared
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.active = None;
            match result {
                Ok(()) => {
                    let bytes = entry.bytes;
                    drop(entry);
                    state.bytes -= bytes;
                    state.admitted_bytes -= bytes;
                }
                Err(error) => {
                    state.errors.insert(entry.inode, error);
                    state.entries.push_front(entry);
                }
            }
            shared.changed.notify_all();
        }
    }
    fn record_entry(
        state: &mut State,
        inode: u64,
        handle: u64,
        bytes: usize,
        job: Job,
        admission: Option<Admission>,
    ) {
        state.next_sequence = state.next_sequence.saturating_add(1);
        state.bytes += bytes;
        if admission.is_none() {
            state.admitted_bytes += bytes;
        }
        state.peak_bytes = state.peak_bytes.max(state.bytes);
        state.peak_pending_bytes = state
            .peak_pending_bytes
            .max(state.bytes - state.admitted_bytes);
        state.entries.push_back(Entry {
            sequence: state.next_sequence,
            inode,
            handle,
            bytes,
            job,
            admission,
        });
        state.peak_entries = state.peak_entries.max(state.entries.len());
    }
    /// The receiver never waits for body quota. A pending entry owns its input,
    /// but ACK is withheld until normal admission. Full reserve fails pre-ACK.
    pub(super) fn submit_deferred_with(
        &self,
        inode: u64,
        handle: u64,
        bytes: usize,
        make: impl FnOnce() -> Result<Job, i32>,
        admitted: impl FnOnce(Result<(), i32>) + Send + 'static,
    ) {
        // Ready results keep the inline fast path. Quota-limited requests
        // retain their callback until promotion. Both run outside the state lock.
        let mut callback: Option<Admission> = Some(Box::new(admitted));
        let result = (|| {
            let mut state = self.shared.state.lock().map_err(|_| EIO)?;
            if state.closing || state.closed {
                return Err(EIO);
            }
            if let Some(error) = state.errors.get(&inode) {
                return Err(*error);
            }
            if bytes > self.shared.admitted_limit {
                return Err(E2BIG);
            }
            if bytes > self.shared.limit.saturating_sub(state.bytes)
                || state.entries.len() >= MAX_WAITING_ENTRIES
            {
                return Err(EAGAIN);
            }
            let pending = bytes
                > self
                    .shared
                    .admitted_limit
                    .saturating_sub(state.admitted_bytes)
                || state
                    .entries
                    .iter()
                    .any(|entry| entry.inode == inode && entry.admission.is_some());
            if pending && !state.errors.is_empty() {
                return Err(EAGAIN);
            }
            let job = make()?;
            let admission = if pending { callback.take() } else { None };
            Self::record_entry(&mut state, inode, handle, bytes, job, admission);
            self.shared.changed.notify_all();
            Ok(())
        })();
        if let Some(callback) = callback {
            callback(result);
        }
    }
    #[cfg(test)]
    pub(super) fn submit_with(
        &self,
        inode: u64,
        handle: u64,
        bytes: usize,
        make: impl FnOnce() -> Result<Job, i32>,
    ) -> Result<(), i32> {
        if bytes > self.shared.limit {
            return Err(E2BIG);
        }
        let mut state = self.shared.state.lock().map_err(|_| EIO)?;
        loop {
            if state.closed || state.closing {
                return Err(EIO);
            }
            if let Some(error) = state.errors.get(&inode) {
                return Err(*error);
            }
            if bytes <= self.shared.limit - state.bytes && state.entries.len() < MAX_WAITING_ENTRIES
            {
                break;
            }
            if !state.errors.is_empty() {
                return Err(EAGAIN);
            }
            state = self.shared.changed.wait(state).map_err(|_| EIO)?;
        }
        Self::record_entry(&mut state, inode, handle, bytes, make()?, None);
        self.shared.changed.notify_all();
        Ok(())
    }
    pub(super) fn wait(&self, inode: Option<u64>, retry: bool) -> Result<(), i32> {
        let mut state = self.shared.state.lock().map_err(|_| EIO)?;
        let through = state.next_sequence;
        if state.closed {
            return Err(EIO);
        }
        if retry && let Some(inode) = inode {
            state.errors.remove(&inode);
            self.shared.changed.notify_all();
        }
        loop {
            if let Some(error) = inode.and_then(|inode| state.errors.get(&inode).copied()) {
                return Err(error);
            }
            let active = state.active.is_some_and(|(file, seq)| {
                seq <= through && inode.is_none_or(|inode| inode == file)
            });
            let pending = state.entries.iter().any(|entry| {
                entry.sequence <= through
                    && inode.is_none_or(|inode| inode == entry.inode)
                    && !state.errors.contains_key(&entry.inode)
            });
            if !active && !pending {
                return Ok(());
            }
            state = self.shared.changed.wait(state).map_err(|_| EIO)?;
        }
    }
    #[cfg(test)]
    pub(super) fn has_pending(&self) -> bool {
        self.shared
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .bytes
            != 0
    }
    pub(super) fn release_handle(&self, inode: u64, handle: u64, last: bool) {
        let mut state = self
            .shared
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut callbacks = Vec::new();
        let mut index = 0;
        while index < state.entries.len() {
            if state.entries[index].handle == handle
                || (last && state.entries[index].inode == inode)
            {
                if let Some(mut entry) = state.entries.remove(index) {
                    let bytes = entry.bytes;
                    if let Some(callback) = entry.admission.take() {
                        callbacks.push((callback, Err(EIO)));
                    } else {
                        state.admitted_bytes -= bytes;
                    }
                    drop(entry);
                    state.bytes -= bytes;
                }
            } else {
                index += 1;
            }
        }
        if last {
            state.errors.remove(&inode);
        }
        self.shared.changed.notify_all();
        drop(state);
        Self::notify_admissions(callbacks);
    }
    pub(super) fn close(&self) -> Result<(), i32> {
        let mut state = self.shared.state.lock().map_err(|_| EIO)?;
        if state.closed {
            return state.errors.values().next().copied().map_or(Ok(()), Err);
        }
        state.closing = true;
        self.shared.changed.notify_all();
        drop(state);
        let drained = self.wait(None, false);
        let mut state = self.shared.state.lock().map_err(|_| EIO)?;
        let result = state.errors.values().next().copied().map_or(drained, Err);
        let mut callbacks = Self::admissions(&self.shared, &mut state);
        callbacks.extend(Self::ready_waiters(&mut state));
        state.closed = true;
        state.entries.clear();
        state.bytes = 0;
        state.admitted_bytes = 0;
        afs_logging::info!("fuse.front_write_summary";
            "peak_bytes" => state.peak_bytes as u64,
            "peak_pending_bytes" => state.peak_pending_bytes as u64,
            "peak_entries" => state.peak_entries as u64,
            "limit_bytes" => self.shared.limit as u64,
            "admitted_limit_bytes" => self.shared.admitted_limit as u64,
        );
        self.shared.changed.notify_all();
        drop(state);
        Self::notify_admissions(callbacks);
        if let Some(worker) = self.worker.lock().map_err(|_| EIO)?.take() {
            worker.join().map_err(|_| EIO)?;
        }
        result
    }
}
impl Drop for FrontWriteQueue {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc,
    };
    use std::time::{Duration, Instant};

    fn assert_state_lock_available(shared: &Shared) {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            if shared.state.try_lock().is_ok() {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "callback retained the state lock"
            );
            thread::yield_now();
        }
    }
    fn submit(
        queue: &FrontWriteQueue,
        inode: u64,
        handle: u64,
        bytes: usize,
        job: impl FnMut() -> Result<(), i32> + Send + 'static,
    ) -> Result<(), i32> {
        queue.submit_with(inode, handle, bytes, || Ok(Box::new(job)))
    }
    #[test]
    fn ready_admission_reply_stays_on_caller_thread() {
        let q = FrontWriteQueue::with_limits(8, 2).unwrap();
        let caller = std::thread::current().id();
        let (tx, rx) = mpsc::channel();
        q.submit_deferred_with(
            1,
            11,
            1,
            || Ok(Box::new(|| Ok(()))),
            move |r| {
                tx.send((r, std::thread::current().id())).unwrap();
            },
        );
        let (result, reply_thread) = rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_eq!(result, Ok(()));
        assert_eq!(
            reply_thread, caller,
            "ready admission must keep the inline fast path"
        );
    }

    #[test]
    fn ready_and_rejected_replies_do_not_wait_for_body_io() {
        let q = FrontWriteQueue::with_limits(8, 2).unwrap();
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let (ack_tx, ack_rx) = mpsc::channel();
        q.submit_deferred_with(
            1,
            11,
            4,
            || {
                Ok(Box::new(move || {
                    started_tx.send(thread::current().id()).unwrap();
                    release_rx.recv().unwrap();
                    Ok(())
                }))
            },
            move |r| ack_tx.send((r, thread::current().id())).unwrap(),
        );
        let body_thread = started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let first = ack_rx.recv_timeout(Duration::from_secs(2));
        let (next_tx, next_rx) = mpsc::channel();
        q.submit_deferred_with(
            2,
            22,
            4,
            || Ok(Box::new(|| Ok(()))),
            move |r| next_tx.send((r, thread::current().id())).unwrap(),
        );
        let next = next_rx.recv_timeout(Duration::from_secs(2));
        let (error_tx, error_rx) = mpsc::channel();
        q.submit_deferred_with(
            3,
            33,
            9,
            || panic!("oversize must not allocate"),
            move |r| error_tx.send((r, thread::current().id())).unwrap(),
        );
        let error = error_rx.recv_timeout(Duration::from_secs(2));
        // Release before asserting so a failure cannot strand the body worker.
        release_tx.send(()).unwrap();
        let (result, reply_thread) = first.unwrap();
        assert_eq!(result, Ok(()));
        assert_eq!(reply_thread, thread::current().id());
        assert_ne!(reply_thread, body_thread);
        assert_eq!(next.unwrap(), (Ok(()), reply_thread));
        assert_eq!(error.unwrap(), (Err(E2BIG), reply_thread));
        q.close().unwrap();
    }

    #[test]
    fn close_settles_admission_and_rejects_new_input_inline() {
        let q = FrontWriteQueue::with_limits(8, 2).unwrap();
        let (tx, rx) = mpsc::channel();
        q.submit_deferred_with(
            1,
            11,
            1,
            || Ok(Box::new(|| Ok(()))),
            move |r| tx.send(r).unwrap(),
        );
        q.close().unwrap();
        assert_eq!(rx.try_recv().unwrap(), Ok(()));
        let (tx, rx) = mpsc::channel();
        q.submit_deferred_with(
            1,
            11,
            1,
            || panic!("closed must not allocate"),
            move |r| tx.send((r, thread::current().id())).unwrap(),
        );
        let (result, id) = rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_eq!(result, Err(EIO));
        assert_eq!(id, thread::current().id());
    }

    #[test]
    fn ready_and_rejected_callbacks_run_outside_the_state_lock() {
        let q = FrontWriteQueue::with_limits(8, 2).unwrap();
        for (bytes, expected) in [(1, Ok(())), (9, Err(E2BIG))] {
            let shared = q.shared.clone();
            let (tx, rx) = mpsc::channel();
            q.submit_deferred_with(
                1,
                11,
                bytes,
                || Ok(Box::new(|| Ok(()))),
                move |result| {
                    assert_state_lock_available(&shared);
                    tx.send(result).unwrap();
                },
            );
            assert_eq!(rx.try_recv().unwrap(), expected);
        }
        q.close().unwrap();
    }

    #[test]
    fn admitted_bodies_keep_cross_file_fifo_without_fair_scheduling() {
        let q = FrontWriteQueue::with_limits(16, 0).unwrap();
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        submit(&q, 1, 11, 1, move || {
            entered_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            Ok(())
        })
        .unwrap();
        entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let (done_tx, done_rx) = mpsc::channel();
        // A1 is active. A2, B1 and A3 must execute in their arrival order.
        for (inode, handle, label) in [(1, 11, "A2"), (2, 22, "B1"), (1, 12, "A3")] {
            let tx = done_tx.clone();
            submit(&q, inode, handle, 1, move || {
                tx.send(label).unwrap();
                Ok(())
            })
            .unwrap();
        }
        release_tx.send(()).unwrap();
        for label in ["A2", "B1", "A3"] {
            assert_eq!(done_rx.recv_timeout(Duration::from_secs(2)).unwrap(), label);
        }
        q.wait(None, false).unwrap();
        assert!(!q.has_pending());
    }

    #[test]
    fn admission_is_early_and_capacity_includes_executing_body() {
        let q = Arc::new(FrontWriteQueue::with_limits(8, 0).unwrap());
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        submit(&q, 1, 1, 8, move || {
            entered_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            Ok(())
        })
        .unwrap();
        entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let (done_tx, done_rx) = mpsc::channel();
        let worker = q.clone();
        let t = thread::spawn(move || done_tx.send(submit(&worker, 2, 2, 1, || Ok(()))).unwrap());
        assert!(done_rx.recv_timeout(Duration::from_millis(50)).is_err());
        release_tx.send(()).unwrap();
        assert_eq!(
            done_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
            Ok(())
        );
        t.join().unwrap();
        q.wait(None, false).unwrap();
        assert!(!q.has_pending());
    }
    #[test]
    fn failures_are_scoped_and_other_files_make_progress() {
        let q = FrontWriteQueue::with_limits(8, 0).unwrap();
        submit(&q, 1, 11, 4, || Err(libc::ENOSPC)).unwrap();
        assert_eq!(q.wait(Some(1), false), Err(libc::ENOSPC));
        let done = Arc::new(AtomicUsize::new(0));
        let complete = done.clone();
        submit(&q, 2, 22, 4, move || {
            complete.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .unwrap();
        q.wait(Some(2), false).unwrap();
        assert_eq!(done.load(Ordering::SeqCst), 1);
        assert_eq!(submit(&q, 1, 11, 1, || Ok(())), Err(libc::ENOSPC));
        assert_eq!(q.wait(Some(1), false), Err(libc::ENOSPC));
        q.release_handle(1, 11, true);
        q.wait(None, false).unwrap();
        assert!(!q.has_pending());
    }
    #[test]
    fn failed_retention_cannot_deadlock_capacity_wait() {
        let q = FrontWriteQueue::with_limits(8, 0).unwrap();
        submit(&q, 1, 11, 8, || Err(EIO)).unwrap();
        assert_eq!(q.wait(Some(1), false), Err(EIO));
        assert_eq!(submit(&q, 2, 22, 1, || Ok(())), Err(EAGAIN));
        q.release_handle(1, 11, true);
        submit(&q, 2, 22, 8, || Ok(())).unwrap();
        q.wait(None, false).unwrap();
    }
    #[test]
    fn retry_keeps_partial_progress_and_fifo_tail() {
        let q = FrontWriteQueue::with_limits(8, 0).unwrap();
        let order = Arc::new(Mutex::new(Vec::new()));
        let first = order.clone();
        let mut progress = 0;
        submit(&q, 1, 11, 4, move || {
            if progress == 0 {
                first.lock().unwrap().push(1);
                progress = 1;
                Err(EIO)
            } else {
                first.lock().unwrap().push(2);
                Ok(())
            }
        })
        .unwrap();
        // Admission of a same-inode tail before the first failure is exercised deterministically below.
        assert_eq!(q.wait(Some(1), false), Err(EIO));
        q.wait(Some(1), true).unwrap();
        let tail = order.clone();
        submit(&q, 1, 12, 4, move || {
            tail.lock().unwrap().push(3);
            Ok(())
        })
        .unwrap();
        q.wait(Some(1), false).unwrap();
        assert_eq!(*order.lock().unwrap(), vec![1, 2, 3]);
    }
    #[test]
    fn same_inode_tail_does_not_overtake_failed_head() {
        let q = FrontWriteQueue::with_limits(8, 0).unwrap();
        let order = Arc::new(Mutex::new(Vec::new()));
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let first = order.clone();
        let mut attempts = 0;
        submit(&q, 1, 11, 4, move || {
            attempts += 1;
            if attempts == 1 {
                entered_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                Err(EIO)
            } else {
                first.lock().unwrap().push(1);
                Ok(())
            }
        })
        .unwrap();
        entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let tail = order.clone();
        submit(&q, 1, 12, 4, move || {
            tail.lock().unwrap().push(2);
            Ok(())
        })
        .unwrap();
        release_tx.send(()).unwrap();
        assert_eq!(q.wait(Some(1), false), Err(EIO));
        assert!(order.lock().unwrap().is_empty());
        q.wait(Some(1), true).unwrap();
        assert_eq!(*order.lock().unwrap(), vec![1, 2]);
    }
    #[test]
    fn failed_close_does_not_close_mount_or_clear_other_open_error() {
        let q = FrontWriteQueue::with_limits(8, 0).unwrap();
        submit(&q, 1, 11, 4, || Err(EIO)).unwrap();
        assert_eq!(q.wait(Some(1), false), Err(EIO));
        q.release_handle(1, 11, false);
        assert_eq!(q.wait(Some(1), false), Err(EIO));
        submit(&q, 2, 22, 4, || Ok(())).unwrap();
        q.wait(Some(2), false).unwrap();
        q.release_handle(1, 12, true);
        q.wait(None, false).unwrap();
    }
    #[test]
    fn ownership_released_before_capacity_and_shutdown_returns() {
        struct Held(Arc<AtomicUsize>);
        impl Drop for Held {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let q = FrontWriteQueue::with_limits(8, 0).unwrap();
        let drops = Arc::new(AtomicUsize::new(0));
        let held = Held(drops.clone());
        submit(&q, 1, 11, 8, move || {
            let _ = &held;
            Err(EIO)
        })
        .unwrap();
        assert_eq!(q.wait(Some(1), false), Err(EIO));
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert_eq!(q.close(), Err(EIO));
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(submit(&q, 1, 11, 1, || Ok(())), Err(EIO));
    }
    #[test]
    fn rejected_allocation_never_charges_capacity() {
        let q = FrontWriteQueue::with_limits(8, 0).unwrap();
        assert_eq!(
            q.submit_with(1, 1, 4, || Err(libc::ENOMEM)),
            Err(libc::ENOMEM)
        );
        assert!(!q.has_pending());
        assert_eq!(submit(&q, 1, 1, 9, || Ok(())), Err(E2BIG));
    }
    #[test]
    fn deferred_quota_full_returns_receiver_and_withholds_ack() {
        let q = FrontWriteQueue::with_limits(56, 8).unwrap();
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let (ack_tx, ack_rx) = mpsc::channel();
        q.submit_deferred_with(
            1,
            11,
            56,
            || {
                Ok(Box::new(move || {
                    entered_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                    Ok(())
                }))
            },
            move |result| ack_tx.send(result).unwrap(),
        );
        assert_eq!(ack_rx.recv_timeout(Duration::from_secs(2)).unwrap(), Ok(()));
        entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let (pending_tx, pending_rx) = mpsc::channel();
        let pending_shared = q.shared.clone();
        q.submit_deferred_with(
            1,
            11,
            8,
            || Ok(Box::new(|| Ok(()))),
            move |result| {
                assert_state_lock_available(&pending_shared);
                pending_tx.send(result).unwrap();
            },
        );
        assert!(
            pending_rx.try_recv().is_err(),
            "full body budget ACKed pending data"
        );
        let (reject_tx, reject_rx) = mpsc::channel();
        q.submit_deferred_with(
            2,
            22,
            1,
            || panic!("must reject before allocation"),
            move |result| reject_tx.send(result).unwrap(),
        );
        assert_eq!(
            reject_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
            Err(EAGAIN)
        );
        // Neither registration nor recovery needs body capacity or another thread.
        drop(q.enter(Some(&[1]), GateAccess::Exclusive, true).unwrap());
        q.wait(Some(2), true).unwrap();
        release_tx.send(()).unwrap();
        assert_eq!(
            pending_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
            Ok(())
        );
        q.wait(Some(1), true).unwrap();
        assert!(!q.has_pending());
    }

    #[test]
    fn deferred_failure_rejects_unacked_tail_and_sync_retries_retained_head() {
        let q = FrontWriteQueue::with_limits(56, 8).unwrap();
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let mut attempt = 0;
        q.submit_deferred_with(
            1,
            11,
            56,
            || {
                Ok(Box::new(move || {
                    attempt += 1;
                    if attempt == 1 {
                        entered_tx.send(()).unwrap();
                        release_rx.recv().unwrap();
                        Err(EIO)
                    } else {
                        Ok(())
                    }
                }))
            },
            |result| assert_eq!(result, Ok(())),
        );
        entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let (tail_tx, tail_rx) = mpsc::channel();
        q.submit_deferred_with(
            1,
            12,
            8,
            || Ok(Box::new(|| panic!("unacknowledged failed tail executed"))),
            move |result| tail_tx.send(result).unwrap(),
        );
        release_tx.send(()).unwrap();
        assert_eq!(q.wait(Some(1), false), Err(EIO));
        assert_eq!(
            tail_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
            Err(EIO)
        );
        let _gate = q.enter(Some(&[1]), GateAccess::Exclusive, true).unwrap();
        q.wait(Some(1), true).unwrap();
        q.release_handle(1, 11, true);
        assert!(!q.has_pending());
    }

    #[test]
    fn deferred_failed_full_retention_allows_close_to_reclaim_and_other_file() {
        let q = FrontWriteQueue::with_limits(56, 8).unwrap();
        q.submit_deferred_with(
            1,
            11,
            56,
            || Ok(Box::new(|| Err(libc::ENOSPC))),
            |result| assert_eq!(result, Ok(())),
        );
        assert_eq!(q.wait(Some(1), false), Err(libc::ENOSPC));
        let (tx, rx) = mpsc::channel();
        q.submit_deferred_with(
            2,
            22,
            8,
            || panic!("failed capacity must not allocate"),
            move |result| tx.send(result).unwrap(),
        );
        assert_eq!(
            rx.recv_timeout(Duration::from_secs(2)).unwrap(),
            Err(EAGAIN)
        );
        let close = q.enter(Some(&[1]), GateAccess::Exclusive, true).unwrap();
        assert_eq!(q.wait(Some(1), true), Err(libc::ENOSPC));
        q.release_handle(1, 11, true);
        drop(close);
        q.submit_deferred_with(
            2,
            22,
            56,
            || Ok(Box::new(|| Ok(()))),
            |result| assert_eq!(result, Ok(())),
        );
        q.wait(Some(2), true).unwrap();
        assert!(!q.has_pending());
    }

    #[test]
    fn file_gates_are_independent_and_recovery_bypasses_namespace_gate() {
        let q = FrontWriteQueue::with_limits(56, 8).unwrap();
        let a = q.enter(Some(&[1]), GateAccess::Exclusive, false).unwrap();
        drop(q.enter(Some(&[2]), GateAccess::Exclusive, false).unwrap());
        drop(a);
        let namespace = q.enter(None, GateAccess::Exclusive, false).unwrap();
        drop(q.enter(Some(&[1]), GateAccess::Exclusive, true).unwrap());
        drop(namespace);
        drop(
            q.enter(Some(&[2, 1, 2]), GateAccess::Exclusive, false)
                .unwrap(),
        );
    }

    /// Runs `enter` on another thread; `Ok(())` arrives once it is admitted.
    fn enter_later(
        q: &Arc<FrontWriteQueue>,
        files: Option<Vec<u64>>,
        access: GateAccess,
    ) -> (mpsc::Receiver<()>, mpsc::Sender<()>) {
        let (entered_tx, entered_rx) = mpsc::channel();
        let (leave_tx, leave_rx) = mpsc::channel::<()>();
        let q = q.clone();
        std::thread::spawn(move || {
            let guard = q.enter(files.as_deref(), access, false).unwrap();
            entered_tx.send(()).unwrap();
            let _ = leave_rx.recv();
            drop(guard);
        });
        (entered_rx, leave_tx)
    }

    const ADMIT: Duration = Duration::from_secs(2);
    const HELD: Duration = Duration::from_millis(100);

    #[test]
    fn shared_file_callbacks_overlap_on_one_inode() {
        let q = FrontWriteQueue::with_limits(56, 8).unwrap();
        let first = q.enter(Some(&[1]), GateAccess::Shared, false).unwrap();
        let second = q.enter(Some(&[1]), GateAccess::Shared, false).unwrap();
        drop((first, second));
        drop(q.enter(Some(&[1]), GateAccess::Exclusive, false).unwrap());
    }

    #[test]
    fn exclusive_file_callback_waits_for_readers_and_fences_new_ones() {
        let q = Arc::new(FrontWriteQueue::with_limits(56, 8).unwrap());
        let reader = q.enter(Some(&[1]), GateAccess::Shared, false).unwrap();
        let (writer_in, writer_out) = enter_later(&q, Some(vec![1]), GateAccess::Exclusive);
        assert!(
            writer_in.recv_timeout(HELD).is_err(),
            "writer overlapped a reader"
        );
        // The waiting writer fences later readers of the same inode only.
        let (late_in, late_out) = enter_later(&q, Some(vec![1]), GateAccess::Shared);
        assert!(
            late_in.recv_timeout(HELD).is_err(),
            "reader overtook a waiting writer"
        );
        drop(q.enter(Some(&[2]), GateAccess::Shared, false).unwrap());
        drop(reader);
        writer_in.recv_timeout(ADMIT).unwrap();
        assert!(
            late_in.recv_timeout(HELD).is_err(),
            "reader overlapped a writer"
        );
        writer_out.send(()).unwrap();
        late_in.recv_timeout(ADMIT).unwrap();
        late_out.send(()).unwrap();
    }

    #[test]
    fn path_lookups_overlap_file_callbacks_but_not_namespace_changes() {
        let q = Arc::new(FrontWriteQueue::with_limits(56, 8).unwrap());
        let file = q.enter(Some(&[1]), GateAccess::Exclusive, false).unwrap();
        let lookup = q.enter(None, GateAccess::Shared, false).unwrap();
        drop(q.enter(None, GateAccess::Shared, false).unwrap());
        drop(file);
        let (change_in, change_out) = enter_later(&q, None, GateAccess::Exclusive);
        assert!(
            change_in.recv_timeout(HELD).is_err(),
            "namespace change overlapped a lookup"
        );
        drop(lookup);
        change_in.recv_timeout(ADMIT).unwrap();
        let (lookup_in, lookup_out) = enter_later(&q, None, GateAccess::Shared);
        assert!(
            lookup_in.recv_timeout(HELD).is_err(),
            "lookup overlapped a namespace change"
        );
        change_out.send(()).unwrap();
        lookup_in.recv_timeout(ADMIT).unwrap();
        lookup_out.send(()).unwrap();
    }

    #[test]
    fn waiting_namespace_change_fences_new_lookups_and_file_callbacks() {
        let q = Arc::new(FrontWriteQueue::with_limits(56, 8).unwrap());
        let reader = q.enter(Some(&[1]), GateAccess::Shared, false).unwrap();
        let (change_in, change_out) = enter_later(&q, None, GateAccess::Exclusive);
        assert!(
            change_in.recv_timeout(HELD).is_err(),
            "namespace change overlapped a reader"
        );
        let (lookup_in, lookup_out) = enter_later(&q, None, GateAccess::Shared);
        let (other_in, other_out) = enter_later(&q, Some(vec![2]), GateAccess::Shared);
        assert!(
            lookup_in.recv_timeout(HELD).is_err(),
            "lookup overtook a waiting change"
        );
        assert!(
            other_in.recv_timeout(HELD).is_err(),
            "reader overtook a waiting change"
        );
        drop(reader);
        change_in.recv_timeout(ADMIT).unwrap();
        change_out.send(()).unwrap();
        lookup_in.recv_timeout(ADMIT).unwrap();
        other_in.recv_timeout(ADMIT).unwrap();
        lookup_out.send(()).unwrap();
        other_out.send(()).unwrap();
    }

    #[test]
    fn recovery_waits_for_readers_and_fences_new_readers() {
        let q = FrontWriteQueue::with_limits(56, 8).unwrap();
        let reader = q.enter(Some(&[1]), GateAccess::Shared, false).unwrap();
        let (tx, rx) = mpsc::channel();
        q.recover(1, false, move |guard, result| {
            tx.send((guard, result)).unwrap();
        });
        assert!(
            rx.recv_timeout(HELD).is_err(),
            "recovery overlapped a reader"
        );
        drop(reader);
        let (guard, result) = rx.recv_timeout(ADMIT).unwrap();
        assert_eq!(result, Ok(()));
        let q = Arc::new(q);
        let (late_in, late_out) = enter_later(&q, Some(vec![1]), GateAccess::Shared);
        assert!(
            late_in.recv_timeout(HELD).is_err(),
            "reader overlapped a recovery"
        );
        drop(guard);
        late_in.recv_timeout(ADMIT).unwrap();
        late_out.send(()).unwrap();
    }

    #[test]
    fn recovery_registration_returns_while_file_gate_is_held() {
        let q = FrontWriteQueue::with_limits(56, 8).unwrap();
        let first = q.enter(Some(&[1]), GateAccess::Exclusive, false).unwrap();
        let (tx, rx) = mpsc::channel();
        q.recover(1, false, move |guard, result| {
            assert!(guard.is_some());
            tx.send(result).unwrap();
        });
        assert!(rx.try_recv().is_err());
        drop(first);
        assert_eq!(rx.recv_timeout(Duration::from_secs(2)).unwrap(), Ok(()));
        assert_eq!(q.controls.load(Ordering::Acquire), 0);
    }

    #[test]
    fn recovery_at_full_budget_waits_without_occupying_receiver_and_reports_failure() {
        let q = FrontWriteQueue::with_limits(56, 8).unwrap();
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let mut attempt = 0;
        q.submit_deferred_with(
            1,
            11,
            56,
            || {
                Ok(Box::new(move || {
                    attempt += 1;
                    if attempt == 1 {
                        entered_tx.send(()).unwrap();
                        release_rx.recv().unwrap();
                    }
                    Err(EIO)
                }))
            },
            |result| assert_eq!(result, Ok(())),
        );
        entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let (pending_tx, pending_rx) = mpsc::channel();
        q.submit_deferred_with(
            1,
            11,
            8,
            || Ok(Box::new(|| Ok(()))),
            move |result| pending_tx.send(result).unwrap(),
        );
        let (sync_tx, sync_rx) = mpsc::channel();
        q.recover(1, false, move |guard, result| {
            assert!(guard.is_some());
            sync_tx.send(result).unwrap();
        });
        assert!(sync_rx.try_recv().is_err());
        let (other_tx, other_rx) = mpsc::channel();
        q.recover(2, false, move |guard, result| {
            assert!(guard.is_some());
            other_tx.send(result).unwrap();
        });
        assert_eq!(
            other_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
            Ok(())
        );
        release_tx.send(()).unwrap();
        assert_eq!(
            pending_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
            Err(EIO)
        );
        assert_eq!(
            sync_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
            Err(EIO)
        );
        let (close_tx, close_rx) = mpsc::channel();
        q.recover(1, true, move |guard, result| {
            assert!(guard.is_some());
            close_tx.send(result).unwrap();
        });
        assert_eq!(
            close_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
            Err(EIO)
        );
        q.release_handle(1, 11, true);
        assert!(!q.has_pending());
    }

    #[test]
    fn ordinary_control_limit_does_not_reject_lifecycle_close() {
        let q = FrontWriteQueue::with_limits(56, 8).unwrap();
        q.controls.store(4096, Ordering::Release);
        let (tx, rx) = mpsc::channel();
        q.recover(1, false, move |guard, result| {
            assert!(guard.is_none());
            tx.send(result).unwrap();
        });
        assert_eq!(
            rx.recv_timeout(Duration::from_secs(2)).unwrap(),
            Err(EAGAIN)
        );
        let (tx, rx) = mpsc::channel();
        q.recover(1, true, move |guard, result| {
            assert!(guard.is_some());
            tx.send(result).unwrap();
        });
        assert_eq!(rx.recv_timeout(Duration::from_secs(2)).unwrap(), Ok(()));
        assert_eq!(q.controls.load(Ordering::Acquire), 4096);
        q.controls.store(0, Ordering::Release);
    }
    #[test]
    fn quota_normal_256_mib_has_additional_32_mib_waiting_reserve() {
        let q = FrontWriteQueue::new(256 * 1024 * 1024).unwrap();
        assert_eq!(q.shared.admitted_limit, 256 * 1024 * 1024);
        assert_eq!(q.shared.limit, 288 * 1024 * 1024);
        assert_eq!(q.shared.limit - q.shared.admitted_limit, 32 * 1024 * 1024);
    }

    #[test]
    fn quota_full_normal_window_defers_only_additional_reserve_and_allows_recovery() {
        let mib = 1024 * 1024;
        let q = FrontWriteQueue::new(256 * mib).unwrap();
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let (ack_tx, ack_rx) = mpsc::channel();
        q.submit_deferred_with(
            1,
            11,
            256 * mib,
            || {
                Ok(Box::new(move || {
                    entered_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                    Ok(())
                }))
            },
            move |result| ack_tx.send(result).unwrap(),
        );
        assert_eq!(ack_rx.recv_timeout(Duration::from_secs(2)).unwrap(), Ok(()));
        entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let (pending_tx, pending_rx) = mpsc::channel();
        q.submit_deferred_with(
            1,
            11,
            32 * mib,
            || Ok(Box::new(|| Ok(()))),
            move |result| pending_tx.send(result).unwrap(),
        );
        let pending_before = pending_rx.try_recv();
        let (reject_tx, reject_rx) = mpsc::channel();
        q.submit_deferred_with(
            2,
            22,
            1,
            || panic!("full total quota must reject before allocating"),
            move |result| reject_tx.send(result).unwrap(),
        );
        let rejected = reject_rx.recv_timeout(Duration::from_secs(2));
        let (control_tx, control_rx) = mpsc::channel();
        q.recover(2, false, move |guard, result| {
            drop(guard);
            control_tx.send(result).unwrap();
        });
        let control_before = control_rx.recv_timeout(Duration::from_secs(2));
        release_tx.send(()).unwrap();
        q.wait(Some(1), false).unwrap();
        assert!(
            pending_before.is_err(),
            "waiting reserve was ACKed before normal quota freed"
        );
        assert_eq!(
            pending_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
            Ok(())
        );
        assert_eq!(rejected.unwrap(), Err(EAGAIN));
        assert_eq!(control_before.unwrap(), Ok(()));
        assert!(!q.has_pending());
    }
}
