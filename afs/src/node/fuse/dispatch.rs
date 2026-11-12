//! Keyed FIFO dispatch queue and worker pool for FUSE callbacks.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;

pub(super) const MAX_OWNED_WRITE_INPUT_BYTES: usize = 32 * 1024 * 1024;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum FuseAccess {
    Exclusive,
    Read,
}

#[derive(Clone, Copy)]
struct FuseTurn {
    key: u64,
    access: FuseAccess,
}

enum FuseActive {
    Exclusive,
    Readers(usize),
}

enum FuseJob {
    Owned(Box<dyn FnOnce() + Send + 'static>),
    // Nonblocking starter only: hand ownership to a future, never run IO or wait here.
    #[cfg(any(feature = "dfs", test))]
    Deferred(Box<dyn FnOnce(std::result::Result<FuseCompletion, i32>) + Send + 'static>),
    Borrowed(Arc<(Mutex<bool>, Condvar)>),
}

/// Owns the admitted task and its ordering turn after the worker is returned.
/// Moving this into a future or completion event does not retire the request.
#[cfg(any(feature = "dfs", test))]
pub(super) struct FuseCompletion {
    inner: Arc<FuseCompletionInner>,
}
#[cfg(any(feature = "dfs", test))]
struct FuseCompletionInner {
    shared: Arc<FuseDispatchShared>,
    turn: Option<FuseTurn>,
}
#[cfg(any(feature = "dfs", test))]
impl FuseCompletion {
    pub(super) fn hold(&self) -> crate::node::read::admission::ReadLifetime {
        self.inner.clone()
    }
}
#[cfg(any(feature = "dfs", test))]
impl Drop for FuseCompletionInner {
    fn drop(&mut self) {
        FuseDispatch::finish(&self.shared, self.turn.take());
    }
}

// Unlike an owned worker callback, a borrowed callback runs on the receiver.
// Keep its FIFO slot active until that callback finishes, including unwinding.
struct BorrowedAdmission<'a> {
    dispatch: &'a FuseDispatch,
    key: u64,
}

impl Drop for BorrowedAdmission<'_> {
    fn drop(&mut self) {
        FuseDispatch::finish(
            &self.dispatch.shared,
            Some(FuseTurn {
                key: self.key,
                access: FuseAccess::Exclusive,
            }),
        );
    }
}

// Independent handles and adjacent eligible reads may run concurrently. A
// queued exclusive callback fences later reads through fsync and release. Capacity waits backpressure only
// receive callbacks: pool jobs must not synchronously reenter this dispatcher or
// wait for the receive thread (in particular, notifier work must be separate).
pub(super) struct FuseDispatch {
    pub(super) shared: Arc<FuseDispatchShared>,
    pub(super) capacity: usize,
}

/// A recovery continuation already owns its file/control admission. Enqueuing
/// it must not wait for the ordinary dispatcher quota from inside a completion.
pub(super) struct FuseRecoverySender {
    pub(super) shared: Arc<FuseDispatchShared>,
    pub(super) normal_capacity: Option<usize>,
}
impl FuseRecoverySender {
    pub(super) fn submit(&self, key: u64, job: impl FnOnce() + Send + 'static) {
        let mut queue = self
            .shared
            .queue
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while self
            .normal_capacity
            .is_some_and(|capacity| queue.jobs >= capacity)
            && !queue.closed
        {
            queue = self
                .shared
                .capacity
                .wait(queue)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        if queue.closed {
            drop(queue);
            drop(job);
            return;
        }
        queue.jobs += 1;
        #[cfg(feature = "dfs")]
        if let Some(metrics) = self.shared.activity.get() {
            metrics.observe_count(
                crate::node::read::activity::Activity::FuseTask,
                queue.jobs as u64,
            );
        }
        let job = FuseJob::Owned(Box::new(job));
        if let std::collections::hash_map::Entry::Vacant(entry) = queue.active.entry(key) {
            entry.insert(FuseActive::Exclusive);
            queue.ready.push_back((
                Some(FuseTurn {
                    key,
                    access: FuseAccess::Exclusive,
                }),
                job,
            ));
            self.shared.ready.notify_one();
        } else {
            queue
                .waiting
                .entry(key)
                .or_default()
                .push_back((FuseAccess::Exclusive, job));
        }
    }
}

pub(super) struct FuseDispatchShared {
    #[cfg(feature = "dfs")]
    pub(super) activity: std::sync::OnceLock<crate::node::read::activity::Activities>,
    pub(super) queue: Mutex<FuseQueue>,
    ready: Condvar,
    capacity: Condvar,
    drained: Condvar,
}

pub(super) struct FuseWriteInput {
    data: Vec<u8>,
    shared: Arc<FuseDispatchShared>,
}

impl FuseWriteInput {
    pub(super) fn as_slice(&self) -> &[u8] {
        &self.data
    }
}

impl Drop for FuseWriteInput {
    fn drop(&mut self) {
        let bytes = self.data.len();
        drop(std::mem::take(&mut self.data));
        if let Ok(mut queue) = self.shared.queue.lock() {
            queue.write_input_bytes = queue.write_input_bytes.saturating_sub(bytes);
        }
        self.shared.capacity.notify_all();
    }
}

pub(super) struct FuseQueue {
    ready: VecDeque<(Option<FuseTurn>, FuseJob)>,
    waiting: HashMap<u64, VecDeque<(FuseAccess, FuseJob)>>,
    active: HashMap<u64, FuseActive>,
    pub(super) jobs: usize,
    pub(super) write_input_bytes: usize,
    closed: bool,
}

impl FuseDispatch {
    #[cfg(test)]
    pub(super) fn new(workers: usize) -> Self {
        Self::with_capacity(workers, workers * 8)
    }

    pub(super) fn with_capacity(workers: usize, capacity: usize) -> Self {
        let shared = Arc::new(FuseDispatchShared {
            #[cfg(feature = "dfs")]
            activity: std::sync::OnceLock::new(),
            queue: Mutex::new(FuseQueue {
                ready: VecDeque::new(),
                waiting: HashMap::new(),
                active: HashMap::new(),
                jobs: 0,
                write_input_bytes: 0,
                closed: false,
            }),
            ready: Condvar::new(),
            capacity: Condvar::new(),
            drained: Condvar::new(),
        });
        for index in 0..workers {
            let shared = shared.clone();
            thread::Builder::new()
                .name(format!("afs-fuse-{index}"))
                .spawn(move || {
                    loop {
                        let (key, job) = {
                            let mut queue = shared.queue.lock().unwrap();
                            while queue.ready.is_empty() {
                                if queue.closed && queue.jobs == 0 {
                                    return;
                                }
                                queue = shared.ready.wait(queue).unwrap();
                            }
                            let Some(job) = queue.ready.pop_front() else {
                                break;
                            };
                            job
                        };
                        match job {
                            FuseJob::Owned(job) => {
                                if std::panic::catch_unwind(std::panic::AssertUnwindSafe(job))
                                    .is_err()
                                {
                                    afs_logging::error!("FUSE worker callback panicked");
                                }
                                Self::finish(&shared, key);
                            }
                            #[cfg(any(feature = "dfs", test))]
                            FuseJob::Deferred(job) => {
                                Self::start_deferred(&shared, key, job);
                            }

                            FuseJob::Borrowed(turn) => {
                                let (started, wake) = &*turn;
                                *started.lock().expect("borrowed FUSE turn mutex poisoned") = true;
                                wake.notify_one();
                                // The receiver retires this slot. Do not occupy a
                                // worker while it copies or executes a sync trigger.
                            }
                        }
                    }
                })
                .expect("FUSE worker thread must start");
        }
        Self { shared, capacity }
    }

    #[cfg(any(feature = "dfs", test))]
    fn start_deferred(
        shared: &Arc<FuseDispatchShared>,
        turn: Option<FuseTurn>,
        starter: impl FnOnce(std::result::Result<FuseCompletion, i32>),
    ) {
        let completion = FuseCompletion {
            inner: Arc::new(FuseCompletionInner {
                shared: shared.clone(),
                turn,
            }),
        };
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| starter(Ok(completion))))
            .is_err()
        {
            afs_logging::error!("FUSE deferred callback panicked");
        }
    }

    pub(super) fn submit(&self, job: impl FnOnce() + Send + 'static) {
        let mut queue = self.shared.queue.lock().unwrap();
        while queue.jobs >= self.capacity && !queue.closed {
            queue = self.shared.capacity.wait(queue).unwrap();
        }
        if queue.closed {
            drop(queue);
            // Not admitted: drop the captured fuser reply to send EIO. Running
            // backend work here would escape the completed shutdown drain.
            drop(job);
            return;
        }
        queue.jobs += 1;
        #[cfg(feature = "dfs")]
        if let Some(metrics) = self.shared.activity.get() {
            metrics.observe_count(
                crate::node::read::activity::Activity::FuseTask,
                queue.jobs as u64,
            );
        }
        queue.ready.push_back((None, FuseJob::Owned(Box::new(job))));
        self.shared.ready.notify_one();
    }

    pub(super) fn copy_write_input(&self, data: &[u8]) -> std::result::Result<FuseWriteInput, i32> {
        if data.len() > MAX_OWNED_WRITE_INPUT_BYTES {
            return Err(libc::E2BIG);
        }
        let mut queue = self.shared.queue.lock().map_err(|_| libc::EIO)?;
        while data.len() > MAX_OWNED_WRITE_INPUT_BYTES - queue.write_input_bytes && !queue.closed {
            queue = self.shared.capacity.wait(queue).map_err(|_| libc::EIO)?;
        }
        if queue.closed {
            return Err(libc::EIO);
        }
        queue.write_input_bytes += data.len();
        drop(queue);
        Ok(FuseWriteInput {
            data: data.to_vec(),
            shared: self.shared.clone(),
        })
    }

    pub(super) fn submit_keyed(&self, key: u64, job: impl FnOnce() + Send + 'static) {
        self.submit_access(key, FuseAccess::Exclusive, job);
    }

    pub(super) fn submit_read_keyed(&self, key: u64, job: impl FnOnce() + Send + 'static) {
        self.submit_access(key, FuseAccess::Read, job);
    }

    #[cfg(test)]
    pub(super) fn submit_read_deferred(
        &self,
        key: u64,
        job: impl FnOnce(FuseCompletion) + Send + 'static,
    ) {
        self.submit_job(
            key,
            FuseAccess::Read,
            FuseJob::Deferred(Box::new(move |result| {
                if let Ok(completion) = result {
                    job(completion);
                }
            })),
        );
    }

    pub(super) fn submit_access(
        &self,
        key: u64,
        access: FuseAccess,
        job: impl FnOnce() + Send + 'static,
    ) {
        self.submit_job(key, access, FuseJob::Owned(Box::new(job)));
    }

    #[cfg(feature = "dfs")]
    pub(super) fn submit_read_deferred_until(
        &self,
        key: u64,
        deadline: std::time::Instant,
        job: impl FnOnce(std::result::Result<FuseCompletion, i32>) + Send + 'static,
    ) {
        if let Err((error, job)) = self.submit_job_until(
            key,
            FuseAccess::Read,
            FuseJob::Deferred(Box::new(job)),
            Some(deadline),
        ) && let FuseJob::Deferred(job) = job
        {
            job(Err(error));
        }
    }

    fn submit_job(&self, key: u64, access: FuseAccess, job: FuseJob) {
        if let Err((_, job)) = self.submit_job_until(key, access, job, None) {
            drop(job);
        }
    }

    fn submit_job_until(
        &self,
        key: u64,
        access: FuseAccess,
        job: FuseJob,
        deadline: Option<std::time::Instant>,
    ) -> std::result::Result<(), (i32, FuseJob)> {
        let mut queue = match self.shared.queue.lock() {
            Ok(queue) => queue,
            Err(_) => return Err((libc::EIO, job)),
        };
        while queue.jobs >= self.capacity && !queue.closed {
            queue = if let Some(deadline) = deadline {
                let remaining = deadline.saturating_duration_since(std::time::Instant::now());
                if remaining.is_zero() {
                    return Err((libc::ETIMEDOUT, job));
                }
                match self.shared.capacity.wait_timeout(queue, remaining) {
                    Ok((queue, _)) => queue,
                    Err(_) => return Err((libc::EIO, job)),
                }
            } else {
                match self.shared.capacity.wait(queue) {
                    Ok(queue) => queue,
                    Err(_) => return Err((libc::EIO, job)),
                }
            };
        }
        if queue.closed {
            drop(queue);
            return Err((libc::EIO, job));
        }
        if deadline.is_some_and(|value| std::time::Instant::now() >= value) {
            return Err((libc::ETIMEDOUT, job));
        }
        queue.jobs += 1;
        #[cfg(feature = "dfs")]
        if let Some(metrics) = self.shared.activity.get() {
            metrics.observe_count(
                crate::node::read::activity::Activity::FuseTask,
                queue.jobs as u64,
            );
        }
        // Readers can join only before an exclusive turn has queued. Counting
        // ready readers as active preserves the barrier before they execute.
        let join_reads = access == FuseAccess::Read
            && !queue.waiting.contains_key(&key)
            && matches!(queue.active.get(&key), Some(FuseActive::Readers(_)));
        if !queue.active.contains_key(&key) || join_reads {
            match queue.active.entry(key) {
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(match access {
                        FuseAccess::Exclusive => FuseActive::Exclusive,
                        FuseAccess::Read => FuseActive::Readers(1),
                    });
                }
                std::collections::hash_map::Entry::Occupied(mut entry) => {
                    let FuseActive::Readers(readers) = entry.get_mut() else {
                        unreachable!("only a read batch can be joined");
                    };
                    *readers += 1;
                }
            }
            let turn = Some(FuseTurn { key, access });
            #[cfg(any(feature = "dfs", test))]
            let job = match job {
                FuseJob::Deferred(starter) => {
                    // Already eligible at admission: starting the owned future
                    // needs no worker hop. Keep its counted task/order turn,
                    // and release the queue lock before a starter can finish.
                    drop(queue);
                    Self::start_deferred(&self.shared, turn, starter);
                    return Ok(());
                }
                job => job,
            };
            queue.ready.push_back((turn, job));
            self.shared.ready.notify_one();
        } else {
            queue
                .waiting
                .entry(key)
                .or_default()
                .push_back((access, job));
        }
        Ok(())
    }

    fn finish(shared: &FuseDispatchShared, turn: Option<FuseTurn>) {
        let mut queue = shared
            .queue
            .lock()
            .expect("FUSE queue mutex poisoned at completion");
        queue.jobs -= 1;
        #[cfg(feature = "dfs")]
        if let Some(metrics) = shared.activity.get() {
            metrics.observe_count(
                crate::node::read::activity::Activity::FuseTask,
                queue.jobs as u64,
            );
        }
        if let Some(FuseTurn { key, access }) = turn {
            let finished = match (access, queue.active.get_mut(&key)) {
                (FuseAccess::Read, Some(FuseActive::Readers(readers))) => {
                    *readers -= 1;
                    *readers == 0
                }
                (FuseAccess::Exclusive, Some(FuseActive::Exclusive)) => true,
                _ => unreachable!("completed FUSE turn must own the active access"),
            };
            if finished {
                queue.active.remove(&key);
                if let Some(mut waiting) = queue.waiting.remove(&key) {
                    let mut readers = 0;
                    while let Some((next_access, _)) = waiting.front() {
                        if readers != 0 && *next_access == FuseAccess::Exclusive {
                            break;
                        }
                        let (next_access, next) = waiting.pop_front().unwrap();
                        queue.ready.push_back((
                            Some(FuseTurn {
                                key,
                                access: next_access,
                            }),
                            next,
                        ));
                        shared.ready.notify_one();
                        if next_access == FuseAccess::Exclusive {
                            queue.active.insert(key, FuseActive::Exclusive);
                            break;
                        }
                        readers += 1;
                    }
                    if readers != 0 {
                        queue.active.insert(key, FuseActive::Readers(readers));
                    }
                    if !waiting.is_empty() {
                        queue.waiting.insert(key, waiting);
                    }
                }
            }
        }
        shared.capacity.notify_one();
        shared.drained.notify_all();
        if queue.jobs == 0 {
            shared.drained.notify_all();
            if queue.closed {
                shared.ready.notify_all();
            }
        }
    }

    /// Wait for this handle's FIFO turn, then execute without owning or copying
    /// its input. Only a bounded wakeup marker crosses to a worker. This blocks
    /// the receive callback, but holds no queue/state lock during backend I/O.
    pub(super) fn run_borrowed_keyed(&self, key: u64, job: impl FnOnce()) {
        let mut queue = self
            .shared
            .queue
            .lock()
            .expect("FUSE queue mutex poisoned at admission");
        while queue.jobs >= self.capacity && !queue.closed {
            queue = self
                .shared
                .capacity
                .wait(queue)
                .expect("FUSE admission wait poisoned");
        }
        if queue.closed {
            drop(queue);
            drop(job);
            return;
        }
        queue.jobs += 1;
        #[cfg(feature = "dfs")]
        if let Some(metrics) = self.shared.activity.get() {
            metrics.observe_count(
                crate::node::read::activity::Activity::FuseTask,
                queue.jobs as u64,
            );
        }
        if let std::collections::hash_map::Entry::Vacant(entry) = queue.active.entry(key) {
            entry.insert(FuseActive::Exclusive);
            drop(queue);
            let admission = BorrowedAdmission {
                dispatch: self,
                key,
            };
            job();
            drop(admission);
            return;
        }
        let turn = Arc::new((Mutex::new(false), Condvar::new()));
        queue
            .waiting
            .entry(key)
            .or_default()
            .push_back((FuseAccess::Exclusive, FuseJob::Borrowed(turn.clone())));
        drop(queue);
        let admission = BorrowedAdmission {
            dispatch: self,
            key,
        };
        let (started, wake) = &*turn;
        let mut started = started.lock().expect("borrowed FUSE turn mutex poisoned");
        while !*started {
            started = wake
                .wait(started)
                .expect("borrowed FUSE turn wait poisoned");
        }
        drop(started);
        job();
        drop(admission);
    }

    pub(super) fn drain(&self) {
        let mut state = self
            .shared
            .queue
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while state.jobs != 0 {
            state = self
                .shared
                .drained
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }

    pub(super) fn drain_key(&self, key: u64) {
        let mut state = self
            .shared
            .queue
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while state.active.contains_key(&key) || state.waiting.contains_key(&key) {
            state = self
                .shared
                .drained
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }

    pub(super) fn close_and_drain(&self) {
        let mut state = self.shared.queue.lock().unwrap();
        state.closed = true;
        self.shared.capacity.notify_all();
        self.shared.ready.notify_all();
        // Already admitted workers and borrowed receiver turns retain callbacks.
        // Closing rejects capacity waiters; it never executes them out of order.
        // The Node process deadline covers a blocked accepted callback.
        while state.jobs != 0 {
            state = self.shared.drained.wait(state).unwrap();
        }
    }
}

impl Drop for FuseDispatch {
    fn drop(&mut self) {
        self.close_and_drain();
    }
}
