//! Bounded look-ahead for confirmed, explicitly direct, single-stream reads.
//! Ordinary replies never enter the retained-result map.
use super::super::{
    read::admission::{ReadLifetime, ReadReplyFuture},
    read_work::ReadWorkMetrics,
};
use afs_error::{Error, Result};
use prost::bytes::Bytes;
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Instant,
};

pub(crate) const UNIT: usize = 1024 * 1024;
const START: u64 = 32 * UNIT as u64;
const INITIAL_WINDOW: u64 = 4 * UNIT as u64;
const MAX_WINDOW: u64 = 16 * UNIT as u64;
pub(crate) type Fetch =
    Arc<dyn Fn(u64, usize, Instant, ReadLifetime) -> ReadReplyFuture + Send + Sync>;
fn stopped() -> Error {
    Error::coded(afs_error::IO_UNAVAILABLE, "native read-ahead retired")
}

pub(crate) fn eligible(flags: i32) -> bool {
    flags & libc::O_ACCMODE == libc::O_RDONLY && flags & libc::O_DIRECT != 0
}
#[derive(Default)]
struct Progress {
    cursor: Option<u64>,
    advanced: u64,
    window: u64,
    used_since_growth: u64,
}
impl Progress {
    fn record(&mut self, start: u64, end: u64) {
        if self.cursor.is_some_and(|p| p != start) {
            *self = Self::default();
        }
        self.advanced = self.advanced.saturating_add(end - start);
        self.cursor = Some(end);
        if self.window == 0 && self.advanced >= START {
            self.window = INITIAL_WINDOW;
        }
    }
    fn consumed(&mut self, bytes: usize, limit: u64) {
        self.used_since_growth = self.used_since_growth.saturating_add(bytes as u64);
        if self.window > 0 && self.used_since_growth >= self.window {
            self.window = (self.window * 2).min(limit);
            self.used_since_growth = 0;
        }
    }
}
/// Single-buffer replies allocate no collection. Only a shape change needs parts.
pub(crate) struct Reply {
    first: Bytes,
    rest: Vec<Bytes>,
}
impl Reply {
    fn one(bytes: Bytes) -> Self {
        Self {
            first: bytes,
            rest: Vec::new(),
        }
    }
    fn push(&mut self, bytes: Bytes) {
        self.rest.push(bytes);
    }
    pub(crate) fn len(&self) -> usize {
        1 + self.rest.len()
    }
    pub(crate) fn into_parts(self) -> impl Iterator<Item = Bytes> {
        std::iter::once(self.first).chain(self.rest)
    }
}
#[cfg(test)]
impl std::ops::Index<usize> for Reply {
    type Output = Bytes;
    fn index(&self, index: usize) -> &Bytes {
        if index == 0 {
            &self.first
        } else {
            &self.rest[index - 1]
        }
    }
}
struct Metrics {
    events: [(
        &'static str,
        afs_metrics::IntCounter,
        afs_metrics::IntCounter,
    ); 11],
    total: afs_metrics::IntGauge,
    active: afs_metrics::IntGauge,
    peak: afs_metrics::IntGauge,
}
impl Metrics {
    fn new(m: ReadWorkMetrics, limit: usize) -> Self {
        let events = [
            "released",
            "ready",
            "unused_ready",
            "cancel_requested",
            "failed_or_cancelled",
            "skip_budget",
            "scheduled",
            "jump",
            "demand_retry",
            "hit_ready",
            "joined_inflight",
        ]
        .map(|event| {
            let (count, bytes) = m.read_buffer_counters("b_prefetch", event);
            (event, count, bytes)
        });
        m.read_buffer_gauge("b_prefetch", "limit").set(limit as i64);
        m.read_buffer_gauge("b_prefetch", "idle").set(0);
        Self {
            events,
            total: m.read_buffer_gauge("b_prefetch", "total"),
            active: m.read_buffer_gauge("b_prefetch", "active"),
            peak: m.read_buffer_gauge("b_prefetch", "peak"),
        }
    }
}
pub(crate) struct Budget {
    used: AtomicUsize,
    peak: AtomicUsize,
    limit: usize,
    metrics: Option<Metrics>,
    pub(crate) assembly: Arc<tokio::sync::Mutex<()>>,
}
impl Budget {
    pub(crate) fn new(limit: usize, metrics: Option<ReadWorkMetrics>) -> Arc<Self> {
        Arc::new(Self {
            used: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            limit,
            metrics: metrics.map(|m| Metrics::new(m, limit)),
            assembly: Arc::new(tokio::sync::Mutex::new(())),
        })
    }
    fn event(&self, event: &'static str, bytes: usize) {
        if let Some(m) = &self.metrics
            && let Some((_, count, size)) = m.events.iter().find(|(name, _, _)| *name == event)
        {
            count.inc();
            size.inc_by(bytes as u64);
        }
    }
    fn observe(&self) {
        if let Some(m) = &self.metrics {
            let used = self.used.load(Ordering::Acquire) as i64;
            m.total.set(used);
            m.active.set(used);
            m.peak.set(self.peak.load(Ordering::Acquire) as i64);
        }
    }
    fn reserve(self: &Arc<Self>, handle: &Arc<AtomicUsize>, charge: usize) -> Option<Arc<Lease>> {
        let add = |counter: &AtomicUsize, limit: usize| {
            counter
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                    n.checked_add(charge).filter(|&v| v <= limit)
                })
                .is_ok()
        };
        if !add(handle, MAX_WINDOW as usize) {
            return None;
        }
        let previous = match self
            .used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                n.checked_add(charge).filter(|&v| v <= self.limit)
            }) {
            Ok(previous) => previous,
            Err(_) => {
                handle.fetch_sub(charge, Ordering::AcqRel);
                return None;
            }
        };
        self.peak.fetch_max(previous + charge, Ordering::AcqRel);
        self.observe();
        Some(Arc::new(Lease {
            budget: self.clone(),
            handle: handle.clone(),
            charge,
        }))
    }
}
struct Lease {
    budget: Arc<Budget>,
    handle: Arc<AtomicUsize>,
    charge: usize,
}
impl Drop for Lease {
    fn drop(&mut self) {
        self.handle.fetch_sub(self.charge, Ordering::AcqRel);
        self.budget.used.fetch_sub(self.charge, Ordering::AcqRel);
        self.budget.event("released", self.charge);
        self.budget.observe();
    }
}
struct Retained {
    bytes: Bytes,
    _lease: Arc<Lease>,
}
impl AsRef<[u8]> for Retained {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

struct Entry {
    start: u64,
    end: u64,
    demanded: AtomicBool,
    used: Mutex<Vec<(u64, u64)>>,
    cancelled: AtomicBool,
    cancel: tokio::sync::Notify,
    result: Mutex<Option<Result<Bytes>>>,
    done: tokio::sync::Notify,
    budget: Arc<Budget>,
}
impl Entry {
    fn new(start: u64, end: u64, budget: Arc<Budget>) -> Arc<Self> {
        Arc::new(Self {
            start,
            end,
            demanded: AtomicBool::new(false),
            used: Mutex::new(Vec::new()),
            cancelled: AtomicBool::new(false),
            cancel: tokio::sync::Notify::new(),
            result: Mutex::new(None),
            done: tokio::sync::Notify::new(),
            budget,
        })
    }
    fn used(&self, start: u64, end: u64) -> bool {
        if let Ok(mut spans) = self.used.lock() {
            spans.push((start, end));
            spans.sort_unstable();
            let mut n = 0usize;
            for i in 0..spans.len() {
                let (a, b) = spans[i];
                if n > 0 && a <= spans[n - 1].1 {
                    spans[n - 1].1 = spans[n - 1].1.max(b);
                } else {
                    spans[n] = (a, b);
                    n += 1;
                }
            }
            spans.truncate(n);
            return n > 128
                || spans.iter().map(|(a, b)| b - a).sum::<u64>() == self.end - self.start;
        }
        true
    }
    fn cancel_unused(&self) {
        if !self.demanded.load(Ordering::Acquire) {
            self.cancelled.store(true, Ordering::Release);
            self.cancel.notify_waiters();
            self.budget
                .event("cancel_requested", (self.end - self.start) as usize);
        }
    }
    async fn wait(&self, deadline: Instant) -> Result<Bytes> {
        loop {
            let notify = self.done.notified();
            tokio::pin!(notify);
            notify.as_mut().enable();
            if let Some(r) = self.result.lock().map_err(|_| stopped())?.as_ref() {
                return r.clone();
            }
            tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), notify)
                .await
                .map_err(|_| stopped())?;
        }
    }
}
impl Drop for Entry {
    fn drop(&mut self) {
        if let Ok(result) = self.result.lock()
            && let Some(Ok(bytes)) = result.as_ref()
            && let Ok(spans) = self.used.lock()
        {
            let used: u64 = spans.iter().map(|(a, b)| b - a).sum();
            let unused = bytes.len().saturating_sub(used as usize);
            if unused > 0 {
                self.budget.event("unused_ready", unused);
            }
        }
    }
}
#[derive(Default)]
struct State {
    observed: Progress,
    entries: BTreeMap<u64, Arc<Entry>>,
    generation: u64,
    active: usize,
    paused: usize,
    closed: bool,
}
impl State {
    fn reset_progress(&mut self) {
        self.observed = Progress::default();
        self.generation = self.generation.saturating_add(1);
    }
    fn clear(&mut self) {
        for e in self.entries.values() {
            e.cancel_unused();
        }
        self.entries.clear();
        self.reset_progress();
    }
    fn covering(&self, offset: u64) -> Option<Arc<Entry>> {
        self.entries
            .range(..=offset)
            .next_back()
            .filter(|(_, e)| e.end > offset)
            .map(|(_, e)| e.clone())
    }
}
pub(crate) struct Controller {
    state: Mutex<State>,
    charged: Arc<AtomicUsize>,
    budget: Arc<Budget>,
    limit: u64,
    fetch: OnceLock<Fetch>,
}
/// Cancellation releases progress ownership without retaining an ordinary body.
struct Turn<'a> {
    controller: &'a Controller,
    generation: u64,
    sequential: bool,
    completed: bool,
}
impl Drop for Turn<'_> {
    fn drop(&mut self) {
        if let Ok(mut s) = self.controller.state.lock() {
            s.active = s.active.saturating_sub(1);
            if !self.completed && s.generation == self.generation {
                s.clear();
            }
        }
    }
}
pub(crate) struct Pause<'a> {
    controller: &'a Controller,
}
impl Drop for Pause<'_> {
    fn drop(&mut self) {
        if let Ok(mut s) = self.controller.state.lock() {
            s.paused = s.paused.saturating_sub(1);
        }
    }
}
impl Controller {
    pub(crate) fn new(budget: Arc<Budget>, limit: u64) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State::default()),
            charged: Arc::new(AtomicUsize::new(0)),
            budget,
            limit: limit.min(MAX_WINDOW),
            fetch: OnceLock::new(),
        })
    }
    pub(crate) fn fetch(&self, init: impl FnOnce() -> Fetch) -> Fetch {
        self.fetch.get_or_init(init).clone()
    }
    pub(crate) fn retire(&self) {
        if let Ok(mut s) = self.state.lock() {
            s.clear();
            s.closed = true;
        }
    }
    pub(crate) fn pause(&self, offset: u64, count: usize) -> Result<Pause<'_>> {
        let end = offset.checked_add(count as u64).ok_or_else(stopped)?;
        let mut s = self.state.lock().map_err(|_| stopped())?;
        s.paused += 1;
        s.reset_progress();
        s.entries.retain(|_, e| {
            let keep = e.start < end && e.end > offset;
            if !keep {
                e.cancel_unused();
            }
            keep
        });
        Ok(Pause { controller: self })
    }
    pub(crate) fn overlaps(&self, offset: u64, count: usize) -> Result<bool> {
        let end = offset.checked_add(count as u64).ok_or_else(stopped)?;
        let s = self.state.lock().map_err(|_| stopped())?;
        Ok(s.entries.values().any(|e| e.start < end && e.end > offset))
    }
    async fn job(
        job: Option<(Arc<Entry>, Arc<Lease>)>,
        fetch: Fetch,
        deadline: Instant,
        lifetime: ReadLifetime,
    ) {
        let Some((e, lease)) = job else {
            return;
        };
        let requested = (e.end - e.start) as usize;
        let child_lifetime: ReadLifetime = Arc::new((lifetime, lease.clone()));
        let work = async {
            let bytes = fetch(e.start, requested, deadline, child_lifetime).await?;
            if bytes.len() != requested {
                return Err(stopped());
            }
            e.budget.event("ready", bytes.len());
            Ok(Bytes::from_owner(Retained {
                bytes,
                _lease: lease,
            }))
        };
        let cancelled = async {
            loop {
                let n = e.cancel.notified();
                tokio::pin!(n);
                n.as_mut().enable();
                if e.cancelled.load(Ordering::Acquire) {
                    return;
                }
                n.await;
            }
        };
        let result = tokio::select! { r=work=>r, ()=cancelled=>Err(stopped()) };
        if result.is_err() {
            e.budget.event("failed_or_cancelled", requested);
        }
        if let Ok(mut slot) = e.result.lock() {
            *slot = Some(result);
        }
        e.done.notify_waiters();
    }
    fn replenish(
        &self,
        generation: u64,
        length: u64,
        deadline: Instant,
        fetch: Fetch,
        lifetime: ReadLifetime,
    ) {
        let mut jobs = Vec::new();
        if let Ok(mut s) = self.state.try_lock() {
            if s.closed || s.generation != generation || s.active != 1 || s.paused != 0 {
                return;
            }
            let window = s
                .observed
                .window
                .min(self.limit)
                .min(self.budget.limit as u64);
            if window == 0 {
                return;
            }
            let outstanding: u64 = s.entries.values().map(|e| e.end - e.start).sum();
            // Refill at a low watermark, not on each foreground completion.
            if outstanding > window / 2 {
                return;
            }
            let mut cursor = s.observed.cursor.unwrap_or(0);
            let end = cursor.saturating_add(window).min(length);
            while cursor < end {
                if let Some(e) = s.covering(cursor) {
                    cursor = e.end;
                    continue;
                }
                let stop = cursor.saturating_add(UNIT as u64).min(end);
                let Some(lease) = self.budget.reserve(&self.charged, UNIT) else {
                    self.budget.event("skip_budget", 0);
                    break;
                };
                let e = Entry::new(cursor, stop, self.budget.clone());
                s.entries.insert(cursor, e.clone());
                jobs.push((e, lease));
                cursor = stop;
            }
        }
        for (e, _) in &jobs {
            self.budget.event("scheduled", (e.end - e.start) as usize);
        }
        let mut jobs = jobs.into_iter();
        loop {
            let Some(a) = jobs.next() else {
                break;
            };
            let (b, c, d) = (jobs.next(), jobs.next(), jobs.next());
            let (fetch, life) = (fetch.clone(), lifetime.clone());
            // Four existing native futures are polled together, retaining RPC aggregation.
            tokio::spawn(async move {
                tokio::join!(
                    Self::job(Some(a), fetch.clone(), deadline, life.clone()),
                    Self::job(b, fetch.clone(), deadline, life.clone()),
                    Self::job(c, fetch.clone(), deadline, life.clone()),
                    Self::job(d, fetch, deadline, life)
                );
            });
        }
    }
    pub(crate) async fn read(
        &self,
        offset: u64,
        count: usize,
        length: u64,
        deadline: Instant,
        fetch: Fetch,
        lifetime: ReadLifetime,
    ) -> Result<Reply> {
        let end = offset
            .checked_add(count as u64)
            .filter(|&v| count > 0 && v <= length)
            .ok_or_else(stopped)?;
        let (mut turn, plan) = {
            let mut s = self.state.lock().map_err(|_| stopped())?;
            if s.closed {
                return Err(stopped());
            }
            if count > UNIT {
                // Internal large reads use one final allocation. Keeping old
                // parts while admitting another full reply can make the two
                // owners wait on each other's byte credits until timeout.
                // Cancelling speculation never releases real child ownership.
                s.clear();
            }
            let sequential = count == UNIT
                && offset.is_multiple_of(UNIT as u64)
                && s.active == 0
                && s.observed.cursor.is_none_or(|p| p == offset);
            if !sequential {
                if s.observed.advanced != 0 || !s.entries.is_empty() {
                    self.budget.event("jump", 0);
                }
                s.reset_progress();
            }
            // Claim all overlapping flights before cancelling unrelated look-ahead.
            for e in s
                .entries
                .values()
                .filter(|e| e.start < end && e.end > offset)
            {
                e.demanded.store(true, Ordering::Release);
            }
            if !sequential {
                s.entries.retain(|_, e| {
                    let keep = e.start < end && e.end > offset;
                    if !keep {
                        e.cancel_unused();
                    }
                    keep
                });
            }
            let mut plan = Vec::new();
            if !s.entries.is_empty() {
                let mut cursor = offset;
                while cursor < end {
                    let entry = s.covering(cursor);
                    let stop = entry.as_ref().map_or_else(
                        || {
                            s.entries
                                .range(cursor..)
                                .next()
                                .map_or(end, |(&a, _)| a.min(end))
                        },
                        |e| e.end.min(end),
                    );
                    plan.push((entry, cursor, stop));
                    cursor = stop;
                }
            }
            s.active += 1;
            (
                Turn {
                    controller: self,
                    generation: s.generation,
                    sequential,
                    completed: false,
                },
                plan,
            )
        };
        let mut used = 0;
        let output = if plan.is_empty() {
            // Original native future: no result registry, spawned task or Notify.
            Reply::one(fetch(offset, count, deadline, lifetime.clone()).await?)
        } else {
            let mut reply = None;
            for (entry, cursor, stop) in plan {
                let bytes = if let Some(e) = entry {
                    let ready = e.result.lock().map_err(|_| stopped())?.is_some();
                    let (bytes, remove) = match e.wait(deadline).await {
                        Ok(bytes) => {
                            let remove = e.used(cursor, stop);
                            (
                                bytes.slice((cursor - e.start) as usize..(stop - e.start) as usize),
                                remove,
                            )
                        }
                        Err(_) => {
                            self.budget.event("demand_retry", (stop - cursor) as usize);
                            (
                                fetch(cursor, (stop - cursor) as usize, deadline, lifetime.clone())
                                    .await?,
                                true,
                            )
                        }
                    };
                    used += (stop - cursor) as usize;
                    self.budget.event(
                        if ready {
                            "hit_ready"
                        } else {
                            "joined_inflight"
                        },
                        (stop - cursor) as usize,
                    );
                    let mut s = self.state.lock().map_err(|_| stopped())?;
                    if remove
                        && s.entries
                            .get(&e.start)
                            .is_some_and(|saved| Arc::ptr_eq(saved, &e))
                    {
                        s.entries.remove(&e.start);
                    }
                    bytes
                } else {
                    fetch(cursor, (stop - cursor) as usize, deadline, lifetime.clone()).await?
                };
                if let Some(r) = &mut reply {
                    Reply::push(r, bytes);
                } else {
                    reply = Some(Reply::one(bytes));
                }
            }
            reply.ok_or_else(stopped)?
        };
        {
            let mut s = self.state.lock().map_err(|_| stopped())?;
            if s.generation == turn.generation && !s.closed && turn.sequential {
                s.observed.record(offset, end);
                s.observed.consumed(used, self.limit);
            }
        }
        if turn.sequential {
            self.replenish(turn.generation, length, deadline, fetch, lifetime);
        }
        turn.completed = true;
        Ok(output)
    }
}
impl Drop for Controller {
    fn drop(&mut self) {
        self.retire();
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn read_config_large_read_discards_speculation_and_returns_one_owner() {
        struct Body {
            data: Vec<u8>,
            _permit: tokio::sync::OwnedSemaphorePermit,
        }
        impl AsRef<[u8]> for Body {
            fn as_ref(&self) -> &[u8] {
                &self.data
            }
        }
        let slots = Arc::new(tokio::sync::Semaphore::new(16 * UNIT));
        let budget = Budget::new(8 * UNIT, None);
        let c = Controller::new(budget.clone(), MAX_WINDOW);
        for i in 0..8 {
            let permit = slots.clone().acquire_many_owned(UNIT as u32).await.unwrap();
            let lease = budget.reserve(&c.charged, UNIT).unwrap();
            let e = Entry::new(i * UNIT as u64, (i + 1) * UNIT as u64, budget.clone());
            *e.result.lock().unwrap() = Some(Ok(Bytes::from_owner(Retained {
                bytes: Bytes::from_owner(Body {
                    data: vec![7; UNIT],
                    _permit: permit,
                }),
                _lease: lease,
            })));
            c.state.lock().unwrap().entries.insert(e.start, e);
        }
        // Replay the old caller's order: the final 12MiB destination cannot
        // be admitted while 8MiB of source parts remain owned in a 16MiB pool.
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(20),
                slots.clone().acquire_many_owned((12 * UNIT) as u32)
            )
            .await
            .is_err()
        );
        let a = slots.clone();
        let fetch: Fetch = Arc::new(move |_, len, deadline, _| {
            let a = a.clone();
            Box::pin(async move {
                let permit = tokio::time::timeout_at(
                    tokio::time::Instant::from_std(deadline),
                    a.acquire_many_owned(len.max(UNIT) as u32),
                )
                .await
                .map_err(|_| stopped())?
                .map_err(|_| stopped())?;
                Ok(Bytes::from_owner(Body {
                    data: vec![7; len],
                    _permit: permit,
                }))
            })
        });
        let reply = c
            .read(
                0,
                12 * UNIT,
                64 * UNIT as u64,
                Instant::now() + std::time::Duration::from_millis(250),
                fetch,
                Arc::new(()),
            )
            .await
            .unwrap();
        assert_eq!(
            reply.len(),
            1,
            "large reads must not need a second full-size assembly buffer"
        );
        assert_eq!(reply[0].len(), 12 * UNIT);
        assert!(reply[0].iter().all(|b| *b == 7));
        assert_eq!(budget.used.load(Ordering::Acquire), 0);
        drop(reply);
        assert_eq!(slots.available_permits(), 16 * UNIT);
    }

    #[test]
    fn native_prefetch_fix_waits_for_thirty_two_mib() {
        let mut p = Progress::default();
        for n in 0..31 {
            p.record(n * UNIT as u64, (n + 1) * UNIT as u64);
            assert_eq!(p.window, 0, "ordinary reads must not activate early");
        }
        p.record(31 * UNIT as u64, 32 * UNIT as u64);
        assert!(p.window > 0);
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn native_prefetch_fix_local_holes_release_ordinary_reply_credits() {
        struct Reply {
            data: Vec<u8>,
            _permit: tokio::sync::OwnedSemaphorePermit,
        }
        impl AsRef<[u8]> for Reply {
            fn as_ref(&self) -> &[u8] {
                &self.data
            }
        }
        let admission = Arc::new(tokio::sync::Semaphore::new(6 * UNIT));
        let a = admission.clone();
        let fetch: Fetch = Arc::new(move |_, len, _deadline, _| {
            let a = a.clone();
            Box::pin(async move {
                let permit = tokio::time::timeout(
                    std::time::Duration::from_millis(100),
                    a.acquire_many_owned(UNIT as u32),
                )
                .await
                .map_err(|_| stopped())?
                .map_err(|_| stopped())?;
                Ok(Bytes::from_owner(Reply {
                    data: vec![3; len],
                    _permit: permit,
                }))
            })
        });
        let c = Controller::new(Budget::new(8 * UNIT, None), MAX_WINDOW);
        for n in 0..32 {
            let result = c
                .read(
                    n * 65536,
                    4096,
                    64 * UNIT as u64,
                    Instant::now() + std::time::Duration::from_secs(2),
                    fetch.clone(),
                    Arc::new(()),
                )
                .await;
            assert!(
                result.is_ok(),
                "completed replies must not stay behind a hole: {n}"
            );
            drop(result);
            assert_eq!(
                admission.available_permits(),
                6 * UNIT,
                "reply credits retained at {n}"
            );
        }
    }
    #[test]
    fn native_prefetch_mode_gate_is_explicit_readonly_direct() {
        assert!(!eligible(libc::O_RDONLY));
        assert!(eligible(libc::O_RDONLY | libc::O_DIRECT));
        assert!(!eligible(libc::O_RDWR | libc::O_DIRECT));
        assert!(!eligible(libc::O_WRONLY | libc::O_DIRECT));
    }
    #[test]
    fn native_prefetch_growth_requires_a_window_of_consumption() {
        let mut p = Progress::default();
        p.record(0, START);
        assert_eq!(p.window, INITIAL_WINDOW);
        p.consumed(UNIT, MAX_WINDOW);
        assert_eq!(p.window, INITIAL_WINDOW);
        p.consumed(3 * UNIT, MAX_WINDOW);
        assert_eq!(p.window, 2 * INITIAL_WINDOW);
    }
    #[test]
    fn native_prefetch_lease_survives_cache_eviction_and_last_reply_reference() {
        let b = Budget::new(UNIT, None);
        let h = Arc::new(AtomicUsize::new(0));
        let lease = b.reserve(&h, UNIT).unwrap();
        assert!(b.reserve(&h, UNIT).is_none());
        let bytes = Bytes::from_owner(Retained {
            bytes: Bytes::from(vec![7; UNIT]),
            _lease: lease.clone(),
        });
        drop(lease);
        let slice = bytes.slice(..1);
        drop(bytes);
        assert_eq!(b.used.load(Ordering::Acquire), UNIT);
        drop(slice);
        assert_eq!(b.used.load(Ordering::Acquire), 0);
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn native_prefetch_shared_flight_partial_reply_and_close_retire() {
        let b = Budget::new(8 * UNIT, None);
        let c = Controller::new(b.clone(), MAX_WINDOW);
        let calls = Arc::new(AtomicUsize::new(0));
        let n = calls.clone();
        let fetch: Fetch = Arc::new(move |offset, len, _deadline, _life| {
            let n = n.clone();
            Box::pin(async move {
                n.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(std::time::Duration::from_millis(2)).await;
                Ok(Bytes::from(vec![(offset / UNIT as u64) as u8; len]))
            })
        });
        for i in 0..32 {
            c.read(
                i * UNIT as u64,
                UNIT,
                632 * UNIT as u64,
                Instant::now() + std::time::Duration::from_secs(2),
                fetch.clone(),
                Arc::new(()),
            )
            .await
            .unwrap();
        }
        let (x, y) = tokio::join!(
            c.read(
                32 * UNIT as u64,
                4096,
                632 * UNIT as u64,
                Instant::now() + std::time::Duration::from_secs(2),
                fetch.clone(),
                Arc::new(())
            ),
            c.read(
                32 * UNIT as u64,
                4096,
                632 * UNIT as u64,
                Instant::now() + std::time::Duration::from_secs(2),
                fetch.clone(),
                Arc::new(())
            )
        );
        assert_eq!(&x.unwrap()[0][..], &[32u8; 4096]);
        assert_eq!(&y.unwrap()[0][..], &[32u8; 4096]);
        assert!(calls.load(Ordering::SeqCst) <= 36);
        c.retire();
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert_eq!(b.used.load(Ordering::Acquire), 0);
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn native_prefetch_eof_random_and_jump_never_run_away() {
        let b = Budget::new(8 * UNIT, None);
        let c = Controller::new(b.clone(), MAX_WINDOW);
        let max = Arc::new(AtomicUsize::new(0));
        let observed = max.clone();
        let fetch: Fetch = Arc::new(move |offset, len, _deadline, _life| {
            let observed = observed.clone();
            Box::pin(async move {
                observed.fetch_max(offset as usize + len, Ordering::SeqCst);
                Ok(Bytes::from(vec![0; len]))
            })
        });
        for i in 0..4 {
            c.read(
                i * UNIT as u64,
                UNIT,
                4 * UNIT as u64,
                Instant::now() + std::time::Duration::from_secs(2),
                fetch.clone(),
                Arc::new(()),
            )
            .await
            .unwrap();
        }
        assert_eq!(max.load(Ordering::SeqCst), 4 * UNIT);
        assert_eq!(b.used.load(Ordering::Acquire), 0);
        for offset in [100 * UNIT as u64, 200 * UNIT as u64, 300 * UNIT as u64] {
            c.read(
                offset,
                4096,
                400 * UNIT as u64,
                Instant::now() + std::time::Duration::from_secs(2),
                fetch.clone(),
                Arc::new(()),
            )
            .await
            .unwrap();
        }
        assert_eq!(b.used.load(Ordering::Acquire), 0);
        c.retire();
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn native_prefetch_kernel_burst_does_not_duplicate_inflight_foreground() {
        let b = Budget::new(8 * UNIT, None);
        let c = Controller::new(b.clone(), MAX_WINDOW);
        let calls = Arc::new(AtomicUsize::new(0));
        let n = calls.clone();
        let fetch: Fetch = Arc::new(move |offset, len, _deadline, _life| {
            let n = n.clone();
            Box::pin(async move {
                n.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(std::time::Duration::from_millis(if offset == 0 {
                    20
                } else {
                    2
                }))
                .await;
                Ok(Bytes::from(vec![0; len]))
            })
        });
        let mut jobs = Vec::new();
        for i in 0..16 {
            let c = c.clone();
            let f = fetch.clone();
            jobs.push(tokio::spawn(async move {
                c.read(
                    i * UNIT as u64,
                    UNIT,
                    16 * UNIT as u64,
                    Instant::now() + std::time::Duration::from_secs(2),
                    f,
                    Arc::new(()),
                )
                .await
                .unwrap()
            }));
        }
        for j in jobs {
            drop(j.await.unwrap());
        }
        assert_eq!(calls.load(Ordering::SeqCst), 16);
        c.retire();
        assert_eq!(b.used.load(Ordering::Acquire), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn native_prefetch_failure_is_retried_by_real_demand() {
        let b = Budget::new(8 * UNIT, None);
        let c = Controller::new(b, MAX_WINDOW);
        let failures = Arc::new(AtomicUsize::new(0));
        let n = failures.clone();
        let fetch: Fetch = Arc::new(move |offset, len, _deadline, _life| {
            let n = n.clone();
            Box::pin(async move {
                if offset == 32 * UNIT as u64 && n.fetch_add(1, Ordering::SeqCst) == 0 {
                    return Err(stopped());
                }
                Ok(Bytes::from(vec![7; len]))
            })
        });
        for i in 0..33 {
            let result = c
                .read(
                    i * UNIT as u64,
                    UNIT,
                    36 * UNIT as u64,
                    Instant::now() + std::time::Duration::from_secs(2),
                    fetch.clone(),
                    Arc::new(()),
                )
                .await
                .unwrap();
            assert_eq!(result[0][0], 7);
        }
        assert_eq!(failures.load(Ordering::SeqCst), 2);
        c.retire();
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn native_prefetch_cancel_keeps_charge_until_real_child_retires() {
        let b = Budget::new(8 * UNIT, None);
        let c = Controller::new(b.clone(), MAX_WINDOW);
        let fetch: Fetch = Arc::new(move |_, len, _deadline, life| {
            Box::pin(async move {
                let (tx, rx) = tokio::sync::oneshot::channel();
                tokio::spawn(async move {
                    let _life = life;
                    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
                    let _ = tx.send(Bytes::from(vec![0; len]));
                });
                rx.await.map_err(|_| stopped())
            })
        });
        for i in 0..32 {
            drop(
                c.read(
                    i * UNIT as u64,
                    UNIT,
                    36 * UNIT as u64,
                    Instant::now() + std::time::Duration::from_secs(2),
                    fetch.clone(),
                    Arc::new(()),
                )
                .await
                .unwrap(),
            );
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        c.retire();
        assert!(b.used.load(Ordering::Acquire) > 0);
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert_eq!(b.used.load(Ordering::Acquire), 0);
    }
    #[tokio::test]
    async fn native_prefetch_fix_ordinary_runs_in_the_callers_task_and_stores_no_body() {
        let caller = tokio::task::try_id();
        let fetch: Fetch = Arc::new(move |_, len, _, _| {
            Box::pin(async move {
                assert_eq!(tokio::task::try_id(), caller);
                Ok(Bytes::from(vec![9; len]))
            })
        });
        let c = Controller::new(Budget::new(8 * UNIT, None), MAX_WINDOW);
        for i in 0..8 {
            drop(
                c.read(
                    i * UNIT as u64,
                    UNIT,
                    64 * UNIT as u64,
                    Instant::now() + std::time::Duration::from_secs(2),
                    fetch.clone(),
                    Arc::new(()),
                )
                .await
                .unwrap(),
            );
            assert!(c.state.lock().unwrap().entries.is_empty());
        }
        assert_eq!(c.charged.load(Ordering::Acquire), 0);
    }
    #[tokio::test]
    async fn native_prefetch_fix_cancelling_ordinary_read_releases_progress_ownership() {
        let fetch: Fetch = Arc::new(move |_, _, _, _| Box::pin(std::future::pending()));
        let c = Controller::new(Budget::new(8 * UNIT, None), MAX_WINDOW);
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(10),
                c.read(
                    0,
                    UNIT,
                    64 * UNIT as u64,
                    Instant::now() + std::time::Duration::from_secs(2),
                    fetch,
                    Arc::new(())
                )
            )
            .await
            .is_err()
        );
        let s = c.state.lock().unwrap();
        assert_eq!(s.active, 0);
        assert!(s.entries.is_empty());
        assert_eq!(s.observed.window, 0);
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn native_prefetch_fix_shape_exit_reuses_overlap_and_fetches_only_gaps() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let calls2 = calls.clone();
        let fetch: Fetch = Arc::new(move |offset, len, _, _| {
            let calls = calls2.clone();
            Box::pin(async move {
                calls.lock().unwrap().push((offset, len));
                Ok(Bytes::from(vec![(offset / UNIT as u64) as u8; len]))
            })
        });
        let b = Budget::new(8 * UNIT, None);
        let c = Controller::new(b.clone(), MAX_WINDOW);
        for i in 0..32 {
            drop(
                c.read(
                    i * UNIT as u64,
                    UNIT,
                    64 * UNIT as u64,
                    Instant::now() + std::time::Duration::from_secs(2),
                    fetch.clone(),
                    Arc::new(()),
                )
                .await
                .unwrap(),
            );
        }
        let offset = 32 * UNIT as u64 - 4096;
        let _assembly = b.assembly.clone().lock_owned().await;
        let _pause = c.pause(offset, 8192).unwrap();
        let r = c
            .read(
                offset,
                8192,
                64 * UNIT as u64,
                Instant::now() + std::time::Duration::from_secs(2),
                fetch,
                Arc::new(()),
            )
            .await
            .unwrap();
        assert_eq!(r.len(), 2);
        assert_eq!(&r[0][..], &[31; 4096]);
        assert_eq!(&r[1][..], &[32; 4096]);
        let calls = calls.lock().unwrap();
        assert_eq!(
            calls.iter().filter(|(a, _)| *a == 32 * UNIT as u64).count(),
            1
        );
        assert!(calls.contains(&(offset, 4096)));
        assert!(!calls.contains(&(offset, 8192)));
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn native_prefetch_fix_eof_slice_keeps_real_lease_until_last_reference() {
        let b = Budget::new(8 * UNIT, None);
        let c = Controller::new(b.clone(), MAX_WINDOW);
        let fetch: Fetch =
            Arc::new(move |_, len, _, _| Box::pin(async move { Ok(Bytes::from(vec![1; len])) }));
        let length = 32 * UNIT as u64 + 4096;
        for i in 0..32 {
            drop(
                c.read(
                    i * UNIT as u64,
                    UNIT,
                    length,
                    Instant::now() + std::time::Duration::from_secs(2),
                    fetch.clone(),
                    Arc::new(()),
                )
                .await
                .unwrap(),
            );
        }
        let bytes = c
            .read(
                32 * UNIT as u64,
                4096,
                length,
                Instant::now() + std::time::Duration::from_secs(2),
                fetch,
                Arc::new(()),
            )
            .await
            .unwrap();
        assert_eq!(bytes[0].len(), 4096);
        c.retire();
        assert_eq!(b.used.load(Ordering::Acquire), UNIT);
        drop(bytes);
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        assert_eq!(b.used.load(Ordering::Acquire), 0);
    }
    #[test]
    fn native_prefetch_fix_fetch_context_is_initialized_once() {
        let c = Controller::new(Budget::new(8 * UNIT, None), MAX_WINDOW);
        let first = c.fetch(|| {
            Arc::new(move |_, len, _, _| Box::pin(async move { Ok(Bytes::from(vec![0; len])) }))
        });
        let second = c.fetch(|| panic!("context must be reused"));
        assert!(Arc::ptr_eq(&first, &second));
    }

    #[test]
    fn native_prefetch_fix_partial_usage_counts_unique_ranges_and_bounds_metadata() {
        let e = Entry::new(0, UNIT as u64, Budget::new(8 * UNIT, None));
        assert!(!e.used(0, 4096));
        assert!(!e.used(0, 4096));
        assert!(!e.used(2048, 8192));
        assert_eq!(e.used.lock().unwrap().as_slice(), &[(0, 8192)]);
        assert!(e.used(8192, UNIT as u64));
        let e = Entry::new(0, UNIT as u64, Budget::new(8 * UNIT, None));
        for i in 0..128 {
            assert!(!e.used(i * 16, i * 16 + 8));
        }
        assert!(
            e.used(128 * 16, 128 * 16 + 8),
            "fragmented entry must stop retaining metadata"
        );
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn native_prefetch_fix_eviction_cannot_lose_a_later_claimed_part() {
        let entered = Arc::new(tokio::sync::Semaphore::new(0));
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let calls = Arc::new(Mutex::new(Vec::new()));
        let (a, b, n) = (entered.clone(), release.clone(), calls.clone());
        let gap = 32 * UNIT as u64 - 4096;
        let fetch: Fetch = Arc::new(move |offset, len, _, _| {
            let (a, b, n) = (a.clone(), b.clone(), n.clone());
            Box::pin(async move {
                n.lock().unwrap().push((offset, len));
                if offset == gap {
                    a.add_permits(1);
                    b.acquire().await.map_err(|_| stopped())?.forget();
                }
                Ok(Bytes::from(vec![5; len]))
            })
        });
        let budget = Budget::new(8 * UNIT, None);
        let c = Controller::new(budget.clone(), MAX_WINDOW);
        for i in 0..32 {
            drop(
                c.read(
                    i * UNIT as u64,
                    UNIT,
                    256 * UNIT as u64,
                    Instant::now() + std::time::Duration::from_secs(2),
                    fetch.clone(),
                    Arc::new(()),
                )
                .await
                .unwrap(),
            );
        }
        let (c2, f) = (c.clone(), fetch.clone());
        let pending = tokio::spawn(async move {
            c2.read(
                gap,
                8192,
                256 * UNIT as u64,
                Instant::now() + std::time::Duration::from_secs(2),
                f,
                Arc::new(()),
            )
            .await
            .unwrap()
        });
        entered.acquire().await.unwrap().forget();
        drop(
            c.read(
                128 * UNIT as u64,
                UNIT,
                256 * UNIT as u64,
                Instant::now() + std::time::Duration::from_secs(2),
                fetch,
                Arc::new(()),
            )
            .await
            .unwrap(),
        );
        release.add_permits(1);
        let result = pending.await.unwrap();
        assert_eq!(result.len(), 2);
        drop(result);
        assert_eq!(
            calls
                .lock()
                .unwrap()
                .iter()
                .filter(|(a, _)| *a == 32 * UNIT as u64)
                .count(),
            1
        );
        c.retire();
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        assert_eq!(budget.used.load(Ordering::Acquire), 0);
    }
}
