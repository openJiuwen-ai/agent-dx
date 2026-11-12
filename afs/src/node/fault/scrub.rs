//! Scrub: the Node's background verification of its own durable copies
//! (S4-16).
//!
//! A copy that only a remote reader would ever check (for example the second
//! replica of data its writer always reads locally) can rot unnoticed until it
//! is the only repair source left. [`ScrubService`] walks the local catalog in
//! chunk-id order, verifies one complete copy at a time and lets a failed
//! verification quarantine the copy, which the repair loop reports to Meta.
//! No protocol, record or format is involved.
//!
//! Pacing: at most one copy in flight, and the next copy starts only when the
//! bytes verified so far fit `bytes_per_second`. The first round starts after
//! `start_delay`; a round starts no earlier than `round_interval` after the
//! previous one started (back to back when a round takes longer).
//!
//! Class: retry. A failed batch, verification or blocking worker is logged and
//! the walk continues; the service returns only at shutdown, observed between
//! copies and inside every wait.
//!
//! Log events: `dfs.scrub_corrupt_found` (warn), `dfs.scrub_round_completed`
//! (info), `dfs.scrub_retry` (warn), `dfs.scrub_worker_failed` (warn).

use std::{sync::Arc, time::Duration};

use afs_error::Result;
use tokio::time::Instant;

use crate::{
    dfs::ChunkId,
    node::chunk::{LocalChunkStore, ScrubItem, ScrubOutcome},
    runtime::{ServiceResult, Services, cancelled},
};

/// Copies selected per catalog pass.
const SCRUB_BATCH: usize = 256;
/// Wait after a failed batch selection.
const SCRUB_RETRY_DELAY: Duration = Duration::from_secs(1);

/// Scrub pacing, from `dfs_scrub_*` configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ScrubSettings {
    /// Verified bytes per second; 0 disables the service.
    pub(crate) bytes_per_second: u64,
    pub(crate) start_delay: Duration,
    pub(crate) round_interval: Duration,
}

/// `afs_dfs_scrub_total{event}`.
#[derive(Clone)]
pub(crate) struct ScrubMetrics {
    events: afs_metrics::IntCounterVec,
}

impl ScrubMetrics {
    pub(crate) const NAME: &'static str = "afs_dfs_scrub_total";

    pub(crate) fn register(
        registry: &afs_metrics::Registry,
    ) -> std::result::Result<Self, afs_metrics::MetricsError> {
        registry.get_or_register(|registry| {
            let events = afs_metrics::IntCounterVec::new(
                afs_metrics::Opts::new(
                    Self::NAME,
                    "Background scrub of local DFS copies: chunks_verified, bytes_verified, \
                     corrupt_found, skipped, errors, rounds_completed.",
                ),
                &["event"],
            )?;
            registry.register(Box::new(events.clone()))?;
            Ok(Self { events })
        })
    }

    fn add(&self, event: &'static str, value: u64) {
        if value > 0 {
            self.events.with_label_values(&[event]).inc_by(value);
        }
    }
}

/// One round's totals, logged when the round completes.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct RoundTotals {
    chunks: u64,
    bytes: u64,
    corrupt: u64,
    skipped: u64,
    errors: u64,
}

pub(crate) struct ScrubService {
    store: Arc<LocalChunkStore>,
    settings: ScrubSettings,
    metrics: Option<ScrubMetrics>,
}

impl ScrubService {
    pub(crate) fn new(
        store: Arc<LocalChunkStore>,
        settings: ScrubSettings,
        metrics: Option<ScrubMetrics>,
    ) -> Self {
        Self {
            store,
            settings,
            metrics,
        }
    }

    /// Start the walk as one service unless disabled; it stops with
    /// `services.stop`.
    pub(crate) fn start(self, services: &mut Services) {
        if self.settings.bytes_per_second == 0 {
            return;
        }
        let stop = services.stop.subscribe();
        let batching = self.store.clone();
        let verifying = self.store;
        services.spawn(run_scrub_loop(
            stop,
            self.settings,
            self.metrics,
            move |after| {
                let store = batching.clone();
                async move {
                    crate::node::bridge::spawn_blocking(move || {
                        store.scrub_batch(after.as_ref(), SCRUB_BATCH)
                    })
                    .await
                    .unwrap_or_else(|error| Err(worker_failed(error)))
                }
            },
            move |chunk_id| {
                let store = verifying.clone();
                async move {
                    crate::node::bridge::spawn_blocking(move || store.scrub_verify(&chunk_id))
                        .await
                        .unwrap_or_else(|error| Err(worker_failed(error)))
                }
            },
        ));
    }
}

fn worker_failed(error: tokio::task::JoinError) -> afs_error::Error {
    afs_logging::warn!("dfs.scrub_worker_failed"; "error" => error.to_string());
    afs_error::Error::coded(
        afs_error::NODE_STORAGE_INVALID,
        format!("scrub worker failed: {error}"),
    )
}

/// Byte pacing: `account(n, started, now)` returns how long to wait at `now`
/// so that copies started back to back take `bytes / rate` seconds each,
/// their own verification time included. Idle time is never banked into a
/// later burst.
struct Pacer {
    bytes_per_second: u64,
    next: Instant,
}

impl Pacer {
    fn new(bytes_per_second: u64, now: Instant) -> Self {
        Self {
            bytes_per_second,
            next: now,
        }
    }

    fn account(&mut self, bytes: u64, started: Instant, now: Instant) -> Duration {
        let cost = Duration::from_secs_f64(bytes as f64 / self.bytes_per_second.max(1) as f64);
        self.next = self.next.max(started) + cost;
        self.next.saturating_duration_since(now)
    }
}

/// Sleep, or return false when shutdown fires first.
async fn wait(
    shutdown: &mut std::pin::Pin<&mut impl std::future::Future<Output = ()>>,
    delay: Duration,
) -> bool {
    if delay.is_zero() {
        return true;
    }
    tokio::select! {
        _ = shutdown.as_mut() => false,
        _ = tokio::time::sleep(delay) => true,
    }
}

async fn run_scrub_loop<B, BF, V, VF>(
    stop: tokio::sync::watch::Receiver<bool>,
    settings: ScrubSettings,
    metrics: Option<ScrubMetrics>,
    batch: B,
    verify: V,
) -> ServiceResult
where
    B: Fn(Option<ChunkId>) -> BF,
    BF: std::future::Future<Output = Result<Vec<ScrubItem>>>,
    V: Fn(ChunkId) -> VF,
    VF: std::future::Future<Output = Result<ScrubOutcome>>,
{
    let shutdown = cancelled(stop);
    tokio::pin!(shutdown);
    if !wait(&mut shutdown, settings.start_delay).await {
        return Ok(());
    }
    let record = |event: &'static str, value: u64| {
        if let Some(metrics) = &metrics {
            metrics.add(event, value);
        }
    };
    let mut pacer = Pacer::new(settings.bytes_per_second, Instant::now());
    loop {
        let started = Instant::now();
        let mut totals = RoundTotals::default();
        let mut after: Option<ChunkId> = None;
        loop {
            let items = match batch(after.clone()).await {
                Ok(items) => items,
                Err(error) => {
                    afs_logging::warn!("dfs.scrub_retry"; "error" => error.to_string());
                    record("errors", 1);
                    totals.errors += 1;
                    if !wait(&mut shutdown, SCRUB_RETRY_DELAY).await {
                        return Ok(());
                    }
                    continue;
                }
            };
            let Some(last) = items.last() else {
                break;
            };
            after = Some(last.chunk_id.clone());
            for item in items {
                let copy_started = Instant::now();
                let charged = match verify(item.chunk_id.clone()).await {
                    Ok(ScrubOutcome::Verified(bytes)) => {
                        totals.chunks += 1;
                        totals.bytes += bytes;
                        record("chunks_verified", 1);
                        record("bytes_verified", bytes);
                        bytes
                    }
                    Ok(ScrubOutcome::Quarantined) => {
                        afs_logging::warn!("dfs.scrub_corrupt_found";
                            "chunk" => item.chunk_id.0.clone());
                        totals.corrupt += 1;
                        record("corrupt_found", 1);
                        item.length
                    }
                    Ok(ScrubOutcome::Skipped) => {
                        totals.skipped += 1;
                        record("skipped", 1);
                        0
                    }
                    Err(error) => {
                        afs_logging::warn!("dfs.scrub_retry";
                            "chunk" => item.chunk_id.0.clone(),
                            "error" => error.to_string());
                        totals.errors += 1;
                        record("errors", 1);
                        item.length
                    }
                };
                let delay = pacer.account(charged, copy_started, Instant::now());
                if !wait(&mut shutdown, delay).await {
                    return Ok(());
                }
            }
        }
        record("rounds_completed", 1);
        afs_logging::info!("dfs.scrub_round_completed";
            "chunks" => totals.chunks,
            "bytes" => totals.bytes,
            "corrupt" => totals.corrupt,
            "skipped" => totals.skipped,
            "errors" => totals.errors,
            "seconds" => started.elapsed().as_secs_f64());
        let next = started + settings.round_interval;
        if !wait(
            &mut shutdown,
            next.saturating_duration_since(Instant::now()),
        )
        .await
        {
            return Ok(());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    fn id(n: usize) -> ChunkId {
        ChunkId::new(format!("chunk-{n:04}"))
    }

    /// A fake catalog of `count` copies of `length` bytes; `corrupt` ids
    /// report `Quarantined`. Records (time since start, id) per verification.
    struct Fake {
        count: usize,
        length: u64,
        corrupt: Vec<ChunkId>,
        verified: Mutex<Vec<(Duration, ChunkId)>>,
        started: Instant,
    }

    fn settings(rate: u64) -> ScrubSettings {
        ScrubSettings {
            bytes_per_second: rate,
            start_delay: Duration::from_secs(10),
            round_interval: Duration::from_secs(100),
        }
    }

    fn spawn(fake: Arc<Fake>, settings: ScrubSettings) -> tokio::sync::watch::Sender<bool> {
        let (stop, rx) = tokio::sync::watch::channel(false);
        let batching = fake.clone();
        let verifying = fake;
        tokio::spawn(run_scrub_loop(
            rx,
            settings,
            None,
            move |after: Option<ChunkId>| {
                let fake = batching.clone();
                async move {
                    Ok((0..fake.count)
                        .map(id)
                        .filter(|chunk| after.as_ref().is_none_or(|after| chunk > after))
                        .take(3)
                        .map(|chunk_id| ScrubItem {
                            chunk_id,
                            length: fake.length,
                        })
                        .collect())
                }
            },
            move |chunk: ChunkId| {
                let fake = verifying.clone();
                async move {
                    fake.verified
                        .lock()
                        .unwrap()
                        .push((fake.started.elapsed(), chunk.clone()));
                    Ok(if fake.corrupt.contains(&chunk) {
                        ScrubOutcome::Quarantined
                    } else {
                        ScrubOutcome::Verified(fake.length)
                    })
                }
            },
        ));
        stop
    }

    #[tokio::test(start_paused = true)]
    async fn rounds_wait_the_start_delay_pace_bytes_and_repeat_on_the_interval() {
        let fake = Arc::new(Fake {
            count: 8,
            length: 4 << 20,
            corrupt: vec![id(5)],
            verified: Mutex::new(Vec::new()),
            started: Instant::now(),
        });
        // 8 MiB/s: each 4 MiB copy costs 0.5 s.
        let stop = spawn(fake.clone(), settings(8 << 20));
        tokio::time::sleep(Duration::from_secs(9)).await;
        assert!(fake.verified.lock().unwrap().is_empty(), "start delay");
        tokio::time::sleep(Duration::from_secs(120)).await;
        let verified = fake.verified.lock().unwrap().clone();
        // Round one: all 8 copies, in id order, 0.5 s apart from t=10 s.
        let first: Vec<_> = verified.iter().take(8).collect();
        for (n, (at, chunk)) in first.iter().enumerate() {
            assert_eq!(*chunk, id(n));
            let want = Duration::from_secs(10) + Duration::from_millis(500) * n as u32;
            assert!(at.abs_diff(want) < Duration::from_millis(5), "{n}: {at:?}");
        }
        // Round two starts 100 s after round one started, not right after it.
        let (second_at, second) = &verified[8];
        assert_eq!(*second, id(0));
        assert!(second_at.abs_diff(Duration::from_secs(110)) < Duration::from_millis(5));
        assert_eq!(verified.len(), 16);
        stop.send(true).unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn a_round_longer_than_the_interval_is_followed_at_once() {
        let fake = Arc::new(Fake {
            count: 4,
            length: 100 << 20,
            corrupt: Vec::new(),
            verified: Mutex::new(Vec::new()),
            started: Instant::now(),
        });
        // 1 MiB/s: each copy costs 100 s, a round 400 s > 100 s interval.
        let stop = spawn(fake.clone(), settings(1 << 20));
        tokio::time::sleep(Duration::from_secs(10 + 400 + 1)).await;
        let verified = fake.verified.lock().unwrap().clone();
        assert_eq!(verified.len(), 5);
        assert_eq!(verified[4].1, id(0));
        assert!(verified[4].0.abs_diff(Duration::from_secs(410)) < Duration::from_millis(5));
        stop.send(true).unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_ends_the_walk_inside_a_wait() {
        let fake = Arc::new(Fake {
            count: 4,
            length: 100 << 20,
            corrupt: Vec::new(),
            verified: Mutex::new(Vec::new()),
            started: Instant::now(),
        });
        let (stop, rx) = tokio::sync::watch::channel(false);
        let verifying = fake.clone();
        let service = tokio::spawn(run_scrub_loop(
            rx,
            settings(1 << 20),
            None,
            |_after| async {
                Ok(vec![ScrubItem {
                    chunk_id: id(0),
                    length: 100 << 20,
                }])
            },
            move |chunk: ChunkId| {
                let fake = verifying.clone();
                async move {
                    fake.verified.lock().unwrap().push((Duration::ZERO, chunk));
                    Ok(ScrubOutcome::Verified(100 << 20))
                }
            },
        ));
        tokio::time::sleep(Duration::from_secs(20)).await;
        assert_eq!(fake.verified.lock().unwrap().len(), 1);
        stop.send(true).unwrap();
        assert!(service.await.unwrap().is_ok());
    }

    #[test]
    fn pacing_does_not_bank_idle_time() {
        let now = Instant::now();
        let mut pacer = Pacer::new(1 << 20, now);
        assert_eq!(pacer.account(1 << 20, now, now), Duration::from_secs(1));
        // Verification time counts toward the copy's own cost.
        let start = now + Duration::from_secs(1);
        assert_eq!(
            pacer.account(1 << 20, start, start + Duration::from_millis(100)),
            Duration::from_millis(900)
        );
        // Ten idle seconds later the next copy still waits its own cost.
        let later = now + Duration::from_secs(12);
        assert_eq!(
            pacer.account(1 << 19, later, later),
            Duration::from_millis(500)
        );
    }
}
