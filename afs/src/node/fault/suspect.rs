//! Suspect copies (R59): a reader's verification of a peer's bytes failed.
//!
//! Only the holder can tell a bad disk from a flipped bit, and Meta accepts a
//! corruption report only for the reporter's own device. So the reader asks
//! the holder to re-verify; a bad copy is quarantined there and takes the
//! normal self-report path. The read path only enqueues, never waits.

use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use afs_error::Result;

use crate::{
    dfs::ChunkId,
    runtime::{ServiceResult, Services, cancelled},
};

const MAX_PENDING: usize = 256;
const MAX_REMEMBERED: usize = 4096;
/// One chunk is reported to, and re-verified by, one holder at most this often.
const REVIEW_COOLDOWN: Duration = Duration::from_secs(60);
/// Bounds the RPCs one pass sends; the rest wait for the next pass.
const MAX_REVIEWS_PER_PASS: usize = 8;
const REVIEW_PERIOD: Duration = Duration::from_secs(5);

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct SuspectCopy {
    pub node_id: String,
    pub node_epoch: u64,
    pub endpoint: String,
    pub chunk_id: ChunkId,
}

/// Remembers keys for [`REVIEW_COOLDOWN`] within a fixed bound.
struct Cooldown<K> {
    last: HashMap<K, Instant>,
}

impl<K: Clone + Eq + std::hash::Hash> Cooldown<K> {
    fn new() -> Self {
        Self {
            last: HashMap::new(),
        }
    }

    fn admit(&mut self, key: &K, now: Instant) -> bool {
        if self
            .last
            .get(key)
            .is_some_and(|last| now.duration_since(*last) < REVIEW_COOLDOWN)
        {
            return false;
        }
        if self.last.len() >= MAX_REMEMBERED {
            self.last
                .retain(|_, last| now.duration_since(*last) < REVIEW_COOLDOWN);
            if self.last.len() >= MAX_REMEMBERED {
                return false;
            }
        }
        self.last.insert(key.clone(), now);
        true
    }
}

/// The reader side: bounded and deduplicated. A full queue drops the report;
/// the next read of that copy reports it again.
pub struct SuspectQueue {
    state: Mutex<(VecDeque<SuspectCopy>, Cooldown<SuspectCopy>)>,
}

impl Default for SuspectQueue {
    fn default() -> Self {
        Self {
            state: Mutex::new((VecDeque::new(), Cooldown::new())),
        }
    }
}

impl SuspectQueue {
    pub fn report(&self, suspect: SuspectCopy) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        let (pending, cooldown) = &mut *state;
        if pending.len() < MAX_PENDING && cooldown.admit(&suspect, Instant::now()) {
            pending.push_back(suspect);
        }
    }

    pub(crate) fn take(&self, limit: usize) -> Vec<SuspectCopy> {
        let Ok(mut state) = self.state.lock() else {
            return Vec::new();
        };
        let count = limit.min(state.0.len());
        state.0.drain(..count).collect()
    }
}

/// The holder side: at most one review at a time and one per chunk per
/// cooldown, so a peer cannot turn reports into unbounded disk scans.
pub struct SuspectReviewGate {
    cooldown: Mutex<Cooldown<ChunkId>>,
    running: Arc<tokio::sync::Semaphore>,
}

impl Default for SuspectReviewGate {
    fn default() -> Self {
        Self {
            cooldown: Mutex::new(Cooldown::new()),
            running: Arc::new(tokio::sync::Semaphore::new(1)),
        }
    }
}

impl SuspectReviewGate {
    pub fn admit(&self, chunk_id: &ChunkId) -> Option<tokio::sync::OwnedSemaphorePermit> {
        let permit = self.running.clone().try_acquire_owned().ok()?;
        let mut cooldown = self.cooldown.lock().ok()?;
        cooldown.admit(chunk_id, Instant::now()).then_some(permit)
    }
}

/// Sends queued reviews every [`REVIEW_PERIOD`] until shutdown. A pass error is
/// logged and dropped: the next read of that copy reports it again.
pub(crate) struct SuspectReviewer<F> {
    pass: F,
}

impl<F> SuspectReviewer<F>
where
    F: Fn(usize) -> Result<usize> + Send + Sync + 'static,
{
    /// `pass` reviews up to the given number of queued suspects.
    pub(crate) fn new(pass: F) -> Self {
        Self { pass }
    }

    pub(crate) fn start(self, services: &mut Services) {
        let stop = services.stop.subscribe();
        services.spawn(run_review_loop(stop, self.pass));
    }
}

async fn run_review_loop(
    stop: tokio::sync::watch::Receiver<bool>,
    pass: impl Fn(usize) -> Result<usize> + Send + Sync + 'static,
) -> ServiceResult {
    let pass = Arc::new(pass);
    let mut tick = tokio::time::interval(REVIEW_PERIOD);
    let shutdown = cancelled(stop);
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            _ = &mut shutdown => return Ok(()),
            _ = tick.tick() => {
                let pass = pass.clone();
                match crate::node::bridge::spawn_blocking(move || pass(MAX_REVIEWS_PER_PASS)).await {
                    Ok(Ok(_)) => {}
                    Ok(Err(error)) => afs_logging::warn!("dfs.suspect_review_retry"; "error" => error.to_string()),
                    Err(error) => afs_logging::warn!("dfs.suspect_review_worker_failed"; "error" => error.to_string()),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn suspect(node: &str, chunk: &str) -> SuspectCopy {
        SuspectCopy {
            node_id: node.into(),
            node_epoch: 1,
            endpoint: "http://x".into(),
            chunk_id: ChunkId::new(chunk),
        }
    }

    #[test]
    fn a_copy_is_queued_once_per_cooldown_and_the_queue_is_bounded() {
        let queue = SuspectQueue::default();
        queue.report(suspect("b", "c"));
        queue.report(suspect("b", "c"));
        queue.report(suspect("d", "c"));
        assert_eq!(queue.take(8).len(), 2);
        queue.report(suspect("b", "c"));
        assert!(queue.take(8).is_empty(), "taken, still cooling down");
        for index in 0..(MAX_PENDING + 10) {
            queue.report(suspect("b", &format!("chunk-{index}")));
        }
        assert_eq!(queue.take(usize::MAX).len(), MAX_PENDING);
    }

    #[test]
    fn the_holder_reviews_one_at_a_time_and_each_chunk_once_per_cooldown() {
        let gate = SuspectReviewGate::default();
        let permit = gate.admit(&ChunkId::new("a")).unwrap();
        assert!(gate.admit(&ChunkId::new("b")).is_none(), "one at a time");
        drop(permit);
        assert!(gate.admit(&ChunkId::new("a")).is_none(), "cooling down");
        assert!(gate.admit(&ChunkId::new("b")).is_some());
    }
}
