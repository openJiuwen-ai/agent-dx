//! Reaper: the Node's background garbage-collection loop.
//!
//! One pass (`node::gc::Collector::collect_once`) asks Meta which GC
//! generation this node must collect and, when it is waiting on this node,
//! retires those local copies through the copy lifecycle; in S3 mode it deletes
//! the objects and lists the store when Meta assigns it. Pins (`node::gc`) stay
//! with the paths that take them.

use std::{sync::Arc, time::Duration};

use afs_error::Result;

use crate::{
    node::gc::Collector,
    runtime::{ServiceResult, Services, cancelled},
};

/// Period of the reaping loop.
const REAPER_PERIOD: Duration = Duration::from_secs(5);

/// Runs `Collector::collect_once` every [`REAPER_PERIOD`] until shutdown.
///
/// Conventions: a collected count above zero is logged (`dfs.gc_collected`);
/// a pass error (`dfs.gc_retry`) or a failed blocking worker
/// (`dfs.gc_worker_failed`) is logged and retried next period; the service
/// itself only ends at shutdown.
pub(crate) struct Reaper {
    collector: Option<Arc<Collector>>,
}

/// What one loop iteration runs. A release wake never plans or collects.
#[derive(Clone, Copy)]
enum PassKind {
    Collect,
    Release,
}

impl Reaper {
    pub(crate) fn new(collector: Option<Arc<Collector>>) -> Self {
        Self { collector }
    }

    /// Start the loop as one service; it stops with `services.stop`.
    pub(crate) fn start(self, services: &mut Services) {
        let stop = services.stop.subscribe();
        let collector = self.collector;
        let waking = collector.clone();
        services.spawn(run_reaper_loop(
            stop,
            move || {
                let waking = waking.clone();
                async move {
                    match waking {
                        Some(collector) => collector.release_wanted().await,
                        None => std::future::pending().await,
                    }
                }
            },
            move |kind| {
                collector.as_ref().map_or(Ok(0), |collector| match kind {
                    PassKind::Release => collector.flush_references(),
                    PassKind::Collect => {
                        #[cfg(feature = "diagnostics")]
                        if super::experiment::gc_collection_off() {
                            return collector.flush_references();
                        }
                        collector.collect_once()
                    }
                })
            },
        ));
    }
}

async fn run_reaper_loop<W, F>(
    stop: tokio::sync::watch::Receiver<bool>,
    release_wanted: W,
    pass: impl Fn(PassKind) -> Result<usize> + Send + Sync + 'static,
) -> ServiceResult
where
    W: Fn() -> F,
    F: std::future::Future<Output = ()>,
{
    let pass = Arc::new(pass);
    let mut tick = tokio::time::interval(REAPER_PERIOD);
    let shutdown = cancelled(stop);
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            _ = &mut shutdown => return Ok(()),
            kind = async {
                tokio::select! {
                    _ = tick.tick() => PassKind::Collect,
                    () = release_wanted() => PassKind::Release,
                }
            } => {
                let pass = pass.clone();
                match crate::node::bridge::spawn_blocking(move || pass(kind)).await {
                    Ok(Ok(count)) if count > 0 => afs_logging::info!("dfs.gc_collected"; "chunks" => count),
                    Ok(Ok(_)) => {}
                    Ok(Err(error)) => afs_logging::warn!("dfs.gc_retry"; "error" => error.to_string()),
                    Err(error) => afs_logging::warn!("dfs.gc_worker_failed"; "error" => error.to_string()),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn reaper_retries_failed_passes_and_stops_on_shutdown() {
        let (stop, rx) = tokio::sync::watch::channel(false);
        let passes = Arc::new(AtomicUsize::new(0));
        let counted = passes.clone();
        let service = tokio::spawn(run_reaper_loop(rx, std::future::pending, move |_| {
            counted.fetch_add(1, Ordering::SeqCst);
            Err(afs_error::Error::coded(
                afs_error::NODE_STORAGE_INVALID,
                "meta unavailable",
            ))
        }));
        // The first interval tick fires at once; an error does not end the loop.
        while passes.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        assert!(!service.is_finished());
        stop.send(true).unwrap();
        assert!(service.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn a_release_wake_flushes_without_a_collection_pass() {
        let (stop, rx) = tokio::sync::watch::channel(false);
        let wake = Arc::new(tokio::sync::Notify::new());
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let recorded = seen.clone();
        let waiter = wake.clone();
        let service = tokio::spawn(run_reaper_loop(
            rx,
            move || {
                let waiter = waiter.clone();
                async move { waiter.notified().await }
            },
            move |kind| {
                recorded
                    .lock()
                    .unwrap()
                    .push(matches!(kind, PassKind::Collect));
                Ok(0)
            },
        ));
        // The first interval tick is the collection pass.
        while seen.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
        wake.notify_one();
        while seen.lock().unwrap().len() < 2 {
            tokio::task::yield_now().await;
        }
        assert_eq!(*seen.lock().unwrap(), vec![true, false]);
        stop.send(true).unwrap();
        assert!(service.await.unwrap().is_ok());
    }
}
