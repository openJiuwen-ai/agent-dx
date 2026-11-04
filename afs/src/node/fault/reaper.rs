//! Reaper: the Node's background garbage-collection loop.
//!
//! One pass asks Meta which GC generation this node must collect and, when it
//! is waiting on this node, retires those local copies through the copy
//! lifecycle. Pins (`node::gc`) stay with the paths that take them. S3 mode
//! has no references, so its passes return 0 (orphan reclaim is R05).

use std::{sync::Arc, time::Duration};

use afs_error::Result;

use crate::{
    node::{dfs_read::DfsReadEngine, gc::GcReferences, vfs::dfs::DistributedFs},
    runtime::{ServiceResult, Services, cancelled},
};

/// Period of the reaping loop.
const REAPER_PERIOD: Duration = Duration::from_secs(5);

/// Runs `DistributedFs::collect_garbage` every [`REAPER_PERIOD`] until
/// shutdown.
///
/// Conventions: a collected count above zero is logged (`dfs.gc_collected`);
/// a pass error (`dfs.gc_retry`) or a failed blocking worker
/// (`dfs.gc_worker_failed`) is logged and retried next period; the service
/// itself only ends at shutdown.
pub(crate) struct Reaper {
    fs: Arc<DistributedFs>,
}

impl Reaper {
    pub(crate) fn new(fs: Arc<DistributedFs>) -> Self {
        Self { fs }
    }

    /// Start the loop as one service; it stops with `services.stop`.
    pub(crate) fn start(self, services: &mut Services) {
        let stop = services.stop.subscribe();
        let fs = self.fs;
        services.spawn(run_reaper_loop(stop, move || fs.collect_garbage()));
    }
}

async fn run_reaper_loop(
    stop: tokio::sync::watch::Receiver<bool>,
    pass: impl Fn() -> Result<usize> + Send + Sync + 'static,
) -> ServiceResult {
    let pass = Arc::new(pass);
    let mut tick = tokio::time::interval(REAPER_PERIOD);
    let shutdown = cancelled(stop);
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            _ = &mut shutdown => return Ok(()),
            _ = tick.tick() => {
                let pass = pass.clone();
                match crate::node::bridge::spawn_blocking(move || pass()).await {
                    Ok(Ok(count)) if count > 0 => afs_logging::info!("dfs.gc_collected"; "chunks" => count),
                    Ok(Ok(_)) => {}
                    Ok(Err(error)) => afs_logging::warn!("dfs.gc_retry"; "error" => error.to_string()),
                    Err(error) => afs_logging::warn!("dfs.gc_worker_failed"; "error" => error.to_string()),
                }
            }
        }
    }
}

/// One collection pass. The caller holds the collection gate, so passes never
/// overlap. Releases are flushed first; local copies are retired only when
/// Meta's batch waits on this node for a newer generation, and completion is
/// acknowledged with an empty inventory.
pub(crate) fn collect_once(
    references: &GcReferences,
    read_engine: &DfsReadEngine,
    node_id: &str,
) -> Result<usize> {
    references.flush()?;
    let completed = read_engine.gc_generation()?;
    let batch = references
        .control
        .collect(completed, read_engine.gc_inventory()?)?;
    if batch.waiting_nodes.iter().any(|node| node == node_id) && batch.generation > completed {
        read_engine.collect_generation(batch.generation, &batch.chunks)?;
        references.control.collect(batch.generation, Vec::new())?;
        return Ok(batch.chunks.len());
    }
    Ok(0)
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
        let service = tokio::spawn(run_reaper_loop(rx, move || {
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
}
