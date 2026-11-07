//! T6 `DfsBackgroundTick`: the DFS background tick, one service that runs its
//! steps serially in one task (M4-B: named steps, original schedule).
//!
//! Per wakeup, in this order:
//!
//! - writeback wakeup (write-pipeline notify, or the
//!   `dfs_background_commit_ms` timer, default 5 s) → `WritebackPump`:
//!   1. notify: `progress_writeback`, the ready write-pipeline work;
//!   2. timer: `writeback_pending`, everything due;
//! - the 5 s tick → one blocking pass over
//!   3. `RemoteReleaseRetrier` (`retry_pending_remote_releases`),
//!   4. `PeerSessionReaper::reap_owner_handles`,
//!   5. `PeerSessionReaper::reap_lock_sessions`,
//!
//!   all attempted, the first error kept (`drain_all`).
//!
//! Both arms share one `select!`, so a slow 5 s pass delays the next writeback
//! progress exactly as before; running them as separate tasks would change
//! that (S3 performance work, not S1).
//!
//! Retry class: a pass error (`dfs.native_write_retry`,
//! `dfs.background_retry`) or a failed blocking worker
//! (`dfs.native_write_worker_failed`, `dfs.background_worker_failed`) is
//! logged and retried at the next wakeup. Progress is logged as
//! `dfs.native_write_progress` (debug), `dfs.remote_releases_retried`,
//! `dfs.peer_owner_handles_reaped` and `dfs.peer_lock_sessions_reaped`.
//! Stop: `services.stop` ends it with `Ok`, checked between passes.

use super::drain_all;
use crate::{
    node::vfs::dfs::DistributedFs,
    runtime::{Services, cancelled},
};
use std::{sync::Arc, time::Duration};

/// Writeback passes by trigger (`notify`: a write-pipeline wakeup, `timer`:
/// the commit timer) and whether the pass committed anything; `skipped` counts
/// notify wakeups that found no work and ran no pass. Registered once
/// per Node; R66 measures wakeups per operation with it.
#[derive(Clone)]
pub(crate) struct WritebackPassMetrics {
    passes: afs_metrics::IntCounterVec,
}

impl WritebackPassMetrics {
    pub(crate) fn register(
        registry: &afs_metrics::Registry,
    ) -> std::result::Result<Self, afs_metrics::MetricsError> {
        registry.get_or_register(|registry| {
            let passes = afs_metrics::IntCounterVec::new(
                afs_metrics::Opts::new(
                    "afs_dfs_writeback_passes_total",
                    "DFS background writeback passes by trigger and outcome; skipped wakeups ran no pass.",
                ),
                &["trigger", "outcome"],
            )?;
            afs_metrics::register_collector(registry, &passes)?;
            for trigger in ["notify", "timer"] {
                for outcome in ["committed", "idle", "error", "skipped"] {
                    passes.with_label_values(&[trigger, outcome]);
                }
            }
            Ok(Self { passes })
        })
    }

    fn observe(&self, timer_due: bool, outcome: &'static str) {
        let trigger = if timer_due { "timer" } else { "notify" };
        self.passes.with_label_values(&[trigger, outcome]).inc();
    }
}

/// T6; see the module documentation.
pub(crate) struct DfsBackgroundTick {
    fs: Arc<DistributedFs>,
    commit_period: Duration,
    passes: WritebackPassMetrics,
}

impl DfsBackgroundTick {
    pub(crate) fn new(
        fs: Arc<DistributedFs>,
        commit_period: Duration,
        passes: WritebackPassMetrics,
    ) -> Self {
        Self {
            fs,
            commit_period,
            passes,
        }
    }

    /// Start the tick as one service; it stops with `services.stop`.
    pub(crate) fn start(self, services: &mut Services) {
        let dfs = self.fs;
        let stop = services.stop.subscribe();
        let writeback_wakeup = dfs.writeback_notify();
        let background_commit_period = self.commit_period;
        let pump = WritebackPump { fs: dfs.clone() };
        let passes = self.passes;
        let releases = RemoteReleaseRetrier { fs: dfs.clone() };
        let peers = PeerSessionReaper { fs: dfs };
        services.spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(5));
            tick.tick().await;
            let mut commit_tick = tokio::time::interval(background_commit_period);
            commit_tick.tick().await;
            let has_write_pipeline = writeback_wakeup.is_some();
            let shutdown = cancelled(stop);
            tokio::pin!(shutdown);
            loop {
                tokio::select! {
                    _ = &mut shutdown => return Ok(()),
                    due = async {
                        match writeback_wakeup.as_ref() {
                            Some(notify) => tokio::select! {
                                _ = notify.notified() => false,
                                _ = commit_tick.tick() => true,
                            },
                            None => std::future::pending::<bool>().await,
                        }
                    }, if has_write_pipeline => {
                        // Most write-pipeline wakeups find nothing to do; skip
                        // the blocking hop for those (R66). The timer pass always runs.
                        if !due && !pump.ready() {
                            passes.observe(false, "skipped");
                            continue;
                        }
                        let pump = pump.clone();
                        match crate::node::bridge::spawn_blocking(move || pump.pass(due)).await {
                            Ok(Ok(count)) if count > 0 => {
                                passes.observe(due, "committed");
                                afs_logging::debug!("dfs.native_write_progress"; "count" => count, "timer_due" => due);
                            }
                            Ok(Ok(_)) => passes.observe(due, "idle"),
                            Ok(Err(error)) => {
                                passes.observe(due, "error");
                                afs_logging::warn!("dfs.native_write_retry"; "error" => error.to_string());
                            }
                            Err(error) => {
                                afs_logging::warn!("dfs.native_write_worker_failed"; "error" => error.to_string());
                            }
                        }
                    }
                    _ = tick.tick() => {
                        let (releases, peers) = (releases.clone(), peers.clone());
                        match crate::node::bridge::spawn_blocking(move || {
                            let [remote_releases, owner_handles, lock_sessions] = drain_all([
                                &|| releases.pass(),
                                &|| peers.reap_owner_handles(),
                                &|| peers.reap_lock_sessions(),
                            ])?;
                            Ok::<_, afs_error::Error>((remote_releases, owner_handles, lock_sessions))
                        }).await {
                            Ok(Ok((remote_releases, owner_handles, lock_sessions)))
                                if remote_releases > 0 || owner_handles > 0 || lock_sessions > 0 =>
                            {
                                if remote_releases > 0 {
                                    afs_logging::info!("dfs.remote_releases_retried"; "count" => remote_releases);
                                }
                                if owner_handles > 0 {
                                    afs_logging::info!("dfs.peer_owner_handles_reaped"; "count" => owner_handles);
                                }
                                if lock_sessions > 0 {
                                    afs_logging::info!("dfs.peer_lock_sessions_reaped"; "count" => lock_sessions);
                                }
                            }
                            Ok(Ok(_)) => {}
                            Ok(Err(error)) => {
                                afs_logging::warn!("dfs.background_retry"; "error" => error.to_string());
                            }
                            Err(error) => {
                                afs_logging::warn!("dfs.background_worker_failed"; "error" => error.to_string());
                            }
                        }
                    }
                }
            }
        });
    }
}

/// Steps 1–2: advance the write pipeline.
#[derive(Clone)]
struct WritebackPump {
    fs: Arc<DistributedFs>,
}

impl WritebackPump {
    /// A notify wakeup progresses ready work; the commit timer writes back
    /// everything due.
    /// Cheap readiness check; an error means "run the pass" so the pass
    /// reports it.
    fn ready(&self) -> bool {
        self.fs.writeback_ready().unwrap_or(true)
    }

    fn pass(&self, timer_due: bool) -> afs_error::Result<usize> {
        if timer_due {
            self.fs.writeback_pending()
        } else {
            self.fs.progress_writeback()
        }
    }
}

/// Step 3: retry remote releases that failed earlier.
#[derive(Clone)]
struct RemoteReleaseRetrier {
    fs: Arc<DistributedFs>,
}

impl RemoteReleaseRetrier {
    fn pass(&self) -> afs_error::Result<usize> {
        self.fs.retry_pending_remote_releases()
    }
}

/// Steps 4–5: reap expired Peer owner handles and Peer lock sessions; the
/// counterpart of the OwnerFs `OwnerPeerReaper` (T3).
#[derive(Clone)]
struct PeerSessionReaper {
    fs: Arc<DistributedFs>,
}

impl PeerSessionReaper {
    fn reap_owner_handles(&self) -> afs_error::Result<usize> {
        self.fs.reap_expired_peer_owner_handles()
    }

    fn reap_lock_sessions(&self) -> afs_error::Result<usize> {
        self.fs.reap_expired_peer_lock_sessions()
    }
}
