//! T11 `RdmaSessionSweeper`.

use crate::{
    node::rpc,
    runtime::{Services, cancelled},
};

/// T11 `RdmaSessionSweeper`: every 5 s drops expired RDMA sessions from the
/// Node control table and, with OwnerFs, from the OwnerFs table. It then
/// retries a bounded batch of quarantined RDMA drains on a blocking worker.
///
/// Retry class with no error path: a sweep cannot fail. No log events of its
/// own. Stop: `services.stop` breaks the loop and returns `Ok`.
pub(crate) struct RdmaSessionSweeper {
    pub(crate) sessions: rpc::control::RdmaSessionRegistry,
    #[cfg(feature = "ownerfs")]
    pub(crate) owner_sessions: rpc::control::RdmaSessionRegistry,
}

impl RdmaSessionSweeper {
    /// Start the sweep as one service; it stops with `services.stop`.
    pub(crate) fn start(self, services: &mut Services) {
        let sessions = self.sessions;
        let stop = services.stop.subscribe();
        #[cfg(feature = "ownerfs")]
        let owner_rdma_cleanup = self.owner_sessions;
        services.spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(5));
            #[cfg(feature = "rdma")]
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            tokio::pin! {let shutdown=cancelled(stop);}
            loop {
                tokio::select! {
                    _ = tick.tick() => {
                        sessions.cleanup_expired().await;
                        #[cfg(feature = "ownerfs")]
                        owner_rdma_cleanup.cleanup_expired().await;
                        #[cfg(feature = "rdma")]
                        if afs_transport::rdma::has_quarantined_drains() {
                            let _ = crate::node::bridge::spawn_blocking(
                                afs_transport::rdma::retry_quarantined_drains
                            ).await;
                        }
                    }
                    _ = &mut shutdown => break,
                }
            }
            Ok(())
        });
    }
}
