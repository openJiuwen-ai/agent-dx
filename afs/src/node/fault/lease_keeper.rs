//! Lease keeper: renewal of DFS writer leases.
//!
//! The background loop renews the leases of active writers well before they
//! expire, so main paths rarely wait on Meta. Three main paths must still hold
//! a fresh lease before they proceed — commit, open for write and
//! copy_file_range — and renew synchronously through [`ensure_fresh`]: that
//! renewal is part of their contract, the one documented exception (S1c J4)
//! to "renewal never runs on a main path".

use std::{sync::Arc, time::Duration};

use afs_error::Result;

use crate::clock::now_unix_ms;
use crate::{
    dfs::{DFS_WRITE_LEASE_SECONDS, WriteLease},
    runtime::{ServiceResult, Services, cancelled},
};

/// Period of the background renewal loop.
const LEASE_KEEPER_PERIOD: Duration = Duration::from_secs(1);
/// A main path renews when the lease expires within this window.
const SYNC_RENEW_BEFORE_EXPIRY_MS: u64 = 5_000;

/// A main path must renew before proceeding: the lease expires within
/// [`SYNC_RENEW_BEFORE_EXPIRY_MS`].
pub(crate) fn should_renew(lease: &WriteLease) -> bool {
    lease.expires_at_unix_ms <= now_unix_ms().saturating_add(SYNC_RENEW_BEFORE_EXPIRY_MS)
}

/// The background loop renews once a third of the lease term has passed.
pub(crate) fn should_background_renew(lease: &WriteLease) -> bool {
    let renew_before_expiry_ms = DFS_WRITE_LEASE_SECONDS.saturating_mul(1_000) * 2 / 3;
    lease.expires_at_unix_ms <= now_unix_ms().saturating_add(renew_before_expiry_ms)
}

/// J4 exception: renew `lease` on the caller's thread if it is about to
/// expire. On error the lease is left unchanged and the error returned, so
/// the caller fails exactly as if it had called Meta itself.
pub(crate) fn ensure_fresh(
    lease: &mut WriteLease,
    renew: impl FnOnce(WriteLease) -> Result<WriteLease>,
) -> Result<()> {
    if should_renew(lease) {
        *lease = renew(lease.clone())?;
    }
    Ok(())
}

/// Runs `DistributedFs::renew_active_write_leases` every
/// [`LEASE_KEEPER_PERIOD`] until shutdown.
///
/// Conventions: a pass error (`dfs.writer_renewal_retry`) or a failed
/// blocking worker (`dfs.writer_renewal_worker_failed`) is logged and retried
/// next period; the service itself only ends at shutdown.
pub(crate) struct LeaseKeeper<F> {
    renew: F,
}

impl<F> LeaseKeeper<F>
where
    F: Fn() -> Result<usize> + Send + Sync + 'static,
{
    /// `renew` is one renewal pass over the filesystem's active writers
    /// (`DistributedFs::renew_active_write_leases`).
    pub(crate) fn new(renew: F) -> Self {
        Self { renew }
    }

    /// Start the loop as one service; it stops with `services.stop`.
    pub(crate) fn start(self, services: &mut Services) {
        let stop = services.stop.subscribe();
        services.spawn(run_lease_loop(stop, self.renew));
    }
}

async fn run_lease_loop(
    stop: tokio::sync::watch::Receiver<bool>,
    pass: impl Fn() -> Result<usize> + Send + Sync + 'static,
) -> ServiceResult {
    let pass = Arc::new(pass);
    let mut tick = tokio::time::interval(LEASE_KEEPER_PERIOD);
    let shutdown = cancelled(stop);
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            _ = &mut shutdown => return Ok(()),
            _ = tick.tick() => {
                let pass = pass.clone();
                match crate::node::bridge::spawn_blocking(move || pass()).await {
                    Ok(Ok(_)) => {}
                    Ok(Err(error)) => afs_logging::warn!("dfs.writer_renewal_retry"; "error" => error.to_string()),
                    Err(error) => afs_logging::warn!("dfs.writer_renewal_worker_failed"; "error" => error.to_string()),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn lease_expiring_at(expires_at_unix_ms: u64) -> WriteLease {
        WriteLease {
            inode_id: crate::dfs::InodeId::new("inode:test"),
            owner_node_id: "node-a".into(),
            owner_session_id: "session-a".into(),
            lease_epoch: 1,
            expires_at_unix_ms,
        }
    }

    #[test]
    fn ensure_fresh_renews_only_near_expiry() {
        let far = now_unix_ms() + 60_000;
        let mut lease = lease_expiring_at(far);
        ensure_fresh(&mut lease, |_| panic!("a fresh lease is not renewed")).unwrap();
        assert_eq!(lease.expires_at_unix_ms, far);

        let mut lease = lease_expiring_at(now_unix_ms() + 1_000);
        ensure_fresh(&mut lease, |old| {
            Ok(WriteLease {
                expires_at_unix_ms: old.expires_at_unix_ms + 30_000,
                ..old
            })
        })
        .unwrap();
        assert!(lease.expires_at_unix_ms > now_unix_ms() + 20_000);
    }

    #[test]
    fn ensure_fresh_keeps_the_lease_on_renewal_error() {
        let soon = now_unix_ms() + 1_000;
        let mut lease = lease_expiring_at(soon);
        let error = ensure_fresh(&mut lease, |_| {
            Err(afs_error::Error::coded(
                afs_error::NODE_STORAGE_INVALID,
                "meta unavailable",
            ))
        })
        .unwrap_err();
        assert_eq!(error.code(), afs_error::NODE_STORAGE_INVALID);
        assert_eq!(lease.expires_at_unix_ms, soon);
    }

    #[tokio::test]
    async fn lease_keeper_retries_failed_passes_and_stops_on_shutdown() {
        let (stop, rx) = tokio::sync::watch::channel(false);
        let passes = Arc::new(AtomicUsize::new(0));
        let counted = passes.clone();
        let service = tokio::spawn(run_lease_loop(rx, move || {
            counted.fetch_add(1, Ordering::SeqCst);
            Err(afs_error::Error::coded(
                afs_error::NODE_STORAGE_INVALID,
                "meta unavailable",
            ))
        }));
        while passes.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        assert!(!service.is_finished());
        stop.send(true).unwrap();
        assert!(service.await.unwrap().is_ok());
    }
}
