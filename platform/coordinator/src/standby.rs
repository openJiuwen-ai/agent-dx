//! Warm-standby read-only synchronization.
//!
//! The standby never takes an epoch and never writes: it periodically reads
//! the full control state through [`RedisStore::read_snapshot`] and tracks
//! how fresh its view is. The tracked state is eventual-consistency and
//! takeover-readiness evidence — the takeover itself never trusts it and
//! always re-reads the complete state after acquiring the lease.
use crate::lease::CoordinatorLease;
use crate::storage::RedisStore;
use adx_core::Result;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

/// The standby's view of its last successful synchronization.
#[derive(Debug, Clone)]
pub struct SyncState {
    /// Header revision at the last successful sync.
    pub revision: u64,
    /// Highest stored generation at the last successful sync.
    pub generation: u64,
    /// When the last successful sync completed.
    pub synced_at: Instant,
    /// Set when the latest refresh attempt failed: the numbers above may be
    /// stale. Cleared by the next successful refresh.
    pub stale: bool,
}

pub struct Standby {
    store: RedisStore,
    state: Mutex<Option<SyncState>>,
}

impl Standby {
    pub fn new(store: RedisStore) -> Self {
        Self {
            store,
            state: Mutex::new(None),
        }
    }

    /// One read-only synchronization attempt. A failure keeps the last
    /// successful state and marks it stale instead of discarding it, so the
    /// standby always knows how far behind it may be. Returns whether this
    /// attempt succeeded.
    pub async fn refresh(&self) -> bool {
        match self.store.read_snapshot().await {
            Ok(snapshot) => {
                let mut state = self.state.lock().await;
                let recovered = state.as_ref().is_some_and(|state| state.stale);
                *state = Some(SyncState {
                    revision: snapshot.revision,
                    generation: snapshot.generation,
                    synced_at: Instant::now(),
                    stale: false,
                });
                drop(state);
                if recovered {
                    adx_observability::info!("standby read-only synchronization recovered");
                }
                true
            }
            Err(error) => {
                let mut state = self.state.lock().await;
                if let Some(state) = state.as_mut() {
                    state.stale = true;
                }
                adx_observability::warn!(
                    "standby read-only sync failed; last snapshot marked stale: {error}"
                );
                false
            }
        }
    }

    /// The latest synchronization state, once any sync has succeeded.
    pub async fn state(&self) -> Option<SyncState> {
        self.state.lock().await.clone()
    }

    /// The standby role loop: read-only synchronization plus lease
    /// observation, competing for the lease once it is absent or expired.
    /// Returns `true` when the lease was acquired (this process should
    /// promote to primary and run `begin`), `false` when `stop` fired.
    ///
    /// While a live lease exists the loop stays completely read-only — no
    /// epoch, no writes, no discovery advertisement — so a running primary
    /// is unaffected by standby presence. Synchronization outcomes latch
    /// the standby's readiness (first success ready; later failures never
    /// un-ready it — a stale-but-synced standby is still promotable).
    pub async fn run_until_promotion(
        &self,
        lease: &CoordinatorLease,
        holder: &str,
        lease_ttl: Duration,
        sync_period: Duration,
        readiness: &crate::readiness::Readiness,
        stop: impl std::future::Future<Output = ()>,
    ) -> Result<bool> {
        let mut sync = tokio::time::interval(sync_period);
        let mut observe = tokio::time::interval(lease_ttl / 3);
        tokio::pin!(stop);
        loop {
            tokio::select! {
                _ = &mut stop => return Ok(false),
                _ = sync.tick() => readiness.standby(self.refresh().await),
                _ = observe.tick() => {
                    match lease.observe().await {
                        Ok(view) if view.live() => {}
                        Ok(_) => {
                            // Absent or expired: compete for takeover. Losing
                            // the race just means another standby won; keep
                            // observing.
                            match lease.acquire(holder, lease_ttl).await {
                                Ok(true) => return Ok(true),
                                Ok(false) => {}
                                Err(error) => adx_observability::warn!(
                                    "Coordinator lease takeover attempt unavailable: {error}"
                                ),
                            }
                        }
                        Err(error) => adx_observability::warn!(
                            "Coordinator lease observation unavailable: {error}"
                        ),
                    }
                }
            }
        }
    }
}
