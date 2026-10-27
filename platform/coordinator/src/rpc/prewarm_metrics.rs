use adx_core::{
    prewarm::{management::*, SlotState},
    Error, Result,
};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    RwLock,
};

#[derive(Default)]
pub(super) struct Metrics {
    acquisitions: AtomicU64,
    hits: AtomicU64,
    cold: AtomicU64,
    preparations: AtomicU64,
    failures: AtomicU64,
    preparation_millis: AtomicU64,
    claim_to_ready_count: AtomicU64,
    claim_to_ready_millis: AtomicU64,
    inventory: RwLock<String>,
}
fn add(counter: &AtomicU64, value: u64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |old| {
        Some(old.saturating_add(value))
    });
}
fn millis(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}
impl Metrics {
    pub(super) fn acquire(&self, selection: Selection) {
        add(&self.acquisitions, 1);
        match selection {
            Selection::Artifact => add(&self.hits, 1),
            Selection::Cold => add(&self.cold, 1),
        }
    }
    pub(super) fn prepare(&self, success: bool, duration: std::time::Duration) {
        add(&self.preparations, 1);
        add(&self.preparation_millis, millis(duration));
        if !success {
            add(&self.failures, 1);
        }
    }
    pub(super) fn ready(&self, duration: std::time::Duration) {
        add(&self.claim_to_ready_count, 1);
        add(&self.claim_to_ready_millis, millis(duration));
    }
    pub(super) fn observe(&self, pools: &[PoolRecord]) -> Result<()> {
        let mut out = adx_observability::metrics::Text::default();
        for pool in pools {
            let labels = [("pool_id", pool.pool_id.clone())];
            let mut bytes = 0u64;
            for state in [
                SlotState::Preparing,
                SlotState::Ready,
                SlotState::Quarantined,
                SlotState::Retiring,
            ] {
                let count = pool.artifacts.iter().filter(|a| a.state == state).count();
                let state = serde_json::to_value(state)
                    .map_err(|_| Error::Conflict)?
                    .as_str()
                    .ok_or(Error::Conflict)?
                    .to_owned();
                out.gauge(
                    "adx_prewarm_inventory",
                    &[("pool_id", pool.pool_id.clone()), ("state", state)],
                    u64::try_from(count).map_err(|_| Error::Conflict)?,
                );
            }
            for artifact in pool.artifacts.iter().filter(|a| a.counts_as_live()) {
                bytes = bytes
                    .checked_add(artifact.size_bytes)
                    .ok_or(Error::Conflict)?;
            }
            out.gauge("adx_prewarm_idle_disk_bytes", &labels, bytes);
            out.gauge(
                "adx_prewarm_max_live",
                &labels,
                u64::from(pool.policy.capacity.max_live),
            );
            out.gauge("adx_prewarm_observed_at_seconds", &labels, pool.observed_at);
        }
        *self
            .inventory
            .write()
            .map_err(|_| Error::Unavailable("prewarm metrics lock unavailable".into()))? =
            out.finish();
        Ok(())
    }
    pub(super) fn render(&self) -> Result<String> {
        let mut text = self
            .inventory
            .read()
            .map_err(|_| Error::Unavailable("prewarm metrics lock unavailable".into()))?
            .clone();
        for (name, value) in [
            ("acquisition_requests_total", &self.acquisitions),
            ("artifact_hit_requests_total", &self.hits),
            ("cold_requests_total", &self.cold),
            ("prepare_attempts_total", &self.preparations),
            ("prepare_failures_total", &self.failures),
            (
                "prepare_duration_milliseconds_total",
                &self.preparation_millis,
            ),
            ("claim_to_ready_count", &self.claim_to_ready_count),
            (
                "claim_to_ready_milliseconds_total",
                &self.claim_to_ready_millis,
            ),
        ] {
            text.push_str(&format!(
                "# TYPE adx_prewarm_{name} counter\nadx_prewarm_{name} {}\n",
                value.load(Ordering::Relaxed)
            ));
        }
        Ok(text)
    }
}
