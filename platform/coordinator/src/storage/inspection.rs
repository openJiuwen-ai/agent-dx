//! The active Coordinator's committed artifact inventory. Reads never contact Redis.
use super::*;
use adx_core::snapshots::Snapshot;
use std::{
    ops::{Deref, DerefMut},
    sync::atomic::AtomicBool,
};

/// Readiness is independent of a writer waiting for Redis. The committed view
/// itself remains protected by the original mutex and must never be read early.
#[derive(Default)]
pub(super) struct InspectionCache {
    value: Mutex<Option<InspectionInventory>>,
    ready: AtomicBool,
}

pub(super) struct InspectionGuard<'a> {
    value: tokio::sync::MutexGuard<'a, Option<InspectionInventory>>,
    ready: &'a AtomicBool,
}

impl InspectionCache {
    pub(super) async fn lock(&self) -> InspectionGuard<'_> {
        InspectionGuard {
            value: self.value.lock().await,
            ready: &self.ready,
        }
    }

    fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }
}

impl Deref for InspectionGuard<'_> {
    type Target = Option<InspectionInventory>;
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl DerefMut for InspectionGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

impl Drop for InspectionGuard<'_> {
    fn drop(&mut self) {
        // A successful writer restores its committed value before releasing
        // the guard. Failed/cancelled writes leave None and require recovery.
        self.ready.store(self.value.is_some(), Ordering::Release);
    }
}

#[derive(Default)]
pub(super) struct InspectionInventory {
    pub snapshots: BTreeMap<String, Snapshot>,
    pub holds: BTreeMap<String, artifacts::ExecutionHold>,
}

impl Session {
    /// Run at startup or after an ambiguous write, before serving cached catalogs.
    pub async fn refresh_inspection_inventory(&self) -> Result<()> {
        let mut inventory = self.inspection.lock().await;
        self.inspection.ready.store(false, Ordering::Release);
        *inventory = None;
        let [header] = self.store.fields([HEADER.into()]).await?;
        self.header(&header)?;
        let mut command = redis::cmd("HGETALL");
        command.arg(self.snapshots_key());
        let snapshots: BTreeMap<String, String> = self.store.query(command).await?;
        let mut command = redis::cmd("HGETALL");
        command.arg(format!("{}:input-holds", self.store.key));
        let holds: BTreeMap<String, String> = self.store.query(command).await?;
        let snapshots = snapshots
            .into_iter()
            .map(|(id, raw)| {
                let snapshot: Snapshot = decode(&raw)?;
                snapshot.validate()?;
                Ok((id, snapshot))
            })
            .collect::<Result<_>>()?;
        let holds = holds
            .into_iter()
            .map(|(id, raw)| Ok((id, decode(&raw)?)))
            .collect::<Result<_>>()?;
        *inventory = Some(InspectionInventory { snapshots, holds });
        Ok(())
    }

    pub async fn inspection_inventory(
        &self,
    ) -> Result<(Vec<Snapshot>, Vec<artifacts::ExecutionHold>)> {
        let inventory = self.inspection.lock().await;
        let inventory = inventory
            .as_ref()
            .ok_or_else(|| Error::Unavailable("inspection inventory requires recovery".into()))?;
        Ok((
            inventory.snapshots.values().cloned().collect(),
            inventory.holds.values().cloned().collect(),
        ))
    }

    pub(crate) async fn inspection_ready(&self) -> bool {
        self.inspection.is_ready()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn readiness_does_not_wait_for_a_writer_and_cancelled_writes_require_recovery() {
        let cache = Arc::new(InspectionCache::default());
        assert!(!cache.is_ready());
        *cache.lock().await = Some(InspectionInventory::default());
        assert!(cache.is_ready());
        let (entered, waiting) = tokio::sync::oneshot::channel();
        let writer = tokio::spawn({
            let cache = cache.clone();
            async move {
                let mut guard = cache.lock().await;
                let _committed = guard.take();
                entered.send(()).unwrap();
                std::future::pending::<()>().await;
            }
        });
        waiting.await.unwrap();
        assert!(
            cache.is_ready(),
            "a pending write does not invalidate the last known view"
        );
        writer.abort();
        assert!(writer.await.unwrap_err().is_cancelled());
        assert!(
            !cache.is_ready(),
            "a cancelled write must force authoritative recovery"
        );
        *cache.lock().await = Some(InspectionInventory::default());
        assert!(cache.is_ready());
    }
}
