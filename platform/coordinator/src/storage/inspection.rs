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

impl InspectionInventory {
    fn node_snapshots(&self, node_id: &str) -> Vec<Snapshot> {
        self.snapshots
            .values()
            .filter(|snapshot| {
                snapshot.source_node_id == node_id
                    && snapshot.state != adx_core::snapshots::SnapshotState::Deleted
            })
            .cloned()
            .collect()
    }
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

    pub(crate) async fn node_inspection_inventory(
        &self,
        node_id: &str,
    ) -> Result<(Vec<Snapshot>, Vec<artifacts::ExecutionHold>)> {
        let inventory = self.inspection.lock().await;
        let inventory = inventory
            .as_ref()
            .ok_or_else(|| Error::Unavailable("inspection inventory requires recovery".into()))?;
        Ok((
            inventory.node_snapshots(node_id),
            inventory.holds.values().cloned().collect(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(target_os = "linux")]
    #[test]
    fn node_inspection_clone_budget() {
        const CHILD: &str = "ADX_NODE_INSPECTION_CLONE_BUDGET_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let result = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "storage::inspection::tests::node_inspection_clone_budget",
                    "--nocapture",
                    "--test-threads=1",
                ])
                .env(CHILD, "1")
                .output()
                .unwrap();
            assert!(
                result.status.success(),
                "isolated allocation regression failed: {}{}",
                String::from_utf8_lossy(&result.stdout),
                String::from_utf8_lossy(&result.stderr)
            );
            eprint!("{}", String::from_utf8_lossy(&result.stderr));
            return;
        }
        let mut inventory = InspectionInventory::default();
        for index in 0..67 {
            let spec: EnvironmentSpec = serde_json::from_value(serde_json::json!({
                "id": format!("source-{index}"), "tenant_id": "tenant", "image": "image",
                "runtime_class": "runsc", "priority": 0,
                "resources": {"cpu_millis": 100, "memory_bytes": 128, "disk_bytes": 0}
            }))
            .unwrap();
            let mut snapshot = Snapshot::new(
                format!("snapshot-{index:04}"),
                vec![],
                spec,
                if index == 66 { "node-b" } else { "node-a" }.into(),
                format!("source-{index}-1"),
                adx_core::CheckpointArtifact {
                    storage: "local".into(),
                    scope: String::new(),
                    location: format!("artifact-{index}"),
                    size_bytes: 1,
                },
            )
            .unwrap();
            snapshot
                .template
                .env
                .insert("fixture".into(), "p".repeat(2 * 1024 * 1024));
            snapshot.state = if index < 64 {
                adx_core::snapshots::SnapshotState::Deleted
            } else if index == 65 {
                adx_core::snapshots::SnapshotState::Deleting
            } else {
                adx_core::snapshots::SnapshotState::Ready
            };
            inventory.snapshots.insert(snapshot.id.clone(), snapshot);
        }
        fn peak_kib() -> u64 {
            std::fs::read_to_string("/proc/self/status")
                .unwrap()
                .lines()
                .find_map(|line| {
                    line.strip_prefix("VmHWM:")
                        .map(|v| v.split_whitespace().next().unwrap().parse().unwrap())
                })
                .unwrap()
        }
        let before = peak_kib();
        let snapshots = inventory.node_snapshots("node-a");
        let extra_kib = peak_kib().saturating_sub(before);
        assert_eq!(snapshots.len(), 2);
        assert!(snapshots
            .iter()
            .all(|snapshot| snapshot.source_node_id == "node-a"
                && snapshot.state != adx_core::snapshots::SnapshotState::Deleted));
        eprintln!("node inspection extra peak KiB: {extra_kib}");
        assert!(
            extra_kib < 16 * 1024,
            "inspection copied unrelated historical metadata: {extra_kib} KiB"
        );
    }

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
