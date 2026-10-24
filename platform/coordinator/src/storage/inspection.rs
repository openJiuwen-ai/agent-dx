//! The active Coordinator's committed artifact inventory. Reads never contact Redis.
use super::*;
use adx_core::snapshots::Snapshot;

#[derive(Default)]
pub(super) struct InspectionInventory {
    pub snapshots: BTreeMap<String, Snapshot>,
    pub holds: BTreeMap<String, artifacts::ExecutionHold>,
}

impl Session {
    /// Run at startup or after an ambiguous write, before serving cached catalogs.
    pub async fn refresh_inspection_inventory(&self) -> Result<()> {
        let mut inventory = self.inspection.lock().await;
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
        self.inspection.lock().await.is_some()
    }
}
