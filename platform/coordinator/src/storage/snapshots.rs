//! Compare opaque records under the current Coordinator epoch. Snapshot publication
//! and final deletion update their Volume protections in the same transaction.
use super::*;
use adx_core::snapshots::{Reference, Snapshot, SnapshotState};
impl Session {
    pub(super) fn snapshots_key(&self) -> String {
        format!("{}:snapshots", self.store.key)
    }
    pub(super) async fn snapshot_raw(&self, id: &str) -> Result<Option<String>> {
        let [header_value] = self.store.fields([HEADER.into()]).await?;
        self.header(&header_value)?;
        let mut command = redis::cmd("HGET");
        command.arg(self.snapshots_key()).arg(id);
        self.store.query(command).await
    }
    async fn snapshot_cas(
        &self,
        id: &str,
        old: Option<&str>,
        next: &Snapshot,
        catalog: Option<&volumes::Catalog>,
    ) -> Result<bool> {
        next.validate()?;
        let _access = if next.state == SnapshotState::Ready {
            Some(
                self.store
                    .lock_artifacts(std::slice::from_ref(&next.artifact))?,
            )
        } else {
            None
        };
        let header_value = match catalog {
            Some(catalog) => Some(catalog.header_value.clone()),
            None => self.store.fields([HEADER.into()]).await?[0].clone(),
        };
        self.header(&header_value)?;
        let header = catalog.map(volumes::Catalog::advanced_header).transpose()?;
        let mut command = redis::cmd("EVAL");
        command
            .arg(
                r#"
            if redis.call('HGET', KEYS[1], 'epoch') ~= ARGV[1] then return 0 end
            if ARGV[6] ~= '' and redis.call('HGET',KEYS[1],'header') ~= ARGV[6] then return 0 end
            local old = redis.call('HGET', KEYS[2], ARGV[2])
            if ARGV[3] == '' then
                if old then return 0 end
            elseif old ~= ARGV[3] then return 0 end
            redis.call('HSET', KEYS[2], ARGV[2], ARGV[4])
            if ARGV[5] ~= '' then redis.call('HSET', KEYS[1], 'header', ARGV[5]) end
            for i = 8, #ARGV, 2 do
                if ARGV[i + 1] == '' then redis.call('HDEL', KEYS[3], ARGV[i])
                else redis.call('HSET', KEYS[3], ARGV[i], ARGV[i + 1]) end
            end
            return 1
        "#,
            )
            .arg(3)
            .arg(&self.store.key)
            .arg(self.snapshots_key())
            .arg(self.volumes_key())
            .arg(self.epoch.to_string())
            .arg(id)
            .arg(old.unwrap_or(""))
            .arg(encode(next)?)
            .arg(header.as_ref().map(encode).transpose()?.unwrap_or_default())
            .arg(
                catalog
                    .map(|catalog| catalog.header_value.as_str())
                    .unwrap_or(""),
            );
        if let Some(catalog) = catalog {
            catalog.append_writes(&mut command)?;
        } else {
            command.arg(0);
        }
        let mut inventory = self.inspection.lock().await;
        let mut committed = inventory.take();
        match self.store.query::<u64>(command).await {
            Ok(1) => {
                if let Some(inventory) = committed.as_mut() {
                    inventory.snapshots.insert(id.into(), next.clone());
                }
                *inventory = committed;
                if let (Some(header), Some(catalog)) = (header, catalog) {
                    self.store.notify_committed_change(
                        header.revision,
                        catalog.change_fields().collect(),
                    );
                }
                Ok(true)
            }
            Ok(_) => {
                *inventory = committed;
                Ok(false)
            }
            Err(error) => {
                *inventory = None;
                Err(error)
            }
        }
    }
    pub async fn publish_snapshot(&self, record: Snapshot) -> Result<Snapshot> {
        record.validate()?;
        if record.state != SnapshotState::Ready
            || record.revision != 1
            || !record.references.is_empty()
        {
            return Err(Error::Invalid(
                "new snapshot must be ready and unreferenced".into(),
            ));
        }
        for _ in 0..ATTEMPTS {
            if let Some(raw) = self.snapshot_raw(&record.id).await? {
                let old: Snapshot = decode(&raw)?;
                old.validate()?;
                return if old.same_content(&record) && old.state == SnapshotState::Ready {
                    Ok(old)
                } else {
                    Err(Error::Conflict)
                };
            }
            let mut next = record.clone();
            let catalog = if volumes::has_volumes(&next.template) {
                let mut catalog = self.volume_catalog().await?;
                catalog.protect_snapshot(&mut next)?;
                Some(catalog)
            } else {
                None
            };
            if self
                .snapshot_cas(&record.id, None, &next, catalog.as_ref())
                .await?
            {
                return Ok(next);
            }
        }
        Err(Error::Unavailable("snapshot publication contention".into()))
    }
    pub async fn get_snapshot(&self, id: &str) -> Result<Snapshot> {
        let raw = self.snapshot_raw(id).await?.ok_or(Error::NotFound)?;
        let s: Snapshot = decode(&raw)?;
        s.validate()?;
        Ok(s)
    }
    pub async fn list_snapshots(&self, tenant: &str) -> Result<Vec<Snapshot>> {
        self.filtered_snapshots(|s| {
            s.template.tenant_id == tenant && s.state == SnapshotState::Ready
        })
        .await
    }
    /// Complete artifact ownership for node recovery, including deferred deletion.
    pub async fn node_snapshots(&self, node: &str) -> Result<Vec<Snapshot>> {
        self.filtered_snapshots(|s| s.source_node_id == node && s.state != SnapshotState::Deleted)
            .await
    }
    pub async fn retained_snapshots(&self) -> Result<Vec<Snapshot>> {
        self.filtered_snapshots(|s| s.state != SnapshotState::Deleted)
            .await
    }
    async fn filtered_snapshots(
        &self,
        include: impl Fn(&Snapshot) -> bool,
    ) -> Result<Vec<Snapshot>> {
        let [header_value] = self.store.fields([HEADER.into()]).await?;
        self.header(&header_value)?;
        let mut command = redis::cmd("HVALS");
        command.arg(self.snapshots_key());
        let raw: Vec<String> = self.store.query(command).await?;
        let mut result = vec![];
        for encoded_snapshot in raw {
            let snapshot: Snapshot = decode(&encoded_snapshot)?;
            snapshot.validate()?;
            if include(&snapshot) {
                result.push(snapshot);
            }
        }
        result.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(result)
    }
    async fn mutate_snapshot(
        &self,
        id: &str,
        change: impl Fn(&mut Snapshot) -> Result<()>,
    ) -> Result<Snapshot> {
        for _ in 0..ATTEMPTS {
            let raw = self.snapshot_raw(id).await?.ok_or(Error::NotFound)?;
            let mut next: Snapshot = decode(&raw)?;
            next.validate()?;
            let revision = next.revision;
            change(&mut next)?;
            if next.revision == revision {
                return Ok(next);
            }
            let catalog =
                if next.state == SnapshotState::Deleted && volumes::has_volumes(&next.template) {
                    let mut catalog = self.volume_catalog().await?;
                    catalog.release_snapshot(&next)?;
                    Some(catalog)
                } else {
                    None
                };
            if self
                .snapshot_cas(id, Some(&raw), &next, catalog.as_ref())
                .await?
            {
                return Ok(next);
            }
        }
        Err(Error::Unavailable("snapshot reference contention".into()))
    }
    pub async fn acquire_snapshot(
        &self,
        id: &str,
        tenant: &str,
        reference: Reference,
    ) -> Result<Snapshot> {
        self.mutate_snapshot(id, |s| s.acquire(tenant, reference.clone()))
            .await
    }
    pub async fn release_snapshot(&self, id: &str, reference: Reference) -> Result<Snapshot> {
        self.mutate_snapshot(id, |s| s.release(&reference)).await
    }
    pub async fn delete_snapshot(&self, id: &str, tenant: &str) -> Result<Snapshot> {
        self.mutate_snapshot(id, |s| s.delete(tenant)).await
    }
    /// Call only after the storage backend confirms physical deletion.
    pub async fn finish_snapshot_deletion(&self, id: &str, revision: u64) -> Result<Snapshot> {
        self.mutate_snapshot(id, |s| s.deleted(revision)).await
    }
}
