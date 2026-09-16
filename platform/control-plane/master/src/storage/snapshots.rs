//! Compare opaque records under the current Master epoch. Snapshot mutations do
//! not change Instance route versions or the scheduling catalog.
use super::*;
use adx_core::snapshots::{Reference, Snapshot, SnapshotState};
impl Session {
    fn snapshots_key(&self) -> String {
        format!("{}:snapshots", self.store.key)
    }
    async fn snapshot_raw(&self, id: &str) -> Result<Option<String>> {
        self.header(&self.store.fields(&[HEADER.into()]).await?[0])?;
        let mut command = redis::cmd("HGET");
        command.arg(self.snapshots_key()).arg(id);
        self.store.query(command).await
    }
    async fn snapshot_cas(&self, id: &str, old: Option<&str>, next: &Snapshot) -> Result<bool> {
        next.validate()?;
        let h = self.store.fields(&[HEADER.into()]).await?;
        self.header(&h[0])?;
        let mut command = redis::cmd("EVAL");
        command
            .arg(
                r#"
            if redis.call('HGET', KEYS[1], 'header') ~= ARGV[1] then return 0 end
            local old = redis.call('HGET', KEYS[2], ARGV[2])
            if ARGV[3] == '' then
                if old then return 0 end
            elseif old ~= ARGV[3] then return 0 end
            redis.call('HSET', KEYS[2], ARGV[2], ARGV[4]); return 1
        "#,
            )
            .arg(2)
            .arg(&self.store.key)
            .arg(self.snapshots_key())
            .arg(h[0].as_deref().ok_or(Error::Conflict)?)
            .arg(id)
            .arg(old.unwrap_or(""))
            .arg(encode(next)?);
        Ok(self.store.query::<u64>(command).await? == 1)
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
            if self.snapshot_cas(&record.id, None, &record).await? {
                return Ok(record);
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
        self.header(&self.store.fields(&[HEADER.into()]).await?[0])?;
        let mut command = redis::cmd("HVALS");
        command.arg(self.snapshots_key());
        let raw: Vec<String> = self.store.query(command).await?;
        let mut result = vec![];
        for s in raw {
            let s: Snapshot = decode(&s)?;
            s.validate()?;
            if include(&s) {
                result.push(s);
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
            if next.revision == revision || self.snapshot_cas(id, Some(&raw), &next).await? {
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
