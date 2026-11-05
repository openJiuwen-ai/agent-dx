//! Compare opaque records under the current Coordinator epoch. Snapshot publication
//! and final deletion update their Volume protections in the same transaction.
use super::*;
use adx_core::snapshots::{Reference, Snapshot, SnapshotState};

struct SourceFence<'a> {
    environment_field: &'a str,
    environment_value: &'a str,
    node_session_field: &'a str,
    node_session_id: &'a str,
}

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
    async fn snapshot_cas(&self, id: &str, old: Option<&str>, next: &mut Snapshot) -> Result<bool> {
        self.snapshot_cas_with_source(id, old, next, None).await
    }
    async fn snapshot_cas_with_source(
        &self,
        id: &str,
        old: Option<&str>,
        next: &mut Snapshot,
        source: Option<SourceFence<'_>>,
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
        // The inspection latch already serializes these commits. Read the
        // compared header after joining that queue, rather than letting each
        // waiter carry a header made stale by the writers ahead of it.
        let mut inventory = self.inspection.lock().await;
        // Volume writes must be derived from the same fresh catalog as their
        // compared header. Replacing only the header of a queued catalog could
        // overwrite snapshot holds committed by an earlier writer.
        let catalog = if volumes::has_volumes(&next.template)
            && (old.is_none() || next.state == SnapshotState::Deleted)
        {
            let mut catalog = self.volume_catalog().await?;
            if old.is_none() {
                catalog.protect_snapshot(next)?;
            } else {
                catalog.release_snapshot(next)?;
            }
            Some(catalog)
        } else {
            None
        };
        let header_value = match catalog.as_ref() {
            Some(catalog) => Some(catalog.header_value.clone()),
            None => self.store.fields([HEADER.into()]).await?[0].clone(),
        };
        let mut current_header = self.header(&header_value)?;
        let header = if let Some(catalog) = catalog.as_ref() {
            Some(catalog.advanced_header()?)
        } else if next.state == SnapshotState::Ready {
            current_header.advance()?;
            Some(current_header)
        } else {
            None
        };
        let mut command = redis::cmd("EVAL");
        command
            .arg(
                r#"
            if redis.call('HGET', KEYS[1], 'epoch') ~= ARGV[1] then return 0 end
            if ARGV[6] ~= '' and redis.call('HGET',KEYS[1],'header') ~= ARGV[6] then return 0 end
            if ARGV[7] ~= '' and redis.call('HGET',KEYS[1],ARGV[7]) ~= ARGV[8] then return 0 end
            if ARGV[9] ~= '' and redis.call('HGET',KEYS[1],ARGV[9]) ~= ARGV[10] then return 0 end
            local old = redis.call('HGET', KEYS[2], ARGV[2])
            if ARGV[3] == '' then
                if old then return 0 end
            elseif old ~= ARGV[3] then return 0 end
            redis.call('HSET', KEYS[2], ARGV[2], ARGV[4])
            if ARGV[5] ~= '' then redis.call('HSET', KEYS[1], 'header', ARGV[5]) end
            local cursor = 12
            for _ = 1, tonumber(ARGV[11]) do
                local key = ARGV[cursor]
                local value = ARGV[cursor + 1]
                if value == '' then redis.call('HDEL', KEYS[3], key)
                else redis.call('HSET', KEYS[3], key, value) end
                cursor = cursor + 2
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
            .arg(header_value.as_deref().unwrap_or(""));
        command
            .arg(source.as_ref().map_or("", |f| f.environment_field))
            .arg(source.as_ref().map_or("", |f| f.environment_value))
            .arg(source.as_ref().map_or("", |f| f.node_session_field))
            .arg(source.as_ref().map_or("", |f| f.node_session_id));
        if let Some(catalog) = catalog.as_ref() {
            catalog.append_writes(&mut command)?;
        } else {
            command.arg(0);
        }
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
        self.publish_snapshot_checked(record, None).await
    }

    /// Node publication compares the exact source bytes in the same CAS. A
    /// retirement that finishes outside the RPC state lock cannot be raced.
    pub(crate) async fn publish_snapshot_from_source(
        &self,
        record: Snapshot,
        assignment: &Assignment,
        node_session_id: &str,
    ) -> Result<Snapshot> {
        self.publish_snapshot_checked(record, Some((assignment, node_session_id)))
            .await
    }

    async fn publish_snapshot_checked(
        &self,
        record: Snapshot,
        publisher: Option<(&Assignment, &str)>,
    ) -> Result<Snapshot> {
        record.validate()?;
        if record.state != SnapshotState::Ready
            || record.revision != 1
            || !record.references.is_empty()
        {
            return Err(Error::Invalid(
                "new snapshot must be ready and unreferenced".into(),
            ));
        }
        let node_session_field = format!("node-session:{}", record.source_node_id);
        for _ in 0..ATTEMPTS {
            if let Some((assignment, session_id)) = publisher {
                let [current_session] = self.store.fields([node_session_field.clone()]).await?;
                if assignment.node_id != record.source_node_id
                    || current_session.as_deref() != Some(session_id)
                {
                    return Err(Error::Conflict);
                }
            }
            if let Some(raw) = self.snapshot_raw(&record.id).await? {
                let old: Snapshot = decode(&raw)?;
                old.validate()?;
                return if old.same_content(&record) && old.state == SnapshotState::Ready {
                    Ok(old)
                } else {
                    Err(Error::Conflict)
                };
            }
            let source_field = format!("environment:{}", record.template.id);
            let source = if let Some((assignment, _)) = publisher {
                let [raw] = self.store.fields([source_field.clone()]).await?;
                let raw = raw.ok_or(Error::Conflict)?;
                let stored: StoredEnvironment = decode(&raw)?;
                stored.validate()?;
                if stored.assignment != *assignment
                    || stored.invalidated
                    || !record.matches_template(&stored.spec)
                    || !stored.result.as_ref().is_some_and(|result| {
                        result.runtime.id == record.source_runtime_id
                            && result.execution_artifacts.contains(&record.artifact)
                    })
                {
                    return Err(Error::Conflict);
                }
                Some(raw)
            } else {
                None
            };
            let mut next = record.clone();
            if self
                .snapshot_cas_with_source(
                    &record.id,
                    None,
                    &mut next,
                    source.as_deref().map(|raw| SourceFence {
                        environment_field: source_field.as_str(),
                        environment_value: raw,
                        node_session_field: node_session_field.as_str(),
                        node_session_id: publisher.map_or("", |(_, id)| id),
                    }),
                )
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
        // The active Coordinator maintains this committed view under the same
        // latch as snapshot CAS. Filter before cloning so Deleted history is
        // neither transferred from Redis nor copied for each catalog query.
        // Standalone storage sessions without an initialized view retain their
        // authoritative durable read, including validation of stored records.
        if let Some(inventory) = self.inspection.lock().await.as_ref() {
            let mut result: Vec<_> = inventory
                .snapshots
                .values()
                .filter(|snapshot| include(snapshot))
                .cloned()
                .collect();
            result.sort_by(|a, b| a.id.cmp(&b.id));
            return Ok(result);
        }
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
            if self.snapshot_cas(id, Some(&raw), &mut next).await? {
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
    /// Release a consumed input only while its exact durable owner is unchanged.
    /// Maintenance can therefore wait for storage without the scheduler mutex.
    pub(crate) async fn release_consumed_snapshot(
        &self,
        id: &str,
        expected: &StoredEnvironment,
    ) -> Result<bool> {
        let field = format!("environment:{}", expected.spec.id);
        for _ in 0..ATTEMPTS {
            let [owner] = self.store.fields([field.clone()]).await?;
            let Some(owner) = owner else { return Ok(false) };
            let current: StoredEnvironment = decode(&owner)?;
            current.validate()?;
            if current != *expected {
                return Ok(false);
            }
            let Some(raw) = self.snapshot_raw(id).await? else {
                return Ok(false);
            };
            let mut next: Snapshot = decode(&raw)?;
            next.validate()?;
            if expected.spec.snapshot_id.as_deref() != Some(id)
                || !expected.result.as_ref().is_some_and(|record| {
                    !record.execution_artifacts.contains(&next.artifact)
                        && record
                            .snapshot_source
                            .as_ref()
                            .is_none_or(|p| p.artifact != next.artifact)
                        && (record
                            .checkpoint
                            .as_ref()
                            .is_some_and(|p| p.artifact != next.artifact)
                            || record.state == EnvironmentState::Deleted)
                })
            {
                return Ok(false);
            }
            let revision = next.revision;
            next.release(&Reference::Restore {
                environment_id: expected.spec.id.clone(),
            })?;
            if next.revision == revision {
                return Ok(true);
            }
            if self
                .snapshot_cas_with_source(
                    id,
                    Some(&raw),
                    &mut next,
                    Some(SourceFence {
                        environment_field: &field,
                        environment_value: &owner,
                        node_session_field: "",
                        node_session_id: "",
                    }),
                )
                .await?
            {
                return Ok(true);
            }
        }
        Err(Error::Unavailable(
            "consumed snapshot owner changed; retry maintenance".into(),
        ))
    }
    pub async fn delete_snapshot(&self, id: &str, tenant: &str) -> Result<Snapshot> {
        self.mutate_snapshot(id, |s| s.delete(tenant)).await
    }
    /// Call only after the storage backend confirms physical deletion.
    pub async fn finish_snapshot_deletion(&self, id: &str, revision: u64) -> Result<Snapshot> {
        self.mutate_snapshot(id, |s| s.deleted(revision)).await
    }
}
