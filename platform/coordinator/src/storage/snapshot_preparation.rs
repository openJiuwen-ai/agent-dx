//! Epoch-fenced preparation records and their atomic active-name index.
use super::*;
use adx_core::snapshot_preparation::{
    normalize_name, valid_preparation_id, validate_template_tenant, PreparationState,
    SnapshotPreparation, SnapshotTemplate, SnapshotTemplateUsage, SuccessfulPreparation,
};

const MAX_TEMPLATES: usize = 4096;
const MAX_RECORD_BYTES: usize = 1024 * 1024;
const MAX_LIST_BYTES: usize = 16 * 1024 * 1024;

pub(super) fn decode_template(raw: &str) -> Result<SnapshotTemplate> {
    if raw.len() > MAX_RECORD_BYTES {
        return Err(Error::Unavailable(
            "snapshot template record exceeds limit".into(),
        ));
    }
    let template: SnapshotTemplate = decode(raw)?;
    template.validate()?;
    Ok(template)
}

fn name_field(tenant: &str, name: &str) -> Result<String> {
    encode(&(tenant, name))
}

impl Session {
    pub(super) fn snapshot_templates_key(&self) -> String {
        format!("{}:snapshot-templates", self.store.key)
    }

    fn snapshot_template_names_key(&self) -> String {
        format!("{}:snapshot-template-names", self.store.key)
    }

    pub(super) async fn snapshot_preparation_field(
        &self,
        key: String,
        field: &str,
    ) -> Result<Option<String>> {
        let mut command = redis::cmd("EVAL");
        command
            .arg(
                r#"
            if redis.call('HGET',KEYS[1],'epoch') ~= ARGV[1] then return {0,''} end
            return {1,redis.call('HGET',KEYS[2],ARGV[2]) or ''}
        "#,
            )
            .arg(2)
            .arg(&self.store.key)
            .arg(key)
            .arg(self.epoch.to_string())
            .arg(field);
        let (active, value): (u64, String) = self.store.query(command).await?;
        if active != 1 {
            return Err(Error::Conflict);
        }
        Ok((!value.is_empty()).then_some(value))
    }

    async fn snapshot_template_cas(
        &self,
        old: Option<&str>,
        next: &SnapshotTemplate,
        expected_name: Option<&str>,
    ) -> Result<bool> {
        next.validate()?;
        let encoded = encode(next)?;
        if encoded.len() > MAX_RECORD_BYTES {
            return Err(Error::Invalid(
                "snapshot template record exceeds 1 MiB".into(),
            ));
        }
        // Join the snapshot inventory ordering before reading the exact bytes that
        // will be bound. Reference mutations must not race a speculative cache update.
        let previous = old.map(decode_template).transpose()?;
        let first_ready = next.preparation.state == PreparationState::Ready
            && previous
                .as_ref()
                .is_none_or(|previous| previous.preparation.state != PreparationState::Ready);
        let mut inventory = if first_ready {
            Some(self.inspection.lock().await)
        } else {
            None
        };
        let binding = if first_ready {
            use adx_core::snapshots::{Snapshot, SnapshotKind, SnapshotState};
            let id = next
                .preparation
                .snapshot_id
                .as_ref()
                .ok_or(Error::Conflict)?;
            let raw = self.snapshot_raw(id).await?.ok_or(Error::Conflict)?;
            let mut snapshot: Snapshot = decode(&raw)?;
            snapshot.validate()?;
            if snapshot.kind != SnapshotKind::Golden
                || snapshot.state != SnapshotState::Ready
                || snapshot.template.tenant_id != next.tenant_id
                || previous
                    .as_ref()
                    .and_then(|previous| previous.preparation.spec.as_ref())
                    .or(next.preparation.spec.as_ref())
                    .is_none_or(|spec| !snapshot.matches_template(spec))
                || snapshot
                    .preparation_template_id
                    .as_ref()
                    .is_some_and(|id| id != &next.id)
            {
                return Err(Error::Conflict);
            }
            if snapshot.preparation_template_id.is_none() {
                snapshot.preparation_template_id = Some(next.id.clone());
                snapshot.revision = snapshot.revision.checked_add(1).ok_or(Error::Conflict)?;
            }
            Some((raw, snapshot))
        } else {
            None
        };
        let mut command = redis::cmd("EVAL");
        command
            .arg(
                r#"
            if redis.call('HGET',KEYS[1],'epoch') ~= ARGV[1] then return -1 end
            local old=redis.call('HGET',KEYS[2],ARGV[2]) or ''
            if old ~= ARGV[3] then return 0 end
            local indexed=redis.call('HGET',KEYS[3],ARGV[5]) or ''
            if indexed ~= ARGV[6] then return 0 end
            if old == '' and redis.call('HLEN',KEYS[2]) >= tonumber(ARGV[8]) then return 2 end
            if ARGV[9] ~= '' and (redis.call('HGET',KEYS[4],ARGV[9]) or '') ~= ARGV[10] then return 0 end
            redis.call('HSET',KEYS[2],ARGV[2],ARGV[4])
            if ARGV[9] ~= '' then redis.call('HSET',KEYS[4],ARGV[9],ARGV[11]) end
            if ARGV[7] == 'active' then
                redis.call('HSET',KEYS[3],ARGV[5],ARGV[2])
            elseif indexed == ARGV[2] then
                redis.call('HDEL',KEYS[3],ARGV[5])
            end
            return 1
        "#,
            )
            .arg(4)
            .arg(&self.store.key)
            .arg(self.snapshot_templates_key())
            .arg(self.snapshot_template_names_key())
            .arg(self.snapshots_key())
            .arg(self.epoch.to_string())
            .arg(&next.id)
            .arg(old.unwrap_or(""))
            .arg(encoded)
            .arg(name_field(&next.tenant_id, &next.name)?)
            .arg(expected_name.unwrap_or(""))
            .arg(if next.deleted { "deleted" } else { "active" })
            .arg(MAX_TEMPLATES)
            .arg(binding.as_ref().map_or("", |(_, snapshot)| snapshot.id.as_str()))
            .arg(binding.as_ref().map_or("", |(raw, _)| raw.as_str()))
            .arg(binding.as_ref().map(|(_, snapshot)| encode(snapshot)).transpose()?.unwrap_or_default());
        // Taking the committed view also invalidates it if this future is
        // cancelled while Redis may already have accepted the write.
        let mut committed = inventory.as_mut().and_then(|inventory| inventory.take());
        let result = self.store.query::<i64>(command).await;
        if let Some(inventory) = &mut inventory {
            if result.is_ok() {
                if let (Ok(1), Some(committed), Some((_, snapshot))) =
                    (&result, committed.as_mut(), binding)
                {
                    committed.snapshots.insert(snapshot.id.clone(), snapshot);
                }
                **inventory = committed;
            }
        }
        match result? {
            1 => Ok(true),
            0 => Ok(false),
            2 => Err(Error::Invalid("snapshot template limit reached".into())),
            _ => Err(Error::Conflict),
        }
    }

    /// Derive a family write from immutable snapshot provenance. The publication
    /// transaction compares these old bytes before writing either creation or usage.
    pub(super) async fn snapshot_template_spawn_write(
        &self,
        snapshot_id: &str,
        tenant: &str,
        settled_at_unix_ms: u64,
    ) -> Result<Vec<String>> {
        let snapshot = match self.get_snapshot(snapshot_id).await {
            Ok(snapshot) => snapshot,
            // Legacy creation records may no longer retain their input capture.
            Err(Error::NotFound) => return Ok(Vec::new()),
            Err(error) => return Err(error),
        };
        if snapshot.template.tenant_id != tenant {
            return Err(Error::Conflict);
        }
        let Some(id) = snapshot.preparation_template_id else {
            return Ok(Vec::new());
        };
        let Some(raw) = self
            .snapshot_preparation_field(self.snapshot_templates_key(), &id)
            .await?
        else {
            return Ok(Vec::new());
        };
        let mut family = decode_template(&raw)?;
        if family.tenant_id != tenant {
            return Err(Error::Conflict);
        }
        let Some(usage) = &mut family.usage else {
            return Ok(Vec::new());
        };
        usage.spawn_count = usage.spawn_count.checked_add(1).ok_or(Error::Conflict)?;
        usage.last_spawned_at_unix_ms = Some(
            usage
                .last_spawned_at_unix_ms
                .map_or(settled_at_unix_ms, |previous| {
                    previous.max(settled_at_unix_ms)
                }),
        );
        family.revision = family.revision.checked_add(1).ok_or(Error::Conflict)?;
        family.updated_at_unix_ms = adx_core::observation::now_millis()?;
        family.validate()?;
        Ok(vec![id, raw, encode(&family)?])
    }

    /// Register a fresh preparation while preserving an active name's family ID.
    /// A family admits one preparation at a time; completed captures remain independent.
    pub async fn register_snapshot_preparation(
        &self,
        tenant: &str,
        name: &str,
        resources: adx_core::Resources,
    ) -> Result<(SnapshotTemplate, SnapshotPreparation)> {
        validate_template_tenant(tenant)?;
        let name = normalize_name(name)?;
        resources.validate()?;
        let created_at_unix_ms = adx_core::observation::now_millis()?;
        let preparation = SnapshotPreparation {
            id: uuid::Uuid::new_v4().to_string(),
            created_at_unix_ms,
            updated_at_unix_ms: created_at_unix_ms,
            resources,
            state: PreparationState::Registered,
            spec: None,
            snapshot_id: None,
            expected_revision: None,
            error: None,
            cleanup_pending: false,
        };
        let fresh_id = uuid::Uuid::new_v4().to_string();
        let index = name_field(tenant, &name)?;
        for _ in 0..ATTEMPTS {
            let indexed = self
                .snapshot_preparation_field(self.snapshot_template_names_key(), &index)
                .await?;
            let raw = match &indexed {
                Some(id) => Some(
                    self.snapshot_preparation_field(self.snapshot_templates_key(), id)
                        .await?
                        .ok_or_else(|| {
                            Error::Unavailable("snapshot template index has no record".into())
                        })?,
                ),
                None => None,
            };
            let existing = raw.as_deref().map(decode_template).transpose()?;
            if existing
                .as_ref()
                .is_some_and(|template| template.tenant_id != tenant || template.name != name)
            {
                return Err(Error::Unavailable(
                    "snapshot template index mismatch".into(),
                ));
            }
            let mut next = existing
                .as_ref()
                .filter(|template| !template.deleted)
                .cloned()
                .unwrap_or_else(|| SnapshotTemplate {
                    id: fresh_id.clone(),
                    tenant_id: tenant.into(),
                    name: name.clone(),
                    created_at_unix_ms,
                    updated_at_unix_ms: created_at_unix_ms,
                    usage: Some(SnapshotTemplateUsage {
                        spawn_count: 0,
                        last_spawned_at_unix_ms: None,
                    }),
                    revision: 0,
                    deleted: false,
                    preparation: preparation.clone(),
                    latest_successful: None,
                });
            if existing.as_ref().is_some_and(|template| {
                !template.deleted
                    && (template.preparation.cleanup_pending
                        || !matches!(
                            template.preparation.state,
                            PreparationState::Ready | PreparationState::Failed
                        ))
            }) {
                return Err(Error::Invalid(
                    "snapshot template preparation is busy".into(),
                ));
            }
            next.revision = next.revision.checked_add(1).ok_or(Error::Conflict)?;
            next.preparation = preparation.clone();
            next.updated_at_unix_ms = adx_core::observation::now_millis()?;
            let old = existing
                .as_ref()
                .filter(|template| !template.deleted)
                .and(raw.as_deref());
            if self
                .snapshot_template_cas(old, &next, indexed.as_deref())
                .await?
            {
                return Ok((next, preparation));
            }
        }
        Err(Error::Unavailable(
            "snapshot preparation registration contention".into(),
        ))
    }

    /// Read a tenant-owned family by UUID or normalized active name. Deleted UUIDs remain
    /// readable for cleanup; their names can already point to a new family.
    pub async fn get_snapshot_template(
        &self,
        tenant: &str,
        id_or_name: &str,
    ) -> Result<SnapshotTemplate> {
        validate_template_tenant(tenant)?;
        if valid_preparation_id(id_or_name) {
            if let Some(raw) = self
                .snapshot_preparation_field(self.snapshot_templates_key(), id_or_name)
                .await?
            {
                let template = decode_template(&raw)?;
                return if template.tenant_id == tenant {
                    Ok(template)
                } else {
                    Err(Error::NotFound)
                };
            }
        }
        let name = normalize_name(id_or_name)?;
        let index = name_field(tenant, &name)?;
        let id = self
            .snapshot_preparation_field(self.snapshot_template_names_key(), &index)
            .await?
            .ok_or(Error::NotFound)?;
        let raw = self
            .snapshot_preparation_field(self.snapshot_templates_key(), &id)
            .await?
            .ok_or(Error::NotFound)?;
        let template = decode_template(&raw)?;
        if template.tenant_id != tenant || template.name != name || template.deleted {
            return Err(Error::NotFound);
        }
        Ok(template)
    }

    /// List this tenant's retained families, including logical deletions for cleanup recovery.
    pub async fn list_snapshot_templates(&self, tenant: &str) -> Result<Vec<SnapshotTemplate>> {
        validate_template_tenant(tenant)?;
        Ok(self
            .retained_snapshot_templates()
            .await?
            .into_iter()
            .filter(|template| template.tenant_id == tenant)
            .collect())
    }

    /// Discover pending preparation work independently of admitted Environment records.
    /// Includes all tenants and tombstones, sorted by ID, under the current epoch.
    /// Reads only this domain hash, bounded at 4096 records and 16 MiB encoded payload.
    pub(crate) async fn retained_snapshot_templates(&self) -> Result<Vec<SnapshotTemplate>> {
        let mut command = redis::cmd("EVAL");
        command
            .arg(
                r#"
            if redis.call('HGET',KEYS[1],'epoch') ~= ARGV[1] then return {0,{}} end
            if redis.call('HLEN',KEYS[2]) > tonumber(ARGV[2]) then return {2,{}} end
            local fields=redis.call('HKEYS',KEYS[2]); local values={}; local bytes=0
            for _,field in ipairs(fields) do
                local value=redis.call('HGET',KEYS[2],field)
                bytes=bytes+#value
                if bytes > tonumber(ARGV[3]) then return {2,{}} end
                table.insert(values,value)
            end
            return {1,values}
        "#,
            )
            .arg(2)
            .arg(&self.store.key)
            .arg(self.snapshot_templates_key())
            .arg(self.epoch.to_string())
            .arg(MAX_TEMPLATES)
            .arg(MAX_LIST_BYTES);
        let (status, values): (u64, Vec<String>) = self.store.query(command).await?;
        if status == 0 {
            return Err(Error::Conflict);
        }
        if status != 1 {
            return Err(Error::Unavailable(
                "snapshot template listing limit reached".into(),
            ));
        }
        let mut templates = Vec::new();
        for raw in values {
            templates.push(decode_template(&raw)?);
        }
        templates.sort_by(|left, right| left.id.cmp(&right.id));
        Ok(templates)
    }

    /// Remove only a completed logical deletion. Active or uncleaned preparations remain durable.
    /// The epoch and exact record are fenced atomically; a reused name keeps its new owner.
    pub async fn remove_finished_snapshot_template(&self, id: &str) -> Result<bool> {
        if !valid_preparation_id(id) {
            return Err(Error::Invalid("invalid snapshot template ID".into()));
        }
        for _ in 0..ATTEMPTS {
            let Some(raw) = self
                .snapshot_preparation_field(self.snapshot_templates_key(), id)
                .await?
            else {
                return Ok(false);
            };
            let template = decode_template(&raw)?;
            if !template.deleted
                || template.preparation.cleanup_pending
                || matches!(
                    template.preparation.state,
                    PreparationState::Creating
                        | PreparationState::Snapshotting
                        | PreparationState::Cleaning
                )
            {
                return Ok(false);
            }
            let mut command = redis::cmd("EVAL");
            command
                .arg(
                    r#"
                if redis.call('HGET',KEYS[1],'epoch') ~= ARGV[1] then return -1 end
                if (redis.call('HGET',KEYS[2],ARGV[2]) or '') ~= ARGV[3] then return 0 end
                redis.call('HDEL',KEYS[2],ARGV[2])
                if redis.call('HGET',KEYS[3],ARGV[4]) == ARGV[2] then
                    redis.call('HDEL',KEYS[3],ARGV[4])
                end
                return 1
            "#,
                )
                .arg(3)
                .arg(&self.store.key)
                .arg(self.snapshot_templates_key())
                .arg(self.snapshot_template_names_key())
                .arg(self.epoch.to_string())
                .arg(id)
                .arg(raw)
                .arg(name_field(&template.tenant_id, &template.name)?);
            match self.store.query::<i64>(command).await? {
                1 => return Ok(true),
                0 => (),
                _ => return Err(Error::Conflict),
            }
        }
        Err(Error::Unavailable(
            "snapshot template removal contention".into(),
        ))
    }

    /// CAS-update mutable preparation state and logical deletion, advancing revision once.
    /// The closure may run again on contention. Family and registered build identities,
    /// resources are immutable; stale epochs and identity edits conflict.
    pub async fn update_snapshot_template(
        &self,
        id: &str,
        change: impl Fn(&mut SnapshotTemplate) -> Result<()>,
    ) -> Result<SnapshotTemplate> {
        if !valid_preparation_id(id) {
            return Err(Error::Invalid("invalid snapshot template ID".into()));
        }
        for _ in 0..ATTEMPTS {
            let raw = self
                .snapshot_preparation_field(self.snapshot_templates_key(), id)
                .await?
                .ok_or(Error::NotFound)?;
            let previous = decode_template(&raw)?;
            let index = name_field(&previous.tenant_id, &previous.name)?;
            let indexed = self
                .snapshot_preparation_field(self.snapshot_template_names_key(), &index)
                .await?;
            let mut next = previous.clone();
            change(&mut next)?;
            if next.id != previous.id
                || next.tenant_id != previous.tenant_id
                || next.name != previous.name
                || next.created_at_unix_ms != previous.created_at_unix_ms
                || next.revision != previous.revision
                || next.updated_at_unix_ms != previous.updated_at_unix_ms
                || next.usage != previous.usage
                || next.preparation.updated_at_unix_ms != previous.preparation.updated_at_unix_ms
                || (previous.deleted && !next.deleted)
                || next.preparation.id != previous.preparation.id
                || next.preparation.created_at_unix_ms != previous.preparation.created_at_unix_ms
                || next.preparation.resources != previous.preparation.resources
                || next.latest_successful != previous.latest_successful
                || (!previous.deleted && indexed.as_deref() != Some(id))
            {
                return Err(Error::Conflict);
            }
            next.revision = next.revision.checked_add(1).ok_or(Error::Conflict)?;
            next.updated_at_unix_ms = adx_core::observation::now_millis()?;
            if next.preparation != previous.preparation {
                next.preparation.updated_at_unix_ms = next.updated_at_unix_ms;
            }
            if next.preparation.state == PreparationState::Ready {
                let snapshot_id =
                    next.preparation.snapshot_id.clone().ok_or_else(|| {
                        Error::Invalid("ready preparation has no snapshot".into())
                    })?;
                next.latest_successful = Some(SuccessfulPreparation {
                    preparation_id: next.preparation.id.clone(),
                    snapshot_id,
                    created_at_unix_ms: next.preparation.created_at_unix_ms,
                    updated_at_unix_ms: next.preparation.updated_at_unix_ms,
                });
            }
            if self
                .snapshot_template_cas(Some(&raw), &next, indexed.as_deref())
                .await?
            {
                return Ok(next);
            }
        }
        Err(Error::Unavailable(
            "snapshot template update contention".into(),
        ))
    }
}
