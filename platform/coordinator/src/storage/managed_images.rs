//! Opaque-byte CAS keeps publication receipts and release tombstones across restarts.
use super::*;
use adx_core::managed_images::{
    image_references, valid_id, validate_dependencies, ImagePublisher, ManagedImage,
    ManagedImageSummary, RegistryScope,
};
use adx_core::snapshots::{Snapshot, SnapshotState};

pub(crate) const IMAGE_REFERENCE_SAMPLE: usize = 32;

pub(crate) struct ImageBuilder {
    pub publisher: ImagePublisher,
    field: String,
    raw: String,
}

pub(super) fn image_gates_key(control: &str) -> String {
    format!("{control}:managed-image-admissions")
}

fn gate_field(tenant: &str, id: &str) -> String {
    format!("{tenant}:{id}")
}

pub(super) fn image_gate_fields(spec: &EnvironmentSpec) -> Vec<String> {
    spec.execution
        .managed_image_publications
        .iter()
        .map(|id| gate_field(&spec.tenant_id, id))
        .collect()
}

// Do all checks before incrementing. Keep the hot-path gate value small and
// independent of the publication's receipt, leases and deletion worklist.
pub(super) fn admission_script(script: &str) -> String {
    format!(
        r#"
local function admit_images(key, fields)
  for _, field in ipairs(fields) do
    local raw = redis.call('HGET',key,field)
    local value = raw and tonumber(raw)
    if not value or value < 0 or value >= 9007199254740991 then return false end
  end
  for _, field in ipairs(fields) do redis.call('HINCRBY',key,field,1) end
  return true
end
{script}
"#
    )
}

impl Session {
    fn managed_images_key(&self, tenant: &str) -> String {
        format!(
            "{}:managed-images:{:x}",
            self.store.key,
            Sha256::digest(tenant.as_bytes())
        )
    }
    async fn image_raw(&self, tenant: &str, id: &str) -> Result<Option<String>> {
        if !valid_id(id) {
            return Err(Error::Invalid("invalid managed image ID".into()));
        }
        let mut cmd = redis::cmd("HGET");
        cmd.arg(self.managed_images_key(tenant)).arg(id);
        self.store.query(cmd).await
    }
    fn decode_image(&self, tenant: &str, id: &str, raw: &str) -> Result<ManagedImage> {
        let value: ManagedImage = decode(raw)?;
        if value.tenant_id != tenant || value.id != id || value.revision == 0 {
            return Err(Error::Unavailable("managed image identity corrupt".into()));
        }
        Ok(value)
    }
    pub async fn managed_image(&self, tenant: &str, id: &str) -> Result<ManagedImage> {
        let raw = self.image_raw(tenant, id).await?.ok_or(Error::NotFound)?;
        let [header] = self.store.fields([HEADER.into()]).await?;
        self.header(&header)?;
        self.decode_image(tenant, id, &raw)
    }
    pub async fn begin_managed_image(
        &self,
        image: ManagedImage,
        max_records: u32,
        max_pending: u32,
    ) -> Result<ManagedImage> {
        let key = self.managed_images_key(&image.tenant_id);
        let mut cmd = redis::cmd("EVAL");
        cmd.arg(
            r#"
            if redis.call('HGET',KEYS[1],'epoch') ~= ARGV[1] then return {-1,''} end
            local previous = redis.call('HGET',KEYS[7],ARGV[7])
            if previous then return {1,previous} end
            if redis.call('ZCARD',KEYS[4]) >= tonumber(ARGV[8])
                or redis.call('SCARD',KEYS[8]) >= tonumber(ARGV[9])
            then return {-2,''} end
            if redis.call('HEXISTS',KEYS[3],ARGV[2]) == 1 then return {-1,''} end
            redis.call('HSET',KEYS[2],ARGV[2],ARGV[3])
            redis.call('HSET',KEYS[3],ARGV[2],ARGV[4])
            redis.call('ZADD',KEYS[4],0,ARGV[2])
            redis.call('HSET',KEYS[5],ARGV[5],'0')
            redis.call('HSET',KEYS[6],ARGV[2],ARGV[6])
            redis.call('HSET',KEYS[7],ARGV[7],ARGV[2])
            redis.call('SADD',KEYS[8],ARGV[2])
            return {1,ARGV[2]}
        "#,
        )
        .arg(8)
        .arg(&self.store.key)
        .arg(&key)
        .arg(format!("{}:managed-image-owners", self.store.key))
        .arg(format!("{key}:index"))
        .arg(image_gates_key(&self.store.key))
        .arg(format!("{key}:summary"))
        .arg(format!("{key}:requests"))
        .arg(format!("{key}:pending"))
        .arg(self.epoch.to_string())
        .arg(&image.id)
        .arg(encode(&image)?)
        .arg(&image.tenant_id)
        .arg(gate_field(&image.tenant_id, &image.id))
        .arg(encode(&image.summary())?)
        .arg(format!("{:x}", Sha256::digest(image.request_id.as_bytes())))
        .arg(max_records)
        .arg(max_pending);
        let (result, id): (i64, String) = self.store.query(cmd).await?;
        match result {
            -2 => return Err(Error::NoCapacity),
            1 => (),
            _ => return Err(Error::Conflict),
        }
        let existing = self.managed_image(&image.tenant_id, &id).await?;
        if existing.request_id != image.request_id
            || existing.initial_retention != image.initial_retention
        {
            return Err(Error::Conflict);
        }
        Ok(existing)
    }
    pub async fn mutate_managed_image(
        &self,
        tenant: &str,
        id: &str,
        change: impl Fn(&mut ManagedImage) -> Result<()>,
    ) -> Result<ManagedImage> {
        self.mutate_managed_image_checked(tenant, id, None, None, change)
            .await
    }

    /// Conclude an observation only while its admission version is current.
    /// Conflicts require a new inventory read before another deletion attempt.
    pub(crate) async fn mutate_managed_image_checked(
        &self,
        tenant: &str,
        id: &str,
        admissions: Option<i64>,
        builder: Option<&ImageBuilder>,
        change: impl Fn(&mut ManagedImage) -> Result<()>,
    ) -> Result<ManagedImage> {
        for _ in 0..ATTEMPTS {
            let raw = self.image_raw(tenant, id).await?.ok_or(Error::NotFound)?;
            let mut next = self.decode_image(tenant, id, &raw)?;
            change(&mut next)?;
            let changed = encode(&next)? != raw;
            if !changed && admissions.is_none() && builder.is_none() {
                // Still validate the process epoch for idempotent mutations.
                let [header] = self.store.fields([HEADER.into()]).await?;
                self.header(&header)?;
                return Ok(next);
            }
            if changed {
                next.revision = next.revision.checked_add(1).ok_or(Error::Conflict)?;
            }
            let mut cmd = redis::cmd("EVAL");
            cmd.arg(r#"
                if redis.call('HGET',KEYS[1],'epoch') ~= ARGV[1] then return -1 end
                local gate = redis.call('HGET',KEYS[3],ARGV[5])
                if not gate or (ARGV[6] ~= '' and gate ~= ARGV[6]) then return -1 end
                if ARGV[9] ~= '' and redis.call('HGET',KEYS[1],ARGV[9]) ~= ARGV[10] then return -1 end
                if redis.call('HGET',KEYS[2],ARGV[2]) ~= ARGV[3] then return 0 end
                if ARGV[3] == ARGV[4] then return 1 end
                redis.call('HSET',KEYS[2],ARGV[2],ARGV[4])
                if ARGV[7] == 'closed' then redis.call('HSET',KEYS[3],ARGV[5],'-1') end
                redis.call('HSET',KEYS[4],ARGV[2],ARGV[8])
                if ARGV[11] == 'settled' then redis.call('SREM',KEYS[5],ARGV[2]) end
                return 1
            "#)
                .arg(5).arg(&self.store.key).arg(self.managed_images_key(tenant))
                .arg(image_gates_key(&self.store.key))
                .arg(format!("{}:summary", self.managed_images_key(tenant)))
                .arg(format!("{}:pending", self.managed_images_key(tenant)))
                .arg(self.epoch.to_string()).arg(id).arg(&raw).arg(encode(&next)?)
                .arg(gate_field(tenant, id)).arg(admissions.map(|n| n.to_string()).unwrap_or_default())
                .arg(if next.admission_open() { "" } else { "closed" }).arg(encode(&next.summary())?)
                .arg(builder.map_or("", |evidence| evidence.field.as_str()))
                .arg(builder.map_or("", |evidence| evidence.raw.as_str()))
                .arg(if next.publication_pending() { "" } else { "settled" });
            match self.store.query::<i64>(cmd).await? {
                1 => return Ok(next),
                -1 => return Err(Error::Conflict),
                _ => (),
            }
        }
        Err(Error::Unavailable("managed image update contention".into()))
    }

    pub(crate) async fn managed_image_builder(
        &self,
        tenant: &str,
        image_id: &str,
        builder_id: &str,
        allowed_profiles: &BTreeSet<String>,
    ) -> Result<ImageBuilder> {
        if builder_id.is_empty() || builder_id.len() > 256 {
            return Err(Error::Invalid(
                "managed build Environment ID required".into(),
            ));
        }
        let field = format!("environment:{builder_id}");
        let [header, raw] = self.store.fields([HEADER.into(), field.clone()]).await?;
        self.header(&header)?;
        let raw = raw.ok_or(Error::NotFound)?;
        let record: StoredEnvironment = decode(&raw)?;
        let managed_profile =
            serde_json::from_str::<serde_json::Value>(&record.spec.execution.extra_config)
                .ok()
                .and_then(|value| {
                    value
                        .get("managedRuntimeProfile")
                        .and_then(|profile| profile.as_str())
                        .map(str::to_owned)
                });
        if record.spec.tenant_id != tenant
            || record.invalidated
            || !record
                .spec
                .execution
                .managed_image_publications
                .contains(image_id)
            || managed_profile
                .as_ref()
                .is_none_or(|name| !allowed_profiles.contains(name))
            || record
                .result
                .as_ref()
                .is_none_or(|result| result.state != EnvironmentState::Running)
        {
            return Err(Error::Conflict);
        }
        Ok(ImageBuilder {
            publisher: ImagePublisher::Environment {
                id: builder_id.to_owned(),
                generation: record.assignment.generation,
            },
            field,
            raw,
        })
    }
    /// Pages read bounded records and keep tenant payloads out of unrelated requests.
    pub async fn managed_images(
        &self,
        tenant: &str,
        after: &str,
        limit: usize,
    ) -> Result<Vec<ManagedImageSummary>> {
        if limit == 0 || limit > 100 || (!after.is_empty() && !valid_id(after)) {
            return Err(Error::Invalid("invalid managed image page".into()));
        }
        let mut cmd = redis::cmd("ZRANGEBYLEX");
        cmd.arg(format!("{}:index", self.managed_images_key(tenant)))
            .arg(if after.is_empty() {
                "-".into()
            } else {
                format!("({after}")
            })
            .arg("+")
            .arg("LIMIT")
            .arg(0)
            .arg(limit);
        let ids: Vec<String> = self.store.query(cmd).await?;
        if ids.is_empty() {
            return Ok(vec![]);
        }
        let mut cmd = redis::cmd("HMGET");
        cmd.arg(format!("{}:summary", self.managed_images_key(tenant)))
            .arg(&ids);
        let rows: Vec<Option<String>> = self.store.query(cmd).await?;
        let [header] = self.store.fields([HEADER.into()]).await?;
        self.header(&header)?;
        ids.into_iter()
            .zip(rows)
            .map(|(id, raw)| {
                let raw =
                    raw.ok_or_else(|| Error::Unavailable("missing managed image summary".into()))?;
                let summary: ManagedImageSummary = decode(&raw)?;
                if summary.id != id || summary.tenant_id != tenant {
                    return Err(Error::Unavailable(
                        "managed image summary identity corrupt".into(),
                    ));
                }
                Ok(summary)
            })
            .collect()
    }
    pub async fn managed_image_admissions(&self, tenant: &str, id: &str) -> Result<i64> {
        if !valid_id(id) {
            return Err(Error::Invalid("invalid managed image ID".into()));
        }
        let mut cmd = redis::cmd("HGET");
        cmd.arg(image_gates_key(&self.store.key))
            .arg(gate_field(tenant, id));
        self.store
            .query::<Option<i64>>(cmd)
            .await?
            .ok_or(Error::NotFound)
    }

    /// Observe the persistent roots in lifecycle order. A snapshot creation or
    /// new Environment increments the admission version in its write. Moving
    /// a lost attempt from an Environment to a hold adds the hold first.
    pub(crate) async fn managed_image_references(
        &self,
        tenant: &str,
        id: &str,
        mut samples: BTreeSet<String>,
        limit: usize,
    ) -> Result<Vec<String>> {
        let matches = |spec: &EnvironmentSpec| {
            spec.tenant_id == tenant && spec.execution.managed_image_publications.contains(id)
        };
        let sources = [
            (self.store.key.clone(), "environment:*"),
            (self.snapshots_key(), "*"),
            (format!("{}:input-holds", self.store.key), "*"),
        ];
        for (kind, (key, pattern)) in sources.into_iter().enumerate() {
            let mut cursor = 0u64;
            while samples.len() < limit {
                let mut command = redis::cmd("HSCAN");
                command
                    .arg(&key)
                    .arg(cursor)
                    .arg("MATCH")
                    .arg(pattern)
                    .arg("COUNT")
                    .arg(128);
                let (next, records): (u64, BTreeMap<String, String>) =
                    self.store.query(command).await?;
                for raw in records.into_values() {
                    let reference = match kind {
                        0 => {
                            let record: StoredEnvironment = decode(&raw)?;
                            matches(&record.spec).then(|| format!("environment:{}", record.spec.id))
                        }
                        1 => {
                            let record: Snapshot = decode(&raw)?;
                            (record.state != SnapshotState::Deleted && matches(&record.template))
                                .then(|| format!("snapshot:{}", record.id))
                        }
                        _ => {
                            let hold: artifacts::ExecutionHold = decode(&raw)?;
                            (hold.tenant_id == tenant
                                && hold.managed_image_publications.contains(id))
                            .then(|| {
                                format!(
                                    "execution:{}:{}",
                                    hold.assignment.environment_id, hold.assignment.generation
                                )
                            })
                        }
                    };
                    if let Some(reference) = reference {
                        samples.insert(reference);
                    }
                    if samples.len() >= limit {
                        break;
                    }
                }
                if next == 0 {
                    break;
                }
                cursor = next;
            }
        }
        Ok(samples.into_iter().take(limit).collect())
    }

    /// A queued consumer is a reference from admission until its durable write.
    pub async fn acquire_managed_images(&self, spec: &EnvironmentSpec) -> Result<()> {
        let fields = image_gate_fields(spec);
        if fields.is_empty() {
            return Ok(());
        }
        let mut command = redis::cmd("EVAL");
        command
            .arg(admission_script(
                r#"
            if redis.call('HGET',KEYS[1],'epoch') ~= ARGV[1] then return 0 end
            if not admit_images(KEYS[2],cjson.decode(ARGV[2])) then return 0 end
            return 1
        "#,
            ))
            .arg(2)
            .arg(&self.store.key)
            .arg(image_gates_key(&self.store.key))
            .arg(self.epoch.to_string())
            .arg(encode(&fields)?);
        match self.store.query::<u8>(command).await? {
            1 => Ok(()),
            _ => Err(Error::Conflict),
        }
    }

    /// Resolve normal image strings once. A matching ID in an unrelated
    /// registry or repository does not enroll that image.
    pub async fn normalize_managed_image_dependencies(
        &self,
        spec: &mut EnvironmentSpec,
        registry: Option<&RegistryScope>,
    ) -> Result<()> {
        let candidates = image_references(spec);
        if candidates.is_empty() {
            return Ok(());
        }
        let mut command = redis::cmd("HMGET");
        command.arg(format!("{}:managed-image-owners", self.store.key));
        for (id, _) in &candidates {
            command.arg(id);
        }
        let owners: Vec<Option<String>> = self.store.query(command).await?;
        let mut ids = BTreeSet::new();
        for ((id, reference), owner) in candidates.into_iter().zip(owners) {
            if let Some(owner) = owner {
                let image = self.managed_image(&owner, id).await?;
                if image.matches_repository(reference) {
                    if owner != spec.tenant_id {
                        return Err(Error::Invalid(
                            "managed image belongs to another tenant".into(),
                        ));
                    }
                    ids.insert(id.to_owned());
                    continue;
                }
            }
            // The reserved namespace may be named before a publisher has
            // registered its plan. Do not admit an untracked future consumer.
            if registry.is_some_and(|scope| scope.contains_publication_reference(id, reference)) {
                return Err(Error::Invalid(
                    "managed image repository requires a registered publication plan".into(),
                ));
            }
        }
        spec.execution.managed_image_publications.extend(ids);
        validate_dependencies(&spec.execution.managed_image_publications)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use adx_core::managed_images::{ImageRetention, RegistryScope};

    #[tokio::test]
    #[ignore = "requires isolated Redis; set ADX_TEST_REDIS_SERVER"]
    async fn managed_image_stale_observation_cannot_close_new_admission() {
        let rig = crate::rpc::cancellation_test_redis::Redis::new().await;
        let session = rig.store().await.begin(1).await.unwrap();
        let image = session
            .begin_managed_image(
                ManagedImage::new(
                    "a".repeat(64),
                    "tenant",
                    "stale-scan",
                    ImageRetention::Ephemeral { grace_seconds: 0 },
                    RegistryScope {
                        authority: "registry.test".into(),
                        repository_prefix: "managed".into(),
                    },
                    1,
                )
                .unwrap(),
                32,
                32,
            )
            .await
            .unwrap();
        let repository = format!(
            "registry.test/managed/{}/publications/{}/service-0",
            "b".repeat(32),
            image.id
        );
        let hash = format!("sha256:{}", "c".repeat(64));
        let reference = format!("{repository}@{hash}");
        session
            .mutate_managed_image("tenant", &image.id, |image| {
                image.plan(
                    BTreeMap::from([("single".into(), format!("{repository}:build"))]),
                    ImagePublisher::Operator,
                )?;
                image.publish(BTreeMap::from([("single".into(), hash.clone())]), 2)?;
                image.release("publisher")?;
                image.observe(&[], 3)
            })
            .await
            .unwrap();
        let zero_reference_proof = session
            .managed_image_admissions("tenant", &image.id)
            .await
            .unwrap();
        let spec: EnvironmentSpec = serde_json::from_value(serde_json::json!({
            "id": "queue", "tenant_id": "tenant", "image": "image", "runtime_class": "runsc", "priority": 0,
            "resources": {"cpu_millis": 100, "memory_bytes": 128, "disk_bytes": 0},
            "execution": {"managed_image_publications": [image.id], "failover": false, "inherit_entrypoint": false, "extra_config": ""}
        })).unwrap();
        session.acquire_managed_images(&spec).await.unwrap();
        let result = session
            .mutate_managed_image_checked(
                "tenant",
                &image.id,
                Some(zero_reference_proof),
                None,
                |image| image.claim_manifests(std::slice::from_ref(&reference), 3),
            )
            .await;
        assert_eq!(result, Err(Error::Conflict));
        assert!(session.acquire_managed_images(&spec).await.is_ok());
    }
}
