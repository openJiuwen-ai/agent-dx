//! Opaque-byte CAS keeps publication receipts and release tombstones across restarts.
use super::*;
use adx_core::managed_images::{valid_id, ManagedImage};

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
    pub async fn begin_managed_image(&self, image: ManagedImage) -> Result<ManagedImage> {
        let [header] = self.store.fields([HEADER.into()]).await?;
        self.header(&header)?;
        let mut cmd = redis::cmd("EVAL");
        cmd.arg("if redis.call('HGET',KEYS[1],'epoch') ~= ARGV[1] then return -1 end; local n=redis.call('HSETNX',KEYS[2],ARGV[2],ARGV[3]); redis.call('HSET',KEYS[3],ARGV[2],ARGV[4]); redis.call('ZADD',KEYS[4],0,ARGV[2]); return n")
            .arg(4).arg(&self.store.key).arg(self.managed_images_key(&image.tenant_id))
            .arg(format!("{}:managed-image-owners", self.store.key))
            .arg(format!("{}:index", self.managed_images_key(&image.tenant_id)))
            .arg(self.epoch.to_string()).arg(&image.id).arg(encode(&image)?).arg(&image.tenant_id);
        let result: i64 = self.store.query(cmd).await?;
        if result < 0 {
            return Err(Error::Conflict);
        }
        let existing = self.managed_image(&image.tenant_id, &image.id).await?;
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
        for _ in 0..ATTEMPTS {
            let raw = self.image_raw(tenant, id).await?.ok_or(Error::NotFound)?;
            let mut next = self.decode_image(tenant, id, &raw)?;
            change(&mut next)?;
            next.revision = next.revision.checked_add(1).ok_or(Error::Conflict)?;
            let mut cmd = redis::cmd("EVAL");
            cmd.arg("if redis.call('HGET',KEYS[1],'epoch') ~= ARGV[1] then return -1 end; if redis.call('HGET',KEYS[2],ARGV[2]) ~= ARGV[3] then return 0 end; redis.call('HSET',KEYS[2],ARGV[2],ARGV[4]); return 1")
                .arg(2).arg(&self.store.key).arg(self.managed_images_key(tenant))
                .arg(self.epoch.to_string()).arg(id).arg(&raw).arg(encode(&next)?);
            match self.store.query::<i64>(cmd).await? {
                1 => return Ok(next),
                -1 => return Err(Error::Conflict),
                _ => (),
            }
        }
        Err(Error::Unavailable("managed image update contention".into()))
    }
    /// Pages read bounded records and keep tenant payloads out of unrelated requests.
    pub async fn managed_images(
        &self,
        tenant: &str,
        after: &str,
        limit: usize,
    ) -> Result<Vec<ManagedImage>> {
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
        let mut result = Vec::new();
        for id in &ids {
            result.push(self.managed_image(tenant, id).await?);
        }
        Ok(result)
    }
    /// Admission resets the grace before a request can enter either scheduler path.
    pub async fn acquire_managed_images(&self, spec: &EnvironmentSpec) -> Result<()> {
        for id in self.managed_image_dependencies(spec).await? {
            self.mutate_managed_image(&spec.tenant_id, &id, |image| image.acquire())
                .await?;
        }
        Ok(())
    }

    /// Unknown external images retain their existing behavior. Explicit
    /// managed dependencies must resolve, including across snapshot restores.
    pub async fn managed_image_dependencies(
        &self,
        spec: &EnvironmentSpec,
    ) -> Result<BTreeSet<String>> {
        let mut result = BTreeSet::new();
        let explicit = adx_core::managed_images::explicit_dependencies(spec)?;
        for id in adx_core::managed_images::dependencies(spec)? {
            let mut command = redis::cmd("HGET");
            command
                .arg(format!("{}:managed-image-owners", self.store.key))
                .arg(&id);
            let owner: Option<String> = self.store.query(command).await?;
            match owner {
                Some(owner) if owner != spec.tenant_id => {
                    return Err(Error::Invalid(
                        "managed image belongs to another tenant".into(),
                    ))
                }
                None if !explicit.contains(&id) => continue,
                None => return Err(Error::NotFound),
                _ => (),
            }
            result.insert(id);
        }
        Ok(result)
    }
}
