//! Durable failure age and CAS retirement of executions with invalidated ownership.
use super::*;

pub(super) fn now() -> Result<u64> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|_| Error::Unavailable("system clock before epoch".into()))
}

impl Session {
    /// Seed old records once. Normal Failed commits record their age atomically.
    pub async fn observe_failed(&self, id: &str, now: u64) -> Result<StoredEnvironment> {
        let field = format!("environment:{id}");
        for _ in 0..ATTEMPTS {
            let [header_value, value] = self.store.fields([HEADER.into(), field.clone()]).await?;
            let mut header = self.header(&header_value)?;
            let mut stored: StoredEnvironment = decode(value.as_deref().ok_or(Error::NotFound)?)?;
            stored.validate()?;
            let old = stored.clone();
            stored.track_failure(now);
            if stored == old {
                return Ok(stored);
            }
            header.advance()?;
            if self
                .store
                .cas(
                    header_value.as_deref().ok_or(Error::Conflict)?,
                    &header,
                    Some((&field, encode(&stored)?)),
                )
                .await?
            {
                return Ok(stored);
            }
        }
        Err(Error::Unavailable(
            "concurrent failure observation; retry".into(),
        ))
    }

    /// Only invalidated, released terminal failures can be retired without an owner RPC.
    /// Returning nodes clean executions absent from the authoritative directory.
    pub async fn retire_invalidated_failed(
        &self,
        expected: &StoredEnvironment,
        now: u64,
    ) -> Result<EnvironmentRecord> {
        let stored = self.get(&expected.spec.id).await?;
        if &stored != expected
            || !stored.invalidated
            || stored.resources_held()
            || !stored.failed_collectable(now, 0)
        {
            return Err(Error::Conflict);
        }
        let mut deleted = stored.result.ok_or(Error::Conflict)?;
        deleted.state = EnvironmentState::Deleted;
        deleted.revision = deleted.revision.checked_add(1).ok_or(Error::Conflict)?;
        deleted.runtime.ip = None;
        deleted.checkpoint = None;
        deleted.last_operation = None;
        self.commit(deleted).await
    }
}
