//! Durable demand and placement decisions share the Environment's Redis CAS.
use super::*;

impl Session {
    pub async fn retire_dormant(&self, expected: &StoredEnvironment) -> Result<EnvironmentRecord> {
        let stored = self.get(&expected.spec.id).await?;
        if stored != *expected
            || !stored.invalidated
            || stored.resources_held()
            || stored
                .result
                .as_ref()
                .is_none_or(|r| r.state != EnvironmentState::Paused)
        {
            return Err(Error::Conflict);
        }
        let mut deleted = stored.result.ok_or(Error::Conflict)?;
        deleted.state = EnvironmentState::Deleted;
        deleted.revision = deleted.revision.checked_add(1).ok_or(Error::Conflict)?;
        deleted.checkpoint = None;
        deleted.snapshot_source = None;
        deleted.execution_artifacts.clear();
        deleted.last_operation = None;
        self.commit(deleted).await
    }
    pub async fn request_resume(&self, id: &str, now: u64) -> Result<StoredEnvironment> {
        let field = format!("environment:{id}");
        for _ in 0..ATTEMPTS {
            let [header_value, value] = self.store.fields([HEADER.into(), field.clone()]).await?;
            let mut header = self.header(&header_value)?;
            let mut stored: StoredEnvironment = decode(value.as_deref().ok_or(Error::NotFound)?)?;
            stored.validate()?;
            let record = stored.result.as_ref().ok_or(Error::Conflict)?;
            if !matches!(
                record.state,
                EnvironmentState::Paused | EnvironmentState::Running
            ) {
                return Err(Error::Conflict);
            }
            if record.state == EnvironmentState::Paused
                && record
                    .checkpoint
                    .as_ref()
                    .or(record.snapshot_source.as_ref())
                    .is_none_or(|cp| cp.expired(now))
            {
                return Err(Error::Invalid("checkpoint expired or missing".into()));
            }
            if let Some(intent) = &mut stored.resume {
                if intent.retry_after_unix_seconds == 0 || now < intent.retry_after_unix_seconds {
                    return Ok(stored);
                }
                // Keep the operation identity while admitting a new capacity round.
                intent.local_rejected = false;
                intent.rejected_nodes.clear();
                intent.retry_after_unix_seconds = 0;
            } else {
                stored.resume = Some(ResumeIntent {
                    operation_id: format!("wake-{}", uuid::Uuid::new_v4()),
                    local_rejected: false,
                    rejected_nodes: Default::default(),
                    retry_after_unix_seconds: 0,
                });
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
        Err(Error::Unavailable("resume demand contention".into()))
    }

    /// Only call after the node returned NoCapacity before invoking restore.
    pub async fn reject_local_resume(
        &self,
        assignment: &Assignment,
        operation_id: &str,
        now: u64,
    ) -> Result<StoredEnvironment> {
        let field = format!("environment:{}", assignment.environment_id);
        for _ in 0..ATTEMPTS {
            let [header_value, value] = self.store.fields([HEADER.into(), field.clone()]).await?;
            let mut header = self.header(&header_value)?;
            let mut stored: StoredEnvironment = decode(value.as_deref().ok_or(Error::NotFound)?)?;
            if stored.assignment != *assignment
                || stored
                    .result
                    .as_ref()
                    .is_none_or(|r| r.state != EnvironmentState::Paused || r.resources_held)
            {
                return Err(Error::Conflict);
            }
            let intent = stored
                .resume
                .as_mut()
                .filter(|r| r.operation_id == operation_id)
                .ok_or(Error::Conflict)?;
            if intent.local_rejected {
                return Ok(stored);
            }
            intent.local_rejected = true;
            intent.rejected_nodes.insert(assignment.node_id.clone());
            if intent.retry_after_unix_seconds == 0 {
                intent.retry_after_unix_seconds = now.saturating_add(30);
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
        Err(Error::Unavailable("resume rejection contention".into()))
    }
}
