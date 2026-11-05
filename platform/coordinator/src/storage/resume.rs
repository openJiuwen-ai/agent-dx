//! Durable demand and placement decisions share the Environment's Redis CAS.
use super::*;

/// Admission is read from the same fenced snapshot as the Environment. A
/// planned migration is waiting, not a failed resume or a new wake intent.
pub(crate) enum ResumeAdmission {
    Upgrading,
    Running(StoredEnvironment),
    Demanded(StoredEnvironment),
}

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
    /// Record ordinary wake demand. Upgrade-aware RPC callers use admission
    /// directly so a read-only migration fence can wait within their deadline.
    pub async fn request_resume(&self, id: &str, now: u64) -> Result<StoredEnvironment> {
        self.request_resume_with_timeout(id, now, None).await
    }

    /// The first accepted wake fixes this run's duration; retries retain that intent.
    pub async fn request_resume_with_timeout(
        &self,
        id: &str,
        now: u64,
        run_timeout_seconds: Option<u64>,
    ) -> Result<StoredEnvironment> {
        match self.admit_resume(id, now, "", run_timeout_seconds).await? {
            ResumeAdmission::Running(stored) | ResumeAdmission::Demanded(stored) => Ok(stored),
            ResumeAdmission::Upgrading => Err(Error::Unavailable(
                "environment is being evacuated; retry after ownership settles".into(),
            )),
        }
    }

    pub(crate) async fn admit_resume(
        &self,
        id: &str,
        now: u64,
        expected_runtime_id: &str,
        run_timeout_seconds: Option<u64>,
    ) -> Result<ResumeAdmission> {
        let field = format!("environment:{id}");
        for _ in 0..ATTEMPTS {
            let [header_value, value, upgrade_value] = self
                .store
                .fields([HEADER.into(), field.clone(), UPGRADE.into()])
                .await?;
            let header = self.header(&header_value)?;
            let upgrade = upgrade_value
                .as_deref()
                .map(decode::<UpgradeTask>)
                .transpose()?;
            let mut stored: StoredEnvironment = decode(value.as_deref().ok_or(Error::NotFound)?)?;
            stored.validate()?;
            if upgrade
                .as_ref()
                .is_some_and(|task| task.blocks_resume(&stored))
            {
                return Ok(ResumeAdmission::Upgrading);
            }
            if let Some(seconds) = run_timeout_seconds {
                let mut policy = stored.spec.run_policy.clone().ok_or_else(|| {
                    Error::Invalid("run policy required for resume duration".into())
                })?;
                policy.timeout_seconds = seconds;
                policy.validate()?;
            }
            let record = stored.result.as_ref().ok_or(Error::Conflict)?;
            if !matches!(
                record.state,
                EnvironmentState::Paused | EnvironmentState::Running
            ) {
                return Err(Error::Conflict);
            }
            // A stale Ingress binding may already have a published replacement.
            // Check the upgrade fence first, including unsettled operations on
            // the target, before returning that execution without waking it.
            if !stored.invalidated
                && stored.resume.is_none()
                && record.state == EnvironmentState::Running
                && !expected_runtime_id.is_empty()
                && record.runtime.id != expected_runtime_id
            {
                return Ok(ResumeAdmission::Running(stored));
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
                    return Ok(ResumeAdmission::Demanded(stored));
                }
                // Keep the operation identity while admitting a new capacity round.
                intent.local_rejected = false;
                intent.rejected_nodes.clear();
                intent.retry_after_unix_seconds = 0;
            } else {
                stored.resume = Some(ResumeIntent {
                    run_timeout_seconds,
                    operation_id: format!("wake-{}", uuid::Uuid::new_v4()),
                    local_rejected: false,
                    rejected_nodes: Default::default(),
                    retry_after_unix_seconds: 0,
                });
            }
            if Box::pin(self.store.object_cas_checked(
                &header,
                Some((&field, value.as_deref(), encode(&stored)?)),
                None,
                &[(UPGRADE.into(), upgrade_value.clone())],
            ))
            .await?
            {
                return Ok(ResumeAdmission::Demanded(stored));
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
            let header = self.header(&header_value)?;
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
            if self
                .store
                .object_cas(
                    header_value.as_deref().ok_or(Error::Conflict)?,
                    &header,
                    Some((&field, value.as_deref(), encode(&stored)?)),
                )
                .await?
            {
                return Ok(stored);
            }
        }
        Err(Error::Unavailable("resume rejection contention".into()))
    }
}
