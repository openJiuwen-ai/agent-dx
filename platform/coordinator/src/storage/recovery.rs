//! Atomic ownership transfer after the source execution has been invalidated.
use super::*;

impl StoredEnvironment {
    pub fn resume_point(&self, now: u64) -> Option<&adx_core::RestorePoint> {
        let record = self.result.as_ref()?;
        if record.state != EnvironmentState::Paused
            || record.resources_held
            || self.resume.is_none()
            || (!self.invalidated && !self.resume.as_ref()?.local_rejected)
        {
            return None;
        }
        record
            .checkpoint
            .as_ref()
            .or(record.snapshot_source.as_ref())
            .filter(|cp| cp.artifact.storage != "local" && !cp.expired(now))
    }
    pub fn recovery_point(&self, now: u64) -> Option<&adx_core::RestorePoint> {
        if !self.invalidated {
            return None;
        }
        let record = self.result.as_ref()?;
        if record.state != EnvironmentState::Failed || record.resources_held {
            return None;
        }
        record
            .checkpoint
            .as_ref()
            .or(record.snapshot_source.as_ref())
            .filter(|cp| cp.artifact.storage != "local" && !cp.expired(now))
    }
}
impl Session {
    pub async fn reserve_recovery(
        &self,
        previous: &Assignment,
        replacement: Assignment,
        now: u64,
    ) -> Result<StoredEnvironment> {
        Box::pin(self.transfer_checkpoint(previous, replacement, now, None, None)).await
    }

    /// Planned transfer requires a published, stopped source; it never simulates node failure.
    pub async fn reserve_migration(
        &self,
        previous: &Assignment,
        replacement: Assignment,
        now: u64,
        resume: bool,
        expected_source_session: &str,
        expected_target_session: &str,
    ) -> Result<StoredEnvironment> {
        Box::pin(self.transfer_checkpoint(
            previous,
            replacement,
            now,
            Some(resume),
            Some((expected_source_session, expected_target_session)),
        ))
        .await
    }

    async fn transfer_checkpoint(
        &self,
        previous: &Assignment,
        replacement: Assignment,
        now: u64,
        planned_resume: Option<bool>,
        expected_sessions: Option<(&str, &str)>,
    ) -> Result<StoredEnvironment> {
        let (planned, resume) = (planned_resume.is_some(), planned_resume.unwrap_or(true));
        if previous.environment_id != replacement.environment_id
            || previous.node_id == replacement.node_id
            || previous.generation >= replacement.generation
        {
            return Err(Error::Conflict);
        }
        if replacement.generation >> 32 == self.epoch {
            self.generations
                .fetch_max(replacement.generation, Ordering::AcqRel);
        }
        let field = format!("environment:{}", previous.environment_id);
        for _ in 0..ATTEMPTS {
            let [header_value, environment_value, node_value, source_node_value, upgrade_value] =
                self.store
                    .fields([
                        HEADER.into(),
                        field.clone(),
                        format!("node:{}", replacement.node_id),
                        format!("node:{}", previous.node_id),
                        UPGRADE.into(),
                    ])
                    .await?;
            let header = self.header(&header_value)?;
            let upgrade = upgrade_value
                .as_deref()
                .map(decode::<UpgradeTask>)
                .transpose()?;
            let mut stored_environment: StoredEnvironment =
                decode(environment_value.as_deref().ok_or(Error::NotFound)?)?;
            stored_environment.validate()?;
            if planned && volumes::has_volumes(&stored_environment.spec) {
                return Err(Error::Invalid(
                    "planned migration of managed Volumes is not supported".into(),
                ));
            }
            if let Some((source_session, target_session)) = expected_sessions {
                let source: StoredNode =
                    decode(source_node_value.as_deref().ok_or(Error::NotFound)?)?;
                let target: StoredNode = decode(node_value.as_deref().ok_or(Error::NotFound)?)?;
                if source_session.is_empty()
                    || target_session.is_empty()
                    || source
                        .session
                        .as_ref()
                        .is_none_or(|s| s.id != source_session)
                    || target
                        .session
                        .as_ref()
                        .is_none_or(|s| s.id != target_session)
                {
                    return Err(Error::Conflict);
                }
            }
            if stored_environment.assignment == replacement
                && stored_environment
                    .recovery
                    .as_ref()
                    .is_some_and(|recovery| &recovery.source == previous)
            {
                return Ok(stored_environment);
            }
            if &stored_environment.assignment != previous {
                return Err(Error::Conflict);
            }
            let planned_point = stored_environment
                .result
                .as_ref()
                .filter(|r| {
                    planned
                        && !stored_environment.invalidated
                        && stored_environment.resume.is_none()
                        && r.state == EnvironmentState::Paused
                        && !r.resources_held
                        && !r.restart_pending
                })
                .and_then(|r| r.checkpoint.as_ref().or(r.snapshot_source.as_ref()))
                .filter(|cp| cp.artifact.storage != "local" && !cp.expired(now));
            let mut checkpoint = if planned {
                planned_point
            } else {
                stored_environment
                    .recovery_point(now)
                    .or_else(|| stored_environment.resume_point(now))
            }
            .ok_or(Error::Conflict)?
            .clone();
            node_value.as_deref().ok_or(Error::NotFound)?;
            let target = self.get_node(&replacement.node_id).await?;
            if planned && !checkpoint.artifact.scope.is_empty() {
                let source: StoredNode =
                    decode(source_node_value.as_deref().ok_or(Error::NotFound)?)?;
                if !source
                    .checkpoint_scopes
                    .contains(&checkpoint.artifact.scope)
                    || !target
                        .checkpoint_scopes
                        .contains(&checkpoint.artifact.scope)
                {
                    return Err(Error::Invalid(
                        "checkpoint backend identity is unavailable on migration nodes".into(),
                    ));
                }
            }
            let canary_target = planned
                && upgrade.as_ref().is_some_and(|task| {
                    task.active()
                        && !task.revoked_canaries.contains(&previous.environment_id)
                        && task.canaries.get(&previous.environment_id) == Some(&replacement.node_id)
                });
            if (!target.node.available && !canary_target)
                || target.shard_id != replacement.shard_id
                || target.session.as_ref().is_some_and(|s| !s.routable)
            {
                return Err(Error::Conflict);
            }
            if checkpoint.origin.is_none() {
                checkpoint.origin = Some(adx_core::runtime::RuntimeIdentity {
                    environment_id: stored_environment.spec.id.clone(),
                    ownership_generation: previous.generation,
                    runtime_id: checkpoint.source_runtime_id.clone(),
                });
            }
            let source = stored_environment
                .result
                .as_ref()
                .ok_or(Error::Conflict)?
                .clone();
            stored_environment.result = Some(EnvironmentRecord {
                observation: None,
                execution_artifacts: vec![],
                snapshot_source: source.snapshot_source.clone(),
                failure: None,
                spec: stored_environment.spec.clone(),
                assignment: replacement.clone(),
                state: EnvironmentState::Paused,
                revision: 1,
                runtime: adx_core::Runtime {
                    id: format!("{}-{}", stored_environment.spec.id, replacement.generation),
                    ip: None,
                },
                resources_held: false,
                checkpoint: if source.snapshot_source.is_some() && source.checkpoint.is_none() {
                    None
                } else {
                    Some(checkpoint.clone())
                },
                last_operation: None,
                restart_attempts: 0,
                restart_pending: false,
            });
            stored_environment.assignment = replacement.clone();
            stored_environment.volume_execution = None;
            stored_environment.invalidated = false;
            if let Some(intent) = &mut stored_environment.resume {
                intent.local_rejected = false;
            }
            stored_environment.failed_since_unix_seconds = None;
            stored_environment.recovery = Some(Recovery {
                source: previous.clone(),
                pending: resume,
            });
            if let Some(last) = stored_environment
                .last_interruption
                .as_mut()
                .filter(|last| last.resolved_at_unix_millis.is_none())
            {
                last.recovery_state = RecoveryState::Recovering;
            }
            stored_environment.validate()?;
            let catalog = if super::volumes::has_volumes(&stored_environment.spec) {
                let mut catalog = self.volume_catalog().await?;
                if Some(&catalog.header_value) != header_value.as_ref() {
                    continue;
                }
                catalog.reserve(&stored_environment, &target)?;
                Some(catalog)
            } else {
                None
            };

            if Box::pin(self.assignment_cas_checked(
                &header,
                environment_value.as_deref(),
                &stored_environment,
                catalog.as_ref(),
                None,
                &[
                    (UPGRADE.into(), upgrade_value.clone()),
                    (
                        format!("node:{}", previous.node_id),
                        source_node_value.clone(),
                    ),
                    (format!("node:{}", replacement.node_id), node_value.clone()),
                ],
            ))
            .await?
            {
                return Ok(stored_environment);
            }
        }
        Err(Error::Unavailable(
            "concurrent recovery assignment; retry".into(),
        ))
    }
}
