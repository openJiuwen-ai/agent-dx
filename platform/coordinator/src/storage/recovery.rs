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
        if previous.environment_id != replacement.environment_id
            || previous.node_id == replacement.node_id
            || previous.generation >= replacement.generation
        {
            return Err(Error::Conflict);
        }
        let field = format!("environment:{}", previous.environment_id);
        for _ in 0..ATTEMPTS {
            let [header_value, environment_value, node_value] = self
                .store
                .fields([
                    HEADER.into(),
                    field.clone(),
                    format!("node:{}", replacement.node_id),
                ])
                .await?;
            let header = self.header(&header_value)?;
            let mut stored_environment: StoredEnvironment =
                decode(environment_value.as_deref().ok_or(Error::NotFound)?)?;
            stored_environment.validate()?;
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
            let mut checkpoint = stored_environment
                .recovery_point(now)
                .or_else(|| stored_environment.resume_point(now))
                .ok_or(Error::Conflict)?
                .clone();
            node_value.as_deref().ok_or(Error::NotFound)?;
            let target = self.get_node(&replacement.node_id).await?;
            if !target.node.available
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
                pending: true,
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

            if self
                .assignment_cas(
                    &header,
                    environment_value.as_deref(),
                    &stored_environment,
                    catalog.as_ref(),
                    None,
                )
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
