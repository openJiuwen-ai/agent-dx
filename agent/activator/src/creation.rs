//! Creation retries retain the saved Env identity. Only confirmed queue cancellation
//! advances the persisted request attempt; ambiguous writes keep their original identity.
use super::*;
use adx_agent_core::{
    environment::{CreateEnvironment, ExecutionSpec},
    mounts::{Mount, Parameters},
    volumes::VolumeSpec,
};
use std::collections::BTreeMap;

enum CreationStep {
    Observed(EnvironmentObservation),
    Deferred(u64),
    PreviousEnvironmentActive,
}

pub(super) fn definitions(
    template: &TemplateVersion,
    parameters: &Parameters,
    mounts: &[Mount],
) -> Result<Vec<VolumeSpec>> {
    let volumes = template
        .render_volumes(parameters)
        .map_err(Error::Invalid)?;
    for mount in mounts {
        if let Some(id) = &mount.volume_id {
            if !volumes.iter().any(|volume| &volume.id == id) {
                return Err(Error::Invalid(
                    "mount references an undeclared Volume".into(),
                ));
            }
        }
    }
    Ok(volumes)
}
impl Activator {
    async fn defer_creation(
        &self,
        binding: &AgentBinding,
        environment: &EnvironmentRef,
        attempt: &adx_agent_core::CreationAttempt,
        cancelled: bool,
    ) -> Result<EnvironmentObservation> {
        let saved = self
            .state
            .defer_creation(
                binding,
                environment,
                attempt,
                cancelled,
                adx_agent_core::unix_time_millis(),
            )
            .await?;
        let retry_after_unix_ms = saved
            .creation_attempts
            .get(&environment.environment_id)
            .ok_or_else(|| Error::Conflict("creation attempt changed".into()))?
            .retry_after_unix_ms;
        Err(Error::CreationDeferred {
            retry_after_unix_ms,
        })
    }

    pub(super) async fn activate_environment(
        &self,
        binding: &AgentBinding,
        environment: &EnvironmentRef,
        mut execution: ExecutionSpec,
        volumes: &[VolumeSpec],
        deadline: u64,
    ) -> Result<EnvironmentObservation> {
        let deadline_at = Instant::now()
            .checked_add(Duration::from_millis(
                deadline.saturating_sub(adx_agent_core::unix_time_millis()),
            ))
            .ok_or_else(|| Error::Invalid("activation deadline is out of range".into()))?;
        let mut fixed: BTreeMap<String, String> = binding.volumes.clone();
        let mut backoff = 50_u64;
        loop {
            if !self.reference_is_active(binding, environment).await? {
                return Err(Error::Conflict("binding changed during activation".into()));
            }
            let saved = self
                .state
                .binding(&binding.scope)
                .await?
                .ok_or(Error::NotFound)?;
            let attempt = saved
                .creation_attempts
                .get(&environment.environment_id)
                .cloned()
                .unwrap_or_default();
            let remaining = deadline_at.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(Error::OutcomeUnknown(
                    "activation deadline expired; query the same AgentBinding".into(),
                ));
            }
            let result = tokio::time::timeout(remaining, async {
                // An existing execution belongs to Platform, including startup, restart,
                // recovery and failure. No status of an existing Env authorizes recreation.
                if let Some(observed) = self
                    .environment
                    .get(&binding.scope.tenant, &environment.environment_id)
                    .await?
                {
                    return Ok(CreationStep::Observed(observed));
                }
                if attempt.retry_after_unix_ms > adx_agent_core::unix_time_millis() {
                    return Ok(CreationStep::Deferred(attempt.retry_after_unix_ms));
                }
                // Every upgrade waits for the previous execution to pause, before
                // its first creation as well as retries, regardless of Volume mode.
                // Platform remains authoritative for actual resource/Volume admission.
                if binding.current != *environment {
                    if let Some(previous) = self
                        .environment
                        .get(&binding.scope.tenant, &binding.current.environment_id)
                        .await?
                    {
                        if previous.id != binding.current.environment_id
                            || previous.tenant != binding.scope.tenant
                        {
                            return Err(EnvironmentError::Unavailable(
                                "previous Environment identity mismatch".into(),
                            ));
                        }
                        if !matches!(
                            previous.phase,
                            EnvironmentPhase::Paused | EnvironmentPhase::Deleted
                        ) {
                            return Ok(CreationStep::PreviousEnvironmentActive);
                        }
                    }
                }
                for volume in volumes {
                    let reference = self
                        .environment
                        .ensure_volume(&binding.scope.tenant, &environment.environment_id, volume)
                        .await?;
                    if reference.id != volume.id
                        || fixed
                            .get(&reference.id)
                            .is_some_and(|uid| uid != &reference.uid)
                    {
                        return Err(EnvironmentError::Conflict(
                            "fixed Volume identity changed during activation".into(),
                        ));
                    }
                    fixed.insert(reference.id, reference.uid);
                }
                self.state
                    .fix_volumes(binding, &fixed)
                    .await
                    .map_err(|error| EnvironmentError::Conflict(error.to_string()))?;
                let current = self
                    .state
                    .binding(&binding.scope)
                    .await
                    .map_err(|error| EnvironmentError::Unavailable(error.to_string()))?
                    .ok_or(EnvironmentError::NotFound)?;
                if current
                    .creation_attempts
                    .get(&environment.environment_id)
                    .cloned()
                    .unwrap_or_default()
                    != attempt
                    || !self
                        .reference_is_active(binding, environment)
                        .await
                        .map_err(|error| EnvironmentError::Unavailable(error.to_string()))?
                {
                    return Err(EnvironmentError::Conflict(
                        "creation attempt changed".into(),
                    ));
                }
                for mount in &mut execution.mounts {
                    if let Some(id) = &mount.volume_id {
                        mount.volume_uid = Some(
                            fixed
                                .get(id)
                                .ok_or_else(|| {
                                    EnvironmentError::Invalid("Volume reference missing".into())
                                })?
                                .clone(),
                        );
                    }
                }
                self.environment
                    .create(&CreateEnvironment {
                        id: environment.environment_id.clone(),
                        tenant: binding.scope.tenant.clone(),
                        execution: execution.clone(),
                        deadline_unix_ms: Some(deadline),
                        creation_attempt: attempt.number,
                    })
                    .await
                    .map(CreationStep::Observed)
            })
            .await
            .map_err(|_| {
                Error::OutcomeUnknown(
                    "activation response deadline expired; query the same AgentBinding".into(),
                )
            })?;
            match result {
                Ok(CreationStep::Observed(observed)) => return Ok(observed),
                Ok(CreationStep::Deferred(retry_after_unix_ms)) => {
                    return Err(Error::CreationDeferred {
                        retry_after_unix_ms,
                    });
                }
                Ok(CreationStep::PreviousEnvironmentActive) => {
                    return self
                        .defer_creation(&saved, environment, &attempt, false)
                        .await
                }
                Err(EnvironmentError::SchedulingCancelled(failure)) => {
                    tracing::debug!(environment_id = %environment.environment_id, attempt = attempt.number,
                        ?failure, "creation queue cancelled before assignment; deferring the next attempt");
                    return self
                        .defer_creation(&saved, environment, &attempt, true)
                        .await;
                }
                Err(EnvironmentError::Unavailable(_) | EnvironmentError::OutcomeUnknown(_)) => {
                    let remaining = deadline_at.saturating_duration_since(Instant::now());
                    let jitter = u64::from(uuid::Uuid::new_v4().as_bytes()[0]) % 50;
                    tokio::time::sleep(Duration::from_millis(backoff + jitter).min(remaining))
                        .await;
                    backoff = (backoff * 2).min(1000);
                }
                Err(error) => return Err(error.into()),
            }
        }
    }
}
