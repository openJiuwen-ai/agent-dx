//! Request-scoped activation retries retain the saved Binding and original deadline.
use super::*;
use adx_agent_core::{
    environment::{CreateEnvironment, ExecutionSpec},
    mounts::{Mount, Parameters},
    volumes::VolumeSpec,
};
use std::collections::BTreeMap;

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
    pub(super) async fn activate_environment(
        &self,
        binding: &AgentBinding,
        mut execution: ExecutionSpec,
        volumes: &[VolumeSpec],
        deadline: u64,
    ) -> Result<EnvironmentObservation> {
        let deadline_at = Instant::now()
            .checked_add(Duration::from_millis(
                deadline.saturating_sub(adx_agent_core::unix_time_millis()),
            ))
            .ok_or_else(|| Error::Invalid("activation deadline is out of range".into()))?;
        let mut fixed = BTreeMap::new();
        let mut backoff = 50_u64;
        let mut retry = false;
        loop {
            if retry && self.state.binding(&binding.scope).await?.as_ref() != Some(binding) {
                return Err(Error::Conflict("binding changed during activation".into()));
            }
            let remaining = deadline_at.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(Error::OutcomeUnknown(
                    "activation deadline expired; query the same AgentBinding".into(),
                ));
            }
            let result = tokio::time::timeout(remaining, async {
                for volume in volumes {
                    let reference = self
                        .environment
                        .ensure_volume(&binding.scope.tenant, &binding.environment_id, volume)
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
                match self
                    .environment
                    .get(&binding.scope.tenant, &binding.environment_id)
                    .await?
                {
                    Some(observed) => Ok(observed),
                    None => {
                        self.environment
                            .create(&CreateEnvironment {
                                id: binding.environment_id.clone(),
                                tenant: binding.scope.tenant.clone(),
                                execution: execution.clone(),
                                deadline_unix_ms: Some(deadline),
                            })
                            .await
                    }
                }
            })
            .await
            .map_err(|_| {
                Error::OutcomeUnknown(
                    "activation response deadline expired; query the same AgentBinding".into(),
                )
            })?;
            match result {
                Ok(observed) => return Ok(observed),
                Err(EnvironmentError::Unavailable(_) | EnvironmentError::OutcomeUnknown(_)) => {
                    retry = true;
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
