//! Freeze the published execution contract and substitute only its anonymous workspace.
use crate::{Activator, Error, Result};
use adx_agent_core::{
    environment::{EnvironmentError, ExecutionSpec},
    mounts::Parameters,
    prewarm::{PrewarmRecipe, SeedExecution, SeedResult, TEMPLATE_HANDOFF_VERSION},
    volumes::{AccessMode, VolumeSource, VolumeSpec},
    TemplateVersion,
};
use adx_agent_store::PreparationJob;
use sha2::{Digest, Sha256};
use std::{sync::Arc, time::Duration};

const WORKSPACE_PARAMETER: &str = "${workspace}";
const PREPARATION_LIFETIME_MS: u64 = 15 * 60 * 1000;
const PREPARATION_ATTEMPT_CAP: Duration = Duration::from_secs(15);
const SEED_PREFIX: &str = "adx-agent-seed-";

fn seed_volume(template: &TemplateVersion, identity: &str) -> Result<VolumeSpec> {
    let ([mount], [volume]) = (template.mounts.as_slice(), template.volumes.as_slice()) else {
        return Err(Error::Invalid(
            "Golden requires one managed workspace mount".into(),
        ));
    };
    let VolumeSource::Directory {
        storage_domain,
        path,
    } = &volume.source;
    if mount.volume_id.as_deref() != Some(volume.id.as_str())
        || mount.volume_uid.is_some()
        || !mount.host_path.is_empty()
        || mount.target.contains("${")
        || mount.options.iter().any(|option| option == "ro")
        || volume.access_mode != AccessMode::Exclusive
        || !volume.create_if_missing
        || !volume.id.ends_with(WORKSPACE_PARAMETER)
        || volume.id.matches("${").count() != 1
        || !path.ends_with("/${workspace}")
        || path.matches("${").count() != 1
        || storage_domain.contains("${")
    {
        return Err(Error::Invalid(
            "Golden requires a writable exclusive directory Volume keyed by workspace".into(),
        ));
    }
    let params = Parameters::from([("workspace".into(), identity.into())]);
    let mut anonymous = volume.render(Some(&params)).map_err(Error::Invalid)?;
    anonymous.id = identity.into();
    Ok(anonymous)
}

pub(super) fn reserved_workspace(parameters: &Parameters) -> bool {
    parameters
        .get("workspace")
        .is_some_and(|value| value.starts_with(SEED_PREFIX))
}

pub(super) fn restore_digest(template: &TemplateVersion) -> Result<String> {
    let spec = ExecutionSpec::from(template);
    let normalized = serde_json::to_vec(&(TEMPLATE_HANDOFF_VERSION, spec, &template.volumes))
        .map_err(|_| Error::Invalid("cannot encode Golden template contract".into()))?;
    Ok(format!("sha256:{:x}", Sha256::digest(normalized)))
}

impl Activator {
    pub(crate) fn seed_job(
        &self,
        tenant: &str,
        template: &TemplateVersion,
    ) -> Result<PreparationJob> {
        if template.isolation_runtime != "runsc" || template.readiness_probe.is_none() {
            return Err(Error::Invalid(
                "Golden requires runsc and an explicit readiness probe".into(),
            ));
        }
        let id = uuid::Uuid::new_v4().to_string();
        let identity = format!("{SEED_PREFIX}{id}");
        let volume = seed_volume(template, &identity)?;
        let params = Parameters::from([("workspace".into(), identity)]);
        let mut execution = ExecutionSpec::from(template);
        execution.env = template.render_env(&params).map_err(Error::Invalid)?;
        execution.lifecycle = Default::default();
        execution.mounts = template.render_mounts(&params).map_err(Error::Invalid)?;
        execution.mounts[0].volume_id = Some(volume.id.clone());
        self.environment.validate_execution(&execution)?;
        let (_, image_digest) = template
            .image
            .rsplit_once('@')
            .ok_or_else(|| Error::Invalid("Golden requires a digest-pinned image".into()))?;
        let recipe = PrewarmRecipe {
            snapshot_id: "pending".into(),
            image_digest: image_digest.into(),
            restore_profile_digest: restore_digest(template)?,
            handoff_contract_version: TEMPLATE_HANDOFF_VERSION.into(),
            restore_env: Default::default(),
        };
        recipe.validate(&template.image).map_err(Error::Invalid)?;
        let mut seed = SeedExecution {
            execution,
            volume,
            native_input: None,
        };
        seed.native_input = self.environment.freeze_preparation(tenant, &id, &seed)?;
        Ok(PreparationJob {
            resolved_snapshot: None,
            discard_reason: None,
            id,
            deadline_unix_ms: adx_agent_core::unix_time_millis()
                .saturating_add(PREPARATION_LIFETIME_MS),
            tenant: tenant.into(),
            template: template.name.clone(),
            version: template.version.clone(),
            seed,
            recipe,
        })
    }

    pub fn start_snapshot_preparations(self: &Arc<Self>) {
        let service = Arc::downgrade(self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(3));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                interval.tick().await;
                let Some(service) = service.upgrade() else {
                    break;
                };
                if let Err(error) = service.reconcile_snapshot_preparations().await {
                    tracing::warn!(%error, "Golden preparation scan incomplete");
                }
            }
        });
    }

    pub async fn reconcile_snapshot_preparations(&self) -> Result<()> {
        let mut cursor = None;
        loop {
            let jobs = self.state.preparation_jobs(cursor.as_deref()).await?;
            if jobs.is_empty() {
                return Ok(());
            }
            for (key, job) in jobs {
                cursor = Some(key);
                match tokio::time::timeout(PREPARATION_ATTEMPT_CAP, self.prepare_golden(&job)).await
                {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => {
                        tracing::warn!(preparation_id = %job.id, tenant = %job.tenant,
                        template = %job.template, version = %job.version, %error, "Golden preparation will retry")
                    }
                    Err(_) => tracing::warn!(preparation_id = %job.id, tenant = %job.tenant,
                        template = %job.template, version = %job.version, "Golden preparation attempt timed out"),
                }
            }
        }
    }

    async fn prepare_golden(&self, job: &PreparationJob) -> Result<()> {
        if job.resolved_snapshot.is_some() {
            return self.finish_resolved_snapshot(job).await;
        }
        let expired = || adx_agent_core::transport::remaining_time(job.deadline_unix_ms).is_zero();
        let snapshot = match self
            .environment
            .prepare_snapshot(&job.tenant, &job.id, &job.seed, job.deadline_unix_ms)
            .await
        {
            Ok(SeedResult::Pending) => return Ok(()),
            Ok(SeedResult::Ready { snapshot_id }) => snapshot_id,
            Ok(SeedResult::Failed) => {
                let reason = if expired() {
                    adx_agent_store::PREPARATION_DEADLINE_EXCEEDED
                } else {
                    "Platform snapshot preparation failed"
                };
                return self
                    .state
                    .finish_preparation(job, Err(reason.into()))
                    .await
                    .map_err(Into::into);
            }
            Err(EnvironmentError::Invalid(_)) => {
                return self
                    .state
                    .finish_preparation(job, Err("Platform rejected the seed specification".into()))
                    .await
                    .map_err(Into::into)
            }
            Err(error) => return Err(error.into()),
        };
        let mut recipe = job.recipe.clone();
        recipe.snapshot_id.clone_from(&snapshot);
        let discard = if expired() {
            Some(adx_agent_store::PREPARATION_DEADLINE_EXCEEDED.into())
        } else if !self
            .environment
            .golden_available(
                &job.tenant,
                &snapshot,
                &job.seed.execution.image,
                &job.seed.execution.isolation_runtime,
            )
            .await?
        {
            Some("prepared Golden is not compatible".into())
        } else {
            recipe.validate(&job.seed.execution.image).err()
        };
        let discard = discard
            .or_else(|| expired().then(|| adx_agent_store::PREPARATION_DEADLINE_EXCEEDED.into()));
        let saved = self
            .state
            .reserve_recipe_owner(job, &snapshot, discard)
            .await?;
        self.finish_resolved_snapshot(&saved).await
    }

    async fn finish_resolved_snapshot(&self, job: &PreparationJob) -> Result<()> {
        let id = job
            .resolved_snapshot
            .as_ref()
            .ok_or_else(|| Error::Conflict("preparation has no resolved snapshot".into()))?;
        let result = if let Some(reason) = &job.discard_reason {
            match self
                .environment
                .update_snapshot_reference(
                    &job.tenant,
                    id,
                    &adx_agent_core::environment::SnapshotChange::Retire,
                )
                .await
            {
                Ok(()) | Err(EnvironmentError::NotFound) => {}
                Err(error) => return Err(error.into()),
            }
            Err(reason.clone())
        } else {
            let mut recipe = job.recipe.clone();
            recipe.snapshot_id.clone_from(id);
            self.register_recipe(job, recipe).await?
        };
        self.state
            .finish_preparation(job, result)
            .await
            .map_err(Into::into)
    }

    async fn register_recipe(
        &self,
        job: &PreparationJob,
        recipe: PrewarmRecipe,
    ) -> Result<std::result::Result<PrewarmRecipe, String>> {
        if recipe.handoff_contract_version == TEMPLATE_HANDOFF_VERSION {
            use adx_agent_core::environment::{SnapshotChange, SnapshotOwner};
            let owner = SnapshotOwner::Template {
                name: job.template.clone(),
                version: job.version.clone(),
            };
            match self
                .environment
                .update_snapshot_reference(
                    &job.tenant,
                    &recipe.snapshot_id,
                    &SnapshotChange::Acquire { owner },
                )
                .await
            {
                Ok(()) => {}
                Err(EnvironmentError::Conflict(_) | EnvironmentError::NotFound) => {
                    return Ok(Err("Golden snapshot is retiring".into()))
                }
                Err(error) => return Err(error.into()),
            }
        }
        Ok(Ok(recipe))
    }
}
