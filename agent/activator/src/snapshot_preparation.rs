//! Freeze the published execution contract and substitute only its anonymous workspace.
use crate::{Activator, Error, Result};
use adx_agent_core::{
    environment::{EnvironmentError, ExecutionSpec},
    mounts::Parameters,
    prewarm::{PrewarmRecipe, SeedExecution, SeedResult, GOLDEN_PHASE, TEMPLATE_HANDOFF_VERSION},
    volumes::{AccessMode, VolumeSource, VolumeSpec},
    TemplateVersion,
};
use adx_agent_store::PreparationJob;
use sha2::{Digest, Sha256};
use std::{sync::Arc, time::Duration};

const WORKSPACE_PARAMETER: &str = "${workspace}";
const SEED_PREFIX: &str = "adx-agent-seed-";

fn safe_seed_env(name: &str, value: &str) -> bool {
    let upper = name.to_ascii_uppercase();
    !name.is_empty()
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        && !upper.starts_with("ADX_")
        && !upper.starts_with("EXECD_")
        && upper != "SECRET"
        && !upper.ends_with("_SECRET")
        && ![
            "PASSWORD",
            "TOKEN",
            "PRIVATE_KEY",
            "API_KEY",
            "API_BASE",
            "ACCESS_KEY",
            "SIGNING_KEY",
            "CREDENTIAL",
        ]
        .iter()
        .any(|marker| upper.contains(marker))
        && !value.chars().any(char::is_control)
        && !reqwest::Url::parse(value).ok().is_some_and(|url| {
            url.has_host()
                && (!url.username().is_empty() || url.password().is_some() || url.query().is_some())
        })
        && (!value.contains("${") || value == WORKSPACE_PARAMETER)
}

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
        for (key, value) in &template.env {
            if key == "DIGITALMATE_GOLDEN_PHASE" || !safe_seed_env(key, value) {
                return Err(Error::Invalid(format!(
                    "Golden cannot capture template environment field {key}"
                )));
            }
        }
        let id = uuid::Uuid::new_v4().to_string();
        let identity = format!("{SEED_PREFIX}{id}");
        let volume = seed_volume(template, &identity)?;
        let params = Parameters::from([("workspace".into(), identity)]);
        let mut execution = ExecutionSpec::from(template);
        execution.env = template.render_env(&params).map_err(Error::Invalid)?;
        execution.env.insert(GOLDEN_PHASE.into(), "seed".into());
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
            restore_env: [(GOLDEN_PHASE.into(), "restore".into())].into(),
        };
        recipe.validate(&template.image).map_err(Error::Invalid)?;
        let mut seed = SeedExecution {
            execution,
            volume,
            native_input: None,
        };
        seed.native_input = self.environment.freeze_preparation(tenant, &id, &seed)?;
        Ok(PreparationJob {
            id,
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
                let result = tokio::time::timeout(
                    Duration::from_secs(15),
                    self.environment
                        .prepare_snapshot(&job.tenant, &job.id, &job.seed),
                )
                .await;
                let result = match result {
                    Ok(Ok(SeedResult::Ready { snapshot_id })) => {
                        match self
                            .environment
                            .golden_available(
                                &job.tenant,
                                &snapshot_id,
                                &job.seed.execution.image,
                                &job.seed.execution.isolation_runtime,
                            )
                            .await
                        {
                            Ok(true) => {}
                            Ok(false) => {
                                self.state
                                    .finish_preparation(
                                        &job,
                                        Err("prepared Golden is not compatible or ready".into()),
                                    )
                                    .await?;
                                continue;
                            }
                            Err(_) => continue,
                        }
                        let mut recipe = job.recipe.clone();
                        recipe.snapshot_id = snapshot_id;
                        if let Err(reason) = recipe.validate(&job.seed.execution.image) {
                            Err(reason)
                        } else if recipe.handoff_contract_version == TEMPLATE_HANDOFF_VERSION {
                            match self.protect_golden(&job.tenant, &recipe.snapshot_id).await {
                                Ok(()) => Ok(recipe),
                                Err(Error::Conflict(reason))
                                    if reason == "Golden snapshot is retiring" =>
                                {
                                    Err(reason)
                                }
                                Err(_) => continue,
                            }
                        } else {
                            Ok(recipe)
                        }
                    }
                    Ok(Ok(SeedResult::Failed)) => {
                        Err("Platform snapshot preparation failed".into())
                    }
                    Ok(Err(EnvironmentError::Invalid(_))) => {
                        Err("Platform rejected the seed specification".into())
                    }
                    _ => continue,
                };
                self.state.finish_preparation(&job, result).await?;
            }
        }
    }
}
