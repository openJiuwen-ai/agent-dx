//! Assemble an anonymous seed from the deployment-owned profile and a fixed template.
use crate::{Activator, Error, Result};
use adx_agent_core::{
    environment::{EnvironmentError, ExecutionSpec},
    mounts::{CreateOptions, Mount, MountType},
    prewarm::{PrewarmRecipe, SeedExecution, SeedResult},
    volumes::{AccessMode, VolumeSource, VolumeSpec},
    TemplateDraft,
};
use adx_agent_store::PreparationJob;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfiguredSeedProfile {
    tenant: String,
    directory_root: String,
    profile: Profile,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Profile {
    schema_version: u32,
    profile: String,
    template: String,
    inherit_from_published_template: Vec<String>,
    requires: Requirements,
    seed: Seed,
    restore: Restore,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Requirements {
    isolation_runtime: String,
    readiness_http_path: String,
    seed_readiness_http_path: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Seed {
    inherit_entrypoint: bool,
    env_from_template: Vec<String>,
    env: BTreeMap<String, String>,
    workspace: Workspace,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Workspace {
    identity_env: String,
    directory_prefix: String,
    storage_domain: String,
    mount_target: String,
    access_mode: AccessMode,
    uid: u32,
    gid: u32,
    mode: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Restore {
    handoff_contract_version: String,
    env: BTreeMap<String, String>,
}

fn safe_seed_env(name: &str, value: &str) -> bool {
    let upper = name.to_ascii_uppercase();
    !name.is_empty()
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        && !upper.starts_with("ADX_")
        && !upper.starts_with("EXECD_")
        && !["PASSWORD", "TOKEN", "PRIVATE_KEY", "API_KEY", "API_BASE"]
            .iter()
            .any(|marker| upper.contains(marker))
        && !upper.ends_with("_SECRET")
        && !value.contains("${")
        && !value.chars().any(char::is_control)
}

impl ConfiguredSeedProfile {
    fn scope(&self) -> (&str, &str) {
        (&self.tenant, &self.profile.template)
    }

    fn build(&self, draft: &TemplateDraft) -> Result<PreparationJob> {
        let profile = &self.profile;
        let seed = &profile.seed;
        let ws = &seed.workspace;
        let reject =
            || Error::Invalid("template is incompatible with its Golden seed profile".into());
        let expected: BTreeSet<_> = ["image", "isolation_runtime", "resources", "readiness_probe"]
            .into_iter()
            .collect();
        if profile.schema_version != 1
            || !seed.inherit_entrypoint
            || profile
                .inherit_from_published_template
                .iter()
                .map(String::as_str)
                .collect::<BTreeSet<_>>()
                != expected
            || profile.requires.isolation_runtime != draft.isolation_runtime
            || profile.requires.seed_readiness_http_path == profile.requires.readiness_http_path
            || !draft
                .readiness_probe
                .as_ref()
                .and_then(|probe| probe.http_get.as_ref())
                .is_some_and(|probe| {
                    probe.path == profile.requires.readiness_http_path
                        && draft
                            .service
                            .iter()
                            .any(|service| service.port == probe.port)
                })
            || ws.access_mode != AccessMode::Exclusive
            || seed.env.contains_key(&ws.identity_env)
        {
            return Err(reject());
        }
        let id = uuid::Uuid::new_v4().to_string();
        let mut env = seed.env.clone();
        for name in &seed.env_from_template {
            if env.contains_key(name) || name == &ws.identity_env || !safe_seed_env(name, "") {
                return Err(reject());
            }
            if let Some(value) = draft.env.get(name) {
                env.insert(name.clone(), value.clone());
            }
        }
        if !safe_seed_env(&ws.identity_env, "")
            || env.iter().any(|(key, value)| !safe_seed_env(key, value))
        {
            return Err(reject());
        }
        if profile.template == "digitalmate" {
            for (key, value) in &seed.env {
                if key != "DIGITALMATE_GOLDEN_PHASE" && draft.env.get(key) != Some(value) {
                    return Err(Error::Invalid(format!(
                        "DigitalMate Golden requires the same fixed template environment field {key}"
                    )));
                }
            }
            for (key, value) in &draft.env {
                if key != &ws.identity_env && env.get(key) != Some(value) {
                    return Err(Error::Invalid(format!(
                        "DigitalMate Golden cannot carry template environment field {key}"
                    )));
                }
            }
            let value = |key: &str| env.get(key).map(String::as_str).filter(|v| !v.is_empty());
            if profile.requires.seed_readiness_http_path != "/health/ready/golden-seed"
                || profile.restore.env.len() != 1
                || draft.env.get(&ws.identity_env).map(String::as_str) != Some("${workspace}")
                || value("DIGITALMATE_GOLDEN_PHASE") != Some("seed")
                || profile
                    .restore
                    .env
                    .get("DIGITALMATE_GOLDEN_PHASE")
                    .map(String::as_str)
                    != Some("restore")
                || ws.identity_env != "DIGITALMATE_CRYPTO_WORKSPACE"
                || value("DIGITALMATE_WORKSPACE") != Some(ws.mount_target.as_str())
                || value("DIGITALMATE_CRYPTO_TENANT").is_none()
                || value("DIGITALMATE_MODEL_CONFIG_NAME").is_none()
                || !value("DIGITALMATE_CRYPTO_URL")
                    .and_then(|v| reqwest::Url::parse(v).ok())
                    .is_some_and(|url| {
                        matches!(url.scheme(), "https" | "http")
                            && url.host().is_some()
                            && url.username().is_empty()
                            && url.password().is_none()
                            && url.query().is_none()
                            && url.fragment().is_none()
                    })
            {
                return Err(reject());
            }
        }
        let identity = format!("adx-agent-seed-{id}");
        env.insert(ws.identity_env.clone(), identity.clone());
        let volume = VolumeSpec {
            id: identity.clone(),
            source: VolumeSource::Directory {
                storage_domain: ws.storage_domain.clone(),
                path: format!("{}/{}/{identity}", self.directory_root, ws.directory_prefix),
            },
            access_mode: AccessMode::Exclusive,
            create_if_missing: true,
            create_options: Some(CreateOptions {
                uid: ws.uid,
                gid: ws.gid,
                mode: ws.mode.clone(),
            }),
        };
        volume.validate().map_err(Error::Invalid)?;
        let mut execution = ExecutionSpec::from(&draft.with_version("1".into()));
        execution.env = env;
        execution.lifecycle = Default::default();
        execution
            .readiness_probe
            .as_mut()
            .and_then(|probe| probe.http_get.as_mut())
            .ok_or_else(reject)?
            .path = profile.requires.seed_readiness_http_path.clone();
        execution.mounts = vec![Mount {
            kind: MountType::Bind,
            host_path: String::new(),
            volume_id: Some(identity),
            volume_uid: None,
            target: ws.mount_target.clone(),
            options: vec!["rw".into()],
            create_if_missing: false,
            create_options: None,
        }];
        execution.validate().map_err(Error::Invalid)?;
        let image_digest = draft
            .image
            .rsplit_once('@')
            .ok_or_else(reject)?
            .1
            .to_owned();
        let seed_env: BTreeMap<_, _> = execution
            .env
            .iter()
            .filter(|(name, _)| *name != &ws.identity_env)
            .collect();
        let normalized = serde_json::to_vec(&(
            &profile.profile,
            profile.schema_version,
            &draft.image,
            &draft.isolation_runtime,
            &draft.resources,
            &execution.readiness_probe,
            &draft.healthcheck,
            &draft.service,
            &draft.mounts,
            &draft.volumes,
            seed_env,
            &profile.restore.handoff_contract_version,
            &profile.restore.env,
        ))
        .map_err(|_| reject())?;
        let recipe = PrewarmRecipe {
            snapshot_id: "pending".into(),
            image_digest,
            restore_profile_digest: format!("sha256:{:x}", Sha256::digest(normalized)),
            handoff_contract_version: profile.restore.handoff_contract_version.clone(),
            restore_env: profile.restore.env.clone(),
        };
        recipe.validate(&draft.image).map_err(Error::Invalid)?;
        Ok(PreparationJob {
            id,
            tenant: self.tenant.clone(),
            template: draft.name.clone(),
            version: String::new(),
            seed: SeedExecution {
                execution,
                volume,
                native_input: None,
            },
            recipe,
        })
    }
}

impl Activator {
    pub fn with_seed_profiles(mut self, profiles: Vec<ConfiguredSeedProfile>) -> Result<Self> {
        for profile in profiles {
            let (tenant, template) = profile.scope();
            adx_agent_core::identifier(tenant, "seed profile tenant").map_err(Error::Invalid)?;
            adx_agent_core::identifier(template, "seed profile template")
                .map_err(Error::Invalid)?;
            if self
                .seed_profiles
                .insert((tenant.into(), template.into()), profile)
                .is_some()
            {
                return Err(Error::Invalid("duplicate Golden seed profile".into()));
            }
        }
        Ok(self)
    }

    pub fn with_deployed_seed_profiles(self) -> Result<Self> {
        match std::env::var_os("ADX_GOLDEN_SEED_PROFILES") {
            None => Ok(self),
            Some(path) => {
                let data = std::fs::read(path)
                    .map_err(|_| Error::Invalid("cannot read Golden seed profiles".into()))?;
                let profiles = serde_json::from_slice(&data)
                    .map_err(|_| Error::Invalid("invalid Golden seed profiles".into()))?;
                self.with_seed_profiles(profiles)
            }
        }
    }

    pub(crate) fn seed_job(&self, tenant: &str, draft: &TemplateDraft) -> Result<PreparationJob> {
        let mut job = self
            .seed_profiles
            .get(&(tenant.into(), draft.name.clone()))
            .ok_or_else(|| {
                Error::Invalid("no Golden seed profile for this tenant and template".into())
            })?
            .build(draft)?;
        job.seed.native_input = self
            .environment
            .freeze_preparation(tenant, &job.id, &job.seed)?;
        Ok(job)
    }

    pub fn start_snapshot_preparations(self: &Arc<Self>) {
        // A deployment may stop admitting new jobs while older jobs still carry
        // a durable seed. Keep reconciling those jobs without reading the profile.
        let period = if self.seed_profiles.is_empty() { 15 } else { 3 };
        let service = Arc::downgrade(self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(period));
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
                        recipe.validate(&job.seed.execution.image).map(|()| recipe)
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
