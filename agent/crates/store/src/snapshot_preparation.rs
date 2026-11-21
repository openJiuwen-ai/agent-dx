//! Version-specific preparation outbox and write-once result. The template body stays immutable.
use crate::{
    state::{check, put},
    *,
};
use adx_agent_core::{
    prewarm::{PrewarmRecipe, SeedExecution, SnapshotPreparation},
    TemplateVersion,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PreparationJob {
    pub id: String,
    /// Absolute retry deadline, retained across worker restarts.
    #[serde(default)]
    pub deadline_unix_ms: u64,
    pub tenant: String,
    pub template: String,
    pub version: String,
    pub seed: SeedExecution,
    /// Validated recipe with a placeholder snapshot ID replaced only on success.
    pub recipe: PrewarmRecipe,
    /// Durable intent before asking Catalog to pin this recipe.
    #[serde(default)]
    pub resolved_snapshot: Option<String>,
    /// If set, this preparation must retire its result before completing as failed.
    #[serde(default)]
    pub discard_reason: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct PreparationResult {
    pub status: SnapshotPreparation,
    pub recipe: Option<PrewarmRecipe>,
}

pub(crate) fn pending_key(job: &PreparationJob) -> Result<Key> {
    preparation_key(&job.tenant, &job.template, &job.version)
}

fn preparation_key(tenant: &str, template: &str, version: &str) -> Result<Key> {
    Key::new("snapshot-preparation", &[tenant, template, version])
}

pub const PREPARATION_DEADLINE_EXCEEDED: &str = "Golden preparation deadline exceeded";

fn result_key(tenant: &str, template: &str, version: &str) -> Result<Key> {
    Key::new("snapshot-preparation-result", &[tenant, template, version])
}

impl AgentState {
    pub async fn preparation_jobs(
        &self,
        after: Option<&str>,
    ) -> Result<Vec<(String, PreparationJob)>> {
        self.store
            .page(&Index::snapshot_preparations(), after, 64)
            .await?
            .into_iter()
            .map(|(key, record)| Ok((key, record.decode()?)))
            .collect()
    }

    pub async fn reserve_recipe_owner(
        &self,
        job: &PreparationJob,
        snapshot: &str,
        discard_reason: Option<String>,
    ) -> Result<PreparationJob> {
        let key = pending_key(job)?;
        for _ in 0..adx_agent_core::limits::CAS_ATTEMPTS {
            let record = self
                .store
                .get(&key)
                .await?
                .ok_or_else(|| Error::Conflict("preparation already completed".into()))?;
            let mut saved: PreparationJob = record.decode()?;
            if saved.id != job.id {
                return Err(Error::Conflict("preparation changed".into()));
            }
            if let Some(id) = &saved.resolved_snapshot {
                return if id == snapshot {
                    Ok(saved)
                } else {
                    Err(Error::Conflict(
                        "preparation produced another snapshot".into(),
                    ))
                };
            }
            saved.resolved_snapshot = Some(snapshot.into());
            saved.discard_reason = discard_reason.clone();
            if self
                .store
                .commit(&Transaction::new(
                    vec![check(&key, &record)],
                    vec![put(&key, &saved)?],
                )?)
                .await?
            {
                return Ok(saved);
            }
        }
        Err(Error::Conflict("preparation contention".into()))
    }
    pub async fn finish_preparation(
        &self,
        job: &PreparationJob,
        result: std::result::Result<PrewarmRecipe, String>,
    ) -> Result<()> {
        let pending = pending_key(job)?;
        let Some(record) = self.store.get(&pending).await? else {
            return Ok(());
        };
        let current: PreparationJob = record.decode()?;
        if current.id != job.id
            || (result.is_err() && current.resolved_snapshot != job.resolved_snapshot)
        {
            return Err(Error::Conflict(
                "snapshot preparation identity changed".into(),
            ));
        }
        let key = result_key(&job.tenant, &job.template, &job.version)?;
        let value = match result {
            Ok(recipe) => PreparationResult {
                status: SnapshotPreparation::Ready,
                recipe: Some(recipe),
            },
            Err(reason) => PreparationResult {
                status: SnapshotPreparation::Failed { reason },
                recipe: None,
            },
        };
        self.store
            .commit(&Transaction::with_deletes(
                vec![
                    check(&pending, &record),
                    Check {
                        key: key.clone(),
                        expected: None,
                    },
                ],
                vec![put(&key, &value)?],
                vec![pending],
            )?)
            .await?;
        Ok(())
    }

    pub(crate) async fn project_preparation(
        &self,
        tenant: &str,
        template: &TemplateVersion,
    ) -> Result<TemplateVersion> {
        let mut view = template.clone();
        if !template.auto_prepare_snapshot {
            return Ok(view);
        }
        let key = result_key(tenant, &template.name, &template.version)?;
        let result = match self.store.get(&key).await? {
            Some(result) => Some(result),
            None => {
                let pending = preparation_key(tenant, &template.name, &template.version)?;
                if let Some(record) = self.store.get(&pending).await? {
                    let job: PreparationJob = record.decode()?;
                    let reason = job.discard_reason.or_else(|| {
                        (job.resolved_snapshot.is_none()
                            && adx_agent_core::transport::remaining_time(job.deadline_unix_ms)
                                .is_zero())
                        .then(|| PREPARATION_DEADLINE_EXCEEDED.into())
                    });
                    if let Some(reason) = reason {
                        view.snapshot_preparation = Some(SnapshotPreparation::Failed { reason });
                    }
                    return Ok(view);
                }
                // The result may have committed between the result and outbox reads.
                self.store.get(&key).await?
            }
        };
        if let Some(record) = result {
            let result: PreparationResult = record.decode()?;
            view.snapshot_preparation = Some(result.status);
            view.prewarm_recipe = result.recipe;
        }
        Ok(view)
    }
}
