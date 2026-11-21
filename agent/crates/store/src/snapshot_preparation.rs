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
}

#[derive(Serialize, Deserialize)]
pub(crate) struct PreparationResult {
    pub status: SnapshotPreparation,
    pub recipe: Option<PrewarmRecipe>,
}

pub(crate) fn pending_key(job: &PreparationJob) -> Result<Key> {
    Key::new(
        "snapshot-preparation",
        &[&job.tenant, &job.template, &job.version],
    )
}

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

    pub async fn finish_preparation(
        &self,
        job: &PreparationJob,
        result: std::result::Result<PrewarmRecipe, String>,
    ) -> Result<()> {
        let pending = pending_key(job)?;
        let Some(record) = self.store.get(&pending).await? else {
            return Ok(());
        };
        if record.decode::<PreparationJob>()?.id != job.id {
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
        if template.auto_prepare_snapshot {
            let key = result_key(tenant, &template.name, &template.version)?;
            if let Some(record) = self.store.get(&key).await? {
                let result: PreparationResult = record.decode()?;
                view.snapshot_preparation = Some(result.status);
                view.prewarm_recipe = result.recipe;
            }
        }
        Ok(view)
    }
}
