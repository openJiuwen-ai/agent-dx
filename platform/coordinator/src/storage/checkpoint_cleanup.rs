//! Durable outbox for checkpoint objects whose Environment has left the catalog.
use super::*;
use adx_core::{CheckpointArtifact, CheckpointCleanup};

/// Canonical request payload for the bounded Deleted receipt.
pub(super) fn normalized_artifacts(artifacts: &[CheckpointArtifact]) -> Vec<CheckpointArtifact> {
    let mut artifacts = artifacts.to_vec();
    artifacts.sort_by(|a, b| {
        (&a.storage, &a.scope, &a.location, a.size_bytes).cmp(&(
            &b.storage,
            &b.scope,
            &b.location,
            b.size_bytes,
        ))
    });
    artifacts.dedup();
    artifacts
}

impl Session {
    fn checkpoint_cleanup_key(&self) -> String {
        format!("{}:checkpoint-cleanups", self.store.key)
    }

    async fn cleanup_records(&self, key: String) -> Result<Vec<CheckpointCleanup>> {
        let [header] = self.store.fields([HEADER.into()]).await?;
        self.header(&header)?;
        let mut command = redis::cmd("HVALS");
        command.arg(key);
        let values: Vec<String> = self.store.query(command).await?;
        let mut tasks = values
            .into_iter()
            .map(|value| {
                let task: CheckpointCleanup = decode(&value)?;
                task.validate()?;
                Ok(task)
            })
            .collect::<Result<Vec<_>>>()?;
        tasks.sort_by_key(|task| task.id());
        Ok(tasks)
    }

    /// No TTL: a crash or an unavailable storage backend cannot expire work.
    pub async fn checkpoint_cleanups(&self) -> Result<Vec<CheckpointCleanup>> {
        self.cleanup_records(self.checkpoint_cleanup_key()).await
    }

    pub(super) async fn prepare_checkpoint_cleanup(
        &self,
        old: &StoredEnvironment,
        result: &EnvironmentRecord,
        artifacts: &[CheckpointArtifact],
    ) -> Result<Option<(CheckpointCleanup, Vec<ArtifactDeletionGuard>)>> {
        let mut artifacts = artifacts.to_vec();
        if let Some(cp) = old.result.as_ref().and_then(|r| r.checkpoint.as_ref()) {
            artifacts.push(cp.artifact.clone());
        }
        if let Some(cp) = &result.checkpoint {
            artifacts.push(cp.artifact.clone());
        }
        for record in old.result.iter().chain(std::iter::once(result)) {
            artifacts.extend(
                record
                    .execution_artifacts
                    .iter()
                    .filter(|artifact| artifact.size_bytes == 0)
                    .cloned(),
            );
        }
        // Shared artifacts already registered by normal lifecycle publication are
        // reclaimed by the existing registry/GC, including refs and execution holds.
        let registered = self.shared_artifacts().await?;
        artifacts.retain(|artifact| {
            artifact.storage == "local"
                || !registered
                    .iter()
                    .any(|registered| registered.aliases(artifact))
        });
        let artifacts = normalized_artifacts(&artifacts);
        if artifacts.is_empty() {
            return Ok(None);
        }
        let task = CheckpointCleanup {
            assignment: result.assignment.clone(),
            revision: result.revision,
            artifacts,
        };
        task.validate()?;
        // Replace the former global header dependency with exclusion only for
        // these candidates. Callers retain the guards until retirement commits.
        let guards = self.store.lock_artifact_deletions(&task.artifacts)?;
        if !self
            .checkpoint_cleanup_unreferenced(&task, Some(&result.spec.id))
            .await?
        {
            return Err(Error::Conflict);
        }
        Ok(Some((task, guards)))
    }

    /// Defense against corrupt aliased ownership and incomplete catalog cleanup.
    pub async fn checkpoint_cleanup_unreferenced(
        &self,
        task: &CheckpointCleanup,
        retiring: Option<&str>,
    ) -> Result<bool> {
        let snapshot = self.snapshot().await?;
        for environment in snapshot.environments.values() {
            if retiring == Some(environment.spec.id.as_str()) {
                continue;
            }
            if environment.result.as_ref().is_some_and(|record| {
                record
                    .checkpoint
                    .iter()
                    .chain(record.snapshot_source.iter())
                    .map(|point| &point.artifact)
                    .chain(record.execution_artifacts.iter())
                    .any(|artifact| task.artifacts.iter().any(|a| a.aliases(artifact)))
            }) {
                return Ok(false);
            }
        }
        if self.execution_holds().await?.iter().any(|hold| {
            hold.artifacts
                .iter()
                .any(|artifact| task.artifacts.iter().any(|a| a.aliases(artifact)))
        }) {
            return Ok(false);
        }
        Ok(!self
            .retained_snapshots()
            .await?
            .iter()
            .any(|s| task.artifacts.iter().any(|a| a.aliases(&s.artifact))))
    }

    /// Compare the full immutable task, not just the reusable Environment name.
    pub async fn finish_checkpoint_cleanup(&self, task: &CheckpointCleanup) -> Result<()> {
        task.validate()?;
        for _ in 0..ATTEMPTS {
            let [header_value] = self.store.fields([HEADER.into()]).await?;
            let mut header = self.header(&header_value)?;
            header.advance()?;
            let mut command = redis::cmd("EVAL");
            command
                .arg(
                    r#"
                if redis.call('HGET', KEYS[1], 'header') ~= ARGV[1] then return 0 end
                local old = redis.call('HGET', KEYS[2], ARGV[3])
                if not old then return 1 end
                if old ~= ARGV[4] then return 2 end
                redis.call('HDEL', KEYS[2], ARGV[3])
                redis.call('HSET', KEYS[1], 'header', ARGV[2]); return 1
            "#,
                )
                .arg(2)
                .arg(&self.store.key)
                .arg(self.checkpoint_cleanup_key())
                .arg(header_value.as_deref().ok_or(Error::Conflict)?)
                .arg(encode(&header)?)
                .arg(task.id())
                .arg(encode(task)?);
            match self.store.query::<u8>(command).await? {
                1 => return Ok(()),
                2 => return Err(Error::Conflict),
                _ => (),
            }
        }
        Err(Error::Unavailable(
            "checkpoint cleanup acknowledgement contention".into(),
        ))
    }
}
