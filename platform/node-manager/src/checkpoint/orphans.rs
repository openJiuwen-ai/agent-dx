//! A node reclaims only uploads from its retired process sessions, after complete
//! catalog recovery. Current-session and unowned objects are never candidates.
use super::io_error;
use super::object::{invalid, ObjectCheckpointStore};
use adx_core::{CheckpointArtifact, Error, Result};
use futures_util::TryStreamExt;
use object_store::ObjectStoreExt;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RemoteGcConfig {
    pub enabled: bool,
    pub min_age_seconds: u64,
    pub interval_seconds: u64,
    pub max_artifacts: usize,
}
impl Default for RemoteGcConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            min_age_seconds: 86400,
            interval_seconds: 300,
            max_artifacts: 100,
        }
    }
}
impl RemoteGcConfig {
    pub fn validate(&self) -> Result<()> {
        if self.interval_seconds == 0
            || self.max_artifacts == 0
            || self.max_artifacts > 10000
            || self.min_age_seconds > i64::MAX as u64
        {
            return Err(invalid("invalid remote GC interval, age or batch limit"));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct UploadOwner {
    pub version: u32,
    pub node_id: String,
    pub session_id: String,
}
impl ObjectCheckpointStore {
    pub fn with_owner(
        mut self,
        node_id: String,
        session_id: String,
        config: RemoteGcConfig,
    ) -> Result<Self> {
        config.validate()?;
        for id in [&node_id, &session_id] {
            if id.is_empty() || id.len() > 256 || id.chars().any(char::is_control) {
                return Err(invalid("invalid upload owner"));
            }
        }
        self.owner = Some(UploadOwner {
            version: 1,
            node_id,
            session_id,
        });
        self.gc_config = config;
        Ok(self)
    }
    pub(super) async fn mark_upload(&self, id: &str) -> Result<()> {
        if let Some(owner) = &self.owner {
            let bytes = serde_json::to_vec(owner).map_err(invalid)?;
            self.remote
                .put(&self.key(id, "owner.json"), bytes.into())
                .await
                .map_err(io_error)?;
        }
        Ok(())
    }
    pub(super) async fn authorize(&self, retained: &[CheckpointArtifact]) -> Result<()> {
        // Preserve the union for this boot. Once registered, ordinary Instance /
        // snapshot deletion owns the artifact's cleanup, not this orphan collector.
        let mut validated = BTreeSet::new();
        for artifact in retained.iter().filter(|a| a.storage == self.alias) {
            self.validate(artifact)?;
            validated.insert(artifact.location.clone());
        }
        self.gc_authority
            .lock()
            .await
            .get_or_insert_with(BTreeSet::new)
            .extend(validated);
        Ok(())
    }
    pub(super) async fn collect(&self) -> Result<usize> {
        let Some(owner) = &self.owner else {
            return Ok(0);
        };
        if !self.gc_config.enabled {
            return Ok(0);
        }
        let authority = self.gc_authority.lock().await;
        let retained = authority.as_ref().ok_or_else(|| {
            Error::Unavailable("remote GC requires complete authoritative recovery".into())
        })?;
        let mut cache = self.cache.lock().await;
        let now = super::now()?;
        let old_enough = |timestamp: i64| {
            timestamp >= 0
                && now
                    .checked_sub(timestamp as u64)
                    .is_some_and(|age| age >= self.gc_config.min_age_seconds)
        };
        let mut objects = self.remote.list(Some(&self.prefix));
        let mut removed = 0;
        while let Some(meta) = objects.try_next().await.map_err(io_error)? {
            let name = meta.location.as_ref();
            let prefix = format!("{}/", self.prefix);
            let Some(relative) = name.strip_prefix(&prefix) else {
                continue;
            };
            let Some(id) = relative.strip_suffix("/owner.json") else {
                continue;
            };
            if uuid::Uuid::parse_str(id).is_err()
                || retained.contains(id)
                || !old_enough(meta.last_modified.timestamp())
                || meta.size > 2048
                || cache
                    .entries
                    .get(id)
                    .is_some_and(|entry| std::sync::Arc::strong_count(&entry.pin) > 1)
            {
                continue;
            }
            let data = match self.remote.get(&meta.location).await {
                Ok(data) if data.meta.size <= 2048 => data.bytes().await.map_err(io_error)?,
                Ok(_) | Err(object_store::Error::NotFound { .. }) => continue,
                Err(error) => return Err(io_error(error)),
            };
            let Ok(candidate) = serde_json::from_slice::<UploadOwner>(&data) else {
                continue;
            };
            if candidate.version != 1
                || candidate.node_id != owner.node_id
                || candidate.session_id.is_empty()
                || candidate.session_id == owner.session_id
            {
                continue;
            }
            // A late object completion moves the age boundary forward. Partial
            // uploads are eligible too; a manifest is not required to find them.
            let mut children = self.remote.list(Some(&self.key(id, "")));
            let mut eligible = true;
            while let Some(child) = children.try_next().await.map_err(io_error)? {
                if !old_enough(child.last_modified.timestamp()) {
                    eligible = false;
                    break;
                }
            }
            if !eligible {
                continue;
            }
            self.delete_objects(id).await?;
            if let Some(entry) = cache.entries.remove(id) {
                tokio::fs::remove_dir_all(entry.path)
                    .await
                    .map_err(io_error)?;
            }
            removed += 1;
            if removed >= self.gc_config.max_artifacts {
                break;
            }
        }
        Ok(removed)
    }
}
