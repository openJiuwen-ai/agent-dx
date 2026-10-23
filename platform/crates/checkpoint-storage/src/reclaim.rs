//! Coordinator-owned authoritative deletion; no staging or cache dependency.
use super::{io_error, s3_scope, FilesystemCheckpointStore};
use adx_core::{CheckpointArtifact, Error, Result};
use async_trait::async_trait;
use futures_util::TryStreamExt;
use object_store::{path::Path as Key, ObjectStore, ObjectStoreExt};
use serde::Deserialize;
use std::{collections::BTreeMap, path::PathBuf, sync::Arc};

#[async_trait]
pub trait ArtifactReclaimer: Send + Sync {
    fn storage_alias(&self) -> &str;
    async fn remove(&self, artifact: &CheckpointArtifact) -> Result<()>;
}
#[derive(Default, Clone)]
pub struct Reclaimers(BTreeMap<String, Arc<dyn ArtifactReclaimer>>);
impl Reclaimers {
    pub fn insert(&mut self, store: Arc<dyn ArtifactReclaimer>) -> Result<()> {
        let key = store.storage_alias().to_owned();
        if key == "local" || self.0.contains_key(&key) {
            return Err(Error::Invalid("duplicate or local shared reclaimer".into()));
        }
        self.0.insert(key, store);
        Ok(())
    }
    pub async fn remove(&self, artifact: &CheckpointArtifact) -> Result<()> {
        self.0
            .get(&artifact.storage)
            .ok_or_else(|| {
                Error::Unavailable(format!(
                    "shared reclaimer {} is not configured",
                    artifact.storage
                ))
            })?
            .remove(artifact)
            .await
    }
}
pub struct ObjectReclaimer {
    alias: String,
    scope: String,
    remote: Arc<dyn ObjectStore>,
    prefix: Key,
}
impl ObjectReclaimer {
    pub fn new(alias: String, remote: Arc<dyn ObjectStore>, prefix: String) -> Result<Self> {
        Self::new_scoped(alias, String::new(), remote, prefix)
    }

    pub fn new_scoped(
        alias: String,
        scope: String,
        remote: Arc<dyn ObjectStore>,
        prefix: String,
    ) -> Result<Self> {
        if alias.is_empty()
            || alias == "local"
            || prefix.is_empty()
            || prefix
                .split('/')
                .any(|c| c.is_empty() || c == "." || c == "..")
        {
            return Err(Error::Invalid("invalid object reclaimer identity".into()));
        }
        Ok(Self {
            alias,
            scope,
            remote,
            prefix: Key::from(prefix),
        })
    }
}
#[async_trait]
impl ArtifactReclaimer for ObjectReclaimer {
    fn storage_alias(&self) -> &str {
        &self.alias
    }
    async fn remove(&self, artifact: &CheckpointArtifact) -> Result<()> {
        if artifact.storage != self.alias
            || (!artifact.scope.is_empty() && artifact.scope != self.scope)
            || uuid::Uuid::parse_str(&artifact.location).is_err()
        {
            return Err(Error::Invalid("artifact outside object reclaimer".into()));
        }
        let prefix = Key::from(format!("{}/{}/", self.prefix, artifact.location));
        let mut listed = self.remote.list(Some(&prefix));
        while let Some(item) = listed.try_next().await.map_err(io_error)? {
            if !item
                .location
                .as_ref()
                .starts_with(&format!("{}/{}/", self.prefix, artifact.location))
            {
                continue;
            }
            match self.remote.delete(&item.location).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => return Err(io_error(e)),
            }
        }
        Ok(())
    }
}
#[async_trait]
impl ArtifactReclaimer for FilesystemCheckpointStore {
    fn storage_alias(&self) -> &str {
        <Self as super::CheckpointAccess>::storage_alias(self)
    }
    async fn remove(&self, artifact: &CheckpointArtifact) -> Result<()> {
        self.reclaim(artifact).await
    }
}
#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ReclaimerConfig {
    S3 {
        alias: String,
        bucket: String,
        region: String,
        endpoint: Option<String>,
        #[serde(default)]
        allow_http: bool,
        prefix: String,
    },
    DistributedFs {
        alias: String,
        mount_root: PathBuf,
        prefix: PathBuf,
    },
}
impl ReclaimerConfig {
    pub fn shared_scope(&self) -> Option<String> {
        let Self::S3 {
            alias,
            bucket,
            region,
            endpoint: Some(endpoint),
            prefix,
            ..
        } = self
        else {
            return None;
        };
        s3_scope(alias, bucket, region, endpoint, prefix)
    }

    pub fn build(self) -> Result<Arc<dyn ArtifactReclaimer>> {
        let scope = self.shared_scope().unwrap_or_default();
        match self {
            Self::DistributedFs {
                alias,
                mount_root,
                prefix,
            } => Ok(Arc::new(FilesystemCheckpointStore::new(
                alias, mount_root, prefix,
            )?)),
            Self::S3 {
                alias,
                bucket,
                region,
                endpoint,
                allow_http,
                prefix,
            } => {
                adx_transport::install_crypto_provider();
                let mut builder = object_store::aws::AmazonS3Builder::from_env()
                    .with_bucket_name(bucket)
                    .with_region(region)
                    .with_allow_http(allow_http);
                if let Some(endpoint) = endpoint {
                    builder = builder.with_endpoint(endpoint);
                }
                Ok(Arc::new(ObjectReclaimer::new_scoped(
                    alias,
                    scope,
                    Arc::new(builder.build().map_err(io_error)?),
                    prefix,
                )?))
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use object_store::ObjectStoreExt;
    #[tokio::test]
    async fn object_deletion_is_exact_idempotent_and_independent_of_source_node() {
        let remote = Arc::new(object_store::memory::InMemory::new());
        let id = uuid::Uuid::new_v4().to_string();
        remote
            .put(
                &Key::from(format!("checkpoints/{id}/files/memory")),
                b"memory".to_vec().into(),
            )
            .await
            .unwrap();
        let sibling = Key::from(format!("checkpoints/{id}-other/files/memory"));
        remote
            .put(&sibling, b"other".to_vec().into())
            .await
            .unwrap();
        let reclaim =
            ObjectReclaimer::new("s3-test".into(), remote.clone(), "checkpoints".into()).unwrap();
        let artifact = CheckpointArtifact {
            storage: "s3-test".into(),
            scope: String::new(),
            location: id.clone(),
            size_bytes: 6,
        };
        reclaim.remove(&artifact).await.unwrap();
        reclaim.remove(&artifact).await.unwrap();
        assert!(remote
            .get(&Key::from(format!("checkpoints/{id}/files/memory")))
            .await
            .is_err());
        assert!(remote.get(&sibling).await.is_ok());
    }
}
