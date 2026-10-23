//! Checkpoint data access and authoritative reclamation for node-local, S3, and shared filesystems.
mod filesystem;
mod object;
mod orphans;
pub mod reclaim;
use adx_core::{CheckpointArtifact, Error, Result};
use async_trait::async_trait;
pub use filesystem::FilesystemCheckpointStore;
pub use object::ObjectCheckpointStore;
use sha2::{Digest, Sha256};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
pub const DEFAULT_CACHE_MAX_ENTRIES: usize = 500;
pub const DEFAULT_CACHE_BUDGET_BYTES: u64 = 50 * 1024 * 1024 * 1024;
fn default_cache_max_entries() -> usize {
    DEFAULT_CACHE_MAX_ENTRIES
}
fn default_cache_budget_bytes() -> u64 {
    DEFAULT_CACHE_BUDGET_BYTES
}
pub fn now() -> Result<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|t| t.as_secs())
        .map_err(|_| Error::Unavailable("system clock before epoch".into()))
}
#[async_trait]
pub trait CheckpointAccess: Send + Sync {
    fn storage_alias(&self) -> &str {
        "local"
    }
    /// A stable identity for the configured shared backend. Local and
    /// unpinned remote configurations must not participate in delegation.
    fn shared_scope(&self) -> Option<&str> {
        None
    }
    fn validate_policy(&self, policy: adx_core::lifecycle::CheckpointStorage) -> Result<()> {
        if policy == adx_core::lifecycle::CheckpointStorage::Shared
            && self.storage_alias() == "local"
        {
            return Err(Error::Invalid(
                "shared checkpoint storage is not configured on this node".into(),
            ));
        }
        Ok(())
    }
    async fn publish_with_policy(
        &self,
        staged: &Path,
        policy: adx_core::lifecycle::CheckpointStorage,
    ) -> Result<CheckpointArtifact> {
        self.validate_policy(policy)?;
        if policy == adx_core::lifecycle::CheckpointStorage::Local {
            self.retain_staged(staged).await
        } else {
            self.seal(staged).await
        }
    }
    /// A private capture directory visible to the execution backend; it may be shared-filesystem backed.
    async fn begin_capture(&self) -> Result<PathBuf>;
    /// Durable input reservations recorded before a running capture starts.
    /// Zero size denotes an incomplete capture, which must never be restored.
    fn capture_references(&self, staged: &Path) -> Result<Vec<CheckpointArtifact>> {
        Ok(vec![CheckpointArtifact {
            storage: "local".into(),
            scope: String::new(),
            location: staged.to_string_lossy().into_owned(),
            size_bytes: 0,
        }])
    }
    /// Discard a local staging directory after its execution has stopped.
    async fn discard_staged(&self, staged: &Path) -> Result<()>;
    /// Preserve a complete local recovery point for running capture or upload rollback.
    async fn retain_staged(&self, staged: &Path) -> Result<CheckpointArtifact>;
    /// Validate a complete, readable local artifact or finish its remote upload.
    /// Completes the backend publication contract. Local-only guarantees readable
    /// files without forcing writeback; shared backends complete their persistence barrier.
    /// The caller must still commit authoritative metadata before acknowledging.
    async fn seal(&self, staged: &Path) -> Result<CheckpointArtifact>;
    async fn prepare_restore(&self, artifact: &CheckpointArtifact) -> Result<RestoreAccess>;
    async fn release(&self, artifact: &CheckpointArtifact) -> Result<()>;
    /// The authoritative result references the published artifact; upload staging
    /// may now be reclaimed independently of downloaded execution files.
    async fn committed(&self, _artifact: &CheckpointArtifact) -> Result<()> {
        Ok(())
    }
    async fn validate_artifact(&self, artifact: &CheckpointArtifact) -> Result<()> {
        self.prepare_restore(artifact).await.map(|_| ())
    }
    /// Admission is closed and backend reconciliation has completed. Only remove
    /// node-private unreferenced files, never cluster-shared objects.
    async fn reconcile_local(&self, _retained: &[CheckpointArtifact]) -> Result<()> {
        Ok(())
    }
}
/// Pins downloaded files until the backend no longer reads them.
/// A restore may keep memory mapped files open after its RPC has returned.
pub struct RestoreAccess {
    path: PathBuf,
    _pin: Option<Box<dyn Send + Sync>>,
}
impl RestoreAccess {
    fn local(path: PathBuf) -> Self {
        Self { path, _pin: None }
    }
    fn pinned(path: PathBuf, pin: object::CacheLease) -> Self {
        Self {
            path,
            _pin: Some(Box::new(pin)),
        }
    }
}
impl std::ops::Deref for RestoreAccess {
    type Target = Path;
    fn deref(&self) -> &Path {
        &self.path
    }
}
/// Node-local durable files; access remains bound to this local storage volume.
pub struct LocalCheckpointStore {
    root: PathBuf,
}
impl LocalCheckpointStore {
    pub fn new(root: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&root).map_err(io_error)?;
        Ok(Self {
            root: std::fs::canonicalize(root).map_err(io_error)?,
        })
    }
    fn owned_path(&self, path: &Path) -> Result<PathBuf> {
        let path = std::fs::canonicalize(path).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                Error::NotFound
            } else {
                io_error(error)
            }
        })?;
        if path.parent() != Some(self.root.as_path()) {
            return Err(Error::Invalid("checkpoint outside storage root".into()));
        }
        Ok(path)
    }
}
fn io_error(e: impl std::fmt::Display) -> Error {
    Error::Unavailable(format!("checkpoint storage: {e}"))
}
fn scan(path: &Path) -> Result<u64> {
    let meta = std::fs::symlink_metadata(path).map_err(io_error)?;
    if meta.is_symlink() {
        return Err(Error::Invalid("checkpoint symlink is not allowed".into()));
    }
    let mut size = 0u64;
    if meta.is_dir() {
        for entry in std::fs::read_dir(path).map_err(io_error)? {
            size = size
                .checked_add(scan(&entry.map_err(io_error)?.path())?)
                .ok_or_else(|| Error::Invalid("checkpoint size overflow".into()))?;
        }
    } else if meta.is_file() {
        size = meta.len();
    } else {
        return Err(Error::Invalid("checkpoint contains special file".into()));
    }
    Ok(size)
}
#[async_trait]
impl CheckpointAccess for LocalCheckpointStore {
    async fn reconcile_local(&self, retained: &[CheckpointArtifact]) -> Result<()> {
        let retained: std::collections::BTreeSet<_> = retained
            .iter()
            .filter(|artifact| artifact.storage == "local")
            .map(|artifact| PathBuf::from(&artifact.location))
            .collect();
        let root = self.root.clone();
        tokio::task::spawn_blocking(move || {
            for entry in std::fs::read_dir(root).map_err(io_error)? {
                let entry = entry.map_err(io_error)?;
                if uuid::Uuid::parse_str(&entry.file_name().to_string_lossy()).is_err()
                    || retained.contains(&entry.path())
                {
                    continue;
                }
                let ty = entry.file_type().map_err(io_error)?;
                if ty.is_dir() {
                    std::fs::remove_dir_all(entry.path()).map_err(io_error)?;
                } else if ty.is_symlink() {
                    std::fs::remove_file(entry.path()).map_err(io_error)?;
                }
            }
            Ok(())
        })
        .await
        .map_err(io_error)?
    }
    async fn begin_capture(&self) -> Result<PathBuf> {
        let path = self.root.join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir(&path).map_err(io_error)?;
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700))
            .map_err(io_error)?;
        Ok(path)
    }
    async fn discard_staged(&self, staged: &Path) -> Result<()> {
        self.release(&CheckpointArtifact {
            storage: "local".into(),
            scope: String::new(),
            location: staged.to_string_lossy().into_owned(),
            size_bytes: 0,
        })
        .await
    }
    async fn retain_staged(&self, staged: &Path) -> Result<CheckpointArtifact> {
        self.seal(staged).await
    }
    async fn seal(&self, staged: &Path) -> Result<CheckpointArtifact> {
        let path = self.owned_path(staged)?;
        let copy = path.clone();
        let size = tokio::task::spawn_blocking(move || scan(&copy))
            .await
            .map_err(io_error)??;
        if size == 0 {
            return Err(Error::Invalid("empty checkpoint".into()));
        }
        Ok(CheckpointArtifact {
            storage: "local".into(),
            scope: String::new(),
            location: path.to_string_lossy().into_owned(),
            size_bytes: size,
        })
    }
    async fn prepare_restore(&self, artifact: &CheckpointArtifact) -> Result<RestoreAccess> {
        if artifact.storage != "local" || artifact.size_bytes == 0 {
            return Err(Error::Invalid("unsupported checkpoint storage".into()));
        }
        let path = self.owned_path(Path::new(&artifact.location))?;
        let copy = path.clone();
        let size = tokio::task::spawn_blocking(move || scan(&copy))
            .await
            .map_err(io_error)??;
        if size != artifact.size_bytes {
            return Err(Error::Invalid("checkpoint artifact size changed".into()));
        }
        Ok(RestoreAccess::local(path))
    }
    async fn release(&self, artifact: &CheckpointArtifact) -> Result<()> {
        if artifact.storage != "local" {
            return Err(Error::Invalid("unsupported checkpoint storage".into()));
        }
        let path = Path::new(&artifact.location);
        if path.parent() != Some(self.root.as_path()) {
            return Err(Error::Invalid("checkpoint outside storage root".into()));
        }
        if !path.exists() {
            return Ok(());
        }
        let path = self.owned_path(path)?;
        tokio::task::spawn_blocking(move || std::fs::remove_dir_all(path))
            .await
            .map_err(io_error)?
            .map_err(io_error)
    }
}

/// Credential values are supplied by the deployment environment (AWS_*), never
/// embedded into portable artifact records. Other stores implement CheckpointAccess.
#[derive(serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum StorageConfig {
    DistributedFs {
        alias: String,
        mount_root: PathBuf,
        prefix: PathBuf,
    },
    Local {
        root: PathBuf,
    },
    S3 {
        alias: String,
        bucket: String,
        region: String,
        #[serde(default)]
        endpoint: Option<String>,
        #[serde(default)]
        allow_http: bool,
        prefix: String,
        root: PathBuf,
        #[serde(default = "default_cache_budget_bytes")]
        cache_budget_bytes: u64,
        #[serde(default = "default_cache_max_entries")]
        cache_max_entries: usize,
    },
}
impl StorageConfig {
    /// Returns a stable, non-secret identity for the exact remote target used by
    /// this process. An SDK-inherited endpoint is intentionally insufficient:
    /// delegation requires a configured target bound into artifact metadata.
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

    pub fn build(self) -> Result<Arc<dyn CheckpointAccess>> {
        self.build_owned(None)
    }
    pub fn build_for_node(
        self,
        node_id: String,
        session_id: String,
    ) -> Result<Arc<dyn CheckpointAccess>> {
        self.build_owned(Some((node_id, session_id)))
    }
    fn build_owned(self, owner: Option<(String, String)>) -> Result<Arc<dyn CheckpointAccess>> {
        let scope = self.shared_scope().unwrap_or_default();
        match self {
            Self::DistributedFs {
                alias,
                mount_root,
                prefix,
            } => Ok(Arc::new(FilesystemCheckpointStore::new(
                alias, mount_root, prefix,
            )?)),
            Self::Local { root } => Ok(Arc::new(LocalCheckpointStore::new(root)?)),
            Self::S3 {
                alias,
                bucket,
                region,
                endpoint,
                allow_http,
                prefix,
                root,
                cache_budget_bytes,
                cache_max_entries,
            } => {
                adx_transport::install_crypto_provider();
                let mut builder = object_store::aws::AmazonS3Builder::from_env()
                    .with_bucket_name(bucket)
                    .with_region(region)
                    .with_allow_http(allow_http);
                if let Some(endpoint) = endpoint {
                    builder = builder.with_endpoint(endpoint);
                }
                let remote = builder.build().map_err(io_error)?;
                let store = ObjectCheckpointStore::new_scoped(
                    alias,
                    scope,
                    Arc::new(remote),
                    prefix,
                    root,
                    cache_budget_bytes,
                )?
                .with_max_entries(cache_max_entries)?;
                let store = if let Some((node, session)) = owner {
                    store.with_owner(node, session)?
                } else {
                    store
                };
                Ok(Arc::new(store))
            }
        }
    }
}

fn s3_scope(
    alias: &str,
    bucket: &str,
    region: &str,
    endpoint: &str,
    prefix: &str,
) -> Option<String> {
    if alias.is_empty()
        || bucket.is_empty()
        || region.is_empty()
        || endpoint.trim().is_empty()
        || prefix.is_empty()
    {
        return None;
    }
    let mut hash = Sha256::new();
    for value in ["s3-v1", alias, endpoint, bucket, region, prefix] {
        hash.update(value.as_bytes());
        hash.update([0]);
    }
    Some(format!("s3:{:x}", hash.finalize()))
}
