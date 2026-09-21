//! Node-owned checkpoint storage and HTTP runtime cooperation.
mod object;
mod orphans;
use adx_core::{CapsuleRecord, CheckpointArtifact, Error, Result};
use async_trait::async_trait;
pub use object::ObjectCheckpointStore;
pub use orphans::RemoteGcConfig;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PauseRequest {
    pub operation_id: String,
    pub expected_revision: u64,
    pub ttl_seconds: u64,
    pub timeout_seconds: u64,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResumeRequest {
    pub operation_id: String,
    pub expected_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotRequest {
    pub operation_id: String,
    pub expected_revision: u64,
    pub names: Vec<String>,
    pub timeout_seconds: u64,
}
#[derive(Debug, Clone)]
pub struct SnapshotResult {
    pub snapshot: adx_core::snapshots::Snapshot,
    pub capsule: crate::OperationResult,
}
#[async_trait]
pub trait SnapshotCatalog: Send + Sync {
    async fn get(&self, id: &str) -> Result<adx_core::snapshots::Snapshot>;
    async fn publish(
        &self,
        snapshot: adx_core::snapshots::Snapshot,
    ) -> Result<adx_core::snapshots::Snapshot>;
}

pub fn now() -> Result<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|t| t.as_secs())
        .map_err(|_| Error::Unavailable("system clock before epoch".into()))
}
pub(crate) fn validate_operation(id: &str, revision: u64) -> Result<()> {
    if id.trim().is_empty() || id.len() > 128 || revision == 0 {
        return Err(Error::Invalid(
            "operation ID and positive expected revision required".into(),
        ));
    }
    Ok(())
}
#[async_trait]
pub trait CheckpointCooperation: Send + Sync {
    async fn prepare(&self, record: &CapsuleRecord, operation_id: &str) -> Result<()>;
    /// Caller guarantees backend checkpoint has not been invoked.
    async fn abort_unstarted(&self, record: &CapsuleRecord, operation_id: &str) -> Result<()>;
}
#[async_trait]
impl CheckpointCooperation for crate::runtime_control::RuntimeControlClient {
    async fn prepare(&self, record: &CapsuleRecord, id: &str) -> Result<()> {
        let status = self.status(record).await?;
        self.prepare(record, id, status.revision).await.map(|_| ())
    }
    async fn abort_unstarted(&self, record: &CapsuleRecord, id: &str) -> Result<()> {
        let status = self.status(record).await?;
        self.abort_unstarted(record, id, status.revision)
            .await
            .map(|_| ())
    }
}
#[async_trait]
pub trait CheckpointStore: Send + Sync {
    /// Private local staging, visible to the execution backend on this node.
    async fn allocate(&self) -> Result<PathBuf>;
    /// Discard a local staging directory after its execution has stopped.
    async fn discard_staged(&self, staged: &Path) -> Result<()>;
    /// Preserve a complete local recovery point for upload failure rollback.
    async fn retain_staged(&self, staged: &Path) -> Result<CheckpointArtifact>;
    /// Complete storage durability (including upload for a remote backend).
    async fn publish(&self, staged: &Path) -> Result<CheckpointArtifact>;
    async fn materialize(&self, artifact: &CheckpointArtifact) -> Result<MaterializedCheckpoint>;
    async fn remove(&self, artifact: &CheckpointArtifact) -> Result<()>;
    /// Create a separately owned artifact for a reusable snapshot. Deleting the
    /// source recovery point must not delete this copy.
    async fn duplicate(&self, artifact: &CheckpointArtifact) -> Result<CheckpointArtifact> {
        let source = self.materialize(artifact).await?;
        let staged = self.allocate().await?;
        let from = source.to_path_buf();
        let to = staged.clone();
        let copied = tokio::task::spawn_blocking(move || copy_tree(&from, &to))
            .await
            .map_err(io_error)?;
        if let Err(error) = copied {
            self.discard_staged(&staged).await?;
            return Err(error);
        }
        let published = self.publish(&staged).await;
        if published.is_err() {
            self.discard_staged(&staged).await?;
        }
        published
    }
    /// The authoritative result references the published artifact; upload staging
    /// may now be reclaimed independently of downloaded execution files.
    async fn committed(&self, _artifact: &CheckpointArtifact) -> Result<()> {
        Ok(())
    }
    async fn validate_artifact(&self, artifact: &CheckpointArtifact) -> Result<()> {
        self.materialize(artifact).await.map(|_| ())
    }
    /// Only called after complete Master catalog recovery and journal replay.
    async fn authorize_remote_gc(&self, _retained: &[CheckpointArtifact]) -> Result<()> {
        Ok(())
    }
    async fn collect_remote_orphans(&self) -> Result<usize> {
        Ok(0)
    }
    /// Admission is closed and backend reconciliation has completed. Only remove
    /// node-private unreferenced files, never cluster-shared objects.
    async fn reconcile_local(&self, _retained: &[CheckpointArtifact]) -> Result<()> {
        Ok(())
    }
}
/// Pins downloaded files until the backend no longer reads them.
/// A restore may keep memory mapped files open after its RPC has returned.
pub struct MaterializedCheckpoint {
    path: PathBuf,
    _pin: Option<Arc<()>>,
}
impl MaterializedCheckpoint {
    fn local(path: PathBuf) -> Self {
        Self { path, _pin: None }
    }
    fn pinned(path: PathBuf, pin: Arc<()>) -> Self {
        Self {
            path,
            _pin: Some(pin),
        }
    }
}
impl std::ops::Deref for MaterializedCheckpoint {
    type Target = Path;
    fn deref(&self) -> &Path {
        &self.path
    }
}
pub(crate) struct CheckpointServices {
    pub store: Arc<dyn CheckpointStore>,
    pub cooperation: Arc<dyn CheckpointCooperation>,
}
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
fn scan(path: &Path, sync: bool) -> Result<u64> {
    let meta = std::fs::symlink_metadata(path).map_err(io_error)?;
    if meta.is_symlink() {
        return Err(Error::Invalid("checkpoint symlink is not allowed".into()));
    }
    let mut size = 0u64;
    if meta.is_dir() {
        for entry in std::fs::read_dir(path).map_err(io_error)? {
            size = size
                .checked_add(scan(&entry.map_err(io_error)?.path(), sync)?)
                .ok_or_else(|| Error::Invalid("checkpoint size overflow".into()))?;
        }
    } else if meta.is_file() {
        size = meta.len();
    } else {
        return Err(Error::Invalid("checkpoint contains special file".into()));
    }
    if sync {
        std::fs::File::open(path)
            .and_then(|f| f.sync_all())
            .map_err(io_error)?;
    }
    Ok(size)
}
#[async_trait]
impl CheckpointStore for LocalCheckpointStore {
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
    async fn allocate(&self) -> Result<PathBuf> {
        let path = self.root.join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir(&path).map_err(io_error)?;
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700))
            .map_err(io_error)?;
        Ok(path)
    }
    async fn discard_staged(&self, staged: &Path) -> Result<()> {
        self.remove(&CheckpointArtifact {
            storage: "local".into(),
            location: staged.to_string_lossy().into_owned(),
            size_bytes: 0,
        })
        .await
    }
    async fn retain_staged(&self, staged: &Path) -> Result<CheckpointArtifact> {
        self.publish(staged).await
    }
    async fn publish(&self, staged: &Path) -> Result<CheckpointArtifact> {
        let path = self.owned_path(staged)?;
        let copy = path.clone();
        let size = tokio::task::spawn_blocking(move || scan(&copy, true))
            .await
            .map_err(io_error)??;
        if size == 0 {
            return Err(Error::Invalid("empty checkpoint".into()));
        }
        std::fs::File::open(&self.root)
            .and_then(|f| f.sync_all())
            .map_err(io_error)?;
        Ok(CheckpointArtifact {
            storage: "local".into(),
            location: path.to_string_lossy().into_owned(),
            size_bytes: size,
        })
    }
    async fn materialize(&self, artifact: &CheckpointArtifact) -> Result<MaterializedCheckpoint> {
        if artifact.storage != "local" || artifact.size_bytes == 0 {
            return Err(Error::Invalid("unsupported checkpoint storage".into()));
        }
        let path = self.owned_path(Path::new(&artifact.location))?;
        let copy = path.clone();
        let size = tokio::task::spawn_blocking(move || scan(&copy, false))
            .await
            .map_err(io_error)??;
        if size != artifact.size_bytes {
            return Err(Error::Invalid("checkpoint artifact size changed".into()));
        }
        Ok(MaterializedCheckpoint::local(path))
    }
    async fn remove(&self, artifact: &CheckpointArtifact) -> Result<()> {
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
/// embedded into portable artifact records. Other stores implement CheckpointStore.
#[derive(serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum StorageConfig {
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
        cache_budget_bytes: u64,
    },
}
impl StorageConfig {
    pub fn build(self) -> Result<Arc<dyn CheckpointStore>> {
        self.build_owned(None)
    }
    pub fn build_for_node(
        self,
        node_id: String,
        session_id: String,
        gc: RemoteGcConfig,
    ) -> Result<Arc<dyn CheckpointStore>> {
        gc.validate()?;
        self.build_owned(Some((node_id, session_id, gc)))
    }
    fn build_owned(
        self,
        owner: Option<(String, String, RemoteGcConfig)>,
    ) -> Result<Arc<dyn CheckpointStore>> {
        match self {
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
            } => {
                let mut builder = object_store::aws::AmazonS3Builder::from_env()
                    .with_bucket_name(bucket)
                    .with_region(region)
                    .with_allow_http(allow_http);
                if let Some(endpoint) = endpoint {
                    builder = builder.with_endpoint(endpoint);
                }
                let remote = builder.build().map_err(io_error)?;
                let store = ObjectCheckpointStore::new(
                    alias,
                    Arc::new(remote),
                    prefix,
                    root,
                    cache_budget_bytes,
                )?;
                let store = if let Some((node, session, gc)) = owner {
                    store.with_owner(node, session, gc)?
                } else {
                    store
                };
                Ok(Arc::new(store))
            }
        }
    }
}

fn copy_tree(source: &Path, destination: &Path) -> Result<()> {
    for entry in std::fs::read_dir(source).map_err(io_error)? {
        let entry = entry.map_err(io_error)?;
        let target = destination.join(entry.file_name());
        let kind = entry.file_type().map_err(io_error)?;
        if kind.is_dir() {
            std::fs::create_dir(&target).map_err(io_error)?;
            copy_tree(&entry.path(), &target)?;
        } else if kind.is_file() {
            std::fs::copy(entry.path(), target).map_err(io_error)?;
        } else {
            return Err(Error::Invalid(
                "checkpoint copy contains special file".into(),
            ));
        }
    }
    Ok(())
}
