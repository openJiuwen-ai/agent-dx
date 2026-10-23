//! Immutable objects with a manifest commit marker and a best-effort local LRU.
mod cache;
use super::{io_error, CheckpointAccess, LocalCheckpointStore, RestoreAccess};
use adx_core::{CheckpointArtifact, Error, Result};
use async_trait::async_trait;
pub(super) use cache::CacheLease;
use cache::{Cache, CacheRecord};
use futures_util::TryStreamExt;
use object_store::{buffered::BufWriter, path::Path as Key, ObjectStore, ObjectStoreExt};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    os::unix::fs::PermissionsExt,
    path::{Component, Path, PathBuf},
    sync::Arc,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const MANIFEST_LIMIT: usize = 4 * 1024 * 1024;
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    version: u32,
    size: u64,
    files: Vec<FileEntry>,
    directories: Vec<String>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileEntry {
    path: String,
    size: u64,
    sha256: String,
    mode: u32,
}
pub struct ObjectCheckpointStore {
    pub(super) owner: Option<super::orphans::UploadOwner>,
    pub(super) alias: String,
    pub(super) scope: String,
    pub(super) remote: Arc<dyn ObjectStore>,
    pub(super) prefix: Key,
    staging: LocalCheckpointStore,
    cache_root: PathBuf,
    budget: u64,
    max_entries: usize,
    index_root: PathBuf,
    pub(super) cache: Arc<tokio::sync::Mutex<Cache>>,
}
pub(super) fn invalid(message: impl std::fmt::Display) -> Error {
    Error::Invalid(format!("checkpoint object: {message}"))
}
fn relative(path: &str) -> Result<()> {
    if path.is_empty()
        || path.contains('\\')
        || path.len() > 4096
        || !Path::new(path)
            .components()
            .all(|p| matches!(p, Component::Normal(_)))
    {
        return Err(invalid("unsafe relative path"));
    }
    Ok(())
}
impl ObjectCheckpointStore {
    pub fn new(
        alias: String,
        remote: Arc<dyn ObjectStore>,
        prefix: String,
        root: PathBuf,
        budget: u64,
    ) -> Result<Self> {
        Self::new_scoped(alias, String::new(), remote, prefix, root, budget)
    }

    pub fn new_scoped(
        alias: String,
        scope: String,
        remote: Arc<dyn ObjectStore>,
        prefix: String,
        root: PathBuf,
        budget: u64,
    ) -> Result<Self> {
        if alias.is_empty()
            || alias == "local"
            || !alias
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
            || budget == 0
        {
            return Err(invalid("storage alias and positive cache budget required"));
        }
        relative(&prefix)?;
        let staging = LocalCheckpointStore::new(root.join("staging"))?;
        let cache_root = root.join("cache");
        std::fs::create_dir_all(&cache_root).map_err(io_error)?;
        let cache_root = std::fs::canonicalize(cache_root).map_err(io_error)?;
        let index_root = root.join("cache-index");
        std::fs::create_dir_all(&index_root).map_err(io_error)?;
        let index_root = std::fs::canonicalize(index_root).map_err(io_error)?;
        let cache = Cache::recover(&index_root, &cache_root, &staging.root)?;
        Ok(Self {
            owner: None,
            alias,
            scope,
            remote,
            prefix: Key::from(prefix),
            staging,
            cache_root,
            budget,
            max_entries: super::DEFAULT_CACHE_MAX_ENTRIES,
            index_root,
            cache: Arc::new(tokio::sync::Mutex::new(cache)),
        })
    }
    pub(super) fn key(&self, id: &str, name: &str) -> Key {
        Key::from(format!("{}/{id}/{name}", self.prefix))
    }
    pub(super) fn validate(&self, a: &CheckpointArtifact) -> Result<()> {
        if a.storage != self.alias
            || (!a.scope.is_empty() && a.scope != self.scope)
            || uuid::Uuid::parse_str(&a.location).is_err()
            || a.size_bytes == 0
        {
            return Err(invalid("artifact does not belong to this store"));
        }
        Ok(())
    }
    async fn manifest(&self, a: &CheckpointArtifact) -> Result<Manifest> {
        self.validate(a)?;
        let result = self
            .remote
            .get(&self.key(&a.location, "manifest.json"))
            .await
            .map_err(io_error)?;
        if result.meta.size > MANIFEST_LIMIT as u64 {
            return Err(invalid("manifest too large"));
        }
        let mut stream = result.into_stream();
        let mut data = Vec::new();
        while let Some(chunk) = stream.try_next().await.map_err(io_error)? {
            if data.len().saturating_add(chunk.len()) > MANIFEST_LIMIT {
                return Err(invalid("manifest too large"));
            }
            data.extend_from_slice(&chunk);
        }
        let m: Manifest = serde_json::from_slice(&data).map_err(invalid)?;
        Self::validate_manifest(&m, a)?;
        Ok(m)
    }
    fn validate_manifest(m: &Manifest, a: &CheckpointArtifact) -> Result<()> {
        if m.version != 1
            || m.size != a.size_bytes
            || m.files.len().saturating_add(m.directories.len()) > 65536
        {
            return Err(invalid("manifest identity mismatch"));
        }
        let mut names = std::collections::BTreeSet::new();
        let mut size = 0u64;
        for f in &m.files {
            relative(&f.path)?;
            if !names.insert(&f.path)
                || f.mode > 0o777
                || f.sha256.len() != 64
                || !f.sha256.bytes().all(|b| b.is_ascii_hexdigit())
            {
                return Err(invalid("invalid file manifest"));
            }
            size = size
                .checked_add(f.size)
                .ok_or_else(|| invalid("size overflow"))?;
        }
        for d in &m.directories {
            relative(d)?;
            if !names.insert(d) {
                return Err(invalid("duplicate manifest path"));
            }
        }
        if size != m.size {
            return Err(invalid("manifest size mismatch"));
        }
        Ok(())
    }
    /// Set the reusable-cache entry cap. Active execution leases are excluded.
    pub fn with_max_entries(mut self, max_entries: usize) -> Result<Self> {
        if max_entries == 0 {
            return Err(invalid("positive cache entry limit required"));
        }
        self.max_entries = max_entries;
        Ok(self)
    }
    pub async fn evict_unreferenced(&self, target: u64) -> Result<()> {
        Cache::trim(
            &mut *self.cache.lock().await,
            target.min(self.budget),
            self.max_entries,
        )
        .await
    }
    async fn valid_cached(path: &Path, m: &Manifest) -> Result<bool> {
        if !tokio::fs::symlink_metadata(path)
            .await
            .map(|metadata| metadata.is_dir())
            .unwrap_or(false)
        {
            return Ok(false);
        }
        for directory in &m.directories {
            if !tokio::fs::symlink_metadata(path.join(directory))
                .await
                .map(|metadata| metadata.is_dir())
                .unwrap_or(false)
            {
                return Ok(false);
            }
        }
        for f in &m.files {
            let p = path.join(&f.path);
            let metadata = match tokio::fs::symlink_metadata(&p).await {
                Ok(m) => m,
                Err(_) => return Ok(false),
            };
            if !metadata.is_file() || metadata.len() != f.size {
                return Ok(false);
            }
            // Every ancestor is checked: no symlink traversal from a reused cache.
            let mut parent = p.parent();
            while let Some(p) = parent {
                if p == path {
                    break;
                }
                if !tokio::fs::symlink_metadata(p)
                    .await
                    .map_err(io_error)?
                    .is_dir()
                {
                    return Ok(false);
                }
                parent = p.parent();
            }
            let mut file = tokio::fs::File::open(&p).await.map_err(io_error)?;
            let mut digest = Sha256::new();
            let mut buf = vec![0; 256 * 1024];
            loop {
                let n = file.read(&mut buf).await.map_err(io_error)?;
                if n == 0 {
                    break;
                }
                digest.update(&buf[..n]);
            }
            if format!("{:x}", digest.finalize()) != f.sha256 {
                return Ok(false);
            }
        }
        Ok(true)
    }
    async fn download(&self, a: &CheckpointArtifact, m: &Manifest, target: &Path) -> Result<()> {
        tokio::fs::create_dir(target).await.map_err(io_error)?;
        tokio::fs::set_permissions(target, std::fs::Permissions::from_mode(0o700))
            .await
            .map_err(io_error)?;
        for d in &m.directories {
            tokio::fs::create_dir_all(target.join(d))
                .await
                .map_err(io_error)?;
        }
        for f in &m.files {
            let path = target.join(&f.path);
            let parent = path
                .parent()
                .ok_or_else(|| invalid("checkpoint file has no parent"))?;
            tokio::fs::create_dir_all(parent).await.map_err(io_error)?;
            let mut output = tokio::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)
                .await
                .map_err(io_error)?;
            let mut stream = self
                .remote
                .get(&self.key(&a.location, &format!("files/{}", f.path)))
                .await
                .map_err(io_error)?
                .into_stream();
            let mut digest = Sha256::new();
            let mut size = 0u64;
            while let Some(chunk) = stream.try_next().await.map_err(io_error)? {
                size = size
                    .checked_add(chunk.len() as u64)
                    .ok_or_else(|| invalid("size overflow"))?;
                if size > f.size {
                    return Err(invalid("object exceeds manifest size"));
                }
                digest.update(&chunk);
                output.write_all(&chunk).await.map_err(io_error)?;
            }
            if size != f.size || format!("{:x}", digest.finalize()) != f.sha256 {
                return Err(invalid("object checksum mismatch"));
            }
            output
                .set_permissions(std::fs::Permissions::from_mode(f.mode))
                .await
                .map_err(io_error)?;
            output.sync_all().await.map_err(io_error)?;
        }
        Ok(())
    }
}
type FileInventory = (Vec<(String, u32)>, Vec<String>);
fn inventory(root: &Path) -> Result<FileInventory> {
    let mut todo = vec![root.to_path_buf()];
    let mut files = vec![];
    let mut dirs = vec![];
    while let Some(path) = todo.pop() {
        let meta = std::fs::symlink_metadata(&path).map_err(io_error)?;
        if meta.is_dir() {
            if path != root {
                dirs.push(
                    path.strip_prefix(root)
                        .map_err(|_| invalid("checkpoint entry escaped its root"))?
                        .to_str()
                        .ok_or_else(|| invalid("non UTF-8 path"))?
                        .to_owned(),
                );
            }
            for entry in std::fs::read_dir(path).map_err(io_error)? {
                todo.push(entry.map_err(io_error)?.path());
            }
        } else if meta.is_file() {
            let path = path
                .strip_prefix(root)
                .map_err(|_| invalid("checkpoint entry escaped its root"))?
                .to_str()
                .ok_or_else(|| invalid("non UTF-8 path"))?
                .to_owned();
            relative(&path)?;
            files.push((path, meta.permissions().mode() & 0o777));
        } else {
            return Err(invalid("symlink or special file in checkpoint"));
        }
        if files.len() + dirs.len() + todo.len() > 65536 {
            return Err(invalid("too many checkpoint entries"));
        }
    }
    Ok((files, dirs))
}
#[async_trait]
impl CheckpointAccess for ObjectCheckpointStore {
    fn storage_alias(&self) -> &str {
        &self.alias
    }
    fn shared_scope(&self) -> Option<&str> {
        (!self.scope.is_empty()).then_some(self.scope.as_str())
    }
    fn capture_references(&self, staged: &Path) -> Result<Vec<CheckpointArtifact>> {
        let path = self.staging.owned_path(staged)?;
        let id = path
            .file_name()
            .and_then(|s| s.to_str())
            .ok_or_else(|| invalid("invalid capture path"))?;
        uuid::Uuid::parse_str(id).map_err(invalid)?;
        let mut refs = self.staging.capture_references(staged)?;
        refs.push(CheckpointArtifact {
            storage: self.alias.clone(),
            scope: self.scope.clone(),
            location: id.into(),
            size_bytes: 0,
        });
        Ok(refs)
    }
    async fn begin_capture(&self) -> Result<PathBuf> {
        self.staging.begin_capture().await
    }
    async fn discard_staged(&self, staged: &Path) -> Result<()> {
        self.staging.discard_staged(staged).await?;
        if let Some(id) = staged.file_name().and_then(|name| name.to_str()) {
            self.remove_index(id).await?;
        }
        Ok(())
    }
    async fn committed(&self, artifact: &CheckpointArtifact) -> Result<()> {
        if artifact.storage == "local" {
            return Ok(());
        }
        self.validate(artifact)?;
        // Metadata is already authoritative. Local cache maintenance must never
        // turn a successful Pause into a failed operation.
        if let Err(error) = self.cache_committed(artifact).await {
            adx_observability::warn!(%error, artifact=%artifact.location, "checkpoint cache admission failed");
        }
        Ok(())
    }
    async fn retain_staged(&self, staged: &Path) -> Result<CheckpointArtifact> {
        self.staging.seal(staged).await
    }
    async fn validate_artifact(&self, artifact: &CheckpointArtifact) -> Result<()> {
        if artifact.storage == "local" {
            return self.staging.validate_artifact(artifact).await;
        }
        self.manifest(artifact).await.map(|_| ())
    }
    async fn seal(&self, staged: &Path) -> Result<CheckpointArtifact> {
        let local = self.staging.seal(staged).await?;
        let root = PathBuf::from(&local.location);
        let scan_root = root.clone();
        let (files, directories) = tokio::task::spawn_blocking(move || inventory(&scan_root))
            .await
            .map_err(io_error)??;
        let id = root
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| invalid("checkpoint artifact has no UTF-8 identifier"))?
            .to_owned();
        self.mark_upload(&id).await?;
        let result = async {
            let mut manifest = Manifest {
                version: 1,
                size: local.size_bytes,
                files: vec![],
                directories,
            };
            for (path, mode) in files {
                let mut input = tokio::fs::File::open(root.join(&path))
                    .await
                    .map_err(io_error)?;
                let mut output = BufWriter::with_capacity(
                    self.remote.clone(),
                    self.key(&id, &format!("files/{path}")),
                    8 * 1024 * 1024,
                )
                .with_max_concurrency(2);
                let mut digest = Sha256::new();
                let mut size = 0u64;
                let mut buf = vec![0; 256 * 1024];
                let copy = async {
                    loop {
                        let n = input.read(&mut buf).await.map_err(io_error)?;
                        if n == 0 {
                            break;
                        }
                        digest.update(&buf[..n]);
                        size = size
                            .checked_add(n as u64)
                            .ok_or_else(|| invalid("size overflow"))?;
                        output.write_all(&buf[..n]).await.map_err(io_error)?;
                    }
                    Ok::<_, Error>(())
                }
                .await;
                if let Err(error) = copy {
                    let _ = output.abort().await;
                    return Err(error);
                }
                output.shutdown().await.map_err(io_error)?;
                manifest.files.push(FileEntry {
                    path,
                    size,
                    sha256: format!("{:x}", digest.finalize()),
                    mode,
                });
            }
            if manifest.files.iter().map(|f| f.size).sum::<u64>() != manifest.size {
                return Err(invalid("staging changed during upload"));
            }
            let data = serde_json::to_vec(&manifest).map_err(invalid)?;
            if data.len() > MANIFEST_LIMIT {
                return Err(invalid("manifest too large"));
            }
            self.remote
                .put(&self.key(&id, "manifest.json"), data.into())
                .await
                .map_err(io_error)?;
            let record = CacheRecord {
                manifest: manifest.clone(),
                staged: true,
                committed: false,
                used: 0,
            };
            if let Err(error) = self.write_index(&id, &record).await {
                adx_observability::warn!(%error, "checkpoint upload cache index unavailable");
            }
            Ok(CheckpointArtifact {
                storage: self.alias.clone(),
                scope: self.scope.clone(),
                location: id.clone(),
                size_bytes: manifest.size,
            })
        }
        .await;
        result
    }
    async fn prepare_restore(&self, artifact: &CheckpointArtifact) -> Result<RestoreAccess> {
        if artifact.storage == "local" {
            return self.staging.prepare_restore(artifact).await;
        }
        self.validate(artifact)?;
        let _operation = self.operation(&artifact.location).await;
        if let Some(files) = self.cached(artifact).await? {
            return Ok(files);
        }
        let manifest = self.manifest(artifact).await?;
        let path = self.cache_root.join(&artifact.location);
        // Failed/canceled downloads are never visible as completed cache entries.
        let temp = self
            .cache_root
            .join(format!(".download-{}", uuid::Uuid::new_v4()));
        if let Err(error) = self.download(artifact, &manifest, &temp).await {
            let _ = tokio::fs::remove_dir_all(&temp).await;
            return Err(error);
        }
        tokio::fs::rename(&temp, &path).await.map_err(io_error)?;
        let record = CacheRecord {
            manifest,
            staged: false,
            committed: true,
            used: 0,
        };
        // Quotas apply when the execution lease is released, not to restoration.
        self.insert(artifact, path, record).await
    }
    async fn release(&self, artifact: &CheckpointArtifact) -> Result<()> {
        if artifact.storage == "local" {
            return self.staging.release(artifact).await;
        }
        self.validate(artifact)?;
        let _operation = self.operation(&artifact.location).await;
        let mut cache = self.cache.lock().await;
        if cache
            .entries
            .get(&artifact.location)
            .is_some_and(|e| Arc::strong_count(&e.pin) > 1)
        {
            return Err(Error::Conflict);
        }
        // Shared objects are reclaimed by Coordinator; this only releases node-private copies.
        let path = self.cache_root.join(&artifact.location);
        if path.exists() {
            tokio::fs::remove_dir_all(path).await.map_err(io_error)?;
        }
        cache.entries.remove(&artifact.location);
        self.remove_index(&artifact.location).await?;
        // A successful publication may retain staging for upload rollback until metadata commit.
        self.staging
            .discard_staged(&self.staging.root.join(&artifact.location))
            .await
    }
    async fn reconcile_local(&self, retained: &[CheckpointArtifact]) -> Result<()> {
        self.reconcile_cache(retained).await
    }
}
