//! Immutable objects with a manifest commit marker and a bounded, pinned cache.
use super::{io_error, CheckpointStore, LocalCheckpointStore, MaterializedCheckpoint};
use adx_core::{CheckpointArtifact, Error, Result};
use async_trait::async_trait;
use futures_util::TryStreamExt;
use object_store::{buffered::BufWriter, path::Path as Key, ObjectStore, ObjectStoreExt};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    os::unix::fs::PermissionsExt,
    path::{Component, Path, PathBuf},
    sync::Arc,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const MANIFEST_LIMIT: usize = 4 * 1024 * 1024;
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    version: u32,
    size: u64,
    files: Vec<FileEntry>,
    directories: Vec<String>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileEntry {
    path: String,
    size: u64,
    sha256: String,
    mode: u32,
}
pub(super) struct CacheEntry {
    pub(super) path: PathBuf,
    size: u64,
    pub(super) pin: Arc<()>,
    used: u64,
}
#[derive(Default)]
pub(super) struct Cache {
    pub(super) entries: BTreeMap<String, CacheEntry>,
    clock: u64,
}

pub struct ObjectCheckpointStore {
    pub(super) owner: Option<super::orphans::UploadOwner>,
    pub(super) gc_config: super::RemoteGcConfig,
    pub(super) gc_authority: tokio::sync::Mutex<Option<std::collections::BTreeSet<String>>>,
    pub(super) alias: String,
    pub(super) remote: Arc<dyn ObjectStore>,
    pub(super) prefix: Key,
    staging: LocalCheckpointStore,
    cache_root: PathBuf,
    budget: u64,
    pub(super) cache: tokio::sync::Mutex<Cache>,
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
        Ok(Self {
            owner: None,
            gc_config: Default::default(),
            gc_authority: Default::default(),
            alias,
            remote,
            prefix: Key::from(prefix),
            staging,
            cache_root,
            budget,
            cache: Default::default(),
        })
    }
    pub(super) fn key(&self, id: &str, name: &str) -> Key {
        Key::from(format!("{}/{id}/{name}", self.prefix))
    }
    pub(super) fn validate(&self, a: &CheckpointArtifact) -> Result<()> {
        if a.storage != self.alias
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
        if m.version != 1 || m.size != a.size_bytes || m.files.len() > 65536 {
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
        Ok(m)
    }
    pub(super) async fn delete_objects(&self, id: &str) -> Result<()> {
        let prefix = self.key(id, "");
        let mut objects = self.remote.list(Some(&prefix));
        let marker = self.key(id, "owner.json");
        while let Some(object) = objects.try_next().await.map_err(io_error)? {
            if object.location == marker {
                continue;
            }
            match self.remote.delete(&object.location).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => (),
                Err(e) => return Err(io_error(e)),
            }
        }
        match self.remote.delete(&marker).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => (),
            Err(error) => return Err(io_error(error)),
        }
        Ok(())
    }
    async fn trim(cache: &mut Cache, target: u64) -> Result<()> {
        let mut size: u64 = cache.entries.values().map(|e| e.size).sum();
        while size > target {
            let candidate = cache
                .entries
                .iter()
                .filter(|(_, e)| Arc::strong_count(&e.pin) == 1)
                .min_by_key(|(_, e)| e.used)
                .map(|(id, _)| id.clone());
            let Some(id) = candidate else {
                return Err(Error::Unavailable(
                    "checkpoint cache budget occupied by active references".into(),
                ));
            };
            let entry = &cache.entries[&id];
            tokio::fs::remove_dir_all(&entry.path)
                .await
                .map_err(io_error)?;
            size -= entry.size;
            cache.entries.remove(&id);
        }
        Ok(())
    }
    pub async fn evict_unreferenced(&self, target: u64) -> Result<()> {
        Self::trim(&mut *self.cache.lock().await, target.min(self.budget)).await
    }
    async fn valid_cached(path: &Path, m: &Manifest) -> Result<bool> {
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
            tokio::fs::create_dir_all(path.parent().unwrap())
                .await
                .map_err(io_error)?;
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
                        .unwrap()
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
                .unwrap()
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
impl CheckpointStore for ObjectCheckpointStore {
    async fn allocate(&self) -> Result<PathBuf> {
        self.staging.allocate().await
    }
    async fn discard_staged(&self, staged: &Path) -> Result<()> {
        self.staging.discard_staged(staged).await
    }
    async fn duplicate(&self, artifact: &CheckpointArtifact) -> Result<CheckpointArtifact> {
        if artifact.storage == "local" {
            return self.staging.duplicate(artifact).await;
        }
        let _cache = self.cache.lock().await;
        let manifest = self.manifest(artifact).await?;
        let id = uuid::Uuid::new_v4().to_string();
        self.mark_upload(&id).await?;
        let result = async {
            for file in &manifest.files {
                let source = self.key(&artifact.location, &format!("files/{}", file.path));
                let target = self.key(&id, &format!("files/{}", file.path));
                if file.size <= 5 * 1024 * 1024 * 1024 {
                    self.remote.copy(&source, &target).await.map_err(io_error)?;
                } else {
                    // S3 single-request copies are bounded; stream larger files.
                    let mut input = self
                        .remote
                        .get(&source)
                        .await
                        .map_err(io_error)?
                        .into_stream();
                    let mut output =
                        BufWriter::with_capacity(self.remote.clone(), target, 8 * 1024 * 1024)
                            .with_max_concurrency(2);
                    let copy = async {
                        while let Some(chunk) = input.try_next().await.map_err(io_error)? {
                            output.write_all(&chunk).await.map_err(io_error)?;
                        }
                        Ok::<_, Error>(())
                    }
                    .await;
                    if let Err(error) = copy {
                        let _ = output.abort().await;
                        return Err(error);
                    }
                    output.shutdown().await.map_err(io_error)?;
                }
            }
            let data = serde_json::to_vec(&manifest).map_err(invalid)?;
            self.remote
                .put(&self.key(&id, "manifest.json"), data.into())
                .await
                .map_err(io_error)?;
            Ok(CheckpointArtifact {
                storage: self.alias.clone(),
                location: id.clone(),
                size_bytes: artifact.size_bytes,
            })
        }
        .await;
        if result.is_err() {
            let _ = self.delete_objects(&id).await;
        }
        result
    }
    async fn committed(&self, artifact: &CheckpointArtifact) -> Result<()> {
        if artifact.storage == "local" {
            return Ok(());
        }
        self.validate(artifact)?;
        self.staging
            .discard_staged(&self.staging.root.join(&artifact.location))
            .await
    }
    async fn retain_staged(&self, staged: &Path) -> Result<CheckpointArtifact> {
        self.staging.publish(staged).await
    }
    async fn validate_artifact(&self, artifact: &CheckpointArtifact) -> Result<()> {
        if artifact.storage == "local" {
            return self.staging.validate_artifact(artifact).await;
        }
        self.manifest(artifact).await.map(|_| ())
    }
    async fn publish(&self, staged: &Path) -> Result<CheckpointArtifact> {
        let local = self.staging.publish(staged).await?;
        let root = PathBuf::from(&local.location);
        let scan_root = root.clone();
        let (files, directories) = tokio::task::spawn_blocking(move || inventory(&scan_root))
            .await
            .map_err(io_error)??;
        let id = root.file_name().unwrap().to_str().unwrap().to_owned();
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
            Ok(CheckpointArtifact {
                storage: self.alias.clone(),
                location: id.clone(),
                size_bytes: manifest.size,
            })
        }
        .await;
        if result.is_err() {
            let _ = self.delete_objects(&id).await;
        }
        result
    }
    async fn materialize(&self, artifact: &CheckpointArtifact) -> Result<MaterializedCheckpoint> {
        if artifact.storage == "local" {
            return self.staging.materialize(artifact).await;
        }
        self.validate(artifact)?;
        let mut cache = self.cache.lock().await;
        cache.clock = cache.clock.wrapping_add(1);
        let used = cache.clock;
        if let Some(e) = cache.entries.get_mut(&artifact.location) {
            if e.size != artifact.size_bytes {
                return Err(invalid("cached artifact identity mismatch"));
            }
            e.used = used;
            return Ok(MaterializedCheckpoint::pinned(
                e.path.clone(),
                e.pin.clone(),
            ));
        }
        if artifact.size_bytes > self.budget {
            return Err(Error::Unavailable("checkpoint exceeds cache budget".into()));
        }
        let manifest = self.manifest(artifact).await?;
        Self::trim(&mut cache, self.budget - artifact.size_bytes).await?;
        let path = self.cache_root.join(&artifact.location);
        if path.exists() {
            if !tokio::fs::symlink_metadata(&path)
                .await
                .map_err(io_error)?
                .is_dir()
                || !Self::valid_cached(&path, &manifest).await?
            {
                return Err(invalid(
                    "existing cache is inconsistent; reconcile after backend cleanup",
                ));
            }
        } else {
            let temp = self
                .cache_root
                .join(format!(".download-{}", uuid::Uuid::new_v4()));
            if let Err(e) = self.download(artifact, &manifest, &temp).await {
                let _ = tokio::fs::remove_dir_all(&temp).await;
                return Err(e);
            }
            tokio::fs::rename(&temp, &path).await.map_err(io_error)?;
        }
        let pin = Arc::new(());
        cache.entries.insert(
            artifact.location.clone(),
            CacheEntry {
                path: path.clone(),
                size: artifact.size_bytes,
                pin: pin.clone(),
                used,
            },
        );
        Ok(MaterializedCheckpoint::pinned(path, pin))
    }
    async fn remove(&self, artifact: &CheckpointArtifact) -> Result<()> {
        if artifact.storage == "local" {
            return self.staging.remove(artifact).await;
        }
        self.validate(artifact)?;
        let mut cache = self.cache.lock().await;
        if cache
            .entries
            .get(&artifact.location)
            .is_some_and(|e| Arc::strong_count(&e.pin) > 1)
        {
            return Err(Error::Conflict);
        }
        self.delete_objects(&artifact.location).await?;
        let path = self.cache_root.join(&artifact.location);
        if path.exists() {
            tokio::fs::remove_dir_all(path).await.map_err(io_error)?;
        }
        cache.entries.remove(&artifact.location);
        // A successful publication may retain staging for upload rollback until metadata commit.
        self.staging
            .discard_staged(&self.staging.root.join(&artifact.location))
            .await
    }
    async fn authorize_remote_gc(&self, retained: &[CheckpointArtifact]) -> Result<()> {
        self.authorize(retained).await
    }
    async fn collect_remote_orphans(&self) -> Result<usize> {
        self.collect().await
    }
    async fn reconcile_local(&self, retained: &[CheckpointArtifact]) -> Result<()> {
        self.staging.reconcile_local(retained).await?;
        let cache = self.cache.lock().await;
        let keep: std::collections::BTreeSet<_> = retained
            .iter()
            .filter(|a| a.storage == self.alias)
            .map(|a| a.location.as_str())
            .collect();
        let mut entries = tokio::fs::read_dir(&self.cache_root)
            .await
            .map_err(io_error)?;
        while let Some(e) = entries.next_entry().await.map_err(io_error)? {
            let name = e.file_name().to_string_lossy().into_owned();
            if keep.contains(name.as_str()) || cache.entries.contains_key(&name) {
                continue;
            }
            if (uuid::Uuid::parse_str(&name).is_ok() || name.starts_with(".download-"))
                && e.file_type().await.map_err(io_error)?.is_dir()
            {
                tokio::fs::remove_dir_all(e.path())
                    .await
                    .map_err(io_error)?;
            }
        }
        Ok(())
    }
}
