//! Local cache accounting. Execution leases are never subject to cache quotas.
use super::{invalid, Manifest, ObjectCheckpointStore};
use crate::{io_error, CheckpointAccess, RestoreAccess};
use adx_core::{CheckpointArtifact, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    sync::{Arc, Weak},
};
use tokio::sync::{Mutex, OwnedMutexGuard};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CacheRecord {
    pub(super) manifest: Manifest,
    pub(super) staged: bool,
    pub(super) committed: bool,
    pub(super) used: u64,
}
pub(crate) struct CacheEntry {
    pub(crate) path: PathBuf,
    pub(crate) pin: Arc<()>,
    pub(super) record: CacheRecord,
    disk_bytes: u64,
    index: PathBuf,
    verified: bool,
}
#[derive(Default)]
pub(crate) struct Cache {
    pub(crate) entries: BTreeMap<String, CacheEntry>,
    pub(crate) operations: BTreeMap<String, Weak<Mutex<()>>>,
    clock: u64,
    recovering: bool,
}
impl Cache {
    pub(super) fn recover(index: &Path, downloads: &Path, staging: &Path) -> Result<Self> {
        let mut cache = Self::default();
        for entry in std::fs::read_dir(index).map_err(io_error)? {
            let path = entry.map_err(io_error)?.path();
            let Some(id) = path.file_stem().and_then(|name| name.to_str()) else {
                continue;
            };
            if uuid::Uuid::parse_str(id).is_err()
                || path.extension().and_then(|s| s.to_str()) != Some("json")
            {
                continue;
            }
            let record = match read_record(&path) {
                Ok(record) if record.committed => record,
                _ => continue,
            };
            let artifact = CheckpointArtifact {
                storage: String::new(),
                scope: String::new(),
                location: id.to_owned(),
                size_bytes: record.manifest.size,
            };
            if ObjectCheckpointStore::validate_manifest(&record.manifest, &artifact).is_err() {
                continue;
            }
            let directory = if record.staged { staging } else { downloads }.join(id);
            let disk_bytes = match allocated(&directory) {
                Ok(bytes) => bytes.saturating_add(allocated(&path)?),
                Err(_) => continue,
            };
            cache.clock = cache.clock.max(record.used);
            cache.entries.insert(
                id.to_owned(),
                CacheEntry {
                    path: directory,
                    pin: Arc::new(()),
                    record,
                    disk_bytes,
                    index: path,
                    verified: false,
                },
            );
        }
        // Rebuild all live references before evicting recovered execution files.
        cache.recovering = !cache.entries.is_empty();
        for directory in [downloads, staging] {
            for entry in std::fs::read_dir(directory).map_err(io_error)? {
                let entry = entry.map_err(io_error)?;
                if uuid::Uuid::parse_str(&entry.file_name().to_string_lossy()).is_ok() {
                    cache.recovering = true;
                }
            }
        }
        Ok(cache)
    }
    pub(super) fn tick(&mut self) -> u64 {
        self.clock = self.clock.saturating_add(1);
        self.clock
    }
    pub(super) async fn trim(cache: &mut Self, bytes: u64, entries: usize) -> Result<()> {
        if cache.recovering {
            return Ok(());
        }
        loop {
            let reusable: Vec<_> = cache
                .entries
                .iter()
                .filter(|(_, e)| e.record.committed && Arc::strong_count(&e.pin) == 1)
                .collect();
            let size = reusable
                .iter()
                .fold(0u64, |sum, (_, e)| sum.saturating_add(e.disk_bytes));
            if reusable.len() <= entries && size <= bytes {
                return Ok(());
            }
            let candidate = reusable
                .into_iter()
                .min_by_key(|(_, e)| e.record.used)
                .map(|(id, _)| id.clone());
            let Some(id) = candidate else { return Ok(()) };
            let entry = &cache.entries[&id];
            remove_directory(&entry.path).await?;
            remove_file(&entry.index).await?;
            cache.entries.remove(&id);
        }
    }
}

/// Drop releases the execution reference and schedules reusable-cache trim.
/// Shutdown may interrupt maintenance; startup reconciliation repeats it.
pub(crate) struct CacheLease {
    pin: Option<Arc<()>>,
    cache: Weak<Mutex<Cache>>,
    budget: u64,
    max_entries: usize,
}
impl Drop for CacheLease {
    fn drop(&mut self) {
        self.pin.take();
        let cache = self.cache.clone();
        let budget = self.budget;
        let max_entries = self.max_entries;
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                if let Some(cache) = cache.upgrade() {
                    if let Err(error) = Cache::trim(&mut *cache.lock().await, budget, max_entries).await {
                        adx_observability::warn!(%error, "checkpoint cache release maintenance failed");
                    }
                }
            });
        }
    }
}
fn read_record(path: &Path) -> Result<CacheRecord> {
    let metadata = std::fs::symlink_metadata(path).map_err(io_error)?;
    if !metadata.is_file() || metadata.len() > super::MANIFEST_LIMIT as u64 + 4096 {
        return Err(invalid("invalid local cache index"));
    }
    serde_json::from_slice(&std::fs::read(path).map_err(io_error)?).map_err(invalid)
}
/// Count allocated blocks, including directories, rather than logical file size.
fn allocated(path: &Path) -> Result<u64> {
    let metadata = std::fs::symlink_metadata(path).map_err(io_error)?;
    if metadata.file_type().is_symlink() || !(metadata.is_file() || metadata.is_dir()) {
        return Err(invalid("unsafe cache entry"));
    }
    let mut bytes = metadata
        .blocks()
        .checked_mul(512)
        .ok_or_else(|| invalid("cache size overflow"))?;
    if metadata.is_dir() {
        for child in std::fs::read_dir(path).map_err(io_error)? {
            bytes = bytes
                .checked_add(allocated(&child.map_err(io_error)?.path())?)
                .ok_or_else(|| invalid("cache size overflow"))?;
        }
    }
    Ok(bytes)
}
async fn remove_file(path: &Path) -> Result<()> {
    match tokio::fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(io_error(error)),
    }
}
async fn remove_directory(path: &Path) -> Result<()> {
    match tokio::fs::remove_dir_all(path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(io_error(error)),
    }
}
impl ObjectCheckpointStore {
    pub(super) async fn operation(&self, id: &str) -> OwnedMutexGuard<()> {
        let lock = {
            let mut cache = self.cache.lock().await;
            cache.operations.retain(|_, lock| lock.strong_count() > 0);
            let lock = cache
                .operations
                .get(id)
                .and_then(Weak::upgrade)
                .unwrap_or_else(|| Arc::new(Mutex::new(())));
            cache
                .operations
                .insert(id.to_owned(), Arc::downgrade(&lock));
            lock
        };
        lock.lock_owned().await
    }
    pub(crate) async fn remove_index(&self, id: &str) -> Result<()> {
        remove_file(&self.index_root.join(format!("{id}.json"))).await
    }
    pub(super) async fn write_index(&self, id: &str, record: &CacheRecord) -> Result<()> {
        let temporary = self
            .index_root
            .join(format!(".index-{}", uuid::Uuid::new_v4()));
        let result = async {
            tokio::fs::write(&temporary, serde_json::to_vec(record).map_err(invalid)?)
                .await
                .map_err(io_error)?;
            tokio::fs::rename(&temporary, self.index_root.join(format!("{id}.json")))
                .await
                .map_err(io_error)
        }
        .await;
        if result.is_err() {
            let _ = remove_file(&temporary).await;
        }
        result
    }
    fn files(&self, entry: &CacheEntry) -> RestoreAccess {
        RestoreAccess::pinned(
            entry.path.clone(),
            CacheLease {
                pin: Some(entry.pin.clone()),
                cache: Arc::downgrade(&self.cache),
                budget: self.budget,
                max_entries: self.max_entries,
            },
        )
    }
    pub(super) async fn insert(
        &self,
        artifact: &CheckpointArtifact,
        path: PathBuf,
        mut record: CacheRecord,
    ) -> Result<RestoreAccess> {
        let disk_bytes = allocated(&path)?;
        let mut cache = self.cache.lock().await;
        record.used = cache.tick();
        // A cache-index write must not block a disk-readable restoration.
        if let Err(error) = self.write_index(&artifact.location, &record).await {
            adx_observability::warn!(%error, "checkpoint cache index write failed");
        }
        let index = self.index_root.join(format!("{}.json", artifact.location));
        let disk_bytes = disk_bytes.saturating_add(allocated(&index).unwrap_or(0));
        let entry = CacheEntry {
            path,
            record,
            disk_bytes,
            index,
            verified: true,
            pin: Arc::new(()),
        };
        let files = self.files(&entry);
        cache.entries.insert(artifact.location.clone(), entry);
        Ok(files)
    }
    pub(super) async fn cached(
        &self,
        artifact: &CheckpointArtifact,
    ) -> Result<Option<RestoreAccess>> {
        let cached = {
            let mut cache = self.cache.lock().await;
            let used = cache.tick();
            cache.entries.get_mut(&artifact.location).map(|entry| {
                entry.record.used = used;
                (self.files(entry), entry.record.clone(), entry.verified)
            })
        };
        if let Some((files, record, verified)) = cached {
            if record.manifest.size != artifact.size_bytes {
                return Err(invalid("cached artifact identity mismatch"));
            }
            if !verified {
                Self::validate_manifest(&record.manifest, artifact)?;
                if !Self::valid_cached(&files, &record.manifest).await? {
                    return Err(invalid("recovered checkpoint cache is inconsistent"));
                }
                if let Some(entry) = self.cache.lock().await.entries.get_mut(&artifact.location) {
                    entry.verified = true;
                }
            }
            if let Err(error) = self.write_index(&artifact.location, &record).await {
                adx_observability::warn!(%error, "checkpoint cache recency persistence failed");
            }
            return Ok(Some(files));
        }
        // Local publication precedes the authoritative commit. This also pins
        // a running Firecracker capture before committed() can trim its base.
        let index = self.index_root.join(format!("{}.json", artifact.location));
        if let Ok(record) = read_record(&index) {
            Self::validate_manifest(&record.manifest, artifact)?;
            let path = if record.staged {
                &self.staging.root
            } else {
                &self.cache_root
            }
            .join(&artifact.location);
            if path.exists() {
                if !Self::valid_cached(&path, &record.manifest).await? {
                    return Err(invalid("local checkpoint cache is inconsistent"));
                }
                return self.insert(artifact, path, record).await.map(Some);
            }
        }
        // The optional index may be lost after upload or process shutdown.
        // Prefer the original upload path: a live Firecracker base can still
        // name it. Never replace it with a download at a different pathname.
        for (root, staged) in [(&self.staging.root, true), (&self.cache_root, false)] {
            let path = root.join(&artifact.location);
            if !path.exists() {
                continue;
            }
            let manifest = self.manifest(artifact).await?;
            if !Self::valid_cached(&path, &manifest).await? {
                return Err(invalid("unindexed checkpoint cache is inconsistent"));
            }
            return self
                .insert(
                    artifact,
                    path,
                    CacheRecord {
                        manifest,
                        staged,
                        committed: !staged,
                        used: 0,
                    },
                )
                .await
                .map(Some);
        }
        Ok(None)
    }
    pub(super) async fn cache_committed(&self, artifact: &CheckpointArtifact) -> Result<()> {
        let _operation = self.operation(&artifact.location).await;
        let mut cache = self.cache.lock().await;
        if let Some(entry) = cache.entries.get_mut(&artifact.location) {
            entry.record.committed = true;
            self.write_index(&artifact.location, &entry.record).await?;
            return Cache::trim(&mut cache, self.budget, self.max_entries).await;
        }
        drop(cache);
        let path = self.staging.root.join(&artifact.location);
        if !path.exists() {
            return Ok(());
        }
        let index = self.index_root.join(format!("{}.json", artifact.location));
        let mut record = match read_record(&index) {
            Ok(record) => record,
            Err(_) => CacheRecord {
                manifest: self.manifest(artifact).await?,
                staged: true,
                committed: true,
                used: 0,
            },
        };
        Self::validate_manifest(&record.manifest, artifact)?;
        record.committed = true;
        // Keep the pathname: a live Firecracker incremental base may point here.
        // Admission changes ownership/accounting without copying the snapshot.
        let files = self.insert(artifact, path, record).await?;
        drop(files);
        Cache::trim(&mut *self.cache.lock().await, self.budget, self.max_entries).await
    }
    pub(super) async fn reconcile_cache(&self, retained: &[CheckpointArtifact]) -> Result<()> {
        let keep: BTreeSet<_> = retained
            .iter()
            .filter(|a| a.storage == self.alias)
            .map(|a| a.location.as_str())
            .collect();
        let mut cache = self.cache.lock().await;
        let ids: Vec<_> = cache.entries.keys().cloned().collect();
        for id in ids {
            let entry = &cache.entries[&id];
            if Arc::strong_count(&entry.pin) > 1 {
                continue;
            }
            if !keep.contains(id.as_str())
                || !Self::valid_cached(&entry.path, &entry.record.manifest)
                    .await
                    .unwrap_or(false)
            {
                remove_directory(&entry.path).await?;
                remove_file(&entry.index).await?;
                cache.entries.remove(&id);
            } else if let Some(entry) = cache.entries.get_mut(&id) {
                entry.verified = true;
                entry.disk_bytes =
                    allocated(&entry.path)?.saturating_add(allocated(&entry.index).unwrap_or(0));
            }
        }
        cache.recovering = false;
        Cache::trim(&mut cache, self.budget, self.max_entries).await?;
        let mut local = retained.to_vec();
        for entry in cache.entries.values().filter(|e| e.record.staged) {
            local.push(CheckpointArtifact {
                storage: "local".into(),
                scope: String::new(),
                location: entry.path.to_string_lossy().into_owned(),
                size_bytes: entry.record.manifest.size,
            });
        }
        let mut downloads = tokio::fs::read_dir(&self.cache_root)
            .await
            .map_err(io_error)?;
        while let Some(entry) = downloads.next_entry().await.map_err(io_error)? {
            let name = entry.file_name().to_string_lossy().into_owned();
            if cache.entries.contains_key(&name) {
                continue;
            }
            if (uuid::Uuid::parse_str(&name).is_ok() || name.starts_with(".download-"))
                && entry.file_type().await.map_err(io_error)?.is_dir()
            {
                remove_directory(&entry.path()).await?;
            }
        }
        let known: BTreeSet<_> = cache.entries.keys().cloned().collect();
        let mut indexes = tokio::fs::read_dir(&self.index_root)
            .await
            .map_err(io_error)?;
        while let Some(entry) = indexes.next_entry().await.map_err(io_error)? {
            let path = entry.path();
            let id = path.file_stem().and_then(|s| s.to_str()).unwrap_or("");
            if !known.contains(id) {
                remove_file(&path).await?;
            }
        }
        drop(cache);
        self.staging.reconcile_local(&local).await
    }
}
