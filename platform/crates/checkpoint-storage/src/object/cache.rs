//! Local cache accounting. Execution leases are never subject to cache quotas.
use super::{invalid, CacheValidation, Manifest, ObjectCheckpointStore};
use crate::{io_error, CheckpointAccess, RestoreAccess};
use adx_core::{CheckpointArtifact, Result};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    sync::{Arc, Weak},
};
use tokio::sync::{Mutex, OwnedMutexGuard};
#[cfg(test)]
mod tests;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CacheRecord {
    pub(super) manifest: Manifest,
    pub(super) staged: bool,
    pub(super) committed: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub(super) invalid: bool,
    pub(super) used: u64,
}
pub(crate) struct CacheEntry {
    pub(crate) path: PathBuf,
    pub(crate) pin: Arc<()>,
    pub(super) record: CacheRecord,
    disk_bytes: u64,
    index: PathBuf,
}
#[derive(Default)]
pub(crate) struct Cache {
    pub(crate) entries: BTreeMap<String, CacheEntry>,
    references: BTreeMap<String, Weak<()>>,
    pub(crate) operations: BTreeMap<String, Weak<Mutex<()>>>,
    clock: u64,
    recovering: bool,
    maintenance: Arc<Mutex<()>>,
    verification: Arc<Mutex<()>>,
    garbage: BTreeSet<String>,
    cleanup: BTreeSet<PathBuf>,
    unverified: BTreeSet<String>,
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
    pub(super) fn pin(&mut self, id: &str) -> Arc<()> {
        let pin = self
            .entries
            .get(id)
            .map(|entry| entry.pin.clone())
            .or_else(|| self.references.get(id).and_then(Weak::upgrade))
            .unwrap_or_default();
        self.references.retain(|_, pin| pin.strong_count() > 0);
        self.references.insert(id.to_owned(), Arc::downgrade(&pin));
        pin
    }
    pub(super) async fn operation(cache: &Arc<Mutex<Self>>, id: &str) -> OwnedMutexGuard<()> {
        let lock = {
            let mut cache = cache.lock().await;
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
    // Caller owns the artifact operation lock. Pins and identity are checked
    // atomically; filesystem work never holds the cache directory mutex.
    pub(super) async fn remove(cache: &Arc<Mutex<Self>>, id: &str) -> Result<bool> {
        let entry = {
            let mut cache = cache.lock().await;
            let pinned = match cache.entries.get(id) {
                Some(entry) => Arc::strong_count(&entry.pin) != 1,
                None => cache
                    .references
                    .get(id)
                    .is_some_and(|pin| pin.strong_count() > 0),
            };
            if pinned {
                return Ok(false);
            }
            cache.entries.remove(id)
        };
        let Some(entry) = entry else { return Ok(true) };
        if let Err(error) = async {
            remove_directory(&entry.path).await?;
            remove_file(&entry.index).await
        }
        .await
        {
            cache.lock().await.entries.insert(id.to_owned(), entry);
            return Err(error);
        }
        Ok(true)
    }
    async fn invalidate(cache: &Arc<Mutex<Self>>, id: &str, checked: &Weak<()>) -> Result<()> {
        let _operation = Self::operation(cache, id).await;
        let marked = {
            let mut cache = cache.lock().await;
            cache
                .entries
                .get_mut(id)
                .filter(|entry| Weak::ptr_eq(checked, &Arc::downgrade(&entry.pin)))
                .map(|entry| {
                    entry.record.invalid = true;
                    (entry.index.clone(), entry.record.clone())
                })
        };
        if let Some((index, record)) = marked {
            if let Err(error) = write_record(&index, &record).await {
                // A missing index forces full content verification after restart.
                if remove_file(&index).await.is_err() {
                    return Err(error);
                }
            }
        }
        Ok(())
    }

    pub(super) async fn trim(cache: &Arc<Mutex<Self>>, bytes: u64, entries: usize) -> Result<()> {
        let maintenance = cache.lock().await.maintenance.clone();
        let _maintenance = maintenance.lock().await;
        loop {
            let candidate = {
                let cache = cache.lock().await;
                if cache.recovering {
                    return Ok(());
                }
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
                reusable
                    .into_iter()
                    .min_by_key(|(_, e)| e.record.used)
                    .map(|(id, entry)| (id.clone(), entry.record.used))
            };
            let Some((id, used)) = candidate else {
                return Ok(());
            };
            let _operation = Self::operation(cache, &id).await;
            let unchanged = cache
                .lock()
                .await
                .entries
                .get(&id)
                .is_some_and(|e| e.record.used == used);
            if unchanged {
                Self::remove(cache, &id).await?;
            }
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
                    if let Err(error) = Cache::trim(&cache, budget, max_entries).await {
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
async fn write_record(index: &Path, record: &CacheRecord) -> Result<()> {
    let temporary = index.with_file_name(format!(".index-{}", uuid::Uuid::new_v4()));
    let result = async {
        tokio::fs::write(&temporary, serde_json::to_vec(record).map_err(invalid)?)
            .await
            .map_err(io_error)?;
        tokio::fs::rename(&temporary, index).await.map_err(io_error)
    }
    .await;
    if result.is_err() {
        let _ = remove_file(&temporary).await;
    }
    result
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
    // Caller holds the artifact operation lock; release and repair share the
    // same pin check before touching either private pathname.
    pub(super) async fn discard_cached(&self, id: &str) -> Result<()> {
        if !Cache::remove(&self.cache, id).await? {
            return Err(adx_core::Error::Conflict);
        }
        for root in [&self.cache_root, &self.staging.root] {
            remove_directory(&root.join(id)).await?;
        }
        self.remove_index(id).await
    }
    pub(super) async fn operation(&self, id: &str) -> OwnedMutexGuard<()> {
        Cache::operation(&self.cache, id).await
    }
    pub(crate) async fn remove_index(&self, id: &str) -> Result<()> {
        remove_file(&self.index_root.join(format!("{id}.json"))).await
    }
    pub(super) async fn write_index(&self, id: &str, record: &CacheRecord) -> Result<()> {
        write_record(&self.index_root.join(format!("{id}.json")), record).await
    }
    pub(super) fn lease(&self, pin: Arc<()>) -> CacheLease {
        CacheLease {
            pin: Some(pin),
            cache: Arc::downgrade(&self.cache),
            budget: self.budget,
            max_entries: self.max_entries,
        }
    }
    fn files(&self, entry: &CacheEntry) -> RestoreAccess {
        RestoreAccess::pinned(entry.path.clone(), self.lease(entry.pin.clone()))
    }
    pub(super) async fn insert(
        &self,
        artifact: &CheckpointArtifact,
        path: PathBuf,
        mut record: CacheRecord,
    ) -> Result<RestoreAccess> {
        let disk_bytes = allocated(&path)?;
        record.used = self.cache.lock().await.tick();
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
            pin: self.cache.lock().await.pin(&artifact.location),
        };
        let files = self.files(&entry);
        self.cache
            .lock()
            .await
            .entries
            .insert(artifact.location.clone(), entry);
        Ok(files)
    }
    pub(super) async fn cached(
        &self,
        artifact: &CheckpointArtifact,
    ) -> Result<Option<(RestoreAccess, CacheRecord)>> {
        let cached = {
            let mut cache = self.cache.lock().await;
            let used = cache.tick();
            cache.entries.get_mut(&artifact.location).map(|entry| {
                entry.record.used = used;
                (
                    (!entry.record.invalid).then(|| self.files(entry)),
                    entry.record.clone(),
                )
            })
        };
        if let Some((files, record)) = cached {
            let Some(files) = files else {
                self.discard_cached(&artifact.location).await?;
                return Ok(None);
            };
            if let Err(error) = self.write_index(&artifact.location, &record).await {
                adx_observability::warn!(%error, "checkpoint cache recency persistence failed");
            }
            return Ok(Some((files, record)));
        }
        // Prefer an existing pathname: live incremental executions may name it.
        if let Ok(record) =
            read_record(&self.index_root.join(format!("{}.json", artifact.location)))
        {
            if record.invalid {
                self.discard_cached(&artifact.location).await?;
                return Ok(None);
            }
            let path = if record.staged {
                &self.staging.root
            } else {
                &self.cache_root
            }
            .join(&artifact.location);
            if path.exists() && Self::validate_manifest(&record.manifest, artifact).is_ok() {
                let files = self.insert(artifact, path, record.clone()).await?;
                return Ok(Some((files, record)));
            }
        }
        for (root, staged) in [(&self.staging.root, true), (&self.cache_root, false)] {
            let path = root.join(&artifact.location);
            if path.exists() {
                let _materialization = self.materialization_slot().await?;
                let manifest = self.manifest(artifact).await?;
                if Self::valid_cached(&path, &manifest, CacheValidation::Content).await?
                    != Some(true)
                {
                    self.discard_cached(&artifact.location).await?;
                    return Ok(None);
                }
                let record = CacheRecord {
                    manifest,
                    staged,
                    committed: !staged,
                    invalid: false,
                    used: 0,
                };
                let files = self.insert(artifact, path, record.clone()).await?;
                return Ok(Some((files, record)));
            }
        }
        Ok(None)
    }
    pub(super) async fn cache_committed(&self, artifact: &CheckpointArtifact) -> Result<()> {
        let _operation = self.operation(&artifact.location).await;
        if !self
            .cache
            .lock()
            .await
            .entries
            .contains_key(&artifact.location)
            && self.staging.root.join(&artifact.location).exists()
        {
            self.cached(artifact).await?;
        }
        let record = {
            let mut cache = self.cache.lock().await;
            cache.entries.get_mut(&artifact.location).map(|entry| {
                entry.record.committed = true;
                entry.record.clone()
            })
        };
        if let Some(record) = record {
            self.write_index(&artifact.location, &record).await?;
            drop(_operation);
            Cache::trim(&self.cache, self.budget, self.max_entries).await?;
        }
        Ok(())
    }

    pub(super) async fn reconcile_cache(&self, retained: &[CheckpointArtifact]) -> Result<()> {
        let keep: BTreeSet<_> = retained
            .iter()
            .filter(|a| a.storage == self.alias)
            .map(|a| a.location.as_str())
            .collect();
        let ids: Vec<_> = self.cache.lock().await.entries.keys().cloned().collect();
        let was_recovering = self.cache.lock().await.recovering;
        self.cache
            .lock()
            .await
            .garbage
            .extend(ids.iter().filter(|id| !keep.contains(id.as_str())).cloned());
        let mut failure = self.maintain().await.err();
        let (local, known) = {
            let cache = self.cache.lock().await;
            let mut local = retained.to_vec();
            local.extend(
                cache
                    .entries
                    .values()
                    .filter(|entry| entry.record.staged)
                    .map(|entry| CheckpointArtifact {
                        storage: "local".into(),
                        scope: String::new(),
                        location: entry.path.to_string_lossy().into_owned(),
                        size_bytes: entry.record.manifest.size,
                    }),
            );
            (
                local,
                cache.entries.keys().cloned().collect::<BTreeSet<_>>(),
            )
        };
        for (root, directories) in [(&self.cache_root, true), (&self.index_root, false)] {
            let mut files = tokio::fs::read_dir(root).await.map_err(io_error)?;
            while let Some(entry) = files.next_entry().await.map_err(io_error)? {
                let name = entry.file_name().to_string_lossy().into_owned();
                let id = if directories {
                    name.as_str()
                } else {
                    name.strip_suffix(".json").unwrap_or("")
                };
                if known.contains(id) {
                    continue;
                }
                if directories
                    && (uuid::Uuid::parse_str(id).is_ok() || name.starts_with(".download-"))
                {
                    if let Err(error) = remove_directory(&entry.path()).await {
                        self.cache.lock().await.cleanup.insert(entry.path());
                        failure = Some(error);
                    }
                } else if !directories {
                    if let Err(error) = remove_file(&entry.path()).await {
                        self.cache.lock().await.cleanup.insert(entry.path());
                        failure = Some(error);
                    }
                }
            }
        }
        if let Err(error) = self.staging.reconcile_local(&local).await {
            failure = Some(error);
        }
        if !was_recovering {
            self.verify_idle(ids).await;
        }
        failure.map_or(Ok(()), Err)
    }

    pub(super) async fn maintain(&self) -> Result<()> {
        let (garbage, cleanup, recovering) = {
            let mut cache = self.cache.lock().await;
            let recovering = cache.recovering;
            cache.recovering = false;
            (cache.garbage.clone(), cache.cleanup.clone(), recovering)
        };
        let mut failure = None;
        for id in garbage {
            let _operation = self.operation(&id).await;
            match Cache::remove(&self.cache, &id).await {
                Ok(true) => {
                    self.cache.lock().await.garbage.remove(&id);
                }
                Ok(false) => (),
                Err(error) => failure = Some(error),
            }
        }
        for path in cleanup {
            let result = if path.parent() == Some(self.index_root.as_path()) {
                let id = path
                    .file_stem()
                    .and_then(|name| name.to_str())
                    .unwrap_or("");
                let _operation = self.operation(id).await;
                if self.cache.lock().await.entries.contains_key(id) {
                    Ok(())
                } else {
                    remove_file(&path).await
                }
            } else {
                let id = path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .ok_or_else(|| invalid("invalid cache cleanup path"))?;
                let _operation = self.operation(id).await;
                match Cache::remove(&self.cache, id).await {
                    Ok(true) => remove_directory(&path).await,
                    Ok(false) => continue,
                    Err(error) => Err(error),
                }
            };
            match result {
                Ok(()) => {
                    self.cache.lock().await.cleanup.remove(&path);
                }
                Err(error) => failure = Some(error),
            }
        }
        if let Err(error) = Cache::trim(&self.cache, self.budget, self.max_entries).await {
            failure = Some(error);
        }
        let ids = if recovering {
            self.cache.lock().await.entries.keys().cloned().collect()
        } else {
            vec![]
        };
        self.verify_idle(ids).await;
        failure.map_or(Ok(()), Err)
    }

    async fn verify_idle(&self, ids: Vec<String>) {
        // Hash only idle files, then mark failures under the artifact lock.
        // Live files stay in place until their execution pins are released.
        let (ids, verification) = {
            let mut cache = self.cache.lock().await;
            cache.unverified.extend(ids);
            if cache.unverified.is_empty() {
                return;
            }
            (
                cache.unverified.iter().cloned().collect::<Vec<_>>(),
                cache.verification.clone(),
            )
        };
        let Ok(verification) = verification.try_lock_owned() else {
            return;
        };
        let cache = Arc::downgrade(&self.cache);
        tokio::spawn(async move {
            let _verification = verification;
            futures_util::stream::iter(ids).for_each_concurrent(2, |id| {
                let cache = cache.clone();
                async move {
                    let candidate = if let Some(cache) = cache.upgrade() {
                        let mut cache = cache.lock().await;
                        match cache.entries.get(&id) {
                            Some(entry) if Arc::strong_count(&entry.pin) == 1 =>
                                Some((entry.path.clone(), entry.record.manifest.clone(), Arc::downgrade(&entry.pin))),
                            Some(_) => None,
                            None => { cache.unverified.remove(&id); None },
                        }
                    } else { None };
                    let Some((path, manifest, pin)) = candidate else { return };
                    let complete = match Self::valid_cached(&path, &manifest, CacheValidation::Idle(&pin)).await {
                        Ok(Some(false)) => {
                            adx_observability::warn!(checkpoint_id=%id, "idle checkpoint cache failed validation");
                            if let Some(cache) = cache.upgrade() {
                                if let Err(error) = Cache::invalidate(&cache, &id, &pin).await {
                                    adx_observability::warn!(checkpoint_id=%id, %error, "checkpoint cache invalidation unavailable");
                                    return;
                                }
                            }
                            true
                        },
                        Ok(Some(true)) => true,
                        Ok(None) => false,
                        Err(error) => {
                            adx_observability::warn!(checkpoint_id=%id, %error, "idle checkpoint cache verification unavailable");
                            false
                        },
                    };
                    if complete {
                        if let Some(cache) = cache.upgrade() {
                            cache.lock().await.unverified.remove(&id);
                        }
                    }
                }
            }).await;
        });
    }
}
