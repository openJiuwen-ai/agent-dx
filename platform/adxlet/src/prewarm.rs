//! Immutable EROFS artifact preparation from deployment-provisioned, digest-pinned
//! sources. No Environment, user mount, credential or application is created here.
use adx_core::{prewarm::*, runtime::RuntimeIdentity, Error, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    os::{
        fd::AsRawFd,
        unix::fs::{OpenOptionsExt, PermissionsExt},
    },
    path::{Path, PathBuf},
    sync::Arc,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ImageProcess {
    pub version: u32,
    pub args: Vec<String>,
    pub cwd: String,
    pub user: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactSource {
    pub image: String,
    pub path: PathBuf,
    pub sha256: String,
    pub architecture: String,
    pub runtime_classes: Vec<String>,
    pub process: ImageProcess,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactCacheConfig {
    pub root: PathBuf,
    pub max_disk_bytes: u64,
    pub sources: Vec<ArtifactSource>,
}
#[derive(Clone)]
pub struct ArtifactCache {
    config: ArtifactCacheConfig,
    runtime_recipe_digest: String,
    gate: Arc<tokio::sync::Mutex<()>>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    id: String,
    profile: PreparationProfile,
    source: ArtifactSource,
    state: SlotState,
    size_bytes: u64,
    leases: BTreeMap<String, RuntimeIdentity>,
}
pub(crate) struct PreparedInput {
    pub rootfs: PathBuf,
    pub process: PathBuf,
    pub env: BTreeMap<String, String>,
}

fn io_error(_: impl std::fmt::Display) -> Error {
    Error::Unavailable("prewarm artifact storage operation failed".into())
}
fn source_recipe(source: &ArtifactSource) -> Result<String> {
    fingerprint(&(
        &source.image,
        &source.sha256,
        &source.architecture,
        &source.process,
        &source.env,
    ))
}
fn canonical_absolute(path: &Path) -> bool {
    path.is_absolute()
        && path != Path::new("/")
        && path.components().all(|c| {
            matches!(
                c,
                std::path::Component::RootDir | std::path::Component::Normal(_)
            )
        })
}
fn object_id(id: &str) -> Result<()> {
    if id.is_empty()
        || id.len() > 128
        || !id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
    {
        return Err(Error::Invalid("invalid artifact identifier".into()));
    }
    Ok(())
}
fn source_file(path: &Path) -> Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                Error::NotFound
            } else {
                io_error(error)
            }
        })?;
    if !file.metadata().map_err(io_error)?.is_file() {
        return Err(Error::Invalid(
            "artifact source must be a regular file".into(),
        ));
    }
    Ok(file)
}
fn hash_file(path: &Path) -> Result<(String, u64)> {
    let mut file = source_file(path)?;
    let mut hash = Sha256::new();
    let mut bytes = [0; 65536];
    let mut size = 0u64;
    loop {
        let count = file.read(&mut bytes).map_err(io_error)?;
        if count == 0 {
            break;
        }
        size = size
            .checked_add(u64::try_from(count).map_err(|_| Error::Conflict)?)
            .ok_or(Error::Conflict)?;
        hash.update(&bytes[..count]);
    }
    Ok((format!("sha256:{:x}", hash.finalize()), size))
}
fn sync_dir(path: &Path) -> Result<()> {
    File::open(path)
        .and_then(|f| f.sync_all())
        .map_err(io_error)
}
fn manifest(path: &Path) -> Result<Manifest> {
    let file = source_file(&path.join("manifest.json"))?;
    let size = file.metadata().map_err(io_error)?.len();
    if size > 1_048_576 {
        return Err(Error::Unavailable("artifact manifest exceeds limit".into()));
    }
    serde_json::from_reader(file)
        .map_err(|_| Error::Unavailable("corrupt artifact manifest".into()))
}
fn persist(path: &Path, record: &Manifest) -> Result<()> {
    let temporary = path.join(format!("manifest-{}.tmp", uuid::Uuid::new_v4()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&temporary)
        .map_err(io_error)?;
    serde_json::to_writer(&mut file, record).map_err(io_error)?;
    file.flush().map_err(io_error)?;
    file.sync_all().map_err(io_error)?;
    fs::rename(&temporary, path.join("manifest.json")).map_err(io_error)?;
    sync_dir(path)
}
fn verify(path: &Path, record: &Manifest) -> Result<()> {
    if record.state != SlotState::Ready {
        return Err(Error::Conflict);
    }
    let (digest, _) = hash_file(&path.join("rootfs.img"))?;
    if digest != record.source.sha256 {
        return Err(Error::Unavailable(
            "prepared artifact integrity mismatch".into(),
        ));
    }
    let expected = serde_json::to_vec(&record.source.process).map_err(io_error)?;
    let actual = fs::read(path.join("process.json")).map_err(io_error)?;
    if actual != expected {
        return Err(Error::Unavailable(
            "prepared process metadata mismatch".into(),
        ));
    }
    Ok(())
}
impl ArtifactCache {
    pub fn new(
        config: ArtifactCacheConfig,
        runtime_profile: Option<&adx_core::runtime_profile::RuntimeProfile>,
    ) -> Result<Self> {
        if !canonical_absolute(&config.root)
            || config.max_disk_bytes == 0
            || config.sources.is_empty()
        {
            return Err(Error::Invalid(
                "prewarm cache requires an absolute root, a disk limit and pinned sources".into(),
            ));
        }
        let mut images = std::collections::BTreeSet::new();
        for source in &config.sources {
            if !images.insert(&source.image)
                || !canonical_absolute(&source.path)
                || !valid_digest(&source.sha256)
                || source
                    .image
                    .rsplit_once('@')
                    .is_none_or(|(_, d)| !valid_digest(d))
                || !matches!(source.architecture.as_str(), "amd64" | "arm64")
                || source.runtime_classes.is_empty()
                || source
                    .runtime_classes
                    .iter()
                    .any(|r| !matches!(r.as_str(), "runc" | "runsc" | "kata"))
                || source.process.version != 1
                || source
                    .process
                    .args
                    .first()
                    .is_none_or(|s| !s.starts_with('/'))
                || source.process.args.iter().any(|s| s.contains('\0'))
                || !source.process.cwd.starts_with('/')
                || source.process.cwd.split('/').any(|c| c == "..")
                || source.process.user.is_empty()
                || source.process.user.contains('\0')
                || source.process.cwd.contains('\0')
                || source.env.iter().any(|(k, v)| {
                    k.is_empty()
                        || k.starts_with("ADX_")
                        || k.starts_with("EXECD_")
                        || k.contains(['=', '\0'])
                        || v.contains('\0')
                })
            {
                return Err(Error::Invalid(
                    "invalid immutable EROFS source or image process contract".into(),
                ));
            }
        }
        fs::create_dir_all(&config.root).map_err(io_error)?;
        if fs::symlink_metadata(&config.root)
            .map_err(io_error)?
            .file_type()
            .is_symlink()
            || fs::canonicalize(&config.root).map_err(io_error)? != config.root
        {
            return Err(Error::Invalid(
                "prewarm root must not traverse symlinks".into(),
            ));
        }
        fs::set_permissions(&config.root, fs::Permissions::from_mode(0o700)).map_err(io_error)?;
        Ok(Self {
            config,
            runtime_recipe_digest: fingerprint(&runtime_profile)?,
            gate: Arc::default(),
        })
    }
    pub fn config(&self) -> &ArtifactCacheConfig {
        &self.config
    }
    async fn io<T: Send + 'static>(
        &self,
        operation: impl FnOnce(Self) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let guard = self.gate.clone().lock_owned().await;
        let cache = self.clone();
        tokio::task::spawn_blocking(move || {
            let _guard = guard;
            let lock = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .mode(0o600)
                .custom_flags(libc::O_NOFOLLOW)
                .open(cache.config.root.join(".lock"))
                .map_err(io_error)?;
            // SAFETY: lock owns the live descriptor; flock only changes its advisory lock state.
            if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX) } != 0 {
                return Err(io_error(std::io::Error::last_os_error()));
            }
            // Closing the file releases the process-shared lock even on an error.
            let _lock = lock;
            operation(cache)
        })
        .await
        .map_err(io_error)?
    }
    fn source(&self, profile: &PreparationProfile) -> Result<&ArtifactSource> {
        profile.validate()?;
        let architecture = match std::env::consts::ARCH {
            "x86_64" => "amd64",
            "aarch64" => "arm64",
            _ => return Err(unsupported()),
        };
        if profile.architecture != architecture {
            return Err(unsupported());
        }
        if profile.runtime_recipe_digest != self.runtime_recipe_digest {
            return Err(unsupported());
        }
        self.config
            .sources
            .iter()
            .find(|s| {
                s.image == profile.image
                    && s.architecture == profile.architecture
                    && s.runtime_classes.contains(&profile.runtime_class)
            })
            .ok_or_else(unsupported)
    }
    pub async fn qualify(&self, profile: &PreparationProfile) -> Result<ArtifactObservation> {
        let source = self.source(profile)?.clone();
        self.io(move |_| {
            let (digest, size) = hash_file(&source.path)?;
            let mut file = source_file(&source.path)?;
            use std::io::{Seek, SeekFrom};
            file.seek(SeekFrom::Start(1024)).map_err(io_error)?;
            let mut magic = [0; 4];
            file.read_exact(&mut magic).map_err(io_error)?;
            if digest != source.sha256 || magic != [0xe2, 0xe1, 0xf5, 0xe0] {
                return Err(Error::Invalid(
                    "EROFS source digest or header mismatch".into(),
                ));
            }
            let process_bytes =
                u64::try_from(serde_json::to_vec(&source.process).map_err(io_error)?.len())
                    .map_err(|_| Error::Conflict)?;
            Ok(ArtifactObservation {
                recipe_digest: source_recipe(&source)?,
                size_bytes: size.checked_add(process_bytes).ok_or(Error::Conflict)?,
                ready: true,
                in_use: false,
            })
        })
        .await
    }
    pub async fn prepare(
        &self,
        id: &str,
        profile: &PreparationProfile,
        max_bytes: u64,
    ) -> Result<ArtifactObservation> {
        object_id(id)?;
        let source = self.source(profile)?.clone();
        let qualified = self.qualify(profile).await?;
        let id = id.to_owned();
        let profile = profile.clone();
        self.io(move |cache| {
            if cache.config.root.join(format!("deleted-{id}")).exists() {
                return Err(Error::Conflict);
            }
            let path = cache.config.root.join(&id);
            let mut record = if path.join("manifest.json").exists() {
                let record = manifest(&path)?;
                if record.id != id
                    || record.profile != profile
                    || fingerprint(&record.source)? != fingerprint(&source)?
                    || record.state == SlotState::Deleted
                {
                    return Err(Error::Conflict);
                }
                if record.state == SlotState::Ready {
                    verify(&path, &record)?;
                    return Ok(ArtifactObservation {
                        recipe_digest: source_recipe(&record.source)?,
                        size_bytes: record.size_bytes,
                        ready: true,
                        in_use: !record.leases.is_empty(),
                    });
                }
                record
            } else {
                let mut used = 0u64;
                for entry in fs::read_dir(&cache.config.root).map_err(io_error)? {
                    let entry = entry.map_err(io_error)?;
                    if !entry.file_type().map_err(io_error)?.is_dir() {
                        continue;
                    }
                    let record = manifest(&entry.path())?;
                    used = used
                        .checked_add(record.size_bytes)
                        .ok_or(Error::NoCapacity)?;
                }
                if qualified.size_bytes > max_bytes
                    || used
                        .checked_add(qualified.size_bytes)
                        .is_none_or(|n| n > cache.config.max_disk_bytes)
                {
                    return Err(Error::NoCapacity);
                }
                fs::create_dir_all(&path).map_err(io_error)?;
                let record = Manifest {
                    id: id.clone(),
                    profile,
                    source: source.clone(),
                    state: SlotState::Preparing,
                    size_bytes: qualified.size_bytes,
                    leases: BTreeMap::new(),
                };
                persist(&path, &record)?;
                sync_dir(&cache.config.root)?;
                record
            };
            if record.size_bytes > max_bytes {
                return Err(Error::NoCapacity);
            }
            let input = source_file(&source.path)?;
            let mut output = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .mode(0o400)
                .custom_flags(libc::O_NOFOLLOW)
                .open(path.join("rootfs.img"))
                .map_err(io_error)?;
            let copied =
                std::io::copy(&mut input.take(record.size_bytes), &mut output).map_err(io_error)?;
            output.sync_all().map_err(io_error)?;
            if copied > record.size_bytes {
                return Err(Error::NoCapacity);
            }
            let (digest, _) = hash_file(&path.join("rootfs.img"))?;
            if digest != source.sha256 {
                return Err(Error::Invalid("EROFS changed during preparation".into()));
            }
            let mut process = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .mode(0o400)
                .custom_flags(libc::O_NOFOLLOW)
                .open(path.join("process.json"))
                .map_err(io_error)?;
            serde_json::to_writer(&mut process, &source.process).map_err(io_error)?;
            process.flush().map_err(io_error)?;
            process.sync_all().map_err(io_error)?;
            record.state = SlotState::Ready;
            persist(&path, &record)?;
            Ok(ArtifactObservation {
                recipe_digest: source_recipe(&record.source)?,
                size_bytes: record.size_bytes,
                ready: true,
                in_use: false,
            })
        })
        .await
    }
    pub async fn observe(&self, id: &str) -> Result<ArtifactObservation> {
        object_id(id)?;
        let id = id.to_owned();
        self.io(move |cache| {
            let path = cache.config.root.join(id);
            let record = manifest(&path)?;
            verify(&path, &record)?;
            Ok(ArtifactObservation {
                recipe_digest: source_recipe(&record.source)?,
                size_bytes: record.size_bytes,
                ready: true,
                in_use: !record.leases.is_empty(),
            })
        })
        .await
    }
    pub async fn pin(
        &self,
        id: &str,
        image: &str,
        tenant: &str,
        identity: &RuntimeIdentity,
    ) -> Result<()> {
        object_id(id)?;
        identity.validate()?;
        let id = id.to_owned();
        let image = image.to_owned();
        let tenant = tenant.to_owned();
        let identity = identity.clone();
        self.io(move |cache| {
            let path = cache.config.root.join(id);
            let mut record = manifest(&path)?;
            if record.state == SlotState::Deleted {
                return Err(Error::NotFound);
            }
            if record.profile.image != image || record.profile.source_ref.tenant != tenant {
                return Err(Error::Conflict);
            }
            verify(&path, &record)?;
            if record
                .leases
                .get(&identity.runtime_id)
                .is_some_and(|old| old != &identity)
            {
                return Err(Error::Conflict);
            }
            record.leases.insert(identity.runtime_id.clone(), identity);
            persist(&path, &record)
        })
        .await
    }
    pub(crate) async fn prepared_input(&self, runtime_id: &str) -> Result<Option<PreparedInput>> {
        let runtime_id = runtime_id.to_owned();
        self.io(move |cache| {
            for entry in fs::read_dir(&cache.config.root).map_err(io_error)? {
                let entry = entry.map_err(io_error)?;
                if !entry.file_type().map_err(io_error)?.is_dir() {
                    continue;
                }
                let record = manifest(&entry.path())?;
                if record.leases.contains_key(&runtime_id) {
                    verify(&entry.path(), &record)?;
                    return Ok(Some(PreparedInput {
                        rootfs: entry.path().join("rootfs.img"),
                        process: entry.path().join("process.json"),
                        env: record.source.env,
                    }));
                }
            }
            Ok(None)
        })
        .await
    }
    pub(crate) async fn holds_environment(&self, environment_id: &str) -> Result<bool> {
        let id = environment_id.to_owned();
        self.io(move |cache| {
            for entry in fs::read_dir(&cache.config.root).map_err(io_error)? {
                let entry = entry.map_err(io_error)?;
                if entry.file_type().map_err(io_error)?.is_dir()
                    && manifest(&entry.path())?
                        .leases
                        .values()
                        .any(|lease| lease.environment_id == id)
                {
                    return Ok(true);
                }
            }
            Ok(false)
        })
        .await
    }
    /// Release only after the runtime driver's targeted deletion is confirmed.
    pub async fn release(&self, runtime_id: &str) -> Result<()> {
        let runtime_id = runtime_id.to_owned();
        self.io(move |cache| {
            for entry in fs::read_dir(&cache.config.root).map_err(io_error)? {
                let entry = entry.map_err(io_error)?;
                if !entry.file_type().map_err(io_error)?.is_dir() {
                    continue;
                }
                let mut record = manifest(&entry.path())?;
                if record.leases.remove(&runtime_id).is_some() {
                    persist(&entry.path(), &record)?;
                }
            }
            Ok(())
        })
        .await
        .map_err(|error| {
            if error == Error::NotFound {
                Error::Unavailable("artifact execution reference storage is unconfirmed".into())
            } else {
                error
            }
        })
    }
    pub async fn destroy(&self, id: &str) -> Result<()> {
        object_id(id)?;
        let id = id.to_owned();
        self.io(move |cache| {
            let path = cache.config.root.join(&id);
            let mut record = if path.exists() {
                Some(manifest(&path)?)
            } else {
                None
            };
            if record.as_ref().is_some_and(|r| !r.leases.is_empty()) {
                return Err(Error::Conflict);
            }
            let tombstone = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(false)
                .mode(0o600)
                .custom_flags(libc::O_NOFOLLOW)
                .open(cache.config.root.join(format!("deleted-{id}")))
                .map_err(io_error)?;
            tombstone.sync_all().map_err(io_error)?;
            sync_dir(&cache.config.root)?;
            let Some(record) = record.as_mut() else {
                return Ok(());
            };
            for name in ["rootfs.img", "process.json"] {
                match fs::remove_file(path.join(name)) {
                    Ok(()) => (),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
                    Err(e) => return Err(io_error(e)),
                }
            }
            record.state = SlotState::Deleted;
            record.size_bytes = 0;
            persist(&path, record)
        })
        .await
    }
}
