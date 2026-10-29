//! Direct filesystem access: capture and restore keep the same pathname.
mod mount;
use super::{io_error, scan, CheckpointAccess, RestoreAccess};
use adx_core::{CheckpointArtifact, Error, Result};
use async_trait::async_trait;
use mount::MountGuard;
use serde::{Deserialize, Serialize};
use std::os::{fd::AsRawFd, unix::fs::MetadataExt};
use std::path::{Component, Path, PathBuf};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    version: u32,
    size_bytes: u64,
    files: Vec<(PathBuf, u64)>,
}

pub struct FilesystemCheckpointStore {
    pub(super) alias: String,
    pub(super) root: PathBuf,
    pub(super) mount_root: PathBuf,
    root_handle: std::fs::File,
    mount_guard: Option<MountGuard>,
}
impl FilesystemCheckpointStore {
    pub fn new(alias: String, mount: PathBuf, prefix: PathBuf) -> Result<Self> {
        Self::open(alias, mount, prefix, true)
    }
    fn open(alias: String, mount: PathBuf, prefix: PathBuf, check_mount: bool) -> Result<Self> {
        if alias.is_empty()
            || alias == "local"
            || !alias
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
            || prefix.as_os_str().is_empty()
            || !prefix
                .components()
                .all(|p| matches!(p, Component::Normal(_)))
        {
            return Err(Error::Invalid(
                "invalid filesystem store identity or prefix".into(),
            ));
        }
        let mount = std::fs::canonicalize(mount).map_err(io_error)?;
        let mount_guard = if check_mount {
            Some(MountGuard::open(mount.clone())?)
        } else {
            None
        };
        let root = mount.join(prefix);
        std::fs::create_dir_all(root.join("artifacts")).map_err(io_error)?;
        std::fs::create_dir_all(root.join("manifests")).map_err(io_error)?;
        let root = std::fs::canonicalize(root).map_err(io_error)?;
        if !root.starts_with(&mount) {
            return Err(Error::Invalid("filesystem prefix escapes mount".into()));
        }
        let identity = root.join("store-id");
        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&identity)
        {
            Ok(mut file) => {
                use std::io::Write;
                file.write_all(alias.as_bytes()).map_err(io_error)?;
                file.sync_all().map_err(io_error)?;
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(io_error(e)),
        }
        if std::fs::read_to_string(identity).map_err(io_error)? != alias {
            return Err(Error::Invalid("filesystem store identity mismatch".into()));
        }
        let root_handle = std::fs::File::open(&root).map_err(io_error)?;
        let store = Self {
            alias,
            root,
            mount_root: mount,
            root_handle,
            mount_guard,
        };
        store.ready()?;
        let mut directory = store.root.as_path();
        loop {
            std::fs::File::open(directory)
                .map_err(io_error)?
                .sync_all()
                .map_err(io_error)?;
            if directory == store.mount_root {
                break;
            }
            directory = directory
                .parent()
                .ok_or_else(|| Error::Invalid("shared prefix escaped mount".into()))?;
        }
        Ok(store)
    }
    pub(super) fn ready(&self) -> Result<()> {
        if let Some(guard) = &self.mount_guard {
            guard.verify()?;
        }
        let current = std::fs::metadata(&self.root).map_err(io_error)?;
        let pinned = self.root_handle.metadata().map_err(io_error)?;
        if (current.dev(), current.ino()) != (pinned.dev(), pinned.ino()) {
            return Err(Error::Unavailable(
                "shared checkpoint namespace was replaced".into(),
            ));
        }
        Ok(())
    }
    /// Keep all cached-backend I/O on the opened mount, even during lazy unmount.
    pub(super) fn pinned_root(&self) -> PathBuf {
        PathBuf::from(format!("/proc/self/fd/{}", self.root_handle.as_raw_fd()))
    }
    fn direct_ready(&self) -> Result<()> {
        self.ready()?;
        if self
            .pinned_root()
            .join("cached-layout")
            .try_exists()
            .map_err(io_error)?
        {
            return Err(Error::Invalid(
                "cached filesystem namespace requires local_cache".into(),
            ));
        }
        Ok(())
    }
    pub(super) fn initialize_cached_layout(&self) -> Result<()> {
        use std::io::Write;
        self.ready()?;
        let marker = self.pinned_root().join("cached-layout");
        if !marker.try_exists().map_err(io_error)? {
            for directory in ["artifacts", "manifests"] {
                if std::fs::read_dir(self.pinned_root().join(directory))
                    .map_err(io_error)?
                    .next()
                    .is_some()
                {
                    return Err(Error::Invalid(
                        "local_cache requires a fresh shared checkpoint prefix".into(),
                    ));
                }
            }
            match std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&marker)
            {
                Ok(mut file) => {
                    file.write_all(b"cached-filesystem-v1\n")
                        .map_err(io_error)?;
                    file.sync_all().map_err(io_error)?;
                    self.root_handle.sync_all().map_err(io_error)?;
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(io_error(error)),
            }
        }
        if std::fs::read(marker).map_err(io_error)? != b"cached-filesystem-v1\n" {
            return Err(Error::Invalid(
                "shared checkpoint cache layout mismatch".into(),
            ));
        }
        Ok(())
    }
    pub(super) fn object_path(&self, relative: &Path) -> Result<PathBuf> {
        self.ready()?;
        if relative.as_os_str().is_empty()
            || !relative
                .components()
                .all(|part| matches!(part, Component::Normal(_)))
        {
            return Err(Error::Invalid("unsafe filesystem object path".into()));
        }
        let mut path = self.pinned_root();
        for part in relative.components() {
            path.push(part);
            match std::fs::symlink_metadata(&path) {
                Ok(metadata) if metadata.file_type().is_symlink() => {
                    return Err(Error::Invalid("symlink in filesystem object path".into()));
                }
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(io_error(error)),
            }
        }
        Ok(path)
    }
    fn artifact_path(&self, artifact: &CheckpointArtifact) -> Result<PathBuf> {
        self.ready()?;
        if artifact.storage != self.alias || uuid::Uuid::parse_str(&artifact.location).is_err() {
            return Err(Error::Invalid(
                "artifact does not belong to filesystem store".into(),
            ));
        }
        Ok(self.root.join("artifacts").join(&artifact.location))
    }
    fn staged_id(&self, staged: &Path) -> Result<String> {
        self.direct_ready()?;
        let canonical = std::fs::canonicalize(staged).map_err(io_error)?;
        if canonical.parent() != Some(self.root.join("artifacts").as_path()) {
            return Err(Error::Invalid("capture outside filesystem store".into()));
        }
        let id = canonical
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| Error::Invalid("invalid capture identity".into()))?;
        uuid::Uuid::parse_str(id).map_err(io_error)?;
        Ok(id.into())
    }
    /// Called only by Coordinator after closing new references and checking all owners.
    pub async fn reclaim(&self, artifact: &CheckpointArtifact) -> Result<()> {
        self.artifact_path(artifact)?;
        let root = self.pinned_root();
        let path = root.join("artifacts").join(&artifact.location);
        let manifest = self
            .pinned_root()
            .join("manifests")
            .join(format!("{}.json", artifact.location));
        tokio::task::spawn_blocking(move || {
            for p in [manifest] {
                match std::fs::remove_file(p) {
                    Ok(()) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => return Err(io_error(e)),
                }
            }
            match std::fs::symlink_metadata(&path) {
                Ok(m) if m.file_type().is_symlink() => {
                    Err(Error::Invalid("artifact directory is a symlink".into()))
                }
                Ok(_) => std::fs::remove_dir_all(path).map_err(io_error),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(e) => Err(io_error(e)),
            }
        })
        .await
        .map_err(io_error)?
    }
}
pub(crate) fn sync_inventory(
    root: &Path,
    files: &mut Vec<(PathBuf, u64)>,
    path: &Path,
) -> Result<()> {
    for entry in std::fs::read_dir(path).map_err(io_error)? {
        let entry = entry.map_err(io_error)?;
        let kind = entry.file_type().map_err(io_error)?;
        if kind.is_dir() {
            sync_inventory(root, files, &entry.path())?;
        } else if kind.is_file() {
            let file = std::fs::File::open(entry.path()).map_err(io_error)?;
            file.sync_all().map_err(io_error)?;
            files.push((
                entry.path().strip_prefix(root).map_err(io_error)?.into(),
                file.metadata().map_err(io_error)?.len(),
            ));
        } else {
            return Err(Error::Invalid(
                "checkpoint contains symlink or special file".into(),
            ));
        }
    }
    Ok(())
}
#[async_trait]
impl CheckpointAccess for FilesystemCheckpointStore {
    fn storage_alias(&self) -> &str {
        &self.alias
    }
    fn validate_policy(&self, policy: adx_core::lifecycle::CheckpointStorage) -> Result<()> {
        self.direct_ready()?;
        if policy == adx_core::lifecycle::CheckpointStorage::Local {
            return Err(Error::Invalid(
                "local-only capture requires a local store".into(),
            ));
        }
        Ok(())
    }
    fn capture_references(&self, staged: &Path) -> Result<Vec<CheckpointArtifact>> {
        Ok(vec![CheckpointArtifact {
            storage: self.alias.clone(),
            location: self.staged_id(staged)?,
            size_bytes: 0,
        }])
    }
    async fn begin_capture(&self) -> Result<PathBuf> {
        self.direct_ready()?;
        let path = self
            .root
            .join("artifacts")
            .join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir(&path).map_err(io_error)?;
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700))
            .map_err(io_error)?;
        Ok(path)
    }
    async fn discard_staged(&self, staged: &Path) -> Result<()> {
        let id = self.staged_id(staged)?;
        if self
            .root
            .join("manifests")
            .join(format!("{id}.json"))
            .exists()
        {
            return Ok(());
        }
        // This private, unpublished capture has no global consumer; caller has stopped its writer.
        std::fs::remove_dir_all(staged).map_err(io_error)
    }
    async fn retain_staged(&self, staged: &Path) -> Result<CheckpointArtifact> {
        self.seal(staged).await
    }
    async fn seal(&self, staged: &Path) -> Result<CheckpointArtifact> {
        let id = self.staged_id(staged)?;
        let staged = staged.to_owned();
        let root = self.root.clone();
        let alias = self.alias.clone();
        tokio::task::spawn_blocking(move || {
            let size = scan(&staged)?;
            if size == 0 {
                return Err(Error::Invalid("empty checkpoint".into()));
            }
            let mut files = Vec::new();
            sync_inventory(&staged, &mut files, &staged)?;
            files.sort();
            let bytes = serde_json::to_vec(&Manifest {
                version: 1,
                size_bytes: size,
                files,
            })
            .map_err(io_error)?;
            let target = root.join("manifests").join(format!("{id}.json"));
            if target.exists() {
                if std::fs::read(&target).map_err(io_error)? != bytes {
                    return Err(Error::Conflict);
                }
            } else {
                use std::io::Write;
                let tmp = root
                    .join("manifests")
                    .join(format!(".{id}-{}", uuid::Uuid::new_v4()));
                let mut file = std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&tmp)
                    .map_err(io_error)?;
                file.write_all(&bytes).map_err(io_error)?;
                file.sync_all().map_err(io_error)?;
                std::fs::rename(&tmp, &target).map_err(io_error)?;
            }
            Ok(CheckpointArtifact {
                storage: alias,
                location: id,
                size_bytes: size,
            })
        })
        .await
        .map_err(io_error)?
    }
    async fn prepare_restore(&self, artifact: &CheckpointArtifact) -> Result<RestoreAccess> {
        self.direct_ready()?;
        let path = self.artifact_path(artifact)?;
        if artifact.size_bytes == 0 {
            return Err(Error::Invalid(
                "incomplete capture cannot be restored".into(),
            ));
        }
        use tokio::io::AsyncReadExt;
        let manifest_file = tokio::fs::File::open(
            self.root
                .join("manifests")
                .join(format!("{}.json", artifact.location)),
        )
        .await
        .map_err(io_error)?;
        let mut bytes = Vec::new();
        manifest_file
            .take(4 * 1024 * 1024 + 1)
            .read_to_end(&mut bytes)
            .await
            .map_err(io_error)?;
        if bytes.len() > 4 * 1024 * 1024 {
            return Err(Error::Invalid("manifest too large".into()));
        }
        let manifest: Manifest = serde_json::from_slice(&bytes).map_err(io_error)?;
        if manifest.version != 1 || manifest.size_bytes != artifact.size_bytes {
            return Err(Error::Conflict);
        }
        let canonical = tokio::fs::canonicalize(&path).await.map_err(io_error)?;
        if canonical != path {
            return Err(Error::Invalid("artifact directory is a symlink".into()));
        }
        let mut seen = std::collections::BTreeSet::new();
        let mut total = 0u64;
        // No content scan or copying: the filesystem resolves data on demand.
        for (file, size) in &manifest.files {
            if file.as_os_str().is_empty()
                || !file.components().all(|c| matches!(c, Component::Normal(_)))
            {
                return Err(Error::Invalid("invalid manifest path".into()));
            }
            if !seen.insert(file) {
                return Err(Error::Invalid("duplicate manifest file".into()));
            }
            let resolved = tokio::fs::canonicalize(path.join(file))
                .await
                .map_err(io_error)?;
            if resolved != path.join(file) {
                return Err(Error::Invalid("symlink in artifact path".into()));
            }
            total = total.checked_add(*size).ok_or(Error::Conflict)?;
            let m = tokio::fs::symlink_metadata(path.join(file))
                .await
                .map_err(io_error)?;
            if !m.is_file() || m.len() != *size {
                return Err(Error::Conflict);
            }
        }
        if total != artifact.size_bytes {
            return Err(Error::Conflict);
        }
        Ok(RestoreAccess::local(path))
    }
    async fn validate_artifact(&self, artifact: &CheckpointArtifact) -> Result<()> {
        self.prepare_restore(artifact).await.map(|_| ())
    }
    async fn release(&self, _: &CheckpointArtifact) -> Result<()> {
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    fn cached_filesystem(shared: &Path, local: &Path) -> crate::ObjectCheckpointStore {
        let filesystem = FilesystemCheckpointStore::open(
            "juicefs-test".into(),
            shared.into(),
            "snapshots".into(),
            false,
        )
        .unwrap();
        crate::ObjectCheckpointStore::from_filesystem(filesystem, local.into(), 1024 * 1024)
            .unwrap()
    }

    #[test]
    fn cached_filesystem_rejects_cache_anywhere_inside_shared_mount() {
        let shared = tempfile::tempdir().unwrap();
        let filesystem = FilesystemCheckpointStore::open(
            "juicefs-test".into(),
            shared.path().into(),
            "snapshots".into(),
            false,
        )
        .unwrap();
        assert!(crate::ObjectCheckpointStore::from_filesystem(
            filesystem,
            shared.path().join("other-cache"),
            1024,
        )
        .is_err());
    }

    #[tokio::test]
    async fn cached_namespace_cannot_mix_with_direct_capture_layout() {
        let shared = tempfile::tempdir().unwrap();
        let local = tempfile::tempdir().unwrap();
        let cached = cached_filesystem(shared.path(), local.path());
        let direct = FilesystemCheckpointStore::open(
            "juicefs-test".into(),
            shared.path().into(),
            "snapshots".into(),
            false,
        )
        .unwrap();
        assert!(direct.begin_capture().await.is_err());
        drop(cached);
        let legacy_shared = tempfile::tempdir().unwrap();
        let legacy = FilesystemCheckpointStore::open(
            "juicefs-test".into(),
            legacy_shared.path().into(),
            "snapshots".into(),
            false,
        )
        .unwrap();
        legacy.begin_capture().await.unwrap();
        assert!(
            crate::ObjectCheckpointStore::from_filesystem(legacy, local.path().into(), 1024,)
                .is_err()
        );
    }

    #[tokio::test]
    async fn replaced_shared_namespace_never_receives_a_checkpoint() {
        let shared = tempfile::tempdir().unwrap();
        let local = tempfile::tempdir().unwrap();
        let source = cached_filesystem(shared.path(), local.path());
        let captured = source.begin_capture().await.unwrap();
        std::fs::write(captured.join("checkpoint.img"), b"memory").unwrap();
        std::fs::rename(
            shared.path().join("snapshots"),
            shared.path().join("old-snapshots"),
        )
        .unwrap();
        std::fs::create_dir(shared.path().join("snapshots")).unwrap();
        assert!(source.seal(&captured).await.is_err());
        assert_eq!(
            std::fs::read_dir(shared.path().join("snapshots"))
                .unwrap()
                .count(),
            0
        );
        assert!(captured.join("checkpoint.img").exists());
    }

    #[test]
    fn distributed_filesystem_accepts_optional_node_local_cache() {
        let config = serde_json::json!({
            "kind": "distributed_fs", "alias": "shared-checkpoints",
            "mount_root": "/mnt/shared", "prefix": "adx/checkpoints",
            "local_cache": {"root": "/home/adx/checkpoints", "budget_bytes": 53687091200_u64,
                "max_entries": 500}
        });
        assert!(serde_json::from_value::<crate::StorageConfig>(config).is_ok());
    }

    #[tokio::test]
    async fn cached_filesystem_restores_after_source_disk_loss_and_pins_local_inputs() {
        let shared = tempfile::tempdir().unwrap();
        let source_root = tempfile::tempdir().unwrap();
        let source = cached_filesystem(shared.path(), source_root.path());
        let captured = source.begin_capture().await.unwrap();
        assert!(captured.starts_with(source_root.path()));
        std::fs::create_dir(captured.join("filestores")).unwrap();
        std::fs::write(captured.join("checkpoint.img"), b"memory-state").unwrap();
        std::fs::write(
            captured.join("filestores/filestores.json"),
            b"filestore-manifest",
        )
        .unwrap();
        let artifact = source.seal(&captured).await.unwrap();
        let persisted = shared
            .path()
            .join("snapshots/artifacts")
            .join(&artifact.location);
        assert!(persisted.join("manifest.json").is_file());
        drop(source);
        drop(source_root);
        let destination_root = tempfile::tempdir().unwrap();
        let destination = cached_filesystem(shared.path(), destination_root.path());
        let access = destination.prepare_restore(&artifact).await.unwrap();
        assert!(access.starts_with(destination_root.path()));
        assert_eq!(
            std::fs::read(access.join("checkpoint.img")).unwrap(),
            b"memory-state"
        );
        assert_eq!(
            std::fs::read(access.join("filestores/filestores.json")).unwrap(),
            b"filestore-manifest"
        );
        destination.evict_unreferenced(0).await.unwrap();
        assert!(access.join("checkpoint.img").exists());
        assert!(destination.release(&artifact).await.is_err());
        drop(access);
        destination.release(&artifact).await.unwrap();
        assert!(
            persisted.exists(),
            "node cache release must preserve shared artifacts"
        );
        let coordinator = FilesystemCheckpointStore::open(
            "juicefs-test".into(),
            shared.path().into(),
            "snapshots".into(),
            false,
        )
        .unwrap();
        coordinator.reclaim(&artifact).await.unwrap();
        coordinator.reclaim(&artifact).await.unwrap();
        assert!(!persisted.exists());
    }

    #[tokio::test]
    async fn cached_filesystem_preserves_sparse_files_and_rejects_corrupt_shared_data() {
        use std::io::{Seek, SeekFrom, Write};
        use std::os::unix::fs::MetadataExt;
        let shared = tempfile::tempdir().unwrap();
        let source_root = tempfile::tempdir().unwrap();
        let source = cached_filesystem(shared.path(), source_root.path());
        let captured = source.begin_capture().await.unwrap();
        let mut sparse = std::fs::File::create(captured.join("filestore.bin")).unwrap();
        sparse.write_all(b"head").unwrap();
        sparse.seek(SeekFrom::Start(8 * 1024 * 1024)).unwrap();
        sparse.write_all(b"tail").unwrap();
        sparse.set_len(16 * 1024 * 1024).unwrap();
        drop(sparse);
        let artifact = source.seal(&captured).await.unwrap();
        let remote_file = shared
            .path()
            .join("snapshots/artifacts")
            .join(&artifact.location)
            .join("files/filestore.bin");
        let metadata = std::fs::metadata(&remote_file).unwrap();
        assert!(
            metadata.blocks() * 512 < metadata.len() / 2,
            "shared transfer must retain sparse extents"
        );
        let destination_root = tempfile::tempdir().unwrap();
        let destination = cached_filesystem(shared.path(), destination_root.path());
        let access = destination.prepare_restore(&artifact).await.unwrap();
        let metadata = std::fs::metadata(access.join("filestore.bin")).unwrap();
        assert!(
            metadata.blocks() * 512 < metadata.len() / 2,
            "restore transfer must retain sparse extents"
        );
        drop(access);
        destination.release(&artifact).await.unwrap();
        std::fs::write(remote_file, b"corrupted").unwrap();
        assert!(destination.prepare_restore(&artifact).await.is_err());
        assert!(!destination_root
            .path()
            .join("cache")
            .join(&artifact.location)
            .exists());
    }

    #[tokio::test]
    async fn cached_filesystem_failed_publication_keeps_local_recovery_files() {
        let shared = tempfile::tempdir().unwrap();
        let source_root = tempfile::tempdir().unwrap();
        let source = cached_filesystem(shared.path(), source_root.path());
        let captured = source.begin_capture().await.unwrap();
        std::fs::write(captured.join("checkpoint.img"), b"memory-state").unwrap();
        std::fs::remove_dir_all(shared.path().join("snapshots/artifacts")).unwrap();
        std::fs::write(
            shared.path().join("snapshots/artifacts"),
            b"not-a-directory",
        )
        .unwrap();
        assert!(source.seal(&captured).await.is_err());
        let retained = source.retain_staged(&captured).await.unwrap();
        let access = source.prepare_restore(&retained).await.unwrap();
        assert_eq!(
            std::fs::read(access.join("checkpoint.img")).unwrap(),
            b"memory-state"
        );
    }

    #[test]
    fn shared_filesystem_configuration_is_independent_of_mounted_implementation() {
        let config = serde_json::json!({"kind":"distributed_fs", "alias":"shared-checkpoints",
            "mount_root":"/mnt/shared", "prefix":"adx/checkpoints"});
        assert!(serde_json::from_value::<crate::StorageConfig>(config.clone()).is_ok());
        assert!(serde_json::from_value::<crate::reclaim::ReclaimerConfig>(config.clone()).is_ok());
        let mut invalid = config;
        invalid["filesystem"] = serde_json::json!("juicefs");
        assert!(serde_json::from_value::<crate::StorageConfig>(invalid.clone()).is_err());
        assert!(serde_json::from_value::<crate::reclaim::ReclaimerConfig>(invalid).is_err());
    }

    #[tokio::test]
    async fn filesystem_keeps_paths_and_node_release_never_deletes_shared_data() {
        let tmp = tempfile::tempdir().unwrap();
        let store = FilesystemCheckpointStore::open(
            "afs-test".into(),
            tmp.path().into(),
            "snapshots".into(),
            false,
        )
        .unwrap();
        let captured = store.begin_capture().await.unwrap();
        std::fs::write(captured.join("memory"), b"memory").unwrap();
        let artifact = store.seal(&captured).await.unwrap();
        assert_eq!(store.seal(&captured).await.unwrap(), artifact);
        let second = FilesystemCheckpointStore::open(
            "afs-test".into(),
            tmp.path().into(),
            "snapshots".into(),
            false,
        )
        .unwrap();
        let access = second.prepare_restore(&artifact).await.unwrap();
        assert_eq!(&*access, captured);
        store.release(&artifact).await.unwrap();
        assert!(captured.exists());
        second.reclaim(&artifact).await.unwrap();
        second.reclaim(&artifact).await.unwrap();
        assert!(!captured.exists());
    }
    #[tokio::test]
    async fn coordinator_reclaims_an_abandoned_capture_reservation() {
        let tmp = tempfile::tempdir().unwrap();
        let store = FilesystemCheckpointStore::open(
            "afs-test".into(),
            tmp.path().into(),
            "snapshots".into(),
            false,
        )
        .unwrap();
        let path = store.begin_capture().await.unwrap();
        let reserved = store.capture_references(&path).unwrap().remove(0);
        std::fs::write(path.join("partial-memory"), b"incomplete").unwrap();
        assert!(store.prepare_restore(&reserved).await.is_err());
        store.reclaim(&reserved).await.unwrap();
        store.reclaim(&reserved).await.unwrap();
        assert!(!path.exists());
    }
    #[tokio::test]
    async fn incomplete_capture_and_symlinked_inputs_are_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        let store = FilesystemCheckpointStore::open(
            "afs-test".into(),
            tmp.path().into(),
            "snapshots".into(),
            false,
        )
        .unwrap();
        let path = store.begin_capture().await.unwrap();
        let reserved = store.capture_references(&path).unwrap().remove(0);
        assert!(store.prepare_restore(&reserved).await.is_err());
        std::fs::create_dir(path.join("nested")).unwrap();
        std::fs::write(path.join("nested/memory"), b"memory").unwrap();
        let artifact = store.seal(&path).await.unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::fs::write(outside.path().join("memory"), b"memory").unwrap();
        std::fs::remove_dir_all(path.join("nested")).unwrap();
        std::os::unix::fs::symlink(outside.path(), path.join("nested")).unwrap();
        assert!(store.prepare_restore(&artifact).await.is_err());
        assert!(store.seal(&path).await.is_err());
    }
    #[tokio::test]
    async fn shared_namespace_identity_and_manifest_size_are_checked() {
        let tmp = tempfile::tempdir().unwrap();
        let store = FilesystemCheckpointStore::open(
            "afs-test".into(),
            tmp.path().into(),
            "snapshots".into(),
            false,
        )
        .unwrap();
        assert!(FilesystemCheckpointStore::open(
            "other".into(),
            tmp.path().into(),
            "snapshots".into(),
            false
        )
        .is_err());
        let path = store.begin_capture().await.unwrap();
        std::fs::write(path.join("memory"), b"memory").unwrap();
        let mut artifact = store.seal(&path).await.unwrap();
        artifact.size_bytes += 1;
        assert!(store.prepare_restore(&artifact).await.is_err());
        let manifest = store
            .root
            .join("manifests")
            .join(format!("{}.json", artifact.location));
        std::fs::write(manifest, vec![b' '; 4 * 1024 * 1024 + 1]).unwrap();
        assert!(store.prepare_restore(&artifact).await.is_err());
    }
    #[test]
    fn missing_afs_mount_never_falls_back_to_local_directory() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(FilesystemCheckpointStore::new(
            "afs-test".into(),
            tmp.path().into(),
            "snapshots".into()
        )
        .is_err());
    }
}
