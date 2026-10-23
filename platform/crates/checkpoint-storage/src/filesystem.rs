//! Direct filesystem access: capture and restore keep the same pathname.
mod mount;
use super::{io_error, scan, CheckpointAccess, RestoreAccess};
use adx_core::{CheckpointArtifact, Error, Result};
use async_trait::async_trait;
use mount::MountGuard;
use serde::{Deserialize, Serialize};
use std::path::{Component, Path, PathBuf};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    version: u32,
    size_bytes: u64,
    files: Vec<(PathBuf, u64)>,
}

pub struct FilesystemCheckpointStore {
    alias: String,
    root: PathBuf,
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
        Ok(Self {
            alias,
            root,
            mount_guard,
        })
    }
    fn ready(&self) -> Result<()> {
        if let Some(guard) = &self.mount_guard {
            guard.verify()?;
        }
        Ok(())
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
        self.ready()?;
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
        let path = self.artifact_path(artifact)?;
        let manifest = self
            .root
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
        self.ready()?;
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
            scope: String::new(),
            location: self.staged_id(staged)?,
            size_bytes: 0,
        }])
    }
    async fn begin_capture(&self) -> Result<PathBuf> {
        self.ready()?;
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
                scope: String::new(),
                location: id,
                size_bytes: size,
            })
        })
        .await
        .map_err(io_error)?
    }
    async fn prepare_restore(&self, artifact: &CheckpointArtifact) -> Result<RestoreAccess> {
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
