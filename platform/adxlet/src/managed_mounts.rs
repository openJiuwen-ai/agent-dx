//! Request-scoped creation of Local bind source directories.
use adx_core::{
    execution::{CreateOptions, StorageSource},
    EnvironmentSpec, Error, Result,
};
use std::fs::{DirBuilder, File, OpenOptions};
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};

fn unavailable(error: impl std::fmt::Display) -> Error {
    Error::Unavailable(format!("Local mount directory: {error}"))
}

/// Prepare explicitly opted-in sources before start or restore. No Agent or user
/// identity is interpreted here. Existing directories retain data and ownership.
/// Missing parents, symlinks and I/O failures return an error; only the final
/// directory is created, so a missing storage mount cannot become a directory tree.
pub async fn prepare(spec: &EnvironmentSpec) -> Result<()> {
    let mut sources = Vec::new();
    for mount in &spec.execution.mounts {
        if mount.create_if_missing {
            mount.validate()?;
            if let StorageSource::Local(source) = &mount.source {
                sources.push((PathBuf::from(source), mount.create_options.clone()));
            }
        }
    }
    if sources.is_empty() {
        return Ok(());
    }
    tokio::task::spawn_blocking(move || {
        for (source, options) in sources {
            prepare_directory(&source, options.as_ref())?;
        }
        Ok(())
    })
    .await
    .map_err(unavailable)?
}

fn open_directory(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
        .open(path)
}

fn prepare_directory(source: &Path, options: Option<&CreateOptions>) -> Result<()> {
    if !source.is_absolute()
        || source
            .components()
            .any(|part| !matches!(part, Component::RootDir | Component::Normal(_)))
    {
        return Err(Error::Invalid(
            "Local directory creation requires an absolute path without traversal".into(),
        ));
    }
    let parent = source
        .parent()
        .ok_or_else(|| Error::Invalid("Local source must have a parent directory".into()))?;
    let name = source
        .file_name()
        .ok_or_else(|| Error::Invalid("Local source directory name required".into()))?;
    let mut directory = open_directory(Path::new("/")).map_err(unavailable)?;
    for component in parent.components() {
        if let Component::Normal(name) = component {
            // The held descriptor anchors each lookup. O_NOFOLLOW prevents
            // symlink traversal in both parents and the final source directory.
            let child =
                PathBuf::from(format!("/proc/self/fd/{}", directory.as_raw_fd())).join(name);
            directory = open_directory(&child).map_err(unavailable)?;
        }
    }
    let child = PathBuf::from(format!("/proc/self/fd/{}", directory.as_raw_fd())).join(name);
    match open_directory(&child) {
        Ok(_) => return Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(unavailable(error)),
    }
    if let Some(options) = options {
        return create_with_options(&child, options);
    }
    match DirBuilder::new().mode(0o755).create(&child) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(unavailable(error)),
    }
    open_directory(&child).map_err(unavailable)?;
    Ok(())
}

// Only the caller that creates the final directory applies creation attributes.
// Existing directories are never renamed, removed or changed. An initialization
// failure leaves the directory in place and fails this start/restore attempt.
fn create_with_options(child: &Path, options: &CreateOptions) -> Result<()> {
    let mode = options.mode_bits()?;
    match DirBuilder::new().mode(0o700).create(child) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            open_directory(child).map_err(unavailable)?;
            return Ok(());
        }
        Err(error) => return Err(unavailable(error)),
    }
    let directory = open_directory(child).map_err(unavailable)?;
    // SAFETY: directory owns a live directory descriptor; uid/gid were checked
    // against the reserved -1 sentinel and have the platform uid_t/gid_t types.
    if unsafe { libc::fchown(directory.as_raw_fd(), options.uid, options.gid) } != 0 {
        return Err(unavailable(io::Error::last_os_error()));
    }
    directory
        .set_permissions(std::fs::Permissions::from_mode(mode))
        .map_err(unavailable)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::os::unix::fs::{symlink, MetadataExt};
    use std::os::unix::process::CommandExt;
    use std::process::Command;

    fn current_options() -> CreateOptions {
        let metadata = fs::metadata("/proc/self").unwrap();
        CreateOptions {
            uid: metadata.uid(),
            gid: metadata.gid(),
            mode: "0700".into(),
        }
    }

    #[test]
    fn first_use_creates_directory_and_reuse_preserves_data() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("w-first");
        let mut options = current_options();
        // Root test runners exercise a different, non-root owner as used by Orbit.
        if options.uid == 0 {
            options.uid = 10001;
            options.gid = 10001;
        }
        prepare_directory(&source, Some(&options)).unwrap();
        let metadata = fs::metadata(&source).unwrap();
        assert_eq!(
            (metadata.uid(), metadata.gid(), metadata.mode() & 0o777),
            (options.uid, options.gid, 0o700)
        );
        fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o755)).unwrap();
        let status = Command::new("sh")
            .args(["-c", "printf persistent > \"$1/state\"", "mount-write"])
            .arg(&source)
            .uid(options.uid)
            .gid(options.gid)
            .status()
            .unwrap();
        assert!(
            status.success(),
            "the configured directory owner must be able to write"
        );
        let mut different = current_options();
        different.mode = "0755".into();
        prepare_directory(&source, Some(&different)).unwrap();
        assert_eq!(fs::metadata(&source).unwrap().mode() & 0o777, 0o700);
        assert_eq!(fs::metadata(&source).unwrap().uid(), options.uid);
        assert_eq!(fs::metadata(&source).unwrap().gid(), options.gid);
        assert_eq!(
            fs::read_to_string(source.join("state")).unwrap(),
            "persistent"
        );
    }

    #[test]
    fn missing_parent_is_not_created() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("unmounted/users/w-first");
        assert!(prepare_directory(&source, None).is_err());
        assert!(!temp.path().join("unmounted").exists());
    }

    #[test]
    fn rejects_symlink_parents_sources_files_and_traversal() {
        let temp = tempfile::tempdir().unwrap();
        let outside = temp.path().join("outside");
        fs::create_dir(&outside).unwrap();
        symlink(&outside, temp.path().join("link")).unwrap();
        fs::write(temp.path().join("file"), "data").unwrap();
        for source in [
            temp.path().join("link"),
            temp.path().join("link/new"),
            temp.path().join("file"),
            temp.path().join("../escape"),
        ] {
            assert!(prepare_directory(&source, None).is_err());
            assert!(prepare_directory(&source, Some(&current_options())).is_err());
        }
        assert_eq!(fs::read_dir(outside).unwrap().count(), 0);
    }

    #[test]
    fn concurrent_preparation_is_idempotent() {
        let temp = tempfile::tempdir().unwrap();
        let options = current_options();
        std::thread::scope(|scope| {
            let threads: Vec<_> = (0..8)
                .map(|_| {
                    scope.spawn(|| {
                        prepare_directory(&temp.path().join("shared"), Some(&options)).unwrap();
                    })
                })
                .collect();
            for thread in threads {
                thread.join().unwrap();
            }
        });
        let metadata = fs::metadata(temp.path().join("shared")).unwrap();
        assert_eq!(
            (metadata.uid(), metadata.gid(), metadata.mode() & 0o777),
            (options.uid, options.gid, 0o700)
        );
        assert_eq!(fs::read_dir(temp.path()).unwrap().count(), 1);
    }

    #[tokio::test]
    async fn creation_requires_explicit_mount_opt_in() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("workspace");
        let mut spec: EnvironmentSpec = serde_json::from_value(serde_json::json!({
            "id":"test", "tenant_id":"test", "image":"image", "runtime_class":"runsc",
            "resources":{"cpu_millis":1000,"memory_bytes":1048576,"disk_bytes":0}, "priority":0
        }))
        .unwrap();
        spec.execution.mounts.push(adx_core::execution::Mount {
            kind: "bind".into(),
            target: "/data".into(),
            options: vec!["rw".into()],
            source: StorageSource::Local(path.to_str().unwrap().into()),
            create_if_missing: false,
            create_options: None,
        });
        prepare(&spec).await.unwrap();
        assert!(!path.exists());
        spec.execution.mounts[0].create_if_missing = true;
        spec.execution.mounts[0].create_options = Some(current_options());
        prepare(&spec).await.unwrap();
        assert!(path.is_dir());
        assert_eq!(fs::metadata(path).unwrap().mode() & 0o777, 0o700);
        let legacy = temp.path().join("legacy");
        spec.execution.mounts[0].source = StorageSource::Local(legacy.to_str().unwrap().into());
        spec.execution.mounts[0].create_options = None;
        prepare(&spec).await.unwrap();
        assert!(legacy.is_dir());
        assert_eq!(fs::metadata(legacy).unwrap().uid(), current_options().uid);
    }
}
