//! Request-scoped creation of Local bind source directories.
use adx_core::{
    execution::{CreateOptions, StorageSource},
    EnvironmentSpec, Error, Result,
};
use std::fs::{DirBuilder, File, OpenOptions};
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};

/// Confirm deployment storage is mounted before an initialization intent is acquired.
pub(crate) async fn verify_volume_backend(root: &str, backend: &str) -> Result<()> {
    let root = PathBuf::from(root);
    let backend = backend.to_owned();
    tokio::task::spawn_blocking(move || verify_backend(&root, &backend))
        .await
        .map_err(unavailable)?
}

pub(crate) async fn prepare_volume_directory(
    root: &str,
    backend: &str,
    relative: &str,
    create: bool,
    verify_existing: bool,
    options: Option<&CreateOptions>,
) -> Result<PathBuf> {
    let root = PathBuf::from(root);
    let backend = backend.to_owned();
    let relative = relative.to_owned();
    let options = options.cloned();
    tokio::task::spawn_blocking(move || {
        verify_backend(&root, &backend)?;
        let source = adx_core::volumes::VolumeSource::Directory {
            storage_domain: "granted".into(),
            path: relative.clone(),
        };
        source.validate()?;
        let source = root.join(relative);
        if create {
            prepare_directory(&source, options.as_ref())?;
            if verify_existing {
                verify_creation_attributes(&source, options.as_ref())?;
            }
        } else {
            checked_directory(&source).map_err(unavailable)?;
        }
        Ok(source)
    })
    .await
    .map_err(unavailable)?
}

/// Report only currently mounted configured backends; never create directories.
/// A failed observation returns an error so the caller withdraws the complete view.
pub async fn mounted_storage(
    configured: std::collections::BTreeMap<String, adx_core::volumes::StorageMount>,
) -> Result<std::collections::BTreeMap<String, adx_core::volumes::StorageMount>> {
    if configured.is_empty() {
        return Ok(std::collections::BTreeMap::new());
    }
    tokio::task::spawn_blocking(move || {
        let mounts = std::fs::read_to_string("/proc/self/mountinfo").map_err(unavailable)?;
        let mut available = std::collections::BTreeMap::new();
        for (domain, mount) in configured {
            mount.validate()?;
            if verify_mounted_backend(Path::new(&mount.root), &mount.backend_id, &mounts).is_ok() {
                available.insert(domain, mount);
            }
        }
        Ok(available)
    })
    .await
    .map_err(unavailable)?
}

// A Pending replay can observe a directory left by an interrupted initializer.
// Only the mkdir winner sets attributes; retries verify them without rewriting data.
fn verify_creation_attributes(source: &Path, options: Option<&CreateOptions>) -> Result<()> {
    let Some(options) = options else {
        return Ok(());
    };
    let metadata = checked_directory(source)
        .map_err(unavailable)?
        .metadata()
        .map_err(unavailable)?;
    if metadata.uid() != options.uid
        || metadata.gid() != options.gid
        || metadata.mode() & 0o7777 != options.mode_bits()?
    {
        return Err(Error::Invalid(
            "Volume initialization directory owner, group or permissions do not match".into(),
        ));
    }
    Ok(())
}

fn mount_path(value: &str) -> String {
    value
        .replace("\\040", " ")
        .replace("\\011", "\t")
        .replace("\\012", "\n")
        .replace("\\134", "\\")
}

fn verify_backend(root: &Path, backend: &str) -> Result<()> {
    let mounts = std::fs::read_to_string("/proc/self/mountinfo").map_err(unavailable)?;
    verify_mounted_backend(root, backend, &mounts)
}

fn verify_mounted_backend(root: &Path, backend: &str, mounts: &str) -> Result<()> {
    checked_directory(root).map_err(unavailable)?;
    let mounted = mounts
        .lines()
        .filter_map(|line| {
            let fields: Vec<_> = line.split_whitespace().collect();
            let separator = fields.iter().position(|field| *field == "-")?;
            let path = PathBuf::from(mount_path(fields.get(4)?));
            root.starts_with(&path)
                .then(|| {
                    fields
                        .get(separator + 2)
                        .map(|source| (path, mount_path(source)))
                })
                .flatten()
        })
        .max_by_key(|(path, _)| path.components().count());
    if mounted.is_none_or(|(_, source)| source != backend) {
        return Err(Error::Unavailable(
            "Volume storage backend is not mounted at its configured root".into(),
        ));
    }
    Ok(())
}

fn checked_directory(path: &Path) -> io::Result<File> {
    if !path.is_absolute()
        || path
            .components()
            .any(|part| !matches!(part, Component::RootDir | Component::Normal(_)))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "absolute directory without traversal required",
        ));
    }
    let mut directory = open_directory(Path::new("/"))?;
    for component in path.components() {
        if let Component::Normal(name) = component {
            let child =
                PathBuf::from(format!("/proc/self/fd/{}", directory.as_raw_fd())).join(name);
            directory = open_directory(&child)?;
        }
    }
    Ok(directory)
}

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
            if let StorageSource::Local(source) = mount.source.inline()? {
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

// These tests exercise the Linux /proc/self/fd descriptor traversal.
#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use std::fs;
    use std::os::unix::fs::symlink;
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

    #[tokio::test]
    async fn storage_reports_only_matching_existing_backends_and_withdraws_missing_roots() {
        use adx_core::volumes::StorageMount;
        use std::collections::BTreeMap;
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("mounted");
        fs::create_dir(&root).unwrap();
        let mounts = fs::read_to_string("/proc/self/mountinfo").unwrap();
        let backend = mounts
            .lines()
            .find_map(|line| {
                let fields: Vec<_> = line.split_whitespace().collect();
                (fields[4] == "/")
                    .then(|| fields[fields.iter().position(|f| *f == "-").unwrap() + 2].to_owned())
            })
            .unwrap();
        let configured = BTreeMap::from([
            (
                "shared".into(),
                StorageMount {
                    backend_id: backend.clone(),
                    root: root.to_str().unwrap().into(),
                },
            ),
            (
                "wrong".into(),
                StorageMount {
                    backend_id: "unmounted:/export".into(),
                    root: root.to_str().unwrap().into(),
                },
            ),
            (
                "missing".into(),
                StorageMount {
                    backend_id: backend,
                    root: temp.path().join("absent").to_str().unwrap().into(),
                },
            ),
        ]);
        let reported = mounted_storage(configured.clone()).await.unwrap();
        assert_eq!(
            reported.keys().map(String::as_str).collect::<Vec<_>>(),
            vec!["shared"]
        );
        assert!(!temp.path().join("absent").exists());
        fs::remove_dir(&root).unwrap();
        assert!(mounted_storage(configured).await.unwrap().is_empty());
        assert!(!root.exists());
    }

    #[tokio::test]
    async fn volume_backend_mismatch_and_symlinks_never_create_a_directory() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().to_str().unwrap();
        assert!(
            prepare_volume_directory(root, "unmounted:/export", "user", true, false, None)
                .await
                .is_err()
        );
        assert!(!temp.path().join("user").exists());
        symlink(temp.path(), temp.path().join("link")).unwrap();
        assert!(verify_volume_backend(
            temp.path().join("link").to_str().unwrap(),
            "unmounted:/export"
        )
        .await
        .is_err());
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
    fn initialization_retry_checks_owner_group_and_mode_without_rewriting_existing_data() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("pending");
        let options = current_options();
        fs::create_dir(&source).unwrap();
        fs::write(source.join("state"), "persistent").unwrap();
        fs::set_permissions(&source, fs::Permissions::from_mode(0o755)).unwrap();
        assert!(verify_creation_attributes(&source, Some(&options)).is_err());
        assert_eq!(fs::metadata(&source).unwrap().mode() & 0o777, 0o755);
        fs::set_permissions(&source, fs::Permissions::from_mode(0o700)).unwrap();
        verify_creation_attributes(&source, Some(&options)).unwrap();
        let mut wrong = options.clone();
        wrong.uid += 1;
        assert!(verify_creation_attributes(&source, Some(&wrong)).is_err());
        wrong = options.clone();
        wrong.gid += 1;
        assert!(verify_creation_attributes(&source, Some(&wrong)).is_err());
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
            source: StorageSource::Local(path.to_str().unwrap().into()).into(),
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
        spec.execution.mounts[0].source =
            StorageSource::Local(legacy.to_str().unwrap().into()).into();
        spec.execution.mounts[0].create_options = None;
        prepare(&spec).await.unwrap();
        assert!(legacy.is_dir());
        assert_eq!(fs::metadata(legacy).unwrap().uid(), current_options().uid);
    }
}
