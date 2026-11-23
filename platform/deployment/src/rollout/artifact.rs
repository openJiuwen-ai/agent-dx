use super::Submission;
pub(crate) mod remote;
use crate::Result;
use sha2::{Digest, Sha256};
use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::Path,
    time::Instant,
};

fn host_target() -> Result<String> {
    match std::env::consts::ARCH {
        "x86_64" | "aarch64" if std::env::consts::OS == "linux" => {
            Ok(format!("{}-unknown-linux-gnu", std::env::consts::ARCH))
        }
        _ => Err("unsupported Linux host architecture".into()),
    }
}

fn check(deadline: Instant) -> Result<()> {
    if Instant::now() >= deadline {
        return Err("preparation deadline exceeded".into());
    }
    Ok(())
}

pub(super) fn verify_file(
    file: &mut File,
    expected: &str,
    deadline: Instant,
    stopped: &std::sync::atomic::AtomicBool,
) -> Result<()> {
    use std::io::{Seek, SeekFrom};
    if !file.metadata()?.is_file() {
        return Err("regular executable required".into());
    }
    file.seek(SeekFrom::Start(0))?;
    let mut hash = Sha256::new();
    let mut bytes = [0; 65536];
    loop {
        check(deadline)?;
        if stopped.load(std::sync::atomic::Ordering::Acquire) {
            return Err("supervisor stopping".into());
        }
        let count = file.read(&mut bytes)?;
        if count == 0 {
            break;
        }
        hash.update(&bytes[..count]);
    }
    if format!("{:x}", hash.finalize()) != expected {
        return Err("pinned executable digest differs".into());
    }
    Ok(())
}

// Directory descriptors pin each traversed parent. O_NOFOLLOW applies to every
// component, including manifest and binary, rather than a racy canonicalize check.
pub(super) fn child(parent: &File, name: &str, directory: bool) -> Result<File> {
    use std::os::fd::{AsRawFd, FromRawFd};
    let name = std::ffi::CString::new(name)?;
    let flags = libc::O_RDONLY
        | libc::O_CLOEXEC
        | libc::O_NOFOLLOW
        | libc::O_NONBLOCK
        | if directory { libc::O_DIRECTORY } else { 0 };
    // SAFETY: parent remains alive; name is NUL terminated. A successful fd is
    // transferred exactly once into File below.
    let fd = unsafe { libc::openat(parent.as_raw_fd(), name.as_ptr(), flags) };
    if fd < 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    // SAFETY: openat returned a new owned descriptor.
    let file = unsafe { File::from_raw_fd(fd) };
    if !directory && !file.metadata()?.is_file() {
        return Err("regular artifact required".into());
    }
    Ok(file)
}

#[derive(serde::Deserialize)]
struct Manifest {
    schema_version: u32,
    commit: String,
    target: String,
    #[serde(deserialize_with = "unique_files")]
    files: std::collections::BTreeMap<String, String>,
}

fn unique_files<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<std::collections::BTreeMap<String, String>, D::Error> {
    struct Files;
    impl<'de> serde::de::Visitor<'de> for Files {
        type Value = std::collections::BTreeMap<String, String>;
        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("unique manifest file keys")
        }
        fn visit_map<M: serde::de::MapAccess<'de>>(
            self,
            mut map: M,
        ) -> std::result::Result<Self::Value, M::Error> {
            let mut files = std::collections::BTreeMap::new();
            while let Some((name, digest)) = map.next_entry::<String, String>()? {
                if files.insert(name, digest).is_some() {
                    return Err(serde::de::Error::custom("duplicate manifest file key"));
                }
            }
            Ok(files)
        }
    }
    deserializer.deserialize_map(Files)
}

#[cfg(test)]
pub(super) fn prepare(
    root: &Path,
    request: &Submission,
    output: &Path,
    deadline: Instant,
) -> Result<PreparedBinary> {
    prepare_cancellable(
        root,
        request,
        output,
        deadline,
        &std::sync::atomic::AtomicBool::new(false),
    )
}

pub(super) fn copy_owned(
    source: &mut File,
    output: &Path,
    stopped: &std::sync::atomic::AtomicBool,
    deadline: Instant,
) -> Result<()> {
    use std::io::{Seek, SeekFrom};
    if !source.metadata()?.is_file() {
        return Err("owned executable must be regular".into());
    }
    source.seek(SeekFrom::Start(0))?;
    let temporary = output.with_extension("partial");
    let mut target = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o700)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&temporary)?;
    let mut hash = Sha256::new();
    let mut buffer = [0u8; 128 * 1024];
    loop {
        check(deadline)?;
        if stopped.load(std::sync::atomic::Ordering::Acquire) {
            return Err("supervisor stopping".into());
        }
        let count = source.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hash.update(&buffer[..count]);
        target.write_all(&buffer[..count])?;
    }
    target.sync_all()?;
    drop(target);
    let expected = hash.finalize();
    let mut copy = File::open(&temporary)?;
    let mut hash = Sha256::new();
    loop {
        check(deadline)?;
        if stopped.load(std::sync::atomic::Ordering::Acquire) {
            return Err("supervisor stopping".into());
        }
        let count = copy.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hash.update(&buffer[..count]);
    }
    if hash.finalize() != expected {
        return Err("owned executable copy mismatch".into());
    }
    check(deadline)?;
    if stopped.load(std::sync::atomic::Ordering::Acquire) {
        return Err("supervisor stopping".into());
    }
    std::fs::rename(&temporary, output)?;
    File::open(output.parent().ok_or("missing parent")?)?.sync_all()?;
    Ok(())
}

pub(super) fn prepare_cancellable(
    root: &Path,
    request: &Submission,
    output: &Path,
    deadline: Instant,
    stopped: &std::sync::atomic::AtomicBool,
) -> Result<PreparedBinary> {
    request.validate()?;
    let root = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
        .open(root)?;
    let package = child(&root, &request.commit, true)?;
    prepare_package(
        &package,
        ExpectedBinary {
            role: request.role,
            commit: &request.commit,
            sha256: request.sha256.as_deref(),
        },
        output,
        deadline,
        stopped,
    )
}

pub(crate) struct PreparedBinary {
    pub(crate) commit: String,
    pub(crate) sha256: String,
}

pub(crate) struct ExpectedBinary<'a> {
    pub(crate) role: crate::config::Role,
    pub(crate) commit: &'a str,
    pub(crate) sha256: Option<&'a str>,
}

pub(crate) fn prepare_package(
    package: &File,
    expected: ExpectedBinary<'_>,
    output: &Path,
    deadline: Instant,
    stopped: &std::sync::atomic::AtomicBool,
) -> Result<PreparedBinary> {
    let ExpectedBinary {
        role,
        commit: expected_commit,
        sha256: expected_sha256,
    } = expected;
    let check = |deadline| -> Result<()> {
        if stopped.load(std::sync::atomic::Ordering::Acquire) {
            return Err("supervisor stopping".into());
        }
        check(deadline)
    };
    check(deadline)?;
    let mut manifest_bytes = Vec::new();
    let mut manifest_file = child(package, "manifest.json", false)?.take(4 * 1024 * 1024 + 1);
    let mut buffer = [0u8; 64 * 1024];
    loop {
        check(deadline)?;
        let count = manifest_file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        manifest_bytes.extend_from_slice(&buffer[..count]);
    }
    if manifest_bytes.len() > 4 * 1024 * 1024 {
        return Err("manifest too large".into());
    }
    let manifest: Manifest = serde_json::from_slice(&manifest_bytes)?;
    if manifest.files.iter().any(|(name, digest)| {
        let path = Path::new(name);
        path.is_absolute()
            || name.is_empty()
            || name.contains('\\')
            || name
                .split('/')
                .any(|part| part.is_empty() || part == "." || part == "..")
            || path
                .components()
                .any(|part| !matches!(part, std::path::Component::Normal(_)))
            || !super::hex(digest, 64)
    }) {
        return Err("invalid manifest file entry".into());
    }
    let relative = format!("bin/{}", role.binary());
    let digest = manifest
        .files
        .get(&relative)
        .ok_or("missing fixed binary")?;
    if manifest.schema_version != 1
        || !super::hex(&manifest.commit, 40)
        || manifest.commit != expected_commit
        || manifest.target != host_target()?
        || expected_sha256.is_some_and(|expected| expected != digest)
    {
        return Err("manifest identity or binary digest mismatch".into());
    }
    use std::os::fd::AsRawFd;
    let mut seen = std::collections::BTreeSet::new();
    let mut pending = vec![String::new()];
    let mut buffer = [0u8; 128 * 1024];
    // Queue names, not open directory handles or recursive stack frames. Reopen
    // every component from the pinned package with NOFOLLOW on each traversal.
    while let Some(prefix) = pending.pop() {
        check(deadline)?;
        let mut directory = package.try_clone()?;
        if !prefix.is_empty() {
            for component in prefix.split('/') {
                check(deadline)?;
                directory = child(&directory, component, true)?;
            }
        }
        for entry in std::fs::read_dir(format!("/proc/self/fd/{}", directory.as_raw_fd()))? {
            check(deadline)?;
            let entry = entry?;
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| "non-UTF8 artifact path")?;
            if name.is_empty() || name.contains(['/', '\\']) || matches!(name.as_str(), "." | "..")
            {
                return Err("invalid artifact path component".into());
            }
            let relative = if prefix.is_empty() {
                name.clone()
            } else {
                format!("{prefix}/{name}")
            };
            let kind = entry.file_type()?;
            if kind.is_dir() {
                pending.try_reserve(1)?;
                pending.push(relative);
            } else {
                let mut file = child(&directory, &name, false)?;
                if relative == "manifest.json" {
                    continue;
                }
                let expected = manifest
                    .files
                    .get(&relative)
                    .ok_or("unlisted artifact file")?;
                let mut hash = Sha256::new();
                loop {
                    check(deadline)?;
                    let count = file.read(&mut buffer)?;
                    if count == 0 {
                        break;
                    }
                    hash.update(&buffer[..count]);
                }
                if format!("{:x}", hash.finalize()) != *expected {
                    return Err("manifest file digest mismatch".into());
                }
                seen.insert(relative);
            }
        }
    }
    if seen.len() != manifest.files.len() {
        return Err("manifest file set mismatch".into());
    }
    let bin = child(package, "bin", true)?;
    let mut source = child(&bin, role.binary(), false)?;
    let temporary = output.with_extension("partial");
    let result = (|| -> Result<()> {
        let mut target = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&temporary)?;
        let mut hash = Sha256::new();
        let mut buffer = [0u8; 128 * 1024];
        loop {
            check(deadline)?;
            let count = source.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            hash.update(&buffer[..count]);
            target.write_all(&buffer[..count])?;
        }
        if format!("{:x}", hash.finalize()) != *digest {
            return Err("artifact digest mismatch".into());
        }
        target.sync_all()?;
        drop(target);
        let mut copy = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&temporary)?;
        let mut hash = Sha256::new();
        loop {
            check(deadline)?;
            let count = copy.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            hash.update(&buffer[..count]);
        }
        if format!("{:x}", hash.finalize()) != *digest {
            return Err("prepared copy digest mismatch".into());
        }
        std::fs::set_permissions(&temporary, std::fs::Permissions::from_mode(0o700))?;
        check(deadline)?;
        std::fs::rename(&temporary, output)?;
        File::open(output.parent().ok_or("missing artifact parent")?)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
        let _ = std::fs::remove_file(output);
    }
    result?;
    Ok(PreparedBinary {
        commit: manifest.commit,
        sha256: digest.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};
    use std::time::Duration;

    #[test]
    fn rollback_copy_uses_pinned_inode_and_stop_prevents_publication() {
        let root = tempfile::tempdir().unwrap();
        let binary = root.path().join("binary");
        std::fs::write(&binary, b"running old inode").unwrap();
        let mut pinned = File::open(&binary).unwrap();
        std::fs::remove_file(&binary).unwrap();
        std::fs::write(&binary, b"replacement at same commit path").unwrap();
        let output = root.path().join("rollback");
        let stopped = std::sync::atomic::AtomicBool::new(false);
        copy_owned(
            &mut pinned,
            &output,
            &stopped,
            Instant::now() + Duration::from_secs(900),
        )
        .unwrap();
        assert_eq!(std::fs::read(&output).unwrap(), b"running old inode");
        stopped.store(true, std::sync::atomic::Ordering::Release);
        let cancelled = root.path().join("cancelled");
        assert!(copy_owned(
            &mut pinned,
            &cancelled,
            &stopped,
            Instant::now() + Duration::from_secs(900)
        )
        .is_err());
        assert!(!cancelled.exists());
    }

    #[test]
    fn copies_verified_fixed_binary_and_rejects_digest_and_symlink() {
        let root = tempfile::tempdir().unwrap();
        let release = "a".repeat(40);
        let package = root.path().join(&release);
        std::fs::create_dir_all(package.join("bin")).unwrap();
        let bytes = b"prepared executable";
        let digest = format!("{:x}", Sha256::digest(bytes));
        std::fs::write(package.join("bin/adx-relay"), bytes).unwrap();
        std::fs::write(
            package.join("manifest.json"),
            serde_json::to_vec(&serde_json::json!({
                "schema_version":1, "commit":release, "target":host_target().unwrap(),
                "files":{"bin/adx-relay":digest}
            }))
            .unwrap(),
        )
        .unwrap();
        let request = super::super::Submission {
            component_config: None,
            task_id: "test".into(),
            role: crate::config::Role::Relay,
            selection: super::super::Selection::LocalManual,
            service_id: "relay".into(),
            commit: release,
            sha256: Some(digest),
            remote_archive: None,
            process: None,
        };
        let output = root.path().join("copy");
        prepare(
            root.path(),
            &request,
            &output,
            Instant::now() + Duration::from_secs(900),
        )
        .unwrap();
        assert_eq!(std::fs::read(&output).unwrap(), bytes);
        let mut derived = request.clone();
        derived.remote_archive = Some(super::super::RemoteArchive {
            url: "https://example.invalid/package.tar.gz".into(),
            archive_sha256: "a".repeat(64),
        });
        derived.sha256 = None;
        let original = derived.clone();
        let prepared = prepare(
            root.path(),
            &derived,
            &root.path().join("derived"),
            Instant::now() + Duration::from_secs(900),
        )
        .unwrap();
        assert_eq!(prepared.sha256, request.sha256.as_deref().unwrap());
        assert_eq!(derived, original);
        derived.remote_archive = None;
        assert!(derived.validate().is_err());
        let pinned = File::open(&package).unwrap();
        let wrong_identity = root.path().join("wrong-identity");
        assert!(prepare_package(
            &pinned,
            ExpectedBinary {
                role: crate::config::Role::Relay,
                commit: &"c".repeat(40),
                sha256: None
            },
            &wrong_identity,
            Instant::now() + Duration::from_secs(900),
            &std::sync::atomic::AtomicBool::new(false)
        )
        .is_err());
        assert!(!wrong_identity.exists());
        // Agreement between caller and manifest is insufficient if bytes differ.
        std::fs::write(package.join("bin/adx-relay"), b"corrupted").unwrap();
        assert!(prepare(
            root.path(),
            &request,
            &root.path().join("corrupted"),
            Instant::now() + Duration::from_secs(900)
        )
        .is_err());
        assert!(!root.path().join("corrupted").exists());
        let mut bad = request.clone();
        bad.sha256 = Some("0".repeat(64));
        assert!(prepare(
            root.path(),
            &bad,
            &root.path().join("bad"),
            Instant::now() + Duration::from_secs(900)
        )
        .is_err());
        std::fs::remove_file(package.join("bin/adx-relay")).unwrap();
        std::os::unix::fs::symlink(&output, package.join("bin/adx-relay")).unwrap();
        assert!(prepare(
            root.path(),
            &request,
            &root.path().join("link"),
            Instant::now() + Duration::from_secs(900)
        )
        .is_err());
    }

    #[test]
    fn expired_budget_never_creates_a_prepared_file() {
        let root = tempfile::tempdir().unwrap();
        let request = super::super::Submission {
            component_config: None,
            task_id: "test".into(),
            role: crate::config::Role::Relay,
            selection: super::super::Selection::LocalManual,
            service_id: "relay".into(),
            commit: "a".repeat(40),
            sha256: Some("b".repeat(64)),
            remote_archive: None,
            process: None,
        };
        let output = root.path().join("copy");
        assert!(prepare(root.path(), &request, &output, Instant::now()).is_err());
        assert!(!output.exists());
    }

    #[test]
    fn manifest_duplicate_keys_and_four_mib_limit_are_rejected() {
        let duplicate = format!(
            "{{\"schema_version\":1,\"commit\":\"{}\",\"target\":\"host\",\"files\":{{\"bin/adx-relay\":\"{}\",\"bin/adx-relay\":\"{}\"}}}}",
            "a".repeat(40), "b".repeat(64), "b".repeat(64)
        );
        assert!(serde_json::from_str::<Manifest>(&duplicate).is_err());
        let root = tempfile::tempdir().unwrap();
        let request = Submission {
            component_config: None,
            task_id: "manifest".into(),
            role: crate::config::Role::Relay,
            selection: super::super::Selection::LocalManual,
            service_id: "relay".into(),
            commit: "a".repeat(40),
            sha256: Some("b".repeat(64)),
            remote_archive: None,
            process: None,
        };
        let package = root.path().join(&request.commit);
        std::fs::create_dir(&package).unwrap();
        std::fs::write(
            package.join("manifest.json"),
            vec![b' '; 4 * 1024 * 1024 + 1],
        )
        .unwrap();
        assert!(prepare(
            root.path(),
            &request,
            &root.path().join("output"),
            Instant::now() + Duration::from_secs(900)
        )
        .is_err());
        assert!(!root.path().join("output").exists());
    }

    #[test]
    fn full_manifest_set_and_every_digest_are_required_before_publication() {
        let root = tempfile::tempdir().unwrap();
        let package = root.path().join("a".repeat(40));
        std::fs::create_dir_all(package.join("bin")).unwrap();
        std::fs::write(package.join("bin/adx-relay"), b"binary").unwrap();
        std::fs::write(package.join("other"), b"other").unwrap();
        let digest = format!("{:x}", Sha256::digest(b"binary"));
        let request = Submission {
            component_config: None,
            task_id: "manifest".into(),
            role: crate::config::Role::Relay,
            selection: super::super::Selection::LocalManual,
            service_id: "relay".into(),
            commit: "a".repeat(40),
            sha256: Some(digest.clone()),
            remote_archive: None,
            process: None,
        };
        for files in [
            serde_json::json!({"bin/adx-relay": digest}),
            serde_json::json!({"bin/adx-relay": digest, "other": "0".repeat(64)}),
            serde_json::json!({"bin/adx-relay": digest, "other": format!("{:x}", Sha256::digest(b"other")), "missing": "a".repeat(64)}),
        ] {
            std::fs::write(package.join("manifest.json"), serde_json::to_vec(&serde_json::json!({
                "schema_version": 1, "commit": request.commit, "target": host_target().unwrap(), "files": files,
            })).unwrap()).unwrap();
            assert!(prepare(
                root.path(),
                &request,
                &root.path().join("output"),
                Instant::now() + Duration::from_secs(900)
            )
            .is_err());
            assert!(!root.path().join("output").exists());
        }
    }

    #[test]
    fn deep_and_wide_packages_keep_full_manifest_validation() {
        for deep in [true, false] {
            let root = tempfile::tempdir().unwrap();
            let commit = "a".repeat(40);
            let package = root.path().join(&commit);
            std::fs::create_dir_all(package.join("bin")).unwrap();
            std::fs::write(package.join("bin/adx-relay"), b"binary").unwrap();
            let digest = format!("{:x}", Sha256::digest(b"binary"));
            let mut files =
                std::collections::BTreeMap::from([("bin/adx-relay".to_owned(), digest.clone())]);
            let mut relative = String::new();
            for index in 0..1024 {
                relative = if deep {
                    format!("{relative}d/")
                } else {
                    format!("d{index}/")
                };
                std::fs::create_dir_all(package.join(&relative)).unwrap();
                let name = format!("{relative}payload");
                std::fs::write(package.join(&name), b"payload").unwrap();
                files.insert(name, format!("{:x}", Sha256::digest(b"payload")));
            }
            let request = Submission {
                component_config: None,
                task_id: "tree".into(),
                role: crate::config::Role::Relay,
                selection: super::super::Selection::LocalManual,
                service_id: "relay".into(),
                commit,
                sha256: Some(digest),
                remote_archive: None,
                process: None,
            };
            let manifest_path = package.join("manifest.json");
            let write_manifest = |files: &std::collections::BTreeMap<String, String>| {
                std::fs::write(
                    &manifest_path,
                    serde_json::to_vec(&serde_json::json!({
                        "schema_version": 1, "commit": request.commit,
                        "target": host_target().unwrap(), "files": files,
                    }))
                    .unwrap(),
                )
                .unwrap();
            };
            write_manifest(&files);
            let deadline = Instant::now() + Duration::from_secs(900);
            prepare(root.path(), &request, &root.path().join("good"), deadline).unwrap();
            for mode in ["digest", "extra", "missing", "stopped"] {
                let mut changed = files.clone();
                match mode {
                    "digest" => {
                        changed.insert(format!("{relative}payload"), "0".repeat(64));
                    }
                    "extra" => {
                        changed.remove(&format!("{relative}payload"));
                    }
                    "missing" => {
                        changed.insert("missing".into(), "0".repeat(64));
                    }
                    _ => (),
                }
                write_manifest(&changed);
                let output = root.path().join(mode);
                assert!(prepare_cancellable(
                    root.path(),
                    &request,
                    &output,
                    deadline,
                    &std::sync::atomic::AtomicBool::new(mode == "stopped")
                )
                .is_err());
                assert!(!output.exists());
            }
        }
    }
}
