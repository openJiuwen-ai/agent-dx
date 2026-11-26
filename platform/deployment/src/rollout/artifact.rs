use super::Submission;
pub(crate) mod remote;
use crate::Result;
use sha2::{Digest, Sha256};
use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::Path,
    sync::atomic::{AtomicBool, Ordering},
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

fn check(deadline: Instant, stopped: &AtomicBool) -> Result<()> {
    if stopped.load(Ordering::Acquire) {
        return Err("supervisor stopping".into());
    }
    if Instant::now() >= deadline {
        return Err("preparation deadline exceeded".into());
    }
    Ok(())
}

pub(super) fn verify_file(
    file: &mut File,
    expected: &str,
    deadline: Instant,
    stopped: &AtomicBool,
) -> Result<()> {
    use std::io::{Seek, SeekFrom};
    if !file.metadata()?.is_file() {
        return Err("regular executable required".into());
    }
    file.seek(SeekFrom::Start(0))?;
    let mut hash = Sha256::new();
    let mut bytes = [0; 65536];
    loop {
        check(deadline, stopped)?;
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
    prepare_cancellable(root, request, output, deadline, &AtomicBool::new(false))
}

pub(super) fn copy_owned(
    source: &mut File,
    output: &Path,
    stopped: &AtomicBool,
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
        .read(true)
        .write(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&temporary)?;
    let result = (|| -> Result<()> {
        let mut hash = Sha256::new();
        let mut buffer = [0u8; 128 * 1024];
        loop {
            check(deadline, stopped)?;
            let count = source.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            hash.update(&buffer[..count]);
            target.write_all(&buffer[..count])?;
        }
        let expected = hash.finalize();
        target.seek(SeekFrom::Start(0))?;
        let mut hash = Sha256::new();
        loop {
            check(deadline, stopped)?;
            let count = target.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            hash.update(&buffer[..count]);
        }
        if hash.finalize() != expected {
            return Err("owned executable copy mismatch".into());
        }
        check(deadline, stopped)?;
        target.set_permissions(std::fs::Permissions::from_mode(0o700))?;
        target.sync_all()?;
        drop(target);
        check(deadline, stopped)?;
        std::fs::rename(&temporary, output)?;
        File::open(output.parent().ok_or("missing parent")?)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    result
}

pub(super) fn prepare_cancellable(
    root: &Path,
    request: &Submission,
    output: &Path,
    deadline: Instant,
    stopped: &AtomicBool,
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

const MAX_MANIFEST: u64 = 4 * 1024 * 1024;
const MAX_BINARY: u64 = 512 * 1024 * 1024;

fn parse_manifest(bytes: &[u8], expected: &ExpectedBinary<'_>) -> Result<PreparedBinary> {
    if u64::try_from(bytes.len())? > MAX_MANIFEST {
        return Err("manifest too large".into());
    }
    let manifest: Manifest = serde_json::from_slice(bytes)?;
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
    let digest = manifest
        .files
        .get(&format!("bin/{}", expected.role.binary()))
        .ok_or("missing fixed binary")?;
    if manifest.schema_version != 1
        || !super::hex(&manifest.commit, 40)
        || manifest.commit != expected.commit
        || manifest.target != host_target()?
        || expected.sha256.is_some_and(|expected| expected != digest)
    {
        return Err("manifest identity or binary digest mismatch".into());
    }
    Ok(PreparedBinary {
        commit: manifest.commit,
        sha256: digest.clone(),
    })
}

// Checks cancellation between bounded decoder reads, including skipped payloads.
struct BudgetedRead<'a, R> {
    inner: R,
    deadline: Instant,
    stopped: &'a AtomicBool,
}

impl<R: Read> Read for BudgetedRead<'_, R> {
    fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
        check(self.deadline, self.stopped).map_err(std::io::Error::other)?;
        let length = bytes.len().min(64 * 1024);
        self.inner.read(&mut bytes[..length])
    }
}

fn copy_binary(
    source: &mut impl Read,
    target: &mut File,
    deadline: Instant,
    stopped: &AtomicBool,
) -> Result<String> {
    let mut hash = Sha256::new();
    let mut buffer = [0u8; 128 * 1024];
    let mut size = 0u64;
    loop {
        check(deadline, stopped)?;
        let count = source.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        size = size
            .checked_add(u64::try_from(count)?)
            .filter(|size| *size <= MAX_BINARY)
            .ok_or("executable exceeds limit")?;
        hash.update(&buffer[..count]);
        target.write_all(&buffer[..count])?;
    }
    Ok(format!("{:x}", hash.finalize()))
}

pub(crate) fn prepare_package(
    package: &File,
    expected: ExpectedBinary<'_>,
    output: &Path,
    deadline: Instant,
    stopped: &AtomicBool,
) -> Result<PreparedBinary> {
    check(deadline, stopped)?;
    let mut manifest_bytes = Vec::new();
    BudgetedRead {
        inner: child(package, "manifest.json", false)?.take(MAX_MANIFEST + 1),
        deadline,
        stopped,
    }
    .read_to_end(&mut manifest_bytes)?;
    let prepared = parse_manifest(&manifest_bytes, &expected)?;
    let bin = child(package, "bin", true)?;
    let mut source = child(&bin, expected.role.binary(), false)?;
    if source.metadata()?.len() > MAX_BINARY {
        return Err("executable exceeds limit".into());
    }
    let temporary = output.with_extension("partial");
    let mut target = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&temporary)?;
    let mut published = false;
    let result = (|| -> Result<()> {
        if copy_binary(&mut source, &mut target, deadline, stopped)? != prepared.sha256 {
            return Err("artifact digest mismatch".into());
        }
        target.set_permissions(std::fs::Permissions::from_mode(0o700))?;
        target.sync_all()?;
        drop(target);
        check(deadline, stopped)?;
        std::fs::rename(&temporary, output)?;
        published = true;
        File::open(output.parent().ok_or("missing artifact parent")?)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
        if published {
            let _ = std::fs::remove_file(output);
        }
    }
    result?;
    Ok(prepared)
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
        let stopped = AtomicBool::new(false);
        copy_owned(
            &mut pinned,
            &output,
            &stopped,
            Instant::now() + Duration::from_secs(900),
        )
        .unwrap();
        assert_eq!(std::fs::read(&output).unwrap(), b"running old inode");
        stopped.store(true, Ordering::Release);
        let cancelled = root.path().join("cancelled");
        assert!(copy_owned(
            &mut pinned,
            &cancelled,
            &stopped,
            Instant::now() + Duration::from_secs(900)
        )
        .is_err());
        assert!(!cancelled.exists());
        assert!(!cancelled.with_extension("partial").exists());
        assert_eq!(
            std::fs::metadata(&output).unwrap().permissions().mode() & 0o777,
            0o700
        );
        let temporary = cancelled.with_extension("partial");
        std::fs::write(&temporary, b"another owner's partial").unwrap();
        assert!(copy_owned(
            &mut pinned,
            &cancelled,
            &stopped,
            Instant::now() + Duration::from_secs(900)
        )
        .is_err());
        assert_eq!(
            std::fs::read(&temporary).unwrap(),
            b"another owner's partial"
        );
        let failed = root.path().join("failed");
        let mut unreadable = OpenOptions::new().write(true).open(&binary).unwrap();
        assert!(copy_owned(
            &mut unreadable,
            &failed,
            &AtomicBool::new(false),
            Instant::now() + Duration::from_secs(900)
        )
        .is_err());
        assert!(!failed.exists());
        assert!(!failed.with_extension("partial").exists());
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
        let cancelled = root.path().join("cancelled");
        assert_eq!(
            prepare_cancellable(
                root.path(),
                &request,
                &cancelled,
                Instant::now() + Duration::from_secs(900),
                &AtomicBool::new(true)
            )
            .err()
            .unwrap()
            .to_string(),
            "supervisor stopping"
        );
        assert!(!cancelled.exists());
        assert!(!cancelled.with_extension("partial").exists());
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
            &AtomicBool::new(false)
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
    fn budgeted_reads_check_cancellation_between_bounded_chunks() {
        struct StopAfterRead<'a>(&'a AtomicBool);
        impl Read for StopAfterRead<'_> {
            fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
                assert!(bytes.len() <= 64 * 1024);
                bytes.fill(0);
                self.0.store(true, Ordering::Release);
                Ok(bytes.len())
            }
        }
        let stopped = AtomicBool::new(false);
        let mut reader = BudgetedRead {
            inner: StopAfterRead(&stopped),
            deadline: Instant::now() + Duration::from_secs(900),
            stopped: &stopped,
        };
        let mut bytes = [0; 128 * 1024];
        assert_eq!(reader.read(&mut bytes).unwrap(), 64 * 1024);
        assert!(reader
            .read(&mut bytes)
            .unwrap_err()
            .to_string()
            .contains("supervisor stopping"));
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
    fn preparation_checks_only_the_selected_binary() {
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
            let output = root.path().join("output");
            prepare(
                root.path(),
                &request,
                &output,
                Instant::now() + Duration::from_secs(900),
            )
            .unwrap();
            assert_eq!(std::fs::read(&output).unwrap(), b"binary");
            std::fs::remove_file(output).unwrap();
        }
    }
}
