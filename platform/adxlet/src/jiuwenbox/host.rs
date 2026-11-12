//! Shared Linux EROFS mount and supervisor resource discovery/cleanup.
use super::{cgroups::SupervisorCgroups, client::unavailable, Bootstrap};
use adx_core::{Error, Result};
use async_trait::async_trait;
use sha2::{Digest, Sha256};
use std::{
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};
use tokio::io::AsyncWriteExt;

/// Privileged host boundary. Component tests exercise lifecycle using a fake host.
#[async_trait]
pub trait HostOperations: Send + Sync {
    async fn prepare(&self, bootstrap: &Bootstrap, root: &Path) -> Result<PathBuf>;
    /// Complete IDs discovered from cgroups and named network namespaces.
    async fn supervisor_inventory(&self) -> Result<Vec<String>>;
    async fn supervisor_absent(&self, id: &str) -> Result<bool>;
    async fn cleanup_supervisor(&self, id: &str) -> Result<()> {
        if self.supervisor_absent(id).await? {
            Ok(())
        } else {
            Err(unavailable("supervisor processes remain"))
        }
    }
}

pub struct LinuxHost;

async fn command(program: &str, args: &[&str], input: Option<&str>) -> Result<String> {
    let mut cmd = tokio::process::Command::new(program);
    cmd.args(args)
        .kill_on_drop(true)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if input.is_some() {
        cmd.stdin(Stdio::piped());
    }
    let mut child = cmd.spawn().map_err(unavailable)?;
    if let Some(input) = input {
        let mut stdin = child
            .stdin
            .take()
            .ok_or_else(|| unavailable("command stdin missing"))?;
        stdin
            .write_all(input.as_bytes())
            .await
            .map_err(unavailable)?;
        drop(stdin);
    }
    let output = tokio::time::timeout(Duration::from_secs(30), child.wait_with_output())
        .await
        .map_err(|_| Error::DeadlineExceeded(format!("host {program}")))?
        .map_err(unavailable)?;
    if !output.status.success() {
        return Err(unavailable(format!(
            "host {program} failed: {}",
            String::from_utf8_lossy(&output.stderr)
                .chars()
                .take(2048)
                .collect::<String>()
        )));
    }
    String::from_utf8(output.stdout).map_err(unavailable)
}

fn mounted_image(info: &str, root: &Path) -> Result<Option<PathBuf>> {
    let wanted = path(root)?;
    let Some(line) = info.lines().find(|line| {
        line.split_whitespace()
            .nth(4)
            .is_some_and(|target| target.replace("\\134", "\\") == wanted)
    }) else {
        return Ok(None);
    };
    let (_, filesystem) = line
        .split_once(" - ")
        .ok_or_else(|| unavailable("invalid mountinfo"))?;
    let mut fields = filesystem.split_whitespace();
    if fields.next() != Some("erofs") {
        return Err(unavailable("bootstrap mount is not EROFS"));
    }
    let source = fields
        .next()
        .filter(|source| Path::new(source).is_absolute())
        .ok_or_else(|| unavailable("missing bootstrap device"))?;
    Ok(Some(PathBuf::from(source)))
}

fn legacy_mounts(info: &str, root: &Path) -> Result<Vec<PathBuf>> {
    let parent = root
        .parent()
        .ok_or_else(|| unavailable("missing bootstrap directory"))?;
    let mut roots = Vec::new();
    for line in info.lines() {
        let Some(target) = line.split_whitespace().nth(4) else {
            continue;
        };
        let target = PathBuf::from(target.replace("\\134", "\\"));
        let owned = target.parent() == Some(parent)
            && target
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| {
                    name.len() == 64 && name.bytes().all(|c| c.is_ascii_hexdigit())
                });
        if owned {
            // Only the driver's former SHA-named EROFS mounts are managed here.
            // Preserve unrelated paths and leave foreign filesystems untouched.
            if let Err(error) = mounted_image(info, &target) {
                adx_observability::warn!(mount=%target.display(), %error, "Jiuwenbox unrecognized old bootstrap mount retained");
                continue;
            }
            roots.push(target);
        }
    }
    Ok(roots)
}

fn checksum(reader: &mut impl std::io::Read) -> Result<String> {
    let mut hash = Sha256::new();
    let mut bytes = [0; 65536];
    loop {
        let count = reader.read(&mut bytes).map_err(unavailable)?;
        if count == 0 {
            break;
        }
        hash.update(&bytes[..count]);
    }
    Ok(format!("{:x}", hash.finalize()))
}

async fn validate_image(bootstrap: &Bootstrap) -> Result<()> {
    let image = bootstrap.image.clone();
    let expected = bootstrap.sha256.clone();
    tokio::task::spawn_blocking(move || {
        use std::io::{Read, Seek, SeekFrom};
        let mut file = std::fs::File::open(image).map_err(unavailable)?;
        if !file.metadata().map_err(unavailable)?.is_file() {
            return Err(Error::Invalid("EROFS image must be a regular file".into()));
        }
        if checksum(&mut file)? != expected {
            return Err(Error::Invalid("Execd EROFS checksum mismatch".into()));
        }
        file.seek(SeekFrom::Start(1024)).map_err(unavailable)?;
        let mut magic = [0; 4];
        file.read_exact(&mut magic).map_err(unavailable)?;
        if magic != [0xe2, 0xe1, 0xf5, 0xe0] {
            return Err(Error::Invalid("Execd artifact is not EROFS".into()));
        }
        Ok(())
    })
    .await
    .map_err(unavailable)?
}

async fn prepare_mount<F, Fut>(
    bootstrap: &Bootstrap,
    root: &Path,
    mounted_source: Option<PathBuf>,
    legacy: Vec<PathBuf>,
    mut run: F,
) -> Result<bool>
where
    F: FnMut(&'static str, Vec<String>) -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    // Validate paths and the replacement before touching an existing mount.
    let image = path(&bootstrap.image)?.to_owned();
    let target = path(root)?.to_owned();
    validate_image(bootstrap).await?;
    std::fs::create_dir_all(root).map_err(unavailable)?;
    for old in legacy {
        // Legacy cleanup is best effort. The fixed current target prevents
        // further version-mount growth while busy old mounts remain visible.
        if let Err(error) = run("umount", vec!["--".into(), path(&old)?.into()]).await {
            adx_observability::warn!(mount=%old.display(), %error, "Jiuwenbox old bootstrap cleanup deferred");
            continue;
        }
        // Remove only an empty former mountpoint, never arbitrary contents.
        if let Err(error) = std::fs::remove_dir(&old) {
            if error.kind() != std::io::ErrorKind::NotFound {
                adx_observability::warn!(mount=%old.display(), %error, "Jiuwenbox old bootstrap directory retained");
            }
        }
    }
    if let Some(source) = mounted_source {
        // Read the mounted device itself: release paths can change, and a
        // backing pathname can be replaced while the loop still holds old data.
        let digest = tokio::task::spawn_blocking(move || {
            checksum(&mut std::fs::File::open(source).map_err(unavailable)?)
        })
        .await
        .map_err(unavailable)??;
        if digest == bootstrap.sha256 {
            return Ok(false);
        }
        adx_observability::warn!(mount=%root.display(), expected_sha256=%bootstrap.sha256,
            mounted_sha256=%digest, "Jiuwenbox bootstrap content changed; remounting");
        // No lazy/forced unmount. A busy mount must remain intact, and a failed
        // unmount must never be followed by another mount on the same target.
        run("umount", vec!["--".into(), target.clone()]).await?;
    }
    let result = run(
        "mount",
        vec![
            "-t".into(),
            "erofs".into(),
            "-o".into(),
            "ro,loop,nodev,nosuid".into(),
            image,
            target.clone(),
        ],
    )
    .await;
    if let Err(error) = result {
        // A mount command can time out or fail after installing its mount.
        // The target was absent here, so a rollback cannot remove a reused mount.
        if let Err(cleanup) = run("umount", vec!["--".into(), target]).await {
            adx_observability::warn!(mount=%root.display(), %cleanup, "Jiuwenbox failed mount cleanup incomplete");
        }
        return Err(error);
    }
    Ok(true)
}

#[async_trait]
impl HostOperations for LinuxHost {
    async fn prepare(&self, bootstrap: &Bootstrap, root: &Path) -> Result<PathBuf> {
        if !cfg!(target_os = "linux") {
            return Err(Error::Invalid(
                "Jiuwenbox EROFS requires a Linux host".into(),
            ));
        }
        let info = std::fs::read_to_string("/proc/self/mountinfo").map_err(unavailable)?;
        let source = mounted_image(&info, root)?;
        let legacy = legacy_mounts(&info, root)?;
        let newly_mounted = prepare_mount(
            bootstrap,
            root,
            source,
            legacy,
            |program, arguments| async move {
                let args: Vec<&str> = arguments.iter().map(String::as_str).collect();
                command(program, &args, None).await.map(|_| ())
            },
        )
        .await?;
        let result = std::fs::read_to_string("/proc/self/mountinfo")
            .map_err(unavailable)
            .and_then(|info| {
                if mounted_image(&info, root)?.is_none() {
                    return Err(unavailable("EROFS mount not visible"));
                }
                Ok(root.to_path_buf())
            });
        if result.is_err() && newly_mounted {
            if let Err(cleanup) = command("umount", &["--", path(root)?], None).await {
                adx_observability::warn!(mount=%root.display(), %cleanup, "Jiuwenbox bootstrap verification cleanup incomplete");
            }
        }
        result
    }
    async fn supervisor_inventory(&self) -> Result<Vec<String>> {
        let mut ids: std::collections::BTreeSet<_> = SupervisorCgroups::new("/sys/fs/cgroup")
            .inventory()?
            .into_iter()
            .collect();
        for (directory, prefix) in [("/run/netns", "jbx-")] {
            let entries = match std::fs::read_dir(directory) {
                Ok(entries) => entries,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(unavailable(e)),
            };
            for entry in entries {
                let name = entry.map_err(unavailable)?.file_name();
                let name = name
                    .to_str()
                    .ok_or_else(|| unavailable("invalid supervisor resource name"))?;
                if let Some(id) = name.strip_prefix(prefix).filter(|id| super::managed_id(id)) {
                    ids.insert(id.to_owned());
                }
            }
        }
        Ok(ids.into_iter().collect())
    }
    async fn supervisor_absent(&self, id: &str) -> Result<bool> {
        SupervisorCgroups::new("/sys/fs/cgroup").absent(id)
    }
    async fn cleanup_supervisor(&self, id: &str) -> Result<()> {
        if !super::managed_id(id) {
            return Err(Error::Invalid("invalid managed supervisor ID".into()));
        }
        let cgroups = SupervisorCgroups::new("/sys/fs/cgroup");
        tokio::time::timeout(Duration::from_secs(5), async {
            while !cgroups.absent(id)? {
                cgroups.kill_once(id)?;
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Ok::<(), Error>(())
        })
        .await
        .map_err(|_| unavailable("supervisor processes remain"))??;
        cgroups.remove_empty(id)?;
        let namespace = format!("jbx-{id}");
        if Path::new("/run/netns").join(&namespace).exists() {
            command("ip", &["netns", "delete", &namespace], None).await?;
        }
        let hash = format!("{:x}", Sha256::digest(id.as_bytes()));
        let interface = format!("jwbH{}", &hash[..8]);
        if Path::new("/sys/class/net").join(&interface).exists() {
            command("ip", &["link", "del", &interface], None).await?;
        }
        Ok(())
    }
}

fn path(path: &Path) -> Result<&str> {
    path.to_str()
        .filter(|s| !s.chars().any(char::is_whitespace))
        .ok_or_else(|| Error::Invalid("host mount paths must be UTF-8 without whitespace".into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        future::ready,
        sync::{Arc, Mutex},
    };

    fn image(directory: &Path, name: &str, content: u8) -> Bootstrap {
        let image = directory.join(name);
        let mut data = vec![content; 4096];
        data[1024..1028].copy_from_slice(&[0xe2, 0xe1, 0xf5, 0xe0]);
        std::fs::write(&image, &data).unwrap();
        Bootstrap {
            image,
            sha256: format!("{:x}", Sha256::digest(&data)),
            target: "/__adx_runtime".into(),
            command: vec!["/__adx_runtime/adx-execd".into()],
        }
    }

    #[tokio::test]
    async fn identical_content_at_another_release_path_reuses_mount() {
        let directory = tempfile::tempdir().unwrap();
        let old = image(directory.path(), "old-release.img", 1);
        let new = image(directory.path(), "new-release.img", 1);
        prepare_mount(
            &new,
            &directory.path().join("mount"),
            Some(old.image),
            Vec::new(),
            |_, _| {
                ready(Err(unavailable(
                    "A content-identical mount must not be changed",
                )))
            },
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn changed_content_unmounts_before_mounting_current_image() {
        let directory = tempfile::tempdir().unwrap();
        let old = image(directory.path(), "old.img", 1);
        let new = image(directory.path(), "new.img", 2);
        let root = directory.path().join("mount");
        let calls = Arc::new(Mutex::new(Vec::new()));
        let observed = calls.clone();
        prepare_mount(
            &new,
            &root,
            Some(old.image),
            Vec::new(),
            move |program, args| {
                observed.lock().unwrap().push((program, args));
                ready(Ok(()))
            },
        )
        .await
        .unwrap();
        let calls = calls.lock().unwrap();
        assert_eq!(
            calls[0],
            ("umount", vec!["--".into(), root.to_str().unwrap().into()])
        );
        assert_eq!(
            calls[1],
            (
                "mount",
                vec![
                    "-t".into(),
                    "erofs".into(),
                    "-o".into(),
                    "ro,loop,nodev,nosuid".into(),
                    new.image.to_str().unwrap().into(),
                    root.to_str().unwrap().into()
                ]
            )
        );
        assert_eq!(calls.len(), 2);
    }

    #[tokio::test]
    async fn busy_mount_is_not_force_unmounted_or_replaced() {
        let directory = tempfile::tempdir().unwrap();
        let old = image(directory.path(), "old.img", 1);
        let new = image(directory.path(), "new.img", 2);
        let root = directory.path().join("mount");
        let calls = Arc::new(Mutex::new(Vec::new()));
        let observed = calls.clone();
        let error = prepare_mount(
            &new,
            &root,
            Some(old.image),
            Vec::new(),
            move |program, args| {
                observed.lock().unwrap().push((program, args));
                ready(Err(unavailable("target is busy")))
            },
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("target is busy"));
        let calls = calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, "umount");
        assert_eq!(calls[0].1, vec!["--", root.to_str().unwrap()]);
    }

    #[tokio::test]
    async fn invalid_new_image_keeps_existing_mount() {
        let directory = tempfile::tempdir().unwrap();
        let old = image(directory.path(), "old.img", 1);
        let new = image(directory.path(), "new.img", 2);
        std::fs::write(&new.image, b"incomplete upgrade").unwrap();
        let error = prepare_mount(
            &new,
            &directory.path().join("mount"),
            Some(old.image),
            Vec::new(),
            |_, _| {
                ready(Err(unavailable(
                    "Invalid artifacts must not change existing mounts",
                )))
            },
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("checksum mismatch"));
    }

    #[tokio::test]
    async fn missing_mount_mounts_current_image_once() {
        let directory = tempfile::tempdir().unwrap();
        let new = image(directory.path(), "new.img", 2);
        let calls = Arc::new(Mutex::new(Vec::new()));
        let observed = calls.clone();
        prepare_mount(
            &new,
            &directory.path().join("mount"),
            None,
            Vec::new(),
            move |program, args| {
                observed.lock().unwrap().push((program, args));
                ready(Ok(()))
            },
        )
        .await
        .unwrap();
        let calls = calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, "mount");
    }

    #[tokio::test]
    async fn matching_checksum_without_erofs_magic_is_rejected() {
        let directory = tempfile::tempdir().unwrap();
        let mut new = image(directory.path(), "new.img", 2);
        let data = vec![0; 4096];
        std::fs::write(&new.image, &data).unwrap();
        new.sha256 = format!("{:x}", Sha256::digest(&data));
        let error = prepare_mount(
            &new,
            &directory.path().join("mount"),
            None,
            Vec::new(),
            |_, _| ready(Err(unavailable("Non-EROFS artifacts must not be mounted"))),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("not EROFS"));
    }

    #[test]
    fn mounted_source_is_the_actual_device_not_the_release_path() {
        let info = "20 1 7:0 / /bootstrap ro - erofs /dev/loop0 ro\n21 1 7:1 / /unrelated ro - erofs /dev/loop1 ro\n";
        assert_eq!(
            mounted_image(info, Path::new("/bootstrap")).unwrap(),
            Some(PathBuf::from("/dev/loop0"))
        );
        assert_eq!(mounted_image(info, Path::new("/absent")).unwrap(), None);
    }

    #[test]
    fn foreign_filesystem_is_not_unmounted_as_bootstrap() {
        let info = "20 1 0:1 / /bootstrap rw - tmpfs tmpfs rw\n";
        let error = mounted_image(info, Path::new("/bootstrap")).unwrap_err();
        assert!(error.to_string().contains("not EROFS"));
    }

    #[test]
    fn mountinfo_backslash_escape_resolves_existing_mount() {
        let info = "20 1 7:0 / /state\\134root/bootstrap ro - erofs /dev/loop0 ro\n";
        assert_eq!(
            mounted_image(info, Path::new("/state\\root/bootstrap")).unwrap(),
            Some(PathBuf::from("/dev/loop0"))
        );
    }

    #[tokio::test]
    async fn upgrade_removes_sha_mounts_before_mounting_stable_target() {
        let directory = tempfile::tempdir().unwrap();
        let old = directory.path().join("a".repeat(64));
        std::fs::create_dir(&old).unwrap();
        let root = directory.path().join("current");
        let new = image(directory.path(), "new.img", 2);
        let calls = Arc::new(Mutex::new(Vec::new()));
        let observed = calls.clone();
        prepare_mount(
            &new,
            &root,
            None,
            vec![old.clone()],
            move |program, args| {
                observed.lock().unwrap().push((program, args));
                ready(Ok(()))
            },
        )
        .await
        .unwrap();
        assert!(!old.exists());
        let calls = calls.lock().unwrap();
        assert_eq!(calls.len(), 2);
        assert_eq!(
            calls[0],
            ("umount", vec!["--".into(), old.to_str().unwrap().into()])
        );
        assert_eq!(calls[1].0, "mount");
        assert_eq!(calls[1].1.last().unwrap(), root.to_str().unwrap());
    }

    #[tokio::test]
    async fn busy_legacy_mount_does_not_block_current_mount() {
        let directory = tempfile::tempdir().unwrap();
        let old = directory.path().join("a".repeat(64));
        std::fs::create_dir(&old).unwrap();
        let new = image(directory.path(), "new.img", 2);
        let calls = Arc::new(Mutex::new(Vec::new()));
        let observed = calls.clone();
        prepare_mount(
            &new,
            &directory.path().join("current"),
            None,
            vec![old.clone()],
            move |program, args| {
                observed.lock().unwrap().push((program, args));
                ready(if program == "umount" {
                    Err(unavailable("target is busy"))
                } else {
                    Ok(())
                })
            },
        )
        .await
        .unwrap();
        assert!(old.exists());
        let calls = calls.lock().unwrap();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].0, "umount");
        assert_eq!(calls[1].0, "mount");
    }

    #[test]
    fn legacy_inventory_is_confined_to_owned_sha_mountpoints() {
        let old = format!("/state/bootstrap/{}", "a".repeat(64));
        let elsewhere = format!("/other/bootstrap/{}", "b".repeat(64));
        let info = format!(
            "20 1 7:0 / {old} ro - erofs /dev/loop0 ro\n\
            21 1 7:1 / /state/bootstrap/current ro - erofs /dev/loop1 ro\n\
            22 1 7:2 / {elsewhere} ro - erofs /dev/loop2 ro\n\
            23 1 7:3 / /state/bootstrap/user-data ro - erofs /dev/loop3 ro\n"
        );
        assert_eq!(
            legacy_mounts(&info, Path::new("/state/bootstrap/current")).unwrap(),
            vec![PathBuf::from(old)]
        );
    }

    #[tokio::test]
    async fn repeated_restart_and_upgrade_keep_a_single_mount() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("current");
        let table = Arc::new(Mutex::new(Vec::<PathBuf>::new()));
        let mut mounted_source = None;
        for version in [1, 1, 2, 2, 3, 3] {
            let new = image(directory.path(), &format!("release-{version}.img"), version);
            let observed = table.clone();
            prepare_mount(
                &new,
                &root,
                mounted_source,
                Vec::new(),
                move |program, _| {
                    let mut mounts = observed.lock().unwrap();
                    if program == "umount" {
                        assert_eq!(mounts.len(), 1);
                        mounts.clear();
                    } else {
                        assert!(
                            mounts.is_empty(),
                            "must not stack a new mount over an old one"
                        );
                        mounts.push(PathBuf::from("current"));
                    }
                    ready(Ok(()))
                },
            )
            .await
            .unwrap();
            assert_eq!(table.lock().unwrap().len(), 1);
            // The next startup observes the same read-only device contents.
            mounted_source = Some(new.image);
        }
    }

    #[tokio::test]
    async fn failed_mount_after_install_is_rolled_back() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("current");
        let new = image(directory.path(), "new.img", 2);
        let table = Arc::new(Mutex::new(Vec::new()));
        let observed = table.clone();
        let error = prepare_mount(&new, &root, None, Vec::new(), move |program, _| {
            let mut mounts = observed.lock().unwrap();
            if program == "mount" {
                mounts.push("current");
                ready(Err(unavailable("mount command failed after install")))
            } else {
                assert_eq!(program, "umount");
                mounts.clear();
                ready(Ok(()))
            }
        })
        .await
        .unwrap_err();
        assert!(error.to_string().contains("failed after install"));
        assert!(table.lock().unwrap().is_empty());
    }

    #[test]
    fn foreign_legacy_filesystem_does_not_block_startup() {
        let old = format!("/state/bootstrap/{}", "a".repeat(64));
        let info = format!("20 1 0:1 / {old} rw - tmpfs tmpfs rw\n");
        assert!(legacy_mounts(&info, Path::new("/state/bootstrap/current"))
            .unwrap()
            .is_empty());
    }
}
