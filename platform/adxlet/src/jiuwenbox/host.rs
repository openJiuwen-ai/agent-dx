//! Shared Linux EROFS mount and supervisor resource discovery/cleanup.
use super::{client::unavailable, Bootstrap};
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

fn mounted(root: &Path) -> Result<bool> {
    let text = std::fs::read_to_string("/proc/self/mountinfo").map_err(unavailable)?;
    let path = root
        .to_str()
        .ok_or_else(|| Error::Invalid("UTF-8 mount path required".into()))?;
    Ok(text.lines().any(|line| {
        line.split_whitespace()
            .nth(4)
            .is_some_and(|p| p.replace("\\040", " ").replace("\\134", "\\") == path)
    }))
}

#[async_trait]
impl HostOperations for LinuxHost {
    async fn prepare(&self, bootstrap: &Bootstrap, root: &Path) -> Result<PathBuf> {
        if !cfg!(target_os = "linux") {
            return Err(Error::Invalid(
                "Jiuwenbox EROFS requires a Linux host".into(),
            ));
        }
        let image = bootstrap.image.clone();
        let expected = bootstrap.sha256.clone();
        tokio::task::spawn_blocking(move || {
            use std::io::{Read, Seek, SeekFrom};
            let mut file = std::fs::File::open(&image).map_err(unavailable)?;
            if !file.metadata().map_err(unavailable)?.is_file() {
                return Err(Error::Invalid("EROFS image must be a regular file".into()));
            }
            let mut hash = Sha256::new();
            let mut bytes = [0; 65536];
            loop {
                let count = file.read(&mut bytes).map_err(unavailable)?;
                if count == 0 {
                    break;
                }
                hash.update(&bytes[..count]);
            }
            if format!("{:x}", hash.finalize()) != expected {
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
        .map_err(unavailable)??;
        std::fs::create_dir_all(root).map_err(unavailable)?;
        if !mounted(root)? {
            command(
                "mount",
                &[
                    "-t",
                    "erofs",
                    "-o",
                    "ro,loop,nodev,nosuid",
                    path(&bootstrap.image)?,
                    path(root)?,
                ],
                None,
            )
            .await?;
        }
        // Reuse only a mount whose loop backing image matches the pinned artifact.
        let info = std::fs::read_to_string("/proc/self/mountinfo").map_err(unavailable)?;
        let wanted = path(root)?;
        let line = info
            .lines()
            .find(|line| line.split_whitespace().nth(4) == Some(wanted))
            .ok_or_else(|| unavailable("EROFS mount not visible"))?;
        let (_, fs) = line
            .split_once(" - ")
            .ok_or_else(|| unavailable("invalid mountinfo"))?;
        let mut fields = fs.split_whitespace();
        if fields.next() != Some("erofs") {
            return Err(unavailable("bootstrap mount is not EROFS"));
        }
        let device = fields
            .next()
            .and_then(|v| Path::new(v).file_name())
            .ok_or_else(|| unavailable("missing loop device"))?;
        let backing = std::fs::read_to_string(
            Path::new("/sys/class/block")
                .join(device)
                .join("loop/backing_file"),
        )
        .map_err(unavailable)?;
        if std::fs::canonicalize(backing.trim()).map_err(unavailable)?
            != std::fs::canonicalize(&bootstrap.image).map_err(unavailable)?
        {
            return Err(unavailable("bootstrap backing image differs"));
        }
        Ok(root.to_path_buf())
    }
    async fn supervisor_inventory(&self) -> Result<Vec<String>> {
        let mut ids = std::collections::BTreeSet::new();
        for (directory, prefix) in [("/sys/fs/cgroup/jiuwenbox", ""), ("/run/netns", "jbx-")] {
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
        let root = Path::new("/sys/fs/cgroup");
        if !root.join("cgroup.controllers").is_file() {
            return Err(unavailable("supervisor recovery requires cgroup v2"));
        }
        let group = root.join("jiuwenbox").join(id);
        match std::fs::read_to_string(group.join("cgroup.procs")) {
            Ok(pids) => Ok(pids.trim().is_empty()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(!group.exists()),
            Err(e) => Err(unavailable(e)),
        }
    }
    async fn cleanup_supervisor(&self, id: &str) -> Result<()> {
        if !super::managed_id(id) {
            return Err(Error::Invalid("invalid managed supervisor ID".into()));
        }
        if !self.supervisor_absent(id).await? {
            // cgroup.kill targets the entire owned group without PID reuse races.
            std::fs::write(
                Path::new("/sys/fs/cgroup/jiuwenbox")
                    .join(id)
                    .join("cgroup.kill"),
                "1",
            )
            .map_err(unavailable)?;
            tokio::time::timeout(Duration::from_secs(5), async {
                while !self.supervisor_absent(id).await? {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Ok::<(), Error>(())
            })
            .await
            .map_err(|_| unavailable("supervisor processes remain"))??;
        }
        let namespace = format!("jbx-{id}");
        if Path::new("/run/netns").join(&namespace).exists() {
            command("ip", &["netns", "delete", &namespace], None).await?;
        }
        let hash = format!("{:x}", Sha256::digest(id.as_bytes()));
        let interface = format!("jwbH{}", &hash[..8]);
        if Path::new("/sys/class/net").join(&interface).exists() {
            command("ip", &["link", "del", &interface], None).await?;
        }
        let group = Path::new("/sys/fs/cgroup/jiuwenbox").join(id);
        match std::fs::remove_dir(group) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(unavailable(e)),
        }
    }
}

fn path(path: &Path) -> Result<&str> {
    path.to_str()
        .filter(|s| !s.chars().any(char::is_whitespace))
        .ok_or_else(|| Error::Invalid("host mount paths must be UTF-8 without whitespace".into()))
}
