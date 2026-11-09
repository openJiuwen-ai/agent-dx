//! Supervisor cgroup discovery follows actual resources across policy changes.
use super::client::unavailable;
use adx_core::{Error, Result};
use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
};

const CONTROLLERS: [&str; 3] = ["memory", "cpu", "pids"];

pub(super) struct SupervisorCgroups {
    root: PathBuf,
}

impl SupervisorCgroups {
    pub(super) fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// Preflight agrees with Jiuwenbox's creation selector. Recovery still scans
    /// both layouts so changing policy cannot hide an older execution.
    pub(super) fn validate_mode(&self, mode: &str) -> Result<()> {
        let v2 = match std::fs::read_to_string(self.root.join("cgroup.controllers")) {
            Ok(text) => CONTROLLERS
                .iter()
                .all(|name| text.split_whitespace().any(|c| c == *name)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => false,
            Err(e) => return Err(unavailable(e)),
        };
        let v1 = CONTROLLERS.iter().all(|name| self.root.join(name).is_dir());
        let selected = match mode {
            "auto" if v2 => "v2",
            "auto" if v1 => "v1",
            "v1" if v1 => "v1",
            "v2" if v2 => "v2",
            "auto" | "v1" | "v2" => return Err(unavailable(format!(
                "requested supervisor cgroup backend {mode} is unavailable (memory/cpu/pids required)"))),
            _ => return Err(Error::Invalid("supervisor cgroup.backend must be auto, v1 or v2".into())),
        };
        if selected == "v1" {
            probe_pidfd()?;
        }
        Ok(())
    }

    fn roots(&self) -> Result<Vec<PathBuf>> {
        let mut roots = BTreeSet::new();
        // An empty controller list still identifies v2 for existing resources.
        if self
            .root
            .join("cgroup.controllers")
            .try_exists()
            .map_err(unavailable)?
        {
            roots.insert(self.root.clone());
        }
        for name in CONTROLLERS {
            let root = self.root.join(name);
            match std::fs::canonicalize(&root) {
                Ok(path) if path.is_dir() => {
                    roots.insert(path);
                }
                Ok(_) => return Err(unavailable("cgroup controller root is not a directory")),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(unavailable(e)),
            }
        }
        Ok(roots.into_iter().collect())
    }

    pub(super) fn inventory(&self) -> Result<Vec<String>> {
        let mut ids = BTreeSet::new();
        for root in self.roots()? {
            let entries = match std::fs::read_dir(root.join("jiuwenbox")) {
                Ok(entries) => entries,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(unavailable(e)),
            };
            for entry in entries {
                let entry = entry.map_err(unavailable)?;
                let name = entry.file_name();
                let name = name
                    .to_str()
                    .ok_or_else(|| unavailable("invalid supervisor cgroup name"))?;
                if super::managed_id(name) {
                    if !entry.file_type().map_err(unavailable)?.is_dir() {
                        return Err(unavailable("supervisor cgroup is not a directory"));
                    }
                    ids.insert(name.to_owned());
                }
            }
        }
        Ok(ids.into_iter().collect())
    }

    fn groups(&self, id: &str) -> Result<Vec<PathBuf>> {
        if !super::managed_id(id) {
            return Err(Error::Invalid("invalid managed supervisor ID".into()));
        }
        if !self
            .root
            .join("cgroup.controllers")
            .try_exists()
            .map_err(unavailable)?
            && !CONTROLLERS.iter().all(|name| self.root.join(name).is_dir())
        {
            return Err(unavailable(
                "no complete supervisor cgroup hierarchy is visible",
            ));
        }
        let mut groups = Vec::new();
        for root in self.roots()? {
            collect_groups(&root.join("jiuwenbox").join(id), &mut groups)?;
        }
        Ok(groups)
    }

    pub(super) fn absent(&self, id: &str) -> Result<bool> {
        for group in self.groups(id)? {
            if !processes(&group)?.is_empty() {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub(super) fn kill_once(&self, id: &str) -> Result<()> {
        for group in self.groups(id)? {
            if processes(&group)?.is_empty() {
                continue;
            }
            if group.starts_with(self.root.join("jiuwenbox")) {
                std::fs::write(group.join("cgroup.kill"), "1").map_err(unavailable)?;
            } else {
                for pid in processes(&group)? {
                    kill_member(&group, pid)?;
                }
            }
        }
        Ok(())
    }

    pub(super) fn remove_empty(&self, id: &str) -> Result<()> {
        for group in self.groups(id)?.into_iter().rev() {
            if !processes(&group)?.is_empty() {
                return Err(unavailable("supervisor processes remain"));
            }
            match std::fs::remove_dir(&group) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(unavailable(e)),
            }
        }
        Ok(())
    }
}

fn collect_groups(path: &Path, groups: &mut Vec<PathBuf>) -> Result<()> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(unavailable(e)),
    };
    if !metadata.is_dir() {
        return Err(unavailable("supervisor cgroup is not a directory"));
    }
    groups.push(path.to_path_buf());
    for entry in std::fs::read_dir(path).map_err(unavailable)? {
        let entry = entry.map_err(unavailable)?;
        let kind = entry.file_type().map_err(unavailable)?;
        if kind.is_symlink() {
            return Err(unavailable("unexpected symlink inside supervisor cgroup"));
        }
        if kind.is_dir() {
            collect_groups(&entry.path(), groups)?;
        }
    }
    Ok(())
}

fn processes(group: &Path) -> Result<BTreeSet<i32>> {
    let text = match std::fs::read_to_string(group.join("cgroup.procs")) {
        Ok(text) => text,
        Err(e)
            if e.kind() == std::io::ErrorKind::NotFound
                && !group.try_exists().map_err(unavailable)? =>
        {
            return Ok(BTreeSet::new())
        }
        Err(e) => return Err(unavailable(e)),
    };
    text.split_whitespace()
        .map(|value| {
            value
                .parse::<i32>()
                .ok()
                .filter(|pid| *pid > 1)
                .ok_or_else(|| unavailable("invalid supervisor cgroup PID"))
        })
        .collect()
}

#[cfg(target_os = "linux")]
fn open_pidfd(pid: i32) -> Result<Option<std::os::fd::OwnedFd>> {
    use std::os::fd::FromRawFd;
    // SAFETY: pidfd_open takes integer arguments and returns a new owned fd.
    let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
    if fd == -1 {
        let error = std::io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::ESRCH) {
            return Ok(None);
        }
        return Err(unavailable(format!(
            "supervisor v1 recovery requires pidfd_open: {error}"
        )));
    }
    let fd = i32::try_from(fd).map_err(unavailable)?;
    // SAFETY: the successful syscall returned a new descriptor owned here.
    Ok(Some(unsafe { std::os::fd::OwnedFd::from_raw_fd(fd) }))
}

#[cfg(target_os = "linux")]
fn probe_pidfd() -> Result<()> {
    let pid = i32::try_from(std::process::id()).map_err(unavailable)?;
    open_pidfd(pid)?.ok_or_else(|| unavailable("cannot open supervisor recovery pidfd"))?;
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn probe_pidfd() -> Result<()> {
    Err(unavailable("supervisor v1 recovery requires Linux pidfds"))
}

#[cfg(target_os = "linux")]
fn kill_member(group: &Path, pid: i32) -> Result<()> {
    use std::os::fd::AsRawFd;
    let Some(fd) = open_pidfd(pid)? else {
        return Ok(());
    };
    // Recheck ownership after pinning identity: a recycled PID outside this
    // execution must never receive a signal. No raw-PID kill fallback.
    if !processes(group)?.contains(&pid) {
        return Ok(());
    }
    // SAFETY: fd remains owned for the syscall; null siginfo requests SIGKILL.
    let result = unsafe {
        libc::syscall(
            libc::SYS_pidfd_send_signal,
            fd.as_raw_fd(),
            libc::SIGKILL,
            std::ptr::null::<libc::siginfo_t>(),
            0,
        )
    };
    if result == -1 {
        let error = std::io::Error::last_os_error();
        if error.raw_os_error() != Some(libc::ESRCH) {
            return Err(unavailable(error));
        }
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn kill_member(_group: &Path, _pid: i32) -> Result<()> {
    probe_pidfd()
}

#[cfg(test)]
mod tests {
    use super::*;
    const ID: &str = "adx-test-34359738369";
    fn v1(root: &Path) {
        for name in CONTROLLERS {
            std::fs::create_dir_all(root.join(name)).unwrap();
        }
    }
    fn group(root: &Path, name: &str, pids: &str) -> PathBuf {
        let dir = root.join(name).join("jiuwenbox").join(ID);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("cgroup.procs"), pids).unwrap();
        dir
    }
    #[test]
    fn v1_absent_and_live_resources() {
        let temp = tempfile::tempdir().unwrap();
        v1(temp.path());
        let cg = SupervisorCgroups::new(temp.path());
        assert!(cg.absent(ID).unwrap());
        group(temp.path(), "memory", "");
        let cpu = group(temp.path(), "cpu", "123\n");
        assert!(!cg.absent(ID).unwrap());
        std::fs::write(cpu.join("cgroup.procs"), "").unwrap();
        assert!(cg.absent(ID).unwrap());
    }
    #[test]
    fn inventory_unions_v1_v2_and_ignores_unmanaged_ids() {
        let temp = tempfile::tempdir().unwrap();
        v1(temp.path());
        std::fs::write(temp.path().join("cgroup.controllers"), "cpu memory pids").unwrap();
        group(temp.path(), "cpu", "");
        group(temp.path(), "memory", "");
        group(temp.path(), "", "");
        std::fs::create_dir_all(temp.path().join("cpu/jiuwenbox/unmanaged")).unwrap();
        let cg = SupervisorCgroups::new(temp.path());
        assert_eq!(cg.inventory().unwrap(), vec![ID]);
        assert!(cg.absent(ID).unwrap());
        std::fs::write(
            temp.path().join("jiuwenbox").join(ID).join("cgroup.procs"),
            "123",
        )
        .unwrap();
        assert!(!cg.absent(ID).unwrap());
    }

    #[test]
    fn nested_processes_are_not_reported_absent() {
        let temp = tempfile::tempdir().unwrap();
        v1(temp.path());
        let parent = group(temp.path(), "memory", "");
        let child = parent.join("child");
        std::fs::create_dir(&child).unwrap();
        std::fs::write(child.join("cgroup.procs"), "234").unwrap();
        assert!(!SupervisorCgroups::new(temp.path()).absent(ID).unwrap());
    }

    #[test]
    fn missing_or_malformed_process_list_is_not_absence() {
        let temp = tempfile::tempdir().unwrap();
        v1(temp.path());
        let path = group(temp.path(), "cpu", "invalid");
        let cg = SupervisorCgroups::new(temp.path());
        assert!(cg.absent(ID).is_err());
        std::fs::remove_file(path.join("cgroup.procs")).unwrap();
        assert!(cg.absent(ID).is_err());
        assert!(cg.absent("../unmanaged").is_err());
    }

    #[test]
    fn missing_hierarchy_cannot_confirm_absence() {
        let temp = tempfile::tempdir().unwrap();
        let cg = SupervisorCgroups::new(temp.path());
        assert!(cg.absent(ID).is_err());
        assert!(cg.inventory().unwrap().is_empty());
        assert!(cg.validate_mode("v2").is_err());
    }

    #[test]
    fn combined_v1_controllers_are_deduplicated() {
        use std::os::unix::fs::symlink;
        let temp = tempfile::tempdir().unwrap();
        v1(temp.path());
        std::fs::remove_dir(temp.path().join("cpu")).unwrap();
        std::fs::create_dir(temp.path().join("cpu,cpuacct")).unwrap();
        symlink("cpu,cpuacct", temp.path().join("cpu")).unwrap();
        group(temp.path(), "cpu", "345");
        let cg = SupervisorCgroups::new(temp.path());
        assert_eq!(cg.inventory().unwrap(), vec![ID]);
        assert!(!cg.absent(ID).unwrap());
    }

    #[test]
    fn policy_selection_is_strict_but_recovery_sees_old_layouts() {
        let temp = tempfile::tempdir().unwrap();
        std::fs::write(temp.path().join("cgroup.controllers"), "cpu memory pids").unwrap();
        let cg = SupervisorCgroups::new(temp.path());
        cg.validate_mode("auto").unwrap();
        cg.validate_mode("v2").unwrap();
        assert!(cg.validate_mode("v1").is_err());
        assert!(cg.validate_mode("invalid").is_err());
        v1(temp.path());
        group(temp.path(), "memory", "456");
        cg.validate_mode("v2").unwrap();
        assert!(!cg.absent(ID).unwrap());
    }

    #[test]
    fn symlink_execution_cannot_escape_ownership() {
        let temp = tempfile::tempdir().unwrap();
        v1(temp.path());
        let outside = tempfile::tempdir().unwrap();
        let parent = temp.path().join("memory/jiuwenbox");
        std::fs::create_dir(&parent).unwrap();
        std::os::unix::fs::symlink(outside.path(), parent.join(ID)).unwrap();
        let cg = SupervisorCgroups::new(temp.path());
        assert!(cg.absent(ID).is_err());
        assert!(cg.inventory().is_err());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn pidfd_cleanup_signals_only_current_members() {
        use std::process::{Command, Stdio};
        let temp = tempfile::tempdir().unwrap();
        v1(temp.path());
        let path = group(temp.path(), "memory", "");
        let mut child = Command::new("sleep")
            .arg("30")
            .stdin(Stdio::null())
            .spawn()
            .unwrap();
        let pid = i32::try_from(child.id()).unwrap();
        // An already moved process must remain alive.
        let excluded = kill_member(&path, pid);
        let alive = child.try_wait().unwrap().is_none();
        std::fs::write(path.join("cgroup.procs"), format!("{pid}\n")).unwrap();
        let included = SupervisorCgroups::new(temp.path()).kill_once(ID);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        let mut terminated = false;
        while std::time::Instant::now() < deadline {
            if child.try_wait().unwrap().is_some() {
                terminated = true;
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        // Always reap the test-owned child, even when a syscall fails.
        child.kill().ok();
        let _ = child.wait();
        excluded.unwrap();
        assert!(alive);
        included.unwrap();
        assert!(terminated, "pidfd cleanup did not terminate the member");
    }
}
