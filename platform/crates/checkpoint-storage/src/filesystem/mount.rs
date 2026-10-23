//! Deployment-selected shared filesystem mounts, pinned for this process lifetime.
use crate::io_error;
use adx_core::{Error, Result};
use std::path::{Path, PathBuf};

#[derive(Debug, Eq, PartialEq)]
struct Identity {
    id: u64,
    device: String,
    root: String,
    filesystem: String,
    source: String,
}

pub(super) struct MountGuard {
    path: PathBuf,
    identity: Identity,
}

impl MountGuard {
    pub(super) fn open(path: PathBuf) -> Result<Self> {
        let info = std::fs::read_to_string("/proc/self/mountinfo").map_err(io_error)?;
        let identity = identify(&info, &path)?;
        Ok(Self { path, identity })
    }

    pub(super) fn verify(&self) -> Result<()> {
        let info = std::fs::read_to_string("/proc/self/mountinfo").map_err(io_error)?;
        self.verify_info(&info)
    }

    fn verify_info(&self, info: &str) -> Result<()> {
        if identify(info, &self.path)? != self.identity {
            return Err(Error::Unavailable(
                "shared filesystem mount was replaced; restart with the intended namespace".into(),
            ));
        }
        Ok(())
    }
}

fn encoded(path: &Path) -> String {
    let mut result = String::new();
    for character in path.to_string_lossy().chars() {
        match character {
            ' ' => result.push_str("\\040"),
            '\t' => result.push_str("\\011"),
            '\n' => result.push_str("\\012"),
            '\\' => result.push_str("\\134"),
            _ => result.push(character),
        }
    }
    result
}

fn identify(info: &str, path: &Path) -> Result<Identity> {
    let path = encoded(path);
    let mut found = None;
    for line in info.lines() {
        let Some((left, right)) = line.split_once(" - ") else {
            continue;
        };
        let fields: Vec<_> = left.split_whitespace().collect();
        let tail: Vec<_> = right.split_whitespace().collect();
        if fields.len() < 6 || tail.len() < 3 || fields[4] != path {
            continue;
        }
        let afs = (tail[0] == "fuse" || tail[0].starts_with("fuse.")) && tail[1] == "afs-dfs";
        let juicefs = tail[0] == "fuse.juicefs"
            && tail[1]
                .strip_prefix("JuiceFS:")
                .is_some_and(|name| !name.is_empty());
        let valid = afs || juicefs;
        if !valid || found.is_some() {
            return Err(Error::Unavailable(
                "shared filesystem mountpoint is covered or ambiguous".into(),
            ));
        }
        found = Some(Identity {
            id: fields[0].parse().map_err(io_error)?,
            device: fields[2].into(),
            root: fields[3].into(),
            filesystem: tail[0].into(),
            source: tail[1].into(),
        });
    }
    found.ok_or_else(|| {
        Error::Unavailable("configured shared filesystem mount is missing or has changed".into())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_both_shared_filesystems_and_rejects_unrecognized_mounts() {
        let root = Path::new("/mnt/shared");
        let afs = "17 1 0:42 / /mnt/shared rw - fuse afs-dfs rw\n";
        let juicefs = "18 1 0:43 / /mnt/shared rw - fuse.juicefs JuiceFS:adx-test rw\n";
        assert!(identify(afs, root).is_ok());
        assert!(identify(juicefs, root).is_ok());
        for text in [
            "",
            "18 1 8:1 / /mnt/shared rw - ext4 /dev/vda rw\n",
            "18 1 0:43 / /mnt/shared rw - fuse.other JuiceFS:adx-test rw\n",
        ] {
            assert!(identify(text, root).is_err());
        }
    }

    #[test]
    fn mount_replacement_and_covering_mount_fail_closed() {
        let info = "18 1 0:43 / /mnt/shared rw - fuse.juicefs JuiceFS:adx-test rw\n";
        let path = PathBuf::from("/mnt/shared");
        let guard = MountGuard {
            identity: identify(info, &path).unwrap(),
            path,
        };
        assert!(guard.verify_info(info).is_ok());
        for changed in [
            info.replacen("18 ", "19 ", 1),
            info.replace("0:43", "0:44"),
            info.replace("adx-test", "other"),
            format!("{info}19 1 8:1 / /mnt/shared rw - ext4 /dev/vda rw\n"),
        ] {
            assert!(guard.verify_info(&changed).is_err());
        }
    }

    #[test]
    fn escaped_mount_paths_and_exact_mountpoint_are_required() {
        let path = Path::new("/mnt/shared space\tline\nback\\slash");
        let info = format!(
            "18 1 0:43 / {} rw - fuse.juicefs JuiceFS:adx-test rw\n",
            encoded(path)
        );
        assert!(identify(&info, path).is_ok());
        assert!(identify(&info, &path.join("subdirectory")).is_err());
    }
}
