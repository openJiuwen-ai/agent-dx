use super::super::OWNERFS_ROOT_INODE;
use super::{fixture, wait_until_mounted};
use crate::node::vfs::{Backend, types::BackendInode};
use afs_error::Result;
use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
use std::{ffi::OsStr, fs, sync::Arc};

struct OfficialFuseMount {
    session: Option<crate::node::fuse::MountedFuse>,
    mount: std::path::PathBuf,
}

impl OfficialFuseMount {
    fn new(session: crate::node::fuse::MountedFuse, mount: std::path::PathBuf) -> Self {
        Self {
            session: Some(session),
            mount,
        }
    }

    fn join(&mut self) -> Result<()> {
        let session = self.session.take().expect("FUSE session already joined");
        session.join()
    }
}

impl Drop for OfficialFuseMount {
    fn drop(&mut self) {
        if let Some(session) = self.session.take() {
            let result = session.join();
            println!("OFFICIAL_FUSER_DROP_JOIN {result:?}");
        }
        let result = std::process::Command::new("fusermount3")
            .arg("-u")
            .arg(&self.mount)
            .output();
        println!("OFFICIAL_FUSER_DROP_UNMOUNT {result:?}");
    }
}

fn official_check_root_linux_fuse() {
    assert_eq!(std::env::consts::OS, "linux");
    let uid = std::process::Command::new("id").arg("-u").output().unwrap();
    assert!(uid.status.success());
    assert_eq!(
        String::from_utf8(uid.stdout).unwrap().trim(),
        "0",
        "real kernel FUSE permission checks require root so the test can drop to uid 501/502"
    );
    assert!(
        std::path::Path::new("/dev/fuse").exists(),
        "real /dev/fuse is required"
    );
    assert!(
        std::process::Command::new("fusermount3")
            .arg("--version")
            .output()
            .unwrap()
            .status
            .success(),
        "fusermount3 is required"
    );
    assert!(
        std::process::Command::new("python3")
            .arg("--version")
            .output()
            .unwrap()
            .status
            .success(),
        "python3 is required"
    );
}

fn official_mode(path: &std::path::Path) -> u32 {
    fs::metadata(path).unwrap().permissions().mode() & 0o7777
}

fn official_write_file(path: &std::path::Path, mode: u32) {
    fs::write(path, b"initial").unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(mode)).unwrap();
    let metadata = fs::metadata(path).unwrap();
    assert_eq!(metadata.uid(), 0);
    assert_eq!(metadata.gid(), 0);
    assert_eq!(metadata.permissions().mode() & 0o7777, mode);
}

fn official_run_python(script: &str, paths: &[&std::path::Path]) {
    let output = std::process::Command::new("python3")
        .arg("-c")
        .arg(script)
        .args(paths)
        .output()
        .unwrap();
    println!(
        "OFFICIAL_FUSER_PYTHON status={:?} stdout={} stderr={}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.status.success());
}

fn official_mountinfo_contains(root: &std::path::Path) -> bool {
    let root = root.to_str().unwrap();
    fs::read_to_string("/proc/self/mountinfo")
        .unwrap()
        .lines()
        .any(|line| {
            line.split_whitespace()
                .nth(4)
                .is_some_and(|path| path.starts_with(root))
        })
}

#[test]
#[ignore = "requires Linux root, /dev/fuse, fusermount3 and python3"]
fn linux_official_fuser_ownerfs_killpriv_permissions_and_lock_fallbacks() {
    official_check_root_linux_fuse();

    let (temp, fs_backend, ctx) = fixture();
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o755)).unwrap();
    let retained = temp.keep();
    let mount = retained.join("mnt");
    fs::create_dir(&mount).unwrap();
    fs::set_permissions(&mount, fs::Permissions::from_mode(0o755)).unwrap();

    let workspace_name = OsStr::new("official-fuser");
    fs_backend
        .mkdir(
            &ctx,
            BackendInode {
                value: OWNERFS_ROOT_INODE,
            },
            workspace_name,
            0o777,
        )
        .unwrap();
    let ownerfs = Arc::new(fs_backend);
    let session = crate::node::fuse::mount_ownerfs(ownerfs, &mount).unwrap();
    let mut mounted = OfficialFuseMount::new(session, mount.clone());
    wait_until_mounted(&mount).unwrap();

    let workspace = mount.join(workspace_name);
    assert!(workspace.is_dir());
    assert_eq!(official_mode(&workspace), 0o777);

    let write_clear = workspace.join("write-clear");
    let truncate_clear = workspace.join("truncate-clear");
    let explicit_chmod_denied = workspace.join("explicit-chmod-denied");
    let root_preserve = workspace.join("root-preserve");
    let sgid_no_exec = workspace.join("sgid-no-exec");
    let zero_write = workspace.join("zero-write");
    let fd_suid = workspace.join("fd-suid");
    let fd_suid_sgid = workspace.join("fd-suid-sgid");
    let utime_denied = workspace.join("utime-denied");
    let lock_record = workspace.join("lock-record");

    official_write_file(&write_clear, 0o6777);
    official_write_file(&truncate_clear, 0o6777);
    official_write_file(&explicit_chmod_denied, 0o6777);
    official_write_file(&root_preserve, 0o6777);
    official_write_file(&sgid_no_exec, 0o2666);
    official_write_file(&zero_write, 0o4777);
    official_write_file(&fd_suid, 0o4666);
    official_write_file(&fd_suid_sgid, 0o6666);
    official_write_file(&utime_denied, 0o666);
    official_write_file(&lock_record, 0o666);

    let user_ops = r#"
import errno, fcntl, os, sys, time

def drop(uid):
    os.setgroups([])
    os.setgid(uid)
    os.setuid(uid)

def expect_errno(label, fn, allowed):
    try:
        fn()
    except OSError as exc:
        print(f"{label}=errno:{exc.errno}")
        assert exc.errno in allowed, (label, exc.errno)
    else:
        raise AssertionError(label + " unexpectedly succeeded")

write_clear, truncate_clear, explicit_chmod_denied, sgid_no_exec, zero_write, utime_denied, lock_record = sys.argv[1:]
drop(501)

fd = os.open(write_clear, os.O_WRONLY)
assert os.write(fd, b"user-write") == 10
os.fsync(fd)
os.close(fd)

fd = os.open(truncate_clear, os.O_WRONLY)
os.ftruncate(fd, 1)
os.fsync(fd)
os.close(fd)

expect_errno("nonowner_chmod_clear", lambda: os.chmod(explicit_chmod_denied, 0o777), {errno.EPERM, errno.EACCES})

fd = os.open(sgid_no_exec, os.O_WRONLY)
assert os.write(fd, b"g") == 1
os.fsync(fd)
os.close(fd)

fd = os.open(zero_write, os.O_WRONLY)
assert os.write(fd, b"") == 0
os.close(fd)

expect_errno("explicit_utime_nonowner", lambda: os.utime(utime_denied, (1, 2)), {errno.EPERM, errno.EACCES})

fd = os.open(lock_record, os.O_RDWR)
for label, op in (
    ("fcntl_lockf_exclusive", lambda: fcntl.lockf(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)),
    ("flock_exclusive", lambda: fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)),
):
    try:
        op()
    except OSError as exc:
        print(f"{label}=errno:{exc.errno}")
    else:
        print(f"{label}=ok")
os.close(fd)
"#;
    official_run_python(
        user_ops,
        &[
            &write_clear,
            &truncate_clear,
            &explicit_chmod_denied,
            &sgid_no_exec,
            &zero_write,
            &utime_denied,
            &lock_record,
        ],
    );

    assert_eq!(official_mode(&write_clear) & 0o6000, 0);
    assert_eq!(official_mode(&truncate_clear) & 0o6000, 0);
    assert_eq!(official_mode(&explicit_chmod_denied), 0o6777);
    assert_eq!(official_mode(&sgid_no_exec) & libc::S_ISGID, libc::S_ISGID);
    assert_eq!(official_mode(&sgid_no_exec) & libc::S_IXGRP, 0);
    assert_eq!(official_mode(&zero_write) & libc::S_ISUID, libc::S_ISUID);
    assert_eq!(official_mode(&utime_denied), 0o666);

    let mut root_file = fs::OpenOptions::new()
        .write(true)
        .open(&root_preserve)
        .unwrap();
    use std::io::Write as _;
    root_file.write_all(b"root-write").unwrap();
    root_file.sync_all().unwrap();
    drop(root_file);
    assert_eq!(official_mode(&root_preserve), 0o6777);

    let opened_fd_write = r#"
import errno, os, sys

fd_suid, fd_suid_sgid = sys.argv[1:]

def child_write_after_root_chmod(path, mode, expected_mode):
    ready_r, ready_w = os.pipe()
    go_r, go_w = os.pipe()
    pid = os.fork()
    if pid == 0:
        try:
            os.close(ready_r)
            os.close(go_w)
            os.setgroups([])
            os.setgid(501)
            os.setuid(501)
            fd = os.open(path, os.O_WRONLY)
            os.write(ready_w, b"r")
            os.close(ready_w)
            assert os.read(go_r, 1) == b"g"
            os.close(go_r)
            assert os.write(fd, b"fd-open") == 7
            os.fsync(fd)
            os.close(fd)
            try:
                fresh = os.open(path, os.O_WRONLY)
            except OSError as exc:
                assert exc.errno in (errno.EACCES, errno.EPERM), exc.errno
            else:
                os.close(fresh)
                raise AssertionError("fresh open unexpectedly retained write authority")
            os._exit(0)
        except BaseException as exc:
            print(f"child_error={exc!r}", flush=True)
            os._exit(97)
    os.close(ready_w)
    os.close(go_r)
    assert os.read(ready_r, 1) == b"r"
    os.close(ready_r)
    os.chmod(path, mode)
    os.write(go_w, b"g")
    os.close(go_w)
    _, status = os.waitpid(pid, 0)
    assert os.WIFEXITED(status), status
    assert os.WEXITSTATUS(status) == 0, status
    actual = os.stat(path).st_mode & 0o7777
    print(f"opened_fd_write path={path} mode={oct(mode)} actual={oct(actual)}")
    assert actual == expected_mode, (path, oct(actual), oct(expected_mode))

child_write_after_root_chmod(fd_suid, 0o4000, 0o0000)
child_write_after_root_chmod(fd_suid_sgid, 0o6000, 0o2000)
"#;
    official_run_python(opened_fd_write, &[&fd_suid, &fd_suid_sgid]);
    assert_eq!(official_mode(&fd_suid), 0);
    assert_eq!(official_mode(&fd_suid_sgid), 0o2000);
    assert_eq!(fs::read(&fd_suid).unwrap(), b"fd-open");
    assert_eq!(fs::read(&fd_suid_sgid).unwrap(), b"fd-open");

    let reopen_read = fs::read_to_string(&write_clear).unwrap();
    assert_eq!(reopen_read, "user-write");
    let mut trunc_content = fs::read(&truncate_clear).unwrap();
    trunc_content.truncate(1);
    assert_eq!(trunc_content, b"i");

    mounted.join().unwrap();
    assert!(
        !official_mountinfo_contains(&retained),
        "joined FUSE session left mountinfo entries under {}",
        retained.display()
    );
    fs::remove_dir_all(&retained).unwrap();
}
