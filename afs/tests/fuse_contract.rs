use std::{
    collections::HashMap,
    ffi::{OsStr, OsString},
    io::{self, Read, Seek, Write},
    os::unix::fs::FileExt,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicU64, Ordering},
        mpsc,
    },
    time::{Duration, UNIX_EPOCH},
};

use afs::node::{
    fuse,
    vfs::{
        Backend, PageCacheIdentity,
        locks::{LockRequest, LockWaiterId},
        types::{
            BackendInode, CreatedFile, DirectoryEntry, DirectoryHandle, Entry, FileAttributes,
            FileHandle, FileKind, FileLockKind, FileLockOwner, FileLockRange, FileLockType,
            FilesystemCapacity, OpenOptions, ReleaseKind, RenameFlags, RequestContext,
            SetAttrOptions, SyncMode, WriteOptions,
        },
    },
};

#[test]
fn vfs_killpriv_options_fail_closed_until_backend_supports_them() {
    let backend = MockBackend::default();
    let ctx = request_context();

    assert!(!backend.supports_killpriv_v2());

    let open_error = backend
        .open_with_options(
            &ctx,
            backend.root_inode(),
            libc::O_WRONLY,
            OpenOptions { kill_suidgid: true },
        )
        .unwrap_err();
    assert_eq!(open_error.code(), afs_error::NODE_VFS_UNIMPLEMENTED);

    let write_error = backend
        .write_with_options(
            &ctx,
            FileHandle(999),
            0,
            b"x",
            WriteOptions { kill_suidgid: true },
        )
        .unwrap_err();
    assert_eq!(write_error.code(), afs_error::NODE_VFS_UNIMPLEMENTED);

    let setattr_error = backend
        .setattr_with_options(
            &ctx,
            backend.root_inode(),
            None,
            &Default::default(),
            SetAttrOptions {
                kill_suidgid: true,
                timestamps_now: false,
            },
        )
        .unwrap_err();
    assert_eq!(setattr_error.code(), afs_error::NODE_VFS_UNIMPLEMENTED);
}

#[test]
fn vfs_options_preserve_legacy_paths_without_killpriv() {
    let backend = MockBackend::default();
    let ctx = request_context();
    let created = backend
        .create_with_options(
            &ctx,
            backend.root_inode(),
            OsStr::new("legacy.txt"),
            0o644,
            libc::O_RDWR,
            OpenOptions::default(),
        )
        .unwrap();
    let handle = backend
        .open_with_options(
            &ctx,
            created.entry.inode,
            libc::O_RDWR,
            OpenOptions::default(),
        )
        .unwrap();

    let written = backend
        .write_with_options(&ctx, handle, 0, b"ok", WriteOptions::default())
        .unwrap();
    assert_eq!(written, 2);

    let attributes = backend
        .setattr_with_options(
            &ctx,
            created.entry.inode,
            Some(handle),
            &Default::default(),
            SetAttrOptions {
                kill_suidgid: false,
                timestamps_now: true,
            },
        )
        .unwrap();
    assert_eq!(attributes.size, 2);
}

#[test]
fn vfs_advisory_lock_methods_fail_closed_until_backend_supports_them() {
    let backend = MockBackend::default();
    let ctx = request_context();
    let owner = FileLockOwner {
        ingress_session_id: "test-mount".to_owned(),
        kernel_owner: 7,
    };
    let request = LockRequest {
        kind: FileLockKind::Posix,
        owner: owner.clone(),
        pid: 123,
        range: FileLockRange { start: 0, end: 99 },
        lock_type: FileLockType::Write,
    };

    assert!(!backend.supports_advisory_locks());
    assert_eq!(
        backend
            .getlk(&ctx, backend.root_inode(), FileHandle(1), request.clone())
            .unwrap_err()
            .code(),
        afs_error::NODE_VFS_UNIMPLEMENTED
    );
    assert_eq!(
        backend
            .setlk(
                &ctx,
                backend.root_inode(),
                FileHandle(1),
                request,
                Some(LockWaiterId {
                    ingress_session_id: "test-mount".to_owned(),
                    request_id: 99,
                }),
            )
            .unwrap_err()
            .code(),
        afs_error::NODE_VFS_UNIMPLEMENTED
    );
    assert_eq!(
        backend
            .cancel_lock_wait(LockWaiterId {
                ingress_session_id: "test-mount".to_owned(),
                request_id: 99,
            })
            .unwrap_err()
            .code(),
        afs_error::NODE_VFS_UNIMPLEMENTED
    );
    backend
        .release_locks(
            &ctx,
            backend.root_inode(),
            FileHandle(1),
            owner,
            ReleaseKind::PosixOwner,
        )
        .unwrap();
    backend.release_lock_session("test-mount").unwrap();
}

#[test]
fn vfs_statfs_fails_closed_until_backend_supports_it() {
    struct StatfsUnsupportedBackend;

    impl Backend for StatfsUnsupportedBackend {
        fn root_inode(&self) -> BackendInode {
            backend_inode(1)
        }
    }

    let error = StatfsUnsupportedBackend
        .statfs(&request_context(), backend_inode(1))
        .unwrap_err();
    assert_eq!(error.code(), afs_error::NODE_VFS_UNIMPLEMENTED);
}

#[test]
#[ignore = "requires Linux /dev/fuse and fusermount3"]
fn fuse_mount_binds_backend_at_mount_root() {
    let temp = tempfile::tempdir().unwrap();
    let mount = temp.path().join("mnt");
    std::fs::create_dir(&mount).unwrap();

    let backend = Arc::new(MockBackend::default());
    let session = fuse::mount_test_backend(backend, &mount).unwrap();
    wait_until_mounted(&mount).unwrap();

    std::fs::write(mount.join("hello.txt"), b"hello").unwrap();
    assert_eq!(std::fs::read(mount.join("hello.txt")).unwrap(), b"hello");

    session.join().unwrap();
    let _ = std::process::Command::new("fusermount3")
        .arg("-u")
        .arg(&mount)
        .status();
}

#[test]
#[ignore = "requires Linux /dev/fuse and fusermount3; supports non-root mount owner"]
fn fuse_empty_mount_normal_join_as_mount_owner() {
    let retained = tempfile::tempdir().unwrap().keep();
    let mount = retained.join("mnt");
    std::fs::create_dir(&mount).unwrap();
    let session = fuse::mount_test_backend(Arc::new(MockBackend::default()), &mount).unwrap();
    wait_until_mounted(&mount).unwrap();
    session.join().unwrap();
    assert!(
        !std::fs::read_to_string("/proc/self/mountinfo")
            .unwrap()
            .lines()
            .any(|line| line.split_whitespace().nth(4) == mount.to_str())
    );
    std::fs::remove_dir_all(retained).unwrap();
}

#[test]
#[ignore = "requires Linux private mount namespace, /dev/fuse and fusermount3"]
fn fuse_normal_join_rejects_busy_mount_without_detaching() {
    let retained = tempfile::tempdir().unwrap().keep();
    let mount = retained.join("mnt");
    std::fs::create_dir(&mount).unwrap();
    let backend = Arc::new(MockBackend::default());
    let session = fuse::mount_test_backend(backend, &mount).unwrap();
    wait_until_mounted(&mount).unwrap();
    use std::os::unix::fs::OpenOptionsExt as _;
    let held = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_PATH)
        .open(&mount)
        .unwrap();
    let error = session.join().unwrap_err();
    assert!(error.to_string().contains("normal FUSE unmount failed"));
    assert!(
        std::fs::read_to_string("/proc/self/mountinfo")
            .unwrap()
            .lines()
            .any(|line| line.split_whitespace().nth(4) == mount.to_str())
    );
    drop(held);
    let output = std::process::Command::new("fusermount3")
        .args(["-u", "--"])
        .arg(&mount)
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    assert!(
        !std::fs::read_to_string("/proc/self/mountinfo")
            .unwrap()
            .lines()
            .any(|line| line.split_whitespace().nth(4) == mount.to_str())
    );
    // Keep failure-path evidence; the private namespace owns all mounts.
}

#[test]
#[ignore = "requires Linux /dev/fuse, fusermount3 and GNU stat"]
fn fuse_statfs_returns_backend_capacity_fields_to_kernel_stat() {
    let temp = tempfile::tempdir().unwrap();
    let mount = temp.path().join("mnt");
    std::fs::create_dir(&mount).unwrap();

    let expected = FilesystemCapacity {
        blocks: 1_234_567,
        bfree: 234_567,
        bavail: 123_456,
        files: 345_678,
        ffree: 45_678,
        bsize: 4096,
        namelen: 233,
        frsize: 1024,
    };
    let _mounted = MountedTestBackend::mount(
        Arc::new(MockBackend::with_statfs_capacity(expected.clone())),
        mount.clone(),
    );

    let observed = stat_f(&mount).unwrap();
    assert_eq!(
        observed,
        StatFsFields {
            blocks: expected.blocks,
            bfree: expected.bfree,
            bavail: expected.bavail,
            files: expected.files,
            ffree: expected.ffree,
            bsize: expected.bsize as u64,
            frsize: expected.frsize as u64,
            namelen: expected.namelen as u64,
        }
    );
}

#[test]
#[ignore = "requires Linux /dev/fuse, fusermount3 and GNU stat"]
fn fuse_statfs_unsupported_backend_fails_instead_of_zero_success() {
    let temp = tempfile::tempdir().unwrap();
    let mount = temp.path().join("mnt");
    std::fs::create_dir(&mount).unwrap();

    let _mounted = MountedTestBackend::mount(Arc::new(MockBackend::default()), mount.clone());

    let output = std::process::Command::new("stat")
        .arg("-f")
        .arg("-c")
        .arg("%b %f %a %c %d %s %S %l")
        .arg(&mount)
        .output()
        .unwrap();
    assert!(
        !output.status.success(),
        "unsupported statfs must fail closed, not inherit fuser zero-success: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
#[ignore = "requires Linux /dev/fuse and fusermount3"]
fn fuse_mount_does_not_invent_backend_namespace_directories() {
    let temp = tempfile::tempdir().unwrap();
    let mount = temp.path().join("mnt");
    std::fs::create_dir(&mount).unwrap();

    let backend = Arc::new(MockBackend::default());
    let session = fuse::mount_test_backend(backend, &mount).unwrap();
    wait_until_mounted(&mount).unwrap();

    assert!(namespace_entries(&mount).is_empty());

    session.join().unwrap();
    let _ = std::process::Command::new("fusermount3")
        .arg("-u")
        .arg(&mount)
        .status();
}

#[test]
#[ignore = "requires Linux /dev/fuse and fusermount3"]
fn fuse_mount_rejects_existing_live_mount() {
    let temp = tempfile::tempdir().unwrap();
    let mount = temp.path().join("mnt");
    std::fs::create_dir(&mount).unwrap();

    let backend = Arc::new(MockBackend::default());
    let session = fuse::mount_test_backend(backend, &mount).unwrap();
    wait_until_mounted(&mount).unwrap();

    let second = Arc::new(MockBackend::default());
    let error = fuse::mount_test_backend(second, &mount).unwrap_err();
    assert_eq!(error.kind(), afs_error::ErrorKind::AlreadyExists);

    session.join().unwrap();
    let _ = std::process::Command::new("fusermount3")
        .arg("-u")
        .arg(&mount)
        .status();
}

#[test]
#[ignore = "requires Linux /dev/fuse and fusermount3"]
fn fuse_mount_dispatches_real_backend_and_preserves_open_handle_identity() {
    let temp = tempfile::tempdir().unwrap();
    let mount = temp.path().join("mnt");
    std::fs::create_dir(&mount).unwrap();

    let backend = Arc::new(MockBackend::default());
    let session = fuse::mount_test_backend(backend.clone(), &mount).unwrap();
    wait_until_mounted(&mount).unwrap();

    let root = mount.clone();
    std::fs::create_dir(root.join("work")).unwrap();
    assert_eq!(backend.root_mkdir_parent.load(Ordering::SeqCst), 1);

    let path = root.join("work").join("file.txt");
    let mut old_fd = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&path)
        .unwrap();
    old_fd.write_all(b"old").unwrap();
    old_fd.flush().unwrap();
    old_fd.sync_all().unwrap();

    let first_size = std::fs::metadata(&path).unwrap().len();
    let second_size = std::fs::metadata(&path).unwrap().len();
    assert!(
        second_size > first_size,
        "TTL=0 should force repeated getattr calls"
    );

    old_fd.rewind().unwrap();
    let mut buf = [0; 3];
    old_fd.read_exact(&mut buf).unwrap();
    old_fd.rewind().unwrap();
    old_fd.read_exact(&mut buf).unwrap();
    assert!(
        backend.read_calls.load(Ordering::SeqCst) >= 2,
        "direct-io should not satisfy repeated reads from the kernel page cache"
    );

    std::fs::remove_file(&path).unwrap();
    std::fs::write(&path, b"new").unwrap();
    old_fd.write_at(b"!", 3).unwrap();

    let mut old_contents = vec![0; 4];
    old_fd.read_at(&mut old_contents, 0).unwrap();
    assert_eq!(old_contents, b"old!");
    assert_eq!(std::fs::read(&path).unwrap(), b"new");

    let missing = std::fs::remove_file(root.join("work").join("missing")).unwrap_err();
    assert_eq!(missing.raw_os_error(), Some(libc::ENOENT));

    drop(old_fd);
    session.join().unwrap();
    let _ = std::process::Command::new("fusermount3")
        .arg("-u")
        .arg(&mount)
        .status();
}

#[test]
#[ignore = "requires Linux /dev/fuse, fusermount3 and python3 mmap"]
fn fuse_direct_io_open_allows_basic_shared_mmap() {
    let temp = tempfile::tempdir().unwrap();
    let mount = temp.path().join("mnt");
    std::fs::create_dir(&mount).unwrap();

    let backend = Arc::new(MockBackend::default());
    let session = fuse::mount_test_backend(backend, &mount).unwrap();
    wait_until_mounted(&mount).unwrap();

    let path = mount.join("mmap.bin");
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&path)
        .unwrap();
    file.set_len(4096).unwrap();
    drop(file);

    let script = r#"
import mmap
import os
import sys

path = sys.argv[1]
payload = b'AFS-MMAP'
with open(path, 'r+b') as handle:
    read_map = mmap.mmap(handle.fileno(), 4096, access=mmap.ACCESS_READ)
    try:
        assert len(read_map) == 4096
        assert read_map[:4] == b'\x00\x00\x00\x00'
    finally:
        read_map.close()

    write_map = mmap.mmap(handle.fileno(), 4096, access=mmap.ACCESS_WRITE)
    try:
        write_map[100:108] = payload
        write_map.flush()
        os.fsync(handle.fileno())
    finally:
        write_map.close()

with open(path, 'rb') as handle:
    handle.seek(100)
    assert handle.read(len(payload)) == payload
"#;
    let output = std::process::Command::new("python3")
        .arg("-c")
        .arg(script)
        .arg(&path)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "python mmap failed: status={:?} stdout={} stderr={}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    session.join().unwrap();
    let _ = std::process::Command::new("fusermount3")
        .arg("-u")
        .arg(&mount)
        .status();
}

#[test]
#[ignore = "requires Linux /dev/fuse and fusermount3"]
fn fuse_ftruncate_on_same_handle_waits_for_prior_write() {
    let temp = tempfile::tempdir().unwrap();
    let mount = temp.path().join("mnt");
    std::fs::create_dir(&mount).unwrap();

    let backend = Arc::new(MockBackend::default());
    let session =
        fuse::mount_dfs_with_options(backend.clone(), &mount, fuse::FuseMountOptions::default())
            .unwrap();
    wait_until_mounted(&mount).unwrap();

    let root = mount.clone();
    let path = root.join("ordered.txt");
    let file = Arc::new(
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
            .unwrap(),
    );

    let (write_entered_tx, write_entered_rx) = mpsc::channel();
    let (release_write_tx, release_write_rx) = mpsc::channel();
    backend.block_next_write(write_entered_tx, release_write_rx);

    let writer = {
        let file = file.clone();
        std::thread::spawn(move || file.write_at(b"abcd", 0).unwrap())
    };
    write_entered_rx
        .recv_timeout(Duration::from_secs(2))
        .unwrap();

    let (truncate_done_tx, truncate_done_rx) = mpsc::channel();
    let truncater = {
        let file = file.clone();
        std::thread::spawn(move || {
            file.set_len(1).unwrap();
            truncate_done_tx.send(()).unwrap();
        })
    };

    std::thread::sleep(Duration::from_millis(100));
    assert!(
        truncate_done_rx.try_recv().is_err(),
        "ftruncate returned before the earlier same-fh write finished"
    );
    assert!(
        !backend.events().contains(&"setattr"),
        "backend setattr overtook an earlier same-fh write"
    );

    release_write_tx.send(()).unwrap();
    assert_eq!(writer.join().unwrap(), 4);
    truncate_done_rx
        .recv_timeout(Duration::from_secs(2))
        .unwrap();
    truncater.join().unwrap();

    let events = backend.events();
    assert_eq!(
        events,
        vec!["write:start", "write:finish", "setattr"],
        "same-fh callbacks must reach the backend in kernel request order"
    );

    drop(file);
    session.join().unwrap();
    let _ = std::process::Command::new("fusermount3")
        .arg("-u")
        .arg(&mount)
        .status();
}

fn namespace_entries(mount: &std::path::Path) -> Vec<String> {
    let mut entries = std::fs::read_dir(mount)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect::<Vec<_>>();
    entries.sort();
    entries
}

fn wait_until_mounted(mount: &std::path::Path) -> io::Result<()> {
    let start = std::time::Instant::now();
    loop {
        match std::fs::read_dir(mount) {
            Ok(_) => return Ok(()),
            Err(error) if start.elapsed() < Duration::from_secs(5) => {
                if !matches!(
                    error.raw_os_error(),
                    Some(libc::ENOTCONN | libc::ENOENT | libc::EAGAIN)
                ) {
                    return Err(error);
                }
                std::thread::sleep(Duration::from_millis(25));
            }
            Err(error) => return Err(error),
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
struct StatFsFields {
    blocks: u64,
    bfree: u64,
    bavail: u64,
    files: u64,
    ffree: u64,
    bsize: u64,
    frsize: u64,
    namelen: u64,
}

fn stat_f(path: &std::path::Path) -> io::Result<StatFsFields> {
    let output = std::process::Command::new("stat")
        .arg("-f")
        .arg("-c")
        .arg("%b %f %a %c %d %s %S %l")
        .arg(path)
        .output()?;
    if !output.status.success() {
        return Err(io::Error::other(format!(
            "stat -f failed: status={:?} stdout={} stderr={}",
            output.status.code(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    let stdout = String::from_utf8(output.stdout)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    let values = stdout
        .split_whitespace()
        .map(str::parse::<u64>)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    if values.len() != 8 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("stat -f returned {} fields: {stdout:?}", values.len()),
        ));
    }
    Ok(StatFsFields {
        blocks: values[0],
        bfree: values[1],
        bavail: values[2],
        files: values[3],
        ffree: values[4],
        bsize: values[5],
        frsize: values[6],
        namelen: values[7],
    })
}

struct MountedTestBackend {
    mount: std::path::PathBuf,
    session: Option<fuse::MountedFuse>,
}

impl MountedTestBackend {
    fn mount(backend: Arc<dyn Backend>, mount: std::path::PathBuf) -> Self {
        let session = fuse::mount_test_backend(backend, &mount).unwrap();
        wait_until_mounted(&mount).unwrap();
        Self {
            mount,
            session: Some(session),
        }
    }

    fn mount_dfs(backend: Arc<dyn Backend>, mount: std::path::PathBuf) -> Self {
        Self::mount_dfs_with_options(backend, mount, fuse::FuseMountOptions::default())
    }

    fn mount_dfs_with_options(
        backend: Arc<dyn Backend>,
        mount: std::path::PathBuf,
        options: fuse::FuseMountOptions,
    ) -> Self {
        let session = fuse::mount_dfs_with_options(backend, &mount, options).unwrap();
        wait_until_mounted(&mount).unwrap();
        eprintln!(
            "DFS readahead requested_bytes={} kernel_kb={}",
            options.fuse_max_readahead_bytes,
            dfs_read_ahead_kb(&mount)
        );
        Self {
            mount,
            session: Some(session),
        }
    }
}

#[test]
#[ignore = "requires Linux /dev/fuse and fusermount3"]
fn dfs_buffered_reopen_keeps_pages_only_for_the_exact_committed_version() {
    let temp = tempfile::tempdir().unwrap();
    let mount = temp.path().join("mnt");
    std::fs::create_dir(&mount).unwrap();
    let data = vec![b'a'; 65536];
    let backend = Arc::new(VersionedReadBackend::new("version-one", data.clone()));
    let _session = MountedTestBackend::mount_dfs(backend.clone(), mount.clone());
    let path = mount.join("file");

    assert_eq!(std::fs::read(&path).unwrap(), data);
    let first_reads = backend.read_calls.load(Ordering::SeqCst);
    assert!(first_reads > 0, "initial data did not reach the backend");
    assert_eq!(std::fs::read(&path).unwrap(), data);
    assert_eq!(
        backend.read_calls.load(Ordering::SeqCst),
        first_reads,
        "unchanged read-only reopen discarded verified kernel pages"
    );

    let changed = vec![b'b'; data.len()];
    backend.publish("version-two", changed.clone());
    assert_eq!(std::fs::read(&path).unwrap(), changed);
    assert!(backend.read_calls.load(Ordering::SeqCst) > first_reads);
}

#[test]
#[ignore = "requires Linux /dev/fuse and fusermount3"]
fn dfs_buffered_reopen_refreshes_growth_and_truncation_with_new_heads() {
    let temp = tempfile::tempdir().unwrap();
    let mount = temp.path().join("mnt");
    std::fs::create_dir(&mount).unwrap();
    let backend = Arc::new(VersionedReadBackend::new("small", b"old".to_vec()));
    let _session = MountedTestBackend::mount_dfs_with_options(
        backend.clone(),
        mount.clone(),
        fuse::FuseMountOptions {
            fuse_max_readahead_bytes: 0,
            ..Default::default()
        },
    );
    assert_eq!(
        dfs_read_ahead_kb(&mount),
        0,
        "zero request must disable whole-page readahead"
    );
    let path = mount.join("file");
    assert_eq!(std::fs::read(&path).unwrap(), b"old");

    let grown = vec![b'g'; 131072];
    backend.publish("grown", grown.clone());
    assert_eq!(std::fs::read(&path).unwrap(), grown);
    backend.publish("truncated", b"new".to_vec());
    assert_eq!(std::fs::read(&path).unwrap(), b"new");
    backend.publish("empty", Vec::new());
    assert!(std::fs::read(&path).unwrap().is_empty());
}

fn dfs_read_ahead_kb(mount: &std::path::Path) -> u64 {
    use std::os::unix::fs::MetadataExt as _;
    let device = mount.metadata().unwrap().dev();
    std::fs::read_to_string(format!(
        "/sys/class/bdi/{}:{}/read_ahead_kb",
        libc::major(device),
        libc::minor(device)
    ))
    .unwrap()
    .trim()
    .parse()
    .unwrap()
}

#[test]
#[ignore = "requires Linux /dev/fuse and fusermount3"]
fn dfs_buffered_open_after_old_lookup_size_reads_fresh_length_without_fstat() {
    let temp = tempfile::tempdir().unwrap();
    let mount = temp.path().join("mnt");
    std::fs::create_dir(&mount).unwrap();
    let backend = Arc::new(VersionedReadBackend::new("old", b"old".to_vec()));
    let _session = MountedTestBackend::mount_dfs(backend.clone(), mount.clone());
    let grown = vec![b'n'; 8192];
    backend.publish_during_next_open("after-lookup", grown.clone());

    // File::open followed by read_exact does not issue an application fstat.
    // The backend changes head inside OPEN, after LOOKUP returned the old size.
    let mut file = std::fs::File::open(mount.join("file")).unwrap();
    assert_eq!(backend.lookup_size_at_publication.load(Ordering::SeqCst), 3);
    let mut observed = vec![0; grown.len()];
    file.read_exact(&mut observed).unwrap();
    assert_eq!(observed, grown);
    assert_eq!(file.read(&mut [0; 1]).unwrap(), 0);
}

#[test]
#[ignore = "requires Linux /dev/fuse and fusermount3"]
fn dfs_buffered_new_open_cannot_reuse_an_old_inflight_read_result() {
    for workers in [1, 2, 32] {
        assert_dfs_open_fences_inflight_read(workers);
    }
}

#[test]
#[ignore = "requires Linux /dev/fuse and fusermount3"]
fn dfs_buffered_same_descriptor_reads_enter_backend_concurrently() {
    for (workers, readers) in [(2, 2), (32, 32)] {
        assert_dfs_same_descriptor_parallel_reads(workers, readers);
    }
}

fn assert_dfs_same_descriptor_parallel_reads(workers: u16, readers: usize) {
    let temp = tempfile::tempdir().unwrap();
    let mount = temp.path().join("mnt");
    std::fs::create_dir(&mount).unwrap();
    let data: Vec<u8> = (0..readers)
        .flat_map(|index| std::iter::repeat_n(index as u8, 4096))
        .collect();
    let backend = Arc::new(VersionedReadBackend::new("parallel", data));
    let probe = Arc::new(ParallelReadProbe {
        target: readers as u64,
        ..Default::default()
    });
    *backend.parallel_read_probe.lock().unwrap() = Some(probe.clone());
    let _session = MountedTestBackend::mount_dfs_with_options(
        backend,
        mount.clone(),
        fuse::FuseMountOptions {
            fuse_workers: workers,
            fuse_max_readahead_bytes: 0,
            ..Default::default()
        },
    );
    let file = Arc::new(std::fs::File::open(mount.join("file")).unwrap());
    let start = Arc::new(std::sync::Barrier::new(readers + 1));
    let mut reads = Vec::new();
    for index in 0..readers {
        let file = file.clone();
        let start = start.clone();
        reads.push(std::thread::spawn(move || {
            let mut bytes = vec![0; 4096];
            start.wait();
            let count = file.read_at(&mut bytes, (index * 4096) as u64)?;
            io::Result::Ok((count, bytes))
        }));
    }
    start.wait();
    for (index, read) in reads.into_iter().enumerate() {
        let (count, bytes) = read.join().unwrap().unwrap();
        assert_eq!(count, 4096);
        assert!(bytes.iter().all(|byte| *byte == index as u8));
    }
    let peak = probe.peak.load(Ordering::SeqCst);
    assert!(peak >= 2, "same fd READ callbacks remained serial");
    assert!(peak <= u64::from(workers));
    assert_eq!(probe.active.load(Ordering::SeqCst), 0);
    eprintln!("parallel READ diagnostic workers={workers} readers={readers} backend_peak={peak}");
}

#[test]
#[ignore = "requires Linux /dev/fuse and fusermount3"]
fn dfs_same_cache_bucket_open_does_not_wait_for_another_inode_read() {
    use std::os::unix::fs::MetadataExt as _;

    let temp = tempfile::tempdir().unwrap();
    let mount = temp.path().join("mnt");
    std::fs::create_dir(&mount).unwrap();
    let backend = Arc::new(MockBackend::default());
    {
        let mut state = backend.state.lock().unwrap();
        for index in 0..65 {
            let inode = state.insert_node(FileKind::Regular, vec![b'b'; 4096]);
            state
                .entries
                .insert((1, OsString::from(format!("bucket-{index}"))), inode);
        }
    }
    let registry = afs_metrics::Registry::new();
    let metrics = fuse::FuseRequestMetrics::register(&registry).unwrap();
    let mut session = MountedTestBackend {
        mount: mount.clone(),
        session: Some(
            fuse::mount_dfs_with_metrics_and_options(
                backend.clone(),
                &mount,
                metrics,
                fuse::FuseMountOptions {
                    fuse_max_readahead_bytes: 0,
                    ..Default::default()
                },
            )
            .unwrap(),
        ),
    };
    wait_until_mounted(&mount).unwrap();
    // Materialize consecutive frontend inode identities, rather than assuming
    // that backend inode numbers are identical to kernel FUSE inode numbers.
    let mut inodes = Vec::new();
    for index in 0..65 {
        inodes.push(
            std::fs::metadata(mount.join(format!("bucket-{index}")))
                .unwrap()
                .ino(),
        );
    }
    assert_ne!(inodes[0], inodes[64]);
    assert_eq!(inodes[0] % 64, inodes[64] % 64);
    let file = std::fs::File::open(mount.join("bucket-0")).unwrap();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    *backend.read_blocker.lock().unwrap() = Some(WriteBlocker {
        entered: entered_tx,
        release: release_rx,
    });
    let reader = std::thread::spawn(move || {
        let mut bytes = [0; 4096];
        file.read_at(&mut bytes, 0).map(|count| (count, bytes))
    });
    let entered = entered_rx.recv_timeout(Duration::from_secs(5));
    let (opened_tx, opened_rx) = mpsc::channel();
    let other = mount.join("bucket-64");
    let opener = std::thread::spawn(move || {
        let started = std::time::Instant::now();
        let result = std::fs::File::open(other);
        let _ = opened_tx.send(());
        result.map(|file| (file, started.elapsed()))
    });
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let open_received = loop {
        let text = afs_metrics::encode_text(&registry).unwrap();
        if text.lines().any(|line| {
            line.starts_with("afs_fuse_callbacks_total{")
                && line.contains("operation=\"open\"")
                && line
                    .split_whitespace()
                    .last()
                    .unwrap()
                    .parse::<u64>()
                    .unwrap()
                    >= 2
        }) {
            break true;
        }
        if std::time::Instant::now() >= deadline {
            break false;
        }
        std::thread::sleep(Duration::from_millis(1));
    };
    let completed_while_read_held = opened_rx.recv_timeout(Duration::from_secs(5)).is_ok();
    // Always release and join both requests before asserting diagnostics, so a
    // failed assertion cannot strand a callback during normal mount teardown.
    let _ = release_tx.send(());
    let (count, bytes) = reader.join().unwrap().unwrap();
    let (other_file, elapsed) = opener.join().unwrap().unwrap();
    assert!(entered.is_ok() && open_received);
    assert_eq!(count, 4096);
    assert_eq!(bytes, [b'b'; 4096]);
    assert!(
        completed_while_read_held,
        "another inode OPEN did not complete while same-bucket READ remained blocked"
    );
    eprintln!(
        "bucket contention diagnostic inode_a={} inode_b={} open_wait_us={} workers={}",
        inodes[0],
        inodes[64],
        elapsed.as_micros(),
        fuse::FuseMountOptions::default().fuse_workers
    );
    // The blocked READ is the correctness barrier, independent of the elapsed
    // OPEN diagnostic. Same-inode OPEN fencing has its own retained contract.
    drop(other_file);
    session.session.take().unwrap().join().unwrap();
}

#[derive(Default)]
struct ParallelReadProbe {
    target: u64,
    active: AtomicU64,
    peak: AtomicU64,
    arrived: Mutex<u64>,
    changed: Condvar,
}

impl ParallelReadProbe {
    fn enter(&self) -> ParallelReadGuard<'_> {
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(active, Ordering::SeqCst);
        let mut arrived = self.arrived.lock().unwrap();
        *arrived += 1;
        self.changed.notify_all();
        drop(
            self.changed
                .wait_timeout_while(arrived, Duration::from_secs(3), |value| {
                    *value < self.target.max(2)
                })
                .unwrap(),
        );
        ParallelReadGuard(self)
    }
}

struct ParallelReadGuard<'a>(&'a ParallelReadProbe);

impl Drop for ParallelReadGuard<'_> {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::SeqCst);
    }
}

fn assert_dfs_open_fences_inflight_read(workers: u16) {
    let temp = tempfile::tempdir().unwrap();
    let mount = temp.path().join("mnt");
    std::fs::create_dir(&mount).unwrap();
    let old = vec![b'o'; 65536];
    let new = vec![b'n'; old.len()];
    let backend = Arc::new(VersionedReadBackend::new("old-inflight", old.clone()));
    let registry = afs_metrics::Registry::new();
    let metrics = fuse::FuseRequestMetrics::register(&registry).unwrap();
    let _session = MountedTestBackend {
        mount: mount.clone(),
        session: Some(
            fuse::mount_dfs_with_metrics_and_options(
                backend.clone(),
                &mount,
                metrics,
                fuse::FuseMountOptions {
                    fuse_workers: workers,
                    ..Default::default()
                },
            )
            .unwrap(),
        ),
    };
    wait_until_mounted(&mount).unwrap();
    let path = mount.join("file");
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    *backend.read_blocker.lock().unwrap() = Some(WriteBlocker {
        entered: entered_tx,
        release: release_rx,
    });
    let first_path = path.clone();
    let first = std::thread::spawn(move || {
        let mut file = std::fs::File::open(first_path)?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        io::Result::Ok((file, bytes))
    });
    let read_entered = entered_rx.recv_timeout(Duration::from_secs(5));
    backend.publish("new-inflight", new.clone());
    let (opened_tx, opened_rx) = mpsc::channel();
    let next_path = path.clone();
    let next = std::thread::spawn(move || {
        let mut file = std::fs::File::open(next_path)?;
        let _ = opened_tx.send(());
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        io::Result::Ok(bytes)
    });
    // With one worker even the competing LOOKUP must queue behind the old
    // READ. With two, require OPEN itself to reach the frontend before release.
    // Neither case requires the backend OPEN to bypass a correct local fence.
    let competing_operation = if workers == 1 { "lookup" } else { "open" };
    let operation_label = format!("operation=\"{competing_operation}\"");
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let new_open_received = loop {
        let metrics = afs_metrics::encode_text(&registry).unwrap();
        if metrics.lines().any(|line| {
            line.starts_with("afs_fuse_callbacks_total{")
                && line.contains("filesystem=\"dfs\"")
                && line.contains(&operation_label)
                && line
                    .split_whitespace()
                    .last()
                    .unwrap()
                    .parse::<u64>()
                    .unwrap()
                    >= 2
        }) {
            break true;
        }
        if std::time::Instant::now() >= deadline {
            break false;
        }
        std::thread::sleep(Duration::from_millis(1));
    };
    // Give a non-barrier implementation the chance to reply before the old
    // body is released. A correct barrier may keep this OPEN pending.
    let _ = opened_rx.recv_timeout(Duration::from_millis(100));
    let _ = release_tx.send(());
    let first_result = first.join().unwrap();
    let next_result = next.join().unwrap();
    assert!(
        read_entered.is_ok(),
        "old READ did not reach its body barrier"
    );
    assert!(new_open_received, "competing open path did not enter FUSE");
    // An existing descriptor is not promised a whole-file snapshot.
    let (mut old_file, first_bytes) = first_result.unwrap();
    assert_eq!(first_bytes.len(), old.len());
    assert!(
        next_result.unwrap() == new,
        "new OPEN reused an old in-flight body"
    );
    assert_eq!(std::fs::read(&path).unwrap(), new);
    old_file.rewind().unwrap();
    let mut old_handle_after_open = vec![0; new.len()];
    old_file.read_exact(&mut old_handle_after_open).unwrap();
    assert!(
        old_handle_after_open == new,
        "an old descriptor restored obsolete pages after fresh open"
    );
}

struct VersionedContent {
    identity: PageCacheIdentity,
    bytes: Vec<u8>,
}

struct VersionedReadState {
    current: Arc<VersionedContent>,
    next_open: Option<Arc<VersionedContent>>,
    handles: HashMap<u64, Arc<VersionedContent>>,
    next_handle: u64,
}

struct VersionedReadBackend {
    state: Mutex<VersionedReadState>,
    read_calls: AtomicU64,
    last_lookup_size: AtomicU64,
    lookup_size_at_publication: AtomicU64,
    read_blocker: Mutex<Option<WriteBlocker>>,
    parallel_read_probe: Mutex<Option<Arc<ParallelReadProbe>>>,
}

impl VersionedReadBackend {
    fn content(version: &str, bytes: Vec<u8>) -> Arc<VersionedContent> {
        Arc::new(VersionedContent {
            identity: PageCacheIdentity {
                version_id: Some(version.to_owned()),
                length: bytes.len() as u64,
            },
            bytes,
        })
    }

    fn new(version: &str, bytes: Vec<u8>) -> Self {
        Self {
            state: Mutex::new(VersionedReadState {
                current: Self::content(version, bytes),
                next_open: None,
                handles: HashMap::new(),
                next_handle: 1,
            }),
            read_calls: AtomicU64::new(0),
            last_lookup_size: AtomicU64::new(u64::MAX),
            lookup_size_at_publication: AtomicU64::new(u64::MAX),
            read_blocker: Mutex::new(None),
            parallel_read_probe: Mutex::new(None),
        }
    }

    fn publish(&self, version: &str, bytes: Vec<u8>) {
        self.state.lock().unwrap().current = Self::content(version, bytes);
    }

    fn publish_during_next_open(&self, version: &str, bytes: Vec<u8>) {
        self.state.lock().unwrap().next_open = Some(Self::content(version, bytes));
    }
}

impl Backend for VersionedReadBackend {
    fn root_inode(&self) -> BackendInode {
        backend_inode(1)
    }

    fn lookup(
        &self,
        _: &RequestContext,
        parent: BackendInode,
        name: &OsStr,
    ) -> afs_error::Result<Entry> {
        if parent.value != 1 || name != OsStr::new("file") {
            return Err(not_found());
        }
        let size = self.state.lock().unwrap().current.identity.length;
        self.last_lookup_size.store(size, Ordering::SeqCst);
        Ok(Entry {
            inode: backend_inode(2),
            attributes: attributes(FileKind::Regular, size),
        })
    }

    fn getattr(
        &self,
        _: &RequestContext,
        inode: BackendInode,
        handle: Option<FileHandle>,
    ) -> afs_error::Result<FileAttributes> {
        if inode.value == 1 {
            return Ok(attributes(FileKind::Directory, 0));
        }
        if inode.value != 2 {
            return Err(not_found());
        }
        let state = self.state.lock().unwrap();
        let content = handle
            .and_then(|handle| state.handles.get(&handle.0))
            .unwrap_or(&state.current);
        Ok(attributes(FileKind::Regular, content.identity.length))
    }

    fn open(
        &self,
        _: &RequestContext,
        inode: BackendInode,
        _: i32,
    ) -> afs_error::Result<FileHandle> {
        if inode.value != 2 {
            return Err(not_found());
        }
        let mut state = self.state.lock().unwrap();
        if let Some(next) = state.next_open.take() {
            self.lookup_size_at_publication.store(
                self.last_lookup_size.load(Ordering::SeqCst),
                Ordering::SeqCst,
            );
            state.current = next;
        }
        let handle = state.next_handle;
        state.next_handle += 1;
        let current = state.current.clone();
        // Native DFS fresh open invalidates existing handle views. Model the
        // resulting next-read refresh, without pinning old descriptors forever.
        // A READ that already captured its Arc still owns its prior version.
        for opened in state.handles.values_mut() {
            *opened = current.clone();
        }
        state.handles.insert(handle, current);
        Ok(FileHandle(handle))
    }

    fn page_cache_identity(&self, handle: FileHandle) -> Option<PageCacheIdentity> {
        self.state
            .lock()
            .unwrap()
            .handles
            .get(&handle.0)
            .map(|content| content.identity.clone())
    }

    fn supports_parallel_reads(&self, handle: FileHandle) -> bool {
        self.state.lock().unwrap().handles.contains_key(&handle.0)
    }

    fn read(
        &self,
        _: &RequestContext,
        handle: FileHandle,
        offset: u64,
        out: &mut [u8],
    ) -> afs_error::Result<usize> {
        self.read_calls.fetch_add(1, Ordering::SeqCst);
        let content = self
            .state
            .lock()
            .unwrap()
            .handles
            .get(&handle.0)
            .cloned()
            .ok_or_else(not_found)?;
        let probe = self.parallel_read_probe.lock().unwrap().clone();
        let _active = probe.as_ref().map(|probe| probe.enter());
        if let Some(blocker) = self.read_blocker.lock().unwrap().take() {
            blocker.entered.send(()).unwrap();
            blocker.release.recv().unwrap();
        }
        let start = usize::try_from(offset).unwrap().min(content.bytes.len());
        let count = out.len().min(content.bytes.len() - start);
        out[..count].copy_from_slice(&content.bytes[start..start + count]);
        Ok(count)
    }

    fn flush(&self, _: &RequestContext, _: FileHandle) -> afs_error::Result<()> {
        Ok(())
    }

    fn release(&self, _: &RequestContext, handle: FileHandle) -> afs_error::Result<()> {
        self.state.lock().unwrap().handles.remove(&handle.0);
        Ok(())
    }

    fn opendir(&self, _: &RequestContext, _: BackendInode) -> afs_error::Result<DirectoryHandle> {
        Ok(DirectoryHandle(1))
    }

    fn readdir(
        &self,
        _: &RequestContext,
        _: DirectoryHandle,
        _: u64,
        _: usize,
    ) -> afs_error::Result<Vec<DirectoryEntry>> {
        Ok(Vec::new())
    }

    fn releasedir(&self, _: &RequestContext, _: DirectoryHandle) -> afs_error::Result<()> {
        Ok(())
    }
}

impl Drop for MountedTestBackend {
    fn drop(&mut self) {
        if let Some(session) = self.session.take() {
            let _ = session.join();
        }
        let _ = std::process::Command::new("fusermount3")
            .arg("-u")
            .arg(&self.mount)
            .status();
    }
}

#[derive(Debug, Default)]
struct MockBackend {
    state: Mutex<MockState>,
    statfs_capacity: Mutex<Option<FilesystemCapacity>>,
    write_blocker: Mutex<Option<WriteBlocker>>,
    read_blocker: Mutex<Option<WriteBlocker>>,
    events: Mutex<Vec<&'static str>>,
    getattr_calls: AtomicU64,
    read_calls: AtomicU64,
    root_mkdir_parent: AtomicU64,
}

#[derive(Debug)]
struct WriteBlocker {
    entered: mpsc::Sender<()>,
    release: mpsc::Receiver<()>,
}

#[derive(Debug)]
struct MockState {
    next_inode: u64,
    next_handle: u64,
    entries: HashMap<(u64, OsString), u64>,
    nodes: HashMap<u64, MockNode>,
    file_handles: HashMap<u64, u64>,
    dir_handles: HashMap<u64, u64>,
}

impl Default for MockState {
    fn default() -> Self {
        Self {
            next_inode: 2,
            next_handle: 1,
            entries: HashMap::new(),
            nodes: HashMap::from([(
                1,
                MockNode {
                    kind: FileKind::Directory,
                    data: Vec::new(),
                },
            )]),
            file_handles: HashMap::new(),
            dir_handles: HashMap::new(),
        }
    }
}

#[derive(Clone, Debug)]
struct MockNode {
    kind: FileKind,
    data: Vec<u8>,
}

impl Backend for MockBackend {
    fn root_inode(&self) -> BackendInode {
        backend_inode(1)
    }

    fn lookup(
        &self,
        _: &RequestContext,
        parent: BackendInode,
        name: &OsStr,
    ) -> afs_error::Result<Entry> {
        let state = self.state.lock().unwrap();
        let inode = state
            .entries
            .get(&(parent.value, name.to_os_string()))
            .copied()
            .ok_or_else(not_found)?;
        Ok(entry(inode, state.nodes.get(&inode).unwrap(), 0))
    }

    fn getattr(
        &self,
        _: &RequestContext,
        inode: BackendInode,
        handle: Option<FileHandle>,
    ) -> afs_error::Result<FileAttributes> {
        let state = self.state.lock().unwrap();
        let inode = handle
            .and_then(|handle| state.file_handles.get(&handle.0).copied())
            .unwrap_or(inode.value);
        let node = state.nodes.get(&inode).ok_or_else(not_found)?;
        let call = self.getattr_calls.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(attributes(node.kind, node.data.len() as u64 + call))
    }

    fn statfs(&self, _: &RequestContext, _: BackendInode) -> afs_error::Result<FilesystemCapacity> {
        self.statfs_capacity.lock().unwrap().clone().ok_or_else(|| {
            afs_error::Error::coded(afs_error::NODE_VFS_UNIMPLEMENTED, "mock statfs unsupported")
        })
    }

    fn create(
        &self,
        _: &RequestContext,
        parent: BackendInode,
        name: &OsStr,
        _: u32,
        _: i32,
    ) -> afs_error::Result<CreatedFile> {
        let mut state = self.state.lock().unwrap();
        if state
            .entries
            .contains_key(&(parent.value, name.to_os_string()))
        {
            return Err(afs_error::Error::coded(
                afs_error::NODE_MOUNT_CONFLICT,
                "entry already exists",
            ));
        }
        let inode = state.insert_node(FileKind::Regular, Vec::new());
        state
            .entries
            .insert((parent.value, name.to_os_string()), inode);
        let handle = state.insert_file_handle(inode);
        let node = state.nodes.get(&inode).unwrap();
        Ok(CreatedFile {
            entry: entry(inode, node, 0),
            handle: FileHandle(handle),
        })
    }

    fn open(
        &self,
        _: &RequestContext,
        inode: BackendInode,
        _: i32,
    ) -> afs_error::Result<FileHandle> {
        let mut state = self.state.lock().unwrap();
        if !state.nodes.contains_key(&inode.value) {
            return Err(not_found());
        }
        let handle = state.insert_file_handle(inode.value);
        Ok(FileHandle(handle))
    }

    fn read(
        &self,
        _: &RequestContext,
        handle: FileHandle,
        offset: u64,
        out: &mut [u8],
    ) -> afs_error::Result<usize> {
        self.read_calls.fetch_add(1, Ordering::SeqCst);
        let blocker = self.read_blocker.lock().unwrap().take();
        if let Some(blocker) = blocker {
            blocker.entered.send(()).unwrap();
            blocker.release.recv().unwrap();
        }
        let state = self.state.lock().unwrap();
        let inode = *state.file_handles.get(&handle.0).ok_or_else(not_found)?;
        let data = &state.nodes.get(&inode).ok_or_else(not_found)?.data;
        let offset = offset as usize;
        if offset >= data.len() {
            return Ok(0);
        }
        let end = data.len().min(offset + out.len());
        let len = end - offset;
        out[..len].copy_from_slice(&data[offset..end]);
        Ok(len)
    }

    fn write(
        &self,
        _: &RequestContext,
        handle: FileHandle,
        offset: u64,
        data: &[u8],
    ) -> afs_error::Result<usize> {
        self.events.lock().unwrap().push("write:start");
        let blocker = self.write_blocker.lock().unwrap().take();
        if let Some(blocker) = blocker {
            blocker.entered.send(()).unwrap();
            blocker.release.recv().unwrap();
        }
        let mut state = self.state.lock().unwrap();
        let inode = *state.file_handles.get(&handle.0).ok_or_else(not_found)?;
        let node = state.nodes.get_mut(&inode).ok_or_else(not_found)?;
        let offset = offset as usize;
        if node.data.len() < offset {
            node.data.resize(offset, 0);
        }
        if node.data.len() < offset + data.len() {
            node.data.resize(offset + data.len(), 0);
        }
        node.data[offset..offset + data.len()].copy_from_slice(data);
        self.events.lock().unwrap().push("write:finish");
        Ok(data.len())
    }

    fn setattr(
        &self,
        _: &RequestContext,
        inode: BackendInode,
        handle: Option<FileHandle>,
        change: &afs::node::vfs::types::AttributeChange,
    ) -> afs_error::Result<FileAttributes> {
        self.events.lock().unwrap().push("setattr");
        let mut state = self.state.lock().unwrap();
        let inode = handle
            .and_then(|handle| state.file_handles.get(&handle.0).copied())
            .unwrap_or(inode.value);
        let node = state.nodes.get_mut(&inode).ok_or_else(not_found)?;
        if let Some(size) = change.size {
            node.data.resize(size as usize, 0);
        }
        if let Some(mode) = change.mode {
            let _ = mode;
        }
        Ok(attributes(node.kind, node.data.len() as u64))
    }

    fn flush(&self, _: &RequestContext, handle: FileHandle) -> afs_error::Result<()> {
        let state = self.state.lock().unwrap();
        state
            .file_handles
            .contains_key(&handle.0)
            .then_some(())
            .ok_or_else(not_found)
    }

    fn fsync(&self, _: &RequestContext, handle: FileHandle, _: SyncMode) -> afs_error::Result<()> {
        self.flush(&request_context(), handle)
    }

    fn release(&self, _: &RequestContext, handle: FileHandle) -> afs_error::Result<()> {
        let mut state = self.state.lock().unwrap();
        state.file_handles.remove(&handle.0);
        Ok(())
    }

    fn opendir(
        &self,
        _: &RequestContext,
        inode: BackendInode,
    ) -> afs_error::Result<DirectoryHandle> {
        let mut state = self.state.lock().unwrap();
        if !matches!(
            state.nodes.get(&inode.value).map(|node| node.kind),
            Some(FileKind::Directory)
        ) {
            return Err(not_found());
        }
        let handle = state.insert_dir_handle(inode.value);
        Ok(DirectoryHandle(handle))
    }

    fn readdir(
        &self,
        _: &RequestContext,
        handle: DirectoryHandle,
        cookie: u64,
        _: usize,
    ) -> afs_error::Result<Vec<DirectoryEntry>> {
        let state = self.state.lock().unwrap();
        let parent = *state.dir_handles.get(&handle.0).ok_or_else(not_found)?;
        let mut children = state
            .entries
            .iter()
            .filter(|((entry_parent, _), _)| *entry_parent == parent)
            .map(|((_, name), inode)| (name.clone(), *inode))
            .collect::<Vec<_>>();
        children.sort_by(|left, right| left.0.cmp(&right.0));
        Ok(children
            .into_iter()
            .enumerate()
            .skip(cookie.saturating_sub(3) as usize)
            .map(|(index, (name, inode))| DirectoryEntry {
                name,
                inode: backend_inode(inode),
                kind: state.nodes.get(&inode).unwrap().kind,
                next_cookie: index as u64 + 4,
            })
            .collect())
    }

    fn fsyncdir(
        &self,
        _: &RequestContext,
        handle: DirectoryHandle,
        _: SyncMode,
    ) -> afs_error::Result<()> {
        let state = self.state.lock().unwrap();
        state
            .dir_handles
            .contains_key(&handle.0)
            .then_some(())
            .ok_or_else(not_found)
    }

    fn releasedir(&self, _: &RequestContext, handle: DirectoryHandle) -> afs_error::Result<()> {
        let mut state = self.state.lock().unwrap();
        state.dir_handles.remove(&handle.0);
        Ok(())
    }

    fn mkdir(
        &self,
        _: &RequestContext,
        parent: BackendInode,
        name: &OsStr,
        _: u32,
    ) -> afs_error::Result<Entry> {
        self.root_mkdir_parent.store(parent.value, Ordering::SeqCst);
        let mut state = self.state.lock().unwrap();
        let inode = state.insert_node(FileKind::Directory, Vec::new());
        state
            .entries
            .insert((parent.value, name.to_os_string()), inode);
        Ok(entry(inode, state.nodes.get(&inode).unwrap(), 0))
    }

    fn unlink(
        &self,
        _: &RequestContext,
        parent: BackendInode,
        name: &OsStr,
    ) -> afs_error::Result<()> {
        let mut state = self.state.lock().unwrap();
        state
            .entries
            .remove(&(parent.value, name.to_os_string()))
            .map(|_| ())
            .ok_or_else(not_found)
    }

    fn rmdir(
        &self,
        _: &RequestContext,
        parent: BackendInode,
        name: &OsStr,
    ) -> afs_error::Result<()> {
        self.unlink(&request_context(), parent, name)
    }

    fn rename(
        &self,
        _: &RequestContext,
        from_parent: BackendInode,
        from_name: &OsStr,
        to_parent: BackendInode,
        to_name: &OsStr,
        _: RenameFlags,
    ) -> afs_error::Result<()> {
        let mut state = self.state.lock().unwrap();
        let inode = state
            .entries
            .remove(&(from_parent.value, from_name.to_os_string()))
            .ok_or_else(not_found)?;
        state
            .entries
            .insert((to_parent.value, to_name.to_os_string()), inode);
        Ok(())
    }
}

impl MockBackend {
    fn with_statfs_capacity(capacity: FilesystemCapacity) -> Self {
        Self {
            statfs_capacity: Mutex::new(Some(capacity)),
            ..Self::default()
        }
    }

    fn block_next_write(&self, entered: mpsc::Sender<()>, release: mpsc::Receiver<()>) {
        *self.write_blocker.lock().unwrap() = Some(WriteBlocker { entered, release });
    }

    fn events(&self) -> Vec<&'static str> {
        self.events.lock().unwrap().clone()
    }
}

impl MockState {
    fn insert_node(&mut self, kind: FileKind, data: Vec<u8>) -> u64 {
        let inode = self.next_inode;
        self.next_inode += 1;
        self.nodes.insert(inode, MockNode { kind, data });
        inode
    }

    fn insert_file_handle(&mut self, inode: u64) -> u64 {
        let handle = self.next_handle;
        self.next_handle += 1;
        self.file_handles.insert(handle, inode);
        handle
    }

    fn insert_dir_handle(&mut self, inode: u64) -> u64 {
        let handle = self.next_handle;
        self.next_handle += 1;
        self.dir_handles.insert(handle, inode);
        handle
    }
}

fn entry(inode: u64, node: &MockNode, extra_size: u64) -> Entry {
    Entry {
        inode: backend_inode(inode),
        attributes: attributes(node.kind, node.data.len() as u64 + extra_size),
    }
}

fn attributes(kind: FileKind, size: u64) -> FileAttributes {
    FileAttributes {
        kind,
        size,
        blocks: size.div_ceil(512),
        mode: match kind {
            FileKind::Directory => 0o755,
            FileKind::Regular => 0o644,
            FileKind::Symlink => 0o777,
            FileKind::Special(_) => 0o644,
        },
        uid: 0,
        gid: 0,
        nlink: 1,
        atime: UNIX_EPOCH,
        mtime: UNIX_EPOCH,
        ctime: UNIX_EPOCH,
    }
}

fn backend_inode(value: u64) -> BackendInode {
    BackendInode { value }
}

fn not_found() -> afs_error::Error {
    afs_error::Error::coded(afs_error::NODE_VFS_NOT_FOUND, "mock entry not found")
}

fn request_context() -> RequestContext {
    RequestContext {
        uid: 0,
        gid: 0,
        pid: 0,
        umask: 0,
        supplementary_gids: Vec::new(),
    }
}

/// Real product Nodes are provisioned by the existing external finite scenario.
/// These tests do not deploy services or assume a particular VM manager.
#[cfg(feature = "dfs")]
mod real_dfs_keep_cache_contract {
    use std::{
        ffi::OsString,
        fs::{File, OpenOptions, Permissions},
        io::{self, Read, Seek, SeekFrom, Write},
        net::SocketAddr,
        os::unix::fs::{FileExt, MetadataExt, PermissionsExt},
        path::{Path, PathBuf},
        time::Duration,
    };

    const FILE_BYTES: usize = 16 * 1024 * 1024;
    const PAGE_BYTES: usize = 4096;
    const TAIL_OFFSET: u64 = 12 * 1024 * 1024;

    fn required_value(name: &str, value: Option<OsString>) -> io::Result<OsString> {
        value.filter(|value| !value.is_empty()).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("required external real-DFS fixture parameter {name} is missing"),
            )
        })
    }

    fn real_dfs_mount(name: &str) -> io::Result<PathBuf> {
        let path = PathBuf::from(required_value(name, std::env::var_os(name))?);
        if !path.is_absolute() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "mount must be absolute",
            ));
        }
        let path = path.canonicalize()?;
        let output = std::process::Command::new("findmnt")
            .args(["--json", "--target"])
            .arg(&path)
            .args(["--output", "TARGET,SOURCE,FSTYPE"])
            .output()?;
        if !output.status.success() {
            return Err(io::Error::other(format!(
                "{name} is not inside a mounted filesystem"
            )));
        }
        let value: serde_json::Value = serde_json::from_slice(&output.stdout)?;
        let mounts = value["filesystems"].as_array().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "findmnt did not return filesystems",
            )
        })?;
        if mounts.len() != 1
            || mounts[0]["source"].as_str() != Some("afs-dfs")
            || !mounts[0]["fstype"]
                .as_str()
                .is_some_and(|kind| kind.starts_with("fuse"))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "expected one real afs-dfs FUSE mount",
            ));
        }
        Ok(path)
    }

    fn dfs_read_callbacks(text: &str) -> io::Result<u64> {
        let mut result = None;
        for line in text.lines() {
            let Some((labels, value)) = line.rsplit_once(' ') else {
                continue;
            };
            if labels.starts_with("afs_fuse_callbacks_total{")
                && labels.contains("filesystem=\"dfs\"")
                && labels.contains("operation=\"read\"")
            {
                let count = value.parse().map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "invalid DFS READ counter")
                })?;
                if result.replace(count).is_some() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "duplicate DFS READ counter",
                    ));
                }
            }
        }
        result.ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "DFS READ counter is missing")
        })
    }

    fn read_counter(address: SocketAddr) -> io::Result<u64> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        runtime.block_on(async {
            // Match the repository's selected provider when constructing the
            // client. An already-installed provider remains unchanged.
            let _ = rustls::crypto::ring::default_provider().install_default();
            let client = reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .map_err(io::Error::other)?;
            let mut response = client
                .get(format!("http://{address}/metrics"))
                .send()
                .await
                .map_err(io::Error::other)?
                .error_for_status()
                .map_err(io::Error::other)?;
            let mut body = Vec::new();
            while let Some(chunk) = response.chunk().await.map_err(io::Error::other)? {
                if body.len().saturating_add(chunk.len()) > 2 * 1024 * 1024 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "metrics response exceeds 2MiB",
                    ));
                }
                body.extend_from_slice(&chunk);
            }
            let text = std::str::from_utf8(&body)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
            dfs_read_callbacks(text)
        })
    }

    fn assert_full_read(reader: &mut impl Read, expected: &[u8]) -> io::Result<blake3::Hash> {
        let mut actual = vec![0; expected.len()];
        reader.read_exact(&mut actual)?;
        let mut eof = [0; 1];
        if reader.read(&mut eof)? != 0 || actual != expected {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "full read length, content or EOF differs",
            ));
        }
        let root = blake3::hash(&actual);
        if root != blake3::hash(expected) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "full read BLAKE3 root differs",
            ));
        }
        Ok(root)
    }

    fn durable_write(path: &Path, payload: &[u8], create: bool) -> io::Result<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(create)
            .open(path)?;
        file.write_all(payload)?;
        file.sync_all()?;
        if create {
            File::open(
                path.parent()
                    .ok_or_else(|| io::Error::other("test file has no parent"))?,
            )?
            .sync_all()?;
        }
        Ok(())
    }

    #[test]
    #[ignore = "requires external real DFS A/B mounts and B metrics on one Linux host; run as non-root"]
    fn dfs_real_nodes_cross_handle_keep_cache_and_permission() -> io::Result<()> {
        // SAFETY: geteuid only reads this process's effective identity; the test
        // never changes credentials in the multithreaded libtest process.
        let uid = unsafe { libc::geteuid() };
        assert_ne!(uid, 0, "run this whole test as a non-root fixture user");
        let writer = real_dfs_mount("AFS_DFS_FUSE_WRITER_MOUNT")?;
        let reader = real_dfs_mount("AFS_DFS_FUSE_READER_MOUNT")?;
        let metrics = required_value(
            "AFS_DFS_FUSE_READER_METRICS",
            std::env::var_os("AFS_DFS_FUSE_READER_METRICS"),
        )?
        .into_string()
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "metrics SocketAddr must be UTF-8",
            )
        })?
        .parse::<SocketAddr>()
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        assert_ne!(
            writer.metadata()?.dev(),
            reader.metadata()?.dev(),
            "A and B must be distinct FUSE mounts"
        );
        assert_eq!(
            super::dfs_read_ahead_kb(&reader),
            0,
            "fixture must disable B readahead"
        );
        // Fresh, caller-owned directory prevents an existing page-cache entry
        // from satisfying H1, and prevents this workload changing other files.
        let directory = tempfile::Builder::new()
            .prefix("afs-h123-")
            .tempdir_in(&writer)?;
        assert_eq!(directory.path().metadata()?.uid(), uid);
        let relative = directory
            .path()
            .file_name()
            .ok_or_else(|| io::Error::other("test directory has no name"))?;
        let path_a = directory.path().join("visibility.bin");
        let path_b = reader.join(relative).join("visibility.bin");
        let mut expected: Vec<u8> = (0..FILE_BYTES).map(|index| index as u8).collect();
        durable_write(&path_a, &expected, true)?;
        let inode = path_a.metadata()?.ino();
        let mut h1 = File::open(&path_b)?;
        let mut page = [0; PAGE_BYTES];
        h1.read_exact(&mut page)?;
        assert_eq!(page, expected[..PAGE_BYTES]);
        let before_v2 = read_counter(metrics)?;
        for byte in &mut expected {
            *byte = 255 - *byte;
        }
        // No truncate/recreate: publish V2 over the exact same inode.
        durable_write(&path_a, &expected, false)?;
        assert_eq!(path_a.metadata()?.ino(), inode);
        let mut h2 = File::open(&path_b)?;
        h2.read_exact(&mut page)?;
        assert_eq!(page, expected[..PAGE_BYTES]);
        let after_h2 = read_counter(metrics)?;
        assert!(after_h2 > before_v2, "H2 reused V1's cached page");
        h1.read_exact_at(&mut page, TAIL_OFFSET)?;
        assert_eq!(
            page,
            expected[TAIL_OFFSET as usize..TAIL_OFFSET as usize + PAGE_BYTES]
        );
        let after_h1 = read_counter(metrics)?;
        assert!(after_h1 > after_h2, "H1 uncached tail did not enter FUSE");
        h2.seek(SeekFrom::Start(0))?;
        let root = assert_full_read(&mut h2, &expected)?;
        // H1 still owns the old fixed view: another OPEN must keep taking the
        // safe direct path instead of mixing two versions in one inode cache.
        let before_conflicting_reopen = read_counter(metrics)?;
        let mut conflicting_reopen = File::open(&path_b)?;
        assert_eq!(assert_full_read(&mut conflicting_reopen, &expected)?, root);
        let after_conflicting_reopen = read_counter(metrics)?;
        assert!(
            after_conflicting_reopen > before_conflicting_reopen,
            "a conflicting old view must prevent shared page-cache admission"
        );
        // Test zero-READ same-version reuse on an independent inode, where no
        // old-version handle can force that fallback. Keep the strict equality.
        let stable_a = directory.path().join("same-version.bin");
        let stable_b = reader.join(relative).join("same-version.bin");
        durable_write(&stable_a, &expected, true)?;
        let mut warm = File::open(&stable_b)?;
        assert_eq!(assert_full_read(&mut warm, &expected)?, root);
        let before_h3 = read_counter(metrics)?;
        let mut h3 = File::open(&stable_b)?;
        assert_eq!(assert_full_read(&mut h3, &expected)?, root);
        let after_h3 = read_counter(metrics)?;
        assert_eq!(
            after_h3, before_h3,
            "H3 discarded same-version cached pages"
        );
        // New OPEN must enforce changed permissions. Existing handles are not
        // used to claim immediate revocation of already granted access.
        std::fs::set_permissions(&path_a, Permissions::from_mode(0o000))?;
        let denied = File::open(&path_b);
        std::fs::set_permissions(&path_a, Permissions::from_mode(0o600))?;
        assert!(
            matches!(denied, Err(ref error) if error.raw_os_error() == Some(libc::EACCES)),
            "mode 000 must deny a new non-root OPEN"
        );
        let mut restored = File::open(&path_b)?;
        assert_eq!(assert_full_read(&mut restored, &expected)?, root);
        eprintln!(
            "{}",
            serde_json::json!({
                "case": "real-dfs-h1-h2-h3", "uid": uid, "bytes": expected.len(),
                "blake3": root.to_hex().to_string(), "eof": true,
                "read_callbacks": {"before_v2": before_v2, "after_h2": after_h2,
                    "after_h1_tail": after_h1, "before_h3": before_h3, "after_h3": after_h3},
                "conflicting_reopen": {"before": before_conflicting_reopen,
                    "after": after_conflicting_reopen, "direct_fallback_required": true},
                "h3_scope": "independent inode, unchanged version, no conflicting handles",
                "new_open_mode_000_denied": true, "old_handle_snapshot_promised": false,
                "topology": "one Linux host, two real Nodes, external local-file Meta fixture",
                "performance_claim": false
            })
        );
        drop((h1, h2, conflicting_reopen, warm, h3, restored));
        directory.close()?;
        Ok(())
    }

    #[test]
    fn real_dfs_contract_helpers_fail_on_missing_or_invalid_evidence() {
        assert!(required_value("mount", None).is_err());
        assert!(required_value("mount", Some(OsString::new())).is_err());
        assert!(dfs_read_callbacks("# no metrics\n").is_err());
        assert!(
            dfs_read_callbacks(
                "afs_fuse_callbacks_total{filesystem=\"ownerfs\",operation=\"read\"} 2\n"
            )
            .is_err()
        );
        let metric = "afs_fuse_callbacks_total{filesystem=\"dfs\",operation=\"read\"}";
        assert_eq!(dfs_read_callbacks(&format!("{metric} 7\n")).unwrap(), 7);
        assert!(dfs_read_callbacks(&format!("{metric} invalid\n")).is_err());
        assert!(dfs_read_callbacks(&format!("{metric} 7\n{metric} 8\n")).is_err());
        assert!(assert_full_read(&mut &b"abc"[..], b"abcd").is_err());
        assert!(assert_full_read(&mut &b"abcde"[..], b"abcd").is_err());
        assert!(assert_full_read(&mut &b"abce"[..], b"abcd").is_err());
        assert_eq!(
            assert_full_read(&mut &b"abcd"[..], b"abcd").unwrap(),
            blake3::hash(b"abcd")
        );
    }
}
