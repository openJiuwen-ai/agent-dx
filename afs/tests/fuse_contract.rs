use std::{
    collections::HashMap,
    ffi::{OsStr, OsString},
    io::{self, Read, Seek, Write},
    os::unix::fs::FileExt,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
        mpsc,
    },
    time::{Duration, UNIX_EPOCH},
};

use afs::node::{
    fuse,
    vfs::{
        Backend,
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
    let session = fuse::mount_test_backend(backend.clone(), &mount).unwrap();
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
