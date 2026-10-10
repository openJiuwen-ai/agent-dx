// Included inside private native_home_tests only. Run one exact ignored test
// inside an owned private Linux mount namespace; this is not a native manager.
fn n2a_mount_command(program: &str, arguments: &[&std::path::Path]) {
    let output = std::process::Command::new(program)
        .args(arguments)
        .output()
        .unwrap();
    println!(
        "N2A_MOUNT_COMMAND program={program:?} arguments={arguments:?} status={:?} stdout={:?} stderr={:?}",
        output.status.code(),
        output.stdout,
        output.stderr
    );
    assert!(output.status.success(), "owned normal mount command failed");
}

struct N2aOwnedCover {
    target: std::path::PathBuf,
    mounted: bool,
}

impl N2aOwnedCover {
    fn unmount(&mut self) {
        n2a_mount_command("umount", &[self.target.as_path()]);
        self.mounted = false;
    }
}

impl Drop for N2aOwnedCover {
    fn drop(&mut self) {
        if self.mounted {
            // Keep all owned data on failure. No lazy unmount and no recursive
            // deletion are permitted while a cover could remain mounted.
            let result = std::process::Command::new("umount")
                .arg(&self.target)
                .output();
            println!("N2A_FAILURE_NORMAL_UNMOUNT {result:?}");
        }
    }
}

#[test]
#[ignore = "requires exact Linux ARM64 private namespace, root and /dev/fuse"]
fn native_home_real_covered_root_lifecycle() {
    assert_eq!(std::env::consts::OS, "linux");
    assert_eq!(std::env::consts::ARCH, "aarch64");
    let uid = std::process::Command::new("id").arg("-u").output().unwrap();
    assert!(uid.status.success());
    assert_eq!(uid.stdout, b"0\n");

    let (temp, fs, ctx, disk) = fixture(true);
    // Retain the directory even if an assertion or normal unmount fails.
    let retained = temp.keep();
    let root = mkdir_root(&fs, &ctx, "native");
    let authority = fs
        .native_home_export_for_current_namespace(OsStr::new("native"))
        .unwrap();
    let source = retained.join(authority.data_dir().as_path());
    let file_source = source.join("file");
    fs::write(&file_source, vec![b'N'; 4096]).unwrap();
    let file = Backend::lookup(&fs, &ctx, root.inode, OsStr::new("file")).unwrap();
    let mount = retained.join("n2a-mount");
    fs::create_dir(&mount).unwrap();
    let target = mount.join("native");
    let ownerfs = Arc::new(fs);
    let namespace = fs::metadata("/proc/self/ns/mnt").unwrap();
    println!(
        "N2A_FUSE_OWNED root={retained:?} namespace_dev={} namespace_ino={}",
        namespace.dev(),
        namespace.ino()
    );
    println!(
        "N2A_MOUNTINFO_BEFORE {}",
        fs::read_to_string("/proc/self/mountinfo").unwrap()
    );
    let session = crate::node::fuse::mount_ownerfs(ownerfs.clone(), &mount).unwrap();
    assert!(fs::metadata(&target).unwrap().is_dir());
    ownerfs.with_fuse_cache_policy(file.inode, |ttl, private| {
        assert_eq!(ttl, Duration::ZERO);
        assert!(!private);
    });
    let mmap = std::process::Command::new("python3")
        .arg("-c")
        .arg("import mmap,os,sys; f=open(sys.argv[1],'r+b',buffering=0); m=mmap.mmap(f.fileno(),4096); m[:4]=b'n2a!'; m.flush(); os.fsync(f.fileno()); m.close(); f.close(); assert open(sys.argv[1],'rb').read()==b'n2a!'+b'N'*4092")
        .arg(target.join("file"))
        .output()
        .unwrap();
    println!(
        "N2A_ELIGIBLE_FUSE_MMAP_SMOKE_CURRENT_OPEN_POLICY status={:?} stdout={:?} stderr={:?}",
        mmap.status.code(),
        mmap.stdout,
        mmap.stderr
    );
    assert!(mmap.status.success());
    assert_eq!(
        fs::read(&file_source).unwrap(),
        [b"n2a!".as_slice(), &vec![b'N'; 4092]].concat()
    );

    n2a_mount_command("mount", &[std::path::Path::new("--bind"), &source, &target]);
    let mut cover = N2aOwnedCover {
        target: target.clone(),
        mounted: true,
    };
    let native = fs::metadata(&target).unwrap();
    let physical = fs::metadata(&source).unwrap();
    assert_eq!(
        (native.dev(), native.ino()),
        (physical.dev(), physical.ino())
    );
    println!(
        "N2A_MOUNTINFO_COVERED {}",
        fs::read_to_string("/proc/self/mountinfo").unwrap()
    );

    ownerfs
        .require_local()
        .unwrap()
        .roots
        .revoke_root(authority.root_id());
    assert!(authority.verify_current(&ownerfs).is_err());
    assert!(Backend::lookup(ownerfs.as_ref(), &ctx, root.inode, OsStr::new("file")).is_err());
    assert!(Backend::open(ownerfs.as_ref(), &ctx, file.inode, libc::O_RDONLY).is_err());
    assert_eq!(
        Backend::lookup(
            ownerfs.as_ref(),
            &ctx,
            ownerfs.root_inode(),
            OsStr::new("native")
        )
        .unwrap()
        .inode,
        root.inode
    );
    Backend::getattr(ownerfs.as_ref(), &ctx, root.inode, None).unwrap();

    // The anchor remains available while the real covered root is normally
    // uncovered. No final runtime clone or production READY is involved.
    cover.unmount();
    assert!(fs::metadata(&target).unwrap().is_dir());
    assert!(fs::read(target.join("file")).is_err());
    drop(authority);
    assert!(
        Backend::lookup(
            ownerfs.as_ref(),
            &ctx,
            ownerfs.root_inode(),
            OsStr::new("native")
        )
        .is_err()
    );
    assert!(Backend::getattr(ownerfs.as_ref(), &ctx, root.inode, None).is_err());
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while fs::metadata(&target).is_ok() {
        assert!(
            std::time::Instant::now() < deadline,
            "kernel root metadata persisted after backend anchor drop; mountinfo={}",
            fs::read_to_string("/proc/self/mountinfo").unwrap()
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    // Normal outer FUSE unmount/shutdown follows normal bind-cover unmount.
    session.join().unwrap();
    let after = fs::read_to_string("/proc/self/mountinfo").unwrap();
    println!("N2A_MOUNTINFO_AFTER {after}");
    assert!(!after.lines().any(|line| {
        line.split_whitespace()
            .nth(4)
            .is_some_and(|path| path.starts_with(retained.to_str().unwrap()))
    }));
    drop(cover);
    drop(ownerfs);
    drop(disk);
    fs::remove_dir_all(&retained).unwrap();
    println!("N2A_COVERED_ROOT_LIFECYCLE PASS normal_unmount=true ready=false runtime_clone=false");
}

#[test]
#[ignore = "requires exact Linux ARM64 private namespace, root and /dev/fuse"]
fn native_home_workspace_bind_mount_core_covers_real_fuse_root_and_detaches_busy() {
    assert_eq!(std::env::consts::OS, "linux");
    assert_eq!(std::env::consts::ARCH, "aarch64");
    let uid = std::process::Command::new("id").arg("-u").output().unwrap();
    assert!(uid.status.success());
    assert_eq!(uid.stdout, b"0\n");

    let (temp, fs, ctx, disk) = fixture(true);
    let retained = temp.keep();
    let root = mkdir_root(&fs, &ctx, "native");
    let mut authority = Some(
        fs.native_home_export_for_current_namespace(OsStr::new("native"))
            .unwrap(),
    );
    authority.as_ref().unwrap().verify_current(&fs).unwrap();

    let ownerfs = Arc::new(fs);
    let mount = retained.join("core-bind-fuse");
    std::fs::create_dir(&mount).unwrap();
    let target = mount.join("native");
    let mut session =
        N2aFuseSession::new(crate::node::fuse::mount_ownerfs(ownerfs.clone(), &mount).unwrap());
    assert!(std::fs::metadata(&target).unwrap().is_dir());
    ownerfs.with_fuse_cache_policy(root.inode, |ttl, private| {
        assert_eq!(ttl, Duration::ZERO);
        assert!(!private);
    });

    let source = authority.as_ref().unwrap().source_descriptor().unwrap();
    let source_identity = n2a_directory_identity(&source).unwrap();
    let fuse_dir = std::fs::File::open(&target).unwrap();
    let fuse_identity = n2a_directory_identity(&fuse_dir).unwrap();
    let namespace = std::fs::metadata("/proc/thread-self/ns/mnt").unwrap();
    let covered_observed = n2a_observe_target_mount(&target).unwrap();
    assert_eq!(covered_observed.identity, fuse_identity);
    assert_eq!(
        n2a_mountinfo_mountpoint(&covered_observed.mountinfo_line),
        Some(mount.to_str().unwrap())
    );
    let covered_mount_id = covered_observed.mount_id;
    println!(
        "N2A_CORE_BIND_FIXTURE root={retained:?} namespace_dev={} namespace_ino={} source={source_identity:?} covered={fuse_identity:?} covered_mount_id={covered_mount_id} covered_line={:?}",
        namespace.dev(),
        namespace.ino(),
        covered_observed.mountinfo_line
    );
    let parent = std::fs::File::open(&mount).unwrap();
    let fuse_file = n2a_fd_child_path(&fuse_dir, "cross.txt");

    {
        let mut bind =
            super::bind_mount::WorkspaceBindMount::prepare(source, parent, OsStr::new("native"))
                .unwrap();
        bind.activate().unwrap();
        let claim = bind.mount_identity().unwrap();
        assert_eq!(bind.source_identity(), source_identity);
        assert_eq!(claim.source, source_identity);
        assert_eq!(claim.covered_target, fuse_identity);
        let observed = n2a_observe_target_mount(&target).unwrap();
        assert_eq!(observed.identity, source_identity);
        assert_eq!(observed.mount_id, claim.mount_id);
        assert_eq!(
            n2a_mountinfo_mountpoint(&observed.mountinfo_line),
            Some(target.to_str().unwrap())
        );
        assert!(
            observed.mountinfo_line.contains(" - "),
            "mountinfo line did not include separator: {:?}",
            observed.mountinfo_line
        );
        println!(
            "N2A_CORE_BIND_ACTIVE claim={claim:?} observed_line={:?}",
            observed.mountinfo_line
        );

        n2a_write_sync_close(&target.join("cross.txt"), b"bind-to-fuse").unwrap();
        assert_eq!(
            n2a_read_fresh(&fuse_file).unwrap().as_slice(),
            b"bind-to-fuse",
            "fresh FUSE open through retained directory fd missed bind write"
        );
        n2a_write_sync_close(&fuse_file, b"fuse-to-bind").unwrap();
        assert_eq!(
            n2a_read_fresh(&target.join("cross.txt"))
                .unwrap()
                .as_slice(),
            b"fuse-to-bind",
            "fresh bind open missed FUSE write"
        );
        drop(fuse_dir);

        let mut child = N2aCwdHolder::spawn(&target);
        child.wait_ready();
        let busy = bind.detach().unwrap_err();
        assert_eq!(busy.raw_os_error(), Some(libc::EBUSY));
        assert_eq!(bind.mount_identity().unwrap(), claim);
        let busy_observed = n2a_observe_target_mount(&target).unwrap();
        assert_eq!(busy_observed.identity, source_identity);
        assert_eq!(busy_observed.mount_id, claim.mount_id);
        assert_eq!(
            n2a_mountinfo_mountpoint(&busy_observed.mountinfo_line),
            Some(target.to_str().unwrap())
        );
        println!(
            "N2A_CORE_BIND_BUSY_RETAINED claim={claim:?} observed_line={:?}",
            busy_observed.mountinfo_line
        );

        child.release_and_wait_zero();
        bind.detach().unwrap();
        let restored = n2a_observe_target_mount(&target).unwrap();
        assert_eq!(restored.identity, fuse_identity);
        assert_eq!(restored.mount_id, covered_mount_id);
        assert_eq!(
            n2a_mountinfo_mountpoint(&restored.mountinfo_line),
            Some(mount.to_str().unwrap())
        );
        assert_eq!(
            n2a_read_fresh(&target.join("cross.txt"))
                .unwrap()
                .as_slice(),
            b"fuse-to-bind"
        );
    }

    drop(authority.take());
    let outer = n2a_observe_target_mount(&mount).unwrap();
    assert_eq!(outer.mount_id, covered_mount_id);
    assert_eq!(
        n2a_mountinfo_mountpoint(&outer.mountinfo_line),
        Some(mount.to_str().unwrap())
    );
    println!(
        "N2A_CORE_BIND_OUTER_FUSE_BEFORE_UMOUNT mount_id={} line={:?}",
        outer.mount_id, outer.mountinfo_line
    );
    session.normal_unmount_and_join(&mount);
    let after = std::fs::read_to_string("/proc/self/mountinfo").unwrap();
    assert!(
        !after.lines().any(|line| {
            line.split_whitespace()
                .nth(4)
                .is_some_and(|path| path.starts_with(retained.to_str().unwrap()))
        }),
        "fixture mount still visible after normal detach and FUSE join: {after}"
    );
    drop(ownerfs);
    drop(disk);
    std::fs::remove_dir_all(&retained).unwrap();
    println!("N2A_CORE_BIND_REAL_FUSE PASS close_to_open=true ebusy=true normal_detach=true");
}

#[test]
#[ignore = "requires exact Linux ARM64 private namespace, root and /dev/fuse"]
fn native_home_authorized_bind_retains_file_and_mmap_until_normal_drain() {
    assert_eq!(std::env::consts::OS, "linux");
    assert_eq!(std::env::consts::ARCH, "aarch64");
    let uid = std::process::Command::new("id").arg("-u").output().unwrap();
    assert!(uid.status.success());
    assert_eq!(uid.stdout, b"0\n");

    let (temp, fs, ctx, disk) = fixture(true);
    let retained = temp.keep();
    mkdir_root(&fs, &ctx, "native");
    let ownerfs = Arc::new(fs);
    let mount = retained.join("reference-drain-fuse");
    std::fs::create_dir(&mount).unwrap();
    let target = mount.join("native");
    let mut session =
        N2aFuseSession::new(crate::node::fuse::mount_ownerfs(ownerfs.clone(), &mount).unwrap());
    let covered = n2a_observe_target_mount(&target).unwrap();
    // Keep a view of the original FUSE directory: this fixture deliberately
    // rejects reacquisition after invalidate_all, so a fresh pathname lookup
    // cannot be used to observe restoration without granting new authority.
    let covered_directory = std::fs::File::open(&target).unwrap();
    assert_eq!(
        n2a_directory_identity(&covered_directory).unwrap(),
        covered.identity
    );
    let covered_fdinfo_path = format!("/proc/self/fdinfo/{}", covered_directory.as_raw_fd());
    let covered_fdinfo = std::fs::read_to_string(&covered_fdinfo_path).unwrap();
    let mut bind = super::bind_mount::AuthorizedWorkspaceBind::prepare(
        ownerfs.clone(),
        &mount,
        OsStr::new("native"),
    )
    .unwrap();
    bind.activate().unwrap();
    let mut cover = N2aOwnedCover {
        target: target.clone(),
        mounted: true,
    };
    bind.verify_current().unwrap();
    let claim = n2a_observe_target_mount(&target).unwrap();
    assert_ne!(claim.mount_id, covered.mount_id);
    assert_ne!(claim.identity, covered.identity);
    println!(
        "N2A_REFERENCE_DRAIN_ACTIVE root={retained:?} mount_id={} line={:?}",
        claim.mount_id, claim.mountinfo_line
    );
    n2a_write_sync_close(&target.join("held.bin"), &[b'R'; 4096]).unwrap();
    let held = std::fs::File::open(target.join("held.bin")).unwrap();
    let fdinfo =
        std::fs::read_to_string(format!("/proc/self/fdinfo/{}", held.as_raw_fd())).unwrap();
    let held_mount_id = fdinfo
        .lines()
        .find_map(|line| line.strip_prefix("mnt_id:"))
        .unwrap()
        .trim()
        .parse::<u64>()
        .unwrap();
    assert_eq!(held_mount_id, claim.mount_id);

    ownerfs.require_local().unwrap().roots.invalidate_all();
    assert!(bind.verify_current().is_err());
    assert_eq!(bind.detach().unwrap_err().raw_os_error(), Some(libc::EBUSY));
    let busy = n2a_observe_target_mount(&target).unwrap();
    assert_eq!(
        (busy.identity, busy.mount_id),
        (claim.identity, claim.mount_id)
    );
    let mut bytes = [0u8; 4096];
    std::os::unix::fs::FileExt::read_exact_at(&held, &mut bytes, 0).unwrap();
    assert_eq!(bytes, [b'R'; 4096]);
    println!(
        "N2A_REFERENCE_DRAIN_FILE_BUSY mount_id={} existing_fd_readable=true authority_current=false",
        busy.mount_id
    );

    let mapping = N2aReadMapping::new(&held, 4096);
    drop(held);
    assert_eq!(mapping.bytes(), &[b'R'; 4096]);
    assert_eq!(bind.detach().unwrap_err().raw_os_error(), Some(libc::EBUSY));
    let busy = n2a_observe_target_mount(&target).unwrap();
    assert_eq!(
        (busy.identity, busy.mount_id),
        (claim.identity, claim.mount_id)
    );
    println!(
        "N2A_REFERENCE_DRAIN_MMAP_BUSY mount_id={} file_fd_closed=true existing_map_readable=true",
        busy.mount_id
    );
    drop(mapping);
    bind.detach().unwrap();
    cover.mounted = false;
    drop(bind);
    // Even fstat would invoke FUSE authorization again. fdinfo observes the
    // retained kernel inode/mount reference without reacquiring a root grant.
    assert_eq!(
        std::fs::read_to_string(&covered_fdinfo_path).unwrap(),
        covered_fdinfo
    );
    let restored = n2a_observe_target_mount(&mount).unwrap();
    assert_eq!(restored.mount_id, covered.mount_id);
    let after_detach = std::fs::read_to_string("/proc/self/mountinfo").unwrap();
    assert!(!after_detach.lines().any(|line| {
        line.split_whitespace()
            .next()
            .and_then(|value| value.parse::<u64>().ok())
            == Some(claim.mount_id)
    }));
    assert!(
        !after_detach
            .lines()
            .any(|line| { line.split_whitespace().nth(4) == Some(target.to_str().unwrap()) })
    );
    drop(covered_directory);
    session.normal_unmount_and_join(&mount);
    let after = std::fs::read_to_string("/proc/self/mountinfo").unwrap();
    assert!(!after.lines().any(|line| {
        line.split_whitespace()
            .nth(4)
            .is_some_and(|path| path.starts_with(retained.to_str().unwrap()))
    }));
    drop(ownerfs);
    drop(disk);
    std::fs::remove_dir_all(&retained).unwrap();
    println!(
        "N2A_REFERENCE_DRAIN PASS file_busy=true mmap_only_busy=true normal_detach=true normal_fuse_join=true"
    );
}

// Readonly mapping owns its kernel reference independently of the original FD.
// The fixed fixture file is never truncated or modified while this map is live.
struct N2aReadMapping {
    address: *mut libc::c_void,
    length: usize,
}

#[allow(unsafe_code)]
impl N2aReadMapping {
    fn new(file: &std::fs::File, length: usize) -> Self {
        assert!(file.metadata().unwrap().len() >= length as u64);
        // SAFETY: the live file covers length bytes; this fixture keeps it unmodified
        // while the read-only mapping is live.
        let address = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                length,
                libc::PROT_READ,
                libc::MAP_SHARED,
                file.as_raw_fd(),
                0,
            )
        };
        assert_ne!(
            address,
            libc::MAP_FAILED,
            "mmap: {}",
            std::io::Error::last_os_error()
        );
        Self { address, length }
    }

    fn bytes(&self) -> &[u8] {
        // SAFETY: new rejected MAP_FAILED; the owned mapping remains readable for
        // self.length bytes for this borrow.
        unsafe { std::slice::from_raw_parts(self.address.cast(), self.length) }
    }
}

#[allow(unsafe_code)]
impl Drop for N2aReadMapping {
    fn drop(&mut self) {
        // SAFETY: this object owns this live mmap region and releases it once during Drop.
        let result = unsafe { libc::munmap(self.address, self.length) };
        assert_eq!(result, 0, "munmap: {}", std::io::Error::last_os_error());
    }
}

struct N2aMountObservation {
    identity: super::bind_mount::DirectoryIdentity,
    mount_id: u64,
    mountinfo_line: String,
}

struct N2aFuseSession {
    session: Option<crate::node::fuse::MountedFuse>,
}

impl N2aFuseSession {
    fn new(session: crate::node::fuse::MountedFuse) -> Self {
        Self {
            session: Some(session),
        }
    }

    fn normal_unmount_and_join(&mut self, mount: &std::path::Path) {
        n2a_normal_unmount(mount).unwrap();
        self.session.take().unwrap().join().unwrap();
    }
}

impl Drop for N2aFuseSession {
    fn drop(&mut self) {
        if let Some(session) = self.session.take() {
            println!("N2A_FUSE_SESSION_FORGOTTEN_AFTER_FAILURE");
            std::mem::forget(session);
        }
    }
}

fn n2a_write_sync_close(path: &std::path::Path, bytes: &[u8]) -> std::io::Result<()> {
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)?;
    std::io::Write::write_all(&mut file, bytes)?;
    file.sync_all()?;
    drop(file);
    Ok(())
}

fn n2a_read_fresh(path: &std::path::Path) -> std::io::Result<Vec<u8>> {
    let mut file = std::fs::File::open(path)?;
    let mut bytes = Vec::new();
    std::io::Read::read_to_end(&mut file, &mut bytes)?;
    Ok(bytes)
}

fn n2a_fd_child_path(parent: &std::fs::File, child: &str) -> std::path::PathBuf {
    std::path::PathBuf::from(format!("/proc/self/fd/{}/{child}", parent.as_raw_fd()))
}

fn n2a_directory_identity(
    file: &std::fs::File,
) -> std::io::Result<super::bind_mount::DirectoryIdentity> {
    let metadata = file.metadata()?;
    assert!(metadata.is_dir());
    Ok(super::bind_mount::DirectoryIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
    })
}

fn n2a_observe_target_mount(path: &std::path::Path) -> std::io::Result<N2aMountObservation> {
    let target = n2a_open_path_directory(path)?;
    let identity = n2a_directory_identity(&target)?;
    let fdinfo = std::fs::read_to_string(format!("/proc/self/fdinfo/{}", target.as_raw_fd()))?;
    let mount_id = fdinfo
        .lines()
        .find_map(|line| line.strip_prefix("mnt_id:"))
        .and_then(|value| value.trim().parse::<u64>().ok())
        .ok_or_else(|| std::io::Error::from_raw_os_error(libc::EINVAL))?;
    drop(target);

    let mountinfo_line = std::fs::read_to_string("/proc/self/mountinfo")?
        .lines()
        .find(|line| {
            let mut fields = line.split_whitespace();
            fields.next().and_then(|field| field.parse::<u64>().ok()) == Some(mount_id)
        })
        .map(ToOwned::to_owned)
        .ok_or_else(|| std::io::Error::from_raw_os_error(libc::ESTALE))?;
    Ok(N2aMountObservation {
        identity,
        mount_id,
        mountinfo_line,
    })
}

fn n2a_mountinfo_mountpoint(line: &str) -> Option<&str> {
    line.split_whitespace().nth(4)
}

#[allow(unsafe_code)]
fn n2a_normal_unmount(path: &std::path::Path) -> std::io::Result<()> {
    use std::os::unix::ffi::OsStrExt;

    let path = std::ffi::CString::new(path.as_os_str().as_bytes()).unwrap();
    // SAFETY: path is a live NUL-terminated CString; flags zero request normal unmount
    // without retained pointers.
    let result = unsafe { libc::umount2(path.as_ptr(), 0) };
    if result == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[allow(unsafe_code)]
fn n2a_open_path_directory(path: &std::path::Path) -> std::io::Result<std::fs::File> {
    use std::os::fd::FromRawFd;
    use std::os::unix::ffi::OsStrExt;

    let path = std::ffi::CString::new(path.as_os_str().as_bytes()).unwrap();
    // SAFETY: path is a live NUL-terminated CString; open returns a fresh owned descriptor
    // on success.
    let fd = unsafe {
        libc::open(
            path.as_ptr(),
            libc::O_PATH | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: open succeeded and this fresh descriptor is adopted exactly once by File.
    Ok(unsafe { std::fs::File::from_raw_fd(fd) })
}

struct N2aCwdHolder {
    child: std::process::Child,
    released: bool,
}

impl N2aCwdHolder {
    fn spawn(target: &std::path::Path) -> Self {
        let child = std::process::Command::new("/bin/sh")
            .arg("-c")
            .arg("cd \"$1\" || exit 111; printf READY; IFS= read _; exit 0")
            .arg("n2a-cwd-holder")
            .arg(target)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        println!("N2A_CWD_HOLDER_SPAWN pid={}", child.id());
        Self {
            child,
            released: false,
        }
    }

    fn wait_ready(&mut self) {
        n2a_wait_ready(&mut self.child);
    }

    fn release_and_wait_zero(&mut self) {
        n2a_release_child_and_wait_zero(&mut self.child);
        self.released = true;
    }
}

impl Drop for N2aCwdHolder {
    fn drop(&mut self) {
        if !self.released {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

#[allow(unsafe_code)]
fn n2a_wait_ready(child: &mut std::process::Child) {
    let stdout_fd = child.stdout.as_ref().unwrap().as_raw_fd();
    n2a_set_nonblocking(stdout_fd).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut seen = Vec::new();
    loop {
        let mut buffer = [0u8; 16];
        // SAFETY: buffer is writable for buffer.len() bytes and the descriptor is held
        // by the child pipe owner during this call.
        let read = unsafe {
            libc::read(
                stdout_fd,
                buffer.as_mut_ptr().cast::<libc::c_void>(),
                buffer.len(),
            )
        };
        if read > 0 {
            seen.extend_from_slice(&buffer[..read as usize]);
            if seen
                .windows(b"READY".len())
                .any(|window| window == b"READY")
            {
                println!("N2A_CWD_HOLDER_READY pid={}", child.id());
                return;
            }
        } else if read == 0 {
            panic!(
                "cwd holder exited before READY; status={:?} stdout={seen:?} stderr={:?}",
                child.try_wait().unwrap(),
                n2a_child_stderr(child)
            );
        } else {
            let error = std::io::Error::last_os_error();
            if !matches!(
                error.raw_os_error(),
                Some(code) if code == libc::EAGAIN || code == libc::EWOULDBLOCK
            ) {
                panic!("cwd holder READY read failed: {error}");
            }
        }
        if let Some(status) = child.try_wait().unwrap() {
            panic!(
                "cwd holder exited before READY; status={status:?} stdout={seen:?} stderr={:?}",
                n2a_child_stderr(child)
            );
        }
        if std::time::Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!(
                "cwd holder did not signal READY before deadline; stdout={seen:?} stderr={:?}",
                n2a_child_stderr(child)
            );
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn n2a_release_child_and_wait_zero(child: &mut std::process::Child) {
    let mut stdin = child.stdin.take().unwrap();
    std::io::Write::write_all(&mut stdin, b"\n").unwrap();
    drop(stdin);
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(
                status.success(),
                "cwd holder exited nonzero: {status:?} stderr={:?}",
                n2a_child_stderr(child)
            );
            println!("N2A_CWD_HOLDER_WAIT pid={} status={status:?}", child.id());
            return;
        }
        if std::time::Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!(
                "cwd holder did not exit after release; stderr={:?}",
                n2a_child_stderr(child)
            );
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[allow(unsafe_code)]
fn n2a_child_stderr(child: &mut std::process::Child) -> Vec<u8> {
    let Some(stderr) = child.stderr.as_ref() else {
        return Vec::new();
    };
    let stderr_fd = stderr.as_raw_fd();
    n2a_set_nonblocking(stderr_fd).unwrap();
    let mut bytes = Vec::new();
    loop {
        let mut buffer = [0u8; 256];
        // SAFETY: buffer is writable for buffer.len() bytes and the descriptor is held
        // by the child pipe owner during this call.
        let read = unsafe {
            libc::read(
                stderr_fd,
                buffer.as_mut_ptr().cast::<libc::c_void>(),
                buffer.len(),
            )
        };
        if read > 0 {
            bytes.extend_from_slice(&buffer[..read as usize]);
        } else {
            break;
        }
    }
    bytes
}

#[allow(unsafe_code)]
fn n2a_set_nonblocking(fd: std::os::fd::RawFd) -> std::io::Result<()> {
    // SAFETY: F_GETFL takes no pointer and the kernel validates the integer descriptor.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if flags < 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: F_SETFL takes an integer flags value and no pointer; the kernel validates the
    // descriptor.
    if unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}
