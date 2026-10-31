// Descriptor-confined Linux OwnerFs workspace bind mounts.
//
// The caller supplies an already-authorized Home source descriptor and a
// trusted managed parent descriptor. This module records the current mount
// namespace plus exact directory/mount identities, prepares an owned claim,
// activates it with descriptor-confined mount syscalls, and tears it down only
// while the same claim is still visible.

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::node) struct DirectoryIdentity {
    pub(in crate::node) device: u64,
    pub(in crate::node) inode: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::node) struct NamespaceIdentity {
    pub(in crate::node) device: u64,
    pub(in crate::node) inode: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::node) struct MountIdentity {
    pub(in crate::node) namespace: NamespaceIdentity,
    pub(in crate::node) mount_id: u64,
    pub(in crate::node) unique_mount_id: u64,
    pub(in crate::node) source: DirectoryIdentity,
    pub(in crate::node) covered_target: DirectoryIdentity,
}

#[cfg(target_os = "linux")]
mod linux {
    use super::{DirectoryIdentity, MountIdentity, NamespaceIdentity};
    use crate::node::vfs::ownerfs::{HomeExportAuthority, OwnerFs};
    use std::{
        ffi::{CString, OsStr, OsString},
        fs::File,
        io,
        os::{
            fd::{AsRawFd, FromRawFd, RawFd},
            unix::{
                ffi::OsStrExt,
                fs::{MetadataExt, OpenOptionsExt},
            },
        },
        path::Path,
        sync::Arc,
    };

    const OPEN_TREE_CLONE: u32 = 1;
    const MOVE_MOUNT_F_EMPTY_PATH: u32 = 0x4;
    const MOVE_MOUNT_T_EMPTY_PATH: u32 = 0x40;
    const MOUNT_ATTR_NOSUID: u64 = 0x2;
    const MOUNT_ATTR_NODEV: u64 = 0x4;

    /// Authorized, fixed-Home export in the caller's current mount namespace.
    /// Keep this owner alive on activation/closure errors: a syscall can attach
    /// a tree before its postcondition fails. Grant revocation does not prevent
    /// cleanup of the already-owned claim and does not revoke existing FDs.
    pub(in crate::node) struct AuthorizedWorkspaceBind {
        owner: Arc<OwnerFs>,
        authority: HomeExportAuthority,
        export: WorkspaceBindMount,
    }

    impl AuthorizedWorkspaceBind {
        /// Prepare an egress bind claim against a native `<egress_root>/<name>`
        /// target (D1: bind target decoupled from the FUSE tree).
        ///
        /// The parent is the administrator-configured native egress root, never
        /// the FUSE mount point; the target directory is created empty and
        /// idempotently (a leftover from a previous run is re-verified below).
        /// The Home source must be a physical backing directory, and the egress
        /// root must itself be native so a bind target can never cover FUSE.
        pub(in crate::node) fn prepare_egress(
            owner: Arc<OwnerFs>,
            egress_root: &Path,
            workspace: &OsStr,
        ) -> io::Result<Self> {
            validate_component(workspace)?;
            let authority = owner
                .native_home_export_for_current_namespace(workspace)
                .map_err(|error| io::Error::other(error.to_string()))?;
            authority
                .verify_current(&owner)
                .map_err(|error| io::Error::other(error.to_string()))?;
            let source = authority
                .source_descriptor()
                .map_err(|error| io::Error::other(error.to_string()))?;
            if is_fuse_mount(&source)? {
                return Err(io::Error::other(
                    "workspace egress bind requires a physical Home source",
                ));
            }
            let parent = File::options()
                .read(true)
                .custom_flags(libc::O_PATH | libc::O_DIRECTORY | libc::O_NOFOLLOW)
                .open(egress_root)?;
            if is_fuse_mount(&parent)? {
                return Err(io::Error::other(
                    "workspace egress bind requires a native egress root",
                ));
            }
            ensure_egress_target(&parent, workspace)?;
            let export = WorkspaceBindMount::prepare(source, parent, workspace)?;
            let expected = authority.source_identity();
            if export.source_identity()
                != (DirectoryIdentity {
                    device: expected.dev,
                    inode: expected.ino,
                })
            {
                return Err(errno(libc::ESTALE));
            }
            Ok(Self {
                owner,
                authority,
                export,
            })
        }

        pub(in crate::node) fn activate(&mut self) -> io::Result<()> {
            self.authority
                .verify_current(&self.owner)
                .map_err(|error| io::Error::other(error.to_string()))?;
            self.export.activate()?;
            // verify_current 失败说明挂载后授权失效：回滚已生效的 bind，
            // 避免上层把错误降级为 FuseOnly 后 bind 仍残留在 namespace 中
            // （review: 挂载生效后的错误路径没有回滚）。
            self.verify_current()
                .inspect_err(|_| self.export.rollback_mount())
        }

        pub(in crate::node) fn verify_current(&self) -> io::Result<()> {
            self.authority
                .verify_current(&self.owner)
                .map_err(|error| io::Error::other(error.to_string()))?;
            let claim = self.export.mount_identity()?;
            if !self.export.activated || self.export.inspect()?.as_ref() != Some(&claim) {
                return Err(errno(libc::ESTALE));
            }
            self.export.check_policy()
        }

        pub(in crate::node) fn detach(&mut self) -> io::Result<()> {
            self.export.detach()
        }
    }

    fn is_fuse_mount(file: &File) -> io::Result<bool> {
        // FUSE root STATFS may intentionally be unsupported. Mountinfo binds
        // the actual descriptor's mount ID without issuing a backend request.
        let expected = mount_id(file)?.to_string();
        let table = std::fs::read_to_string("/proc/thread-self/mountinfo")?;
        let filesystem = table
            .lines()
            .find_map(|line| {
                let (identity, detail) = line.split_once(" - ")?;
                (identity.split_whitespace().next()? == expected)
                    .then(|| detail.split_whitespace().next())
                    .flatten()
            })
            .ok_or_else(|| errno(libc::ESTALE))?;
        Ok(filesystem == "fuse" || filesystem.starts_with("fuse."))
    }

    /// Create the empty egress target directory `<egress_root>/<name>` as the
    /// bind destination. Idempotent: EEXIST (a leftover from a previous run or
    /// a concurrent creator) is accepted and re-verified by prepare below.
    #[allow(unsafe_code)]
    fn ensure_egress_target(parent: &File, name: &OsStr) -> io::Result<()> {
        let name = validate_component(name)?;
        // SAFETY: parent and name are live for this synchronous syscall; mode
        // is masked by the caller's umask.
        let result = unsafe { libc::mkdirat(parent.as_raw_fd(), name.as_ptr(), 0o700) };
        if result == 0 {
            return Ok(());
        }
        let error = io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::EEXIST) {
            return Ok(());
        }
        Err(error)
    }

    /// Scan `/proc/self/mountinfo` for residual workspace egress binds mounted
    /// directly under `egress_root`. Returns the child names whose mount
    /// carries the managed NOSUID|NODEV signature (D4 §4.4 startup reconcile).
    pub(in crate::node) fn scan_egress_binds(egress_root: &Path) -> io::Result<Vec<OsString>> {
        use std::io::{BufRead, BufReader};

        let mut names = Vec::new();
        let mountinfo = std::fs::File::open("/proc/self/mountinfo")?;
        for line in BufReader::new(mountinfo).lines() {
            let line = line?;
            if let Some(name) = egress_bind_name_from_line(&line, egress_root) {
                names.push(name);
            }
        }
        Ok(names)
    }

    /// Decide whether a mountinfo line describes a managed egress bind directly
    /// under `egress_root` with the NOSUID|NODEV policy, returning the child name.
    ///
    /// mountinfo lines split at " - " into identity and detail. `nosuid,nodev`
    /// are **mount options** in the identity part (0-based field 5), not the
    /// super options in the detail part (field 2) — reading super options would
    /// miss our own mounts and leave residual binds unrecoverable (review:
    /// mountinfo 读取字段错误导致残留挂载无法回收).
    fn egress_bind_name_from_line(line: &str, egress_root: &Path) -> Option<OsString> {
        let root = egress_root.as_os_str().as_bytes();
        let (identity, _detail) = line.split_once(" - ")?;
        let mount_point = identity.split_whitespace().nth(4)?;
        let point = mount_point.as_bytes();
        if !point.starts_with(root) || point.len() <= root.len() {
            return None;
        }
        let tail = point.get(root.len()..)?;
        if !tail.starts_with(b"/") || tail.len() == 1 || tail[1..].contains(&b'/') {
            return None;
        }
        // Managed signature: the mount options carry NOSUID|NODEV. Bind mounts
        // report source paths from the activating fd, so the target location
        // plus policy options identify our own exports.
        let options = identity.split_whitespace().nth(5).unwrap_or("");
        if !options.contains("nosuid") || !options.contains("nodev") {
            return None;
        }
        Some(OsString::from(OsStr::from_bytes(&tail[1..])))
    }

    /// Detach a residual workspace egress bind by path (D4 §4.4).
    ///
    /// Re-verifies that the target is a direct child of `egress_root` and still
    /// carries the managed NOSUID|NODEV signature before detaching with
    /// MNT_DETACH, so a busy tree can never strand the cleanup.
    pub(in crate::node) fn detach_egress_bind(egress_root: &Path, name: &OsStr) -> io::Result<()> {
        validate_component(name)?;
        let target = egress_root.join(name);
        let file = File::options()
            .read(true)
            .custom_flags(libc::O_PATH | libc::O_DIRECTORY | libc::O_NOFOLLOW)
            .open(&target)?;
        check_mount_policy(&file)?;
        let bytes = CString::new(target.as_os_str().as_bytes()).map_err(|_| errno(libc::EINVAL))?;
        lazy_unmount(&bytes)
    }

    #[derive(Debug)]
    pub(in crate::node) struct WorkspaceBindMount {
        namespace: NamespaceIdentity,
        source_identity: DirectoryIdentity,
        target_identity: DirectoryIdentity,
        covered_unique_mount_id: u64,
        mount: Option<MountIdentity>,
        source: File,
        parent: File,
        name: CString,
        activated: bool,
    }

    impl WorkspaceBindMount {
        pub(in crate::node) fn prepare(
            source: File,
            parent: File,
            name: &OsStr,
        ) -> io::Result<Self> {
            let namespace = current_namespace()?;
            let source_identity = directory_identity(&source)?;
            if !parent.metadata()?.is_dir() {
                return Err(errno(libc::ENOTDIR));
            }
            let name = validate_component(name)?;
            let target = open_child(&parent, &name)?;
            let target_identity = directory_identity(&target)?;
            let covered_unique_mount_id = unique_mount_id(&target)?;
            if covered_unique_mount_id != unique_mount_id(&parent)? {
                return Err(errno(libc::ESTALE));
            }

            Ok(Self {
                namespace,
                source_identity,
                target_identity,
                covered_unique_mount_id,
                mount: None,
                source,
                parent,
                name,
                activated: false,
            })
        }

        pub(in crate::node) fn activate(&mut self) -> io::Result<()> {
            if self.activated {
                return Ok(());
            }

            let fresh_target = self.recheck_prepared_target()?;

            // Try the new mount API (open_tree + move_mount, Linux 5.2+).
            // Fall back to legacy mount(2) MS_BIND on older kernels (ENOSYS).
            let mount = match self.activate_new_api(&fresh_target) {
                Ok(mount) => mount,
                Err(e) if e.raw_os_error() == Some(libc::ENOSYS) => self.activate_legacy_bind()?,
                Err(e) => {
                    drop(fresh_target);
                    return Err(e);
                }
            };
            drop(fresh_target);

            // 挂载已生效：后续任何校验失败都必须回滚（MNT_DETACH），否则
            // bind 残留在 namespace 中且上层只把错误降级为 FuseOnly，挂载
            // 无法回收（review: 挂载生效后的错误路径没有回滚）。
            if mount.source != self.source_identity {
                self.rollback_mount();
                return Err(errno(libc::ESTALE));
            }
            self.mount = Some(mount);
            self.activated = true;

            if self.inspect()?.as_ref() != Some(&mount) {
                self.rollback_mount();
                return Err(errno(libc::ESTALE));
            }
            if let Err(error) = self.check_policy() {
                self.rollback_mount();
                return Err(error);
            }
            Ok(())
        }

        /// Best-effort detach of a just-activated mount by its target path, and
        /// reset the mount state so a later detach() is a no-op. Ignores errors:
        /// rollback must not mask the original failure.
        fn rollback_mount(&mut self) {
            if let Ok(path) = self.target_proc_path() {
                let _ = lazy_unmount(&path);
            }
            self.mount = None;
            self.activated = false;
        }

        /// New mount API path: open_tree + mount_setattr + move_mount (Linux 5.2+).
        fn activate_new_api(&self, target: &File) -> io::Result<MountIdentity> {
            let tree = clone_tree(&self.source)?;

            // mount_setattr is Linux 5.13+; if unavailable, apply policy via
            // legacy remount after move_mount succeeds.
            let policy_applied = match apply_policy(&tree) {
                Ok(()) => true,
                Err(e) if e.raw_os_error() == Some(libc::ENOSYS) => false,
                Err(e) => {
                    drop(tree);
                    return Err(e);
                }
            };

            let mount = MountIdentity {
                namespace: self.namespace,
                mount_id: mount_id(&tree)?,
                unique_mount_id: unique_mount_id(&tree)?,
                source: directory_identity(&tree)?,
                covered_target: self.target_identity,
            };

            attach_tree(&tree, target)?;

            // If mount_setattr was unavailable, apply NOSUID|NODEV via remount.
            // Keep the attached tree alive: target still refers to the covered directory.
            // attach_tree 成功后挂载已生效：remount 失败必须 lazy_unmount
            // 回滚，否则 bind 残留在 namespace 中且上层只把错误降级为
            // FuseOnly，挂载无法回收（review: 挂载生效后的错误路径没有回滚）。
            if !policy_applied && let Err(error) = apply_policy_legacy_remount(&tree) {
                if let Ok(path) = fd_proc_path(&tree) {
                    let _ = lazy_unmount(&path);
                }
                return Err(error);
            }
            drop(tree);

            Ok(mount)
        }

        /// Legacy mount(2) path for kernels < 5.2 (e.g. 4.18).
        fn activate_legacy_bind(&self) -> io::Result<MountIdentity> {
            let source_path =
                CString::new(format!("/proc/thread-self/fd/{}", self.source.as_raw_fd()))
                    .map_err(|_| errno(libc::EINVAL))?;

            let target_path = self.target_proc_path()?;

            // Legacy bind mount: mount(2) with MS_BIND.
            mount_bind(source_path.as_c_str(), target_path.as_c_str())?;

            // mount_bind 成功后挂载已生效：后续任何失败都 lazy_unmount 回滚，
            // 否则 bind 残留在 namespace 中且无法回收（review: 挂载生效后
            // 的错误路径没有回滚）。
            let rollback = |error: io::Error| -> io::Error {
                let _ = lazy_unmount(target_path.as_c_str());
                error
            };

            // Make the bind private so detach cannot fail on shared peer busy (D4).
            if let Err(error) = make_private(target_path.as_c_str()) {
                return Err(rollback(error));
            }

            // Apply NOSUID|NODEV via remount: mount(2) with MS_REMOUNT.
            if let Err(error) = mount_remount_nosuid_nodev(target_path.as_c_str()) {
                return Err(rollback(error));
            }

            // Re-open target to get mount identity after bind mount.
            let target = open_child(&self.parent, &self.name).map_err(rollback)?;
            let identity = MountIdentity {
                namespace: self.namespace,
                mount_id: mount_id(&target).map_err(rollback)?,
                unique_mount_id: unique_mount_id(&target).map_err(rollback)?,
                source: directory_identity(&target).map_err(rollback)?,
                covered_target: self.target_identity,
            };
            Ok(identity)
        }

        pub(in crate::node) fn detach(&mut self) -> io::Result<()> {
            if !self.activated {
                if self.inspect()?.is_none() {
                    return Ok(());
                }
                return Err(errno(libc::ESTALE));
            }
            let mount = self.mount_identity()?;
            if directory_identity(&self.source)? != self.source_identity {
                return Err(errno(libc::ESTALE));
            }
            if self.inspect()?.as_ref() != Some(&mount) {
                return Err(errno(libc::ESTALE));
            }
            // MNT_DETACH: detach immediately without failing on a busy tree
            // (active container fds); the covered target reappears once the
            // last reference closes (D4 §3.2).
            lazy_unmount(&self.target_proc_path()?)?;
            if self.inspect()?.is_some() {
                return Err(errno(libc::ESTALE));
            }
            self.activated = false;
            Ok(())
        }

        pub(in crate::node) fn mount_identity(&self) -> io::Result<MountIdentity> {
            self.mount.ok_or_else(|| errno(libc::ESTALE))
        }

        pub(in crate::node) fn source_identity(&self) -> DirectoryIdentity {
            self.source_identity
        }

        fn recheck_prepared_target(&self) -> io::Result<File> {
            self.check_current_namespace()?;
            if directory_identity(&self.source)? != self.source_identity {
                return Err(errno(libc::ESTALE));
            }
            let target = open_child(&self.parent, &self.name)?;
            if unique_mount_id(&target)? != self.covered_unique_mount_id
                || directory_identity(&target)? != self.target_identity
            {
                return Err(errno(libc::ESTALE));
            }
            Ok(target)
        }

        fn check_current_namespace(&self) -> io::Result<()> {
            if current_namespace()? == self.namespace {
                Ok(())
            } else {
                Err(errno(libc::ESTALE))
            }
        }

        fn inspect(&self) -> io::Result<Option<MountIdentity>> {
            self.check_current_namespace()?;
            let target = open_child(&self.parent, &self.name)?;
            let actual_unique_id = unique_mount_id(&target)?;
            let actual_directory = directory_identity(&target)?;
            if actual_unique_id == self.covered_unique_mount_id {
                if actual_directory != self.target_identity {
                    return Err(errno(libc::ESTALE));
                }
                return Ok(None);
            }
            Ok(Some(MountIdentity {
                namespace: self.namespace,
                mount_id: mount_id(&target)?,
                unique_mount_id: actual_unique_id,
                source: actual_directory,
                covered_target: self.target_identity,
            }))
        }

        fn check_policy(&self) -> io::Result<()> {
            let target = open_child(&self.parent, &self.name)?;
            check_mount_policy(&target)
        }

        fn target_proc_path(&self) -> io::Result<CString> {
            let mut bytes =
                format!("/proc/thread-self/fd/{}/", self.parent.as_raw_fd()).into_bytes();
            bytes.extend_from_slice(self.name.as_bytes());
            CString::new(bytes).map_err(|_| errno(libc::EINVAL))
        }
    }

    pub(in crate::node) fn current_namespace() -> io::Result<NamespaceIdentity> {
        let metadata = File::open("/proc/thread-self/ns/mnt")?.metadata()?;
        namespace_identity_from_metadata(&metadata)
    }

    fn namespace_identity(file: &File) -> io::Result<NamespaceIdentity> {
        namespace_identity_from_metadata(&file.metadata()?)
    }

    fn namespace_identity_from_metadata(
        metadata: &std::fs::Metadata,
    ) -> io::Result<NamespaceIdentity> {
        Ok(NamespaceIdentity {
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }

    fn directory_identity(file: &File) -> io::Result<DirectoryIdentity> {
        let metadata = file.metadata()?;
        if !metadata.is_dir() {
            return Err(errno(libc::ENOTDIR));
        }
        Ok(DirectoryIdentity {
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }

    pub(in crate::node) fn inspect_secondary_clone(
        namespace: &File,
        root: &File,
        target_component: &OsStr,
    ) -> io::Result<(NamespaceIdentity, DirectoryIdentity, u64)> {
        let target_component = validate_component(target_component)?;
        in_mount_namespace(namespace, || {
            let namespace_identity = namespace_identity(namespace)?;
            let target = open_child(root, &target_component)?;
            check_mount_policy(&target)?;
            Ok((
                namespace_identity,
                directory_identity(&target)?,
                unique_mount_id(&target)?,
            ))
        })
    }

    pub(in crate::node) fn detach_secondary_clone(
        namespace: &File,
        root: &File,
        target_component: &OsStr,
        expected_source: DirectoryIdentity,
        expected_unique_mount_id: u64,
        covered: DirectoryIdentity,
    ) -> io::Result<()> {
        let target_component = validate_component(target_component)?;
        in_mount_namespace(namespace, || {
            let target = open_child(root, &target_component)?;
            if directory_identity(&target)? != expected_source
                || unique_mount_id(&target)? != expected_unique_mount_id
            {
                return Err(errno(libc::ESTALE));
            }
            check_mount_policy(&target)?;
            drop(target);
            change_directory(root)?;
            normal_unmount(&target_component)?;
            let restored = open_child(root, &target_component)?;
            if directory_identity(&restored)? != covered
                || unique_mount_id(&restored)? == expected_unique_mount_id
            {
                return Err(errno(libc::ESTALE));
            }
            Ok(())
        })
    }

    #[allow(unsafe_code)]
    fn check_mount_policy(target: &File) -> io::Result<()> {
        // Query the mount held by this descriptor, including per-mount flags.
        // A stopped container's procfs cannot resolve this host thread.
        // SAFETY: libc::statvfs consists of integer fields and permits zero
        // initialization before fstatvfs fills it.
        let mut attributes: libc::statvfs = unsafe { std::mem::zeroed() };
        // SAFETY: the descriptor and output storage remain live for the call.
        if unsafe { libc::fstatvfs(target.as_raw_fd(), &mut attributes) } != 0 {
            return Err(io::Error::last_os_error());
        }
        let required = libc::ST_NOSUID | libc::ST_NODEV;
        if attributes.f_flag & required == required {
            Ok(())
        } else {
            Err(errno(libc::EPERM))
        }
    }

    fn in_mount_namespace<T>(
        namespace: &File,
        body: impl FnOnce() -> io::Result<T>,
    ) -> io::Result<T> {
        let original = File::open("/proc/thread-self/ns/mnt")?;
        let original_directory = File::open(".")?;
        // A successful setns with CLONE_NEWNS enters this exact held namespace
        // descriptor. Do not consult the container's procfs after entry.
        setns_mount(namespace)?;
        let result = body();
        let restore = setns_mount(&original).and_then(|_| change_directory(&original_directory));
        match (result, restore) {
            (Ok(value), Ok(())) => Ok(value),
            (Err(error), Ok(())) => Err(error),
            (_, Err(error)) => Err(error),
        }
    }

    #[allow(unsafe_code)]
    fn change_directory(directory: &File) -> io::Result<()> {
        // SAFETY: the directory descriptor is owned and live. This controller
        // thread has its own CLONE_FS context; the outer scope restores cwd.
        if unsafe { libc::fchdir(directory.as_raw_fd()) } == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    #[allow(unsafe_code)]
    fn setns_mount(namespace: &File) -> io::Result<()> {
        // SAFETY: namespace is an owned live descriptor; setns consumes no pointer and
        // its error is propagated.
        let result = unsafe { libc::setns(namespace.as_raw_fd(), libc::CLONE_NEWNS) };
        if result == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    fn validate_component(name: &OsStr) -> io::Result<CString> {
        let bytes = name.as_bytes();
        if bytes.is_empty()
            || bytes.len() > 255
            || bytes == b"."
            || bytes == b".."
            || bytes.contains(&b'/')
        {
            return Err(errno(libc::EINVAL));
        }
        CString::new(bytes).map_err(|_| errno(libc::EINVAL))
    }

    fn errno(code: i32) -> io::Error {
        io::Error::from_raw_os_error(code)
    }

    #[allow(unsafe_code)]
    fn owned_fd(fd: RawFd) -> io::Result<File> {
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: callers pass a fresh successful syscall/open result exactly once.
        Ok(unsafe { File::from_raw_fd(fd) })
    }

    #[allow(unsafe_code)]
    fn open_child(parent: &File, name: &std::ffi::CStr) -> io::Result<File> {
        // SAFETY: parent and name are live for this synchronous syscall.
        owned_fd(unsafe {
            libc::openat(
                parent.as_raw_fd(),
                name.as_ptr(),
                libc::O_PATH | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            )
        })
    }

    fn mount_id(file: &File) -> io::Result<u64> {
        statx_mount_id(file, libc::STATX_MNT_ID)
    }

    fn unique_mount_id(file: &File) -> io::Result<u64> {
        // STATX_MNT_ID_UNIQUE (0x4000) is Linux 6.8+; older kernels report only
        // STATX_MNT_ID (4.18+). Request both bits in one statx: on 6.8+ the
        // kernel sets the UNIQUE bit (sharing the stx_mnt_id field), while older
        // kernels set only the MNT_ID bit and ignore the unknown 0x4000 bit.
        //
        // Requesting both bits in a single call avoids the readlink-based
        // mountinfo fallback, which cannot resolve detached mount fds (their
        // /proc/self/fd readlink reports "/") and can mis-resolve O_PATH fds
        // whose readlink is unavailable in restricted environments, wrongly
        // attributing the root mount id to the file.
        statx_mount_id(file, 0x4000 | libc::STATX_MNT_ID)
    }

    #[allow(unsafe_code)]
    fn statx_mount_id(file: &File, mask: u32) -> io::Result<u64> {
        // SAFETY: statx writes into initialized storage and retains no pointer.
        let mut stat: libc::statx = unsafe { std::mem::zeroed() };
        // SAFETY: stat is initialized writable storage; the held descriptor and empty C
        // string stay live.
        let result = unsafe {
            libc::statx(
                file.as_raw_fd(),
                c"".as_ptr(),
                libc::AT_EMPTY_PATH,
                mask,
                &mut stat,
            )
        };
        if result == 0 && (stat.stx_mask & mask) != 0 && stat.stx_mnt_id != 0 {
            return Ok(stat.stx_mnt_id);
        }
        // Fallback for kernels/glibc that don't report STATX_MNT_ID (e.g. Linux
        // 4.18 with older glibc statx wrapper that falls back to fstat). Parse
        // /proc/self/mountinfo to find the mount ID by matching st_dev.
        mount_id_from_mountinfo(file)
    }

    /// Resolve mount ID by parsing /proc/self/mountinfo, matching the file's
    /// st_dev (major:minor) and selecting the most specific mount point.
    #[allow(unsafe_code)]
    fn mount_id_from_mountinfo(file: &File) -> io::Result<u64> {
        use std::io::{BufRead, BufReader};

        // Get st_dev from fstat to match against mountinfo major:minor.
        // SAFETY: libc::stat consists of integer fields and permits zero
        // initialization before fstat fills it.
        let mut st: libc::stat = unsafe { std::mem::zeroed() };
        // SAFETY: st is zero-initialized writable storage; fd is owned and live.
        if unsafe { libc::fstat(file.as_raw_fd(), &mut st) } != 0 {
            return Err(io::Error::last_os_error());
        }
        let dev = st.st_dev;
        // Decode major:minor from dev_t (glibc encoding for 64-bit dev_t).
        let major = ((dev >> 8) & 0xfff) as u32;
        let minor = ((dev & 0xff) | ((dev >> 12) & 0xfff00)) as u32;
        let dev_str = format!("{}:{}", major, minor);

        // Get the file's path via /proc/self/fd/<fd> readlink for prefix matching.
        let fd_link = format!("/proc/self/fd/{}", file.as_raw_fd());
        let file_path = std::fs::read_link(&fd_link).ok();

        // Parse /proc/self/mountinfo.
        let mountinfo = std::fs::File::open("/proc/self/mountinfo")?;
        let reader = BufReader::new(mountinfo);

        let mut best_id: Option<u64> = None;
        let mut best_len: usize = 0;

        for line in reader.lines() {
            let line = line?;
            let fields: Vec<&str> = line.split_whitespace().collect();
            if fields.len() < 5 {
                continue;
            }
            // fields[0] = mount ID, fields[2] = major:minor, fields[4] = mount point
            if fields[2] != dev_str {
                continue;
            }
            let mount_id: u64 = match fields[0].parse() {
                Ok(id) => id,
                Err(_) => continue,
            };

            if let Some(ref path) = file_path {
                let mount_point = fields[4].as_bytes();
                let file_bytes = path.as_os_str().as_bytes();
                // Check if mount_point is a prefix of file_path.
                let is_prefix = file_bytes.len() >= mount_point.len()
                    && &file_bytes[..mount_point.len()] == mount_point;
                if !is_prefix {
                    continue;
                }
                // Pick the longest matching mount point (most specific).
                if mount_point.len() > best_len {
                    best_len = mount_point.len();
                    best_id = Some(mount_id);
                }
            } else {
                // No path available; take the first match by dev_t.
                best_id = Some(mount_id);
                break;
            }
        }

        best_id.ok_or_else(|| errno(libc::ENOTSUP))
    }

    #[allow(unsafe_code)]
    fn clone_tree(source: &File) -> io::Result<File> {
        // SAFETY: fd and empty pathname are live; open_tree returns an owned mount fd.
        owned_fd(unsafe {
            libc::syscall(
                libc::SYS_open_tree,
                source.as_raw_fd(),
                c"".as_ptr(),
                OPEN_TREE_CLONE | libc::O_CLOEXEC as u32 | libc::AT_EMPTY_PATH as u32,
            )
        } as RawFd)
    }

    #[repr(C)]
    struct MountAttr {
        attr_set: u64,
        attr_clr: u64,
        propagation: u64,
        userns_fd: u64,
    }

    #[allow(unsafe_code)]
    fn apply_policy(tree: &File) -> io::Result<()> {
        let attr = MountAttr {
            attr_set: MOUNT_ATTR_NOSUID | MOUNT_ATTR_NODEV,
            attr_clr: 0,
            // 独立传播：bind 挂载点必须脱离 source 的 shared peer group，
            // 否则 detach（正常 umount 和 MNT_DETACH 均如此）会因任一 peer
            // busy 而整体失败（EBUSY），留下无法清理的孤儿挂载（D4）。
            propagation: libc::MS_PRIVATE,
            userns_fd: 0,
        };
        // SAFETY: mount_attr has the Linux UAPI layout and lives for the syscall.
        let result = unsafe {
            libc::syscall(
                libc::SYS_mount_setattr,
                tree.as_raw_fd(),
                c"".as_ptr(),
                libc::AT_EMPTY_PATH as u32,
                &attr as *const MountAttr,
                std::mem::size_of::<MountAttr>(),
            )
        };
        if result == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    #[allow(unsafe_code)]
    fn attach_tree(tree: &File, target: &File) -> io::Result<()> {
        // SAFETY: fds and empty pathnames are live for the synchronous syscall.
        let result = unsafe {
            libc::syscall(
                libc::SYS_move_mount,
                tree.as_raw_fd(),
                c"".as_ptr(),
                target.as_raw_fd(),
                c"".as_ptr(),
                MOVE_MOUNT_F_EMPTY_PATH | MOVE_MOUNT_T_EMPTY_PATH,
            )
        };
        if result == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    /// Apply NOSUID|NODEV policy via legacy mount(2) remount on a File descriptor.
    fn apply_policy_legacy_remount(target: &File) -> io::Result<()> {
        // The held tree descriptor follows the attached mount; the covered target does not.
        let path = fd_proc_path(target)?;
        make_private(path.as_c_str())?;
        mount_remount_nosuid_nodev(path.as_c_str())
    }

    /// Resolve a file descriptor to a `/proc/thread-self/fd/<fd>` path usable as
    /// a mount(2)/umount2(2) target, keeping every operation descriptor-confined.
    fn fd_proc_path(file: &File) -> io::Result<CString> {
        CString::new(format!("/proc/thread-self/fd/{}", file.as_raw_fd()))
            .map_err(|_| errno(libc::EINVAL))
    }

    #[allow(unsafe_code)]
    fn mount_bind(source: &std::ffi::CStr, target: &std::ffi::CStr) -> io::Result<()> {
        // SAFETY: source and target are NUL-terminated C strings that live for the call.
        let result = unsafe {
            libc::mount(
                source.as_ptr(),
                target.as_ptr(),
                std::ptr::null(),
                libc::MS_BIND,
                std::ptr::null(),
            )
        };
        if result == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    /// Change the mount point's propagation type to private (`mount --make-private`).
    ///
    /// With source and filesystemtype both NULL, mount(2) only changes the
    /// propagation type of target. Private mounts are not part of any shared
    /// peer group, so detach never fails on a busy peer (D4).
    #[allow(unsafe_code)]
    fn make_private(target: &std::ffi::CStr) -> io::Result<()> {
        // SAFETY: target is a NUL-terminated C string that lives for the call.
        let result = unsafe {
            libc::mount(
                std::ptr::null(),
                target.as_ptr(),
                std::ptr::null(),
                libc::MS_PRIVATE,
                std::ptr::null(),
            )
        };
        if result == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    #[allow(unsafe_code)]
    fn mount_remount_nosuid_nodev(target: &std::ffi::CStr) -> io::Result<()> {
        // MS_BIND restricts this policy change to the export, not the backing filesystem.
        // SAFETY: target is a NUL-terminated C string that lives for the call.
        let result = unsafe {
            libc::mount(
                std::ptr::null(),
                target.as_ptr(),
                std::ptr::null(),
                libc::MS_BIND | libc::MS_REMOUNT | libc::MS_NOSUID | libc::MS_NODEV,
                std::ptr::null(),
            )
        };
        if result == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    #[allow(unsafe_code)]
    fn normal_unmount(path: &std::ffi::CStr) -> io::Result<()> {
        // SAFETY: path is NUL-terminated and live. Flags 0 request normal unmount.
        let result = unsafe { libc::umount2(path.as_ptr(), 0) };
        if result == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    #[allow(unsafe_code)]
    fn lazy_unmount(path: &std::ffi::CStr) -> io::Result<()> {
        // SAFETY: path is NUL-terminated and live. MNT_DETACH (2) detaches the
        // mount from the namespace immediately; it never fails on a busy tree,
        // and the covered target reappears once the last reference closes.
        let result = unsafe { libc::umount2(path.as_ptr(), libc::MNT_DETACH) };
        if result == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::{fs, os::unix::ffi::OsStrExt, path::Path};

        fn dir_fd(path: &Path) -> File {
            fs::File::open(path).unwrap()
        }

        #[test]
        fn authorized_workspace_egress_rejects_ordinary_owner_and_prepares_native_target() {
            use crate::node::vfs::ownerfs::native_home_tests::{fixture, mkdir_root};
            for eligible in [false, true] {
                let (_temp, owner, ctx, _disk) = fixture(eligible);
                mkdir_root(&owner, &ctx, "workspace");
                let egress = tempfile::tempdir().unwrap();
                let result = AuthorizedWorkspaceBind::prepare_egress(
                    Arc::new(owner),
                    egress.path(),
                    OsStr::new("workspace"),
                );
                if eligible {
                    // A native egress root yields a claim without mounting; the
                    // empty target directory is created for the bind.
                    result.expect("native-eligible owner must prepare an egress claim");
                    assert!(egress.path().join("workspace").is_dir());
                } else {
                    let error = result.err().expect("ordinary owner must fail");
                    assert!(error.to_string().contains("native"));
                }
            }
        }

        #[test]
        fn prepare_egress_rejects_non_directory_target_and_re_adopts_empty_dir() {
            use crate::node::vfs::ownerfs::native_home_tests::{fixture, mkdir_root};
            let (_temp, owner, ctx, _disk) = fixture(true);
            mkdir_root(&owner, &ctx, "workspace");
            let owner = Arc::new(owner);
            let egress = tempfile::tempdir().unwrap();

            // A non-directory target must be rejected rather than bound over.
            std::fs::write(egress.path().join("workspace"), b"plain file").unwrap();
            let error = AuthorizedWorkspaceBind::prepare_egress(
                owner.clone(),
                egress.path(),
                OsStr::new("workspace"),
            )
            .err()
            .expect("a file target must fail");
            assert!(matches!(
                error.raw_os_error(),
                Some(libc::ENOTDIR | libc::ELOOP)
            ));
            std::fs::remove_file(egress.path().join("workspace")).unwrap();

            // An existing empty directory from a previous run is re-adopted.
            fs::create_dir(egress.path().join("workspace")).unwrap();
            AuthorizedWorkspaceBind::prepare_egress(owner, egress.path(), OsStr::new("workspace"))
                .expect("an existing empty target is idempotent");
        }

        #[test]
        #[ignore = "requires root Linux private mount namespace; run explicitly"]
        fn authorized_workspace_egress_binds_and_detaches_after_revocation() {
            use crate::node::vfs::ownerfs::native_home_tests::{fixture, mkdir_root};
            assert!(can_create_private_mount_namespace());
            let (temp, owner, ctx, _disk) = fixture(true);
            mkdir_root(&owner, &ctx, "workspace");
            let owner = Arc::new(owner);
            let retained = temp.keep();
            let egress = retained.join("egress");
            fs::create_dir(&egress).unwrap();
            let mut bind = AuthorizedWorkspaceBind::prepare_egress(
                owner.clone(),
                &egress,
                OsStr::new("workspace"),
            )
            .unwrap();
            bind.activate().unwrap();
            bind.verify_current().unwrap();
            let host_target = egress.join("workspace");
            std::thread::spawn(move || {
                fs::write(host_target.join("proof"), b"physical Home").unwrap();
                File::open(host_target.join("proof"))
                    .unwrap()
                    .sync_all()
                    .unwrap();
            })
            .join()
            .unwrap();
            assert_eq!(
                fs::read(format!(
                    "/proc/self/fd/{}/proof",
                    bind.export.source.as_raw_fd()
                ))
                .unwrap(),
                b"physical Home"
            );
            // A stale authority rejects continued admission, but cannot strand
            // the owned mount by making normal detach depend on that authority.
            owner.require_local().unwrap().roots.invalidate_all();
            assert!(bind.verify_current().is_err());
            bind.detach().unwrap();
            drop(bind);
            fs::remove_dir_all(retained).unwrap();
        }

        #[test]
        fn egress_bind_name_from_line_matches_managed_signature_only() {
            let root = Path::new("/egress_root");
            // 合成 mountinfo 行：identity 的第 5 个字段（0 基）是挂载选项，
            // nosuid,nodev 位于其中；detail 的第 3 个字段是 super options。
            let line = |mount_options: &str, super_options: &str| {
                format!(
                    "42 7 8:1 / {}/workspace {} - ext4 /var/lib/afs rw,relatime {}",
                    root.display(),
                    mount_options,
                    super_options
                )
            };

            // 挂载选项携带策略 → 识别为 managed bind。
            let name = egress_bind_name_from_line(&line("rw,nosuid,nodev", "rw,relatime"), root);
            assert_eq!(
                name.as_deref().map(OsStr::new),
                Some(OsStr::new("workspace"))
            );

            // 挂载选项缺策略 → 拒绝。
            assert!(
                egress_bind_name_from_line(&line("rw,relatime", "rw,relatime"), root).is_none()
            );

            // 策略只出现在 super options（detail 字段）而挂载选项没有 →
            // 不是本模块创建的挂载，必须拒绝。回归 review: mountinfo 读取
            // 字段错误导致残留挂载无法回收。
            assert!(
                egress_bind_name_from_line(&line("rw,relatime", "rw,nosuid,nodev"), root).is_none()
            );

            // 挂载选项与 super options 都带策略 → 识别。
            let name =
                egress_bind_name_from_line(&line("rw,nosuid,nodev", "rw,nosuid,nodev"), root);
            assert_eq!(
                name.as_deref().map(OsStr::new),
                Some(OsStr::new("workspace"))
            );

            // 非直系子路径 /egress_root/a/b → 拒绝。
            let nested = format!(
                "42 7 8:1 / {}/a/b rw,nosuid,nodev - ext4 /x rw",
                root.display()
            );
            assert!(egress_bind_name_from_line(&nested, root).is_none());

            // 挂载点就是 egress_root 本身 → 拒绝。
            let exact = format!("42 7 8:1 / {} rw,nosuid,nodev - ext4 /x rw", root.display());
            assert!(egress_bind_name_from_line(&exact, root).is_none());

            // 不在 egress_root 之下 → 拒绝。
            let foreign = "42 7 8:1 / /other/workspace rw,nosuid,nodev - ext4 /x rw";
            assert!(egress_bind_name_from_line(foreign, root).is_none());
        }

        #[test]
        fn rejects_invalid_components_before_mount_work() {
            let temp = tempfile::tempdir().unwrap();
            let source = dir_fd(temp.path());
            let parent = dir_fd(temp.path());

            for name in ["", ".", "..", "nested/name"] {
                let error = WorkspaceBindMount::prepare(
                    source.try_clone().unwrap(),
                    parent.try_clone().unwrap(),
                    OsStr::new(name),
                )
                .unwrap_err();
                assert_eq!(error.raw_os_error(), Some(libc::EINVAL));
            }
        }

        #[test]
        fn rejects_symlink_target_component() {
            let temp = tempfile::tempdir().unwrap();
            fs::create_dir(temp.path().join("source")).unwrap();
            std::os::unix::fs::symlink("source", temp.path().join("target")).unwrap();

            let error = WorkspaceBindMount::prepare(
                dir_fd(&temp.path().join("source")),
                dir_fd(temp.path()),
                OsStr::new("target"),
            )
            .unwrap_err();
            assert!(matches!(
                error.raw_os_error(),
                Some(libc::ENOTDIR | libc::ELOOP)
            ));
        }

        #[test]
        fn secondary_clone_rejects_invalid_component_before_namespace_entry() {
            let temp = tempfile::tempdir().unwrap();
            let namespace = File::open("/proc/thread-self/ns/mnt").unwrap();
            let root = dir_fd(temp.path());
            let before = current_namespace().unwrap();
            let error =
                inspect_secondary_clone(&namespace, &root, OsStr::new("nested/name")).unwrap_err();
            assert_eq!(error.raw_os_error(), Some(libc::EINVAL));
            assert_eq!(current_namespace().unwrap(), before);
            let root_identity = directory_identity(&root).unwrap();
            let error = detach_secondary_clone(
                &namespace,
                &root,
                OsStr::new("nested/name"),
                root_identity,
                1,
                root_identity,
            )
            .unwrap_err();
            assert_eq!(error.raw_os_error(), Some(libc::EINVAL));
            assert_eq!(current_namespace().unwrap(), before);
        }

        #[test]
        #[ignore = "requires root Linux private mount namespace; run explicitly"]
        fn attaches_and_detaches_in_private_mount_namespace() {
            assert!(
                can_create_private_mount_namespace(),
                "private mount namespace admission failed"
            );
            let temp = tempfile::tempdir().unwrap();
            fs::create_dir(temp.path().join("source")).unwrap();
            fs::create_dir(temp.path().join("target")).unwrap();
            fs::write(temp.path().join("source/file"), b"native").unwrap();

            let target_before = dir_fd(&temp.path().join("target"));
            let covered = directory_identity(&target_before).unwrap();
            let mut export = WorkspaceBindMount::prepare(
                dir_fd(&temp.path().join("source")),
                dir_fd(temp.path()),
                OsStr::new("target"),
            )
            .expect("workspace bind mount preparation failed");
            export
                .activate()
                .expect("workspace bind mount activation failed");
            assert_eq!(
                export.source_identity(),
                export.mount_identity().unwrap().source
            );
            assert_eq!(
                fs::read(temp.path().join("target/file")).unwrap(),
                b"native"
            );

            export.detach().unwrap();
            let target_after = dir_fd(&temp.path().join("target"));
            assert_eq!(directory_identity(&target_after).unwrap(), covered);
            assert!(!temp.path().join("target/file").exists());
        }

        #[test]
        #[ignore = "requires root Linux private mount namespace; run explicitly"]
        fn detach_rejects_identity_drift_and_retains_claim() {
            assert!(
                can_create_private_mount_namespace(),
                "private mount namespace admission failed"
            );
            let temp = tempfile::tempdir().unwrap();
            fs::create_dir(temp.path().join("source")).unwrap();
            fs::create_dir(temp.path().join("foreign")).unwrap();
            fs::create_dir(temp.path().join("target")).unwrap();

            let mut export = WorkspaceBindMount::prepare(
                dir_fd(&temp.path().join("source")),
                dir_fd(temp.path()),
                OsStr::new("target"),
            )
            .expect("workspace bind mount preparation failed");
            export
                .activate()
                .expect("workspace bind mount activation failed");
            let claim = export.mount_identity().unwrap();
            bind_mount_private(&temp.path().join("foreign"), &temp.path().join("target")).unwrap();

            let error = export.detach().unwrap_err();
            assert_eq!(error.raw_os_error(), Some(libc::ESTALE));
            assert_eq!(export.mount_identity().unwrap(), claim);

            normal_unmount(
                &CString::new(temp.path().join("target").as_os_str().as_bytes()).unwrap(),
            )
            .unwrap();
            export.detach().unwrap();
        }

        #[test]
        #[ignore = "requires root Linux private mount namespace; run explicitly"]
        fn detach_of_unactivated_prepare_rejects_foreign_mount() {
            assert!(
                can_create_private_mount_namespace(),
                "private mount namespace admission failed"
            );
            let temp = tempfile::tempdir().unwrap();
            fs::create_dir(temp.path().join("source")).unwrap();
            fs::create_dir(temp.path().join("foreign")).unwrap();
            fs::create_dir(temp.path().join("target")).unwrap();

            let mut export = WorkspaceBindMount::prepare(
                dir_fd(&temp.path().join("source")),
                dir_fd(temp.path()),
                OsStr::new("target"),
            )
            .expect("workspace bind mount preparation failed");
            bind_mount_private(&temp.path().join("foreign"), &temp.path().join("target")).unwrap();

            let error = export.detach().unwrap_err();
            assert_eq!(error.raw_os_error(), Some(libc::ESTALE));
            normal_unmount(
                &CString::new(temp.path().join("target").as_os_str().as_bytes()).unwrap(),
            )
            .unwrap();
            export.detach().unwrap();
        }

        #[test]
        #[ignore = "requires root Linux private mount namespace; run explicitly"]
        fn secondary_clone_detach_rejects_wrong_identity_and_flags_with_proc_hidden() {
            assert!(
                can_create_private_mount_namespace(),
                "private mount namespace admission failed"
            );
            let controller_namespace = File::open("/proc/thread-self/ns/mnt").unwrap();
            let controller_identity = current_namespace().unwrap();
            let controller_cwd = dir_fd(Path::new("."));
            let controller_directory = directory_identity(&controller_cwd).unwrap();
            let temp = tempfile::tempdir().unwrap();
            fs::create_dir(temp.path().join("source")).unwrap();
            fs::create_dir(temp.path().join("root")).unwrap();
            fs::create_dir(temp.path().join("root/project")).unwrap();
            fs::write(temp.path().join("source/file"), b"native").unwrap();

            let root = dir_fd(&temp.path().join("root"));
            let component = OsStr::new("project");
            let component_c = validate_component(component).unwrap();
            let covered = directory_identity(&open_child(&root, &component_c).unwrap()).unwrap();
            let mut export = WorkspaceBindMount::prepare(
                dir_fd(&temp.path().join("source")),
                root.try_clone().unwrap(),
                component,
            )
            .expect("workspace bind mount preparation failed");
            export
                .activate()
                .expect("workspace bind mount activation failed");
            let claim = export.mount_identity().unwrap();
            assert_eq!(
                fs::read(temp.path().join("root/project/file")).unwrap(),
                b"native"
            );

            assert!(
                can_create_private_mount_namespace(),
                "final clone mount namespace admission failed"
            );
            let final_root = dir_fd(&temp.path().join("root"));
            let final_namespace = File::open("/proc/thread-self/ns/mnt").unwrap();
            hide_proc().unwrap();
            assert_eq!(
                File::open("/proc/thread-self/ns/mnt").unwrap_err().kind(),
                io::ErrorKind::NotFound
            );

            setns_mount(&controller_namespace).expect("restore controller namespace before probe");
            change_directory(&controller_cwd).unwrap();
            let (observed_namespace, observed_source, observed_unique) =
                inspect_secondary_clone(&final_namespace, &final_root, component).unwrap();
            assert_ne!(observed_namespace, claim.namespace);
            assert_eq!(observed_source, claim.source);
            assert_ne!(observed_unique, claim.unique_mount_id);
            assert_eq!(current_namespace().unwrap(), controller_identity);

            in_mount_namespace(&final_namespace, || {
                let target = open_child(&final_root, &component_c)?;
                clear_policy(&target)
            })
            .unwrap();
            assert_eq!(
                inspect_secondary_clone(&final_namespace, &final_root, component)
                    .unwrap_err()
                    .raw_os_error(),
                Some(libc::EPERM)
            );
            assert_eq!(current_namespace().unwrap(), controller_identity);
            assert_eq!(
                detach_secondary_clone(
                    &final_namespace,
                    &final_root,
                    component,
                    claim.source,
                    observed_unique,
                    covered
                )
                .unwrap_err()
                .raw_os_error(),
                Some(libc::EPERM)
            );
            assert_eq!(current_namespace().unwrap(), controller_identity);
            in_mount_namespace(&final_namespace, || {
                let target = open_child(&final_root, &component_c)?;
                apply_policy(&target)
            })
            .unwrap();
            let wrong = DirectoryIdentity {
                device: claim.source.device,
                inode: claim.source.inode.wrapping_add(1),
            };
            let error = detach_secondary_clone(
                &final_namespace,
                &final_root,
                component,
                wrong,
                observed_unique,
                covered,
            )
            .unwrap_err();
            assert_eq!(error.raw_os_error(), Some(libc::ESTALE));
            assert_eq!(current_namespace().unwrap(), controller_identity);
            let (_, still_source, still_unique) =
                inspect_secondary_clone(&final_namespace, &final_root, component).unwrap();
            assert_eq!(still_source, claim.source);
            assert_eq!(still_unique, observed_unique);
            assert_eq!(current_namespace().unwrap(), controller_identity);
            assert_eq!(
                directory_identity(&dir_fd(Path::new("."))).unwrap(),
                controller_directory
            );
            assert_eq!(
                detach_secondary_clone(
                    &final_namespace,
                    &final_root,
                    component,
                    claim.source,
                    observed_unique.wrapping_add(1),
                    covered
                )
                .unwrap_err()
                .raw_os_error(),
                Some(libc::ESTALE)
            );
            assert_eq!(current_namespace().unwrap(), controller_identity);
            assert_eq!(
                directory_identity(&dir_fd(Path::new("."))).unwrap(),
                controller_directory
            );

            detach_secondary_clone(
                &final_namespace,
                &final_root,
                component,
                claim.source,
                observed_unique,
                covered,
            )
            .unwrap();
            assert_eq!(current_namespace().unwrap(), controller_identity);
            assert_eq!(
                directory_identity(&dir_fd(Path::new("."))).unwrap(),
                controller_directory
            );
            let (restored_source, restored_unique) = in_mount_namespace(&final_namespace, || {
                let target = open_child(&final_root, &component_c)?;
                Ok((directory_identity(&target)?, unique_mount_id(&target)?))
            })
            .unwrap();
            assert_eq!(restored_source, covered);
            assert_ne!(restored_unique, observed_unique);
            assert_eq!(
                fs::read(temp.path().join("root/project/file")).unwrap(),
                b"native"
            );
            export.detach().unwrap();
            let restored = open_child(&root, &component_c).unwrap();
            assert_eq!(directory_identity(&restored).unwrap(), covered);
            assert!(!temp.path().join("root/project/file").exists());
        }

        #[allow(unsafe_code)]
        fn hide_proc() -> io::Result<()> {
            // Only this test's cloned private namespace loses its proc view.
            // SAFETY: all C strings have static lifetime, optional arguments are
            // null, and this test owns a private mount namespace.
            if unsafe {
                libc::mount(
                    std::ptr::null(),
                    c"/proc".as_ptr(),
                    c"tmpfs".as_ptr(),
                    0,
                    std::ptr::null(),
                )
            } == 0
            {
                Ok(())
            } else {
                Err(io::Error::last_os_error())
            }
        }

        #[allow(unsafe_code)]
        fn clear_policy(target: &File) -> io::Result<()> {
            let attr = MountAttr {
                attr_set: 0,
                attr_clr: MOUNT_ATTR_NOSUID | MOUNT_ATTR_NODEV,
                propagation: 0,
                userns_fd: 0,
            };
            // SAFETY: same live descriptor and UAPI layout as production setter;
            // confined to this test's owned final mount namespace.
            if unsafe {
                libc::syscall(
                    libc::SYS_mount_setattr,
                    target.as_raw_fd(),
                    c"".as_ptr(),
                    libc::AT_EMPTY_PATH as u32,
                    &attr as *const MountAttr,
                    std::mem::size_of::<MountAttr>(),
                )
            } != 0
            {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        }
        #[allow(unsafe_code)]
        fn can_create_private_mount_namespace() -> bool {
            // SAFETY: unshare takes integer flags; the subsequent mount uses a live
            // CString inside this private test namespace.
            unsafe {
                if libc::unshare(libc::CLONE_NEWNS) != 0 {
                    return false;
                }
                let slash = CString::new("/").unwrap();
                libc::mount(
                    std::ptr::null(),
                    slash.as_ptr(),
                    std::ptr::null(),
                    libc::MS_REC | libc::MS_PRIVATE,
                    std::ptr::null(),
                ) == 0
            }
        }

        #[allow(unsafe_code)]
        fn bind_mount_private(source: &Path, target: &Path) -> io::Result<()> {
            let source = CString::new(source.as_os_str().as_bytes()).unwrap();
            let target = CString::new(target.as_os_str().as_bytes()).unwrap();
            // SAFETY: source and target are live NUL-terminated CStrings; mount uses
            // no retained Rust pointers.
            let result = unsafe {
                libc::mount(
                    source.as_ptr(),
                    target.as_ptr(),
                    std::ptr::null(),
                    libc::MS_BIND,
                    std::ptr::null(),
                )
            };
            if result == 0 {
                Ok(())
            } else {
                Err(io::Error::last_os_error())
            }
        }
    }
}

#[cfg(target_os = "linux")]
pub(in crate::node) use linux::{
    AuthorizedWorkspaceBind, WorkspaceBindMount, detach_egress_bind, detach_secondary_clone,
    inspect_secondary_clone, scan_egress_binds,
};
