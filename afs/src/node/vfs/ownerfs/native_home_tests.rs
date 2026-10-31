use super::*;
use std::os::{fd::AsRawFd, unix::fs::MetadataExt};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use crate::node::vfs::ownerfs::root::{
    OwnerRootInventory, PresentedRootAccess, RootLocation, RootMeta, RootReservation,
};

struct RejectRemoteFactory;

impl RemoteFilesFactory for RejectRemoteFactory {
    fn connect(&self, _: &str) -> Result<Arc<dyn remote::RemoteFiles>> {
        Err(Error::coded(
            afs_error::NODE_VFS_UNIMPLEMENTED,
            "native home test does not connect remote files",
        ))
    }
}

struct NativeHomeMeta {
    node_id: String,
    session_id: String,
    next_epoch: AtomicU64,
    active: Mutex<HashMap<RootId, RootLocation>>,
    fail_deactivate: AtomicBool,
}

impl NativeHomeMeta {
    fn new() -> Self {
        Self {
            node_id: "node-a".into(),
            session_id: "session-a".into(),
            next_epoch: AtomicU64::new(1),
            active: Mutex::new(HashMap::new()),
            fail_deactivate: AtomicBool::new(false),
        }
    }

    /// 故障注入：置真后 `deactivate_root` 返回 Meta 不可达错误（VC-D6）。
    fn set_fail_deactivate(&self, fail: bool) {
        self.fail_deactivate.store(fail, Ordering::SeqCst);
    }
}

impl RootMeta for NativeHomeMeta {
    fn reserve_root(&self, id: &RootId, create_intent_id: &str) -> Result<RootReservation> {
        let epoch = self.next_epoch.fetch_add(1, Ordering::SeqCst);
        Ok(RootReservation {
            id: id.clone(),
            epoch,
            home_node_id: self.node_id.clone(),
            session_id: self.session_id.clone(),
            create_intent_id: create_intent_id.to_owned(),
            prepare_token: format!("prepare-{epoch}"),
        })
    }

    fn activate_root(&self, prepared: &root::PreparedRoot) -> Result<RootGrant> {
        self.active.lock().unwrap().insert(
            prepared.reservation().id.clone(),
            RootLocation {
                id: prepared.reservation().id.clone(),
                epoch: prepared.reservation().epoch,
                home_node_id: self.node_id.clone(),
                home_session_id: self.session_id.clone(),
            },
        );
        Ok(RootGrant {
            id: prepared.reservation().id.clone(),
            epoch: prepared.reservation().epoch,
            home_node_id: self.node_id.clone(),
            home_session_id: self.session_id.clone(),
            holder_node_id: self.node_id.clone(),
            session_id: self.session_id.clone(),
            access_generation: prepared.reservation().epoch,
            rights: vec![RootRight::Lookup, RootRight::Read, RootRight::Write],
            fencing_token: format!("fence-{}", prepared.reservation().epoch),
        })
    }

    fn abort_root(&self, _: &RootReservation) -> Result<()> {
        Ok(())
    }

    fn deactivate_root(&self, root_id: &RootId, _: u64, _: &str, _: &str) -> Result<()> {
        if self.fail_deactivate.load(Ordering::SeqCst) {
            return Err(Error::coded(
                afs_error::NODE_VFS_UNAVAILABLE,
                "meta unreachable (injected)",
            ));
        }
        self.active.lock().unwrap().remove(root_id);
        Ok(())
    }

    fn lookup_root(&self, id: &RootId) -> Result<Option<RootLocation>> {
        Ok(self.active.lock().unwrap().get(id).cloned())
    }

    fn list_owner_roots(&self, _: &str) -> Result<OwnerRootInventory> {
        Ok(OwnerRootInventory {
            active: self.active.lock().unwrap().values().cloned().collect(),
            pending: Vec::new(),
        })
    }

    fn acquire_root(&self, _: &RootId, _: RootRight) -> Result<RootGrant> {
        Err(Error::coded(
            afs_error::NODE_OWNER_GRANT_UNAVAILABLE,
            "native home test does not acquire remote roots",
        ))
    }

    fn validate_root_access(
        &self,
        presented: &PresentedRootAccess,
        authenticated_peer_node_id: &str,
    ) -> Result<RootGrant> {
        Ok(RootGrant {
            id: presented.id.clone(),
            epoch: presented.epoch,
            home_node_id: presented.home_node_id.clone(),
            home_session_id: presented.home_session_id.clone(),
            holder_node_id: authenticated_peer_node_id.to_owned(),
            session_id: presented.session_id.clone(),
            access_generation: presented.access_generation,
            rights: vec![RootRight::Lookup, RootRight::Read, RootRight::Write],
            fencing_token: presented.fencing_token.clone(),
        })
    }

    fn recover_root(
        &self,
        record: &catalog::LocalRootRecord,
        new_session_id: &str,
    ) -> Result<RootGrant> {
        Ok(RootGrant {
            id: record.id.clone(),
            epoch: record.epoch,
            home_node_id: self.node_id.clone(),
            home_session_id: new_session_id.to_owned(),
            holder_node_id: self.node_id.clone(),
            session_id: new_session_id.to_owned(),
            access_generation: record.epoch,
            rights: vec![RootRight::Lookup, RootRight::Read, RootRight::Write],
            fencing_token: format!("recover-fence-{}", record.epoch),
        })
    }
}

pub(crate) fn fixture(
    native_home_eligible: bool,
) -> (tempfile::TempDir, OwnerFs, RequestContext, Arc<LocalFs>) {
    let (temp, fs, ctx, disk, _meta) = fixture_with_meta(native_home_eligible);
    (temp, fs, ctx, disk)
}

/// `fixture` 的变体，额外返回共享 Meta 句柄（用于故障注入断言）。
fn fixture_with_meta(
    native_home_eligible: bool,
) -> (
    tempfile::TempDir,
    OwnerFs,
    RequestContext,
    Arc<LocalFs>,
    Arc<NativeHomeMeta>,
) {
    let temp = tempfile::tempdir().unwrap();
    let disk = Arc::new(LocalFs::open(temp.path()).unwrap());
    let meta = Arc::new(NativeHomeMeta::new());
    let roots = Arc::new(RootManager::new(
        "node-a".into(),
        "session-a".into(),
        meta.clone(),
        disk.clone(),
    ));
    let metadata = fs::metadata(temp.path()).unwrap();
    let ctx = RequestContext {
        uid: metadata.uid(),
        gid: metadata.gid(),
        pid: 42,
        umask: 0,
        supplementary_gids: Vec::new(),
    };
    let fs = if native_home_eligible {
        OwnerFs::new_local_native_home_for_tests(roots, disk.clone())
    } else {
        OwnerFs::new_local(roots, disk.clone())
    };
    (temp, fs, ctx, disk, meta)
}

/// Build a native-Home-eligible OwnerFs with a MountManager attached, ready
/// for mkdir-triggered bind mount integration tests. `egress_root` is passed
/// through to the MountManager; the caller must call `attach_owner` after
/// wrapping the result in an `Arc`.
pub(crate) fn fixture_with_mount_manager(
    egress_root: PathBuf,
) -> (tempfile::TempDir, OwnerFs, RequestContext) {
    let temp = tempfile::tempdir().unwrap();
    let disk = Arc::new(LocalFs::open(temp.path()).unwrap());
    let meta = Arc::new(NativeHomeMeta::new());
    let roots = Arc::new(RootManager::new(
        "node-a".into(),
        "session-a".into(),
        meta,
        disk.clone(),
    ));
    let metadata = fs::metadata(temp.path()).unwrap();
    let ctx = RequestContext {
        uid: metadata.uid(),
        gid: metadata.gid(),
        pid: 42,
        umask: 0,
        supplementary_gids: Vec::new(),
    };
    let fs = OwnerFs::new_local_with_mount_manager(
        roots,
        disk,
        Arc::new(RejectRemoteFactory),
        egress_root,
    );
    (temp, fs, ctx)
}

fn fixture_native_home_eligible_with_remote() -> (tempfile::TempDir, OwnerFs, RequestContext) {
    let temp = tempfile::tempdir().unwrap();
    let disk = Arc::new(LocalFs::open(temp.path()).unwrap());
    let meta = Arc::new(NativeHomeMeta::new());
    let roots = Arc::new(RootManager::new(
        "node-a".into(),
        "session-a".into(),
        meta,
        disk.clone(),
    ));
    let metadata = fs::metadata(temp.path()).unwrap();
    let ctx = RequestContext {
        uid: metadata.uid(),
        gid: metadata.gid(),
        pid: 42,
        umask: 0,
        supplementary_gids: Vec::new(),
    };
    let fs =
        OwnerFs::new_local_native_eligible_with_remote(roots, disk, Arc::new(RejectRemoteFactory));
    (temp, fs, ctx)
}

pub(crate) fn mkdir_root(fs: &OwnerFs, ctx: &RequestContext, name: &str) -> Entry {
    Backend::mkdir(fs, ctx, fs.root_inode(), OsStr::new(name), 0o755).unwrap()
}

fn lookup_root(fs: &OwnerFs, ctx: &RequestContext, name: &str) -> Result<Entry> {
    Backend::lookup(fs, ctx, fs.root_inode(), OsStr::new(name))
}

#[test]
fn native_home_fresh_lookup_after_native_rename_opens_observed_path() {
    let (temp, owner, ctx, _disk) = fixture(true);
    let root = mkdir_root(&owner, &ctx, "workspace");
    let created = Backend::create(
        &owner,
        &ctx,
        root.inode,
        OsStr::new("old"),
        0o600,
        libc::O_RDWR,
    )
    .unwrap();
    Backend::write(&owner, &ctx, created.handle, 0, b"confirmed").unwrap();
    Backend::flush(&owner, &ctx, created.handle).unwrap();
    let authority = owner
        .native_home_export_for_current_namespace(OsStr::new("workspace"))
        .unwrap();
    let physical = temp.path().join(authority.data_dir().as_path());
    fs::rename(physical.join("old"), physical.join("new")).unwrap();

    let observed = Backend::lookup(&owner, &ctx, root.inode, OsStr::new("new")).unwrap();
    assert_eq!(observed.inode, created.entry.inode);
    assert_eq!(
        Backend::getattr(&owner, &ctx, observed.inode, None)
            .unwrap()
            .size,
        9
    );
    let fresh = Backend::open(&owner, &ctx, observed.inode, libc::O_RDONLY).unwrap();
    for handle in [created.handle, fresh] {
        let mut data = [0_u8; 9];
        assert_eq!(
            Backend::read(&owner, &ctx, handle, 0, &mut data).unwrap(),
            9
        );
        assert_eq!(&data, b"confirmed");
        Backend::release(&owner, &ctx, handle).unwrap();
    }
    assert!(Backend::lookup(&owner, &ctx, root.inode, OsStr::new("old")).is_err());
}

#[test]
fn native_home_cached_hardlink_lookup_rebinds_after_native_unlink() {
    let (temp, owner, ctx, _disk) = fixture(true);
    let root = mkdir_root(&owner, &ctx, "workspace");
    let created = Backend::create(
        &owner,
        &ctx,
        root.inode,
        OsStr::new("old"),
        0o600,
        libc::O_RDWR,
    )
    .unwrap();
    Backend::write(&owner, &ctx, created.handle, 0, b"linked").unwrap();
    Backend::flush(&owner, &ctx, created.handle).unwrap();
    let authority = owner
        .native_home_export_for_current_namespace(OsStr::new("workspace"))
        .unwrap();
    let physical = temp.path().join(authority.data_dir().as_path());
    fs::hard_link(physical.join("old"), physical.join("alias")).unwrap();
    let alias = Backend::lookup(&owner, &ctx, root.inode, OsStr::new("alias")).unwrap();
    assert_eq!(alias.inode, created.entry.inode);
    fs::remove_file(physical.join("old")).unwrap();

    let observed = Backend::lookup(&owner, &ctx, root.inode, OsStr::new("alias")).unwrap();
    assert_eq!(observed.inode, alias.inode);
    let fresh = Backend::open(&owner, &ctx, observed.inode, libc::O_RDONLY).unwrap();
    let mut data = [0_u8; 6];
    assert_eq!(Backend::read(&owner, &ctx, fresh, 0, &mut data).unwrap(), 6);
    assert_eq!(&data, b"linked");
    Backend::release(&owner, &ctx, fresh).unwrap();
    Backend::release(&owner, &ctx, created.handle).unwrap();
}

#[test]
fn native_home_ordinary_instance_rejects_authority_and_keeps_private_cache() {
    let (_temp, fs, ctx, _disk) = fixture(false);
    let root = mkdir_root(&fs, &ctx, "ordinary");

    let error = match fs.native_home_export_for_current_namespace(OsStr::new("ordinary")) {
        Ok(_) => panic!("ordinary OwnerFs unexpectedly issued native Home authority"),
        Err(error) => error,
    };
    assert_eq!(error.code(), afs_error::NODE_OWNER_INVALID_GRANT);

    fs.with_fuse_cache_policy(root.inode, |ttl, private| {
        assert_eq!(ttl, Duration::from_secs(1));
        assert!(private);
    });
}

#[test]
fn native_home_eligible_instance_uses_zero_ttl_from_first_reply() {
    let (_temp, fs, ctx, _disk) = fixture(true);
    let root = mkdir_root(&fs, &ctx, "native");

    fs.with_fuse_cache_policy(root.inode, |ttl, private| {
        assert_eq!(ttl, Duration::ZERO);
        assert!(!private);
    });

    let authority: HomeExportAuthority = fs
        .native_home_export_for_current_namespace(OsStr::new("native"))
        .unwrap();
    authority.verify_current(&fs).unwrap();
}

#[test]
fn native_home_explicit_remote_constructor_is_eligible() {
    let (_temp, fs, ctx) = fixture_native_home_eligible_with_remote();
    let root = mkdir_root(&fs, &ctx, "native");

    fs.with_fuse_cache_policy(root.inode, |ttl, private| {
        assert_eq!(ttl, Duration::ZERO);
        assert!(!private);
    });

    fs.native_home_export_for_current_namespace(OsStr::new("native"))
        .unwrap()
        .verify_current(&fs)
        .unwrap();
}

#[test]
fn native_home_authority_exports_thread_namespace_and_cloned_source_descriptor() {
    let (_temp, fs, ctx, _disk) = fixture(true);
    mkdir_root(&fs, &ctx, "native");

    let authority = fs
        .native_home_export_for_current_namespace(OsStr::new("native"))
        .unwrap();
    assert_eq!(authority.name(), OsStr::new("native"));
    assert_eq!(authority.grant().home_node_id, "node-a");
    assert_eq!(authority.grant().holder_node_id, "node-a");
    assert_eq!(authority.grant().home_session_id, "session-a");
    assert_eq!(authority.grant().session_id, "session-a");
    assert_eq!(authority.grant().access_generation, authority.grant().epoch);
    for right in [RootRight::Lookup, RootRight::Read, RootRight::Write] {
        assert!(authority.grant().rights.contains(&right));
    }

    let expected_namespace = fs::metadata("/proc/thread-self/ns/mnt").unwrap();
    assert_eq!(
        authority.namespace(),
        NativeHomeNamespaceIdentity {
            dev: expected_namespace.dev(),
            ino: expected_namespace.ino(),
        }
    );

    let first = authority.source_descriptor().unwrap();
    let second = authority.source_descriptor().unwrap();
    assert_ne!(first.as_raw_fd(), second.as_raw_fd());
    let metadata = first.metadata().unwrap();
    assert!(metadata.is_dir());
    assert_eq!(
        authority.source_identity(),
        NativeHomeDirectoryIdentity {
            dev: metadata.dev(),
            ino: metadata.ino(),
        }
    );
    authority.verify_current(&fs).unwrap();
}

#[test]
fn native_home_authority_fails_closed_for_wrong_owner() {
    let (_left_temp, left, left_ctx, _left_disk) = fixture(true);
    mkdir_root(&left, &left_ctx, "native");
    let authority = left
        .native_home_export_for_current_namespace(OsStr::new("native"))
        .unwrap();

    let (_right_temp, right, right_ctx, _right_disk) = fixture(true);
    mkdir_root(&right, &right_ctx, "native");

    let error = authority.verify_current(&right).unwrap_err();
    assert_eq!(error.code(), afs_error::NODE_OWNER_INVALID_GRANT);
}

#[test]
fn native_home_authority_fails_closed_after_source_replacement() {
    let (temp, fs, ctx, _disk) = fixture(true);
    mkdir_root(&fs, &ctx, "native");
    let authority = fs
        .native_home_export_for_current_namespace(OsStr::new("native"))
        .unwrap();

    let data_path = temp.path().join(authority.data_dir().as_path());
    let backup_path = temp.path().join("native-replaced-backup");
    fs::rename(&data_path, &backup_path).unwrap();
    fs::create_dir(&data_path).unwrap();

    let error = authority.verify_current(&fs).unwrap_err();
    assert_eq!(error.code(), afs_error::NODE_OWNER_INVALID_GRANT);
}

#[test]
fn native_home_authority_fails_closed_after_revoke_and_invalidate() {
    let (_temp, fs, ctx, _disk) = fixture(true);
    mkdir_root(&fs, &ctx, "native");
    let authority = fs
        .native_home_export_for_current_namespace(OsStr::new("native"))
        .unwrap();
    let root_id = authority.root_id().clone();

    fs.require_local().unwrap().roots.revoke_root(&root_id);
    assert!(authority.verify_current(&fs).is_err());

    drop(authority);
    mkdir_root(&fs, &ctx, "fresh");
    let fresh = fs
        .native_home_export_for_current_namespace(OsStr::new("fresh"))
        .unwrap();
    fs.require_local().unwrap().roots.invalidate_all();
    assert!(fresh.verify_current(&fs).is_err());
}

#[test]
fn native_home_anchor_covers_only_root_metadata_until_authority_drops() {
    let (_temp, fs, ctx, _disk) = fixture(true);
    let root = mkdir_root(&fs, &ctx, "native");
    Backend::mkdir(&fs, &ctx, root.inode, OsStr::new("child"), 0o755).unwrap();
    let authority = fs
        .native_home_export_for_current_namespace(OsStr::new("native"))
        .unwrap();
    let root_id = authority.root_id().clone();

    fs.require_local().unwrap().roots.revoke_root(&root_id);

    let anchored = lookup_root(&fs, &ctx, "native").unwrap();
    assert_eq!(anchored.inode, root.inode);
    Backend::getattr(&fs, &ctx, root.inode, None).unwrap();
    assert!(Backend::lookup(&fs, &ctx, root.inode, OsStr::new("child")).is_err());

    drop(authority);
    assert!(lookup_root(&fs, &ctx, "native").is_err());
}

#[test]
fn native_home_stale_full_grant_is_rejected() {
    let (_temp, fs, ctx, _disk) = fixture(true);
    mkdir_root(&fs, &ctx, "native");
    let authority = fs
        .native_home_export_for_current_namespace(OsStr::new("native"))
        .unwrap();
    let root_id = authority.root_id().clone();

    fs.require_local().unwrap().roots.revoke_root(&root_id);
    let error = authority.verify_current(&fs).unwrap_err();
    assert_eq!(error.code(), afs_error::NODE_OWNER_GRANT_UNAVAILABLE);
}

/// 取本地 catalog 中该 root 的记录（id + 物理路径）。
fn root_snapshot(
    owner: &OwnerFs,
    temp: &std::path::Path,
    name: &str,
) -> (RootId, std::path::PathBuf) {
    let roots = owner.require_local().unwrap().roots.clone();
    let records = roots.cached_local_roots().unwrap();
    let record = records
        .iter()
        .find(|record| record.name == name)
        .expect("root present in catalog");
    (record.id.clone(), temp.join(record.data_dir.as_path()))
}

/// mountinfo 是否出现 `<egress_root>/<name>` 挂载点。
fn egress_bind_mounted(target: &std::path::Path) -> bool {
    std::fs::read_to_string("/proc/self/mountinfo")
        .unwrap()
        .lines()
        .any(|line| {
            line.split_whitespace()
                .nth(4)
                .is_some_and(|p| p == target.to_str().unwrap())
        })
}

/// 等待异步 mount worker 完成真实 bind（超时 5s）。
fn wait_for_egress_bind(target: &std::path::Path) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while !egress_bind_mounted(target) {
        assert!(
            std::time::Instant::now() < deadline,
            "bind worker did not mount egress target within timeout"
        );
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}

/// VC-D1：FUSE-only root rmdir 一次成功，root 及全部元数据清除。
#[test]
fn rmdir_root_deletes_fuse_only_root_completely() {
    let (temp, owner, ctx, _disk) = fixture(false);
    let root = mkdir_root(&owner, &ctx, "d1");
    let (root_id, data_dir) = root_snapshot(&owner, temp.path(), "d1");
    assert!(data_dir.is_dir());
    // rm -rf 完整形态：先写子文件再删除子路径（走 FUSE unlink），最后 rmdir root。
    let created = Backend::create(
        &owner,
        &ctx,
        root.inode,
        OsStr::new("file"),
        0o600,
        libc::O_RDWR,
    )
    .unwrap();
    Backend::write(&owner, &ctx, created.handle, 0, b"x").unwrap();
    Backend::flush(&owner, &ctx, created.handle).unwrap();
    Backend::unlink(&owner, &ctx, root.inode, OsStr::new("file")).unwrap();

    Backend::rmdir(&owner, &ctx, owner.root_inode(), OsStr::new("d1")).unwrap();

    // lookup ENOENT；物理目录删除；Meta active 无记录；catalog 无记录。
    assert!(lookup_root(&owner, &ctx, "d1").is_err());
    assert!(!data_dir.exists());
    let roots = owner.require_local().unwrap().roots.clone();
    assert!(roots.lookup_root_location(&root_id).unwrap().is_none());
    assert!(roots.cached_local_roots().unwrap().is_empty());
}

/// VC-D3：删除后同名 root 可重建，新 root 有不同 epoch 且可读写。
#[test]
fn rmdir_root_then_recreate_same_name_succeeds() {
    let (temp, owner, ctx, _disk) = fixture(false);
    mkdir_root(&owner, &ctx, "d3");
    let (first_id, _) = root_snapshot(&owner, temp.path(), "d3");
    let first_epoch = owner
        .require_local()
        .unwrap()
        .roots
        .lookup_root_location(&first_id)
        .unwrap()
        .unwrap()
        .epoch;

    Backend::rmdir(&owner, &ctx, owner.root_inode(), OsStr::new("d3")).unwrap();

    let second = mkdir_root(&owner, &ctx, "d3");
    let (second_id, second_data_dir) = root_snapshot(&owner, temp.path(), "d3");
    let second_epoch = owner
        .require_local()
        .unwrap()
        .roots
        .lookup_root_location(&second_id)
        .unwrap()
        .unwrap()
        .epoch;
    // root_id 由 name 派生，同名重建 id 相同；epoch 必须不同。
    assert_eq!(first_id, second_id);
    assert_ne!(first_epoch, second_epoch);
    // 新 root 数据目录可写、FUSE 侧可见。
    let created = Backend::create(
        &owner,
        &ctx,
        second.inode,
        OsStr::new("new"),
        0o600,
        libc::O_RDWR,
    )
    .unwrap();
    Backend::write(&owner, &ctx, created.handle, 0, b"y").unwrap();
    Backend::flush(&owner, &ctx, created.handle).unwrap();
    assert!(second_data_dir.join("new").exists());
}

/// VC-D4 / §2.16：并发 rmdir 均成功（删除幂等）。后到的一方在缓存或
/// catalog 上看到前一方已完成的工作，按"已删除"收敛而不是报错。
#[test]
fn concurrent_rmdir_root_is_idempotent() {
    let (_temp, owner, ctx, _disk) = fixture(false);
    mkdir_root(&owner, &ctx, "d4");
    let owner = Arc::new(owner);
    let ctx = Arc::new(ctx);
    let results: Vec<afs_error::Result<()>> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..2)
            .map(|_| {
                let owner = owner.clone();
                let ctx = ctx.clone();
                scope.spawn(move || {
                    Backend::rmdir(owner.as_ref(), &ctx, owner.root_inode(), OsStr::new("d4"))
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect()
    });
    for result in &results {
        assert!(
            result.is_ok(),
            "concurrent rmdir must be idempotent: {result:?}"
        );
    }
    // 最终状态干净：lookup ENOENT、catalog 空。
    assert!(Backend::lookup(owner.as_ref(), &ctx, owner.root_inode(), OsStr::new("d4")).is_err());
    assert!(
        owner
            .require_local()
            .unwrap()
            .roots
            .cached_local_roots()
            .unwrap()
            .is_empty()
    );
}

/// VC-D6：Meta 不可达时 rmdir 返回错误；失败发生在 deactivate（delete_root
/// 步骤 3），Meta 记录、物理目录与 catalog 记录保留，缓存移除已发生（并发
/// 线性化点），故障恢复后状态可收敛。
#[test]
fn rmdir_root_meta_deactivate_failure_returns_error_and_keeps_state_repairable() {
    let (temp, owner, ctx, _disk, meta) = fixture_with_meta(false);
    mkdir_root(&owner, &ctx, "d6");
    let (root_id, data_dir) = root_snapshot(&owner, temp.path(), "d6");

    meta.set_fail_deactivate(true);
    let error = Backend::rmdir(&owner, &ctx, owner.root_inode(), OsStr::new("d6")).unwrap_err();
    assert_eq!(error.code(), afs_error::NODE_VFS_UNAVAILABLE);

    // 物理删除与缓存移除先于 deactivate 执行（线性化点与 Meta 无关）。
    assert!(!data_dir.exists());
    assert!(
        owner
            .require_local()
            .unwrap()
            .roots
            .cached_local_roots()
            .unwrap()
            .is_empty()
    );
    // 残留 = Meta active + catalog 记录（zombie 检测的收敛输入，不可丢失）。
    // catalog 独占锁由 manager 持有，从磁盘布局验证记录文件仍在。
    assert!(meta.lookup_root(&root_id).unwrap().is_some());
    let catalog_dir = temp.path().join(".ownerfs-roots");
    assert_eq!(
        std::fs::read_dir(&catalog_dir)
            .unwrap()
            .filter_map(|entry| entry.ok())
            .count(),
        1
    );

    // 故障恢复后 Meta 可正常清理，active 不再残留（状态可收敛）。
    meta.set_fail_deactivate(false);
    meta.deactivate_root(&root_id, 0, "node-a", "session-a")
        .unwrap();
    assert!(meta.lookup_root(&root_id).unwrap().is_none());
}

#[test]
#[ignore = "requires root Linux mount privileges; run explicitly"]
fn e2e_mkdir_egress_bind_then_rmdir_root_detaches_and_cleans() {
    // 真实 bind + rm -rf 链路：mkdir 触发异步 egress bind（D1 解耦路径），
    // root 级 rmdir 走 rmdir_root 五步（request_unmount detach bind → 物理目录删除
    // → state/cache 清理）。核心断言是 MNT_DETACH 后物理目录 remove_dir 不再 EBUSY。
    use std::time::{Duration, Instant};

    let retained = tempfile::tempdir().unwrap();
    let egress_root = retained.path().join("egress");
    std::fs::create_dir(&egress_root).unwrap();
    let (temp, fs, ctx) = fixture_with_mount_manager(egress_root.clone());
    let owner = Arc::new(fs);
    owner.mount_manager().unwrap().attach_owner(&owner);
    let root = mkdir_root(&owner, &ctx, "workspace");
    let target = egress_root.join("workspace");

    // 等待异步 mount worker 完成真实 bind：mountinfo 出现 <egress_root>/workspace。
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let mounted = std::fs::read_to_string("/proc/self/mountinfo")
            .unwrap()
            .lines()
            .any(|line| {
                line.split_whitespace()
                    .nth(4)
                    .is_some_and(|p| p == target.to_str().unwrap())
            });
        if mounted {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "bind worker did not mount egress target within timeout"
        );
        std::thread::sleep(Duration::from_millis(10));
    }

    // 真实 bind 已生效：写入穿透到物理目录，FUSE 侧共享视图可见。
    std::fs::write(target.join("proof"), b"physical-home").unwrap();
    let seen = Backend::lookup(owner.as_ref(), &ctx, root.inode, OsStr::new("proof")).unwrap();
    let handle = Backend::open(owner.as_ref(), &ctx, seen.inode, libc::O_RDONLY).unwrap();
    let mut buf = [0_u8; 13];
    Backend::read(owner.as_ref(), &ctx, handle, 0, &mut buf).unwrap();
    assert_eq!(&buf, b"physical-home");
    Backend::release(owner.as_ref(), &ctx, handle).unwrap();

    // 记录物理 data_dir 路径，供删除断言使用。
    let data_dir = {
        let authority = owner
            .native_home_export_for_current_namespace(OsStr::new("workspace"))
            .unwrap();
        temp.path().join(authority.data_dir().as_path())
    };
    assert!(data_dir.join("proof").exists());

    // 模拟 rm -rf 时序：先删子文件（FUSE unlink 走物理删除），再 rmdir root。
    Backend::unlink(owner.as_ref(), &ctx, root.inode, OsStr::new("proof")).unwrap();
    Backend::rmdir(
        owner.as_ref(),
        &ctx,
        owner.root_inode(),
        OsStr::new("workspace"),
    )
    .unwrap();

    // 1) bind 已 detach，mountinfo 无残留。
    let after = std::fs::read_to_string("/proc/self/mountinfo").unwrap();
    assert!(!after.lines().any(|line| {
        line.split_whitespace()
            .nth(4)
            .is_some_and(|p| p == target.to_str().unwrap())
    }));
    // 2) egress 目标空目录已清理。
    assert!(!target.exists());
    // 3) 物理 data_dir 已删除（bind 不残留覆盖，remove_dir 成功的关键）。
    assert!(!data_dir.exists());
    // 4) FUSE 侧 root 已消失。
    assert!(
        Backend::lookup(
            owner.as_ref(),
            &ctx,
            owner.root_inode(),
            OsStr::new("workspace")
        )
        .is_err()
    );
}

/// VC-D2B + VC-D7 + VC-20：活跃 fd 下 root 删除不被 detach 阻塞
/// （MNT_DETACH 立即分离，不因 busy 失败）；物理目录删除成功；活跃 fd 按
/// Linux unlink 语义继续工作直到关闭；出口 bind 挂载选项含 nosuid + nodev。
#[test]
#[ignore = "requires root Linux mount privileges; run explicitly"]
fn e2e_rmdir_with_open_fd_detaches_without_busy_and_bind_has_nosuid_nodev() {
    use std::io::Read;

    let retained = tempfile::tempdir().unwrap();
    let egress_root = retained.path().join("egress");
    std::fs::create_dir(&egress_root).unwrap();
    let (temp, fs, ctx) = fixture_with_mount_manager(egress_root.clone());
    let owner = Arc::new(fs);
    owner.mount_manager().unwrap().attach_owner(&owner);
    let root = mkdir_root(&owner, &ctx, "d7");
    let target = egress_root.join("d7");
    wait_for_egress_bind(&target);

    // VC-20：出口 bind 的挂载选项必须包含 nosuid + nodev（mountinfo 第 6 列）。
    let options = std::fs::read_to_string("/proc/self/mountinfo")
        .unwrap()
        .lines()
        .find(|line| {
            line.split_whitespace()
                .nth(4)
                .is_some_and(|p| p == target.to_str().unwrap())
        })
        .expect("egress bind present in mountinfo")
        .split_whitespace()
        .nth(5)
        .unwrap()
        .to_owned();
    assert!(
        options.split(',').any(|option| option == "nosuid"),
        "egress bind missing nosuid: {options}"
    );
    assert!(
        options.split(',').any(|option| option == "nodev"),
        "egress bind missing nodev: {options}"
    );

    // VC-D7 前置：通过 bind 目标写文件并保持 fd 打开。
    std::fs::write(target.join("file"), b"held-open").unwrap();
    let mut fd = std::fs::File::open(target.join("file")).unwrap();

    // 物理 data_dir 路径（egress 解耦后 source=物理目录、target=<egress_root>/<name>）。
    let data_dir = {
        let authority = owner
            .native_home_export_for_current_namespace(OsStr::new("d7"))
            .unwrap();
        temp.path().join(authority.data_dir().as_path())
    };
    assert!(data_dir.join("file").exists());

    // rm -rf 时序：先 unlink 子文件（fd 保持打开），再 rmdir root。
    Backend::unlink(owner.as_ref(), &ctx, root.inode, OsStr::new("file")).unwrap();
    Backend::rmdir(owner.as_ref(), &ctx, owner.root_inode(), OsStr::new("d7")).unwrap();

    // VC-D2B：MNT_DETACH 立即分离，不因活跃 fd 报 busy；mountinfo 无残留。
    assert!(!egress_bind_mounted(&target));
    // VC-D7：活跃 fd 按 Linux unlink 语义继续可读，直到关闭。
    let mut buf = [0_u8; 9];
    fd.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, b"held-open");
    // 物理目录已删除（source 非挂载点，remove_dir 不被 fd 阻塞）。
    assert!(!data_dir.exists());
    // 无残留：egress target 与 FUSE 侧 root 均消失。
    assert!(!target.exists());
    assert!(Backend::lookup(owner.as_ref(), &ctx, owner.root_inode(), OsStr::new("d7")).is_err());
}

/// VC-D8：删除后 reconcile 不恢复 —— rmdir 删除 root 后，显式触发
/// reconcile_egress（空活跃集合）不应重新挂载或重建任何条目。
#[test]
#[ignore = "requires root Linux mount privileges; run explicitly"]
fn e2e_reconcile_after_rmdir_does_not_restore_bind_or_metadata() {
    let retained = tempfile::tempdir().unwrap();
    let egress_root = retained.path().join("egress");
    std::fs::create_dir(&egress_root).unwrap();
    let (temp, fs, ctx) = fixture_with_mount_manager(egress_root.clone());
    let owner = Arc::new(fs);
    let manager = owner.mount_manager().unwrap();
    manager.attach_owner(&owner);
    mkdir_root(&owner, &ctx, "d8");
    let target = egress_root.join("d8");
    wait_for_egress_bind(&target);
    let data_dir = {
        let authority = owner
            .native_home_export_for_current_namespace(OsStr::new("d8"))
            .unwrap();
        temp.path().join(authority.data_dir().as_path())
    };
    assert!(data_dir.is_dir());

    Backend::rmdir(owner.as_ref(), &ctx, owner.root_inode(), OsStr::new("d8")).unwrap();
    assert!(!egress_bind_mounted(&target));
    assert!(!data_dir.exists());

    // 删除后 reconcile（空活跃集合）不得恢复已删除 root 的 bind。
    manager
        .reconcile_egress(&std::collections::HashSet::new())
        .unwrap();
    assert!(!egress_bind_mounted(&target));
    assert!(!target.exists());
    assert!(Backend::lookup(owner.as_ref(), &ctx, owner.root_inode(), OsStr::new("d8")).is_err());
}

/// 只读 mmap 区域：持有独立的内核页引用，文件 unlink / 目录删除后映射仍可读
/// （VC-D7 的 mmap 形态，缺口 1）。
struct MmapReadRegion {
    address: *mut libc::c_void,
    length: usize,
}

#[allow(unsafe_code)]
impl MmapReadRegion {
    fn new(file: &std::fs::File, length: usize) -> Self {
        assert!(file.metadata().unwrap().len() >= length as u64);
        // SAFETY: 映射固定只读长度；本对象持有该映射，file 在映射创建期间存活。
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
            "mmap failed: {}",
            std::io::Error::last_os_error()
        );
        Self { address, length }
    }

    fn bytes(&self) -> &[u8] {
        // SAFETY: 映射区域在本对象存活期内有效。
        unsafe { std::slice::from_raw_parts(self.address as *const u8, self.length) }
    }
}

impl Drop for MmapReadRegion {
    fn drop(&mut self) {
        // SAFETY: 释放本对象持有的映射区域。
        unsafe {
            libc::munmap(self.address, self.length);
        }
    }
}

/// 缺口 1（VC-D7 强化）：rmdir root（unlink + 物理目录删除 + MNT_DETACH）后，
/// 仍存活的 mmap 映射按 Linux 语义继续可读，直到 munmap。
#[test]
#[ignore = "requires root Linux mount privileges; run explicitly"]
fn e2e_rmdir_keeps_live_mmap_readable_until_unmap() {
    let retained = tempfile::tempdir().unwrap();
    let egress_root = retained.path().join("egress");
    std::fs::create_dir(&egress_root).unwrap();
    let (_temp, fs, ctx) = fixture_with_mount_manager(egress_root.clone());
    let owner = Arc::new(fs);
    owner.mount_manager().unwrap().attach_owner(&owner);
    let root = mkdir_root(&owner, &ctx, "m1");
    let target = egress_root.join("m1");
    wait_for_egress_bind(&target);

    // 通过 egress bind 视图写文件并建立只读 mmap（保持映射存活）。
    std::fs::write(target.join("mapped.bin"), [b'M'; 4096]).unwrap();
    let held = std::fs::File::open(target.join("mapped.bin")).unwrap();
    let mapping = MmapReadRegion::new(&held, 4096);

    // rm -rf 时序：先 unlink 子文件（映射保持），再 rmdir root。
    Backend::unlink(owner.as_ref(), &ctx, root.inode, OsStr::new("mapped.bin")).unwrap();
    Backend::rmdir(owner.as_ref(), &ctx, owner.root_inode(), OsStr::new("m1")).unwrap();

    // mmap 按 Linux unlink 语义继续可读（映射持有页，与文件目录项无关）。
    assert_eq!(mapping.bytes(), &[b'M'; 4096]);
    // detach 与物理目录删除已生效。
    assert!(!egress_bind_mounted(&target));
    assert!(!target.exists());

    // 释放映射后测试正常结束。
    drop(mapping);
    drop(held);
}

/// 缺口 2：真实 FUSE 会话与 egress bind 并存（D1 解耦，FUSE 路径不被覆盖）；
/// FUSE 视图 rm -rf 触发完整 root 删除（detach bind + 物理目录删除），
/// FUSE 会话随后正常 unmount + join，mountinfo 无残留。
#[test]
#[ignore = "requires root Linux mount privileges, /dev/fuse and a private mount namespace; run explicitly"]
fn e2e_real_fuse_rmdir_triggers_egress_detach_and_clean_session_join() {
    let retained = tempfile::tempdir().unwrap();
    let egress_root = retained.path().join("egress");
    std::fs::create_dir(&egress_root).unwrap();
    let (temp, fs, ctx) = fixture_with_mount_manager(egress_root.clone());
    let owner = Arc::new(fs);
    owner.mount_manager().unwrap().attach_owner(&owner);
    mkdir_root(&owner, &ctx, "real");

    // 真实 FUSE 挂载：挂载点下可见 workspace root；egress bind 在独立路径，
    // FUSE 路径永不被覆盖（D1）。
    let mount = retained.path().join("fuse-mount");
    std::fs::create_dir(&mount).unwrap();
    let mut session =
        N2aFuseSession::new(crate::node::fuse::mount_ownerfs(owner.clone(), &mount).unwrap());
    let target = egress_root.join("real");
    wait_for_egress_bind(&target);
    assert!(std::fs::metadata(mount.join("real")).unwrap().is_dir());
    assert!(std::fs::metadata(target.clone()).unwrap().is_dir());
    assert!(egress_bind_mounted(&target));

    let data_dir = {
        let authority = owner
            .native_home_export_for_current_namespace(OsStr::new("real"))
            .unwrap();
        temp.path().join(authority.data_dir().as_path())
    };

    // 全真 rm -rf：FUSE 视图写文件，egress 共享视图与物理目录读回一致。
    std::fs::write(mount.join("real").join("proof"), b"through-fuse").unwrap();
    assert_eq!(
        std::fs::read(target.join("proof")).unwrap(),
        b"through-fuse"
    );
    assert_eq!(
        std::fs::read(data_dir.join("proof")).unwrap(),
        b"through-fuse"
    );

    // FUSE 视图删除：先删子路径，再 rmdir root（走 FUSE 回调 → rmdir_root
    // → request_unmount detach egress bind → 物理目录删除）。
    std::fs::remove_file(mount.join("real").join("proof")).unwrap();
    std::fs::remove_dir(mount.join("real")).unwrap();

    // egress bind 已 detach、物理目录已删除、FUSE 视图 root 消失。
    assert!(!egress_bind_mounted(&target));
    assert!(!data_dir.exists());
    assert!(std::fs::metadata(mount.join("real")).is_err());

    // FUSE 会话正常 unmount + join，mountinfo 无 FUSE 挂载点残留。
    session.normal_unmount_and_join(&mount);
    let after = std::fs::read_to_string("/proc/self/mountinfo").unwrap();
    assert!(!after.lines().any(|line| {
        line.split_whitespace()
            .nth(4)
            .is_some_and(|p| p == mount.to_str().unwrap())
    }));
    assert!(!target.exists());
}

include!("native_home_fuse_fixture.rs");
