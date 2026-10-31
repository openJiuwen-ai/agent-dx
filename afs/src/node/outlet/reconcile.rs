//! Outlet 启动序列与对账（spec §12）。
//!
//! 启动：校验 public_root 已挂载且为 tmpfs（fail-fast，不自行挂载——
//! 表现层 tmpfs 归部署方），确保 `.pub/`、`.quarantine/` 私有目录存在。
//! 对账 diff1（表现层）：meta Active 集 − 本地 symlink 集 = 补发布；
//! 本地 symlink 集 − Active 集 = 撤除；farm 中非 symlink 实体（ghost
//! 目录/文件）移入隔离区保留证据（spec §13 防线 2/3）。两个 diff 均幂等。

use std::{
    collections::BTreeMap,
    ffi::{OsStr, OsString},
    fs::DirBuilder,
    os::unix::fs::DirBuilderExt,
    path::Path,
};

use afs_error::{Error, Result};

use super::publish::{DesiredRoot, FarmState, OutletPublisher, QUARANTINE_DIR, STAGING_DIR};
use crate::node::rpc::meta::OutletActiveRoot;
use crate::node::{mountinfo_path_bytes, parse_exact_mountinfo_line};

/// 启动序列（spec §12 afs-node 重启行）：public_root 必须是部署方已挂载的
/// tmpfs，缺失/类型不符即节点启动失败（AFS 永不自行挂载）。
pub(crate) fn prepare_public_root(public_root: &Path) -> Result<()> {
    let mountinfo = std::fs::read("/proc/self/mountinfo").map_err(Error::from)?;
    if !public_root_is_mounted_tmpfs(&mountinfo, public_root) {
        return Err(Error::coded(
            afs_error::CONFIG_INVALID,
            format!(
                "outlet public_root {} must be a mounted tmpfs provided by deployment; AFS never mounts it",
                public_root.display()
            ),
        ));
    }
    for name in [STAGING_DIR, QUARANTINE_DIR] {
        ensure_private_dir(public_root, name)?;
    }
    Ok(())
}

/// mountinfo 中该精确路径的最顶层挂载是否为 tmpfs（纯函数，可测）。
fn public_root_is_mounted_tmpfs(mountinfo: &[u8], public_root: &Path) -> bool {
    let encoded = mountinfo_path_bytes(public_root);
    mountinfo
        .split(|byte| *byte == b'\n')
        .filter_map(|line| parse_exact_mountinfo_line(line, &encoded))
        .next_back()
        .is_some_and(|entry| entry.fs_type == b"tmpfs")
}

/// 确保表现层下的私有目录存在且为目录（0700；已存在则不改动权限）。
fn ensure_private_dir(public_root: &Path, name: &str) -> Result<()> {
    let path = public_root.join(name);
    match std::fs::symlink_metadata(&path) {
        Ok(meta) if meta.is_dir() => Ok(()),
        Ok(_) => Err(Error::coded(
            afs_error::CONFIG_INVALID,
            format!("outlet {} exists but is not a directory", path.display()),
        )),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => DirBuilder::new()
            .mode(0o700)
            .create(&path)
            .map_err(Error::from),
        Err(error) => Err(Error::from(error)),
    }
}

/// 对账 diff1（spec §12/D15）：以 meta Active 快照为期望态，扫描 farm 重建
/// 发布状态。返回 publisher worker 的初始 `FarmState`（已亮出集 = farm 中
/// 匹配 Active 名字的 symlink）。幂等：同一输入产生同一输出。
pub(crate) fn reconcile_farm(
    publisher: &OutletPublisher,
    active: &[OutletActiveRoot],
) -> Result<FarmState> {
    publisher.sweep_staging()?;
    let mut state = FarmState::default();
    let mut active_by_name: BTreeMap<&OsStr, &OutletActiveRoot> = BTreeMap::new();
    for root in active {
        // workspace 名全局唯一由 meta ReserveRoot CAS 保证（spec §8）；此处
        // 覆盖仅作"若发生则可见"的护栏（n-3）。
        if let Some(previous) = active_by_name.insert(&root.name, root) {
            afs_logging::warn!(
                "outlet.reconcile.duplicate_active_name";
                "workspace" => ?root.name,
                "replaced" => previous.root_id.clone(),
                "kept" => root.root_id.clone()
            );
        }
        state.roots.insert(
            root.root_id.clone(),
            DesiredRoot {
                name: root.name.clone(),
                home: root.home_node_id.clone(),
                epoch: root.epoch,
            },
        );
    }
    let public_root = publisher.public_root();
    let entries = std::fs::read_dir(public_root).map_err(Error::from)?;
    let mut skipped = 0usize;
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                skipped += 1;
                afs_logging::warn!(
                    "outlet.reconcile.entry_skipped";
                    "error" => error.to_string()
                );
                continue;
            }
        };
        let name: OsString = entry.file_name();
        if name == OsStr::new(STAGING_DIR) || name == OsStr::new(QUARANTINE_DIR) {
            continue;
        }
        let path = public_root.join(&name);
        let metadata = match std::fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) => {
                skipped += 1;
                afs_logging::warn!(
                    "outlet.reconcile.entry_skipped";
                    "workspace" => ?name, "error" => error.to_string()
                );
                continue;
            }
        };
        if metadata.file_type().is_symlink() {
            match active_by_name.get(name.as_os_str()) {
                Some(root) => {
                    // 名字命中后必须校验指向：删除事件丢失窗口内同名重建
                    // 且 home 变化时，旧代 symlink 文本不匹配新期望 target，
                    // 直接认领会把消费方 bind 到错误数据树（spec §10 静默
                    // 错连防线）。指向不符或无法计算期望 → 摘除并留在未
                    // 发布集，收敛循环按正常门控重发。
                    match publisher.expected_target(&root.name, &root.home_node_id, root.epoch) {
                        Ok(expected) => match std::fs::read_link(&path) {
                            Ok(actual) if actual == expected => {
                                // 文本一致仍不足以证明可用：本地 home 的
                                // target 可能已被删除（悬空 symlink 永不自
                                // 愈），需追加存在性复核。
                                if local_target_present(publisher, root, &expected) {
                                    state.published.insert(root.root_id.clone());
                                } else if let Err(error) = publisher.unpublish(&name) {
                                    afs_logging::warn!(
                                        "outlet.reconcile.unpublish_failed";
                                        "workspace" => ?name, "error" => error.to_string()
                                    );
                                }
                            }
                            Ok(_) => {
                                if let Err(error) = publisher.unpublish(&name) {
                                    afs_logging::warn!(
                                        "outlet.reconcile.unpublish_failed";
                                        "workspace" => ?name, "error" => error.to_string()
                                    );
                                }
                            }
                            Err(error) => {
                                skipped += 1;
                                afs_logging::warn!(
                                    "outlet.reconcile.entry_skipped";
                                    "workspace" => ?name, "error" => error.to_string()
                                );
                            }
                        },
                        // 期望 target 算不出来 = 该 home 的传输布局不可知，
                        // 与"指向不符"同姿态：摘除并留在未发布集，由收敛
                        // 按正常门控重发。
                        Err(error) => {
                            afs_logging::warn!(
                                "outlet.reconcile.expected_target_failed";
                                "workspace" => ?root.name, "home" => root.home_node_id.clone(),
                                "error" => error.to_string()
                            );
                            if let Err(error) = publisher.unpublish(&name) {
                                afs_logging::warn!(
                                    "outlet.reconcile.unpublish_failed";
                                    "workspace" => ?name, "error" => error.to_string()
                                );
                            }
                        }
                    }
                }
                // 不在 Active 集（含悬空者）：撤除（D15 diff1）。
                None => {
                    if let Err(error) = publisher.unpublish(&name) {
                        afs_logging::warn!(
                            "outlet.reconcile.unpublish_failed";
                            "workspace" => ?name, "error" => error.to_string()
                        );
                    }
                }
            }
        } else {
            // ghost 目录/文件（消费方违约或 A1 物理残留）→ 隔离保留证据。
            publisher.quarantine(&path);
        }
    }
    if skipped > 0 {
        afs_logging::warn!(
            "outlet.reconcile.degraded";
            "skipped" => skipped,
            "public_root" => ?public_root
        );
    }
    Ok(state)
}

/// 本地 home 的 target 存在性复核（M-2）。
///
/// 远端 home 一律返回 true（不查）：target 落在 NFS import 挂载点上，hard
/// mount 下对端失联时 `stat` **不返回错误而是不返回**，会挂死 60s 对账
/// 线程。只有 `NotFound` 判为不可用；其它 IO 错误是瞬时抖动，保留入口。
fn local_target_present(
    publisher: &OutletPublisher,
    root: &OutletActiveRoot,
    target: &Path,
) -> bool {
    if !publisher.is_local_home(&root.home_node_id) {
        return true;
    }
    match std::fs::symlink_metadata(target) {
        Ok(_) => true,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
        Err(error) => {
            afs_logging::warn!(
                "outlet.reconcile.target_stat_failed";
                "workspace" => ?root.name, "target" => ?target,
                "error" => error.to_string()
            );
            true
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::outlet::{
        topology::TopologyState,
        transport::{NodeId, TransportBackend, TransportPathState},
    };
    use std::path::PathBuf;
    use std::{os::unix::fs::MetadataExt, sync::Arc};
    use tokio::sync::watch;

    struct FakeBackend;

    impl TransportBackend for FakeBackend {
        fn path_state(&self, _node: &NodeId) -> TransportPathState {
            TransportPathState::Missing
        }

        fn healthy(&self, _node: &NodeId) -> bool {
            false
        }

        fn resolve_target(&self, name: &OsStr, home: &NodeId, epoch: u64) -> Result<PathBuf> {
            if name == OsStr::new("bad-name") {
                return Err(Error::from(std::io::Error::other("test: unresolvable")));
            }
            Ok(PathBuf::from("/imports")
                .join(home)
                .join(format!("{}-e{epoch}", name.to_string_lossy())))
        }
    }

    fn publisher(root: &tempfile::TempDir) -> OutletPublisher {
        let public = root.path().join("public");
        std::fs::create_dir(&public).unwrap();
        std::fs::create_dir(public.join(STAGING_DIR)).unwrap();
        std::fs::create_dir(public.join(QUARANTINE_DIR)).unwrap();
        let (_topology_tx, topology_rx) = watch::channel(TopologyState::default());
        OutletPublisher::new(
            public,
            root.path().join("users"),
            Arc::new(FakeBackend),
            topology_rx,
            "A".to_owned(),
        )
    }

    fn active(root_id: &str, name: &str, home: &str) -> OutletActiveRoot {
        active_epoch(root_id, name, home, 1)
    }

    fn active_epoch(root_id: &str, name: &str, home: &str, epoch: u64) -> OutletActiveRoot {
        OutletActiveRoot {
            root_id: root_id.to_owned(),
            name: name.into(),
            home_node_id: home.to_owned(),
            epoch,
        }
    }

    #[test]
    fn public_root_is_mounted_tmpfs_matches_topmost_exact_mount() {
        let mountinfo = b"41 30 0:40 / /users rw,relatime - tmpfs tmpfs rw\n\
42 30 0:41 / /run/afs/imports/B/users rw,relatime - nfs4 10.0.0.2:/users rw\n";
        assert!(public_root_is_mounted_tmpfs(mountinfo, Path::new("/users")));
        assert!(!public_root_is_mounted_tmpfs(
            mountinfo,
            Path::new("/run/afs/imports/B/users")
        ));
        assert!(!public_root_is_mounted_tmpfs(
            mountinfo,
            Path::new("/not-mounted")
        ));
        // 堆叠挂载取顶层：tmpfs 之上叠了 ext4 → 判非 tmpfs。
        let stacked = b"41 30 0:40 / /users rw,relatime - tmpfs tmpfs rw\n\
42 30 0:41 / /users rw,relatime - ext4 /dev/sda1 rw\n";
        assert!(!public_root_is_mounted_tmpfs(stacked, Path::new("/users")));
    }

    #[test]
    fn ensure_private_dir_creates_0700_and_is_idempotent() {
        let root = tempfile::tempdir().unwrap();
        ensure_private_dir(root.path(), STAGING_DIR).unwrap();
        let mode = std::fs::symlink_metadata(root.path().join(STAGING_DIR))
            .unwrap()
            .mode();
        assert_eq!(mode & 0o777, 0o700);
        // 已存在（含非默认权限）：幂等不改。
        ensure_private_dir(root.path(), STAGING_DIR).unwrap();
        // 已存在但非目录：拒绝。
        std::fs::write(root.path().join("occupied"), b"x").unwrap();
        assert!(ensure_private_dir(root.path(), "occupied").is_err());
    }

    #[test]
    fn reconcile_fam_applies_diff1_and_quarantines_foreign_entries() {
        let root = tempfile::tempdir().unwrap();
        let publisher = publisher(&root);
        let public = publisher.public_root();
        let expected_keep = publisher
            .expected_target(OsStr::new("keep-a"), &"A".to_owned(), 1)
            .unwrap();
        // M-2：本地 home 的 target 必须真实存在，否则同文本也会被摘除。
        std::fs::create_dir_all(&expected_keep).unwrap();

        // farm 现状：Active 且指向正确的 symlink、多余（不在 Active）的
        // symlink、ghost 目录、.pub 残留各一。
        std::os::unix::fs::symlink(&expected_keep, public.join("keep-a")).unwrap();
        std::os::unix::fs::symlink("/users/stale-b", public.join("stale-b")).unwrap();
        std::fs::create_dir(public.join("ghost-c")).unwrap();
        std::os::unix::fs::symlink("/users/keep-a", public.join(STAGING_DIR).join("residue"))
            .unwrap();

        let state = reconcile_farm(
            &publisher,
            &[
                active("root-keep", "keep-a", "A"),
                active("root-pending", "pending-d", "A"),
            ],
        )
        .unwrap();

        // Active − farm = 补发布（收敛循环重试，此处验证期望态）。
        assert!(state.roots.contains_key("root-keep"));
        assert!(state.roots.contains_key("root-pending"));
        // farm − Active = 撤除。
        assert!(std::fs::symlink_metadata(public.join("stale-b")).is_err());
        // 名字命中且指向正确 → 记为已亮出。
        assert_eq!(state.published, ["root-keep".to_owned()].into());
        // ghost 目录进隔离区。
        let quarantined: Vec<_> = std::fs::read_dir(public.join(QUARANTINE_DIR))
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        assert_eq!(quarantined.len(), 1);
        assert!(quarantined[0].to_string_lossy().starts_with("ghost-c-"));
        // .pub 残留被清扫。
        assert_eq!(
            std::fs::read_dir(public.join(STAGING_DIR)).unwrap().count(),
            0
        );
        // 指向正确的 Active symlink 原样保留。
        assert_eq!(
            std::fs::read_link(public.join("keep-a")).unwrap(),
            expected_keep
        );
    }

    /// 删除事件丢失窗口内同名重建且 home 变化：旧代 symlink 的名字命中
    /// Active 集但指向不符，必须摘除（否则消费方 bind 到旧 home 的数据树）。
    #[test]
    fn reconcile_fam_removes_symlink_with_stale_target() {
        let root = tempfile::tempdir().unwrap();
        let publisher = publisher(&root);
        let public = publisher.public_root();
        // Active：ws-a home = 远端 B；farm 残留：ws-a 指向旧 home A。
        std::os::unix::fs::symlink(
            publisher
                .expected_target(OsStr::new("ws-a"), &"A".to_owned(), 1)
                .unwrap(),
            public.join("ws-a"),
        )
        .unwrap();

        let state = reconcile_farm(&publisher, &[active("root-ws", "ws-a", "B")]).unwrap();

        // 指向不符：不认领、摘除，留给收敛循环按拓扑门控重发。
        assert!(state.published.is_empty());
        assert!(std::fs::symlink_metadata(public.join("ws-a")).is_err());
    }

    /// M-6：同名下一代（epoch 变化、home 不变）同样必须判指向不符并摘除。
    /// 删除事件丢失窗口内同名重建时，旧代 symlink 会让消费方 bind 到
    /// `-e<旧代>` 的数据树，因此不能只看 home 与名字是否一致。
    #[test]
    fn reconcile_fam_removes_symlink_pointing_at_previous_epoch() {
        let root = tempfile::tempdir().unwrap();
        let publisher = publisher(&root);
        let public = publisher.public_root();
        let previous = publisher
            .expected_target(OsStr::new("ws-a"), &"B".to_owned(), 1)
            .unwrap();
        let current = publisher
            .expected_target(OsStr::new("ws-a"), &"B".to_owned(), 2)
            .unwrap();
        // 代次进入 target 文本，这是摘除判据成立的前提。
        assert_ne!(previous, current);
        std::os::unix::fs::symlink(&previous, public.join("ws-a")).unwrap();

        let state = reconcile_farm(&publisher, &[active_epoch("root-ws", "ws-a", "B", 2)]).unwrap();

        assert!(state.published.is_empty());
        assert!(std::fs::symlink_metadata(public.join("ws-a")).is_err());
    }

    #[test]
    fn reconcile_farm_on_empty_farm_is_clean() {
        let root = tempfile::tempdir().unwrap();
        let publisher = publisher(&root);
        let state = reconcile_farm(&publisher, &[]).unwrap();
        assert!(state.roots.is_empty());
        assert!(state.published.is_empty());
    }

    /// M-2：本地 home 且 target 文本相等，但物理不存在 → 判悬空并摘除。
    #[test]
    fn reconcile_fam_removes_dangling_local_target() {
        let root = tempfile::tempdir().unwrap();
        let publisher = publisher(&root);
        let public = publisher.public_root();
        let target = publisher
            .expected_target(OsStr::new("local-a"), &"A".to_owned(), 1)
            .unwrap();
        std::os::unix::fs::symlink(&target, public.join("local-a")).unwrap();

        let state = reconcile_farm(&publisher, &[active("root-local", "local-a", "A")]).unwrap();

        assert!(state.published.is_empty());
        assert!(std::fs::symlink_metadata(public.join("local-a")).is_err());
    }

    /// M-2：远端 home 一律不查存在性（NFS hard mount 会挂死对账线程）→ 保留。
    #[test]
    fn reconcile_fam_keeps_remote_target_without_stat() {
        let root = tempfile::tempdir().unwrap();
        let publisher = publisher(&root);
        let public = publisher.public_root();
        let target = publisher
            .expected_target(OsStr::new("remote-a"), &"B".to_owned(), 1)
            .unwrap();
        std::os::unix::fs::symlink(&target, public.join("remote-a")).unwrap();

        let state = reconcile_farm(&publisher, &[active("root-remote", "remote-a", "B")]).unwrap();

        assert_eq!(state.published, ["root-remote".to_owned()].into());
        assert_eq!(std::fs::read_link(public.join("remote-a")).unwrap(), target);
    }

    /// M-10：单条目无法计算期望 target 时降级（摘除 + 继续），整轮仍返回 Ok。
    #[test]
    fn reconcile_fam_skips_unresolvable_entry_and_keeps_round_ok() {
        let root = tempfile::tempdir().unwrap();
        let publisher = publisher(&root);
        let public = publisher.public_root();
        let good = publisher
            .expected_target(OsStr::new("good-a"), &"A".to_owned(), 1)
            .unwrap();
        std::fs::create_dir_all(&good).unwrap();
        std::os::unix::fs::symlink("/x", public.join("bad-name")).unwrap();
        std::os::unix::fs::symlink(&good, public.join("good-a")).unwrap();

        let state = reconcile_farm(
            &publisher,
            &[
                active("root-bad", "bad-name", "B"),
                active("root-good", "good-a", "A"),
            ],
        )
        .unwrap();

        assert!(std::fs::symlink_metadata(public.join("bad-name")).is_err());
        assert_eq!(state.published, ["root-good".to_owned()].into());
    }

    /// M-10：`public_root` 不可读属结构性失败，整轮返回 Err（启动路径致命）。
    #[test]
    fn reconcile_fam_fails_when_public_root_unreadable() {
        let root = tempfile::tempdir().unwrap();
        let publisher = publisher(&root);
        std::fs::remove_dir_all(publisher.public_root()).unwrap();
        assert!(reconcile_farm(&publisher, &[]).is_err());
    }
}
