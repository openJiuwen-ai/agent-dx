//! NFS 传输后端（spec §7.1/A5）：NFS 知识的唯一居所。
//!
//! 姿态恒为"声明约束 + 运行时只读校验"：解析 `/proc/self/mountinfo` 校验
//! 全网格 import 挂载（`<import_root>/<node>/users`），探活走挂载点元数据
//! 往返。缺失/漂移只报告，不补挂、不修复；nfsd/exportfs/挂载建立全部归
//! 部署方，server 侧静态整树 export，运行期零 NFS 写操作。

use std::{
    ffi::OsStr,
    os::unix::ffi::OsStrExt,
    path::{Path, PathBuf},
};

use afs_error::{Error, Result};

use super::{NodeId, TransportBackend, TransportPathState};
use crate::node::{
    mountinfo_path_bytes, parse_exact_mountinfo_line, vfs::ownerfs::root::root_dir_name,
};

/// NFS import 后端。路径布局：对端 `<node>` 的 users 树挂载在
/// `<import_root>/<node>/users`；workspace 的 symlink target 为该挂载点下
/// 的同名子目录（spec §6 三层目录）。
pub struct NfsBackend {
    import_root: PathBuf,
}

impl NfsBackend {
    #[must_use]
    pub fn new(import_root: PathBuf) -> Self {
        Self { import_root }
    }

    /// `<import_root>/<node>/users`：对端 users 树的 import 挂载点。
    fn import_mount_point(&self, node: &NodeId) -> PathBuf {
        self.import_root.join(node).join("users")
    }

    fn path_state_in(&self, node: &NodeId, mountinfo: &[u8]) -> TransportPathState {
        let mount_point = self.import_mount_point(node);
        match topmost_fs_type_at(mountinfo, &mount_point) {
            Some(fs_type) if fs_type.starts_with(b"nfs") => TransportPathState::Present,
            Some(fs_type) => TransportPathState::Drift {
                reason: format!(
                    "mount at {} uses filesystem type {}, expected nfs",
                    mount_point.display(),
                    String::from_utf8_lossy(fs_type)
                ),
            },
            None => TransportPathState::Missing,
        }
    }
}

impl TransportBackend for NfsBackend {
    fn path_state(&self, node: &NodeId) -> TransportPathState {
        let mountinfo = match std::fs::read("/proc/self/mountinfo") {
            Ok(mountinfo) => mountinfo,
            Err(error) => {
                return TransportPathState::Drift {
                    reason: format!("failed to read /proc/self/mountinfo: {error}"),
                };
            }
        };
        self.path_state_in(node, &mountinfo)
    }

    fn healthy(&self, node: &NodeId) -> bool {
        // 沿挂载点做一次元数据往返；hard mount 下对端失联会阻塞至恢复，
        // 由调用方（低频探测循环 + 阻塞线程）承受。
        std::fs::metadata(self.import_mount_point(node)).is_ok()
    }

    fn resolve_target(&self, name: &OsStr, home: &NodeId, epoch: u64) -> Result<PathBuf> {
        if !is_single_normal_component(name) {
            return Err(Error::coded(
                afs_error::CLIENT_ARGUMENT_INVALID,
                "workspace name must be a single path component",
            ));
        }
        // 目录名与 Home 侧物理布局必须同源：直接复用 ownerfs 的命名函数，
        // 避免两处各写一遍 `<name>-e<epoch>` 后漂移。
        Ok(self
            .import_mount_point(home)
            .join(root_dir_name(name, epoch)?))
    }
}

/// 挂载表中该精确路径的最顶层（最后一条）挂载的文件系统类型。
fn topmost_fs_type_at<'a>(mountinfo: &'a [u8], path: &Path) -> Option<&'a [u8]> {
    let encoded = mountinfo_path_bytes(path);
    mountinfo
        .split(|byte| *byte == b'\n')
        .filter_map(|line| parse_exact_mountinfo_line(line, &encoded))
        .next_back()
        .map(|entry| entry.fs_type)
}

/// 边界校验：名字是单个普通路径分量（非空、无分隔符、非 `.`/`..`）。
/// meta-first 已保证到达下游的 workspace 名合法，这里是防穿越的兜底。
fn is_single_normal_component(name: &OsStr) -> bool {
    let bytes = name.as_bytes();
    !bytes.is_empty()
        && !bytes.contains(&b'/')
        && name != OsStr::new(".")
        && name != OsStr::new("..")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn backend() -> NfsBackend {
        NfsBackend::new(PathBuf::from("/run/afs/imports"))
    }

    const MOUNTINFO: &[u8] =
        b"41 30 0:33 / /run/afs/imports/B/users rw,relatime - nfs4 10.0.0.2:/users rw\n\
42 30 0:34 / /run/afs/imports/C/users rw,relatime - nfs 10.0.0.3:/users rw\n\
43 30 0:35 / /mnt/other rw,relatime - ext4 /dev/sda1 rw\n";

    #[test]
    fn nfs_mount_reports_present() {
        let backend = backend();
        assert_eq!(
            backend.path_state_in(&"B".to_owned(), MOUNTINFO),
            TransportPathState::Present
        );
        assert_eq!(
            backend.path_state_in(&"C".to_owned(), MOUNTINFO),
            TransportPathState::Present
        );
    }

    #[test]
    fn absent_mount_reports_missing() {
        assert_eq!(
            backend().path_state_in(&"D".to_owned(), MOUNTINFO),
            TransportPathState::Missing
        );
    }

    #[test]
    fn foreign_filesystem_reports_drift() {
        let mountinfo = b"44 30 0:36 / /run/afs/imports/E/users rw,relatime - ext4 /dev/sdb1 rw\n";
        assert_eq!(
            backend().path_state_in(&"E".to_owned(), mountinfo),
            TransportPathState::Drift {
                reason: "mount at /run/afs/imports/E/users uses filesystem type ext4, expected nfs"
                    .to_owned()
            }
        );
    }

    #[test]
    fn topmost_stacked_mount_wins() {
        // 同路径堆叠挂载：后一条（更顶层）是判定依据。
        let stacked_nfs =
            b"43 30 0:35 / /run/afs/imports/E/users rw,relatime - ext4 /dev/sdb1 rw\n\
44 30 0:36 / /run/afs/imports/E/users rw,relatime - nfs4 10.0.0.5:/users rw\n";
        assert_eq!(
            backend().path_state_in(&"E".to_owned(), stacked_nfs),
            TransportPathState::Present
        );

        let stacked_ext4 =
            b"43 30 0:36 / /run/afs/imports/E/users rw,relatime - nfs4 10.0.0.5:/users rw\n\
44 30 0:35 / /run/afs/imports/E/users rw,relatime - ext4 /dev/sdb1 rw\n";
        assert!(matches!(
            backend().path_state_in(&"E".to_owned(), stacked_ext4),
            TransportPathState::Drift { .. }
        ));
    }

    #[test]
    fn escaped_mount_point_matches() {
        // 挂载点含空格时 mountinfo 以八进制转义记录；成员名须按同一规则编码。
        let mountinfo =
            b"45 30 0:37 / /run/afs/imports/B\\0401/users rw,relatime - nfs4 10.0.0.6:/users rw\n";
        assert_eq!(
            backend().path_state_in(&"B 1".to_owned(), mountinfo),
            TransportPathState::Present
        );
    }

    #[test]
    fn path_state_reads_proc_mountinfo() {
        // 本机不存在对应 import 挂载时必须如实报告 Missing（不补挂）。
        assert_eq!(
            backend().path_state_in(&"no-such-node".to_owned(), &[]),
            TransportPathState::Missing
        );
    }

    #[test]
    fn resolve_target_composes_import_layout_with_epoch() {
        let target = backend()
            .resolve_target(OsStr::new("agent-a1"), &"B".to_owned(), 7)
            .unwrap();
        assert_eq!(
            target,
            PathBuf::from("/run/afs/imports/B/users/agent-a1-e7")
        );
        // 同一名字的两代必须解析出不同文本，否则旧链接会跨代认领。
        let first = backend()
            .resolve_target(OsStr::new("agent-a1"), &"B".to_owned(), 1)
            .unwrap();
        assert_ne!(first, target);
    }

    #[test]
    fn resolve_target_rejects_traversal() {
        for name in ["a/b", "..", ".", ""] {
            assert!(
                backend()
                    .resolve_target(OsStr::new(name), &"B".to_owned(), 1)
                    .is_err()
            );
        }
    }
}
