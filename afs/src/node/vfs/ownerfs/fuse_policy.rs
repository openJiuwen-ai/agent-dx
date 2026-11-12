//! OwnerFs 接入公共 FUSE 层的挂载策略。
//!
//! 独用 Home 根可短暂缓存（TTL 1s、允许页缓存），共享根与原生 Home 不缓存；
//! 本机 Home 的 inode 在 FUSE 接收线程内联处理；首次远端访问通过 notifier
//! 失效本机缓存。OwnerFs 不使用 DFS 的写队列、读流水线、fd 桥与内核页缓存
//! 特性，因此只校验公共执行选项。

use std::{path::Path, sync::Arc, time::Duration};

use afs_error::Result;
use fuser::{FileHandle as FuseFileHandle, Notifier, ReplyOpen};

use super::OwnerFs;
use crate::node::fuse::{
    FuseExecOptions, FuseMountOptions, FuseRequestMetrics, MountedFuse, mount_with_policy,
    open_reply_flags,
    policy::{FuseMountFeatures, FuseMountPolicy},
};
use crate::node::vfs::types::BackendInode;

pub(crate) struct OwnerFsFusePolicy(pub(crate) Arc<OwnerFs>);

impl FuseMountPolicy for OwnerFsFusePolicy {
    fn label(&self) -> &'static str {
        "ownerfs"
    }

    fn remember_inode(&self, ino: u64) {
        self.0.remember_fuse_inode(ino);
    }

    fn inline_local(&self, inode: BackendInode) -> bool {
        self.0.is_local_inode(inode)
    }

    fn with_cache_policy(&self, inode: BackendInode, reply: &mut dyn FnMut(Duration, bool)) {
        self.0.with_fuse_cache_policy(inode, reply);
    }

    fn reply_opened(&self, inode: BackendInode, fh: u64, reply: ReplyOpen) -> Option<ReplyOpen> {
        self.0.with_fuse_cache_policy(inode, |_, private| {
            reply.opened(FuseFileHandle(fh), open_reply_flags(!private));
        });
        None
    }

    fn on_mounted(&self, notifier: Notifier) {
        self.0.register_fuse_notifier(notifier);
    }
}

/// OwnerFs consumes only the shared execution settings; DFS-only settings
/// are neither validated nor applied.
fn ownerfs_mount_plan(options: FuseMountOptions) -> Result<(FuseExecOptions, FuseMountFeatures)> {
    Ok((options.exec.validate()?, FuseMountFeatures::default()))
}

pub fn mount_ownerfs(ownerfs: Arc<OwnerFs>, path: &Path) -> Result<MountedFuse> {
    mount_ownerfs_inner(ownerfs, path, None, FuseMountOptions::default())
}

pub fn mount_ownerfs_with_metrics(
    ownerfs: Arc<OwnerFs>,
    path: &Path,
    metrics: FuseRequestMetrics,
) -> Result<MountedFuse> {
    mount_ownerfs_with_metrics_and_options(ownerfs, path, metrics, FuseMountOptions::default())
}

pub fn mount_ownerfs_with_metrics_and_options(
    ownerfs: Arc<OwnerFs>,
    path: &Path,
    metrics: FuseRequestMetrics,
    options: FuseMountOptions,
) -> Result<MountedFuse> {
    mount_ownerfs_inner(ownerfs, path, Some(metrics), options)
}

fn mount_ownerfs_inner(
    ownerfs: Arc<OwnerFs>,
    path: &Path,
    metrics: Option<FuseRequestMetrics>,
    options: FuseMountOptions,
) -> Result<MountedFuse> {
    let (exec, features) = ownerfs_mount_plan(options)?;
    mount_with_policy(
        ownerfs.clone(),
        path,
        Arc::new(OwnerFsFusePolicy(ownerfs)),
        features,
        exec,
        metrics,
        #[cfg(any(feature = "dfs", test))]
        None,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ownerfs_mount_ignores_invalid_dfs_only_options() {
        let mut options = FuseMountOptions::default();
        options.dfs.write_queue_bytes = 0;
        options.dfs.read_pipeline.max_rpcs = 0;
        let (exec, features) = ownerfs_mount_plan(options).unwrap();
        assert_eq!(exec.fuse_workers, options.exec.fuse_workers);
        assert!(features.write_queue_bytes.is_none());
        assert!(features.read_pipeline.is_none());
        assert!(features.fd_bridge.is_none());
        assert!(!features.kernel_cache);
        // The same options still fail a DFS mount, and invalid shared
        // execution settings still fail an OwnerFs mount.
        assert!(FuseMountFeatures::dfs(options.dfs, None).is_err());
        options.exec.fuse_workers = 0;
        assert!(ownerfs_mount_plan(options).is_err());
    }
}
