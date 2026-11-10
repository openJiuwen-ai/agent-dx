//! 段 13b: outlet 名字层与 OwnerFs 目录回收的后台任务。
//!
//! 两者都在 `Services` 起跑前完成启动门禁：门禁失败必须让进程直接退出，而
//! 不是带着一个永远发布不出名字的镜像循环继续服务。

use super::ownerfs::OwnerFsAssembly;
use crate::{
    config::Config,
    node::outlet::{
        live::{self, ImportExecutor, MirrorConfig},
        mount::{self, KernelMounter},
    },
    node::vfs::ownerfs::root::{RootManager, RootMeta},
    runtime::{BoxError, Services, cancelled},
};
use std::{sync::Arc, time::Duration};

/// Cross-node outlet name layer.
///
/// Runs before the reclaim sweep so a startup gate failure aborts before any
/// background task is spawned.
pub(super) fn start_mirror(
    cfg: &Config,
    ownerfs: &OwnerFsAssembly,
    services: &mut Services,
) -> Result<(), BoxError> {
    let (Some(outlet), Some(control)) = (cfg.outlet.as_ref(), ownerfs.outlet_control.as_ref())
    else {
        return Ok(());
    };
    let mountinfo = std::fs::read("/proc/self/mountinfo").map_err(|error| {
        afs_error::Error::coded(
            afs_error::CONFIG_INVALID,
            format!("outlet requires /proc/self/mountinfo: {error}"),
        )
    })?;
    live::ensure_public_root_is_tmpfs(&outlet.public_root, &mountinfo)?;
    live::prepare(&outlet.public_root)?;
    let config = MirrorConfig {
        public_root: outlet.public_root.clone(),
        import_root: outlet.import_root.to_string_lossy().into_owned(),
        interval: outlet.mirror_interval(),
    };
    // 动态导入的启动门禁。R1/R7 是硬前提：不满足就拒绝启动，而不是运行期
    // 反复失败却无人受益（设计第九节）。
    if !mount::shares_host_namespace(&mountinfo) {
        return Err(Box::new(afs_error::Error::coded(
            afs_error::CONFIG_INVALID,
            "outlet requires the host mount namespace; \
             PrivateMounts must be disabled for the service",
        )));
    }
    // 父目录必须可写（AFS 自己建 <home>/users），但 public_root 自己由
    // 部署方建。挂载前用一个临时点试探权限。
    if let Err(error) = mount::probe_mount_permission(&outlet.import_root) {
        return Err(Box::new(afs_error::Error::coded(
            afs_error::CONFIG_INVALID,
            format!(
                "outlet requires mount(2) privilege at {}: {error}",
                outlet.import_root.display()
            ),
        )));
    }
    afs_logging::info!(
        "outlet.import.enabled";
        "users_root" => outlet.users_root.display().to_string(),
        "mount_options" => outlet.mount_options.clone()
    );
    let import_executor = ImportExecutor::new(
        outlet.import_root.clone(),
        outlet.users_root.clone(),
        outlet.mount_options.clone(),
        cfg.id.clone(),
        Arc::new(KernelMounter),
    );
    afs_logging::info!(
        "outlet.mirror.starting";
        "public_root" => outlet.public_root.display().to_string(),
        "import_root" => outlet.import_root.display().to_string(),
        "interval_ms" => outlet.mirror_interval_ms
    );
    let meta: Arc<dyn RootMeta> = control.meta.clone();
    let stop = services.stop.subscribe();
    let mirror_config = config.clone();
    services.spawn(async move {
        let shutdown = cancelled(stop);
        tokio::pin!(shutdown);
        live::run(mirror_config, meta, shutdown, import_executor).await;
        Ok(())
    });
    // Read-only topology diagnosis. A separate task so a slow mount scan can
    // never delay publication (spec H3).
    let stop = services.stop.subscribe();
    services.spawn(async move {
        let shutdown = cancelled(stop);
        tokio::pin!(shutdown);
        live::run_diagnosis(config, shutdown).await;
        Ok(())
    });
    Ok(())
}

/// Periodic sweep of data directories left behind by a root deletion whose
/// physical removal was deferred (`RootManager::reclaim_deferred`).
///
/// The first tick is skipped: startup reconciliation already ran, and an
/// instant sweep would race the first mount requests.
pub(super) fn start_reclaim_sweep(
    cfg: &Config,
    ownerfs: &OwnerFsAssembly,
    services: &mut Services,
) {
    let Some(control) = ownerfs.outlet_control.as_ref() else {
        return;
    };
    let roots = control.roots.clone();
    let stop = services.stop.subscribe();
    let period = Duration::from_millis(cfg.ownerfs_reclaim_interval_ms);
    services.spawn(async move {
        let mut tick = tokio::time::interval(period);
        tick.tick().await;
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let shutdown = cancelled(stop);
        tokio::pin!(shutdown);
        loop {
            tokio::select! {
                _ = &mut shutdown => return Ok(()),
                _ = tick.tick() => {
                    let roots = roots.clone();
                    // remove_dir is a blocking syscall; keep it off the async
                    // workers.
                    match tokio::task::spawn_blocking(move || roots.reclaim_deferred()).await {
                        Ok(Ok((removed, pending))) if removed > 0 || pending > 0 => {
                            afs_logging::info!(
                                "ownerfs.reclaim.swept";
                                "removed" => removed,
                                "pending" => pending
                            );
                        }
                        Ok(Ok(_)) => {}
                        Ok(Err(error)) => {
                            afs_logging::warn!(
                                "ownerfs.reclaim.failed";
                                "error" => error.to_string()
                            );
                        }
                        Err(error) => {
                            afs_logging::warn!(
                                "ownerfs.reclaim.worker_failed";
                                "error" => error.to_string()
                            );
                        }
                    }
                }
            }
        }
    });
}

/// The OwnerFs handles the two starters need. Taken from the OwnerFs construction
/// site because `OwnerFs` itself does not expose them.
pub(super) struct OutletControl {
    pub(super) meta: Arc<dyn RootMeta>,
    pub(super) roots: Arc<RootManager>,
}
