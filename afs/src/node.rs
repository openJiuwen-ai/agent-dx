//! afs-node：近计算部署的单一节点进程。
//!
//! FUSE、SDK/REST、节点间 RPC 接入同一个 Node，内容经 P2P 直达数据节点。
//! 不另建 Home 进程，不保留 NFS 后端。OwnerFs/DFS 共用 Backend 与 FUSE 实现，
//! 但使用独立 mount、session、数据表示、缓存、一致性与恢复语义。
//! 阻塞 I/O/设备等待不可占住异步执行线程；调度和局部保护归各业务模块。

//!
//! 阅读启动顺序：`assembly`（`run_node` 的编排与各段装配）→ `registration`（Meta 注册与心跳）
//! → `readiness`（就绪观测）→ `background`（有名的后台任务）→ `assembly::shutdown`（关闭顺序）。
//! gRPC 的控制与数据 service 共用 TCP listener；SDK 使用另一条本机 UDS listener。
//! OwnerFs 与 DFS 都通过 FUSE→Backend 进入各自的真实文件路径；
//! REST diagnostics/SDK→Storage 仍是独立诊断链，不属于任何文件系统的数据面。

pub mod api;
mod assembly;
mod background;
pub(crate) mod bridge;
#[cfg(feature = "dfs")]
pub(crate) mod call_deadline;
pub mod chunk;
pub(crate) mod chunk_crc32c;
pub mod connectors;
pub mod diagnostics;
#[cfg(feature = "dfs")]
pub mod external_chunk;
pub(crate) mod fault;
pub mod fuse;
#[cfg(feature = "dfs")]
pub mod gc;
pub mod metrics;
#[cfg(feature = "ownerfs")]
pub mod outlet;
pub mod read;
mod readiness;
mod registration;
#[cfg(feature = "dfs")]
pub mod replication;
pub mod rpc;
pub mod storage;
pub mod vfs;

pub use readiness::{AfsMountIdentity, NodeReadiness, ReadinessObservation, sample_rdma_sysfs_at};
pub(crate) use readiness::{current_visible_afs_mount_id_in_mountinfo, parse_exact_mountinfo_line};

use crate::{
    config::Config,
    runtime::{BoxError, Observability},
};
use std::sync::Arc;

/// REST 持有的进程级共享对象，不是另一个 Home 服务进程。
/// OwnerFs 和 DFS 分别绑定自己的 FUSE session；诊断 Storage 与 RDMA 会话表独立。
pub struct Node {
    pub config: Config,
    pub observability: Observability,
    /// 每次进程启动生成的新会话，旧远端句柄不能跨此边界复用。
    pub session_id: String,
    pub readiness: Arc<NodeReadiness>,
    #[cfg(feature = "dfs")]
    pub read_work_metrics: metrics::work::ReadWorkMetrics,
    #[cfg(feature = "ownerfs")]
    pub ownerfs: Option<Arc<vfs::ownerfs::OwnerFs>>,
    #[cfg(feature = "dfs")]
    pub dfs: Option<Arc<vfs::dfs::DistributedFs>>,
}

/// 组装并持有 Node 的所有入口。启动失败清理已经建立的资源，正常退出卸载本进程挂载。
pub async fn run(cfg: Config, obs: Observability) -> Result<(), BoxError> {
    assembly::run_node(cfg, obs, || {}).await
}

pub async fn run_with_shutdown(
    cfg: Config,
    obs: Observability,
    shutdown: crate::runtime::ShutdownTrigger,
) -> Result<(), BoxError> {
    assembly::run_node(cfg, obs, move || shutdown.arm()).await
}

#[cfg(all(test, feature = "dfs"))]
mod shutdown_order_tests;
