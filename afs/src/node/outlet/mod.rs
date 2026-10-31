//! AFS Outlet 预挂载网格（spec：afs/outlet-premounted-spec-v1.md）的节点侧组件。
//!
//! - `transport`：机制层接口与 NFS 后端（§7.1，传输知识唯一居所）；
//! - `topology`：集群拓扑对齐（§7.2，策略层）；
//! - `publish`：表现层单写者发布 worker（§9）；
//! - `reconcile`：启动序列与对账 diff（§12）。
//!
//! 策略层只依赖 `transport::TransportBackend`，零具体传输知识。

pub mod topology;
pub mod transport;

pub(crate) mod publish;
pub(crate) mod reconcile;
