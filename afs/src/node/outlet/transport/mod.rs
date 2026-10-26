//! Outlet 传输机制层接口（spec §7.1）。
//!
//! [`TransportBackend`] 是机制层唯一边界：发布 worker、探测契约、对账等
//! 策略层只依赖本 trait，零具体传输知识；NFS 的全部知识封闭在
//! `transport/nfs.rs`。检验标准：删掉 NFS 后端文件后，outlet 其余代码除
//! 后端注册行外零编译失败（换掉传输后策略层逻辑仍成立的才属于策略层）。

pub mod nfs;

use std::{ffi::OsStr, path::PathBuf};

use afs_error::Result;

/// 集群成员标识。v1 即 Meta 注册的 node_id 字符串。
pub type NodeId = String;

/// 单条数据通路的实然状态（spec §7/A5）：只读校验部署结果，缺失/漂移仅
/// 报告，不补挂、不修复（检测归 AFS，修复归部署方）。
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TransportPathState {
    /// 通路存在且与部署声明一致。
    Present,
    /// 期望的挂载不存在。
    Missing,
    /// 挂载点存在但属性与声明不符（如文件系统类型漂移），或无法完成校验。
    Drift { reason: String },
}

/// 单个对端通路的健康快照。通路存在 ≠ 通路健康（spec §7.1）；
/// `healthy` 仅在 `Present` 下被探测。
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PathHealth {
    pub state: TransportPathState,
    pub healthy: bool,
}

impl PathHealth {
    /// 表现层发布判据：该通路可承载 symlink target。
    #[must_use]
    pub fn usable(&self) -> bool {
        matches!(self.state, TransportPathState::Present) && self.healthy
    }
}

/// 机制层接口：一种共享数据传输如何呈现为本节点的数据通路。
///
/// 实现约束（spec §7/A5）：后端只做只读校验与探活，缺失/漂移只报告，
/// 不补挂、不修复；挂载建立与 server 侧导出全部归部署方。
pub trait TransportBackend: Send + Sync {
    /// 单个对端通路的实然状态：解析本地挂载表校验部署结果。
    fn path_state(&self, node: &NodeId) -> TransportPathState;

    /// 数据面探活：通路背后的远端是否可响应。仅在 `Present` 下被调用。
    /// 可能无限阻塞（hard mount 下对端失联时等待恢复而非报错），调用方
    /// 必须在每成员独立的探针线程中执行——阻塞只困住该成员自己的线程
    /// （spec §12：挂死 = 在跑容器由 kernel 重试无缝续上；报错 = 停发）。
    fn healthy(&self, node: &NodeId) -> bool;

    /// 发布时问路：远端 home 的 workspace `<name>` 在本节点的数据路径
    /// （表现层 symlink target），路径布局由后端决定。调用方保证 `home`
    /// 为远端 live 成员且通路可用；本地 home 的 target 由发布层直接取
    /// 物理层 users_root，不经本接口。
    fn resolve_target(&self, name: &OsStr, home: &NodeId) -> Result<PathBuf>;
}
