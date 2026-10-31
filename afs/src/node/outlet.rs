//! 跨节点 Workspace 出口（outlet）的公共名字面。
//!
//! 每个节点在本地 tmpfs 上维护 `/public/<name>` → 数据面路径的 symlink，
//! 由 Meta 的 Active root 快照驱动收敛。本模块只做本节点内的名字面，
//! 不涉及数据面挂载，也不签发任何授权。
//!
//! 设计要点见 `afs/outlet-cross-node-spec-v2.md`；本文只记录实现契约。

pub mod mirror;
