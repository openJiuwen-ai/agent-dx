//! 跨节点 Workspace 出口（outlet）的公共名字面。
//!
//! 每个节点在本地 tmpfs 上维护 `/public/<name>` → 数据面路径的 symlink，
//! 由 Meta 的 Active root 快照驱动收敛。本模块只做本节点内的名字面，
//! 不涉及数据面挂载，也不签发任何授权。
//!
//! 名字面只发布 `/public/<name>`：调用方用创建时的名字绑定，而不是内部的
//! `root-<hex(name)>` 编码。名字与 root id 的互转见
//! `ownerfs::root::{root_id_from_name, root_name_from_id}`。

pub mod imports;
pub mod live;
pub mod mirror;
pub mod mount;
pub mod topology;
