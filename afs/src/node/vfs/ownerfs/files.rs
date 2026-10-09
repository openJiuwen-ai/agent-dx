//! OwnerFs 文件身份与打开句柄的业务类型。
//!
//! 同一个 OwnerFs 可在 Home 节点持有本地文件，也可在访问节点持有由 Home
//! 签发的远端句柄。FUSE 的 ino/fh 数字属于接入层，不在这里当作文件身份。
//! 本地与远端文件表都使用这些类型；句柄仅在签发它的 Node 会话中有效。

use super::root::{PresentedRootAccess, RootId};
use crate::node::{
    storage::localfs::{LocalDirectory, LocalFile},
    vfs::types::FileAttributes,
};

/// 由 Home 产生的文件身份。路径被删除并同名重建后必须改变此身份。
///
/// 首版避免 unsafe 的 `name_to_handle_at`，把 dev+ino+创建时间+类型
/// 元数据编码进 opaque bytes；它是本进程防陈旧的身份，不承诺跨重启可重新打开旧对象。
/// 远端只保存和原样回传，不能从字节推断磁盘路径或持久位置。
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileIdentity(pub Vec<u8>);

/// lookup 的业务结果。已查询到的身份可用于之后的 open 防陈旧检查。
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OwnerEntry {
    pub root_id: RootId,
    pub identity: FileIdentity,
    pub attributes: FileAttributes,
}

/// OwnerFs 打开文件的两种落点；两者都在打开期间保持原文件身份。
/// 本地分支持有实际 OS 文件；远端分支持有 Home 签发的会话内令牌。
#[derive(Debug)]
pub enum OpenFile {
    Local(LocalOpenFile),
    Remote(RemoteFile),
}

/// Home 上的打开文件与其身份一起保存。rename/unlink 后仍通过已打开
/// 的 OS 文件句柄操作旧对象，不按路径重新查找。
#[derive(Debug)]
pub struct LocalOpenFile {
    pub root_id: RootId,
    #[cfg(test)]
    pub(super) private_binding: Option<Box<super::root::PrivateRootBinding>>,
    pub identity: FileIdentity,
    pub file: LocalFile,
    /// None for a Home-local open. A peer open is bound to the authenticated
    /// node and the exact grant presented at OPEN, not to the request fields
    /// supplied on later READ/WRITE/RELEASE calls.
    pub peer: Option<PeerOpenScope>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PeerOpenScope {
    pub node_id: String,
    pub access: PresentedRootAccess,
}

/// 远端句柄只能用于签发它的 Home 进程会话。Home 重启后必须重新 open；
/// B 不能将旧令牌解释为新进程的文件，也不能退化为按旧路径打开。
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemoteFile {
    pub root_id: RootId,
    pub owner_node_id: String,
    pub owner_session_id: String,
    pub identity: FileIdentity,
    pub handle: Vec<u8>,
}

/// 目录游标与普通文件句柄分开，防止把文件 `fsync`/`release` 错用于目录。
#[derive(Debug)]
pub enum OpenDirectory {
    Local(LocalOpenDirectory),
    Remote(RemoteDirectory),
}

#[derive(Debug)]
pub struct LocalOpenDirectory {
    pub root_id: RootId,
    #[cfg(test)]
    pub(super) private_binding: Option<Box<super::root::PrivateRootBinding>>,
    pub identity: FileIdentity,
    pub directory: LocalDirectory,
    pub peer: Option<PeerOpenScope>,
}

/// B 只保存 A 给出的目录游标；其有效期绑定 A 的进程会话。
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemoteDirectory {
    pub root_id: RootId,
    pub owner_node_id: String,
    pub owner_session_id: String,
    pub identity: FileIdentity,
    pub handle: Vec<u8>,
}
