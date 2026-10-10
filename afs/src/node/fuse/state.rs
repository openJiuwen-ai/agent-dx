//! FUSE session-local inode and handle tables.
//!
//! One mounted session is bound to one backend. Kernel-facing inode and handle
//! numbers remain separate from backend-local identities.

use std::collections::HashMap;

use crate::node::vfs::types::{BackendInode, DirectoryHandle, Entry, FileHandle};

pub const ROOT_INO: u64 = 1;
const FIRST_BACKEND_INO: u64 = 2;
const FIRST_HANDLE: u64 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FuseNode {
    Root(BackendInode),
    Backend(BackendInode),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FuseFileHandle {
    pub handle: FileHandle,
    /// This handle was opened read-only on its local Home. Its callbacks can
    /// run on the FUSE receive thread without blocking on a remote RPC.
    pub inline_local_read: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FuseDirectoryHandle {
    pub handle: DirectoryHandle,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FuseHandle {
    File(FuseFileHandle),
    Directory(FuseDirectoryHandle),
}

#[derive(Debug)]
struct NodeRecord {
    node: FuseNode,
    lookup_count: u64,
}

#[derive(Debug)]
pub struct FuseState {
    root_inode: BackendInode,
    next_ino: u64,
    next_handle: u64,
    nodes: HashMap<u64, NodeRecord>,
    backend_to_fuse: HashMap<BackendInode, u64>,
    handles: HashMap<u64, FuseHandle>,
}

impl FuseState {
    pub fn new(root_inode: BackendInode) -> Self {
        Self {
            root_inode,
            next_ino: FIRST_BACKEND_INO,
            next_handle: FIRST_HANDLE,
            nodes: HashMap::from([(
                ROOT_INO,
                NodeRecord {
                    node: FuseNode::Root(root_inode),
                    lookup_count: u64::MAX,
                },
            )]),
            backend_to_fuse: HashMap::from([(root_inode, ROOT_INO)]),
            handles: HashMap::new(),
        }
    }

    pub fn node(&self, ino: u64) -> Option<FuseNode> {
        self.nodes.get(&ino).map(|record| record.node)
    }

    pub fn backend_inode(&self, ino: u64) -> Option<BackendInode> {
        match self.node(ino)? {
            FuseNode::Root(inode) | FuseNode::Backend(inode) => Some(inode),
        }
    }

    pub fn remember_lookup(&mut self, entry: &Entry) -> u64 {
        let ino = self.remember_backend_inode(entry.inode);
        if ino != ROOT_INO
            && let Some(record) = self.nodes.get_mut(&ino)
        {
            record.lookup_count = record.lookup_count.saturating_add(1);
        }
        ino
    }

    pub fn remember_readdir_entry(&mut self, entry: &Entry) -> u64 {
        self.remember_backend_inode(entry.inode)
    }

    pub fn forget(&mut self, ino: u64, nlookup: u64) {
        if ino == ROOT_INO {
            return;
        }
        let Some(record) = self.nodes.get_mut(&ino) else {
            return;
        };
        record.lookup_count = record.lookup_count.saturating_sub(nlookup);
        if record.lookup_count == 0 {
            if let FuseNode::Backend(inode) = record.node {
                self.backend_to_fuse.remove(&inode);
            }
            self.nodes.remove(&ino);
        }
    }

    pub fn insert_file_handle(&mut self, handle: FileHandle) -> u64 {
        self.insert_file_handle_with_policy(handle, false)
    }

    pub fn insert_file_handle_with_policy(
        &mut self,
        handle: FileHandle,
        inline_local_read: bool,
    ) -> u64 {
        self.insert_handle(FuseHandle::File(FuseFileHandle {
            handle,
            inline_local_read,
        }))
    }

    pub fn insert_directory_handle(&mut self, handle: DirectoryHandle) -> u64 {
        self.insert_handle(FuseHandle::Directory(FuseDirectoryHandle { handle }))
    }

    pub fn file_handle(&self, fh: u64) -> Option<FuseFileHandle> {
        match self.handles.get(&fh).copied()? {
            FuseHandle::File(handle) => Some(handle),
            FuseHandle::Directory(_) => None,
        }
    }

    pub fn directory_handle(&self, fh: u64) -> Option<FuseDirectoryHandle> {
        match self.handles.get(&fh).copied()? {
            FuseHandle::Directory(handle) => Some(handle),
            FuseHandle::File(_) => None,
        }
    }

    pub fn remove_file_handle(&mut self, fh: u64) -> Option<FuseFileHandle> {
        match self.handles.remove(&fh)? {
            FuseHandle::File(handle) => Some(handle),
            FuseHandle::Directory(handle) => {
                self.handles.insert(fh, FuseHandle::Directory(handle));
                None
            }
        }
    }

    pub fn remove_directory_handle(&mut self, fh: u64) -> Option<FuseDirectoryHandle> {
        match self.handles.remove(&fh)? {
            FuseHandle::Directory(handle) => Some(handle),
            FuseHandle::File(handle) => {
                self.handles.insert(fh, FuseHandle::File(handle));
                None
            }
        }
    }

    fn remember_backend_inode(&mut self, inode: BackendInode) -> u64 {
        if inode == self.root_inode {
            return ROOT_INO;
        }
        if let Some(ino) = self.backend_to_fuse.get(&inode).copied() {
            return ino;
        }
        let ino = self.next_ino;
        self.next_ino = self.next_ino.saturating_add(1);
        self.backend_to_fuse.insert(inode, ino);
        self.nodes.insert(
            ino,
            NodeRecord {
                node: FuseNode::Backend(inode),
                lookup_count: 0,
            },
        );
        ino
    }

    fn insert_handle(&mut self, handle: FuseHandle) -> u64 {
        let fh = self.next_handle;
        self.next_handle = self.next_handle.saturating_add(1);
        self.handles.insert(fh, handle);
        fh
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::vfs::types::{FileAttributes, FileKind};
    use std::time::UNIX_EPOCH;

    fn entry(value: u64) -> Entry {
        Entry {
            inode: BackendInode { value },
            attributes: FileAttributes {
                kind: FileKind::Regular,
                size: 0,
                blocks: 0,
                mode: 0o644,
                uid: 0,
                gid: 0,
                nlink: 1,
                atime: UNIX_EPOCH,
                mtime: UNIX_EPOCH,
                ctime: UNIX_EPOCH,
            },
        }
    }

    #[test]
    fn forget_drops_backend_inode_after_lookup_refs_are_released() {
        let mut state = FuseState::new(BackendInode { value: 1 });
        let entry = entry(42);
        let ino = state.remember_lookup(&entry);
        assert_eq!(state.backend_inode(ino), Some(entry.inode));
        state.forget(ino, 1);
        assert_eq!(state.backend_inode(ino), None);
    }

    #[test]
    fn forget_never_drops_backend_root() {
        let root = BackendInode { value: 1 };
        let mut state = FuseState::new(root);
        state.forget(ROOT_INO, u64::MAX);
        assert_eq!(state.node(ROOT_INO), Some(FuseNode::Root(root)));
    }

    #[test]
    fn handles_survive_inode_lookup_forget() {
        let mut state = FuseState::new(BackendInode { value: 1 });
        let entry = entry(99);
        let ino = state.remember_lookup(&entry);
        let fh = state.insert_file_handle(FileHandle(7));
        state.forget(ino, 1);
        assert_eq!(state.backend_inode(ino), None);
        assert_eq!(
            state.remove_file_handle(fh),
            Some(FuseFileHandle {
                handle: FileHandle(7),
                inline_local_read: false,
            })
        );
    }
}
