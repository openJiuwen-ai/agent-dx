use super::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DfsNodeLocation {
    pub node_id: String,
    pub node_epoch: u64,
    pub data_endpoint: String,
}

/// Round 2 B2: what one `OpenFileRead` call answers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OpenedForRead {
    /// `None`: the checked name no longer names the inode.
    pub inode: Option<InodeRecord>,
    /// Meta registered the inode's head as the reader the call named.
    pub registered: bool,
    pub meta_term: u64,
    /// Another session holds a live write lease on the inode (D-R3-2 ③).
    pub writer_elsewhere: bool,
}

pub trait DfsRemoteOwnerFactory: Send + Sync {
    fn connect(&self, location: DfsNodeLocation) -> Result<Arc<dyn RemoteDfsOwner>>;
}

/// Calls run under the client's configured timeout. A caller with its own
/// budget wraps a call in `crate::node::call_deadline`, which may only shorten
/// it; implementations that block read that scope on the calling thread.
pub trait DfsMeta: Send + Sync {
    fn lookup(&self, parent: &InodeId, name: &[u8]) -> Result<Option<InodeRecord>>;
    /// `first_generation`: admit this write generation's `write:` reference in
    /// the same transaction (round 2 B1).
    fn create(
        &self,
        operation_id: &OperationId,
        parent: &InodeId,
        name: &[u8],
        attributes: InodeAttributes,
        first_generation: Option<&OperationId>,
        caller: CallerContext,
    ) -> Result<(InodeRecord, WriteLease)>;
    /// `create` with the opened handle registered as in-memory reader
    /// `reader` (round 2 C-1). The third value is the Meta writer term when
    /// Meta registered it; `None`: the caller pins the handle itself. The
    /// default registers nothing.
    #[allow(clippy::too_many_arguments)]
    fn create_registered(
        &self,
        operation_id: &OperationId,
        parent: &InodeId,
        name: &[u8],
        attributes: InodeAttributes,
        first_generation: Option<&OperationId>,
        _reader: Option<u64>,
        caller: CallerContext,
    ) -> Result<(InodeRecord, WriteLease, Option<u64>)> {
        self.create(
            operation_id,
            parent,
            name,
            attributes,
            first_generation,
            caller,
        )
        .map(|(inode, lease)| (inode, lease, None))
    }
    fn get_inode(&self, inode_id: &InodeId) -> Result<InodeRecord>;
    /// Round 2 B2: resolve `inode_id` for a read open (checking that
    /// `name` still names it), register reader `reader` of its head and
    /// learn whether another session writes it, in one call. The default is
    /// today's resolution with nothing registered.
    fn open_file_read(
        &self,
        _reader: u64,
        inode_id: &InodeId,
        name: Option<(&InodeId, &[u8])>,
    ) -> Result<OpenedForRead> {
        let inode = match name {
            Some((parent, name)) => self
                .lookup(parent, name)?
                .filter(|record| record.inode_id == *inode_id),
            None => Some(self.get_inode(inode_id)?),
        };
        Ok(OpenedForRead {
            inode,
            registered: false,
            meta_term: 0,
            writer_elsewhere: false,
        })
    }
    fn get_file_version(&self, version_id: &FileVersionId) -> Result<(FileVersion, LayoutRoot)>;
    fn get_file_version_async(
        &self,
        version_id: FileVersionId,
    ) -> crate::node::read::admission::ReadFuture<'_, (FileVersion, LayoutRoot)> {
        Box::pin(async move { self.get_file_version(&version_id) })
    }

    fn open_write(&self, inode_id: &InodeId) -> Result<(InodeRecord, WriteLease)>;
    fn resolve_lock_authority(&self, inode_id: &InodeId) -> Result<(InodeRecord, WriteLease)> {
        self.open_write(inode_id)
    }
    fn resolve_write_authority(&self, inode_id: &InodeId) -> Result<(InodeRecord, WriteLease)> {
        self.open_write(inode_id)
    }
    /// Observe an existing owner without any lease mutation.
    fn observe_write_authority(&self, _inode_id: &InodeId) -> Result<(InodeRecord, WriteLease)> {
        Err(Error::coded(
            afs_error::NODE_VFS_UNAVAILABLE,
            "read-only write authority resolution is unavailable",
        ))
    }
    /// Callable on an async worker. The default leaves the worker for the sync
    /// method; a network client overrides it with its native future.
    fn resolve_write_authority_async(
        &self,
        inode_id: InodeId,
    ) -> crate::node::read::admission::ReadFuture<'_, (InodeRecord, WriteLease)> {
        Box::pin(async move {
            crate::node::bridge::run_sync_backend(|| self.resolve_write_authority(&inode_id))
        })
    }
    fn renew_write_lease(&self, lease: WriteLease) -> Result<WriteLease>;
    fn sync_inode_metadata(&self, sync: SyncInodeMetadata) -> Result<InodeRecord>;
    fn commit_file_version(&self, commit: CommitFileVersion) -> Result<InodeRecord>;
    fn lookup_node_location(&self, node_id: &str) -> Result<Option<DfsNodeLocation>>;
    fn current_node_session(&self, node_id: &str) -> Result<Option<String>>;
    /// See [`DfsMeta::resolve_write_authority_async`].
    fn current_node_session_async(
        &self,
        node_id: String,
    ) -> crate::node::read::admission::ReadFuture<'_, Option<String>> {
        Box::pin(async move {
            crate::node::bridge::run_sync_backend(|| self.current_node_session(&node_id))
        })
    }
    fn mkdir(
        &self,
        operation_id: &OperationId,
        parent: &InodeId,
        name: &[u8],
        attributes: InodeAttributes,
        caller: CallerContext,
    ) -> Result<InodeRecord>;
    /// One page of `parent` in name order, strictly after `after` (empty starts
    /// at the beginning); Meta caps `limit`.
    fn read_dir_page(&self, parent: &InodeId, after: &[u8], limit: u32) -> Result<DirPage>;
    fn link(
        &self,
        operation_id: &OperationId,
        inode_id: &InodeId,
        expected_inode_revision: u64,
        parent: &InodeId,
        name: &[u8],
        caller: CallerContext,
    ) -> Result<InodeRecord>;
    fn symlink(&self, request: SymlinkRequest) -> Result<InodeRecord>;
    fn mknod(&self, request: MknodRequest) -> Result<InodeRecord>;
    fn read_link(&self, request: ReadLinkRequest) -> Result<Vec<u8>>;
    fn unlink(
        &self,
        operation_id: &OperationId,
        parent: &InodeId,
        name: &[u8],
        caller: CallerContext,
    ) -> Result<InodeRecord>;
    fn rmdir(
        &self,
        operation_id: &OperationId,
        parent: &InodeId,
        name: &[u8],
        caller: CallerContext,
    ) -> Result<InodeRecord>;
    #[allow(clippy::too_many_arguments)] // Mirrors the authenticated Meta rename contract.
    fn rename(
        &self,
        operation_id: &OperationId,
        old_parent: &InodeId,
        old_name: &[u8],
        new_parent: &InodeId,
        new_name: &[u8],
        mode: RenameMode,
        caller: CallerContext,
    ) -> Result<RenameOutcome>;

    fn set_inode_attributes(&self, request: SetInodeAttrRequest) -> Result<InodeRecord>;
    fn get_xattr(&self, request: GetXattrRequest) -> Result<Vec<u8>>;
    fn list_xattr(&self, request: ListXattrRequest) -> Result<Vec<Vec<u8>>>;
    fn restore_file_chunk(
        &self,
        _request: crate::dfs::RestoreFileChunk,
        _timeout: Duration,
    ) -> Result<()> {
        Err(Error::coded(
            afs_error::IO_NOT_SUPPORTED,
            "Meta does not support source restoration",
        ))
    }
    fn bind_lazy_source(&self, _request: crate::dfs::BindLazySourceRequest) -> Result<InodeRecord> {
        Err(Error::coded(
            afs_error::IO_NOT_SUPPORTED,
            "Meta does not support lazy source binding",
        ))
    }
    fn set_xattr(&self, request: SetXattrRequest) -> Result<InodeRecord>;
    fn remove_xattr(&self, request: RemoveXattrRequest) -> Result<InodeRecord>;
}
