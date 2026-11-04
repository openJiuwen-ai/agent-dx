use super::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DfsNodeLocation {
    pub node_id: String,
    pub node_epoch: u64,
    pub data_endpoint: String,
}

pub trait DfsRemoteOwnerFactory: Send + Sync {
    fn connect(&self, location: DfsNodeLocation) -> Result<Arc<dyn RemoteDfsOwner>>;
}

pub trait DfsMeta: Send + Sync {
    fn lookup(&self, parent: &InodeId, name: &[u8]) -> Result<Option<InodeRecord>>;
    fn create(
        &self,
        operation_id: &OperationId,
        parent: &InodeId,
        name: &[u8],
        attributes: InodeAttributes,
    ) -> Result<(InodeRecord, WriteLease)>;
    fn get_inode(&self, inode_id: &InodeId) -> Result<InodeRecord>;
    fn get_inode_async(
        &self,
        inode_id: InodeId,
    ) -> crate::node::read_pipeline::ReadFuture<'_, InodeRecord> {
        Box::pin(async move { self.get_inode(&inode_id) })
    }

    fn get_file_version(&self, version_id: &FileVersionId) -> Result<(FileVersion, LayoutRoot)>;
    fn get_file_version_async(
        &self,
        version_id: FileVersionId,
    ) -> crate::node::read_pipeline::ReadFuture<'_, (FileVersion, LayoutRoot)> {
        Box::pin(async move { self.get_file_version(&version_id) })
    }

    fn open_write(&self, inode_id: &InodeId) -> Result<(InodeRecord, WriteLease)>;
    fn resolve_lock_authority(&self, inode_id: &InodeId) -> Result<(InodeRecord, WriteLease)> {
        self.open_write(inode_id)
    }
    fn resolve_write_authority(&self, inode_id: &InodeId) -> Result<(InodeRecord, WriteLease)> {
        self.open_write(inode_id)
    }
    fn renew_write_lease(&self, lease: WriteLease) -> Result<WriteLease>;
    fn renew_write_lease_with_timeout(
        &self,
        lease: WriteLease,
        _timeout: Duration,
    ) -> Result<WriteLease> {
        self.renew_write_lease(lease)
    }
    fn sync_inode_metadata(&self, sync: SyncInodeMetadata) -> Result<InodeRecord>;
    fn sync_inode_metadata_with_timeout(
        &self,
        sync: SyncInodeMetadata,
        _timeout: Duration,
    ) -> Result<InodeRecord> {
        self.sync_inode_metadata(sync)
    }
    fn commit_file_version(&self, commit: CommitFileVersion) -> Result<InodeRecord>;
    fn commit_file_version_with_timeout(
        &self,
        commit: CommitFileVersion,
        _timeout: Duration,
    ) -> Result<InodeRecord> {
        self.commit_file_version(commit)
    }
    fn lookup_node_location(&self, node_id: &str) -> Result<Option<DfsNodeLocation>>;
    fn current_node_session(&self, node_id: &str) -> Result<Option<String>>;
    fn current_node_session_with_timeout(
        &self,
        node_id: &str,
        _timeout: Duration,
    ) -> Result<Option<String>> {
        self.current_node_session(node_id)
    }

    fn mkdir(
        &self,
        operation_id: &OperationId,
        parent: &InodeId,
        name: &[u8],
        attributes: InodeAttributes,
        caller: CallerContext,
    ) -> Result<InodeRecord>;
    fn read_dir(&self, parent: &InodeId) -> Result<Vec<DentryRecord>>;
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
    fn set_xattr(&self, request: SetXattrRequest) -> Result<InodeRecord>;
    fn remove_xattr(&self, request: RemoveXattrRequest) -> Result<InodeRecord>;
}
