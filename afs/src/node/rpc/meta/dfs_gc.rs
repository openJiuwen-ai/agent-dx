//! The GcControl implementation over GrpcDfsMeta.

use super::*;

#[cfg(feature = "dfs")]
impl crate::node::gc::GcControl for GrpcDfsMeta {
    fn retain(
        &self,
        id: &str,
        inode: &crate::dfs::InodeId,
        version: Option<&crate::dfs::FileVersionId>,
        publication: bool,
    ) -> Result<()> {
        self.update_gc(id, Some(inode), version, publication, Vec::new(), false)
    }
    fn retain_fenced(
        &self,
        id: &str,
        inode: &crate::dfs::InodeId,
        version: Option<&crate::dfs::FileVersionId>,
    ) -> Result<crate::dfs::GcFence> {
        self.update_gc_with_fence(id, Some(inode), version, true, Vec::new(), false, true)?
            .ok_or_else(|| {
                Error::coded(
                    afs_error::NODE_VFS_UNIMPLEMENTED,
                    "Meta did not return a GC fence",
                )
            })
    }
    fn release(&self, id: &str) -> Result<()> {
        self.update_gc(id, None, None, false, Vec::new(), true)
    }
    fn reserve_publication(
        &self,
        operation: &crate::dfs::OperationId,
        chunks: Vec<crate::dfs::ChunkObject>,
    ) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            match self.update_gc(
                &format!("publish:{}", operation.0),
                None,
                None,
                true,
                chunks.clone(),
                false,
            ) {
                Err(error)
                    if error.code() == afs_error::META_DFS_GC_BUSY && Instant::now() < deadline =>
                {
                    std::thread::sleep(Duration::from_millis(50))
                }
                result => return result,
            }
        }
    }
    fn collect(
        &self,
        completed: u64,
        inventory: Vec<crate::dfs::ChunkObject>,
    ) -> Result<crate::dfs::GcState> {
        let mut client =
            afs_protocol::meta::dfs_meta_client::DfsMetaClient::new(self.channel.clone());
        let reply = self
            .run(
                client.collect_garbage(afs_protocol::meta::CollectDfsGarbageRequest {
                    node_id: self.node_id.clone(),
                    session_id: self.session_id.clone(),
                    completed_generation: completed,
                    inventory: inventory.into_iter().map(wire_chunk_object).collect(),
                }),
            )?
            .into_inner();
        Ok(crate::dfs::GcState {
            generation: reply.generation,
            chunks: reply
                .chunk_ids
                .into_iter()
                .map(crate::dfs::ChunkId::new)
                .collect(),
            waiting_nodes: if reply.pending_for_node {
                vec![self.node_id.clone()]
            } else {
                Vec::new()
            },
        })
    }
}

#[cfg(feature = "dfs")]
impl GrpcDfsMeta {
    fn update_gc(
        &self,
        id: &str,
        inode: Option<&crate::dfs::InodeId>,
        version: Option<&crate::dfs::FileVersionId>,
        publication: bool,
        chunks: Vec<crate::dfs::ChunkObject>,
        release: bool,
    ) -> Result<()> {
        self.update_gc_with_fence(id, inode, version, publication, chunks, release, false)
            .map(|_| ())
    }

    #[allow(clippy::too_many_arguments)] // Mirrors the existing fixed GC request shape.
    fn update_gc_with_fence(
        &self,
        id: &str,
        inode: Option<&crate::dfs::InodeId>,
        version: Option<&crate::dfs::FileVersionId>,
        publication: bool,
        chunks: Vec<crate::dfs::ChunkObject>,
        release: bool,
        capture_gc_fence: bool,
    ) -> Result<Option<crate::dfs::GcFence>> {
        let mut client =
            afs_protocol::meta::dfs_meta_client::DfsMetaClient::new(self.channel.clone());
        let reply = self
            .run(
                client.update_gc_reference(afs_protocol::meta::UpdateDfsGcReferenceRequest {
                    open_read: Vec::new(),
                    close_open_read: false,

                    capture_gc_fence,
                    node_id: self.node_id.clone(),
                    session_id: self.session_id.clone(),
                    reference_id: id.into(),
                    admission_deadline_unix_ms: if inode.is_some() && !release {
                        crate::meta::store::now_unix_ms().saturating_add(30_000)
                    } else {
                        0
                    },
                    inode_id: inode.map(|id| id.0.clone()).unwrap_or_default(),
                    version_id: version.map(|id| id.0.clone()).unwrap_or_default(),
                    publication,
                    chunks: chunks.into_iter().map(wire_chunk_object).collect(),
                    release,
                }),
            )?
            .into_inner();
        reply
            .gc_fence
            .map(crate::dfs::GcFence::from_wire)
            .transpose()
    }
}
