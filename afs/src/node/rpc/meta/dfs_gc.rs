//! The GcControl implementation over GrpcDfsMeta.

use super::*;

#[cfg(feature = "dfs")]
impl crate::node::gc::GcControl for GrpcDfsMeta {
    fn retain(
        &self,
        id: &str,
        inode: &crate::dfs::InodeId,
        version: Option<&crate::dfs::FileVersionId>,
    ) -> Result<()> {
        self.update_gc(id, inode, version, false, false).map(|_| ())
    }
    fn retain_fenced(
        &self,
        id: &str,
        inode: &crate::dfs::InodeId,
        version: Option<&crate::dfs::FileVersionId>,
    ) -> Result<crate::dfs::GcFence> {
        self.update_gc(id, inode, version, true, true)?
            .ok_or_else(|| {
                Error::coded(
                    afs_error::NODE_VFS_UNIMPLEMENTED,
                    "Meta did not return a GC fence",
                )
            })
    }
    fn release(&self, ids: &[String]) -> Result<()> {
        let mut client =
            afs_protocol::meta::dfs_meta_client::DfsMetaClient::new(self.channel.clone());
        self.run(client.release_gc_references(
            afs_protocol::meta::ReleaseDfsGcReferencesRequest {
                node_id: self.node_id.clone(),
                session_id: self.session_id.clone(),
                reference_ids: ids.to_vec(),
            },
        ))?;
        Ok(())
    }
    fn retain_reader(
        &self,
        reader: u64,
        inode: &crate::dfs::InodeId,
        version: Option<&crate::dfs::FileVersionId>,
    ) -> Result<u64> {
        let mut client =
            afs_protocol::meta::dfs_meta_client::DfsMetaClient::new(self.channel.clone());
        Ok(self
            .run(
                client.retain_reader(afs_protocol::meta::RetainDfsReaderRequest {
                    node_id: self.node_id.clone(),
                    session_id: self.session_id.clone(),
                    reader,
                    inode_id: inode.0.clone(),
                    version_id: version.map(|id| id.0.clone()).unwrap_or_default(),
                }),
            )?
            .into_inner()
            .meta_term)
    }
    fn release_readers(&self, readers: &[u64]) -> Result<()> {
        let mut client =
            afs_protocol::meta::dfs_meta_client::DfsMetaClient::new(self.channel.clone());
        self.run(
            client.release_readers(afs_protocol::meta::ReleaseDfsReadersRequest {
                node_id: self.node_id.clone(),
                session_id: self.session_id.clone(),
                readers: readers.to_vec(),
            }),
        )?;
        Ok(())
    }
    fn report_readers(
        &self,
        term: u64,
        low_watermark: u64,
        readers: &[crate::node::gc::SoftReader],
    ) -> Result<u64> {
        let mut client =
            afs_protocol::meta::dfs_meta_client::DfsMetaClient::new(self.channel.clone());
        Ok(self
            .run(
                client.report_readers(afs_protocol::meta::ReportDfsReadersRequest {
                    node_id: self.node_id.clone(),
                    session_id: self.session_id.clone(),
                    meta_term: term,
                    low_watermark,
                    readers: readers
                        .iter()
                        .map(|reader| afs_protocol::meta::DfsReaderEntry {
                            reader: reader.reader,
                            inode_id: reader.inode.0.clone(),
                            version_id: reader
                                .version
                                .as_ref()
                                .map(|id| id.0.clone())
                                .unwrap_or_default(),
                        })
                        .collect(),
                }),
            )?
            .into_inner()
            .meta_term)
    }
    fn collect(
        &self,
        completed: u64,
        inventory: Vec<crate::dfs::ChunkObject>,
    ) -> Result<crate::dfs::GcState> {
        self.collect_batch(completed, inventory)
            .map(|batch| batch.state)
    }
    fn collect_batch(
        &self,
        completed: u64,
        inventory: Vec<crate::dfs::ChunkObject>,
    ) -> Result<crate::node::gc::GcBatch> {
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
        Ok(crate::node::gc::GcBatch {
            list_external_inventory: reply.list_external_inventory,
            state: crate::dfs::GcState {
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
            },
        })
    }
}

#[cfg(feature = "dfs")]
impl GrpcDfsMeta {
    fn update_gc(
        &self,
        id: &str,
        inode: &crate::dfs::InodeId,
        version: Option<&crate::dfs::FileVersionId>,
        publication: bool,
        capture_gc_fence: bool,
    ) -> Result<Option<crate::dfs::GcFence>> {
        let mut client =
            afs_protocol::meta::dfs_meta_client::DfsMetaClient::new(self.channel.clone());
        let reply = self
            .run(
                client.update_gc_reference(afs_protocol::meta::UpdateDfsGcReferenceRequest {
                    capture_gc_fence,
                    node_id: self.node_id.clone(),
                    session_id: self.session_id.clone(),
                    reference_id: id.into(),
                    admission_deadline_unix_ms: crate::clock::now_unix_ms().saturating_add(30_000),
                    inode_id: inode.0.clone(),
                    version_id: version.map(|id| id.0.clone()).unwrap_or_default(),
                    publication,
                    // Per-chunk publication references were removed (R33).
                    chunks: Vec::new(),
                }),
            )?
            .into_inner();
        reply
            .gc_fence
            .map(crate::dfs::GcFence::from_wire)
            .transpose()
    }
}
