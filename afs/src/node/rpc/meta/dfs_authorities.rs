//! Placement, repair, read-source and external-read authority implementations over GrpcDfsMeta.

use super::*;

#[cfg(feature = "dfs")]
impl crate::node::replication::PlacementProvider for GrpcDfsMeta {
    fn snapshot(&self) -> afs_error::Result<Arc<crate::dfs::PlacementSnapshot>> {
        if let Some(snapshot) = self
            .placement
            .lock()
            .map_err(|_| {
                Error::coded(
                    afs_error::CLIENT_PROTOCOL_VIOLATION,
                    "DFS placement cache lock poisoned",
                )
            })?
            .clone()
        {
            return Ok(snapshot);
        }
        self.refresh(0)
    }

    fn refresh(
        &self,
        minimum_revision: u64,
    ) -> afs_error::Result<Arc<crate::dfs::PlacementSnapshot>> {
        self.read_diagnostic(
            crate::node::read_work::ReadDiagnosticStage::MetaPlacementSnapshot,
            || {
                let reply = self
                    .run(self.client().get_placement_snapshot(
                        afs_protocol::meta::GetDfsPlacementSnapshotRequest {
                            caller_id: self.node_id.clone(),
                            minimum_revision,
                        },
                    ))?
                    .into_inner();
                let snapshot = Arc::new(domain_placement_snapshot(required(
                    reply.snapshot,
                    "GetPlacementSnapshot.snapshot",
                )?)?);
                *self.placement.lock().map_err(|_| {
                    Error::coded(
                        afs_error::CLIENT_PROTOCOL_VIOLATION,
                        "DFS placement cache lock poisoned",
                    )
                })? = Some(snapshot.clone());
                Ok(snapshot)
            },
        )
    }
}

#[cfg(feature = "dfs")]
impl crate::node::replication::ReplicationTaskAuthority for GrpcDfsMeta {
    fn claim(
        &self,
        request: crate::dfs::ClaimReplicationTask,
    ) -> afs_error::Result<Option<crate::dfs::ReplicationClaim>> {
        GrpcDfsMeta::claim_replication_task(self, request)
    }

    fn report(
        &self,
        request: crate::dfs::ReportReplicationTask,
    ) -> afs_error::Result<crate::dfs::ReplicationTask> {
        GrpcDfsMeta::report_replication_task(self, request)
    }

    fn report_corrupt_chunk(
        &self,
        request: crate::dfs::ReportChunkCorruption,
    ) -> afs_error::Result<()> {
        GrpcDfsMeta::report_corrupt_chunk(self, request)
    }
}

#[cfg(feature = "dfs")]
impl crate::node::read::engine::ReadSourceProvider for GrpcDfsMeta {
    fn sources_for_async(
        &self,
        request: crate::dfs::DfsChunkSourcesRequest,
    ) -> crate::node::read::admission::ReadFuture<'_, crate::dfs::DfsChunkSourcesReply> {
        Box::pin(async move {
            self.record_read_rpc("get_chunk_sources");
            let mut client = self.client();
            let reply = tokio::time::timeout(
                self.timeout,
                client.get_chunk_sources(afs_tracing::request_with_current_context(
                    afs_protocol::meta::GetDfsChunkSourcesRequest {
                        caller_id: request.caller_id,
                        namespace_id: request.namespace_id.0,
                        file_version_id: request.file_version_id.0,
                        layout_root_id: request.layout_root_id.0,
                        chunk_ids: request.chunk_ids.into_iter().map(|id| id.0).collect(),
                    },
                )),
            )
            .await
            .map_err(|_| {
                afs_error::Error::coded(
                    afs_error::CLIENT_DEADLINE_EXCEEDED,
                    "async source lookup deadline exceeded",
                )
            })?
            .map_err(afs_transport::grpc::error_status::status_to_error)?
            .into_inner();
            Ok(crate::dfs::DfsChunkSourcesReply {
                revision: reply.revision,
                chunks: reply
                    .chunks
                    .into_iter()
                    .map(domain_dfs_chunk_sources)
                    .collect::<afs_error::Result<Vec<_>>>()?,
            })
        })
    }
    fn sources_for(
        &self,
        request: crate::dfs::DfsChunkSourcesRequest,
    ) -> afs_error::Result<crate::dfs::DfsChunkSourcesReply> {
        self.read_diagnostic(
            crate::node::read_work::ReadDiagnosticStage::MetaGetChunkSources,
            || {
                self.record_read_rpc("get_chunk_sources");
                let reply = self
                    .run(
                        self.client()
                            .get_chunk_sources(afs_tracing::request_with_current_context(
                                afs_protocol::meta::GetDfsChunkSourcesRequest {
                                    caller_id: request.caller_id,
                                    namespace_id: request.namespace_id.0,
                                    file_version_id: request.file_version_id.0,
                                    layout_root_id: request.layout_root_id.0,
                                    chunk_ids: request
                                        .chunk_ids
                                        .into_iter()
                                        .map(|chunk_id| chunk_id.0)
                                        .collect(),
                                },
                            )),
                    )?
                    .into_inner();
                Ok(crate::dfs::DfsChunkSourcesReply {
                    revision: reply.revision,
                    chunks: reply
                        .chunks
                        .into_iter()
                        .map(domain_dfs_chunk_sources)
                        .collect::<afs_error::Result<Vec<_>>>()?,
                })
            },
        )
    }
}

#[cfg(feature = "dfs")]
impl crate::node::read::engine::ExternalReadAuthorizer for GrpcDfsMeta {
    fn authorize_current_external_read(
        &self,
        request: crate::dfs::AuthorizeCurrentExternalRead,
    ) -> afs_error::Result<Option<crate::dfs::AuthorizeCurrentExternalReadReply>> {
        GrpcDfsMeta::authorize_current_external_read(self, request)
    }

    fn authorize_external_version_read(
        &self,
        request: crate::dfs::AuthorizeExternalVersionRead,
    ) -> afs_error::Result<Option<crate::dfs::AuthorizeExternalVersionReadReply>> {
        GrpcDfsMeta::authorize_external_version_read(self, request).map(Some)
    }

    fn validate_external_read_grants(
        &self,
        caller_id: &str,
        validations: Vec<crate::dfs::DfsReadValidation>,
    ) -> afs_error::Result<Vec<crate::dfs::DfsExternalAuthorizedRead>> {
        GrpcDfsMeta::validate_external_read_grants(
            self,
            crate::dfs::ValidateExternalReadGrants {
                caller_id: caller_id.to_owned(),
                validations,
            },
        )
    }
}
