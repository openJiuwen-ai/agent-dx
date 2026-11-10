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
            crate::node::metrics::read_timing::ReadDiagnosticStage::MetaPlacementSnapshot,
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
    fn publish_read_cache(
        &self,
        request: crate::dfs::PublishReadCache,
        deadline: std::time::Instant,
    ) -> crate::node::read::admission::ReadFuture<'_, crate::dfs::CopyRecord> {
        Box::pin(async move {
            let expected = crate::dfs::read_cache_copy(&request.receipt)?;
            let mut client = self.client();
            let reply = tokio::time::timeout_at(
                tokio::time::Instant::from_std(deadline),
                client.publish_read_cache(afs_tracing::request_with_current_context(
                    afs_protocol::meta::PublishDfsReadCacheRequest {
                        caller_id: request.caller_id,
                        caller_session_id: request.caller_session_id,
                        caller_node_epoch: request.caller_node_epoch,
                        operation_id: request.operation_id.0,
                        namespace_id: request.namespace_id.0,
                        file_version_id: request.file_version_id.0,
                        layout_root_id: request.layout_root_id.0,
                        receipt: Some(wire_replica_ack(request.receipt)),
                    },
                )),
            )
            .await
            .map_err(|_| {
                afs_error::Error::coded(
                    afs_error::NODE_TRANSFER_UNAVAILABLE,
                    "read cache publication expired",
                )
            })?
            .map_err(afs_transport::grpc::error_status::status_to_error)?
            .into_inner();
            if reply.copy_id != expected.id.0 {
                return Err(afs_error::Error::coded(
                    afs_error::CLIENT_PROTOCOL_VIOLATION,
                    "read cache publication identity differs",
                ));
            }
            Ok(expected)
        })
    }

    fn retire_read_cache(
        &self,
        request: crate::dfs::RetireReadCache,
        deadline: std::time::Instant,
    ) -> crate::node::read::admission::ReadFuture<'_, ()> {
        Box::pin(async move {
            let crate::dfs::CopyLocation::Node {
                node_id,
                node_epoch,
                device_id,
                device_epoch,
                catalog_revision,
            } = request.copy.location
            else {
                return Err(afs_error::Error::coded(
                    afs_error::IO_INVALID,
                    "read cache retirement requires node copy",
                ));
            };
            if request.copy.role != crate::dfs::CopyRole::VerifiedCache
                || request.copy.state != crate::dfs::CopyState::Ready
            {
                return Err(afs_error::Error::coded(
                    afs_error::IO_INVALID,
                    "read cache retirement requires verified cache",
                ));
            }
            let ack = crate::dfs::ReplicaAck {
                operation_id: request.operation_id.clone(),
                chunk_id: request.copy.chunk_id,
                placement_revision: 0,
                placement_epoch: 0,
                node_id,
                node_epoch,
                device_id,
                device_epoch,
                catalog_revision,
                persisted_bytes: request.copy.persisted_bytes,
                verified_digest: request.copy.verified_digest,
            };
            let mut client = self.client();
            tokio::time::timeout_at(
                tokio::time::Instant::from_std(deadline),
                client.retire_read_cache(afs_tracing::request_with_current_context(
                    afs_protocol::meta::RetireDfsReadCacheRequest {
                        caller_id: request.caller_id,
                        caller_session_id: request.caller_session_id,
                        caller_node_epoch: request.caller_node_epoch,
                        operation_id: request.operation_id.0,
                        copy_id: request.copy.id.0,
                        receipt: Some(wire_replica_ack(ack)),
                    },
                )),
            )
            .await
            .map_err(|_| {
                afs_error::Error::coded(
                    afs_error::NODE_TRANSFER_UNAVAILABLE,
                    "read cache retirement expired",
                )
            })?
            .map_err(afs_transport::grpc::error_status::status_to_error)?;
            Ok(())
        })
    }

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
            crate::node::metrics::read_timing::ReadDiagnosticStage::MetaGetChunkSources,
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
