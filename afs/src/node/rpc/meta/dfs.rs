//! The GrpcDfsMeta client: runtime, diagnostics, read authorization and repair RPCs.

use super::*;

/// Synchronous adapter used by the DFS Backend on FUSE worker threads.
#[cfg(feature = "dfs")]
pub struct GrpcDfsMeta {
    pub(super) channel: afs_tracing::TracedChannel,
    pub(super) runtime: tokio::runtime::Handle,
    pub(super) node_id: String,
    pub(super) session_id: String,
    pub(super) namespace_id: crate::dfs::NamespaceId,
    pub(super) timeout: Duration,
    sequence: AtomicU64,
    pub(super) placement: Mutex<Option<Arc<crate::dfs::PlacementSnapshot>>>,
    pub(super) read_work: Option<crate::node::read_work::ReadWorkMetrics>,
}

#[cfg(feature = "dfs")]
impl GrpcDfsMeta {
    pub fn new(
        endpoint: &str,
        node_id: String,
        session_id: String,
        namespace_id: crate::dfs::NamespaceId,
        timeout: Duration,
        tls: TlsConfig,
    ) -> afs_error::Result<Self> {
        let security = SecurityManager::new(tls)
            .map_err(|e| Error::coded(CLIENT_ARGUMENT_INVALID, e.to_string()))?;
        let endpoint = GrpcConfig {
            connect_timeout: timeout,
            request_timeout: timeout,
            ..Default::default()
        }
        .configure_client(
            Endpoint::from_shared(security.endpoint_uri(endpoint))
                .map_err(|error| Error::coded(CLIENT_ARGUMENT_INVALID, error.to_string()))?,
        );
        let channel = security
            .connect_lazy(endpoint)
            .map_err(|e| Error::coded(CLIENT_ARGUMENT_INVALID, e.to_string()))?;
        Ok(Self {
            channel: afs_tracing::traced_channel(channel),
            runtime: tokio::runtime::Handle::current(),
            node_id,
            session_id,
            namespace_id,
            timeout,
            sequence: AtomicU64::new(1),
            placement: Mutex::new(None),
            read_work: None,
        })
    }

    pub(crate) fn with_read_work_metrics(
        mut self,
        metrics: crate::node::read_work::ReadWorkMetrics,
    ) -> Self {
        self.read_work = Some(metrics);
        self
    }

    pub(super) fn record_read_rpc(&self, method: &'static str) {
        if let Some(metrics) = &self.read_work {
            metrics.read_rpc(method);
        }
    }

    // Covers request preparation, caller queue/transport wait, reply decoding
    // and domain conversion. It is a client adapter interval, not server CPU.
    pub(super) fn read_diagnostic<T>(
        &self,
        stage: crate::node::read_work::ReadDiagnosticStage,
        call: impl FnOnce() -> afs_error::Result<T>,
    ) -> afs_error::Result<T> {
        let (diagnostic, _coarse) = self.begin_read_diagnostic(stage);
        let result = call();
        if let Some(diagnostic) = diagnostic {
            diagnostic.finish(result.is_ok());
        }
        result
    }

    pub(super) fn begin_read_diagnostic(
        &self,
        stage: crate::node::read_work::ReadDiagnosticStage,
    ) -> (
        Option<crate::node::read_work::ReadDiagnosticSpan>,
        Option<crate::node::read_work::ReadCoarseDiagnosticSpan>,
    ) {
        use crate::node::read_work::ReadDiagnosticStage;
        let diagnostic = self
            .read_work
            .as_ref()
            .and_then(|metrics| metrics.diagnostic(stage));
        let phase = match stage {
            ReadDiagnosticStage::MetaPlacementSnapshot => Some("meta_placement_rpc"),
            ReadDiagnosticStage::MetaCreate => Some("meta_create_rpc"),
            ReadDiagnosticStage::MetaOpenWrite => Some("meta_open_write_rpc"),
            ReadDiagnosticStage::MetaResolveWriteAuthority => Some("meta_resolve_write_rpc"),
            ReadDiagnosticStage::MetaRenewWriteLease => Some("meta_renew_write_lease_rpc"),
            ReadDiagnosticStage::MetaSyncInodeMetadata => Some("meta_sync_inode_metadata_rpc"),
            _ => None,
        };
        let coarse = phase.and_then(|phase| {
            self.read_work
                .as_ref()
                .and_then(|metrics| metrics.coarse_diagnostic(phase, 0))
        });
        (diagnostic, coarse)
    }

    pub(super) fn request_id(&self) -> String {
        crate::meta::store::timed_request_id(format!(
            "{}-dfs-{}",
            self.session_id,
            self.sequence.fetch_add(1, Ordering::Relaxed)
        ))
    }

    /// Blocks the calling thread, under its `call_deadline` scope if any.
    pub(super) fn run<T>(
        &self,
        future: impl std::future::Future<Output = std::result::Result<T, tonic::Status>>,
    ) -> afs_error::Result<T> {
        crate::node::bridge::block_on(&self.runtime, self.run_async(future))
    }

    /// One request under the configured timeout, shortened by the polling
    /// thread's `call_deadline` scope. `block_on` polls on the calling thread, so
    /// a synchronous caller's scope applies here.
    pub(super) async fn run_async<T>(
        &self,
        future: impl std::future::Future<Output = std::result::Result<T, tonic::Status>>,
    ) -> afs_error::Result<T> {
        let timeout = crate::node::call_deadline::bounded(self.timeout);
        tokio::time::timeout(timeout, future)
            .await
            .map_err(|_| {
                Error::coded(
                    afs_error::CLIENT_DEADLINE_EXCEEDED,
                    "DFS Meta request timed out",
                )
            })?
            .map_err(Self::dfs_meta_status_to_error)
    }

    #[cfg(feature = "dfs")]
    pub(super) fn dfs_meta_status_to_error(status: tonic::Status) -> Error {
        if Self::status_source_is_timeout_expired(&status) {
            return Error::coded(
                afs_error::CLIENT_DEADLINE_EXCEEDED,
                "DFS Meta request timed out",
            );
        }
        afs_transport::grpc::error_status::status_to_error(status)
    }

    fn status_source_is_timeout_expired(status: &tonic::Status) -> bool {
        let mut source = std::error::Error::source(status);
        while let Some(error) = source {
            if error.is::<tonic::TimeoutExpired>() {
                return true;
            }
            source = error.source();
        }
        false
    }

    pub(super) fn client(
        &self,
    ) -> afs_protocol::meta::dfs_meta_client::DfsMetaClient<afs_tracing::TracedChannel> {
        afs_protocol::meta::dfs_meta_client::DfsMetaClient::new(self.channel.clone())
    }

    pub(super) fn root_client(
        &self,
    ) -> afs_protocol::meta::meta_client::MetaClient<afs_tracing::TracedChannel> {
        afs_protocol::meta::meta_client::MetaClient::new(self.channel.clone())
    }

    pub(crate) fn authorization_metrics(&self) -> Option<crate::node::read_work::ReadWorkMetrics> {
        self.read_work.clone()
    }

    pub fn validate_read_grants(
        &self,
        request: crate::dfs::ValidateDfsReadGrants,
    ) -> afs_error::Result<Vec<crate::dfs::DfsAuthorizedRead>> {
        self.read_diagnostic(
            crate::node::read_work::ReadDiagnosticStage::MetaValidateReadGrants,
            || {
                self.record_read_rpc("validate_read_grants");
                let validations = request
                    .validations
                    .into_iter()
                    .map(|validation| {
                        let grant = validation.grant;
                        afs_protocol::meta::DfsReadValidation {
                            grant: Some(afs_protocol::meta::DfsReadGrant {
                                namespace_id: grant.namespace_id.0,
                                file_version_id: grant.file_version_id.0,
                                layout_root_id: grant.layout_root_id.0,
                                caller_node_id: grant.caller_node_id,
                                caller_node_epoch: grant.caller_node_epoch,
                                expires_at_unix_ms: grant.expires_at_unix_ms,
                                fence: grant.fence,
                                token: grant.token,
                            }),
                            chunk_id: validation.chunk_id.0,
                            copy_id: validation.copy_id.0,
                            chunk_offset: validation.chunk_offset,
                            length: validation.length,
                        }
                    })
                    .collect();
                let reply = self
                    .run(self.client().validate_read_grants(
                        afs_protocol::meta::ValidateDfsReadGrantsRequest {
                            receiver_node_id: request.receiver_node_id,
                            receiver_node_epoch: request.receiver_node_epoch,
                            peer_node_id: request.peer_node_id,
                            validations,
                        },
                    ))?
                    .into_inner();
                reply
                    .authorizations
                    .into_iter()
                    .map(|entry| {
                        let validation =
                            required(entry.validation, "ReadAuthorization.validation")?;
                        Ok(crate::dfs::DfsAuthorizedRead {
                            validation: crate::dfs::DfsReadValidation {
                                grant: domain_dfs_read_grant(required(
                                    validation.grant,
                                    "ReadAuthorization.grant",
                                )?),
                                chunk_id: crate::dfs::ChunkId::new(validation.chunk_id),
                                copy_id: crate::dfs::CopyId::new(validation.copy_id),
                                chunk_offset: validation.chunk_offset,
                                length: validation.length,
                            },
                            allowed_ranges: entry
                                .allowed_ranges
                                .into_iter()
                                .map(|range| (range.offset, range.length))
                                .collect(),
                            expires_at_unix_ms: entry.expires_at_unix_ms,
                            open_sources: entry.open_sources,
                        })
                    })
                    .collect()
            },
        )
    }

    pub fn validate_external_read_grants(
        &self,
        request: crate::dfs::ValidateExternalReadGrants,
    ) -> afs_error::Result<Vec<crate::dfs::DfsExternalAuthorizedRead>> {
        let reply = self
            .run(
                self.client().validate_external_read_grants(
                    afs_tracing::request_with_current_context(
                        afs_protocol::meta::ValidateExternalReadGrantsRequest {
                            caller_id: request.caller_id,
                            validations: request
                                .validations
                                .into_iter()
                                .map(wire_read_validation)
                                .collect(),
                        },
                    ),
                ),
            )?
            .into_inner();
        reply
            .authorizations
            .into_iter()
            .map(domain_external_authorized_read)
            .collect()
    }

    pub fn authorize_current_external_read(
        &self,
        request: crate::dfs::AuthorizeCurrentExternalRead,
    ) -> afs_error::Result<Option<crate::dfs::AuthorizeCurrentExternalReadReply>> {
        let response = crate::node::bridge::block_on(&self.runtime, async {
            Ok::<_, Error>(
                tokio::time::timeout(
                    self.timeout,
                    self.client().authorize_current_external_read(
                        afs_tracing::request_with_current_context(
                            afs_protocol::meta::AuthorizeCurrentExternalReadRequest {
                                caller_id: request.caller_id,
                                inode_id: request.inode_id.0,
                                namespace_id: request.namespace_id.0,
                                expected_head_version_id: request.expected_head_version.0,
                                layout_root_id: request.layout_root_id.0,
                                validations: request
                                    .validations
                                    .into_iter()
                                    .map(wire_read_validation)
                                    .collect(),
                                missing_sources: request
                                    .missing_sources
                                    .into_iter()
                                    .map(|missing| afs_protocol::meta::DfsMissingReadSource {
                                        chunk_id: missing.chunk_id.0,
                                        chunk_offset: missing.chunk_offset,
                                        length: missing.length,
                                    })
                                    .collect(),
                            },
                        ),
                    ),
                )
                .await,
            )
        })?;
        let reply = match response {
            Err(_) => {
                return Err(Error::coded(
                    afs_error::CLIENT_DEADLINE_EXCEEDED,
                    "DFS Meta request timed out",
                ));
            }
            Ok(Err(status)) if current_external_read_is_unimplemented(&status) => return Ok(None),
            Ok(Err(status)) => return Err(Self::dfs_meta_status_to_error(status)),
            Ok(Ok(response)) => response.into_inner(),
        };
        domain_authorize_current_external_read_reply(reply).map(Some)
    }

    pub fn authorize_external_version_read(
        &self,
        request: crate::dfs::AuthorizeExternalVersionRead,
    ) -> afs_error::Result<crate::dfs::AuthorizeExternalVersionReadReply> {
        let response = crate::node::bridge::block_on(&self.runtime, async {
            Ok::<_, Error>(
                tokio::time::timeout(
                    self.timeout,
                    self.client().authorize_external_version_read(
                        afs_tracing::request_with_current_context(
                            afs_protocol::meta::AuthorizeExternalVersionReadRequest {
                                caller_id: request.caller_id,
                                inode_id: request.inode_id.0,
                                namespace_id: request.namespace_id.0,
                                file_version_id: request.file_version_id.0,
                                layout_root_id: request.layout_root_id.0,
                                requests: request
                                    .requests
                                    .into_iter()
                                    .map(|request| afs_protocol::meta::DfsMissingReadSource {
                                        chunk_id: request.chunk_id.0,
                                        chunk_offset: request.chunk_offset,
                                        length: request.length,
                                    })
                                    .collect(),
                            },
                        ),
                    ),
                )
                .await,
            )
        })?;
        let reply = match response {
            Err(_) => {
                return Err(Error::coded(
                    afs_error::CLIENT_DEADLINE_EXCEEDED,
                    "DFS Meta request timed out",
                ));
            }
            Ok(Err(status)) => return Err(Self::dfs_meta_status_to_error(status)),
            Ok(Ok(response)) => response.into_inner(),
        };
        domain_authorize_external_version_read_reply(reply)
    }

    pub fn validate_replica_write(
        &self,
        request: crate::dfs::ValidateReplicaWriteRequest,
    ) -> afs_error::Result<crate::dfs::ReplicaWriteGrant> {
        let reply = self
            .run(
                self.client()
                    .validate_replica_write(wire_validate_replica_write_request(request)),
            )?
            .into_inner();
        domain_replica_write_grant(required(reply.grant, "ValidateReplicaWrite.grant")?)
    }

    pub fn claim_replication_task(
        &self,
        request: crate::dfs::ClaimReplicationTask,
    ) -> afs_error::Result<Option<crate::dfs::ReplicationClaim>> {
        let reply = self
            .run(self.client().claim_replication_task(
                afs_protocol::meta::ClaimDfsReplicationTaskRequest {
                    caller_id: request.caller_id,
                    caller_session_id: request.caller_session_id,
                    caller_node_epoch: request.caller_node_epoch,
                    operation_id: request.operation_id.0,
                    lease_seconds: request.lease_seconds,
                },
            ))?
            .into_inner();
        if !reply.claimed {
            return Ok(None);
        }
        required(reply.claim, "ClaimReplicationTask.claim")
            .and_then(domain_replication_claim)
            .map(Some)
    }

    pub fn report_replication_task(
        &self,
        request: crate::dfs::ReportReplicationTask,
    ) -> afs_error::Result<crate::dfs::ReplicationTask> {
        let reply = self
            .run(
                self.client().report_replication_task(
                    afs_protocol::meta::ReportDfsReplicationTaskRequest {
                        caller_id: request.caller_id,
                        caller_session_id: request.caller_session_id,
                        caller_node_epoch: request.caller_node_epoch,
                        operation_id: request.operation_id.0,
                        claim: Some(wire_replication_claim(&request.claim)),
                        durable_acks: request
                            .durable_acks
                            .into_iter()
                            .map(wire_replica_ack)
                            .collect(),
                        error: request.error,
                        source_invalid: request.source_invalid,
                    },
                ),
            )?
            .into_inner();
        required(reply.task, "ReportReplicationTask.task").and_then(domain_replication_task)
    }

    pub fn report_chunk_corruption(
        &self,
        request: crate::dfs::ReportChunkCorruption,
    ) -> afs_error::Result<()> {
        self.run(self.client().report_chunk_corruption(
            afs_protocol::meta::ReportDfsChunkCorruptionRequest {
                caller_id: request.caller_id,
                caller_session_id: request.caller_session_id,
                caller_node_epoch: request.caller_node_epoch,
                operation_id: request.operation_id.0,
                chunk_id: request.chunk_id.0,
                device_id: request.device_id,
                device_epoch: request.device_epoch,
                catalog_revision: request.catalog_revision,
            },
        ))?;
        Ok(())
    }

    pub fn report_corrupt_chunk(
        &self,
        request: crate::dfs::ReportChunkCorruption,
    ) -> afs_error::Result<()> {
        self.report_chunk_corruption(request)
    }
}
