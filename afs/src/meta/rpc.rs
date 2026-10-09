//! Generated gRPC adapter around the same Meta authority used by REST.
//!
//! OwnerRoots tonic methods authenticate and translate protobuf/domain types;
//! their business transitions live in `owner_roots.rs` and `Meta`; RPC does
//! not own authority state or decide durable commits.
//! Without a store, authority RPCs fail closed instead of fabricating a grant.

use afs_protocol::meta::{
    AbortRootReply, AbortRootRequest, AckRevocationReply, AckRevocationRequest, AcquireRootReply,
    AcquireRootRequest, ActivateRootReply, ActivateRootRequest, CommitFileVersionReply,
    CommitFileVersionRequest, DfsCallerContext as PbDfsCallerContext, DfsCommitMetadataMode,
    DfsCreateReply, DfsCreateRequest, DfsDentryRecord as PbDfsDentryRecord, DfsGetXattrReply,
    DfsGetXattrRequest, DfsInodeAttributeUpdate as PbDfsInodeAttributeUpdate,
    DfsInodeAttributes as PbDfsInodeAttributes, DfsInodeKind as PbDfsInodeKind,
    DfsInodeRecord as PbDfsInodeRecord, DfsLayoutRoot as PbDfsLayoutRoot, DfsLinkReply,
    DfsLinkRequest, DfsListXattrReply, DfsListXattrRequest, DfsLookupReply, DfsLookupRequest,
    DfsMkdirReply, DfsMkdirRequest, DfsMknodReply, DfsMknodRequest, DfsReadDirReply,
    DfsReadDirRequest, DfsReadLinkReply, DfsReadLinkRequest, DfsRemoveXattrReply,
    DfsRemoveXattrRequest, DfsRenameMode as PbDfsRenameMode, DfsRenameReply, DfsRenameRequest,
    DfsRmdirReply, DfsRmdirRequest, DfsSetInodeAttributesReply, DfsSetInodeAttributesRequest,
    DfsSetXattrReply, DfsSetXattrRequest, DfsSpecialNode as PbDfsSpecialNode,
    DfsSpecialNodeKind as PbDfsSpecialNodeKind, DfsSymlinkReply, DfsSymlinkRequest, DfsUnlinkReply,
    DfsUnlinkRequest, DfsWriteLeaseReply, DfsXattrRecord as PbDfsXattrRecord,
    DfsXattrSetMode as PbDfsXattrSetMode, GetDfsChunkSourcesReply, GetDfsChunkSourcesRequest,
    GetDfsInodeReply, GetDfsInodeRequest, GetDfsPlacementSnapshotReply,
    GetDfsPlacementSnapshotRequest, GetFileVersionReply, GetFileVersionRequest,
    ListOwnerRootsReply, ListOwnerRootsRequest, LookupNodeReply, LookupNodeRequest,
    LookupRootReply, LookupRootRequest, MetaBackendPersistence, NodeDescriptor, NodeEndpoint,
    OpenDfsWriteReply, OpenDfsWriteRequest, PingReply, PingRequest, PollRootCommandBatchRequest,
    PresentedRootAccess, RecoverRootReply, RecoverRootRequest, RegisterNodeReply,
    RegisterNodeRequest, RenewDfsWriteLeaseRequest, ReserveRootReply, ReserveRootRequest,
    ResolveDfsLockAuthorityReply, ResolveDfsLockAuthorityRequest, ResolveDfsWriteAuthorityReply,
    ResolveDfsWriteAuthorityRequest, RootCommand, RootCommandBatchCompacted,
    RootCommandBatchEvents, RootCommandBatchReply, RootCommandBatchUnsupported,
    RootCommandRecoveryCursor, RootCommandRecoveryReason, RootCommandType, RootLocation,
    RootReservation, RootRight as PbRootRight, SyncDfsInodeMetadataReply,
    SyncDfsInodeMetadataRequest, ValidateDfsReplicaWriteReply, ValidateDfsReplicaWriteRequest,
    ValidateRootAccessReply, ValidateRootAccessRequest, WatchRootCommandsRequest,
    dfs_meta_server::DfsMeta as DfsMetaService, meta_server::Meta as MetaService,
    owner_roots_server::OwnerRoots as OwnerRootsService,
};
use std::{pin::Pin, sync::Arc, time::Duration};
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use super::dfs::CreateFileRequest;
use super::store::{
    BackendPersistence, BackendReadiness, NodeSessionLease, RecoveryReason, RequestKey,
    RootAccessGrant, RootCommandRecord, RootCommandType as StoreRootCommandType,
    RootReservationRecord, RootRight, StoreRevision,
};

pub struct MetaRpc(pub Arc<super::Meta>);
pub struct OwnerRootsRpc(pub Arc<super::Meta>);
pub struct DfsMetaRpc(pub Arc<super::Meta>);

type RootCommandStream = Pin<Box<dyn Stream<Item = Result<RootCommand, Status>> + Send + 'static>>;

fn invalid(message: impl Into<String>) -> Status {
    afs_transport::grpc::error_status::error_to_status(afs_error::Error::coded(
        afs_error::META_CATALOG_INVALID_REQUEST,
        message,
    ))
}

fn permission_denied(message: impl Into<String>) -> Status {
    afs_transport::grpc::error_status::error_to_status(afs_error::Error::coded(
        afs_error::CLIENT_PERMISSION_DENIED,
        message,
    ))
}

fn authenticated_node_id<T>(
    meta: &super::Meta,
    request: &Request<T>,
) -> Result<Option<String>, Status> {
    if !meta.enforce_peer_identity {
        return Ok(None);
    }
    let certs = request.peer_certs().ok_or_else(|| {
        permission_denied("mTLS peer certificate is required for Meta authority RPC")
    })?;
    let leaf = certs.first().ok_or_else(|| {
        permission_denied("mTLS peer certificate chain did not contain a leaf certificate")
    })?;
    let node_id = meta
        .trusted_nodes_by_der
        .get(leaf.as_ref())
        .ok_or_else(|| permission_denied("mTLS peer certificate is not trusted for any node"))?;
    Ok(Some(node_id.clone()))
}

fn validate_root_access_identities(
    authenticated_meta_node_id: Option<&str>,
    observed_peer_node_id: &str,
    validator_home_node_id: &str,
    validator_home_session_id: &str,
    presented: &PresentedRootAccess,
) -> Result<(), Status> {
    if let Some(authenticated) = authenticated_meta_node_id
        && authenticated != validator_home_node_id
    {
        return Err(permission_denied(format!(
            "authenticated validator {authenticated} does not match validator_home_node_id {validator_home_node_id}",
        )));
    }
    if observed_peer_node_id != presented.holder_node_id {
        return Err(invalid(
            "observed peer node id does not match presented grant holder",
        ));
    }
    if validator_home_node_id != presented.home_node_id
        || validator_home_session_id != presented.home_session_id
    {
        return Err(invalid("validator is not the presented Home session"));
    }
    Ok(())
}

fn request_key(caller_id: impl Into<String>, request_id: impl Into<String>) -> RequestKey {
    RequestKey::new(caller_id, request_id)
}

fn require_text(value: &str, field: &'static str) -> Result<(), Status> {
    if value.is_empty() {
        Err(invalid(format!("{field} is required")))
    } else {
        Ok(())
    }
}

fn validate_caller(authenticated: Option<&str>, caller_id: &str) -> Result<(), Status> {
    require_text(caller_id, "caller_id")?;
    if let Some(authenticated) = authenticated
        && authenticated != caller_id
    {
        return Err(permission_denied(format!(
            "authenticated node {authenticated} does not match caller_id {caller_id}",
        )));
    }
    Ok(())
}

fn domain_rights(rights: &[i32]) -> Result<Vec<RootRight>, Status> {
    let mut out = Vec::new();
    for right in rights {
        let right = PbRootRight::try_from(*right).map_err(|_| invalid("unknown root right"))?;
        let right = match right {
            PbRootRight::Unspecified => continue,
            PbRootRight::Lookup => RootRight::Lookup,
            PbRootRight::Read => RootRight::Read,
            PbRootRight::Write => RootRight::Write,
            PbRootRight::Admin => RootRight::Admin,
        };
        if !out.contains(&right) {
            out.push(right);
        }
    }
    if out.is_empty() {
        out.extend([RootRight::Lookup, RootRight::Read, RootRight::Write]);
    }
    Ok(out)
}

fn wire_rights(rights: &[RootRight]) -> Vec<i32> {
    rights
        .iter()
        .map(|right| match right {
            RootRight::Lookup => PbRootRight::Lookup.into(),
            RootRight::Read => PbRootRight::Read.into(),
            RootRight::Write => PbRootRight::Write.into(),
            RootRight::Admin => PbRootRight::Admin.into(),
        })
        .collect()
}

fn wire_access(grant: RootAccessGrant) -> afs_protocol::meta::RootAccess {
    afs_protocol::meta::RootAccess {
        root_id: grant.root_id,
        root_epoch: grant.root_epoch,
        home_node_id: grant.home_node_id,
        holder_node_id: grant.holder_node_id,
        session_id: grant.holder_session_id,
        access_generation: grant.access_generation,
        rights: wire_rights(&grant.rights),
        fencing_token: grant.fencing_token,
        home_session_id: grant.home_session_id,
    }
}

fn wire_location(root: super::store::RootRecord) -> RootLocation {
    RootLocation {
        root_id: root.root_id,
        root_epoch: root.root_epoch,
        home_node_id: root.home_node_id,
        home_session_id: root.home_session_id,
    }
}

fn wire_node(session: super::store::NodeSession) -> NodeDescriptor {
    NodeDescriptor {
        node_id: session.node_id,
        endpoint: Some(NodeEndpoint {
            grpc_addr: session.grpc_addr,
            data_addr: session.data_addr,
            rest_addr: session.rest_addr,
        }),
        labels: Default::default(),
        capabilities: Vec::new(),
        session_id: session.session_id,
        storage_devices: session
            .storage_devices
            .into_iter()
            .map(|device| afs_protocol::meta::DfsStorageDevice {
                device_id: device.device_id,
                device_epoch: device.device_epoch,
                catalog_revision: device.catalog_revision,
                failure_domain: device.failure_domain,
            })
            .collect(),
    }
}

fn wire_backend_persistence(persistence: BackendPersistence) -> i32 {
    match persistence {
        BackendPersistence::Unknown => MetaBackendPersistence::Unknown,
        BackendPersistence::Volatile => MetaBackendPersistence::Volatile,
        BackendPersistence::Persistent => MetaBackendPersistence::Persistent,
    }
    .into()
}

fn register_node_reply(
    session: super::store::NodeSession,
    readiness: BackendReadiness,
) -> RegisterNodeReply {
    RegisterNodeReply {
        node_id: session.node_id,
        lease_epoch: session.lease_epoch,
        expires_at_unix_ms: session.expires_at_unix_ms,
        meta_backend_persistence: wire_backend_persistence(readiness.persistence),
        meta_backend_healthy: readiness.healthy,
        meta_persistence_ready: readiness.persistent_ready,
        meta_persistence_detail: readiness.detail,
    }
}

fn wire_reservation(reservation: RootReservationRecord) -> RootReservation {
    RootReservation {
        root_id: reservation.root_id,
        root_epoch: reservation.root_epoch,
        home_node_id: reservation.home_node_id,
        session_id: reservation.home_session_id,
        create_intent_id: reservation.create_intent_id,
        prepare_token: reservation.prepare_token,
    }
}

fn command_from_record(record: RootCommandRecord, revision: StoreRevision) -> RootCommand {
    let command_type = match record.command_type {
        StoreRootCommandType::RevokeAccess => RootCommandType::RevokeAccess,
        StoreRootCommandType::InvalidateCache => RootCommandType::InvalidateCache,
    };
    let access = RootAccessGrant {
        root_id: record.root_id,
        root_epoch: record.root_epoch,
        home_node_id: record.home_node_id,
        home_session_id: record.home_session_id,
        holder_node_id: String::new(),
        holder_session_id: String::new(),
        access_generation: record.old_access_generation,
        rights: Vec::new(),
        fencing_token: String::new(),
        issued_at_revision: revision,
    };
    RootCommand {
        command_id: record.command_id,
        command_type: command_type.into(),
        access: Some(wire_access(access)),
        revision: revision.0,
    }
}

fn recovery_reason_to_wire(reason: RecoveryReason) -> RootCommandRecoveryReason {
    match reason {
        RecoveryReason::WatchCompacted => RootCommandRecoveryReason::WatchCompacted,
        RecoveryReason::NodeSessionRestarted => RootCommandRecoveryReason::NodeSessionRestarted,
        RecoveryReason::BackendLeaderChanged => RootCommandRecoveryReason::BackendLeaderChanged,
    }
}

fn root_command_batch_reply_from_domain(
    batch: super::owner_roots::RootCommandBatch,
) -> RootCommandBatchReply {
    let result = match batch {
        super::owner_roots::RootCommandBatch::Events {
            start_revision,
            next_revision,
            commands,
        } => afs_protocol::meta::root_command_batch_reply::Result::Events(RootCommandBatchEvents {
            start_revision: start_revision.0,
            next_revision: next_revision.0,
            commands: commands
                .into_iter()
                .map(|command| command_from_record(command.command, command.revision))
                .collect(),
        }),
        super::owner_roots::RootCommandBatch::Compacted {
            requested_after,
            compacted_to,
            recovery,
        } => afs_protocol::meta::root_command_batch_reply::Result::Compacted(
            RootCommandBatchCompacted {
                requested_after: requested_after.0,
                compacted_to: compacted_to.0,
                recovery_cursor: Some(RootCommandRecoveryCursor {
                    resume_after: recovery.resume_after.0,
                    reason: recovery_reason_to_wire(recovery.reason).into(),
                }),
            },
        ),
        super::owner_roots::RootCommandBatch::Unsupported { message } => {
            afs_protocol::meta::root_command_batch_reply::Result::Unsupported(
                RootCommandBatchUnsupported { message },
            )
        }
    };
    RootCommandBatchReply {
        result: Some(result),
    }
}

#[tonic::async_trait]
impl MetaService for MetaRpc {
    async fn ping(&self, request: Request<PingRequest>) -> Result<Response<PingReply>, Status> {
        let span = afs_tracing::tracing::info_span!("meta.ping");
        let _entered = span.enter();
        let message = self
            .0
            .ping(&request.into_inner().node_id)
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(PingReply {
            message,
            instance: self.0.id.clone(),
        }))
    }

    async fn register_node(
        &self,
        request: Request<RegisterNodeRequest>,
    ) -> Result<Response<RegisterNodeReply>, Status> {
        self.0.observability.record("meta", "register_node", true);
        let authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        let node = request
            .node
            .ok_or_else(|| invalid("node descriptor is required"))?;
        require_text(&request.request_id, "request_id")?;
        require_text(&node.node_id, "node.node_id")?;
        if let Some(authenticated) = authenticated
            && authenticated != node.node_id
        {
            return Err(permission_denied(format!(
                "authenticated node {authenticated} does not match node.node_id {}",
                node.node_id
            )));
        }
        require_text(&node.session_id, "node.session_id")?;
        let endpoint = node
            .endpoint
            .ok_or_else(|| invalid("node.endpoint is required"))?;
        let ttl = Duration::from_secs(request.lease_seconds.max(1));
        let session = self
            .0
            .register_node(
                request_key(
                    format!("{}/{}", node.node_id, node.session_id),
                    request.request_id,
                ),
                NodeSessionLease {
                    node_id: node.node_id,
                    session_id: node.session_id,
                    grpc_addr: endpoint.grpc_addr,
                    data_addr: endpoint.data_addr,
                    rest_addr: endpoint.rest_addr,
                    storage_devices: node
                        .storage_devices
                        .into_iter()
                        .map(|device| crate::dfs::StorageDeviceDescriptor {
                            device_id: device.device_id,
                            device_epoch: device.device_epoch,
                            catalog_revision: device.catalog_revision,
                            failure_domain: device.failure_domain,
                        })
                        .collect(),
                    lease_ttl: ttl,
                },
            )
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        let readiness = self
            .0
            .store
            .as_deref()
            .ok_or_else(|| {
                afs_transport::grpc::error_status::error_to_status(
                    super::store::unavailable_meta_store(),
                )
            })?
            .backend_readiness()
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(register_node_reply(session, readiness)))
    }

    async fn lookup_node(
        &self,
        request: Request<LookupNodeRequest>,
    ) -> Result<Response<LookupNodeReply>, Status> {
        let request = request.into_inner();
        require_text(&request.node_id, "node_id")?;
        let session = self
            .0
            .lookup_node(&request.node_id)
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        let (node, lease_epoch, expires_at_unix_ms) = match session {
            Some(session) => {
                let lease_epoch = session.lease_epoch;
                let expires_at_unix_ms = session.expires_at_unix_ms;
                (Some(wire_node(session)), lease_epoch, expires_at_unix_ms)
            }
            _ => (None, 0, 0),
        };
        Ok(Response::new(LookupNodeReply {
            node,
            lease_epoch,
            expires_at_unix_ms,
            found: lease_epoch != 0,
        }))
    }
}

#[tonic::async_trait]
impl OwnerRootsService for OwnerRootsRpc {
    type WatchRootCommandsStream = RootCommandStream;

    async fn reserve_root(
        &self,
        request: Request<ReserveRootRequest>,
    ) -> Result<Response<ReserveRootReply>, Status> {
        let authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        require_text(&request.request_id, "request_id")?;
        require_text(&request.root_id, "root_id")?;
        require_text(&request.preferred_home_node_id, "preferred_home_node_id")?;
        if let Some(authenticated) = authenticated
            && authenticated != request.preferred_home_node_id
        {
            return Err(permission_denied(format!(
                "authenticated node {authenticated} does not match preferred_home_node_id {}",
                request.preferred_home_node_id
            )));
        }
        require_text(&request.session_id, "session_id")?;
        require_text(&request.create_intent_id, "create_intent_id")?;
        let rights = domain_rights(&request.rights)?;
        let output = self
            .0
            .owner_roots
            .reserve_root(super::owner_roots::ReserveRootInput {
                request_id: request.request_id,
                root_id: request.root_id,
                preferred_home_node_id: request.preferred_home_node_id,
                session_id: request.session_id,
                rights,
                expected_root_epoch: request.expected_root_epoch,
                create_intent_id: request.create_intent_id,
            })
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(ReserveRootReply {
            reservation: Some(wire_reservation(output.reservation)),
            already_reserved_by_same_intent: output.already_reserved_by_same_intent,
        }))
    }

    async fn activate_root(
        &self,
        request: Request<ActivateRootRequest>,
    ) -> Result<Response<ActivateRootReply>, Status> {
        let authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        require_text(&request.request_id, "request_id")?;
        require_text(&request.local_prepare_id, "local_prepare_id")?;
        if !request.parent_fsync_complete {
            return Err(invalid("parent_fsync_complete is required"));
        }
        let reservation = request
            .reservation
            .ok_or_else(|| invalid("reservation is required"))?;
        require_text(&reservation.root_id, "reservation.root_id")?;
        require_text(&reservation.home_node_id, "reservation.home_node_id")?;
        require_text(&reservation.session_id, "reservation.session_id")?;
        if let Some(authenticated) = authenticated
            && authenticated != reservation.home_node_id
        {
            return Err(permission_denied(format!(
                "authenticated node {authenticated} does not match reservation.home_node_id {}",
                reservation.home_node_id
            )));
        }
        let grant = self
            .0
            .owner_roots
            .activate_root(super::owner_roots::ActivateRootInput {
                request_id: request.request_id,
                reservation: super::owner_roots::WireRootReservation {
                    root_id: reservation.root_id,
                    root_epoch: reservation.root_epoch,
                    home_node_id: reservation.home_node_id,
                    session_id: reservation.session_id,
                    create_intent_id: reservation.create_intent_id,
                    prepare_token: reservation.prepare_token,
                },
                local_prepare_id: request.local_prepare_id,
            })
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(ActivateRootReply {
            access: Some(wire_access(grant)),
        }))
    }

    async fn abort_root(
        &self,
        request: Request<AbortRootRequest>,
    ) -> Result<Response<AbortRootReply>, Status> {
        let authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        require_text(&request.request_id, "request_id")?;
        require_text(&request.root_id, "root_id")?;
        require_text(&request.session_id, "session_id")?;
        require_text(&request.create_intent_id, "create_intent_id")?;
        require_text(&request.prepare_token, "prepare_token")?;
        self.0
            .owner_roots
            .abort_root(super::owner_roots::AbortRootInput {
                request_id: request.request_id,
                root_id: request.root_id,
                root_epoch: request.root_epoch,
                session_id: request.session_id,
                create_intent_id: request.create_intent_id,
                prepare_token: request.prepare_token,
                authenticated_home_node_id: authenticated,
            })
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(AbortRootReply {}))
    }

    async fn lookup_root(
        &self,
        request: Request<LookupRootRequest>,
    ) -> Result<Response<LookupRootReply>, Status> {
        let request = request.into_inner();
        require_text(&request.root_id, "root_id")?;
        let location = self
            .0
            .owner_roots
            .lookup_root(super::owner_roots::LookupRootInput {
                root_id: request.root_id,
            })
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?
            .map(wire_location);
        Ok(Response::new(LookupRootReply {
            found: location.is_some(),
            location,
        }))
    }

    async fn list_owner_roots(
        &self,
        request: Request<ListOwnerRootsRequest>,
    ) -> Result<Response<ListOwnerRootsReply>, Status> {
        let authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        require_text(&request.request_id, "request_id")?;
        require_text(&request.home_node_id, "home_node_id")?;
        if let Some(authenticated) = authenticated
            && authenticated != request.home_node_id
        {
            return Err(permission_denied(format!(
                "authenticated node {authenticated} does not match home_node_id {}",
                request.home_node_id
            )));
        }
        let output = self
            .0
            .owner_roots
            .list_owner_roots(super::owner_roots::ListOwnerRootsInput {
                home_node_id: request.home_node_id,
            })
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(ListOwnerRootsReply {
            active_roots: output.active_roots.into_iter().map(wire_location).collect(),
            pending_reservations: output
                .pending_reservations
                .into_iter()
                .map(wire_reservation)
                .collect(),
            authority_revision: output.authority_revision.0,
        }))
    }

    async fn acquire_root(
        &self,
        request: Request<AcquireRootRequest>,
    ) -> Result<Response<AcquireRootReply>, Status> {
        let authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        require_text(&request.request_id, "request_id")?;
        require_text(&request.root_id, "root_id")?;
        require_text(&request.requester_node_id, "requester_node_id")?;
        if let Some(authenticated) = authenticated
            && authenticated != request.requester_node_id
        {
            return Err(permission_denied(format!(
                "authenticated node {authenticated} does not match requester_node_id {}",
                request.requester_node_id
            )));
        }
        require_text(&request.session_id, "session_id")?;
        let requested_rights = domain_rights(&request.rights)?;
        let grant = self
            .0
            .owner_roots
            .acquire_root(super::owner_roots::AcquireRootInput {
                request_id: request.request_id,
                root_id: request.root_id,
                requester_node_id: request.requester_node_id,
                session_id: request.session_id,
                rights: requested_rights,
                expected_root_epoch: request.expected_root_epoch,
                expected_access_generation: request.expected_access_generation,
            })
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(AcquireRootReply {
            access: Some(wire_access(grant)),
        }))
    }

    async fn validate_root_access(
        &self,
        request: Request<ValidateRootAccessRequest>,
    ) -> Result<Response<ValidateRootAccessReply>, Status> {
        let authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        let presented = request
            .presented_access
            .ok_or_else(|| invalid("presented_access is required"))?;
        validate_root_access_identities(
            authenticated.as_deref(),
            &request.observed_peer_node_id,
            &request.validator_home_node_id,
            &request.validator_home_session_id,
            &presented,
        )?;
        let output = self
            .0
            .owner_roots
            .validate_root_access(super::owner_roots::ValidateRootAccessInput {
                root_id: presented.root_id,
                root_epoch: presented.root_epoch,
                home_session_id: presented.home_session_id,
                holder_node_id: presented.holder_node_id,
                holder_session_id: presented.session_id,
                access_generation: presented.access_generation,
                fencing_token: presented.fencing_token,
            })
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(ValidateRootAccessReply {
            access: Some(wire_access(output.grant)),
            authority_revision: output.authority_revision.0,
        }))
    }

    async fn watch_root_commands(
        &self,
        request: Request<WatchRootCommandsRequest>,
    ) -> Result<Response<Self::WatchRootCommandsStream>, Status> {
        let request = request.into_inner();
        require_text(&request.node_id, "node_id")?;
        require_text(&request.session_id, "session_id")?;
        let commands = self
            .0
            .owner_roots
            .watch_root_commands(super::owner_roots::WatchRootCommandsInput {
                node_id: request.node_id,
                session_id: request.session_id,
                after_revision: request.after_revision,
            })
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        let commands = commands
            .into_iter()
            .map(|command| Ok(command_from_record(command.command, command.revision)))
            .collect::<Vec<_>>();
        Ok(Response::new(Box::pin(tokio_stream::iter(commands))))
    }

    async fn poll_root_command_batch(
        &self,
        request: Request<PollRootCommandBatchRequest>,
    ) -> Result<Response<RootCommandBatchReply>, Status> {
        let authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        require_text(&request.node_id, "node_id")?;
        require_text(&request.session_id, "session_id")?;
        if let Some(authenticated) = authenticated
            && authenticated != request.node_id
        {
            return Err(permission_denied(format!(
                "authenticated node {authenticated} does not match node_id {}",
                request.node_id
            )));
        }
        let batch = self
            .0
            .owner_roots
            .poll_root_command_batch(super::owner_roots::WatchRootCommandsInput {
                node_id: request.node_id,
                session_id: request.session_id,
                after_revision: request.after_revision,
            })
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(root_command_batch_reply_from_domain(batch)))
    }

    async fn ack_revocation(
        &self,
        request: Request<AckRevocationRequest>,
    ) -> Result<Response<AckRevocationReply>, Status> {
        let authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        require_text(&request.request_id, "request_id")?;
        require_text(&request.command_id, "command_id")?;
        require_text(&request.node_id, "node_id")?;
        require_text(&request.session_id, "session_id")?;
        require_text(&request.root_id, "root_id")?;
        if let Some(authenticated) = authenticated
            && authenticated != request.node_id
        {
            return Err(permission_denied(format!(
                "authenticated node {authenticated} does not match node_id {}",
                request.node_id
            )));
        }
        let accepted_at_unix_ms = self
            .0
            .owner_roots
            .ack_revocation(super::owner_roots::AckRevocationInput {
                request_id: request.request_id,
                command_id: request.command_id,
                node_id: request.node_id,
                session_id: request.session_id,
                root_id: request.root_id,
                root_epoch: request.root_epoch,
                access_generation: request.access_generation,
                success: request.success,
                message: request.message,
            })
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(AckRevocationReply {
            accepted_at_unix_ms,
        }))
    }

    async fn recover_root(
        &self,
        request: Request<RecoverRootRequest>,
    ) -> Result<Response<RecoverRootReply>, Status> {
        let authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        require_text(&request.request_id, "request_id")?;
        require_text(&request.root_id, "root_id")?;
        require_text(&request.home_node_id, "home_node_id")?;
        if let Some(authenticated) = authenticated
            && authenticated != request.home_node_id
        {
            return Err(permission_denied(format!(
                "authenticated node {authenticated} does not match home_node_id {}",
                request.home_node_id
            )));
        }
        require_text(&request.home_session_id, "home_session_id")?;
        require_text(&request.local_prepare_id, "local_prepare_id")?;
        let grant = self
            .0
            .owner_roots
            .recover_root(super::owner_roots::RecoverRootInput {
                request_id: request.request_id,
                root_id: request.root_id,
                expected_root_epoch: request.expected_root_epoch,
                home_node_id: request.home_node_id,
                home_session_id: request.home_session_id,
                local_prepare_id: request.local_prepare_id,
            })
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(RecoverRootReply {
            access: Some(wire_access(grant)),
        }))
    }
}

#[tonic::async_trait]
impl DfsMetaService for DfsMetaRpc {
    async fn lookup(
        &self,
        request: Request<DfsLookupRequest>,
    ) -> Result<Response<DfsLookupReply>, Status> {
        let request = request.into_inner();
        require_text(&request.namespace_id, "namespace_id")?;
        require_text(&request.parent_inode_id, "parent_inode_id")?;
        let inode = dfs_service(&self.0)?
            .lookup(
                crate::dfs::NamespaceId::new(request.namespace_id),
                crate::dfs::InodeId::new(request.parent_inode_id),
                request.name,
            )
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(DfsLookupReply {
            found: inode.is_some(),
            inode: inode.map(wire_dfs_inode),
        }))
    }

    async fn create(
        &self,
        request: Request<DfsCreateRequest>,
    ) -> Result<Response<DfsCreateReply>, Status> {
        let authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        validate_caller(authenticated.as_deref(), &request.caller_id)?;
        require_text(&request.operation_id, "operation_id")?;
        require_text(&request.namespace_id, "namespace_id")?;
        require_text(&request.parent_inode_id, "parent_inode_id")?;
        require_text(&request.owner_session_id, "owner_session_id")?;
        let attributes = request
            .attributes
            .ok_or_else(|| invalid("DFS create requires attributes"))?;
        let (inode, lease) = dfs_service(&self.0)?
            .create(CreateFileRequest {
                caller_id: request.caller_id,
                owner_session_id: request.owner_session_id,
                operation_id: crate::dfs::OperationId::new(request.operation_id),
                namespace_id: crate::dfs::NamespaceId::new(request.namespace_id),
                parent_inode_id: crate::dfs::InodeId::new(request.parent_inode_id),
                name: request.name,
                attributes: domain_dfs_attributes(attributes),
                lease_seconds: request.lease_seconds,
            })
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(DfsCreateReply {
            inode: Some(wire_dfs_inode(inode)),
            write_lease: Some(wire_dfs_write_lease(lease)),
        }))
    }

    async fn mkdir(
        &self,
        request: Request<DfsMkdirRequest>,
    ) -> Result<Response<DfsMkdirReply>, Status> {
        let authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        validate_caller(authenticated.as_deref(), &request.caller_id)?;
        require_text(&request.operation_id, "operation_id")?;
        require_text(&request.namespace_id, "namespace_id")?;
        require_text(&request.parent_inode_id, "parent_inode_id")?;
        let attributes = request
            .attributes
            .ok_or_else(|| invalid("DFS mkdir requires attributes"))?;
        let caller = request
            .caller
            .ok_or_else(|| invalid("DFS mkdir requires caller context"))?;
        let inode = dfs_service(&self.0)?
            .mkdir(crate::dfs::MkdirRequest {
                caller_id: request.caller_id,
                operation_id: crate::dfs::OperationId::new(request.operation_id),
                namespace_id: crate::dfs::NamespaceId::new(request.namespace_id),
                parent_inode_id: crate::dfs::InodeId::new(request.parent_inode_id),
                name: request.name,
                attributes: domain_dfs_attributes(attributes),
                caller: domain_caller_context(caller),
            })
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(DfsMkdirReply {
            inode: Some(wire_dfs_inode(inode)),
        }))
    }

    async fn read_dir(
        &self,
        request: Request<DfsReadDirRequest>,
    ) -> Result<Response<DfsReadDirReply>, Status> {
        let request = request.into_inner();
        require_text(&request.namespace_id, "namespace_id")?;
        require_text(&request.parent_inode_id, "parent_inode_id")?;
        let entries = dfs_service(&self.0)?
            .read_dir(
                crate::dfs::NamespaceId::new(request.namespace_id),
                crate::dfs::InodeId::new(request.parent_inode_id),
            )
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(DfsReadDirReply {
            entries: entries.into_iter().map(wire_dfs_dentry).collect(),
        }))
    }

    async fn unlink(
        &self,
        request: Request<DfsUnlinkRequest>,
    ) -> Result<Response<DfsUnlinkReply>, Status> {
        let authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        validate_caller(authenticated.as_deref(), &request.caller_id)?;
        require_text(&request.operation_id, "operation_id")?;
        require_text(&request.namespace_id, "namespace_id")?;
        require_text(&request.parent_inode_id, "parent_inode_id")?;
        let caller = request
            .caller
            .ok_or_else(|| invalid("DFS unlink requires caller context"))?;
        let inode = dfs_service(&self.0)?
            .unlink(crate::dfs::UnlinkRequest {
                caller_id: request.caller_id,
                operation_id: crate::dfs::OperationId::new(request.operation_id),
                namespace_id: crate::dfs::NamespaceId::new(request.namespace_id),
                parent_inode_id: crate::dfs::InodeId::new(request.parent_inode_id),
                name: request.name,
                caller: domain_caller_context(caller),
            })
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(DfsUnlinkReply {
            inode: Some(wire_dfs_inode(inode)),
        }))
    }

    async fn rmdir(
        &self,
        request: Request<DfsRmdirRequest>,
    ) -> Result<Response<DfsRmdirReply>, Status> {
        let authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        validate_caller(authenticated.as_deref(), &request.caller_id)?;
        require_text(&request.operation_id, "operation_id")?;
        require_text(&request.namespace_id, "namespace_id")?;
        require_text(&request.parent_inode_id, "parent_inode_id")?;
        let caller = request
            .caller
            .ok_or_else(|| invalid("DFS rmdir requires caller context"))?;
        let inode = dfs_service(&self.0)?
            .rmdir(crate::dfs::RmdirRequest {
                caller_id: request.caller_id,
                operation_id: crate::dfs::OperationId::new(request.operation_id),
                namespace_id: crate::dfs::NamespaceId::new(request.namespace_id),
                parent_inode_id: crate::dfs::InodeId::new(request.parent_inode_id),
                name: request.name,
                caller: domain_caller_context(caller),
            })
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(DfsRmdirReply {
            inode: Some(wire_dfs_inode(inode)),
        }))
    }

    async fn rename(
        &self,
        request: Request<DfsRenameRequest>,
    ) -> Result<Response<DfsRenameReply>, Status> {
        let authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        validate_caller(authenticated.as_deref(), &request.caller_id)?;
        require_text(&request.operation_id, "operation_id")?;
        require_text(&request.namespace_id, "namespace_id")?;
        require_text(&request.old_parent_inode_id, "old_parent_inode_id")?;
        require_text(&request.new_parent_inode_id, "new_parent_inode_id")?;
        let mode = match PbDfsRenameMode::try_from(request.mode) {
            Ok(PbDfsRenameMode::NoReplace) => crate::dfs::RenameMode::NoReplace,
            Ok(PbDfsRenameMode::Replace) => crate::dfs::RenameMode::Replace,
            Ok(PbDfsRenameMode::Unspecified) | Err(_) => {
                return Err(invalid("DFS rename mode is required"));
            }
        };
        let caller = request
            .caller
            .ok_or_else(|| invalid("DFS rename requires caller context"))?;
        let outcome = dfs_service(&self.0)?
            .rename(crate::dfs::RenameRequest {
                caller_id: request.caller_id,
                operation_id: crate::dfs::OperationId::new(request.operation_id),
                namespace_id: crate::dfs::NamespaceId::new(request.namespace_id),
                old_parent_inode_id: crate::dfs::InodeId::new(request.old_parent_inode_id),
                old_name: request.old_name,
                new_parent_inode_id: crate::dfs::InodeId::new(request.new_parent_inode_id),
                new_name: request.new_name,
                mode,
                caller: domain_caller_context(caller),
            })
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(DfsRenameReply {
            inode: Some(wire_dfs_inode(outcome.inode)),
            replaced: outcome.replaced_inode.is_some(),
            replaced_inode: outcome.replaced_inode.map(wire_dfs_inode),
        }))
    }

    async fn link(
        &self,
        request: Request<DfsLinkRequest>,
    ) -> Result<Response<DfsLinkReply>, Status> {
        let authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        validate_caller(authenticated.as_deref(), &request.caller_id)?;
        require_text(&request.operation_id, "operation_id")?;
        require_text(&request.namespace_id, "namespace_id")?;
        require_text(&request.existing_inode_id, "existing_inode_id")?;
        require_text(&request.parent_inode_id, "parent_inode_id")?;
        let caller = request
            .caller
            .ok_or_else(|| invalid("DFS link requires caller context"))?;
        let inode = dfs_service(&self.0)?
            .link(crate::dfs::LinkRequest {
                caller_id: request.caller_id,
                operation_id: crate::dfs::OperationId::new(request.operation_id),
                namespace_id: crate::dfs::NamespaceId::new(request.namespace_id),
                existing_inode_id: crate::dfs::InodeId::new(request.existing_inode_id),
                expected_inode_revision: request.expected_inode_revision,
                parent_inode_id: crate::dfs::InodeId::new(request.parent_inode_id),
                name: request.name,
                caller: domain_caller_context(caller),
            })
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(DfsLinkReply {
            inode: Some(wire_dfs_inode(inode)),
        }))
    }

    async fn symlink(
        &self,
        request: Request<DfsSymlinkRequest>,
    ) -> Result<Response<DfsSymlinkReply>, Status> {
        let authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        validate_caller(authenticated.as_deref(), &request.caller_id)?;
        require_text(&request.operation_id, "operation_id")?;
        require_text(&request.namespace_id, "namespace_id")?;
        require_text(&request.parent_inode_id, "parent_inode_id")?;
        let attributes = request
            .attributes
            .ok_or_else(|| invalid("DFS symlink requires attributes"))?;
        let caller = request
            .caller
            .ok_or_else(|| invalid("DFS symlink requires caller context"))?;
        let inode = dfs_service(&self.0)?
            .symlink(crate::dfs::SymlinkRequest {
                caller_id: request.caller_id,
                operation_id: crate::dfs::OperationId::new(request.operation_id),
                namespace_id: crate::dfs::NamespaceId::new(request.namespace_id),
                parent_inode_id: crate::dfs::InodeId::new(request.parent_inode_id),
                name: request.name,
                target: request.target,
                attributes: domain_dfs_attributes(attributes),
                caller: domain_caller_context(caller),
            })
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(DfsSymlinkReply {
            inode: Some(wire_dfs_inode(inode)),
        }))
    }

    async fn mknod(
        &self,
        request: Request<DfsMknodRequest>,
    ) -> Result<Response<DfsMknodReply>, Status> {
        let authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        validate_caller(authenticated.as_deref(), &request.caller_id)?;
        require_text(&request.operation_id, "operation_id")?;
        require_text(&request.namespace_id, "namespace_id")?;
        require_text(&request.parent_inode_id, "parent_inode_id")?;
        let attributes = request
            .attributes
            .ok_or_else(|| invalid("DFS mknod requires attributes"))?;
        let caller = request
            .caller
            .ok_or_else(|| invalid("DFS mknod requires caller context"))?;
        let special_node = request
            .special_node
            .ok_or_else(|| invalid("DFS mknod requires special_node"))?;
        let request = crate::dfs::MknodRequest {
            caller_id: request.caller_id,
            operation_id: crate::dfs::OperationId::new(request.operation_id),
            namespace_id: crate::dfs::NamespaceId::new(request.namespace_id),
            parent_inode_id: crate::dfs::InodeId::new(request.parent_inode_id),
            name: request.name,
            kind: domain_dfs_special_node(special_node)?,
            attributes: domain_dfs_attributes(attributes),
            caller: domain_caller_context(caller),
        };
        let inode = dfs_service(&self.0)?
            .mknod(request)
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(DfsMknodReply {
            inode: Some(wire_dfs_inode(inode)),
        }))
    }

    async fn read_link(
        &self,
        request: Request<DfsReadLinkRequest>,
    ) -> Result<Response<DfsReadLinkReply>, Status> {
        let request = request.into_inner();
        require_text(&request.namespace_id, "namespace_id")?;
        require_text(&request.inode_id, "inode_id")?;
        let target = dfs_service(&self.0)?
            .read_link(crate::dfs::ReadLinkRequest {
                namespace_id: crate::dfs::NamespaceId::new(request.namespace_id),
                inode_id: crate::dfs::InodeId::new(request.inode_id),
            })
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(DfsReadLinkReply { target }))
    }

    async fn set_inode_attributes(
        &self,
        request: Request<DfsSetInodeAttributesRequest>,
    ) -> Result<Response<DfsSetInodeAttributesReply>, Status> {
        let authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        validate_caller(authenticated.as_deref(), &request.caller_id)?;
        require_text(&request.operation_id, "operation_id")?;
        require_text(&request.inode_id, "inode_id")?;
        let inode = dfs_service(&self.0)?
            .set_inode_attributes(crate::dfs::SetInodeAttrRequest {
                caller_id: request.caller_id,
                operation_id: crate::dfs::OperationId::new(request.operation_id),
                caller: domain_caller_context(
                    request
                        .caller
                        .ok_or_else(|| invalid("DFS setattr requires caller context"))?,
                ),
                inode_id: crate::dfs::InodeId::new(request.inode_id),
                expected_inode_revision: request.expected_inode_revision,
                update: domain_inode_attr_update(
                    request
                        .update
                        .ok_or_else(|| invalid("DFS setattr requires update"))?,
                ),
            })
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(DfsSetInodeAttributesReply {
            inode: Some(wire_dfs_inode(inode)),
        }))
    }

    async fn get_xattr(
        &self,
        request: Request<DfsGetXattrRequest>,
    ) -> Result<Response<DfsGetXattrReply>, Status> {
        let _authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        require_text(&request.inode_id, "inode_id")?;
        let value = dfs_service(&self.0)?
            .get_xattr(crate::dfs::GetXattrRequest {
                caller: domain_caller_context(
                    request
                        .caller
                        .ok_or_else(|| invalid("DFS getxattr requires caller context"))?,
                ),
                inode_id: crate::dfs::InodeId::new(request.inode_id),
                name: request.name,
            })
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(DfsGetXattrReply { value }))
    }

    async fn list_xattr(
        &self,
        request: Request<DfsListXattrRequest>,
    ) -> Result<Response<DfsListXattrReply>, Status> {
        let _authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        require_text(&request.inode_id, "inode_id")?;
        let names = dfs_service(&self.0)?
            .list_xattr(crate::dfs::ListXattrRequest {
                caller: domain_caller_context(
                    request
                        .caller
                        .ok_or_else(|| invalid("DFS listxattr requires caller context"))?,
                ),
                inode_id: crate::dfs::InodeId::new(request.inode_id),
            })
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(DfsListXattrReply { names }))
    }

    async fn set_xattr(
        &self,
        request: Request<DfsSetXattrRequest>,
    ) -> Result<Response<DfsSetXattrReply>, Status> {
        let authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        validate_caller(authenticated.as_deref(), &request.caller_id)?;
        require_text(&request.operation_id, "operation_id")?;
        require_text(&request.inode_id, "inode_id")?;
        let mode = match PbDfsXattrSetMode::try_from(request.mode) {
            Ok(PbDfsXattrSetMode::Upsert) => crate::dfs::XattrSetMode::Upsert,
            Ok(PbDfsXattrSetMode::Create) => crate::dfs::XattrSetMode::Create,
            Ok(PbDfsXattrSetMode::Replace) => crate::dfs::XattrSetMode::Replace,
            Ok(PbDfsXattrSetMode::Unspecified) | Err(_) => {
                return Err(invalid("DFS setxattr mode is required"));
            }
        };
        let inode = dfs_service(&self.0)?
            .set_xattr(crate::dfs::SetXattrRequest {
                caller_id: request.caller_id,
                operation_id: crate::dfs::OperationId::new(request.operation_id),
                caller: domain_caller_context(
                    request
                        .caller
                        .ok_or_else(|| invalid("DFS setxattr requires caller context"))?,
                ),
                inode_id: crate::dfs::InodeId::new(request.inode_id),
                expected_inode_revision: request.expected_inode_revision,
                name: request.name,
                value: request.value,
                mode,
            })
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(DfsSetXattrReply {
            inode: Some(wire_dfs_inode(inode)),
        }))
    }

    async fn remove_xattr(
        &self,
        request: Request<DfsRemoveXattrRequest>,
    ) -> Result<Response<DfsRemoveXattrReply>, Status> {
        let authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        validate_caller(authenticated.as_deref(), &request.caller_id)?;
        require_text(&request.operation_id, "operation_id")?;
        require_text(&request.inode_id, "inode_id")?;
        let inode = dfs_service(&self.0)?
            .remove_xattr(crate::dfs::RemoveXattrRequest {
                caller_id: request.caller_id,
                operation_id: crate::dfs::OperationId::new(request.operation_id),
                caller: domain_caller_context(
                    request
                        .caller
                        .ok_or_else(|| invalid("DFS removexattr requires caller context"))?,
                ),
                inode_id: crate::dfs::InodeId::new(request.inode_id),
                expected_inode_revision: request.expected_inode_revision,
                name: request.name,
            })
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(DfsRemoveXattrReply {
            inode: Some(wire_dfs_inode(inode)),
        }))
    }

    async fn open_write(
        &self,
        request: Request<OpenDfsWriteRequest>,
    ) -> Result<Response<OpenDfsWriteReply>, Status> {
        let authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        validate_caller(authenticated.as_deref(), &request.caller_id)?;
        require_text(&request.owner_session_id, "owner_session_id")?;
        require_text(&request.operation_id, "operation_id")?;
        require_text(&request.inode_id, "inode_id")?;
        let (inode, lease) = dfs_service(&self.0)?
            .open_write(
                request.caller_id,
                request.owner_session_id,
                crate::dfs::OperationId::new(request.operation_id),
                crate::dfs::InodeId::new(request.inode_id),
                request.lease_seconds,
            )
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(OpenDfsWriteReply {
            inode: Some(wire_dfs_inode(inode)),
            write_lease: Some(wire_dfs_write_lease(lease)),
        }))
    }

    async fn resolve_lock_authority(
        &self,
        request: Request<ResolveDfsLockAuthorityRequest>,
    ) -> Result<Response<ResolveDfsLockAuthorityReply>, Status> {
        let authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        validate_caller(authenticated.as_deref(), &request.caller_id)?;
        require_text(&request.owner_session_id, "owner_session_id")?;
        require_text(&request.operation_id, "operation_id")?;
        require_text(&request.inode_id, "inode_id")?;
        let (inode, lease) = dfs_service(&self.0)?
            .resolve_lock_authority(
                request.caller_id,
                request.owner_session_id,
                crate::dfs::OperationId::new(request.operation_id),
                crate::dfs::InodeId::new(request.inode_id),
                request.lease_seconds,
            )
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(ResolveDfsLockAuthorityReply {
            inode: Some(wire_dfs_inode(inode)),
            write_lease: Some(wire_dfs_write_lease(lease)),
        }))
    }

    async fn resolve_write_authority(
        &self,
        request: Request<ResolveDfsWriteAuthorityRequest>,
    ) -> Result<Response<ResolveDfsWriteAuthorityReply>, Status> {
        let authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        validate_caller(authenticated.as_deref(), &request.caller_id)?;
        require_text(&request.owner_session_id, "owner_session_id")?;
        require_text(&request.operation_id, "operation_id")?;
        require_text(&request.inode_id, "inode_id")?;
        let (inode, lease) = dfs_service(&self.0)?
            .resolve_write_authority(
                request.caller_id,
                request.owner_session_id,
                crate::dfs::OperationId::new(request.operation_id),
                crate::dfs::InodeId::new(request.inode_id),
                request.lease_seconds,
            )
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(ResolveDfsWriteAuthorityReply {
            inode: Some(wire_dfs_inode(inode)),
            write_lease: Some(wire_dfs_write_lease(lease)),
        }))
    }

    async fn renew_write_lease(
        &self,
        request: Request<RenewDfsWriteLeaseRequest>,
    ) -> Result<Response<DfsWriteLeaseReply>, Status> {
        let authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        validate_caller(authenticated.as_deref(), &request.caller_id)?;
        require_text(&request.owner_session_id, "owner_session_id")?;
        require_text(&request.operation_id, "operation_id")?;
        let current = domain_dfs_write_lease(
            request
                .current_lease
                .ok_or_else(|| invalid("DFS renew requires current lease"))?,
        );
        let lease = dfs_service(&self.0)?
            .renew_write_lease(
                request.caller_id,
                request.owner_session_id,
                crate::dfs::OperationId::new(request.operation_id),
                current,
                request.lease_seconds,
            )
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(DfsWriteLeaseReply {
            write_lease: Some(wire_dfs_write_lease(lease)),
        }))
    }

    async fn get_inode(
        &self,
        request: Request<GetDfsInodeRequest>,
    ) -> Result<Response<GetDfsInodeReply>, Status> {
        require_text(&request.get_ref().inode_id, "inode_id")?;
        let inode = dfs_service(&self.0)?
            .get_inode(crate::dfs::InodeId::new(request.into_inner().inode_id))
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(GetDfsInodeReply {
            inode: Some(wire_dfs_inode(inode)),
        }))
    }

    async fn get_file_version(
        &self,
        request: Request<GetFileVersionRequest>,
    ) -> Result<Response<GetFileVersionReply>, Status> {
        require_text(&request.get_ref().version_id, "version_id")?;
        let (version, layout) = dfs_service(&self.0)?
            .get_file_version(crate::dfs::FileVersionId::new(
                request.into_inner().version_id,
            ))
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(GetFileVersionReply {
            version: Some(wire_dfs_version(version)),
            layout: Some(wire_dfs_layout(layout)),
        }))
    }

    async fn get_placement_snapshot(
        &self,
        request: Request<GetDfsPlacementSnapshotRequest>,
    ) -> Result<Response<GetDfsPlacementSnapshotReply>, Status> {
        let authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        validate_caller(authenticated.as_deref(), &request.caller_id)?;
        let snapshot = dfs_service(&self.0)?
            .placement_snapshot(request.caller_id)
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        if snapshot.revision < request.minimum_revision {
            return Err(invalid("placement snapshot is older than minimum_revision"));
        }
        Ok(Response::new(GetDfsPlacementSnapshotReply {
            snapshot: Some(wire_placement_snapshot(snapshot)),
        }))
    }

    async fn validate_replica_write(
        &self,
        request: Request<ValidateDfsReplicaWriteRequest>,
    ) -> Result<Response<ValidateDfsReplicaWriteReply>, Status> {
        let authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        validate_caller(authenticated.as_deref(), &request.requester_node_id)?;
        let grant = dfs_service(&self.0)?
            .validate_replica_write(domain_validate_replica_write(request)?)
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(ValidateDfsReplicaWriteReply {
            grant: Some(wire_replica_write_grant(grant)),
        }))
    }

    async fn claim_replication_task(
        &self,
        request: Request<afs_protocol::meta::ClaimDfsReplicationTaskRequest>,
    ) -> Result<Response<afs_protocol::meta::ClaimDfsReplicationTaskReply>, Status> {
        let authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        validate_caller(authenticated.as_deref(), &request.caller_id)?;
        require_text(&request.caller_session_id, "caller_session_id")?;
        require_text(&request.operation_id, "operation_id")?;
        let claim = dfs_service(&self.0)?
            .claim_replication_task(crate::dfs::ClaimReplicationTask {
                caller_id: request.caller_id,
                caller_session_id: request.caller_session_id,
                caller_node_epoch: request.caller_node_epoch,
                operation_id: crate::dfs::OperationId::new(request.operation_id),
                lease_seconds: request.lease_seconds,
            })
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(
            afs_protocol::meta::ClaimDfsReplicationTaskReply {
                claimed: claim.is_some(),
                claim: claim.map(wire_replication_claim),
            },
        ))
    }

    async fn report_replication_task(
        &self,
        request: Request<afs_protocol::meta::ReportDfsReplicationTaskRequest>,
    ) -> Result<Response<afs_protocol::meta::ReportDfsReplicationTaskReply>, Status> {
        let authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        validate_caller(authenticated.as_deref(), &request.caller_id)?;
        require_text(&request.caller_session_id, "caller_session_id")?;
        require_text(&request.operation_id, "operation_id")?;
        let durable_acks = request
            .durable_acks
            .into_iter()
            .map(domain_replica_ack)
            .collect::<Result<Vec<_>, Status>>()?;
        let task = dfs_service(&self.0)?
            .report_replication_task(crate::dfs::ReportReplicationTask {
                caller_id: request.caller_id,
                caller_session_id: request.caller_session_id,
                caller_node_epoch: request.caller_node_epoch,
                operation_id: crate::dfs::OperationId::new(request.operation_id),
                claim: domain_replication_claim(
                    request
                        .claim
                        .ok_or_else(|| invalid("DFS replication report requires claim"))?,
                )?,
                durable_acks,
                error: request.error,
                source_invalid: request.source_invalid,
            })
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(
            afs_protocol::meta::ReportDfsReplicationTaskReply {
                task: Some(wire_replication_task(task)),
            },
        ))
    }

    async fn report_chunk_corruption(
        &self,
        request: Request<afs_protocol::meta::ReportDfsChunkCorruptionRequest>,
    ) -> Result<Response<afs_protocol::meta::ReportDfsChunkCorruptionReply>, Status> {
        let authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        validate_caller(authenticated.as_deref(), &request.caller_id)?;
        require_text(&request.caller_session_id, "caller_session_id")?;
        require_text(&request.operation_id, "operation_id")?;
        require_text(&request.chunk_id, "chunk_id")?;
        require_text(&request.device_id, "device_id")?;
        dfs_service(&self.0)?
            .report_chunk_corruption(crate::dfs::ReportChunkCorruption {
                caller_id: request.caller_id,
                caller_session_id: request.caller_session_id,
                caller_node_epoch: request.caller_node_epoch,
                operation_id: crate::dfs::OperationId::new(request.operation_id),
                chunk_id: crate::dfs::ChunkId::new(request.chunk_id),
                device_id: request.device_id,
                device_epoch: request.device_epoch,
                catalog_revision: request.catalog_revision,
            })
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(
            afs_protocol::meta::ReportDfsChunkCorruptionReply {},
        ))
    }

    async fn validate_read_grants(
        &self,
        request: Request<afs_protocol::meta::ValidateDfsReadGrantsRequest>,
    ) -> Result<Response<afs_protocol::meta::ValidateDfsReadGrantsReply>, Status> {
        let authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        validate_caller(authenticated.as_deref(), &request.receiver_node_id)?;
        let validations = request
            .validations
            .into_iter()
            .map(|validation| {
                let grant = validation
                    .grant
                    .ok_or_else(|| invalid("read validation grant is missing"))?;
                Ok(crate::dfs::DfsReadValidation {
                    grant: crate::dfs::DfsReadGrant {
                        namespace_id: crate::dfs::NamespaceId::new(grant.namespace_id),
                        file_version_id: crate::dfs::FileVersionId::new(grant.file_version_id),
                        layout_root_id: crate::dfs::LayoutRootId::new(grant.layout_root_id),
                        caller_node_id: grant.caller_node_id,
                        caller_node_epoch: grant.caller_node_epoch,
                        expires_at_unix_ms: grant.expires_at_unix_ms,
                        fence: grant.fence,
                        token: grant.token,
                    },
                    chunk_id: crate::dfs::ChunkId::new(validation.chunk_id),
                    copy_id: crate::dfs::CopyId::new(validation.copy_id),
                    chunk_offset: validation.chunk_offset,
                    length: validation.length,
                })
            })
            .collect::<Result<Vec<_>, Status>>()?;
        let authorized = dfs_service(&self.0)?
            .validate_read_grants(crate::dfs::ValidateDfsReadGrants {
                receiver_node_id: request.receiver_node_id,
                receiver_node_epoch: request.receiver_node_epoch,
                peer_node_id: request.peer_node_id,
                validations,
            })
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(
            afs_protocol::meta::ValidateDfsReadGrantsReply {
                authorizations: authorized
                    .into_iter()
                    .map(|entry| {
                        let validation = entry.validation;
                        afs_protocol::meta::DfsAuthorizedRead {
                            validation: Some(afs_protocol::meta::DfsReadValidation {
                                grant: Some(wire_read_grant(validation.grant)),
                                chunk_id: validation.chunk_id.0,
                                copy_id: validation.copy_id.0,
                                chunk_offset: validation.chunk_offset,
                                length: validation.length,
                            }),
                            allowed_ranges: entry
                                .allowed_ranges
                                .into_iter()
                                .map(|(offset, length)| {
                                    afs_protocol::meta::dfs_authorized_read::Range {
                                        offset,
                                        length,
                                    }
                                })
                                .collect(),
                            expires_at_unix_ms: entry.expires_at_unix_ms,
                        }
                    })
                    .collect(),
            },
        ))
    }

    async fn get_chunk_sources(
        &self,
        request: Request<GetDfsChunkSourcesRequest>,
    ) -> Result<Response<GetDfsChunkSourcesReply>, Status> {
        let authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        validate_caller(authenticated.as_deref(), &request.caller_id)?;
        require_text(&request.namespace_id, "namespace_id")?;
        require_text(&request.file_version_id, "file_version_id")?;
        require_text(&request.layout_root_id, "layout_root_id")?;
        let reply = dfs_service(&self.0)?
            .chunk_sources(crate::dfs::DfsChunkSourcesRequest {
                caller_id: request.caller_id,
                namespace_id: crate::dfs::NamespaceId::new(request.namespace_id),
                file_version_id: crate::dfs::FileVersionId::new(request.file_version_id),
                layout_root_id: crate::dfs::LayoutRootId::new(request.layout_root_id),
                chunk_ids: request
                    .chunk_ids
                    .into_iter()
                    .map(crate::dfs::ChunkId::new)
                    .collect(),
            })
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(wire_chunk_sources_reply(reply)))
    }

    async fn sync_inode_metadata(
        &self,
        request: Request<SyncDfsInodeMetadataRequest>,
    ) -> Result<Response<SyncDfsInodeMetadataReply>, Status> {
        let authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        validate_caller(authenticated.as_deref(), &request.caller_id)?;
        require_text(&request.operation_id, "operation_id")?;
        require_text(&request.inode_id, "inode_id")?;
        let inode = dfs_service(&self.0)?
            .sync_inode_metadata(
                request.caller_id,
                crate::dfs::SyncInodeMetadata {
                    operation_id: crate::dfs::OperationId::new(request.operation_id),
                    inode_id: crate::dfs::InodeId::new(request.inode_id),
                    write_lease: domain_dfs_write_lease(
                        request
                            .write_lease
                            .ok_or_else(|| invalid("DFS metadata sync requires write lease"))?,
                    ),
                    expected_inode_revision: request.expected_inode_revision,
                    expected_head_version: optional_id(request.expected_head_version_id)
                        .map(crate::dfs::FileVersionId::new),
                    metadata_delta: domain_dfs_metadata_delta(
                        request
                            .metadata_delta
                            .ok_or_else(|| invalid("DFS metadata sync requires metadata delta"))?,
                    )?,
                },
            )
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(SyncDfsInodeMetadataReply {
            inode: Some(wire_dfs_inode(inode)),
        }))
    }

    async fn commit_file_version(
        &self,
        request: Request<CommitFileVersionRequest>,
    ) -> Result<Response<CommitFileVersionReply>, Status> {
        let authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        validate_caller(authenticated.as_deref(), &request.caller_id)?;
        require_text(&request.operation_id, "operation_id")?;
        require_text(&request.inode_id, "inode_id")?;
        let version = domain_dfs_version(
            request
                .version
                .ok_or_else(|| invalid("DFS commit requires FileVersion"))?,
        );
        let layout = domain_dfs_layout(
            request
                .layout
                .ok_or_else(|| invalid("DFS commit requires LayoutRoot"))?,
        );
        let receipts = request
            .chunk_receipts
            .into_iter()
            .map(domain_chunk_receipt)
            .collect::<Result<Vec<_>, _>>()?;
        let write_lease = domain_dfs_write_lease(
            request
                .write_lease
                .ok_or_else(|| invalid("DFS commit requires write lease"))?,
        );
        let metadata_delta = domain_dfs_metadata_delta(
            request
                .metadata_delta
                .ok_or_else(|| invalid("DFS commit requires metadata delta"))?,
        )?;
        let inode = dfs_service(&self.0)?
            .commit_file_version(
                request.caller_id,
                crate::dfs::CommitFileVersion {
                    operation_id: crate::dfs::OperationId::new(request.operation_id),
                    inode_id: crate::dfs::InodeId::new(request.inode_id),
                    write_lease,
                    expected_inode_revision: request.expected_inode_revision,
                    expected_head_version: optional_id(request.expected_head_version_id)
                        .map(crate::dfs::FileVersionId::new),
                    file_version: version,
                    layout_root: layout,
                    chunk_receipts: receipts,
                    metadata_delta,
                },
            )
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(CommitFileVersionReply {
            inode: Some(wire_dfs_inode(inode)),
        }))
    }
}

fn dfs_service(meta: &super::Meta) -> Result<&super::dfs::DfsService, Status> {
    meta.dfs.as_ref().ok_or_else(|| {
        afs_transport::grpc::error_status::error_to_status(super::store::unavailable_meta_store())
    })
}

fn wire_dfs_dentry(entry: crate::dfs::DentryRecord) -> PbDfsDentryRecord {
    PbDfsDentryRecord {
        name: entry.name,
        inode: Some(wire_dfs_inode(entry.inode)),
    }
}

fn wire_dfs_inode(inode: crate::dfs::InodeRecord) -> PbDfsInodeRecord {
    PbDfsInodeRecord {
        namespace_id: inode.namespace_id.0,
        inode_id: inode.inode_id.0,
        kind: match inode.kind {
            crate::dfs::InodeKind::Regular => PbDfsInodeKind::Regular as i32,
            crate::dfs::InodeKind::Directory => PbDfsInodeKind::Directory as i32,
            crate::dfs::InodeKind::Symlink => PbDfsInodeKind::Symlink as i32,
            crate::dfs::InodeKind::Special(_) => PbDfsInodeKind::Special as i32,
        },
        attributes: Some(PbDfsInodeAttributes {
            mode: inode.attributes.mode,
            uid: inode.attributes.uid,
            gid: inode.attributes.gid,
            nlink: inode.attributes.nlink,
            atime_unix_ms: inode.attributes.atime_unix_ms,
            mtime_unix_ms: inode.attributes.mtime_unix_ms,
            ctime_unix_ms: inode.attributes.ctime_unix_ms,
        }),
        head_version_id: inode.head_version.map_or_else(String::new, |id| id.0),
        revision: inode.revision,
        symlink_target: inode.symlink_target.unwrap_or_default(),
        xattrs: inode
            .xattrs
            .into_iter()
            .map(|(name, value)| PbDfsXattrRecord { name, value })
            .collect(),
        special_node: match inode.kind {
            crate::dfs::InodeKind::Special(kind) => Some(wire_dfs_special_node(kind)),
            _ => None,
        },
    }
}

fn domain_dfs_special_node(
    special: PbDfsSpecialNode,
) -> Result<crate::dfs::SpecialNodeKind, Status> {
    match PbDfsSpecialNodeKind::try_from(special.kind) {
        Ok(PbDfsSpecialNodeKind::Fifo) if special.rdev == 0 => {
            Ok(crate::dfs::SpecialNodeKind::Fifo)
        }
        Ok(PbDfsSpecialNodeKind::Socket) if special.rdev == 0 => {
            Ok(crate::dfs::SpecialNodeKind::Socket)
        }
        Ok(PbDfsSpecialNodeKind::BlockDevice) => {
            Ok(crate::dfs::SpecialNodeKind::BlockDevice { rdev: special.rdev })
        }
        Ok(PbDfsSpecialNodeKind::CharDevice) => {
            Ok(crate::dfs::SpecialNodeKind::CharDevice { rdev: special.rdev })
        }
        _ => Err(invalid(
            "DFS special node has an invalid kind/rdev combination",
        )),
    }
}

fn wire_dfs_special_node(kind: crate::dfs::SpecialNodeKind) -> PbDfsSpecialNode {
    let (kind, rdev) = match kind {
        crate::dfs::SpecialNodeKind::Fifo => (PbDfsSpecialNodeKind::Fifo, 0),
        crate::dfs::SpecialNodeKind::Socket => (PbDfsSpecialNodeKind::Socket, 0),
        crate::dfs::SpecialNodeKind::BlockDevice { rdev } => {
            (PbDfsSpecialNodeKind::BlockDevice, rdev)
        }
        crate::dfs::SpecialNodeKind::CharDevice { rdev } => {
            (PbDfsSpecialNodeKind::CharDevice, rdev)
        }
    };
    PbDfsSpecialNode {
        kind: kind.into(),
        rdev,
    }
}

fn domain_caller_context(caller: PbDfsCallerContext) -> crate::dfs::CallerContext {
    crate::dfs::CallerContext {
        uid: caller.uid,
        gid: caller.gid,
        supplementary_gids: caller.supplementary_gids,
    }
}

fn domain_inode_attr_update(update: PbDfsInodeAttributeUpdate) -> crate::dfs::InodeAttrUpdate {
    crate::dfs::InodeAttrUpdate {
        mode: update.mode,
        uid: update.uid,
        gid: update.gid,
        atime_unix_ms: update.atime_unix_ms,
        mtime_unix_ms: update.mtime_unix_ms,
        ctime_unix_ms: update.ctime_unix_ms,
        timestamps_now: update.timestamps_now,
    }
}

fn domain_dfs_attributes(attributes: PbDfsInodeAttributes) -> crate::dfs::InodeAttributes {
    crate::dfs::InodeAttributes {
        mode: attributes.mode,
        uid: attributes.uid,
        gid: attributes.gid,
        nlink: attributes.nlink,
        atime_unix_ms: attributes.atime_unix_ms,
        mtime_unix_ms: attributes.mtime_unix_ms,
        ctime_unix_ms: attributes.ctime_unix_ms,
    }
}

fn wire_dfs_version(version: crate::dfs::FileVersion) -> afs_protocol::meta::DfsFileVersion {
    afs_protocol::meta::DfsFileVersion {
        version_id: version.id.0,
        inode_id: version.inode_id.0,
        parent_version_id: version.parent_version.map_or_else(String::new, |id| id.0),
        length: version.length,
        layout_root_id: version.layout_root.0,
        created_at_unix_ms: version.created_at_unix_ms,
    }
}

fn domain_dfs_version(version: afs_protocol::meta::DfsFileVersion) -> crate::dfs::FileVersion {
    crate::dfs::FileVersion {
        id: crate::dfs::FileVersionId::new(version.version_id),
        inode_id: crate::dfs::InodeId::new(version.inode_id),
        parent_version: optional_id(version.parent_version_id).map(crate::dfs::FileVersionId::new),
        length: version.length,
        layout_root: crate::dfs::LayoutRootId::new(version.layout_root_id),
        created_at_unix_ms: version.created_at_unix_ms,
    }
}

fn wire_dfs_layout(layout: crate::dfs::LayoutRoot) -> PbDfsLayoutRoot {
    PbDfsLayoutRoot {
        layout_root_id: layout.id.0,
        file_length: layout.file_length,
        inline_extents: layout
            .inline_extents
            .into_iter()
            .map(|extent| afs_protocol::meta::DfsExtent {
                file_offset: extent.file_offset,
                length: extent.length,
                chunk_id: extent.chunk_id.0,
                chunk_offset: extent.chunk_offset,
            })
            .collect(),
    }
}

fn wire_placement_snapshot(
    snapshot: crate::dfs::PlacementSnapshot,
) -> afs_protocol::meta::DfsPlacementSnapshot {
    afs_protocol::meta::DfsPlacementSnapshot {
        revision: snapshot.revision,
        replication: Some(wire_replication_config(snapshot.replication)),
        replica_groups: snapshot
            .replica_groups
            .into_iter()
            .map(wire_replica_group)
            .collect(),
    }
}

fn wire_replication_config(
    replication: crate::dfs::ReplicationConfig,
) -> afs_protocol::meta::DfsReplicationConfig {
    use afs_protocol::meta::DfsLocalCopyPolicy;

    afs_protocol::meta::DfsReplicationConfig {
        desired_copies: u32::from(replication.desired_copies),
        sync_required_copies: u32::from(replication.sync_required_copies),
        min_distinct_nodes: u32::from(replication.min_distinct_nodes),
        min_distinct_failure_domains: u32::from(replication.min_distinct_failure_domains),
        local_copy: match replication.local_copy {
            crate::dfs::LocalCopyPolicy::Required => DfsLocalCopyPolicy::Required as i32,
            crate::dfs::LocalCopyPolicy::Preferred => DfsLocalCopyPolicy::Preferred as i32,
            crate::dfs::LocalCopyPolicy::NotRequired => DfsLocalCopyPolicy::NotRequired as i32,
        },
    }
}

fn wire_replica_group(group: crate::dfs::ReplicaGroup) -> afs_protocol::meta::DfsReplicaGroup {
    afs_protocol::meta::DfsReplicaGroup {
        replica_group_id: group.id.0,
        placement_epoch: group.placement_epoch,
        targets: group.targets.into_iter().map(wire_replica_target).collect(),
    }
}

fn wire_replica_target(target: crate::dfs::ReplicaTarget) -> afs_protocol::meta::DfsReplicaTarget {
    afs_protocol::meta::DfsReplicaTarget {
        node_id: target.node_id,
        node_epoch: target.node_epoch,
        data_endpoint: target.data_endpoint,
        device: Some(afs_protocol::meta::DfsStorageDevice {
            device_id: target.device.device_id,
            device_epoch: target.device.device_epoch,
            catalog_revision: target.device.catalog_revision,
            failure_domain: target.device.failure_domain,
        }),
    }
}

fn wire_replica_write_grant(
    grant: crate::dfs::ReplicaWriteGrant,
) -> afs_protocol::meta::DfsReplicaWriteGrant {
    afs_protocol::meta::DfsReplicaWriteGrant {
        requester_node_id: grant.requester_node_id,
        requester_node_epoch: grant.requester_node_epoch,
        initiator_node_id: grant.initiator_node_id,
        initiator_node_epoch: grant.initiator_node_epoch,
        operation_id: grant.operation_id.0,
        chunk_id: grant.chunk_id.0,
        chunk_length: grant.chunk_length,
        content_digest: grant.content_digest.bytes.to_vec(),
        content_digest_algorithm: match grant.content_digest.algorithm {
            crate::dfs::DigestAlgorithm::Blake3 => {
                afs_protocol::meta::DfsDigestAlgorithm::Blake3 as i32
            }
        },
        placement_revision: grant.placement_revision,
        placement_epoch: grant.placement_epoch,
        replica_group_id: grant.replica_group_id.0,
        target_index: grant.target_index,
        replication: Some(wire_replication_config(grant.replication)),
        replica_group: Some(wire_replica_group(grant.replica_group)),
        expires_at_unix_ms: grant.expires_at_unix_ms,
        fence: grant.fence,
        token: grant.token,
    }
}

fn wire_chunk_object(chunk: crate::dfs::ChunkObject) -> afs_protocol::meta::DfsChunkObject {
    afs_protocol::meta::DfsChunkObject {
        chunk_id: chunk.id.0,
        length: chunk.length,
        content_digest: chunk.content_digest.bytes.to_vec(),
        content_digest_algorithm: match chunk.content_digest.algorithm {
            crate::dfs::DigestAlgorithm::Blake3 => {
                afs_protocol::meta::DfsDigestAlgorithm::Blake3 as i32
            }
        },
    }
}

fn wire_replication_claim(
    claim: crate::dfs::ReplicationClaim,
) -> afs_protocol::meta::DfsReplicationClaim {
    afs_protocol::meta::DfsReplicationClaim {
        task_id: claim.task_id.0,
        operation_id: claim.operation_id.0,
        worker_node_id: claim.worker_node_id,
        worker_node_epoch: claim.worker_node_epoch,
        worker_session_id: claim.worker_session_id,
        expires_at_unix_ms: claim.expires_at_unix_ms,
        fence: claim.fence,
        chunk: Some(wire_chunk_object(claim.chunk)),
        source_copy_id: claim.source_copy_id.0,
        placement_revision: claim.placement_revision,
        replica_group: Some(wire_replica_group(claim.replica_group)),
        replication: Some(wire_replication_config(claim.replication)),
    }
}

fn wire_replication_task(
    task: crate::dfs::ReplicationTask,
) -> afs_protocol::meta::DfsReplicationTask {
    afs_protocol::meta::DfsReplicationTask {
        task_id: task.id.0,
        chunk_id: task.chunk_id.0,
        placement_epoch: task.placement_epoch,
        desired_copies: u32::from(task.desired_copies),
        existing_copy_ids: task
            .existing_copies
            .into_iter()
            .map(|copy| copy.0)
            .collect(),
        state: match task.state {
            crate::dfs::ReplicationTaskState::Pending => {
                afs_protocol::meta::DfsReplicationTaskState::Pending as i32
            }
            crate::dfs::ReplicationTaskState::Running => {
                afs_protocol::meta::DfsReplicationTaskState::Running as i32
            }
            crate::dfs::ReplicationTaskState::RetryWaiting => {
                afs_protocol::meta::DfsReplicationTaskState::RetryWaiting as i32
            }
            crate::dfs::ReplicationTaskState::Completed => {
                afs_protocol::meta::DfsReplicationTaskState::Completed as i32
            }
            crate::dfs::ReplicationTaskState::BlockedNoSource => {
                afs_protocol::meta::DfsReplicationTaskState::BlockedNoSource as i32
            }
        },
        attempt: task.attempt,
        next_retry_unix_ms: task.next_retry_unix_ms,
        last_error: task.last_error,
        claim: task.claim.map(|claim| wire_replication_claim(*claim)),
    }
}

fn wire_chunk_sources_reply(
    reply: crate::dfs::DfsChunkSourcesReply,
) -> afs_protocol::meta::GetDfsChunkSourcesReply {
    afs_protocol::meta::GetDfsChunkSourcesReply {
        revision: reply.revision,
        chunks: reply
            .chunks
            .into_iter()
            .map(|chunk| afs_protocol::meta::DfsChunkSources {
                chunk_id: chunk.chunk_id.0,
                sources: chunk
                    .sources
                    .into_iter()
                    .map(wire_source_candidate)
                    .collect(),
            })
            .collect(),
    }
}

fn wire_source_candidate(
    source: crate::dfs::SourceCandidate,
) -> afs_protocol::meta::DfsSourceCandidate {
    afs_protocol::meta::DfsSourceCandidate {
        copy_id: source.copy_id.0,
        chunk_id: source.chunk_id.0,
        role: match source.role {
            crate::dfs::CopyRole::DurableReplica => {
                afs_protocol::meta::DfsCopyRole::DurableReplica as i32
            }
            crate::dfs::CopyRole::VerifiedCache => {
                afs_protocol::meta::DfsCopyRole::VerifiedCache as i32
            }
            crate::dfs::CopyRole::ExternalCommitted => {
                afs_protocol::meta::DfsCopyRole::ExternalCommitted as i32
            }
        },
        state: match source.state {
            crate::dfs::CopyState::Ready => afs_protocol::meta::DfsCopyState::Ready as i32,
            crate::dfs::CopyState::Corrupt => afs_protocol::meta::DfsCopyState::Corrupt as i32,
            crate::dfs::CopyState::Deleting => afs_protocol::meta::DfsCopyState::Deleting as i32,
            crate::dfs::CopyState::LegacyStaging => {
                afs_protocol::meta::DfsCopyState::Unspecified as i32
            }
        },
        location: Some(wire_copy_location(source.location)),
        data_endpoint: source.data_endpoint,
        load_hint: source.load_hint,
        read_grant: Some(wire_read_grant(source.read_grant)),
    }
}

fn wire_copy_location(location: crate::dfs::CopyLocation) -> afs_protocol::meta::DfsCopyLocation {
    use afs_protocol::meta::dfs_copy_location::{External, Location, Node};

    afs_protocol::meta::DfsCopyLocation {
        location: Some(match location {
            crate::dfs::CopyLocation::Node {
                node_id,
                node_epoch,
                device_id,
                device_epoch,
                catalog_revision,
            } => Location::Node(Node {
                node_id,
                node_epoch,
                device_id,
                device_epoch,
                catalog_revision,
            }),
            crate::dfs::CopyLocation::External {
                store_id,
                object_key,
                object_revision,
            } => Location::External(External {
                store_id,
                object_key,
                object_revision,
            }),
        }),
    }
}

fn wire_read_grant(grant: crate::dfs::DfsReadGrant) -> afs_protocol::meta::DfsReadGrant {
    afs_protocol::meta::DfsReadGrant {
        namespace_id: grant.namespace_id.0,
        file_version_id: grant.file_version_id.0,
        layout_root_id: grant.layout_root_id.0,
        caller_node_id: grant.caller_node_id,
        caller_node_epoch: grant.caller_node_epoch,
        expires_at_unix_ms: grant.expires_at_unix_ms,
        fence: grant.fence,
        token: grant.token,
    }
}

fn domain_dfs_layout(layout: PbDfsLayoutRoot) -> crate::dfs::LayoutRoot {
    crate::dfs::LayoutRoot {
        id: crate::dfs::LayoutRootId::new(layout.layout_root_id),
        file_length: layout.file_length,
        inline_extents: layout
            .inline_extents
            .into_iter()
            .map(|extent| crate::dfs::Extent {
                file_offset: extent.file_offset,
                length: extent.length,
                chunk_id: crate::dfs::ChunkId::new(extent.chunk_id),
                chunk_offset: extent.chunk_offset,
            })
            .collect(),
    }
}

fn wire_dfs_write_lease(lease: crate::dfs::WriteLease) -> afs_protocol::meta::DfsWriteLease {
    afs_protocol::meta::DfsWriteLease {
        inode_id: lease.inode_id.0,
        owner_node_id: lease.owner_node_id,
        owner_session_id: lease.owner_session_id,
        lease_epoch: lease.lease_epoch,
        expires_at_unix_ms: lease.expires_at_unix_ms,
    }
}

fn domain_dfs_write_lease(lease: afs_protocol::meta::DfsWriteLease) -> crate::dfs::WriteLease {
    crate::dfs::WriteLease {
        inode_id: crate::dfs::InodeId::new(lease.inode_id),
        owner_node_id: lease.owner_node_id,
        owner_session_id: lease.owner_session_id,
        lease_epoch: lease.lease_epoch,
        expires_at_unix_ms: lease.expires_at_unix_ms,
    }
}

fn domain_dfs_metadata_delta(
    delta: afs_protocol::meta::DfsCommitMetadataDelta,
) -> Result<crate::dfs::CommitMetadataDelta, Status> {
    let mode = match DfsCommitMetadataMode::try_from(delta.mode)
        .map_err(|_| invalid("DFS commit metadata mode is invalid"))?
    {
        DfsCommitMetadataMode::DataOnly => crate::dfs::CommitMetadataMode::DataOnly,
        DfsCommitMetadataMode::Full => crate::dfs::CommitMetadataMode::Full,
        DfsCommitMetadataMode::Unspecified => {
            return Err(invalid("DFS commit metadata mode is required"));
        }
    };
    Ok(crate::dfs::CommitMetadataDelta {
        mode,
        mtime_unix_ms: (delta.mtime_unix_ms != 0).then_some(delta.mtime_unix_ms),
        ctime_unix_ms: (delta.ctime_unix_ms != 0).then_some(delta.ctime_unix_ms),
        kill_suidgid: delta.kill_suidgid,
    })
}

fn domain_validate_replica_write(
    request: afs_protocol::meta::ValidateDfsReplicaWriteRequest,
) -> Result<crate::dfs::ValidateReplicaWriteRequest, Status> {
    let digest: [u8; 32] = request
        .content_digest
        .try_into()
        .map_err(|_| invalid("DFS replica write digest must contain 32 bytes"))?;
    Ok(crate::dfs::ValidateReplicaWriteRequest {
        requester_node_id: request.requester_node_id,
        requester_node_epoch: request.requester_node_epoch,
        initiator_node_id: request.initiator_node_id,
        initiator_node_epoch: request.initiator_node_epoch,
        operation_id: crate::dfs::OperationId::new(request.operation_id),
        chunk_id: crate::dfs::ChunkId::new(request.chunk_id),
        chunk_length: request.chunk_length,
        content_digest: crate::dfs::ContentDigest {
            algorithm: domain_digest_algorithm(request.content_digest_algorithm)?,
            bytes: digest,
        },
        placement_revision: request.placement_revision,
        placement_epoch: request.placement_epoch,
        replica_group_id: crate::dfs::ReplicaGroupId::new(request.replica_group_id),
        target_index: request.target_index,
        ordered_targets: request
            .ordered_targets
            .into_iter()
            .map(domain_replica_target)
            .collect::<Result<Vec<_>, Status>>()?,
        repair_claim: request
            .repair_claim
            .map(domain_replication_claim)
            .transpose()?,
    })
}

fn domain_chunk_object(
    chunk: afs_protocol::meta::DfsChunkObject,
) -> Result<crate::dfs::ChunkObject, Status> {
    let digest: [u8; 32] = chunk
        .content_digest
        .try_into()
        .map_err(|_| invalid("DFS chunk digest must contain 32 bytes"))?;
    Ok(crate::dfs::ChunkObject {
        id: crate::dfs::ChunkId::new(chunk.chunk_id),
        length: chunk.length,
        content_digest: crate::dfs::ContentDigest {
            algorithm: domain_digest_algorithm(chunk.content_digest_algorithm)?,
            bytes: digest,
        },
        encoding: crate::dfs::ChunkEncoding::Raw,
    })
}

fn domain_replication_config(
    replication: afs_protocol::meta::DfsReplicationConfig,
) -> Result<crate::dfs::ReplicationConfig, Status> {
    let local_copy = match afs_protocol::meta::DfsLocalCopyPolicy::try_from(replication.local_copy)
        .map_err(|_| invalid("DFS replication local-copy policy is invalid"))?
    {
        afs_protocol::meta::DfsLocalCopyPolicy::Required => crate::dfs::LocalCopyPolicy::Required,
        afs_protocol::meta::DfsLocalCopyPolicy::Preferred => crate::dfs::LocalCopyPolicy::Preferred,
        afs_protocol::meta::DfsLocalCopyPolicy::NotRequired => {
            crate::dfs::LocalCopyPolicy::NotRequired
        }
        afs_protocol::meta::DfsLocalCopyPolicy::Unspecified => {
            return Err(invalid("DFS replication local-copy policy is required"));
        }
    };
    let replication = crate::dfs::ReplicationConfig {
        desired_copies: u16::try_from(replication.desired_copies)
            .map_err(|_| invalid("desired_copies exceeds u16"))?,
        sync_required_copies: u16::try_from(replication.sync_required_copies)
            .map_err(|_| invalid("sync_required_copies exceeds u16"))?,
        min_distinct_nodes: u16::try_from(replication.min_distinct_nodes)
            .map_err(|_| invalid("min_distinct_nodes exceeds u16"))?,
        min_distinct_failure_domains: u16::try_from(replication.min_distinct_failure_domains)
            .map_err(|_| invalid("min_distinct_failure_domains exceeds u16"))?,
        local_copy,
    };
    if !replication.is_valid() {
        return Err(invalid("DFS replication config is invalid"));
    }
    Ok(replication)
}

fn domain_replica_group(
    group: afs_protocol::meta::DfsReplicaGroup,
) -> Result<crate::dfs::ReplicaGroup, Status> {
    Ok(crate::dfs::ReplicaGroup {
        id: crate::dfs::ReplicaGroupId::new(group.replica_group_id),
        placement_epoch: group.placement_epoch,
        targets: group
            .targets
            .into_iter()
            .map(domain_replica_target)
            .collect::<Result<Vec<_>, Status>>()?,
    })
}

fn domain_replication_claim(
    claim: afs_protocol::meta::DfsReplicationClaim,
) -> Result<crate::dfs::ReplicationClaim, Status> {
    Ok(crate::dfs::ReplicationClaim {
        task_id: crate::dfs::ReplicationTaskId::new(claim.task_id),
        operation_id: crate::dfs::OperationId::new(claim.operation_id),
        worker_node_id: claim.worker_node_id,
        worker_node_epoch: claim.worker_node_epoch,
        worker_session_id: claim.worker_session_id,
        expires_at_unix_ms: claim.expires_at_unix_ms,
        fence: claim.fence,
        chunk: domain_chunk_object(
            claim
                .chunk
                .ok_or_else(|| invalid("DFS replication claim requires chunk"))?,
        )?,
        source_copy_id: crate::dfs::CopyId::new(claim.source_copy_id),
        placement_revision: claim.placement_revision,
        replica_group: domain_replica_group(
            claim
                .replica_group
                .ok_or_else(|| invalid("DFS replication claim requires replica group"))?,
        )?,
        replication: domain_replication_config(
            claim
                .replication
                .ok_or_else(|| invalid("DFS replication claim requires config"))?,
        )?,
    })
}

fn domain_replica_ack(
    ack: afs_protocol::meta::DfsReplicaAck,
) -> Result<crate::dfs::ReplicaAck, Status> {
    let verified_digest: [u8; 32] = ack
        .verified_digest
        .try_into()
        .map_err(|_| invalid("DFS replica digest must contain 32 bytes"))?;
    Ok(crate::dfs::ReplicaAck {
        operation_id: crate::dfs::OperationId::new(ack.operation_id),
        chunk_id: crate::dfs::ChunkId::new(ack.chunk_id),
        placement_revision: ack.placement_revision,
        placement_epoch: ack.placement_epoch,
        node_id: ack.node_id,
        node_epoch: ack.node_epoch,
        device_id: ack.device_id,
        device_epoch: ack.device_epoch,
        catalog_revision: ack.catalog_revision,
        persisted_bytes: ack.persisted_bytes,
        verified_digest: crate::dfs::ContentDigest {
            algorithm: domain_digest_algorithm(ack.verified_digest_algorithm)?,
            bytes: verified_digest,
        },
    })
}

fn domain_replica_target(
    target: afs_protocol::meta::DfsReplicaTarget,
) -> Result<crate::dfs::ReplicaTarget, Status> {
    let device = target
        .device
        .ok_or_else(|| invalid("DfsReplicaTarget.device is required"))?;
    Ok(crate::dfs::ReplicaTarget {
        node_id: target.node_id,
        node_epoch: target.node_epoch,
        data_endpoint: target.data_endpoint,
        device: crate::dfs::StorageDeviceDescriptor {
            device_id: device.device_id,
            device_epoch: device.device_epoch,
            catalog_revision: device.catalog_revision,
            failure_domain: device.failure_domain,
        },
    })
}

fn domain_chunk_receipt(
    receipt: afs_protocol::meta::DfsChunkReceipt,
) -> Result<crate::dfs::ChunkReceipt, Status> {
    let digest: [u8; 32] = receipt
        .content_digest
        .try_into()
        .map_err(|_| invalid("DFS chunk digest must contain 32 bytes"))?;
    let digest = crate::dfs::ContentDigest {
        algorithm: domain_digest_algorithm(receipt.content_digest_algorithm)?,
        bytes: digest,
    };
    let chunk_id = crate::dfs::ChunkId::new(receipt.chunk_id);
    let durable_acks = receipt
        .durable_acks
        .into_iter()
        .map(domain_replica_ack)
        .collect::<Result<Vec<_>, Status>>()?;
    Ok(crate::dfs::ChunkReceipt {
        operation_id: crate::dfs::OperationId::new(receipt.operation_id),
        chunk: crate::dfs::ChunkObject {
            id: chunk_id.clone(),
            length: receipt.chunk_length,
            content_digest: digest.clone(),
            encoding: crate::dfs::ChunkEncoding::Raw,
        },
        placement_revision: receipt.placement_revision,
        placement_epoch: receipt.placement_epoch,
        replica_group_id: crate::dfs::ReplicaGroupId::new(receipt.replica_group_id),
        durable_acks,
    })
}

fn domain_digest_algorithm(value: i32) -> Result<crate::dfs::DigestAlgorithm, Status> {
    match afs_protocol::meta::DfsDigestAlgorithm::try_from(value)
        .map_err(|_| invalid("DFS digest algorithm is invalid"))?
    {
        afs_protocol::meta::DfsDigestAlgorithm::Blake3 => Ok(crate::dfs::DigestAlgorithm::Blake3),
        afs_protocol::meta::DfsDigestAlgorithm::Unspecified => {
            Err(invalid("DFS digest algorithm is required"))
        }
    }
}

fn optional_id(value: String) -> Option<String> {
    (!value.is_empty()).then_some(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::Code;

    fn presented_access() -> PresentedRootAccess {
        PresentedRootAccess {
            root_id: "workspace-a".into(),
            root_epoch: 1,
            home_node_id: "node-a".into(),
            holder_node_id: "node-b".into(),
            session_id: "session-b".into(),
            access_generation: 1,
            fencing_token: "fence:workspace-a:session-b:1".into(),
            home_session_id: "session-a".into(),
        }
    }

    #[test]
    fn validate_root_access_binds_meta_tls_to_validator_not_presented_holder() {
        let presented = presented_access();
        validate_root_access_identities(
            Some("node-a"),
            "node-b",
            "node-a",
            "session-a",
            &presented,
        )
        .expect("Home A should validate B's grant when P2P observed peer is B");
    }

    #[test]
    fn authority_caller_must_match_authenticated_node() {
        validate_caller(Some("node-a"), "node-a").unwrap();
        assert_eq!(
            validate_caller(Some("node-a"), "node-b")
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );
    }

    #[test]
    fn n2b1_root_command_batch_compaction_maps_typed_recovery_cursor() {
        let reply = root_command_batch_reply_from_domain(
            super::super::owner_roots::RootCommandBatch::Compacted {
                requested_after: StoreRevision(10),
                compacted_to: StoreRevision(20),
                recovery: crate::meta::store::RecoveryCursor {
                    resume_after: StoreRevision(19),
                    reason: RecoveryReason::WatchCompacted,
                },
            },
        );
        let Some(afs_protocol::meta::root_command_batch_reply::Result::Compacted(compacted)) =
            reply.result
        else {
            panic!("expected compacted batch reply");
        };
        assert_eq!(compacted.requested_after, 10);
        assert_eq!(compacted.compacted_to, 20);
        let cursor = compacted.recovery_cursor.expect("recovery cursor");
        assert_eq!(cursor.resume_after, 19);
        assert_eq!(
            cursor.reason,
            RootCommandRecoveryReason::WatchCompacted as i32
        );
    }

    #[test]
    fn n2b1_root_command_batch_unsupported_maps_typed_reply() {
        let reply = root_command_batch_reply_from_domain(
            super::super::owner_roots::RootCommandBatch::Unsupported {
                message: "batch polling unavailable".into(),
            },
        );
        let Some(afs_protocol::meta::root_command_batch_reply::Result::Unsupported(unsupported)) =
            reply.result
        else {
            panic!("expected unsupported batch reply");
        };
        assert_eq!(unsupported.message, "batch polling unavailable");
    }

    #[test]
    fn validate_root_access_rejects_wrong_meta_tls_validator() {
        let presented = presented_access();
        let error = validate_root_access_identities(
            Some("node-b"),
            "node-b",
            "node-a",
            "session-a",
            &presented,
        )
        .unwrap_err();
        assert_eq!(error.code(), Code::PermissionDenied);
    }

    #[test]
    fn validate_root_access_rejects_wrong_observed_peer() {
        let presented = presented_access();
        let error = validate_root_access_identities(
            Some("node-a"),
            "node-x",
            "node-a",
            "session-a",
            &presented,
        )
        .unwrap_err();
        assert_eq!(error.code(), Code::InvalidArgument);
    }
}
