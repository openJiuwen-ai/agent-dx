//! Generated gRPC adapter around the same Meta authority used by REST.
//!
//! OwnerRoots tonic methods authenticate and translate protobuf/domain types;
//! their business transitions live in `owner_roots.rs` and `Meta`; RPC does
//! not own authority state or decide durable commits.
//! Without a store, authority RPCs fail closed instead of fabricating a grant.

use afs_protocol::meta::{
    AbortRootReply, AbortRootRequest, AckRevocationReply, AckRevocationRequest, AcquireRootReply,
    AcquireRootRequest, ActivateRootReply, ActivateRootRequest, CommitFileVersionReply,
    CommitFileVersionRequest, DeactivateRootReply, DeactivateRootRequest,
    DfsCallerContext as PbDfsCallerContext, DfsCommitMetadataMode, DfsCreateReply,
    DfsCreateRequest, DfsDirEntry as PbDfsDirEntry, DfsGetXattrReply, DfsGetXattrRequest,
    DfsInodeAttributeUpdate as PbDfsInodeAttributeUpdate,
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
    ListActiveOutletRootsReply, ListActiveOutletRootsRequest, ListOwnerRootsReply,
    ListOwnerRootsRequest, LookupNodeReply, LookupNodeRequest, LookupRootReply, LookupRootRequest,
    MetaBackendPersistence, NodeDescriptor, NodeEndpoint, OpenDfsWriteReply, OpenDfsWriteRequest,
    PingReply, PingRequest, PollRootCommandBatchRequest, PresentedRootAccess, RecoverRootReply,
    RecoverRootRequest, RegisterNodeReply, RegisterNodeRequest, RenewDfsWriteLeaseRequest,
    ReserveRootReply, ReserveRootRequest, ResolveDfsLockAuthorityReply,
    ResolveDfsLockAuthorityRequest, ResolveDfsWriteAuthorityReply, ResolveDfsWriteAuthorityRequest,
    RootCommand, RootCommandBatchCompacted, RootCommandBatchEvents, RootCommandBatchReply,
    RootCommandBatchUnsupported, RootCommandRecoveryCursor, RootCommandRecoveryReason,
    RootCommandType, RootLocation, RootReservation, RootRight as PbRootRight,
    SyncDfsInodeMetadataReply, SyncDfsInodeMetadataRequest, ValidateDfsReplicaWriteReply,
    ValidateDfsReplicaWriteRequest, ValidateRootAccessReply, ValidateRootAccessRequest,
    WatchRootCommandsRequest, dfs_meta_server::DfsMeta as DfsMetaService,
    meta_server::Meta as MetaService, owner_roots_server::OwnerRoots as OwnerRootsService,
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

mod dfs;
mod dfs_wire;
mod node;
use dfs_wire::*;

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
    let certs = afs_transport::grpc::peer_certificates(request).ok_or_else(|| {
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
        home_nfs_addr: root.nfs_addr,
    }
}

fn wire_node(session: super::store::NodeSession) -> NodeDescriptor {
    NodeDescriptor {
        node_id: session.node_id,
        endpoint: Some(NodeEndpoint {
            grpc_addr: session.grpc_addr,
            data_addr: session.data_addr,
            rest_addr: session.rest_addr,
            nfs_addr: session.nfs_addr,
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
    now_unix_ms: u64,
) -> RegisterNodeReply {
    RegisterNodeReply {
        node_id: session.node_id,
        lease_epoch: session.lease_epoch,
        expires_at_unix_ms: session.expires_at_unix_ms,
        lease_remaining_ms: session.expires_at_unix_ms.saturating_sub(now_unix_ms),
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
        requester_node_id: reservation.requester_node_id,
        requester_session_id: reservation.requester_session_id,
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
                    requester_node_id: reservation.requester_node_id,
                    requester_session_id: reservation.requester_session_id,
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

    async fn deactivate_root(
        &self,
        request: Request<DeactivateRootRequest>,
    ) -> Result<Response<DeactivateRootReply>, Status> {
        let authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        require_text(&request.request_id, "request_id")?;
        require_text(&request.root_id, "root_id")?;
        require_text(&request.home_node_id, "home_node_id")?;
        require_text(&request.home_session_id, "home_session_id")?;
        self.0
            .owner_roots
            .deactivate_root(super::owner_roots::DeactivateRootInput {
                request_id: request.request_id,
                root_id: request.root_id,
                root_epoch: request.root_epoch,
                home_node_id: request.home_node_id,
                home_session_id: request.home_session_id,
                reason: request.reason,
                authenticated_home_node_id: authenticated,
            })
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(DeactivateRootReply {}))
    }

    async fn force_deactivate_root(
        &self,
        request: Request<afs_protocol::meta::ForceDeactivateRootRequest>,
    ) -> Result<Response<DeactivateRootReply>, Status> {
        let authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        require_text(&request.requester_node_id, "requester_node_id")?;
        require_text(&request.requester_session_id, "requester_session_id")?;
        let root = request
            .root
            .ok_or_else(|| Status::invalid_argument("root is required"))?;
        require_text(&root.request_id, "request_id")?;
        require_text(&root.root_id, "root_id")?;
        self.0
            .owner_roots
            .force_deactivate_root(super::owner_roots::ForceDeactivateRootInput {
                root: super::owner_roots::DeactivateRootInput {
                    request_id: root.request_id,
                    root_id: root.root_id,
                    root_epoch: root.root_epoch,
                    home_node_id: root.home_node_id,
                    home_session_id: root.home_session_id,
                    reason: root.reason,
                    authenticated_home_node_id: authenticated,
                },
                requester_node_id: request.requester_node_id,
                requester_session_id: request.requester_session_id,
            })
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(DeactivateRootReply {}))
    }

    async fn lookup_root(
        &self,
        request: Request<LookupRootRequest>,
    ) -> Result<Response<LookupRootReply>, Status> {
        let authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        require_text(&request.root_id, "root_id")?;
        let state = self
            .0
            .owner_roots
            .lookup_root(super::owner_roots::LookupRootInput {
                root_id: request.root_id,
            })
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        let location = state.root.clone().map(wire_location);
        let mut reservation = None;
        if let Some(authenticated) = authenticated {
            let session = self
                .0
                .lookup_node(&authenticated)
                .await
                .map_err(afs_transport::grpc::error_status::error_to_status)?;
            if let Some(candidate) = state.reservation.as_ref() {
                let is_home = candidate.home_node_id == authenticated;
                let is_live_requester = candidate.requester_node_id == authenticated
                    && session.as_ref().is_some_and(|session| {
                        candidate.requester_session_id == session.session_id
                    });
                if is_home || is_live_requester {
                    reservation = Some(wire_reservation(candidate.clone()));
                }
            }
        }
        Ok(Response::new(LookupRootReply {
            found: location.is_some(),
            location,
            reservation,
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

    async fn list_active_outlet_roots(
        &self,
        request: Request<ListActiveOutletRootsRequest>,
    ) -> Result<Response<ListActiveOutletRootsReply>, Status> {
        // 只读快照：任何已认证 live 会话都可以读，不绑定 home 节点。
        // enforce_peer_identity 开启时必须有可信 mTLS 证书（与其它 authority RPC 一致）。
        // 返回值是调用方 node id，本 RPC 不需要它，但身份校验本身必须执行。
        let _caller = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        require_text(&request.request_id, "request_id")?;
        let known_digest = if request.known_digest.is_empty() {
            None
        } else {
            Some(request.known_digest)
        };
        let output = self
            .0
            .owner_roots
            .list_active_outlet_roots(super::owner_roots::ListActiveOutletRootsInput {
                known_digest,
            })
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(ListActiveOutletRootsReply {
            active_roots: output.roots.into_iter().map(wire_location).collect(),
            authority_revision: output.revision.0,
            digest: output.digest,
            unchanged: output.unchanged,
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
        let ownership = self
            .0
            .watch_budget
            .reserve()
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        let commands = tokio::time::timeout_at(
            ownership.deadline,
            self.0
                .owner_roots
                .watch_root_commands(super::owner_roots::WatchRootCommandsInput {
                    node_id: request.node_id,
                    session_id: request.session_id,
                    after_revision: request.after_revision,
                }),
        )
        .await
        .map_err(|_| {
            afs_transport::grpc::error_status::error_to_status(super::watch::unavailable(
                "watch response construction expired",
            ))
        })?
        .map_err(afs_transport::grpc::error_status::error_to_status)?;
        let commands = commands
            .into_iter()
            .map(|command| command_from_record(command.command, command.revision))
            .collect::<Vec<_>>();
        let stream = ownership
            .stream(commands)
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(Box::pin(stream)))
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
        let ownership = self
            .0
            .watch_budget
            .reserve()
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        let batch = tokio::time::timeout_at(
            ownership.deadline,
            self.0.owner_roots.poll_root_command_batch(
                super::owner_roots::WatchRootCommandsInput {
                    node_id: request.node_id,
                    session_id: request.session_id,
                    after_revision: request.after_revision,
                },
            ),
        )
        .await
        .map_err(|_| {
            afs_transport::grpc::error_status::error_to_status(super::watch::unavailable(
                "watch poll construction expired",
            ))
        })?
        .map_err(afs_transport::grpc::error_status::error_to_status)?;
        let reply = root_command_batch_reply_from_domain(batch);
        if let Some(afs_protocol::meta::root_command_batch_reply::Result::Events(events)) =
            &reply.result
        {
            super::watch::wire_charge(&events.commands)
                .map_err(afs_transport::grpc::error_status::error_to_status)?;
        }
        ownership
            .check()
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(reply))
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

#[cfg(test)]
mod tests;
