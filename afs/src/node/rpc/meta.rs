//! Node owns its generated Meta caller; common transport only configures it.
//!
//! 调用端直接使用 Proto 生成的 MetaClient，再应用 common/transport/grpc 的配置。
//! 这是 Node 的内部控制 caller，不是对外的本机高性能 client/ SDK。
//! 示例每次显式 ping 建连；未来节点注册/watch 的连接复用由相应业务模块管理。
#[cfg(feature = "ownerfs")]
use super::ownerfs::{reservation_from_wire, reservation_wire};
#[cfg(feature = "ownerfs")]
use crate::node::vfs::ownerfs::{
    catalog::LocalRootRecord,
    root::{
        OwnerRootInventory, PreparedRoot, PresentedRootAccess, RootGrant, RootId, RootLocation,
        RootLookupState, RootMeta, RootReservation, RootRight,
    },
};
#[cfg(any(feature = "ownerfs", feature = "dfs"))]
use afs_error::CLIENT_PROTOCOL_VIOLATION;
use afs_error::{
    CLIENT_ARGUMENT_INVALID, CLIENT_CONNECTION_UNAVAILABLE, CLIENT_DEADLINE_EXCEEDED, Error, Result,
};
#[cfg(feature = "ownerfs")]
use afs_protocol::meta::{
    AbortRootRequest, AcquireRootRequest, ActivateRootRequest, DeactivateRootRequest,
    ListOwnerRootsRequest, LookupNodeRequest, LookupRootRequest, PollRootCommandBatchRequest,
    RecoverRootRequest, ReserveConflictPolicy, ReserveRootRequest, ValidateRootAccessRequest,
    owner_roots_client::OwnerRootsClient,
};
use afs_protocol::meta::{
    MetaBackendPersistence, PingRequest, RegisterNodeReply, RegisterNodeRequest,
    meta_client::MetaClient,
};
use afs_transport::grpc::{GrpcConfig, SecurityManager, TlsConfig};
use std::error::Error as StdError;
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(feature = "dfs")]
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
#[cfg(feature = "ownerfs")]
use tonic::transport::Channel;
use tonic::transport::Endpoint;

#[cfg(test)]
mod ping_tls_tests;

#[cfg(test)]
mod registration_lease_tests;

#[cfg(test)]
mod registration_connect_tests;

#[cfg(feature = "dfs")]
mod dfs;
#[cfg(feature = "dfs")]
mod dfs_authorities;
#[cfg(feature = "dfs")]
mod dfs_gc;
#[cfg(feature = "dfs")]
mod dfs_namespace;
#[cfg(feature = "dfs")]
mod dfs_wire;
mod registration;
#[cfg(feature = "dfs")]
pub use dfs::GrpcDfsMeta;
#[cfg(feature = "dfs")]
use dfs_wire::*;
#[cfg(feature = "dfs")]
pub(crate) use dfs_wire::{domain_replication_claim, wire_replication_claim};
#[cfg(test)]
use registration::*;
pub use registration::{
    MetaPersistenceCapability, NodeRegistrationReadiness, ping, register_node,
    register_node_with_readiness, registration_readiness_from_reply,
};

/// 同步 VFS/FUSE 回调的 Meta adapter。调用方必须在 FUSE 线程或
/// `spawn_blocking` 中，不能在 Tokio worker 上直接调用 RootMeta 方法。
/// 创建/授权/恢复是慢路径；根内操作不经过本模块。
#[cfg(feature = "ownerfs")]
pub struct GrpcRootMeta {
    channel: Channel,
    runtime: tokio::runtime::Handle,
    node_id: String,
    session_id: String,
    timeout: Duration,
    sequence: AtomicU64,
}

#[cfg(feature = "ownerfs")]
impl GrpcRootMeta {
    pub fn new(
        endpoint: &str,
        node_id: String,
        session_id: String,
        timeout: Duration,
        tls: TlsConfig,
    ) -> Result<Self> {
        let security = SecurityManager::new(tls)
            .map_err(|e| Error::coded(CLIENT_ARGUMENT_INVALID, e.to_string()))?;
        let endpoint = GrpcConfig {
            connect_timeout: timeout,
            request_timeout: timeout,
            ..Default::default()
        }
        .configure_client(
            Endpoint::from_shared(security.endpoint_uri(endpoint))
                .map_err(|e| Error::coded(CLIENT_ARGUMENT_INVALID, e.to_string()))?,
        );
        let channel = security
            .connect_lazy(endpoint)
            .map_err(|e| Error::coded(CLIENT_ARGUMENT_INVALID, e.to_string()))?;
        Ok(Self {
            channel,
            runtime: tokio::runtime::Handle::current(),
            node_id,
            session_id,
            timeout,
            sequence: AtomicU64::new(1),
        })
    }

    fn request_id(&self) -> String {
        crate::meta::store::timed_request_id(format!(
            "{}-{}",
            self.session_id,
            self.sequence.fetch_add(1, Ordering::Relaxed)
        ))
    }

    fn client(&self) -> OwnerRootsClient<Channel> {
        OwnerRootsClient::new(self.channel.clone())
    }

    pub(crate) fn poll_root_command_batch(&self, after_revision: u64) -> Result<RootCommandBatch> {
        let mut client = self.client();
        let reply = self
            .run(client.poll_root_command_batch(PollRootCommandBatchRequest {
                node_id: self.node_id.clone(),
                session_id: self.session_id.clone(),
                after_revision,
            }))?
            .into_inner();
        root_command_batch_from_wire(after_revision, reply)
    }

    /// 节点位置来自 Meta 注册表，而非调用者在数据请求中自报的地址。
    /// 返回完整 URI，供 OwnerFs 在根授权缓存未命中时建立 P2P 连接。
    pub fn lookup_node_endpoint(&self, node_id: &str) -> Result<String> {
        self.lookup_node_location(node_id)
            .map(|(endpoint, _)| endpoint)
    }

    /// Returns the registered endpoint and Node incarnation for connection reuse.
    pub fn lookup_node_location(&self, node_id: &str) -> Result<(String, u64)> {
        let mut client = MetaClient::new(self.channel.clone());
        let reply = self
            .run(client.lookup_node(LookupNodeRequest {
                request_id: self.request_id(),
                node_id: node_id.to_owned(),
            }))?
            .into_inner();
        if !reply.found {
            return Err(Error::coded(
                afs_error::NODE_VFS_NOT_FOUND,
                "Home Node is not registered",
            ));
        }
        let node_epoch = reply.lease_epoch;
        let node = required(reply.node, "LookupNode.node")?;
        let endpoint = required(node.endpoint, "LookupNode.node.endpoint")?.grpc_addr;
        if endpoint.is_empty() {
            return Err(Error::coded(
                CLIENT_PROTOCOL_VIOLATION,
                "Home Node has no gRPC endpoint",
            ));
        }
        Ok((endpoint, node_epoch))
    }

    fn run<T>(
        &self,
        future: impl std::future::Future<Output = std::result::Result<T, tonic::Status>>,
    ) -> Result<T> {
        crate::node::bridge::block_on(&self.runtime, async {
            tokio::time::timeout(self.timeout, future)
                .await
                .map_err(|_| {
                    Error::coded(
                        afs_error::CLIENT_DEADLINE_EXCEEDED,
                        "Meta control request timed out",
                    )
                })?
                .map_err(afs_transport::grpc::error_status::status_to_error)
        })
    }
}

#[cfg(any(feature = "ownerfs", feature = "dfs"))]
fn required<T>(value: Option<T>, field: &str) -> Result<T> {
    value.ok_or_else(|| {
        Error::coded(
            CLIENT_PROTOCOL_VIOLATION,
            format!("Meta reply missing {field}"),
        )
    })
}

#[cfg(feature = "ownerfs")]
fn right_wire(right: RootRight) -> i32 {
    use afs_protocol::meta::RootRight as Wire;
    (match right {
        RootRight::Lookup => Wire::Lookup,
        RootRight::Read => Wire::Read,
        RootRight::Write => Wire::Write,
        RootRight::Admin => Wire::Admin,
    }) as i32
}

#[cfg(feature = "ownerfs")]
fn remote_session_rights(requested: RootRight) -> Vec<i32> {
    let mut rights = vec![
        right_wire(RootRight::Lookup),
        right_wire(RootRight::Read),
        right_wire(RootRight::Write),
    ];
    if requested == RootRight::Admin {
        rights.push(right_wire(RootRight::Admin));
    }
    rights
}

#[cfg(feature = "ownerfs")]
fn grant_from_wire(value: afs_protocol::meta::RootAccess) -> Result<RootGrant> {
    use afs_protocol::meta::RootRight as Wire;
    let rights = value
        .rights
        .into_iter()
        .map(|right| match Wire::try_from(right).ok() {
            Some(Wire::Lookup) => Ok(RootRight::Lookup),
            Some(Wire::Read) => Ok(RootRight::Read),
            Some(Wire::Write) => Ok(RootRight::Write),
            Some(Wire::Admin) => Ok(RootRight::Admin),
            _ => Err(Error::coded(
                CLIENT_PROTOCOL_VIOLATION,
                "Meta returned invalid root right",
            )),
        })
        .collect::<Result<Vec<_>>>()?;
    if value.root_id.is_empty() || value.fencing_token.is_empty() || rights.is_empty() {
        return Err(Error::coded(
            CLIENT_PROTOCOL_VIOLATION,
            "Meta returned incomplete root access",
        ));
    }
    Ok(RootGrant {
        id: RootId(value.root_id),
        epoch: value.root_epoch,
        home_node_id: value.home_node_id,
        home_session_id: value.home_session_id,
        holder_node_id: value.holder_node_id,
        session_id: value.session_id,
        access_generation: value.access_generation,
        rights,
        fencing_token: value.fencing_token,
    })
}

#[cfg(feature = "ownerfs")]
impl RootMeta for GrpcRootMeta {
    fn lookup_node_endpoint(&self, node_id: &str) -> Result<String> {
        GrpcRootMeta::lookup_node_endpoint(self, node_id)
    }

    fn reserve_root(&self, id: &RootId, create_intent_id: &str) -> Result<RootReservation> {
        let request = ReserveRootRequest {
            request_id: self.request_id(),
            root_id: id.0.clone(),
            preferred_home_node_id: self.node_id.clone(),
            session_id: self.session_id.clone(),
            rights: [
                RootRight::Lookup,
                RootRight::Read,
                RootRight::Write,
                RootRight::Admin,
            ]
            .map(right_wire)
            .to_vec(),
            expected_root_epoch: 0,
            parent_root_id: String::new(),
            create_intent_id: create_intent_id.to_owned(),
            conflict_policy: ReserveConflictPolicy::ReuseIfSameIntent as i32,
        };
        let mut client = self.client();
        let reply = self.run(client.reserve_root(request))?.into_inner();
        reservation_from_wire(required(reply.reservation, "ReserveRoot.reservation")?)
    }

    fn lookup_root_state(&self, id: &RootId) -> Result<RootLookupState> {
        let mut client = self.client();
        let reply = self
            .run(client.lookup_root(LookupRootRequest {
                request_id: self.request_id(),
                root_id: id.0.clone(),
            }))?
            .into_inner();
        let location = if reply.found {
            let value = required(reply.location, "LookupRoot.location")?;
            Some(RootLocation {
                id: RootId(value.root_id),
                epoch: value.root_epoch,
                home_node_id: value.home_node_id,
                home_session_id: value.home_session_id,
            })
        } else {
            None
        };
        Ok(RootLookupState {
            location,
            reservation: reply.reservation.map(reservation_from_wire).transpose()?,
        })
    }

    fn activate_root(&self, prepared: &PreparedRoot) -> Result<RootGrant> {
        let mut client = self.client();
        let reply = self
            .run(client.activate_root(ActivateRootRequest {
                request_id: self.request_id(),
                reservation: Some(reservation_wire(prepared.reservation())),
                local_prepare_id: prepared.local_prepare_id().to_owned(),
                parent_fsync_generation: prepared.parent_fsync_generation(),
                parent_fsync_complete: true,
            }))?
            .into_inner();
        grant_from_wire(required(reply.access, "ActivateRoot.access")?)
    }

    fn abort_root(&self, reservation: &RootReservation) -> Result<()> {
        let mut client = self.client();
        self.run(client.abort_root(AbortRootRequest {
            request_id: self.request_id(),
            root_id: reservation.id.0.clone(),
            root_epoch: reservation.epoch,
            session_id: reservation.session_id.clone(),
            create_intent_id: reservation.create_intent_id.clone(),
            reason: "local root preparation failed".into(),
            prepare_token: reservation.prepare_token.clone(),
        }))?;
        Ok(())
    }

    fn deactivate_root(
        &self,
        root_id: &RootId,
        epoch: u64,
        home_node_id: &str,
        session_id: &str,
    ) -> Result<()> {
        let mut client = self.client();
        self.run(client.deactivate_root(DeactivateRootRequest {
            request_id: self.request_id(),
            root_id: root_id.0.clone(),
            root_epoch: epoch,
            home_node_id: home_node_id.to_owned(),
            home_session_id: session_id.to_owned(),
            reason: "root directory deleted".into(),
        }))?;
        Ok(())
    }

    fn lookup_root(&self, id: &RootId) -> Result<Option<RootLocation>> {
        let mut client = self.client();
        let reply = self
            .run(client.lookup_root(LookupRootRequest {
                request_id: self.request_id(),
                root_id: id.0.clone(),
            }))?
            .into_inner();
        if !reply.found {
            return Ok(None);
        }
        let value = required(reply.location, "LookupRoot.location")?;
        Ok(Some(RootLocation {
            id: RootId(value.root_id),
            epoch: value.root_epoch,
            home_node_id: value.home_node_id,
            home_session_id: value.home_session_id,
        }))
    }

    fn list_owner_roots(&self, home_node_id: &str) -> Result<OwnerRootInventory> {
        let mut client = self.client();
        let reply = self
            .run(client.list_owner_roots(ListOwnerRootsRequest {
                request_id: self.request_id(),
                home_node_id: home_node_id.to_owned(),
            }))?
            .into_inner();
        Ok(OwnerRootInventory {
            active: reply
                .active_roots
                .into_iter()
                .map(|value| RootLocation {
                    id: RootId(value.root_id),
                    epoch: value.root_epoch,
                    home_node_id: value.home_node_id,
                    home_session_id: value.home_session_id,
                })
                .collect(),
            pending: reply
                .pending_reservations
                .into_iter()
                .map(reservation_from_wire)
                .collect::<Result<Vec<_>>>()?,
        })
    }

    fn acquire_root(&self, id: &RootId, right: RootRight) -> Result<RootGrant> {
        let mut client = self.client();
        let reply = self
            .run(client.acquire_root(AcquireRootRequest {
                request_id: self.request_id(),
                root_id: id.0.clone(),
                requester_node_id: self.node_id.clone(),
                session_id: self.session_id.clone(),
                rights: remote_session_rights(right),
                expected_root_epoch: 0,
                expected_access_generation: 0,
            }))?
            .into_inner();
        grant_from_wire(required(reply.access, "AcquireRoot.access")?)
    }

    fn current_node_session(&self, node_id: &str) -> Result<Option<String>> {
        let mut client = MetaClient::new(self.channel.clone());
        let reply = self
            .run(client.lookup_node(LookupNodeRequest {
                request_id: self.request_id(),
                node_id: node_id.to_owned(),
            }))?
            .into_inner();
        if !reply.found {
            return Ok(None);
        }
        let node = required(reply.node, "LookupNode.node")?;
        if node.session_id.is_empty() {
            return Err(Error::coded(
                CLIENT_PROTOCOL_VIOLATION,
                "LookupNode returned an empty process session",
            ));
        }
        Ok(Some(node.session_id))
    }

    fn validate_root_access(
        &self,
        presented: &PresentedRootAccess,
        authenticated_peer_node_id: &str,
    ) -> Result<RootGrant> {
        // observed_peer_node_id 必须由传输认证层给出，不能照抄请求体里的 holder。
        let mut client = self.client();
        let reply = self
            .run(client.validate_root_access(ValidateRootAccessRequest {
                request_id: self.request_id(),
                presented_access: Some(afs_protocol::meta::PresentedRootAccess {
                    root_id: presented.id.0.clone(),
                    root_epoch: presented.epoch,
                    home_node_id: presented.home_node_id.clone(),
                    holder_node_id: presented.holder_node_id.clone(),
                    session_id: presented.session_id.clone(),
                    access_generation: presented.access_generation,
                    fencing_token: presented.fencing_token.clone(),
                    home_session_id: presented.home_session_id.clone(),
                }),
                observed_peer_node_id: authenticated_peer_node_id.to_owned(),
                validator_home_node_id: self.node_id.clone(),
                validator_home_session_id: self.session_id.clone(),
            }))?
            .into_inner();
        grant_from_wire(required(reply.access, "ValidateRootAccess.access")?)
    }

    fn recover_root(&self, record: &LocalRootRecord, new_session_id: &str) -> Result<RootGrant> {
        let mut client = self.client();
        let reply = self
            .run(client.recover_root(RecoverRootRequest {
                request_id: self.request_id(),
                root_id: record.id.0.clone(),
                expected_root_epoch: record.epoch,
                home_node_id: self.node_id.clone(),
                home_session_id: new_session_id.to_owned(),
                local_prepare_id: record.local_prepare_id.clone(),
            }))?
            .into_inner();
        grant_from_wire(required(reply.access, "RecoverRoot.access")?)
    }
}

#[cfg(feature = "ownerfs")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RootCommandType {
    RevokeAccess,
}

#[cfg(feature = "ownerfs")]
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct RootCommand {
    pub(crate) command_id: String,
    pub(crate) command_type: RootCommandType,
    pub(crate) revision: u64,
    pub(crate) root_id: String,
    pub(crate) root_epoch: u64,
    pub(crate) home_node_id: String,
    pub(crate) home_session_id: String,
    pub(crate) access_generation: u64,
}

#[cfg(feature = "ownerfs")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RootCommandRecoveryReason {
    WatchCompacted,
    NodeSessionRestarted,
    BackendLeaderChanged,
}

#[cfg(feature = "ownerfs")]
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum RootCommandBatch {
    Events {
        start_revision: u64,
        next_revision: u64,
        commands: Vec<RootCommand>,
    },
    Compacted {
        requested_after: u64,
        compacted_to: u64,
        recovery_resume_after: u64,
        recovery_reason: RootCommandRecoveryReason,
    },
    Unsupported {
        message: String,
    },
}

#[cfg(feature = "ownerfs")]
fn root_command_batch_from_wire(
    requested_after: u64,
    value: afs_protocol::meta::RootCommandBatchReply,
) -> Result<RootCommandBatch> {
    match value.result {
        Some(afs_protocol::meta::root_command_batch_reply::Result::Events(events)) => {
            if events.start_revision != requested_after {
                return Err(Error::coded(
                    CLIENT_PROTOCOL_VIOLATION,
                    "RootCommandBatchEvents start revision does not match request",
                ));
            }
            if events.next_revision == 0 || events.next_revision <= events.start_revision {
                return Err(Error::coded(
                    CLIENT_PROTOCOL_VIOLATION,
                    "RootCommandBatchEvents has invalid next revision",
                ));
            }
            let mut seen_ids = std::collections::BTreeSet::new();
            let mut last_revision = events.start_revision;
            let commands = events
                .commands
                .into_iter()
                .map(|command| {
                    if command.command_id.is_empty() || !seen_ids.insert(command.command_id.clone())
                    {
                        return Err(Error::coded(
                            CLIENT_PROTOCOL_VIOLATION,
                            "RootCommand has an empty or duplicate command id",
                        ));
                    }
                    if command.revision < last_revision {
                        return Err(Error::coded(
                            CLIENT_PROTOCOL_VIOLATION,
                            "RootCommand revisions are not ordered",
                        ));
                    }
                    last_revision = command.revision;
                    if command.revision <= events.start_revision
                        || command.revision >= events.next_revision
                    {
                        return Err(Error::coded(
                            CLIENT_PROTOCOL_VIOLATION,
                            "RootCommand revision is outside the batch range",
                        ));
                    }
                    let command_type =
                        afs_protocol::meta::RootCommandType::try_from(command.command_type)
                            .map_err(|_| {
                                Error::coded(
                                    CLIENT_PROTOCOL_VIOLATION,
                                    "RootCommand has an unknown command type",
                                )
                            })?;
                    let command_type = match command_type {
                        afs_protocol::meta::RootCommandType::RevokeAccess => {
                            RootCommandType::RevokeAccess
                        }
                        afs_protocol::meta::RootCommandType::InvalidateCache => {
                            return Err(Error::coded(
                                CLIENT_PROTOCOL_VIOLATION,
                                "RootCommand InvalidateCache is not implemented by OwnerFs",
                            ));
                        }
                        afs_protocol::meta::RootCommandType::Unspecified => {
                            return Err(Error::coded(
                                CLIENT_PROTOCOL_VIOLATION,
                                "RootCommand has unspecified command type",
                            ));
                        }
                    };
                    let access = command.access.ok_or_else(|| {
                        Error::coded(
                            CLIENT_PROTOCOL_VIOLATION,
                            "RootCommand is missing root access",
                        )
                    })?;
                    if access.root_id.is_empty()
                        || access.root_epoch == 0
                        || access.home_node_id.is_empty()
                        || access.home_session_id.is_empty()
                        || access.access_generation == 0
                    {
                        return Err(Error::coded(
                            CLIENT_PROTOCOL_VIOLATION,
                            "RootCommand has invalid root identity",
                        ));
                    }
                    Ok(RootCommand {
                        command_id: command.command_id,
                        command_type,
                        revision: command.revision,
                        root_id: access.root_id,
                        root_epoch: access.root_epoch,
                        home_node_id: access.home_node_id,
                        home_session_id: access.home_session_id,
                        access_generation: access.access_generation,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(RootCommandBatch::Events {
                start_revision: events.start_revision,
                next_revision: events.next_revision,
                commands,
            })
        }
        Some(afs_protocol::meta::root_command_batch_reply::Result::Compacted(compacted)) => {
            let cursor = compacted.recovery_cursor.ok_or_else(|| {
                Error::coded(
                    CLIENT_PROTOCOL_VIOLATION,
                    "RootCommandBatchCompacted is missing recovery cursor",
                )
            })?;
            // This private consumer accepts forward compacted-history diagnostics
            // only. Other recovery shapes stay unsupported, never authoritative.
            if compacted.requested_after != requested_after
                || compacted.compacted_to <= compacted.requested_after
                || cursor.resume_after <= compacted.requested_after
            {
                return Err(Error::coded(
                    CLIENT_PROTOCOL_VIOLATION,
                    "RootCommandBatchCompacted has invalid diagnostic cursor",
                ));
            }
            let recovery_reason =
                match afs_protocol::meta::RootCommandRecoveryReason::try_from(cursor.reason) {
                    Ok(afs_protocol::meta::RootCommandRecoveryReason::WatchCompacted) => {
                        RootCommandRecoveryReason::WatchCompacted
                    }
                    Ok(afs_protocol::meta::RootCommandRecoveryReason::NodeSessionRestarted) => {
                        RootCommandRecoveryReason::NodeSessionRestarted
                    }
                    Ok(afs_protocol::meta::RootCommandRecoveryReason::BackendLeaderChanged) => {
                        RootCommandRecoveryReason::BackendLeaderChanged
                    }
                    Ok(afs_protocol::meta::RootCommandRecoveryReason::Unspecified) | Err(_) => {
                        return Err(Error::coded(
                            CLIENT_PROTOCOL_VIOLATION,
                            "RootCommandBatchCompacted has invalid recovery reason",
                        ));
                    }
                };
            Ok(RootCommandBatch::Compacted {
                requested_after: compacted.requested_after,
                compacted_to: compacted.compacted_to,
                recovery_resume_after: cursor.resume_after,
                recovery_reason,
            })
        }
        Some(afs_protocol::meta::root_command_batch_reply::Result::Unsupported(unsupported)) => {
            Ok(RootCommandBatch::Unsupported {
                message: unsupported.message,
            })
        }
        None => Err(Error::coded(
            CLIENT_PROTOCOL_VIOLATION,
            "RootCommandBatchReply is empty",
        )),
    }
}

#[cfg(all(test, feature = "ownerfs"))]
mod n2b2_root_command_batch_tests {
    use super::*;

    fn access() -> afs_protocol::meta::RootAccess {
        afs_protocol::meta::RootAccess {
            root_id: "root-a".into(),
            root_epoch: 3,
            home_node_id: "home-a".into(),
            holder_node_id: "holder-a".into(),
            session_id: "holder-session-a".into(),
            access_generation: 5,
            rights: vec![afs_protocol::meta::RootRight::Lookup as i32],
            fencing_token: "fence-a".into(),
            home_session_id: "home-session-a".into(),
        }
    }

    #[test]
    fn root_command_batch_adapter_uses_next_minus_one_cursor_and_exact_access_fields() {
        let reply = afs_protocol::meta::RootCommandBatchReply {
            result: Some(
                afs_protocol::meta::root_command_batch_reply::Result::Events(
                    afs_protocol::meta::RootCommandBatchEvents {
                        start_revision: 11,
                        next_revision: 14,
                        commands: vec![afs_protocol::meta::RootCommand {
                            command_id: "cmd-a".into(),
                            command_type: afs_protocol::meta::RootCommandType::RevokeAccess as i32,
                            access: Some(access()),
                            revision: 12,
                        }],
                    },
                ),
            ),
        };

        let batch = root_command_batch_from_wire(11, reply).unwrap();

        assert_eq!(
            batch,
            RootCommandBatch::Events {
                start_revision: 11,
                next_revision: 14,
                commands: vec![RootCommand {
                    command_id: "cmd-a".into(),
                    command_type: RootCommandType::RevokeAccess,
                    revision: 12,
                    root_id: "root-a".into(),
                    root_epoch: 3,
                    home_node_id: "home-a".into(),
                    home_session_id: "home-session-a".into(),
                    access_generation: 5,
                }],
            }
        );
    }

    #[test]
    fn root_command_batch_adapter_compaction_is_non_authorizing() {
        let reply = afs_protocol::meta::RootCommandBatchReply {
            result: Some(
                afs_protocol::meta::root_command_batch_reply::Result::Compacted(
                    afs_protocol::meta::RootCommandBatchCompacted {
                        requested_after: 8,
                        compacted_to: 20,
                        recovery_cursor: Some(afs_protocol::meta::RootCommandRecoveryCursor {
                            resume_after: 20,
                            reason: afs_protocol::meta::RootCommandRecoveryReason::WatchCompacted
                                as i32,
                        }),
                    },
                ),
            ),
        };

        let batch = root_command_batch_from_wire(8, reply).unwrap();

        assert_eq!(
            batch,
            RootCommandBatch::Compacted {
                requested_after: 8,
                compacted_to: 20,
                recovery_resume_after: 20,
                recovery_reason: RootCommandRecoveryReason::WatchCompacted,
            }
        );
    }

    #[test]
    fn root_command_batch_adapter_rejects_malformed_compaction_diagnostics() {
        use afs_protocol::meta::RootCommandRecoveryReason;
        for (requested_after, compacted_to, resume_after, reason) in [
            (8, 20, 19, RootCommandRecoveryReason::Unspecified as i32),
            (8, 20, 19, 99),
            (8, 8, 19, RootCommandRecoveryReason::WatchCompacted as i32),
            (8, 20, 8, RootCommandRecoveryReason::WatchCompacted as i32),
            (8, 20, 0, RootCommandRecoveryReason::WatchCompacted as i32),
        ] {
            let reply = afs_protocol::meta::RootCommandBatchReply {
                result: Some(
                    afs_protocol::meta::root_command_batch_reply::Result::Compacted(
                        afs_protocol::meta::RootCommandBatchCompacted {
                            requested_after,
                            compacted_to,
                            recovery_cursor: Some(afs_protocol::meta::RootCommandRecoveryCursor {
                                resume_after,
                                reason,
                            }),
                        },
                    ),
                ),
            };
            assert_eq!(
                root_command_batch_from_wire(8, reply).unwrap_err().code(),
                CLIENT_PROTOCOL_VIOLATION
            );
        }
    }

    #[test]
    fn root_command_batch_adapter_unsupported_is_non_authorizing() {
        let reply = afs_protocol::meta::RootCommandBatchReply {
            result: Some(
                afs_protocol::meta::root_command_batch_reply::Result::Unsupported(
                    afs_protocol::meta::RootCommandBatchUnsupported {
                        message: "batch polling is disabled".into(),
                    },
                ),
            ),
        };

        let batch = root_command_batch_from_wire(8, reply).unwrap();

        assert_eq!(
            batch,
            RootCommandBatch::Unsupported {
                message: "batch polling is disabled".into(),
            }
        );
    }

    #[test]
    fn root_command_batch_adapter_rejects_empty_and_invalid_event_batches() {
        let error = root_command_batch_from_wire(
            14,
            afs_protocol::meta::RootCommandBatchReply { result: None },
        )
        .expect_err("empty reply must not authorize anything");
        assert_eq!(error.code(), CLIENT_PROTOCOL_VIOLATION);

        let error = root_command_batch_from_wire(
            14,
            afs_protocol::meta::RootCommandBatchReply {
                result: Some(
                    afs_protocol::meta::root_command_batch_reply::Result::Events(
                        afs_protocol::meta::RootCommandBatchEvents {
                            start_revision: 14,
                            next_revision: 14,
                            commands: Vec::new(),
                        },
                    ),
                ),
            },
        )
        .expect_err("non-advancing events cannot produce a durable cursor");
        assert_eq!(error.code(), CLIENT_PROTOCOL_VIOLATION);

        let error = root_command_batch_from_wire(
            14,
            afs_protocol::meta::RootCommandBatchReply {
                result: Some(
                    afs_protocol::meta::root_command_batch_reply::Result::Events(
                        afs_protocol::meta::RootCommandBatchEvents {
                            start_revision: 14,
                            next_revision: 16,
                            commands: vec![afs_protocol::meta::RootCommand {
                                command_id: "already-observed".into(),
                                command_type: afs_protocol::meta::RootCommandType::RevokeAccess
                                    as i32,
                                access: Some(access()),
                                revision: 14,
                            }],
                        },
                    ),
                ),
            },
        )
        .expect_err("start_revision is the exclusive previously observed revision");
        assert_eq!(error.code(), CLIENT_PROTOCOL_VIOLATION);
    }

    #[test]
    fn root_command_batch_adapter_accepts_empty_idle_progress_without_new_command() {
        let reply = afs_protocol::meta::RootCommandBatchReply {
            result: Some(
                afs_protocol::meta::root_command_batch_reply::Result::Events(
                    afs_protocol::meta::RootCommandBatchEvents {
                        start_revision: 14,
                        next_revision: 15,
                        commands: Vec::new(),
                    },
                ),
            ),
        };

        assert_eq!(
            root_command_batch_from_wire(14, reply).unwrap(),
            RootCommandBatch::Events {
                start_revision: 14,
                next_revision: 15,
                commands: Vec::new(),
            }
        );
    }

    #[test]
    fn root_command_batch_adapter_rejects_request_start_mismatch() {
        let reply = afs_protocol::meta::RootCommandBatchReply {
            result: Some(
                afs_protocol::meta::root_command_batch_reply::Result::Events(
                    afs_protocol::meta::RootCommandBatchEvents {
                        start_revision: 15,
                        next_revision: 16,
                        commands: Vec::new(),
                    },
                ),
            ),
        };

        let error = root_command_batch_from_wire(14, reply)
            .expect_err("batch must identify the requested exclusive start");
        assert_eq!(error.code(), CLIENT_PROTOCOL_VIOLATION);
    }

    #[test]
    fn root_command_batch_adapter_rejects_unimplemented_command_type() {
        let reply = afs_protocol::meta::RootCommandBatchReply {
            result: Some(
                afs_protocol::meta::root_command_batch_reply::Result::Events(
                    afs_protocol::meta::RootCommandBatchEvents {
                        start_revision: 11,
                        next_revision: 14,
                        commands: vec![afs_protocol::meta::RootCommand {
                            command_id: "cmd-cache".into(),
                            command_type: afs_protocol::meta::RootCommandType::InvalidateCache
                                as i32,
                            access: Some(access()),
                            revision: 12,
                        }],
                    },
                ),
            ),
        };

        let error = root_command_batch_from_wire(11, reply)
            .expect_err("known but unimplemented commands must fail close");
        assert_eq!(error.code(), CLIENT_PROTOCOL_VIOLATION);
    }

    #[test]
    fn root_command_batch_adapter_rejects_duplicate_or_unordered_commands() {
        let mut first = access();
        first.root_id = "root-a".into();
        let mut second = access();
        second.root_id = "root-b".into();
        let duplicate = afs_protocol::meta::RootCommandBatchReply {
            result: Some(
                afs_protocol::meta::root_command_batch_reply::Result::Events(
                    afs_protocol::meta::RootCommandBatchEvents {
                        start_revision: 11,
                        next_revision: 14,
                        commands: vec![
                            afs_protocol::meta::RootCommand {
                                command_id: "cmd-dup".into(),
                                command_type: afs_protocol::meta::RootCommandType::RevokeAccess
                                    as i32,
                                access: Some(first.clone()),
                                revision: 12,
                            },
                            afs_protocol::meta::RootCommand {
                                command_id: "cmd-dup".into(),
                                command_type: afs_protocol::meta::RootCommandType::RevokeAccess
                                    as i32,
                                access: Some(second.clone()),
                                revision: 12,
                            },
                        ],
                    },
                ),
            ),
        };
        assert_eq!(
            root_command_batch_from_wire(11, duplicate)
                .unwrap_err()
                .code(),
            CLIENT_PROTOCOL_VIOLATION
        );

        let unordered = afs_protocol::meta::RootCommandBatchReply {
            result: Some(
                afs_protocol::meta::root_command_batch_reply::Result::Events(
                    afs_protocol::meta::RootCommandBatchEvents {
                        start_revision: 11,
                        next_revision: 15,
                        commands: vec![
                            afs_protocol::meta::RootCommand {
                                command_id: "cmd-later".into(),
                                command_type: afs_protocol::meta::RootCommandType::RevokeAccess
                                    as i32,
                                access: Some(first),
                                revision: 13,
                            },
                            afs_protocol::meta::RootCommand {
                                command_id: "cmd-earlier".into(),
                                command_type: afs_protocol::meta::RootCommandType::RevokeAccess
                                    as i32,
                                access: Some(second),
                                revision: 12,
                            },
                        ],
                    },
                ),
            ),
        };
        assert_eq!(
            root_command_batch_from_wire(11, unordered)
                .unwrap_err()
                .code(),
            CLIENT_PROTOCOL_VIOLATION
        );
    }

    #[test]
    fn root_command_batch_adapter_accepts_distinct_commands_at_same_revision() {
        let mut first = access();
        first.root_id = "root-a".into();
        let mut second = access();
        second.root_id = "root-b".into();
        let reply = afs_protocol::meta::RootCommandBatchReply {
            result: Some(
                afs_protocol::meta::root_command_batch_reply::Result::Events(
                    afs_protocol::meta::RootCommandBatchEvents {
                        start_revision: 11,
                        next_revision: 14,
                        commands: vec![
                            afs_protocol::meta::RootCommand {
                                command_id: "cmd-a".into(),
                                command_type: afs_protocol::meta::RootCommandType::RevokeAccess
                                    as i32,
                                access: Some(first),
                                revision: 12,
                            },
                            afs_protocol::meta::RootCommand {
                                command_id: "cmd-b".into(),
                                command_type: afs_protocol::meta::RootCommandType::RevokeAccess
                                    as i32,
                                access: Some(second),
                                revision: 12,
                            },
                        ],
                    },
                ),
            ),
        };

        let RootCommandBatch::Events { commands, .. } =
            root_command_batch_from_wire(11, reply).unwrap()
        else {
            panic!("events batch expected");
        };
        assert_eq!(commands.len(), 2);
        assert_eq!(commands[0].revision, commands[1].revision);
    }

    #[test]
    fn root_command_batch_adapter_rejects_zero_home_tuple() {
        let mut invalid = access();
        invalid.root_epoch = 0;
        let reply = afs_protocol::meta::RootCommandBatchReply {
            result: Some(
                afs_protocol::meta::root_command_batch_reply::Result::Events(
                    afs_protocol::meta::RootCommandBatchEvents {
                        start_revision: 11,
                        next_revision: 14,
                        commands: vec![afs_protocol::meta::RootCommand {
                            command_id: "cmd-a".into(),
                            command_type: afs_protocol::meta::RootCommandType::RevokeAccess as i32,
                            access: Some(invalid),
                            revision: 12,
                        }],
                    },
                ),
            ),
        };

        let error = root_command_batch_from_wire(11, reply)
            .expect_err("nonzero root identity is required for revocation");
        assert_eq!(error.code(), CLIENT_PROTOCOL_VIOLATION);
    }
}

#[cfg(all(test, feature = "dfs"))]
mod tests;

#[cfg(all(test, feature = "dfs"))]
mod crc32c_wire_tests;
