//! Authorizing inbound DFS replica writes.

use super::*;

/// Maximum staged immutable chunk; independent from individual gRPC frames.
#[cfg(feature = "dfs")]
pub(crate) const MAX_DFS_REPLICA_BYTES: usize = crate::node::chunk::MAX_STAGED_CHUNK_BYTES;

#[cfg(feature = "dfs")]
pub(crate) const DFS_REPLICA_FRAME_BYTES: usize = 64 * 1024;

#[cfg(feature = "dfs")]
pub struct AuthorizedReplicaWrite {
    pub op: crate::node::replication::ReplicaPeerOp,
    pub expires_at_unix_ms: u64,
}

#[cfg(feature = "dfs")]
pub trait DfsReplicaAuthorizer: Send + Sync + 'static {
    fn authorize(
        &self,
        peer: &str,
        header: &afs_protocol::node_data::DfsPutReplicaHeader,
    ) -> afs_error::Result<AuthorizedReplicaWrite>;
}

#[cfg(feature = "dfs")]
pub struct DenyDfsReplicaAuthorizer;

#[cfg(feature = "dfs")]
impl DfsReplicaAuthorizer for DenyDfsReplicaAuthorizer {
    fn authorize(
        &self,
        _: &str,
        _: &afs_protocol::node_data::DfsPutReplicaHeader,
    ) -> afs_error::Result<AuthorizedReplicaWrite> {
        Err(replica_denied("DFS replica authority is not configured"))
    }
}

/// First delivery lane validates each receiving hop with Meta. A reusable,
/// authenticated authority snapshot may later remove that RPC from the hot path.
#[cfg(feature = "dfs")]
pub struct MetaReplicaAuthorizer {
    pub meta: Arc<super::super::meta::GrpcDfsMeta>,
    pub node_id: String,
    pub node_epoch: u64,
}

#[cfg(feature = "dfs")]
impl DfsReplicaAuthorizer for MetaReplicaAuthorizer {
    fn authorize(
        &self,
        peer: &str,
        header: &afs_protocol::node_data::DfsPutReplicaHeader,
    ) -> afs_error::Result<AuthorizedReplicaWrite> {
        use crate::dfs::*;
        let digest = replica_header_digest(header)?;
        let targets = header
            .ordered_targets
            .iter()
            .map(|target| {
                let device = target
                    .device
                    .as_ref()
                    .ok_or_else(|| replica_denied("replica target device is missing"))?;
                Ok(ReplicaTarget {
                    node_id: target.node_id.clone(),
                    node_epoch: target.node_epoch,
                    data_endpoint: target.data_endpoint.clone(),
                    device: StorageDeviceDescriptor {
                        device_id: device.device_id.clone(),
                        device_epoch: device.device_epoch,
                        catalog_revision: device.catalog_revision,
                        failure_domain: device.failure_domain.clone(),
                    },
                })
            })
            .collect::<afs_error::Result<Vec<_>>>()?;
        let index = header.target_index as usize;
        let receiver = targets
            .get(index)
            .ok_or_else(|| replica_denied("replica receiver index is invalid"))?;
        let sender = if index == 0 {
            header.initiator_node_id.as_str()
        } else {
            targets[index - 1].node_id.as_str()
        };
        if peer != sender
            || receiver.node_id != self.node_id
            || receiver.node_epoch != self.node_epoch
        {
            return Err(replica_denied("replica sender or receiver session differs"));
        }
        let request = ValidateReplicaWriteRequest {
            requester_node_id: self.node_id.clone(),
            requester_node_epoch: self.node_epoch,
            initiator_node_id: header.initiator_node_id.clone(),
            initiator_node_epoch: header.initiator_node_epoch,
            operation_id: OperationId::new(header.operation_id.clone()),
            chunk_id: ChunkId::new(header.chunk_id.clone()),
            chunk_length: header.chunk_length,
            content_digest: digest,
            placement_revision: header.placement_revision,
            placement_epoch: header.placement_epoch,
            replica_group_id: ReplicaGroupId::new(header.replica_group_id.clone()),
            target_index: header.target_index,
            ordered_targets: targets,
            repair_claim: header
                .repair_claim
                .clone()
                .map(|claim| super::super::meta::domain_replication_claim(*claim))
                .transpose()?,
        };
        let grant = self.meta.validate_replica_write(request.clone())?;
        if grant.requester_node_id != request.requester_node_id
            || grant.requester_node_epoch != request.requester_node_epoch
            || grant.initiator_node_id != request.initiator_node_id
            || grant.initiator_node_epoch != request.initiator_node_epoch
            || grant.operation_id != request.operation_id
            || grant.chunk_id != request.chunk_id
            || grant.chunk_length != request.chunk_length
            || grant.content_digest != request.content_digest
            || grant.placement_revision != request.placement_revision
            || grant.placement_epoch != request.placement_epoch
            || grant.replica_group_id != request.replica_group_id
            || grant.target_index != request.target_index
            || grant.replica_group.targets != request.ordered_targets
        {
            return Err(replica_denied(
                "Meta replica grant does not bind the precise request",
            ));
        }
        ensure_replica_grant_live(grant.expires_at_unix_ms)?;
        let mut op = crate::node::replication::ReplicaPeerOp::from_grant(&grant)?;
        op.repair_claim = request.repair_claim;
        op.validate_shape()?;
        op.validate_sender(peer)?;
        Ok(AuthorizedReplicaWrite {
            op,
            expires_at_unix_ms: grant.expires_at_unix_ms,
        })
    }
}

#[cfg(feature = "dfs")]
pub(super) fn replica_denied(message: &str) -> afs_error::Error {
    afs_error::Error::coded(afs_error::IO_PERMISSION_DENIED, message)
}

#[cfg(feature = "dfs")]
pub(super) fn ensure_replica_grant_live(expires: u64) -> afs_error::Result<()> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| replica_denied("system clock is before epoch"))?
        .as_millis();
    if expires as u128 <= now {
        return Err(replica_denied("replica grant expired before persistence"));
    }
    Ok(())
}

#[cfg(feature = "dfs")]
pub(super) fn replica_header_digest(
    header: &afs_protocol::node_data::DfsPutReplicaHeader,
) -> afs_error::Result<crate::dfs::ContentDigest> {
    if header.operation_id.is_empty()
        || header.chunk_id.is_empty()
        || header.chunk_length == 0
        || header.chunk_length > MAX_DFS_REPLICA_BYTES as u64
        || header.initiator_node_id.is_empty()
        || header.initiator_node_epoch == 0
        || header.placement_revision == 0
        || header.placement_epoch == 0
    {
        return Err(replica_denied("replica header identity or size is invalid"));
    }
    let bytes: [u8; 32] = header
        .content_digest
        .as_slice()
        .try_into()
        .map_err(|_| replica_denied("replica digest length is invalid"))?;
    Ok(crate::dfs::ContentDigest {
        algorithm: super::super::peer::domain_data_digest(header.content_digest_algorithm)?,
        bytes,
    })
}

#[cfg(feature = "dfs")]
pub(super) fn wire_replica_ack(ack: crate::dfs::ReplicaAck) -> DfsReplicaAck {
    DfsReplicaAck {
        operation_id: ack.operation_id.0,
        chunk_id: ack.chunk_id.0,
        placement_revision: ack.placement_revision,
        placement_epoch: ack.placement_epoch,
        node_id: ack.node_id,
        node_epoch: ack.node_epoch,
        device_id: ack.device_id,
        device_epoch: ack.device_epoch,
        catalog_revision: ack.catalog_revision,
        persisted_bytes: ack.persisted_bytes,
        verified_digest: ack.verified_digest.bytes.to_vec(),
        verified_digest_algorithm: super::super::peer::wire_data_digest(
            ack.verified_digest.algorithm,
        ),
    }
}
