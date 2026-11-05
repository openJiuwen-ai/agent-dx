//! DFS wire and domain conversions on the Node side.

use super::*;

#[cfg(feature = "dfs")]
pub(super) fn wire_dfs_caller_context(
    caller: crate::dfs::CallerContext,
) -> afs_protocol::meta::DfsCallerContext {
    afs_protocol::meta::DfsCallerContext {
        uid: caller.uid,
        gid: caller.gid,
        supplementary_gids: caller.supplementary_gids,
    }
}

#[cfg(feature = "dfs")]
pub(super) fn wire_dfs_attr_update(
    update: crate::dfs::InodeAttrUpdate,
) -> afs_protocol::meta::DfsInodeAttributeUpdate {
    afs_protocol::meta::DfsInodeAttributeUpdate {
        mode: update.mode,
        uid: update.uid,
        gid: update.gid,
        atime_unix_ms: update.atime_unix_ms,
        mtime_unix_ms: update.mtime_unix_ms,
        ctime_unix_ms: update.ctime_unix_ms,
        timestamps_now: update.timestamps_now,
    }
}

#[cfg(feature = "dfs")]
pub(super) fn domain_dfs_dentry(
    entry: afs_protocol::meta::DfsDentryRecord,
) -> afs_error::Result<crate::dfs::DentryRecord> {
    Ok(crate::dfs::DentryRecord {
        name: entry.name,
        inode: domain_dfs_inode(required(entry.inode, "DfsDentryRecord.inode")?)?,
    })
}

#[cfg(feature = "dfs")]
pub(super) fn wire_dfs_attributes(
    attributes: crate::dfs::InodeAttributes,
) -> afs_protocol::meta::DfsInodeAttributes {
    afs_protocol::meta::DfsInodeAttributes {
        mode: attributes.mode,
        uid: attributes.uid,
        gid: attributes.gid,
        nlink: attributes.nlink,
        atime_unix_ms: attributes.atime_unix_ms,
        mtime_unix_ms: attributes.mtime_unix_ms,
        ctime_unix_ms: attributes.ctime_unix_ms,
    }
}

#[cfg(feature = "dfs")]
pub(super) fn domain_dfs_inode(
    inode: afs_protocol::meta::DfsInodeRecord,
) -> afs_error::Result<crate::dfs::InodeRecord> {
    let attributes = required(inode.attributes, "DfsInodeRecord.attributes")?;
    let kind = match afs_protocol::meta::DfsInodeKind::try_from(inode.kind) {
        Ok(afs_protocol::meta::DfsInodeKind::Regular) => crate::dfs::InodeKind::Regular,
        Ok(afs_protocol::meta::DfsInodeKind::Directory) => crate::dfs::InodeKind::Directory,
        Ok(afs_protocol::meta::DfsInodeKind::Symlink) => crate::dfs::InodeKind::Symlink,
        Ok(afs_protocol::meta::DfsInodeKind::Special) => crate::dfs::InodeKind::Special(
            domain_dfs_special_node(required(inode.special_node, "DfsInodeRecord.special_node")?)?,
        ),
        _ => {
            return Err(Error::coded(
                afs_error::CLIENT_PROTOCOL_VIOLATION,
                "DfsInodeRecord has an invalid inode kind",
            ));
        }
    };
    Ok(crate::dfs::InodeRecord {
        namespace_id: crate::dfs::NamespaceId::new(inode.namespace_id),
        inode_id: crate::dfs::InodeId::new(inode.inode_id),
        kind,
        attributes: crate::dfs::InodeAttributes {
            mode: attributes.mode,
            uid: attributes.uid,
            gid: attributes.gid,
            nlink: attributes.nlink,
            atime_unix_ms: attributes.atime_unix_ms,
            mtime_unix_ms: attributes.mtime_unix_ms,
            ctime_unix_ms: attributes.ctime_unix_ms,
        },
        head_version: (!inode.head_version_id.is_empty())
            .then(|| crate::dfs::FileVersionId::new(inode.head_version_id)),
        symlink_target: (!inode.symlink_target.is_empty()).then_some(inode.symlink_target),
        xattrs: inode
            .xattrs
            .into_iter()
            .map(|record| (record.name, record.value))
            .collect(),
        revision: inode.revision,
    })
}

#[cfg(feature = "dfs")]
fn domain_dfs_special_node(
    special: afs_protocol::meta::DfsSpecialNode,
) -> afs_error::Result<crate::dfs::SpecialNodeKind> {
    match afs_protocol::meta::DfsSpecialNodeKind::try_from(special.kind) {
        Ok(afs_protocol::meta::DfsSpecialNodeKind::Fifo) if special.rdev == 0 => {
            Ok(crate::dfs::SpecialNodeKind::Fifo)
        }
        Ok(afs_protocol::meta::DfsSpecialNodeKind::Socket) if special.rdev == 0 => {
            Ok(crate::dfs::SpecialNodeKind::Socket)
        }
        Ok(afs_protocol::meta::DfsSpecialNodeKind::BlockDevice) => {
            Ok(crate::dfs::SpecialNodeKind::BlockDevice { rdev: special.rdev })
        }
        Ok(afs_protocol::meta::DfsSpecialNodeKind::CharDevice) => {
            Ok(crate::dfs::SpecialNodeKind::CharDevice { rdev: special.rdev })
        }
        _ => Err(Error::coded(
            afs_error::CLIENT_PROTOCOL_VIOLATION,
            "DfsSpecialNode has an invalid kind/rdev combination",
        )),
    }
}

#[cfg(feature = "dfs")]
pub(super) fn wire_dfs_special_node(
    kind: crate::dfs::SpecialNodeKind,
) -> afs_protocol::meta::DfsSpecialNode {
    let (kind, rdev) = match kind {
        crate::dfs::SpecialNodeKind::Fifo => (afs_protocol::meta::DfsSpecialNodeKind::Fifo, 0),
        crate::dfs::SpecialNodeKind::Socket => (afs_protocol::meta::DfsSpecialNodeKind::Socket, 0),
        crate::dfs::SpecialNodeKind::BlockDevice { rdev } => {
            (afs_protocol::meta::DfsSpecialNodeKind::BlockDevice, rdev)
        }
        crate::dfs::SpecialNodeKind::CharDevice { rdev } => {
            (afs_protocol::meta::DfsSpecialNodeKind::CharDevice, rdev)
        }
    };
    afs_protocol::meta::DfsSpecialNode {
        kind: kind.into(),
        rdev,
    }
}

#[cfg(feature = "dfs")]
pub(super) fn wire_dfs_version(
    version: crate::dfs::FileVersion,
) -> afs_protocol::meta::DfsFileVersion {
    afs_protocol::meta::DfsFileVersion {
        version_id: version.id.0,
        inode_id: version.inode_id.0,
        parent_version_id: version.parent_version.map_or_else(String::new, |id| id.0),
        length: version.length,
        layout_root_id: version.layout_root.0,
        created_at_unix_ms: version.created_at_unix_ms,
    }
}

#[cfg(feature = "dfs")]
pub(super) fn domain_dfs_version(
    version: afs_protocol::meta::DfsFileVersion,
) -> crate::dfs::FileVersion {
    crate::dfs::FileVersion {
        id: crate::dfs::FileVersionId::new(version.version_id),
        inode_id: crate::dfs::InodeId::new(version.inode_id),
        parent_version: (!version.parent_version_id.is_empty())
            .then(|| crate::dfs::FileVersionId::new(version.parent_version_id)),
        length: version.length,
        layout_root: crate::dfs::LayoutRootId::new(version.layout_root_id),
        created_at_unix_ms: version.created_at_unix_ms,
    }
}

#[cfg(feature = "dfs")]
pub(super) fn wire_dfs_layout(layout: crate::dfs::LayoutRoot) -> afs_protocol::meta::DfsLayoutRoot {
    afs_protocol::meta::DfsLayoutRoot {
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

#[cfg(feature = "dfs")]
pub(super) fn domain_dfs_layout(
    layout: afs_protocol::meta::DfsLayoutRoot,
) -> crate::dfs::LayoutRoot {
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

#[cfg(feature = "dfs")]
pub(super) fn wire_dfs_write_lease(
    lease: crate::dfs::WriteLease,
) -> afs_protocol::meta::DfsWriteLease {
    afs_protocol::meta::DfsWriteLease {
        inode_id: lease.inode_id.0,
        owner_node_id: lease.owner_node_id,
        owner_session_id: lease.owner_session_id,
        lease_epoch: lease.lease_epoch,
        expires_at_unix_ms: lease.expires_at_unix_ms,
    }
}

#[cfg(feature = "dfs")]
pub(super) fn domain_dfs_write_lease(
    lease: afs_protocol::meta::DfsWriteLease,
) -> crate::dfs::WriteLease {
    crate::dfs::WriteLease {
        inode_id: crate::dfs::InodeId::new(lease.inode_id),
        owner_node_id: lease.owner_node_id,
        owner_session_id: lease.owner_session_id,
        lease_epoch: lease.lease_epoch,
        expires_at_unix_ms: lease.expires_at_unix_ms,
    }
}

#[cfg(feature = "dfs")]
pub(super) fn wire_dfs_metadata_delta(
    delta: crate::dfs::CommitMetadataDelta,
) -> afs_protocol::meta::DfsCommitMetadataDelta {
    afs_protocol::meta::DfsCommitMetadataDelta {
        mode: match delta.mode {
            crate::dfs::CommitMetadataMode::DataOnly => {
                afs_protocol::meta::DfsCommitMetadataMode::DataOnly as i32
            }
            crate::dfs::CommitMetadataMode::Full => {
                afs_protocol::meta::DfsCommitMetadataMode::Full as i32
            }
        },
        mtime_unix_ms: delta.mtime_unix_ms.unwrap_or_default(),
        ctime_unix_ms: delta.ctime_unix_ms.unwrap_or_default(),
        kill_suidgid: delta.kill_suidgid,
    }
}

#[cfg(feature = "dfs")]
pub(super) fn wire_validate_replica_write_request(
    request: crate::dfs::ValidateReplicaWriteRequest,
) -> afs_protocol::meta::ValidateDfsReplicaWriteRequest {
    afs_protocol::meta::ValidateDfsReplicaWriteRequest {
        requester_node_id: request.requester_node_id,
        requester_node_epoch: request.requester_node_epoch,
        initiator_node_id: request.initiator_node_id,
        initiator_node_epoch: request.initiator_node_epoch,
        operation_id: request.operation_id.0,
        chunk_id: request.chunk_id.0,
        chunk_length: request.chunk_length,
        content_digest: request.content_digest.bytes.to_vec(),
        content_digest_algorithm: wire_digest_algorithm(request.content_digest.algorithm),
        placement_revision: request.placement_revision,
        placement_epoch: request.placement_epoch,
        replica_group_id: request.replica_group_id.0,
        target_index: request.target_index,
        ordered_targets: request
            .ordered_targets
            .into_iter()
            .map(wire_replica_target)
            .collect(),
        repair_claim: request.repair_claim.as_ref().map(wire_replication_claim),
    }
}

#[cfg(feature = "dfs")]
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

#[cfg(feature = "dfs")]
pub(super) fn wire_chunk_receipt(
    receipt: crate::dfs::ChunkReceipt,
) -> afs_protocol::meta::DfsChunkReceipt {
    afs_protocol::meta::DfsChunkReceipt {
        operation_id: receipt.operation_id.0,
        chunk_id: receipt.chunk.id.0,
        chunk_length: receipt.chunk.length,
        content_digest: receipt.chunk.content_digest.bytes.to_vec(),
        content_digest_algorithm: wire_digest_algorithm(receipt.chunk.content_digest.algorithm),
        placement_revision: receipt.placement_revision,
        placement_epoch: receipt.placement_epoch,
        replica_group_id: receipt.replica_group_id.0,
        external_commit: receipt.external_commit.map(|external| {
            afs_protocol::meta::DfsExternalCommitReceipt {
                store_id: external.store_id,
                generation: external.generation,
                object_key: external.object_key,
                persisted_bytes: external.persisted_bytes,
                verified_digest: external.verified_digest.bytes.to_vec(),
                verified_digest_algorithm: wire_digest_algorithm(
                    external.verified_digest.algorithm,
                ),
            }
        }),
        durable_acks: receipt
            .durable_acks
            .into_iter()
            .map(wire_replica_ack)
            .collect(),
    }
}

#[cfg(feature = "dfs")]
pub(super) fn wire_replica_ack(ack: crate::dfs::ReplicaAck) -> afs_protocol::meta::DfsReplicaAck {
    afs_protocol::meta::DfsReplicaAck {
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
        verified_digest_algorithm: wire_digest_algorithm(ack.verified_digest.algorithm),
    }
}

#[cfg(feature = "dfs")]
pub(super) fn wire_replication_config(
    replication: crate::dfs::ReplicationConfig,
) -> afs_protocol::meta::DfsReplicationConfig {
    afs_protocol::meta::DfsReplicationConfig {
        desired_copies: u32::from(replication.desired_copies),
        sync_required_copies: u32::from(replication.sync_required_copies),
        min_distinct_nodes: u32::from(replication.min_distinct_nodes),
        min_distinct_failure_domains: u32::from(replication.min_distinct_failure_domains),
        local_copy: match replication.local_copy {
            crate::dfs::LocalCopyPolicy::Required => {
                afs_protocol::meta::DfsLocalCopyPolicy::Required as i32
            }
            crate::dfs::LocalCopyPolicy::Preferred => {
                afs_protocol::meta::DfsLocalCopyPolicy::Preferred as i32
            }
            crate::dfs::LocalCopyPolicy::NotRequired => {
                afs_protocol::meta::DfsLocalCopyPolicy::NotRequired as i32
            }
        },
    }
}

#[cfg(feature = "dfs")]
fn wire_replica_group(group: crate::dfs::ReplicaGroup) -> afs_protocol::meta::DfsReplicaGroup {
    afs_protocol::meta::DfsReplicaGroup {
        replica_group_id: group.id.0,
        placement_epoch: group.placement_epoch,
        targets: group.targets.into_iter().map(wire_replica_target).collect(),
    }
}

#[cfg(feature = "dfs")]
pub(super) fn wire_chunk_object(
    chunk: crate::dfs::ChunkObject,
) -> afs_protocol::meta::DfsChunkObject {
    afs_protocol::meta::DfsChunkObject {
        chunk_id: chunk.id.0,
        length: chunk.length,
        content_digest: chunk.content_digest.bytes.to_vec(),
        content_digest_algorithm: wire_digest_algorithm(chunk.content_digest.algorithm),
    }
}

#[cfg(feature = "dfs")]
pub(crate) fn wire_replication_claim(
    claim: &crate::dfs::ReplicationClaim,
) -> afs_protocol::meta::DfsReplicationClaim {
    afs_protocol::meta::DfsReplicationClaim {
        task_id: claim.task_id.0.clone(),
        operation_id: claim.operation_id.0.clone(),
        worker_node_id: claim.worker_node_id.clone(),
        worker_node_epoch: claim.worker_node_epoch,
        worker_session_id: claim.worker_session_id.clone(),
        expires_at_unix_ms: claim.expires_at_unix_ms,
        fence: claim.fence,
        chunk: Some(wire_chunk_object(claim.chunk.clone())),
        source_copy_id: claim.source_copy_id.0.clone(),
        placement_revision: claim.placement_revision,
        replica_group: Some(wire_replica_group(claim.replica_group.clone())),
        replication: Some(wire_replication_config(claim.replication.clone())),
    }
}

#[cfg(feature = "dfs")]
pub(super) fn wire_digest_algorithm(algorithm: crate::dfs::DigestAlgorithm) -> i32 {
    match algorithm {
        crate::dfs::DigestAlgorithm::Blake3 => {
            afs_protocol::meta::DfsDigestAlgorithm::Blake3 as i32
        }
        crate::dfs::DigestAlgorithm::Crc32cSegmentsV1 => {
            afs_protocol::meta::DfsDigestAlgorithm::Crc32cSegmentsV1 as i32
        }
    }
}

#[cfg(feature = "dfs")]
pub(super) fn domain_digest_algorithm(
    value: i32,
) -> afs_error::Result<crate::dfs::DigestAlgorithm> {
    match afs_protocol::meta::DfsDigestAlgorithm::try_from(value) {
        Ok(afs_protocol::meta::DfsDigestAlgorithm::Blake3) => {
            Ok(crate::dfs::DigestAlgorithm::Blake3)
        }
        Ok(afs_protocol::meta::DfsDigestAlgorithm::Crc32cSegmentsV1) => {
            Ok(crate::dfs::DigestAlgorithm::Crc32cSegmentsV1)
        }
        _ => Err(Error::coded(
            CLIENT_PROTOCOL_VIOLATION,
            "DFS digest algorithm is invalid",
        )),
    }
}

#[cfg(feature = "dfs")]
pub(super) fn domain_replica_write_grant(
    grant: afs_protocol::meta::DfsReplicaWriteGrant,
) -> afs_error::Result<crate::dfs::ReplicaWriteGrant> {
    let digest: [u8; 32] = grant.content_digest.try_into().map_err(|_| {
        Error::coded(
            CLIENT_PROTOCOL_VIOLATION,
            "DfsReplicaWriteGrant digest must contain 32 bytes",
        )
    })?;
    Ok(crate::dfs::ReplicaWriteGrant {
        requester_node_id: grant.requester_node_id,
        requester_node_epoch: grant.requester_node_epoch,
        initiator_node_id: grant.initiator_node_id,
        initiator_node_epoch: grant.initiator_node_epoch,
        operation_id: crate::dfs::OperationId::new(grant.operation_id),
        chunk_id: crate::dfs::ChunkId::new(grant.chunk_id),
        chunk_length: grant.chunk_length,
        content_digest: crate::dfs::ContentDigest {
            algorithm: domain_digest_algorithm(grant.content_digest_algorithm)?,
            bytes: digest,
        },
        placement_revision: grant.placement_revision,
        placement_epoch: grant.placement_epoch,
        replica_group_id: crate::dfs::ReplicaGroupId::new(grant.replica_group_id),
        target_index: grant.target_index,
        replication: domain_replication_config(required(
            grant.replication,
            "DfsReplicaWriteGrant.replication",
        )?)?,
        replica_group: domain_replica_group(required(
            grant.replica_group,
            "DfsReplicaWriteGrant.replica_group",
        )?)?,
        expires_at_unix_ms: grant.expires_at_unix_ms,
        fence: grant.fence,
        token: grant.token,
    })
}

#[cfg(feature = "dfs")]
pub(super) fn domain_chunk_object(
    chunk: afs_protocol::meta::DfsChunkObject,
) -> afs_error::Result<crate::dfs::ChunkObject> {
    let digest: [u8; 32] = chunk.content_digest.try_into().map_err(|_| {
        Error::coded(
            CLIENT_PROTOCOL_VIOLATION,
            "DfsChunkObject digest must contain 32 bytes",
        )
    })?;
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

#[cfg(feature = "dfs")]
pub(crate) fn domain_replication_claim(
    claim: afs_protocol::meta::DfsReplicationClaim,
) -> afs_error::Result<crate::dfs::ReplicationClaim> {
    Ok(crate::dfs::ReplicationClaim {
        task_id: crate::dfs::ReplicationTaskId::new(claim.task_id),
        operation_id: crate::dfs::OperationId::new(claim.operation_id),
        worker_node_id: claim.worker_node_id,
        worker_node_epoch: claim.worker_node_epoch,
        worker_session_id: claim.worker_session_id,
        expires_at_unix_ms: claim.expires_at_unix_ms,
        fence: claim.fence,
        chunk: domain_chunk_object(required(claim.chunk, "DfsReplicationClaim.chunk")?)?,
        source_copy_id: crate::dfs::CopyId::new(claim.source_copy_id),
        placement_revision: claim.placement_revision,
        replica_group: domain_replica_group(required(
            claim.replica_group,
            "DfsReplicationClaim.replica_group",
        )?)?,
        replication: domain_replication_config(required(
            claim.replication,
            "DfsReplicationClaim.replication",
        )?)?,
    })
}

#[cfg(feature = "dfs")]
pub(super) fn domain_replication_task(
    task: afs_protocol::meta::DfsReplicationTask,
) -> afs_error::Result<crate::dfs::ReplicationTask> {
    let state = match afs_protocol::meta::DfsReplicationTaskState::try_from(task.state) {
        Ok(afs_protocol::meta::DfsReplicationTaskState::Pending) => {
            crate::dfs::ReplicationTaskState::Pending
        }
        Ok(afs_protocol::meta::DfsReplicationTaskState::Running) => {
            crate::dfs::ReplicationTaskState::Running
        }
        Ok(afs_protocol::meta::DfsReplicationTaskState::RetryWaiting) => {
            crate::dfs::ReplicationTaskState::RetryWaiting
        }
        Ok(afs_protocol::meta::DfsReplicationTaskState::Completed) => {
            crate::dfs::ReplicationTaskState::Completed
        }
        Ok(afs_protocol::meta::DfsReplicationTaskState::BlockedNoSource) => {
            crate::dfs::ReplicationTaskState::BlockedNoSource
        }
        _ => {
            return Err(Error::coded(
                CLIENT_PROTOCOL_VIOLATION,
                "DfsReplicationTask has an invalid state",
            ));
        }
    };
    Ok(crate::dfs::ReplicationTask {
        id: crate::dfs::ReplicationTaskId::new(task.task_id),
        chunk_id: crate::dfs::ChunkId::new(task.chunk_id),
        placement_epoch: task.placement_epoch,
        desired_copies: u16::try_from(task.desired_copies).map_err(|_| {
            Error::coded(
                CLIENT_PROTOCOL_VIOLATION,
                "DfsReplicationTask desired_copies exceeds u16",
            )
        })?,
        existing_copies: task
            .existing_copy_ids
            .into_iter()
            .map(crate::dfs::CopyId::new)
            .collect(),
        state,
        attempt: task.attempt,
        next_retry_unix_ms: task.next_retry_unix_ms,
        last_error: task.last_error,
        claim: task
            .claim
            .map(domain_replication_claim)
            .transpose()?
            .map(Box::new),
    })
}

#[cfg(feature = "dfs")]
fn domain_replication_config(
    replication: afs_protocol::meta::DfsReplicationConfig,
) -> afs_error::Result<crate::dfs::ReplicationConfig> {
    let local_copy = match afs_protocol::meta::DfsLocalCopyPolicy::try_from(replication.local_copy)
    {
        Ok(afs_protocol::meta::DfsLocalCopyPolicy::Required) => {
            crate::dfs::LocalCopyPolicy::Required
        }
        Ok(afs_protocol::meta::DfsLocalCopyPolicy::Preferred) => {
            crate::dfs::LocalCopyPolicy::Preferred
        }
        Ok(afs_protocol::meta::DfsLocalCopyPolicy::NotRequired) => {
            crate::dfs::LocalCopyPolicy::NotRequired
        }
        _ => {
            return Err(Error::coded(
                CLIENT_PROTOCOL_VIOLATION,
                "DFS replication config has an invalid local-copy policy",
            ));
        }
    };
    let replication = crate::dfs::ReplicationConfig {
        desired_copies: u16::try_from(replication.desired_copies)
            .map_err(|_| Error::coded(CLIENT_PROTOCOL_VIOLATION, "desired_copies exceeds u16"))?,
        sync_required_copies: u16::try_from(replication.sync_required_copies).map_err(|_| {
            Error::coded(
                CLIENT_PROTOCOL_VIOLATION,
                "sync_required_copies exceeds u16",
            )
        })?,
        min_distinct_nodes: u16::try_from(replication.min_distinct_nodes).map_err(|_| {
            Error::coded(CLIENT_PROTOCOL_VIOLATION, "min_distinct_nodes exceeds u16")
        })?,
        min_distinct_failure_domains: u16::try_from(replication.min_distinct_failure_domains)
            .map_err(|_| {
                Error::coded(
                    CLIENT_PROTOCOL_VIOLATION,
                    "min_distinct_failure_domains exceeds u16",
                )
            })?,
        local_copy,
    };
    if !replication.is_valid() {
        return Err(Error::coded(
            CLIENT_PROTOCOL_VIOLATION,
            "Meta returned an invalid DFS replication config",
        ));
    }
    Ok(replication)
}

#[cfg(feature = "dfs")]
fn domain_replica_group(
    group: afs_protocol::meta::DfsReplicaGroup,
) -> afs_error::Result<crate::dfs::ReplicaGroup> {
    Ok(crate::dfs::ReplicaGroup {
        id: crate::dfs::ReplicaGroupId::new(group.replica_group_id),
        placement_epoch: group.placement_epoch,
        targets: group
            .targets
            .into_iter()
            .map(domain_replica_target)
            .collect::<afs_error::Result<Vec<_>>>()?,
    })
}

#[cfg(feature = "dfs")]
fn domain_replica_target(
    target: afs_protocol::meta::DfsReplicaTarget,
) -> afs_error::Result<crate::dfs::ReplicaTarget> {
    let device = required(target.device, "DfsReplicaTarget.device")?;
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

#[cfg(feature = "dfs")]
pub(super) fn domain_placement_snapshot(
    snapshot: afs_protocol::meta::DfsPlacementSnapshot,
) -> afs_error::Result<crate::dfs::PlacementSnapshot> {
    let replication = domain_replication_config(required(
        snapshot.replication,
        "DfsPlacementSnapshot.replication",
    )?)?;
    Ok(crate::dfs::PlacementSnapshot {
        revision: snapshot.revision,
        // Not on the wire: effective equals declared until Meta can lower it.
        local_copy: crate::dfs::EffectiveLocalCopy::declared(&replication),
        replication,
        replica_groups: snapshot
            .replica_groups
            .into_iter()
            .map(domain_replica_group)
            .collect::<afs_error::Result<Vec<_>>>()?,
    })
}

#[cfg(feature = "dfs")]
pub(super) fn wire_read_validation(
    validation: crate::dfs::DfsReadValidation,
) -> afs_protocol::meta::DfsReadValidation {
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
}

#[cfg(feature = "dfs")]
pub(super) fn current_external_read_is_unimplemented(status: &tonic::Status) -> bool {
    status.code() == tonic::Code::Unimplemented
}

#[cfg(feature = "dfs")]
pub(super) fn domain_authorize_current_external_read_reply(
    reply: afs_protocol::meta::AuthorizeCurrentExternalReadReply,
) -> afs_error::Result<crate::dfs::AuthorizeCurrentExternalReadReply> {
    let inode = domain_dfs_inode(required(reply.inode, "AuthorizeCurrentExternalRead.inode")?)?;
    if reply.head_changed {
        if !reply.authorizations.is_empty() {
            return Err(Error::coded(
                CLIENT_PROTOCOL_VIOLATION,
                "head-changed current external read must not include authorizations",
            ));
        }
        return Ok(crate::dfs::AuthorizeCurrentExternalReadReply::HeadChanged(
            inode,
        ));
    }
    let authorizations = reply
        .authorizations
        .into_iter()
        .map(domain_external_authorized_read)
        .collect::<afs_error::Result<Vec<_>>>()?;
    Ok(crate::dfs::AuthorizeCurrentExternalReadReply::Authorized {
        inode,
        authorizations,
    })
}

#[cfg(feature = "dfs")]
pub(super) fn domain_authorize_external_version_read_reply(
    reply: afs_protocol::meta::AuthorizeExternalVersionReadReply,
) -> afs_error::Result<crate::dfs::AuthorizeExternalVersionReadReply> {
    let authorizations = reply
        .authorizations
        .into_iter()
        .map(domain_external_authorized_read)
        .collect::<afs_error::Result<Vec<_>>>()?;
    if !authorizations.is_empty() && reply.remaining_validity_ms == 0 {
        return Err(Error::coded(
            CLIENT_PROTOCOL_VIOLATION,
            "external version read authorization must include a positive remaining validity",
        ));
    }
    if reply.remaining_validity_ms > 5_000 {
        return Err(Error::coded(
            CLIENT_PROTOCOL_VIOLATION,
            "external version read validity exceeds the supported bound",
        ));
    }
    Ok(crate::dfs::AuthorizeExternalVersionReadReply {
        authorizations,
        remaining_validity_ms: reply.remaining_validity_ms,
    })
}

#[cfg(feature = "dfs")]
pub(super) fn domain_external_authorized_read(
    entry: afs_protocol::meta::DfsExternalAuthorizedRead,
) -> afs_error::Result<crate::dfs::DfsExternalAuthorizedRead> {
    let authorized = required(entry.authorized, "ExternalReadAuthorization.authorized")?;
    let validation = required(
        authorized.validation,
        "ExternalReadAuthorization.validation",
    )?;
    let chunk = domain_chunk_object(required(entry.chunk, "ExternalReadAuthorization.chunk")?)?;
    let location = domain_dfs_copy_location(required(
        entry.location,
        "ExternalReadAuthorization.location",
    )?)?;
    let crate::dfs::CopyLocation::External {
        store_id,
        object_key,
        object_revision,
    } = &location
    else {
        return Err(Error::coded(
            CLIENT_PROTOCOL_VIOLATION,
            "external authorization requires external location",
        ));
    };
    let expected_key = crate::dfs::external_object_key(store_id, object_revision, &chunk.id)
        .map_err(|error| Error::coded(CLIENT_PROTOCOL_VIOLATION, error.to_string()))?;
    if chunk.id.0 != validation.chunk_id || *object_key != expected_key {
        return Err(Error::coded(
            CLIENT_PROTOCOL_VIOLATION,
            "external authorization identity mismatch",
        ));
    }
    Ok(crate::dfs::DfsExternalAuthorizedRead {
        authorized: crate::dfs::DfsAuthorizedRead {
            validation: crate::dfs::DfsReadValidation {
                grant: domain_dfs_read_grant(required(
                    validation.grant,
                    "ExternalReadAuthorization.grant",
                )?),
                chunk_id: crate::dfs::ChunkId::new(validation.chunk_id),
                copy_id: crate::dfs::CopyId::new(validation.copy_id),
                chunk_offset: validation.chunk_offset,
                length: validation.length,
            },
            allowed_ranges: authorized
                .allowed_ranges
                .into_iter()
                .map(|range| (range.offset, range.length))
                .collect(),
            expires_at_unix_ms: authorized.expires_at_unix_ms,
        },
        chunk,
        location,
    })
}

#[cfg(feature = "dfs")]
pub(super) fn domain_dfs_chunk_sources(
    value: afs_protocol::meta::DfsChunkSources,
) -> afs_error::Result<crate::dfs::ChunkSources> {
    let chunk_id = crate::dfs::ChunkId::new(value.chunk_id);
    let chunk = value.chunk.map(domain_chunk_object).transpose()?;
    if chunk.as_ref().is_some_and(|object| object.id != chunk_id) {
        return Err(Error::coded(
            CLIENT_PROTOCOL_VIOLATION,
            "chunk source object identity mismatch",
        ));
    }
    if value.allowed_ranges.len() > 128
        || value.allowed_ranges.iter().any(|range| {
            range.length == 0
                || chunk.as_ref().is_none_or(|object| {
                    range
                        .offset
                        .checked_add(range.length)
                        .is_none_or(|end| end > object.length)
                })
        })
    {
        return Err(Error::coded(
            CLIENT_PROTOCOL_VIOLATION,
            "chunk source grant ranges exceed the bounded object identity",
        ));
    }
    Ok(crate::dfs::ChunkSources {
        chunk_id,
        chunk,
        sources: value
            .sources
            .into_iter()
            .map(domain_dfs_source_candidate)
            .collect::<afs_error::Result<Vec<_>>>()?,
        allowed_ranges: value
            .allowed_ranges
            .into_iter()
            .map(|range| (range.offset, range.length))
            .collect(),
    })
}

#[cfg(feature = "dfs")]
fn domain_dfs_source_candidate(
    value: afs_protocol::meta::DfsSourceCandidate,
) -> afs_error::Result<crate::dfs::SourceCandidate> {
    let location =
        domain_dfs_copy_location(required(value.location, "DfsSourceCandidate.location")?)?;
    match &location {
        crate::dfs::CopyLocation::Node { .. }
            if value.data_endpoint.as_deref().is_none_or(str::is_empty) =>
        {
            return Err(Error::coded(
                CLIENT_PROTOCOL_VIOLATION,
                "Node source requires a Peer endpoint",
            ));
        }
        crate::dfs::CopyLocation::External { .. } if value.data_endpoint.is_some() => {
            return Err(Error::coded(
                CLIENT_PROTOCOL_VIOLATION,
                "External source cannot carry a Peer endpoint",
            ));
        }
        _ => {}
    }
    Ok(crate::dfs::SourceCandidate {
        copy_id: crate::dfs::CopyId::new(value.copy_id),
        chunk_id: crate::dfs::ChunkId::new(value.chunk_id),
        role: domain_dfs_copy_role(value.role)?,
        state: domain_dfs_copy_state(value.state)?,
        location,
        data_endpoint: value.data_endpoint,
        load_hint: value.load_hint,
        read_grant: domain_dfs_read_grant(required(
            value.read_grant,
            "DfsSourceCandidate.read_grant",
        )?),
    })
}

#[cfg(feature = "dfs")]
fn domain_dfs_copy_role(value: i32) -> afs_error::Result<crate::dfs::CopyRole> {
    match afs_protocol::meta::DfsCopyRole::try_from(value) {
        Ok(afs_protocol::meta::DfsCopyRole::DurableReplica) => {
            Ok(crate::dfs::CopyRole::DurableReplica)
        }
        Ok(afs_protocol::meta::DfsCopyRole::VerifiedCache) => {
            Ok(crate::dfs::CopyRole::VerifiedCache)
        }
        Ok(afs_protocol::meta::DfsCopyRole::ExternalCommitted) => {
            Ok(crate::dfs::CopyRole::ExternalCommitted)
        }
        _ => Err(Error::coded(
            afs_error::CLIENT_PROTOCOL_VIOLATION,
            "DfsSourceCandidate has an invalid copy role",
        )),
    }
}

#[cfg(feature = "dfs")]
fn domain_dfs_copy_state(value: i32) -> afs_error::Result<crate::dfs::CopyState> {
    match afs_protocol::meta::DfsCopyState::try_from(value) {
        Ok(afs_protocol::meta::DfsCopyState::Ready) => Ok(crate::dfs::CopyState::Ready),
        Ok(afs_protocol::meta::DfsCopyState::Corrupt) => Ok(crate::dfs::CopyState::Corrupt),
        Ok(afs_protocol::meta::DfsCopyState::Deleting) => Ok(crate::dfs::CopyState::Deleting),
        _ => Err(Error::coded(
            afs_error::CLIENT_PROTOCOL_VIOLATION,
            "DfsSourceCandidate has an invalid copy state",
        )),
    }
}

#[cfg(feature = "dfs")]
fn domain_dfs_copy_location(
    value: afs_protocol::meta::DfsCopyLocation,
) -> afs_error::Result<crate::dfs::CopyLocation> {
    match value.location {
        Some(afs_protocol::meta::dfs_copy_location::Location::Node(node)) => {
            Ok(crate::dfs::CopyLocation::Node {
                node_id: node.node_id,
                node_epoch: node.node_epoch,
                device_id: node.device_id,
                device_epoch: node.device_epoch,
                catalog_revision: node.catalog_revision,
            })
        }
        Some(afs_protocol::meta::dfs_copy_location::Location::External(external)) => {
            Ok(crate::dfs::CopyLocation::External {
                store_id: external.store_id,
                object_key: external.object_key,
                object_revision: external.object_revision,
            })
        }
        None => Err(Error::coded(
            afs_error::CLIENT_PROTOCOL_VIOLATION,
            "DfsCopyLocation is empty",
        )),
    }
}

#[cfg(feature = "dfs")]
pub(super) fn domain_dfs_read_grant(
    value: afs_protocol::meta::DfsReadGrant,
) -> crate::dfs::DfsReadGrant {
    crate::dfs::DfsReadGrant {
        namespace_id: crate::dfs::NamespaceId::new(value.namespace_id),
        file_version_id: crate::dfs::FileVersionId::new(value.file_version_id),
        layout_root_id: crate::dfs::LayoutRootId::new(value.layout_root_id),
        caller_node_id: value.caller_node_id,
        caller_node_epoch: value.caller_node_epoch,
        expires_at_unix_ms: value.expires_at_unix_ms,
        fence: value.fence,
        token: value.token,
    }
}
