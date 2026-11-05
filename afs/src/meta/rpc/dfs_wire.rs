//! DFS wire and domain conversions.

use super::*;

pub(super) fn wire_dfs_dentry(entry: crate::dfs::DentryRecord) -> PbDfsDentryRecord {
    PbDfsDentryRecord {
        name: entry.name,
        inode: Some(wire_dfs_inode(entry.inode)),
    }
}

pub(super) fn wire_dfs_inode(inode: crate::dfs::InodeRecord) -> PbDfsInodeRecord {
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

pub(super) fn domain_dfs_special_node(
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

pub(super) fn domain_caller_context(caller: PbDfsCallerContext) -> crate::dfs::CallerContext {
    crate::dfs::CallerContext {
        uid: caller.uid,
        gid: caller.gid,
        supplementary_gids: caller.supplementary_gids,
    }
}

pub(super) fn domain_inode_attr_update(
    update: PbDfsInodeAttributeUpdate,
) -> crate::dfs::InodeAttrUpdate {
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

pub(super) fn domain_dfs_attributes(
    attributes: PbDfsInodeAttributes,
) -> crate::dfs::InodeAttributes {
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

pub(super) fn domain_dfs_version(
    version: afs_protocol::meta::DfsFileVersion,
) -> crate::dfs::FileVersion {
    crate::dfs::FileVersion {
        id: crate::dfs::FileVersionId::new(version.version_id),
        inode_id: crate::dfs::InodeId::new(version.inode_id),
        parent_version: optional_id(version.parent_version_id).map(crate::dfs::FileVersionId::new),
        length: version.length,
        layout_root: crate::dfs::LayoutRootId::new(version.layout_root_id),
        created_at_unix_ms: version.created_at_unix_ms,
    }
}

pub(super) fn wire_dfs_layout(layout: crate::dfs::LayoutRoot) -> PbDfsLayoutRoot {
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

pub(super) fn wire_placement_snapshot(
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

pub(super) fn wire_replica_write_grant(
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
            crate::dfs::DigestAlgorithm::Crc32cSegmentsV1 => {
                afs_protocol::meta::DfsDigestAlgorithm::Crc32cSegmentsV1 as i32
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

pub(super) fn wire_chunk_object(
    chunk: crate::dfs::ChunkObject,
) -> afs_protocol::meta::DfsChunkObject {
    afs_protocol::meta::DfsChunkObject {
        chunk_id: chunk.id.0,
        length: chunk.length,
        content_digest: chunk.content_digest.bytes.to_vec(),
        content_digest_algorithm: match chunk.content_digest.algorithm {
            crate::dfs::DigestAlgorithm::Blake3 => {
                afs_protocol::meta::DfsDigestAlgorithm::Blake3 as i32
            }
            crate::dfs::DigestAlgorithm::Crc32cSegmentsV1 => {
                afs_protocol::meta::DfsDigestAlgorithm::Crc32cSegmentsV1 as i32
            }
        },
    }
}

pub(super) fn wire_replication_claim(
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

pub(super) fn wire_replication_task(
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

pub(super) fn wire_chunk_sources_reply(
    reply: crate::dfs::DfsChunkSourcesReply,
) -> afs_protocol::meta::GetDfsChunkSourcesReply {
    afs_protocol::meta::GetDfsChunkSourcesReply {
        revision: reply.revision,
        chunks: reply
            .chunks
            .into_iter()
            .map(|chunk| afs_protocol::meta::DfsChunkSources {
                chunk_id: chunk.chunk_id.0,
                chunk: chunk.chunk.map(wire_chunk_object),
                sources: chunk
                    .sources
                    .into_iter()
                    .map(wire_source_candidate)
                    .collect(),
                allowed_ranges: chunk
                    .allowed_ranges
                    .into_iter()
                    .map(
                        |(offset, length)| afs_protocol::meta::dfs_authorized_read::Range {
                            offset,
                            length,
                        },
                    )
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
            crate::dfs::CopyState::Reserved0 => {
                afs_protocol::meta::DfsCopyState::Unspecified as i32
            }
        },
        location: Some(wire_copy_location(source.location)),
        data_endpoint: source.data_endpoint,
        load_hint: source.load_hint,
        read_grant: Some(wire_read_grant(source.read_grant)),
    }
}

pub(super) fn wire_copy_location(
    location: crate::dfs::CopyLocation,
) -> afs_protocol::meta::DfsCopyLocation {
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

pub(super) fn wire_read_grant(grant: crate::dfs::DfsReadGrant) -> afs_protocol::meta::DfsReadGrant {
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

pub(super) fn domain_dfs_layout(layout: PbDfsLayoutRoot) -> crate::dfs::LayoutRoot {
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

pub(super) fn domain_dfs_metadata_delta(
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

pub(super) fn domain_validate_replica_write(
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

pub(super) fn domain_chunk_object(
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

pub(super) fn domain_replication_claim(
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

pub(super) fn domain_replica_ack(
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

pub(super) fn domain_missing_read_source(
    missing: afs_protocol::meta::DfsMissingReadSource,
) -> Result<crate::dfs::DfsMissingReadSource, Status> {
    require_text(&missing.chunk_id, "missing_source.chunk_id")?;
    Ok(crate::dfs::DfsMissingReadSource {
        chunk_id: crate::dfs::ChunkId::new(missing.chunk_id),
        chunk_offset: missing.chunk_offset,
        length: missing.length,
    })
}

pub(super) fn domain_read_validation(
    validation: afs_protocol::meta::DfsReadValidation,
) -> Result<crate::dfs::DfsReadValidation, Status> {
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
}

pub(super) fn wire_authorized_read(
    entry: crate::dfs::DfsAuthorizedRead,
) -> afs_protocol::meta::DfsAuthorizedRead {
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
            .map(
                |(offset, length)| afs_protocol::meta::dfs_authorized_read::Range {
                    offset,
                    length,
                },
            )
            .collect(),
        expires_at_unix_ms: entry.expires_at_unix_ms,
    }
}

pub(super) fn domain_external_commit(
    receipt: afs_protocol::meta::DfsExternalCommitReceipt,
) -> Result<crate::dfs::ExternalCommitReceipt, Status> {
    crate::dfs::validate_external_store_identity(&receipt.store_id, &receipt.generation)
        .map_err(afs_transport::grpc::error_status::error_to_status)?;
    let digest = receipt
        .verified_digest
        .try_into()
        .map_err(|_| invalid("external digest must contain 32 bytes"))?;
    Ok(crate::dfs::ExternalCommitReceipt {
        store_id: receipt.store_id,
        generation: receipt.generation,
        object_key: receipt.object_key,
        persisted_bytes: receipt.persisted_bytes,
        verified_digest: crate::dfs::ContentDigest {
            algorithm: domain_digest_algorithm(receipt.verified_digest_algorithm)?,
            bytes: digest,
        },
    })
}

pub(super) fn domain_chunk_receipt(
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
        external_commit: receipt
            .external_commit
            .map(domain_external_commit)
            .transpose()?,
    })
}

fn domain_digest_algorithm(value: i32) -> Result<crate::dfs::DigestAlgorithm, Status> {
    match afs_protocol::meta::DfsDigestAlgorithm::try_from(value)
        .map_err(|_| invalid("DFS digest algorithm is invalid"))?
    {
        afs_protocol::meta::DfsDigestAlgorithm::Blake3 => Ok(crate::dfs::DigestAlgorithm::Blake3),
        afs_protocol::meta::DfsDigestAlgorithm::Crc32cSegmentsV1 => {
            Ok(crate::dfs::DigestAlgorithm::Crc32cSegmentsV1)
        }
        afs_protocol::meta::DfsDigestAlgorithm::Unspecified => {
            Err(invalid("DFS digest algorithm is required"))
        }
    }
}

pub(super) fn optional_id(value: String) -> Option<String> {
    (!value.is_empty()).then_some(value)
}
