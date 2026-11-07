//! Republish verified replicas of an existing immutable chunk. File contents and
//! external-system protocols are deliberately absent from this authority layer.
use super::*;

impl DfsService {
    pub async fn restore_file_chunk(&self, request: crate::dfs::RestoreFileChunk) -> Result<()> {
        require_id(&request.caller_id, "caller_id")?;
        require_id(&request.operation_id.0, "operation_id")?;
        if self.is_external_backend()
            || request.receipt.operation_id != request.operation_id
            || request.write_lease.owner_node_id != request.caller_id
        {
            return Err(invalid(
                "restore requires the native owner's replication receipt",
            ));
        }
        let digest = namespace_request_digest(&request)?;
        if self
            .replayed_namespace_result(
                &request.caller_id,
                &request.operation_id,
                StoreOperation::DfsRestoreFileChunk,
                digest,
            )
            .await?
            .is_some()
        {
            return Ok(());
        }
        for _ in 0..16 {
            let lease = self
                .validate_write_lease(
                    &request.caller_id,
                    &request.write_lease.inode_id,
                    &request.write_lease,
                )
                .await?;
            let view = self.store.read_view().await?;
            let Some(MetaEntity::DfsInode(inode)) = view
                .read(MetaRead::DfsInode(lease.inode_id.clone()))
                .await?
                .entity
            else {
                return Err(not_found("restore inode is absent"));
            };
            if inode.head_version.as_ref() != Some(&request.file_version_id) {
                return Err(conflict("restore file head changed"));
            }
            let Some(MetaEntity::DfsFileVersion(version)) = view
                .read(MetaRead::DfsFileVersion(request.file_version_id.clone()))
                .await?
                .entity
            else {
                return Err(not_found("restore version is absent"));
            };
            let Some(MetaEntity::DfsLayoutRoot(layout)) = view
                .read(MetaRead::DfsLayoutRoot(request.layout_root_id.clone()))
                .await?
                .entity
            else {
                return Err(not_found("restore layout is absent"));
            };
            let Some(MetaEntity::DfsChunk(chunk)) = view
                .read(MetaRead::DfsChunk(request.receipt.chunk.id.clone()))
                .await?
                .entity
            else {
                return Err(not_found("restore chunk is absent"));
            };
            if version.inode_id != inode.inode_id
                || version.layout_root != layout.id
                || chunk != request.receipt.chunk
                || !read_range_covered(&read_chunk_intervals(&layout, &chunk)?, 0, chunk.length)
            {
                return Err(permission_denied(
                    "restore must preserve a fully referenced immutable chunk",
                ));
            }
            let Some(MetaEntity::DfsPlacement(placement)) = view
                .read(MetaRead::DfsPlacement(chunk.id.clone()))
                .await?
                .entity
            else {
                return Err(not_found("restore placement is absent"));
            };
            let gc = view.read(MetaRead::DfsGc).await?;
            if matches!(&gc.entity, Some(MetaEntity::DfsGc(state)) if state.chunks.contains(&chunk.id))
            {
                return Err(conflict("restore chunk is pending GC deletion"));
            }
            let replication = self.current_replication_config().await?;
            let copies = self
                .validate_chunk_receipt(&request.caller_id, &request.receipt, &replication)
                .await?;
            let key = RequestKey::new(&request.caller_id, &request.operation_id.0);
            let mut txn = MetaTxn::new(key.clone(), StoreOperation::DfsRestoreFileChunk);
            txn.conditions = vec![
                TxnCondition::RequestAbsent(key.clone()),
                TxnCondition::NodeSessionCurrent {
                    node_id: request.caller_id.clone(),
                    session_id: lease.owner_session_id.clone(),
                },
                TxnCondition::EntityEquals(MetaEntity::DfsWriteLease(lease)),
                TxnCondition::EntityEquals(MetaEntity::DfsInode(inode)),
                TxnCondition::EntityEquals(MetaEntity::DfsFileVersion(version)),
                TxnCondition::EntityEquals(MetaEntity::DfsLayoutRoot(layout)),
                TxnCondition::EntityEquals(MetaEntity::DfsChunk(chunk)),
                TxnCondition::EntityEquals(MetaEntity::DfsPlacement(placement.clone())),
                TxnCondition::EntityEquals(MetaEntity::DfsReplicationConfig(replication.clone())),
            ];
            txn.conditions.push(match gc.entity {
                Some(entity) => TxnCondition::EntityEquals(entity),
                None => TxnCondition::Missing(MetaKey::DfsGc),
            });
            let mut updated = placement;
            updated.replica_group_id = request.receipt.replica_group_id.clone();
            updated.placement_epoch = request.receipt.placement_epoch;
            updated.desired_copies = replication.desired_copies;
            updated.health = if copies.len() >= usize::from(replication.desired_copies) {
                PlacementHealth::Satisfied
            } else {
                PlacementHealth::UnderReplicated
            };
            for copy in copies {
                let CopyLocation::Node {
                    node_id,
                    node_epoch,
                    catalog_revision,
                    ..
                } = &copy.location
                else {
                    return Err(invalid("restore requires node replicas"));
                };
                let session = self
                    .current_session(node_id, *node_epoch, None, now_unix_ms())
                    .await?;
                txn.conditions.push(TxnCondition::NodeSessionCurrent {
                    node_id: node_id.clone(),
                    session_id: session.session_id,
                });
                let old = view.read(MetaRead::DfsCopy(copy.id.clone())).await?.entity;
                if let Some(MetaEntity::DfsCopy(old_copy)) = &old
                    && let CopyLocation::Node {
                        catalog_revision: old_revision,
                        ..
                    } = old_copy.location
                    && old_revision > *catalog_revision
                {
                    return Err(conflict("restore receipt was superseded"));
                }
                txn.conditions.push(match old {
                    Some(value) => TxnCondition::EntityEquals(value),
                    None => TxnCondition::Missing(MetaKey::DfsCopy(copy.id.clone())),
                });
                if !updated.copies.contains(&copy.id) {
                    updated.copies.push(copy.id.clone());
                }
                txn.mutations
                    .push(TxnMutation::Put(MetaEntity::DfsCopy(copy)));
            }
            txn.mutations.extend([
                TxnMutation::Put(MetaEntity::DfsPlacement(updated)),
                TxnMutation::RecordRequestOutcome(RequestOutcome {
                    request: key,
                    operation: StoreOperation::DfsRestoreFileChunk,
                    result: namespace_result(digest, OperationResult::Empty),
                }),
            ]);
            match empty_namespace_outcome(
                self.store.compare_and_commit(txn).await?,
                StoreOperation::DfsRestoreFileChunk,
                digest,
            ) {
                Ok(()) => return Ok(()),
                Err(error) if error.code() == afs_error::META_DFS_CONFLICT => continue,
                Err(error) => return Err(error),
            }
        }
        Err(conflict("restore authority changed repeatedly"))
    }
}
