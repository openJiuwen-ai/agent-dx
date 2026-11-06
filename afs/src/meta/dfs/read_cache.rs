//! Read-cache discovery uses CopyRecord/Placement and existing session fencing.
//! No new object identities, write owners, leases or durability accounting.
use super::*;
use crate::dfs::{PublishReadCache, RetireReadCache};

const MAX_READ_CACHE_COPIES: usize = 64;
const MAX_PUBLICATION_ATTEMPTS: usize = 16;

impl DfsService {
    pub async fn publish_read_cache(&self, request: PublishReadCache) -> Result<CopyRecord> {
        let digest = namespace_request_digest(&request)?;
        require_id(&request.caller_id, "caller_id")?;
        require_id(&request.caller_session_id, "caller_session_id")?;
        require_id(&request.operation_id.0, "operation_id")?;
        let ack = &request.receipt;
        if ack.node_id != request.caller_id
            || ack.node_epoch != request.caller_node_epoch
            || ack.operation_id != request.operation_id
            || ack.catalog_revision == 0
            || ack.device_epoch == 0
            || ack.node_epoch == 0
        {
            return Err(permission_denied(
                "read cache receipt does not identify caller operation",
            ));
        }
        let copy = crate::dfs::read_cache_copy(ack)?;
        if self
            .replayed_namespace_result(
                &request.caller_id,
                &request.operation_id,
                StoreOperation::DfsPublishReadCache,
                digest,
            )
            .await?
            .is_some()
        {
            return Ok(copy);
        }
        for _ in 0..MAX_PUBLICATION_ATTEMPTS {
            let session = self
                .current_session(
                    &request.caller_id,
                    request.caller_node_epoch,
                    Some(&request.caller_session_id),
                    now_unix_ms(),
                )
                .await?;
            if !session
                .storage_devices
                .iter()
                .any(|d| d.device_id == ack.device_id && d.device_epoch == ack.device_epoch)
            {
                return Err(permission_denied(
                    "read cache device is not owned by caller",
                ));
            }
            let view = self.store.read_view().await?;
            let Some(MetaEntity::DfsFileVersion(version)) = view
                .read(MetaRead::DfsFileVersion(request.file_version_id.clone()))
                .await?
                .entity
            else {
                return Err(not_found("read cache version is absent"));
            };
            if version.layout_root != request.layout_root_id {
                return Err(invalid("read cache layout differs from version"));
            }
            let Some(MetaEntity::DfsInode(inode)) = view
                .read(MetaRead::DfsInode(version.inode_id.clone()))
                .await?
                .entity
            else {
                return Err(not_found("read cache inode is absent"));
            };
            if inode.namespace_id != request.namespace_id {
                return Err(permission_denied("read cache namespace differs"));
            }
            let Some(MetaEntity::DfsLayoutRoot(layout)) = view
                .read(MetaRead::DfsLayoutRoot(request.layout_root_id.clone()))
                .await?
                .entity
            else {
                return Err(not_found("read cache layout is absent"));
            };
            let Some(MetaEntity::DfsChunk(chunk)) = view
                .read(MetaRead::DfsChunk(ack.chunk_id.clone()))
                .await?
                .entity
            else {
                return Err(not_found("read cache chunk is absent"));
            };
            if ack.persisted_bytes != chunk.length
                || ack.verified_digest != chunk.content_digest
                || !read_range_covered(&read_chunk_intervals(&layout, &chunk)?, 0, chunk.length)
            {
                return Err(permission_denied(
                    "read cache requires complete verified bytes covered by this version",
                ));
            }
            let Some(MetaEntity::DfsPlacement(placement)) = view
                .read(MetaRead::DfsPlacement(ack.chunk_id.clone()))
                .await?
                .entity
            else {
                return Err(not_found("read cache placement is absent"));
            };
            let old = view.read(MetaRead::DfsCopy(copy.id.clone())).await?.entity;
            let mut conditions = vec![
                TxnCondition::NodeSessionCurrent {
                    node_id: request.caller_id.clone(),
                    session_id: request.caller_session_id.clone(),
                },
                TxnCondition::EntityEquals(MetaEntity::DfsFileVersion(version)),
                TxnCondition::EntityEquals(MetaEntity::DfsLayoutRoot(layout)),
                TxnCondition::EntityEquals(MetaEntity::DfsChunk(chunk)),
                TxnCondition::EntityEquals(MetaEntity::DfsPlacement(placement.clone())),
            ];
            if let Some(MetaEntity::DfsCopy(old)) = old {
                if old.role != CopyRole::VerifiedCache {
                    return Err(conflict("read cache cannot replace durable copy"));
                }
                if let CopyLocation::Node {
                    node_epoch,
                    catalog_revision,
                    ..
                } = &old.location
                    && (*node_epoch > ack.node_epoch
                        || (*node_epoch == ack.node_epoch
                            && *catalog_revision > ack.catalog_revision))
                {
                    return Err(conflict("read cache receipt was superseded"));
                }
                conditions.push(TxnCondition::EntityEquals(MetaEntity::DfsCopy(old)));
            } else {
                conditions.push(TxnCondition::Missing(MetaKey::DfsCopy(copy.id.clone())));
            }
            let mut updated = placement.clone();
            if !updated.copies.contains(&copy.id) {
                // Read copies have a stable per-device ID; repeated reads do not grow this list.
                if updated
                    .copies
                    .iter()
                    .filter(|id| id.0.starts_with("cache:"))
                    .count()
                    >= MAX_READ_CACHE_COPIES
                {
                    return Err(Error::coded(
                        afs_error::IO_CAPACITY,
                        "read cache source capacity reached",
                    ));
                }
                updated.copies.push(copy.id.clone());
            }
            let key = RequestKey::new(&request.caller_id, &request.operation_id.0);
            conditions.push(TxnCondition::RequestAbsent(key.clone()));
            let mut txn = MetaTxn::new(key.clone(), StoreOperation::DfsPublishReadCache);
            txn.conditions = conditions;
            txn.mutations = vec![
                TxnMutation::Put(MetaEntity::DfsCopy(copy.clone())),
                TxnMutation::Put(MetaEntity::DfsPlacement(updated)),
                TxnMutation::RecordRequestOutcome(RequestOutcome {
                    request: key,
                    operation: StoreOperation::DfsPublishReadCache,
                    result: namespace_result(digest, OperationResult::Empty),
                }),
            ];
            let result = self.store.compare_and_commit(txn).await?;
            match empty_namespace_outcome(result, StoreOperation::DfsPublishReadCache, digest) {
                Ok(()) => return Ok(copy),
                Err(e) if e.code() == afs_error::META_DFS_CONFLICT => continue,
                Err(e) => return Err(e),
            }
        }
        Err(conflict("read cache placement changed repeatedly"))
    }

    pub async fn retire_read_cache(&self, request: RetireReadCache) -> Result<()> {
        let digest = namespace_request_digest(&request)?;
        require_id(&request.caller_id, "caller_id")?;
        require_id(&request.caller_session_id, "caller_session_id")?;
        require_id(&request.operation_id.0, "operation_id")?;
        if request.copy.role != CopyRole::VerifiedCache
            || !matches!(&request.copy.location, CopyLocation::Node {node_id, node_epoch, ..} if node_id == &request.caller_id && *node_epoch <= request.caller_node_epoch)
        {
            return Err(permission_denied(
                "only caller's verified cache may be retired",
            ));
        }
        if self
            .replayed_namespace_result(
                &request.caller_id,
                &request.operation_id,
                StoreOperation::DfsRetireReadCache,
                digest,
            )
            .await?
            .is_some()
        {
            return Ok(());
        }
        for _ in 0..MAX_PUBLICATION_ATTEMPTS {
            self.current_session(
                &request.caller_id,
                request.caller_node_epoch,
                Some(&request.caller_session_id),
                now_unix_ms(),
            )
            .await?;
            let view = self.store.read_view().await?;
            let old = view
                .read(MetaRead::DfsCopy(request.copy.id.clone()))
                .await?
                .entity;
            let key = RequestKey::new(&request.caller_id, &request.operation_id.0);
            let mut txn = MetaTxn::new(key.clone(), StoreOperation::DfsRetireReadCache);
            txn.conditions = vec![
                TxnCondition::RequestAbsent(key.clone()),
                TxnCondition::NodeSessionCurrent {
                    node_id: request.caller_id.clone(),
                    session_id: request.caller_session_id.clone(),
                },
                match old.as_ref() {
                    Some(entity) => TxnCondition::EntityEquals(entity.clone()),
                    None => TxnCondition::Missing(MetaKey::DfsCopy(request.copy.id.clone())),
                },
            ];
            if old == Some(MetaEntity::DfsCopy(request.copy.clone())) {
                let placement = view
                    .read(MetaRead::DfsPlacement(request.copy.chunk_id.clone()))
                    .await?
                    .entity;
                match placement {
                    Some(MetaEntity::DfsPlacement(mut placement)) => {
                        txn.conditions
                            .push(TxnCondition::EntityEquals(MetaEntity::DfsPlacement(
                                placement.clone(),
                            )));
                        placement.copies.retain(|id| id != &request.copy.id);
                        txn.mutations
                            .push(TxnMutation::Put(MetaEntity::DfsPlacement(placement)));
                    }
                    None => txn
                        .conditions
                        .push(TxnCondition::Missing(MetaKey::DfsPlacement(
                            request.copy.chunk_id.clone(),
                        ))),
                    Some(_) => return Err(invalid("read cache placement has wrong type")),
                }
                txn.mutations.push(TxnMutation::Delete(MetaKey::DfsCopy(
                    request.copy.id.clone(),
                )));
            }
            // A no-op also has a durable outcome. Replays cannot delete a later registration.
            txn.mutations
                .push(TxnMutation::RecordRequestOutcome(RequestOutcome {
                    request: key,
                    operation: StoreOperation::DfsRetireReadCache,
                    result: namespace_result(digest, OperationResult::Empty),
                }));
            let result = self.store.compare_and_commit(txn).await?;
            match empty_namespace_outcome(result, StoreOperation::DfsRetireReadCache, digest) {
                Ok(()) => return Ok(()),
                Err(e) if e.code() == afs_error::META_DFS_CONFLICT => continue,
                Err(e) => return Err(e),
            }
        }
        Err(conflict(
            "read cache retirement placement changed repeatedly",
        ))
    }
}
