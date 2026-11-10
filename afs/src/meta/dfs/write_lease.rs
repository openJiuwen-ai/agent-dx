//! Write leases, lock and write authority, and inode metadata sync.

use super::*;

impl DfsService {
    async fn lazy_owner_session_replaced(
        &self,
        inode: &InodeRecord,
        lease: &WriteLease,
        caller_id: &str,
        session_id: &str,
    ) -> Result<bool> {
        // A lazy fill can reconstruct any uncommitted bytes from its
        // immutable source. Ordinary writes keep their existing lease window.
        if !inode
            .xattrs
            .contains_key(crate::dfs::LAZY_FILE_DESCRIPTOR_XATTR)
            || lease.owner_node_id != caller_id
            || lease.owner_session_id == session_id
        {
            return Ok(false);
        }
        let current = self
            .store
            .read(MetaRead::CurrentNodeSession {
                node_id: caller_id.to_owned(),
            })
            .await?;
        Ok(
            matches!(current.entity, Some(MetaEntity::NodeSession(session))
            if session.session_id == session_id && session.is_live_at_unix_ms(now_unix_ms())),
        )
    }

    async fn acquire_write_lease(
        &self,
        caller_id: String,
        owner_session_id: String,
        operation_id: OperationId,
        inode_id: InodeId,
        lease_seconds: u64,
    ) -> Result<WriteLease> {
        require_id(&caller_id, "caller_id")?;
        require_id(&owner_session_id, "owner_session_id")?;
        require_id(&operation_id.0, "operation_id")?;
        require_id(&inode_id.0, "inode_id")?;
        if let Some(lease) = self
            .replayed_lease(
                &caller_id,
                &operation_id,
                StoreOperation::DfsAcquireWriteLease,
            )
            .await?
        {
            return Ok(lease);
        }
        let inode = self.get_inode(inode_id.clone()).await?;
        if inode.kind != InodeKind::Regular {
            return Err(invalid("DFS write lease requires a regular file"));
        }
        let snapshot = self
            .store
            .read(MetaRead::DfsWriteLease(inode_id.clone()))
            .await?;
        let existing = match snapshot.entity {
            Some(MetaEntity::DfsWriteLease(lease)) => Some(lease),
            _ => None,
        };
        let now = now_unix_ms();
        let mut lease = existing.clone().unwrap_or_else(|| WriteLease {
            inode_id: inode_id.clone(),
            owner_node_id: caller_id.clone(),
            owner_session_id: owner_session_id.clone(),
            lease_epoch: 0,
            expires_at_unix_ms: 0,
        });
        let same_owner =
            lease.owner_node_id == caller_id && lease.owner_session_id == owner_session_id;
        let live = lease.expires_at_unix_ms > now;
        let replacing_lazy_owner = live
            && !same_owner
            && self
                .lazy_owner_session_replaced(&inode, &lease, &caller_id, &owner_session_id)
                .await?;
        if live && !same_owner && !replacing_lazy_owner {
            return Err(conflict("DFS write lease is held by another live owner"));
        }
        lease.owner_node_id = caller_id.clone();
        lease.owner_session_id = owner_session_id;
        if !same_owner || !live {
            lease.lease_epoch = lease.lease_epoch.saturating_add(1);
        }
        let requested_expiry = lease_expiry(lease_seconds)?;
        lease.expires_at_unix_ms = if same_owner && live {
            lease.expires_at_unix_ms.max(requested_expiry)
        } else {
            requested_expiry
        };

        let request = RequestKey::new(caller_id, operation_id.0.clone());
        let outcome = RequestOutcome {
            request: request.clone(),
            operation: StoreOperation::DfsAcquireWriteLease,
            result: OperationResult::DfsWriteLease(lease.clone()),
        };
        let mut txn = MetaTxn::new(request.clone(), StoreOperation::DfsAcquireWriteLease);
        txn.conditions.push(TxnCondition::RequestAbsent(request));
        match existing {
            Some(current) => {
                txn.conditions
                    .push(TxnCondition::EntityEquals(MetaEntity::DfsWriteLease(
                        current,
                    )))
            }
            None => txn
                .conditions
                .push(TxnCondition::Missing(MetaKey::DfsWriteLease(inode_id))),
        }
        if replacing_lazy_owner {
            txn.conditions.extend([
                TxnCondition::NodeSessionCurrent {
                    node_id: lease.owner_node_id.clone(),
                    session_id: lease.owner_session_id.clone(),
                },
                TxnCondition::EntityEquals(MetaEntity::DfsInode(inode)),
            ]);
        }
        txn.mutations.extend([
            TxnMutation::Put(MetaEntity::DfsWriteLease(lease)),
            TxnMutation::RecordRequestOutcome(outcome),
        ]);
        lease_outcome(
            self.store.compare_and_commit(txn).await?,
            StoreOperation::DfsAcquireWriteLease,
        )
    }

    async fn rebind_lazy_owner_for_peer(
        &self,
        inode: &InodeRecord,
        old: &WriteLease,
        caller_id: &str,
        caller_session_id: &str,
        lease_seconds: u64,
    ) -> Result<Option<WriteLease>> {
        if old.owner_node_id == caller_id
            || !inode
                .xattrs
                .contains_key(crate::dfs::LAZY_FILE_DESCRIPTOR_XATTR)
        {
            return Ok(None);
        }
        let current = self
            .store
            .read(MetaRead::CurrentNodeSession {
                node_id: old.owner_node_id.clone(),
            })
            .await?;
        let Some(MetaEntity::NodeSession(current)) = current.entity else {
            return Ok(None);
        };
        if current.session_id == old.owner_session_id || !current.is_live_at_unix_ms(now_unix_ms())
        {
            return Ok(None);
        }

        let mut lease = old.clone();
        lease.owner_session_id = current.session_id;
        lease.lease_epoch = lease.lease_epoch.saturating_add(1);
        lease.expires_at_unix_ms = lease_expiry(lease_seconds)?;
        // This is an internal repair of the existing owner's authority. The
        // peer's resolve request stays a fresh read and never acquires a lease.
        let key = RequestKey::new(caller_id, format!("lazy-rebind-{}", uuid::Uuid::new_v4()));
        let operation = StoreOperation::DfsAcquireWriteLease;
        let mut txn = MetaTxn::new(key.clone(), operation);
        txn.conditions.extend([
            TxnCondition::RequestAbsent(key.clone()),
            TxnCondition::EntityEquals(MetaEntity::DfsInode(inode.clone())),
            TxnCondition::EntityEquals(MetaEntity::DfsWriteLease(old.clone())),
            TxnCondition::NodeSessionCurrent {
                node_id: old.owner_node_id.clone(),
                session_id: lease.owner_session_id.clone(),
            },
            TxnCondition::NodeSessionCurrent {
                node_id: caller_id.to_owned(),
                session_id: caller_session_id.to_owned(),
            },
        ]);
        txn.mutations.extend([
            TxnMutation::Put(MetaEntity::DfsWriteLease(lease.clone())),
            TxnMutation::RecordRequestOutcome(RequestOutcome {
                request: key,
                operation,
                result: OperationResult::DfsWriteLease(lease),
            }),
        ]);
        lease_outcome(self.store.compare_and_commit(txn).await?, operation).map(Some)
    }

    pub async fn open_write(
        &self,
        caller_id: String,
        owner_session_id: String,
        operation_id: OperationId,
        inode_id: InodeId,
        lease_seconds: u64,
    ) -> Result<(InodeRecord, WriteLease)> {
        let inode = self.get_inode(inode_id.clone()).await?;
        let snapshot = self
            .store
            .read(MetaRead::DfsWriteLease(inode_id.clone()))
            .await?;
        if let Some(MetaEntity::DfsWriteLease(lease)) = snapshot.entity
            && lease.expires_at_unix_ms > now_unix_ms()
            && (lease.owner_node_id != caller_id || lease.owner_session_id != owner_session_id)
        {
            return Ok((inode, lease));
        }
        let lease = self
            .acquire_write_lease(
                caller_id,
                owner_session_id,
                operation_id,
                inode_id,
                lease_seconds,
            )
            .await?;
        Ok((inode, lease))
    }

    async fn resolve_fresh_write_authority(
        &self,
        caller_id: String,
        owner_session_id: String,
        operation_id: OperationId,
        inode_id: InodeId,
        lease_seconds: u64,
        context: &'static str,
    ) -> Result<(InodeRecord, WriteLease)> {
        require_id(&caller_id, "caller_id")?;
        require_id(&owner_session_id, "owner_session_id")?;
        require_id(&operation_id.0, "operation_id")?;
        require_id(&inode_id.0, "inode_id")?;
        let view = self.store.read_view().await?;
        let recorded = view
            .read(MetaRead::RequestOutcome(RequestKey::new(
                caller_id.clone(),
                operation_id.0.clone(),
            )))
            .await?
            .request_outcome;
        let inode = match view
            .read(MetaRead::DfsInode(inode_id.clone()))
            .await?
            .entity
        {
            Some(MetaEntity::DfsInode(inode)) if inode.inode_id == inode_id => inode,
            _ => {
                return Err(Error::coded(
                    afs_error::NODE_VFS_NOT_FOUND,
                    "DFS inode was not found",
                ));
            }
        };
        if inode.kind != InodeKind::Regular {
            return Err(invalid(format!(
                "DFS {context} authority requires a regular file"
            )));
        }
        let observe_only = context == "write" && lease_seconds == 0;
        // Recover a lost acquire reply before reading a newer live authority.
        // Read-only resolutions have no recorded result and remain fresh reads.
        if let Some(recorded) = recorded.filter(|_| !observe_only) {
            if recorded.operation != StoreOperation::DfsAcquireWriteLease {
                return Err(invalid(
                    "DFS operation_id was already used for another operation",
                ));
            }
            return match recorded.result {
                OperationResult::DfsWriteLease(lease)
                    if lease.inode_id == inode_id
                        && lease.owner_node_id == caller_id
                        && lease.owner_session_id == owner_session_id
                        && lease.lease_epoch != 0 =>
                {
                    Ok((inode, lease))
                }
                _ => Err(invalid(format!(
                    "DFS {context} acquire replay does not match caller and inode"
                ))),
            };
        }
        if let Some(MetaEntity::DfsWriteLease(lease)) = view
            .read(MetaRead::DfsWriteLease(inode_id.clone()))
            .await?
            .entity
        {
            if lease.inode_id != inode_id
                || lease.lease_epoch == 0
                || lease.owner_node_id.is_empty()
                || lease.owner_session_id.is_empty()
            {
                return Err(invalid(format!(
                    "DFS stored {context} authority is incomplete"
                )));
            }
            if lease.expires_at_unix_ms > now_unix_ms() {
                if observe_only {
                    return Ok((inode, lease));
                }
                if context == "write"
                    && let Some(rebound) = self
                        .rebind_lazy_owner_for_peer(
                            &inode,
                            &lease,
                            &caller_id,
                            &owner_session_id,
                            lease_seconds,
                        )
                        .await?
                {
                    return Ok((inode, rebound));
                }
                if !self
                    .lazy_owner_session_replaced(&inode, &lease, &caller_id, &owner_session_id)
                    .await?
                {
                    return Ok((inode, lease));
                }
            }
        }
        if observe_only {
            return Err(Error::coded(
                afs_error::NODE_VFS_UNAVAILABLE,
                "DFS has no unexpired write authority to observe",
            ));
        }
        let lease = self
            .acquire_write_lease(
                caller_id.clone(),
                owner_session_id.clone(),
                operation_id,
                inode_id,
                lease_seconds,
            )
            .await?;
        if lease.inode_id != inode.inode_id
            || lease.owner_node_id != caller_id
            || lease.owner_session_id != owner_session_id
            || lease.lease_epoch == 0
        {
            return Err(invalid(format!(
                "DFS {context} acquire result does not match caller and inode"
            )));
        }
        Ok((inode, lease))
    }

    pub async fn resolve_lock_authority(
        &self,
        caller_id: String,
        owner_session_id: String,
        operation_id: OperationId,
        inode_id: InodeId,
        lease_seconds: u64,
    ) -> Result<(InodeRecord, WriteLease)> {
        self.resolve_fresh_write_authority(
            caller_id,
            owner_session_id,
            operation_id,
            inode_id,
            lease_seconds,
            "lock",
        )
        .await
    }

    pub async fn resolve_write_authority(
        &self,
        caller_id: String,
        owner_session_id: String,
        operation_id: OperationId,
        inode_id: InodeId,
        lease_seconds: u64,
    ) -> Result<(InodeRecord, WriteLease)> {
        self.resolve_fresh_write_authority(
            caller_id,
            owner_session_id,
            operation_id,
            inode_id,
            lease_seconds,
            "write",
        )
        .await
    }

    pub async fn renew_write_lease(
        &self,
        caller_id: String,
        owner_session_id: String,
        operation_id: OperationId,
        current: WriteLease,
        lease_seconds: u64,
    ) -> Result<WriteLease> {
        require_id(&caller_id, "caller_id")?;
        require_id(&owner_session_id, "owner_session_id")?;
        require_id(&operation_id.0, "operation_id")?;
        require_id(&current.inode_id.0, "current.inode_id")?;
        if current.owner_node_id != caller_id || current.owner_session_id != owner_session_id {
            return Err(conflict(
                "DFS write lease owner does not match renew caller",
            ));
        }
        if let Some(lease) = self
            .replayed_lease(
                &caller_id,
                &operation_id,
                StoreOperation::DfsRenewWriteLease,
            )
            .await?
        {
            return Ok(lease);
        }
        // Expiry is a mutable renewal hint, not the fencing identity. A file
        // handle and its lock authority can hold different expiry snapshots
        // of the same live owner/epoch after another open or renewal.
        for _ in 0..16 {
            let stored = self
                .validate_write_lease(&caller_id, &current.inode_id, &current)
                .await?;
            let mut renewed = stored.clone();
            renewed.expires_at_unix_ms =
                stored.expires_at_unix_ms.max(lease_expiry(lease_seconds)?);
            let request = RequestKey::new(caller_id.clone(), operation_id.0.clone());
            let outcome = RequestOutcome {
                request: request.clone(),
                operation: StoreOperation::DfsRenewWriteLease,
                result: OperationResult::DfsWriteLease(renewed.clone()),
            };
            let mut txn = MetaTxn::new(request.clone(), StoreOperation::DfsRenewWriteLease);
            txn.conditions.extend([
                TxnCondition::RequestAbsent(request),
                TxnCondition::EntityEquals(MetaEntity::DfsWriteLease(stored)),
            ]);
            txn.mutations.extend([
                TxnMutation::Put(MetaEntity::DfsWriteLease(renewed)),
                TxnMutation::RecordRequestOutcome(outcome),
            ]);
            match self.store.compare_and_commit(txn).await? {
                TxnOutcome::ConditionFailed {
                    existing_outcome: None,
                    ..
                } => continue,
                outcome => {
                    return lease_outcome(outcome, StoreOperation::DfsRenewWriteLease);
                }
            }
        }
        Err(Error::coded(
            afs_error::META_DFS_LEASE_RETRY,
            "DFS lease renewal remained contended after retries",
        ))
    }

    pub async fn sync_inode_metadata(
        &self,
        caller_id: String,
        sync: SyncInodeMetadata,
    ) -> Result<InodeRecord> {
        for _ in 0..FILE_COMMIT_RETRY_LIMIT {
            match self
                .sync_inode_metadata_attempt(caller_id.clone(), sync.clone())
                .await?
            {
                FileCommitAttempt::Committed(inode) => return Ok(inode),
                FileCommitAttempt::Retry => tokio::task::yield_now().await,
            }
        }
        // Retain the exact operation identity on retryable contention instead
        // of permanently poisoning the inode's write state.
        Err(Error::coded(
            afs_error::IO_UNAVAILABLE,
            "DFS metadata contention; retry the same operation",
        ))
    }

    async fn sync_inode_metadata_attempt(
        &self,
        caller_id: String,
        sync: SyncInodeMetadata,
    ) -> Result<FileCommitAttempt> {
        require_id(&caller_id, "caller_id")?;
        require_id(&sync.operation_id.0, "operation_id")?;
        require_id(&sync.inode_id.0, "inode_id")?;
        if let Some(inode) = self
            .replayed_inode(
                &caller_id,
                &sync.operation_id,
                StoreOperation::DfsSyncInodeMetadata,
            )
            .await?
        {
            return Ok(FileCommitAttempt::Committed(inode));
        }
        if sync.metadata_delta.mode != CommitMetadataMode::Full {
            return Err(invalid("DFS inode metadata sync requires full metadata"));
        }
        let current = self.get_inode(sync.inode_id.clone()).await?;
        let current_lease = self
            .validate_write_lease(&caller_id, &sync.inode_id, &sync.write_lease)
            .await?;
        if current.revision < sync.expected_inode_revision
            || current.head_version != sync.expected_head_version
        {
            return Err(conflict("DFS inode changed before metadata sync"));
        }
        let mut updated = current.clone();
        updated.revision = updated.revision.saturating_add(1);
        updated.attributes.mtime_unix_ms = sync
            .metadata_delta
            .mtime_unix_ms
            .ok_or_else(|| invalid("DFS full metadata sync requires mtime"))?;
        updated.attributes.ctime_unix_ms = sync
            .metadata_delta
            .ctime_unix_ms
            .ok_or_else(|| invalid("DFS full metadata sync requires ctime"))?;
        if sync.metadata_delta.kill_suidgid {
            updated.attributes.mode = cleared_write_privileges(updated.attributes.mode);
        }

        let request = RequestKey::new(caller_id, sync.operation_id.0.clone());
        let outcome = RequestOutcome {
            request: request.clone(),
            operation: StoreOperation::DfsSyncInodeMetadata,
            result: OperationResult::DfsInode(updated.clone()),
        };
        let mut txn = MetaTxn::new(request.clone(), StoreOperation::DfsSyncInodeMetadata);
        txn.conditions.extend([
            TxnCondition::RequestAbsent(request),
            TxnCondition::EntityEquals(MetaEntity::DfsInode(current)),
            TxnCondition::EntityEquals(MetaEntity::DfsWriteLease(current_lease)),
        ]);
        txn.mutations.extend([
            TxnMutation::Put(MetaEntity::DfsInode(updated)),
            TxnMutation::RecordRequestOutcome(outcome),
        ]);
        file_commit_attempt(
            self.store.compare_and_commit(txn).await?,
            StoreOperation::DfsSyncInodeMetadata,
        )
    }

    pub(super) async fn validate_write_lease(
        &self,
        caller_id: &str,
        inode_id: &InodeId,
        presented: &WriteLease,
    ) -> Result<WriteLease> {
        if presented.inode_id != *inode_id || presented.owner_node_id != caller_id {
            return Err(conflict(
                "DFS write lease does not belong to caller and inode",
            ));
        }
        let lease_snapshot = self
            .store
            .read(MetaRead::DfsWriteLease(inode_id.clone()))
            .await?;
        let current = match lease_snapshot.entity {
            Some(MetaEntity::DfsWriteLease(lease)) => lease,
            _ => return Err(conflict("DFS operation requires a current write lease")),
        };
        if current.inode_id != presented.inode_id
            || current.owner_node_id != presented.owner_node_id
            || current.owner_session_id != presented.owner_session_id
            || current.lease_epoch != presented.lease_epoch
            || current.expires_at_unix_ms <= now_unix_ms()
        {
            return Err(conflict("DFS write lease changed or expired"));
        }
        Ok(current)
    }
}
