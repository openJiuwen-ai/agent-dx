//! Namespace operations: lookup, create, directories, links, rename, attributes and xattrs.

use super::*;

impl DfsService {
    pub async fn lookup(
        &self,
        namespace_id: NamespaceId,
        parent_inode_id: InodeId,
        name: Vec<u8>,
    ) -> Result<Option<InodeRecord>> {
        require_id(&namespace_id.0, "namespace_id")?;
        require_id(&parent_inode_id.0, "parent_inode_id")?;
        validate_name(&name)?;
        let key = DentryKey {
            namespace_id,
            parent_inode_id,
            name,
        };
        let snapshot = self.store.read(MetaRead::DfsDentry(key)).await?;
        let Some(MetaEntity::DfsDentry(dentry)) = snapshot.entity else {
            return Ok(None);
        };
        self.get_inode(dentry.inode_id).await.map(Some)
    }

    pub async fn create(&self, request: CreateFileRequest) -> Result<(InodeRecord, WriteLease)> {
        self.create_registered(request, None)
            .await
            .map(|(inode, lease, _)| (inode, lease))
    }

    /// `create`, with the handle it opens registered as in-memory reader
    /// `reader` of the new inode (round 2 C-1) instead of a durable
    /// `open:create-` reference in the Create transaction. Returns the Meta
    /// writer term when the reader is registered; `None` asks the Node to pin
    /// the handle itself (a replay that finds the inode already changed).
    ///
    /// Why this is the C-1 registration (design §3.4): the entry `(I, no
    /// version)` is inserted before the transaction that makes `I` visible,
    /// so it precedes every view in which `I` exists (L1); `I`'s head in the
    /// transaction's own state is the empty version and `I` is linked, which
    /// is exactly what the entry pins (L7). A GC plan that sees `I` read the
    /// table after the entry was inserted, so the entry is among its inputs.
    pub async fn create_registered(
        &self,
        request: CreateFileRequest,
        reader: Option<u64>,
    ) -> Result<(InodeRecord, WriteLease, Option<u64>)> {
        let request_digest = namespace_request_digest(&request)?;
        let CreateFileRequest {
            caller_id,
            owner_session_id,
            operation_id,
            namespace_id,
            parent_inode_id,
            name,
            mut attributes,
            lease_seconds,
            first_generation,
            caller,
        } = request;
        require_id(&caller_id, "caller_id")?;
        require_id(&owner_session_id, "owner_session_id")?;
        require_id(&operation_id.0, "operation_id")?;
        require_id(&namespace_id.0, "namespace_id")?;
        require_id(&parent_inode_id.0, "parent_inode_id")?;
        if reader.is_some() && !self.readers.is_reported(&caller_id, &owner_session_id) {
            return Err(afs_error::Error::coded(
                afs_error::META_DFS_SESSION_NOT_REPORTED,
                "Node session has not reported its readers to this Meta",
            ));
        }
        if let Some((inode, lease)) = self
            .replayed_namespace_inode_and_lease(
                &caller_id,
                &operation_id,
                StoreOperation::DfsCreate,
                request_digest,
            )
            .await?
        {
            // A retry after a lost reply: register as any open would.
            let term = match reader {
                Some(reader) => self
                    .retain_reader(&caller_id, &owner_session_id, reader, &inode.inode_id, None)
                    .await
                    .ok(),
                None => None,
            };
            return Ok((inode, lease, term));
        }
        validate_name(&name)?;
        // The same rules as mkdir, mknod and symlink, checked once, on the
        // parent as first read and before anything is registered for the new
        // inode. A retry continues only on a parent whose timestamps and
        // revision moved (`create_parent_retry_drift_only`); a concurrent
        // chmod or chown of the parent races with this create as it does
        // with any other.
        let parent = self
            .directory_for_mutation(&namespace_id, &parent_inode_id)
            .await?;
        if let Some(parent) = &parent {
            ensure_directory_create_access(&caller, parent)?;
        }
        attributes.uid = caller.uid;
        attributes.gid = inherited_child_gid(&caller, parent.as_ref());
        let inode = InodeRecord {
            namespace_id: namespace_id.clone(),
            inode_id: namespace_inode_id(&caller_id, &operation_id),
            kind: InodeKind::Regular,
            attributes,
            head_version: None,
            symlink_target: None,
            xattrs: Default::default(),
            revision: 1,
        };
        let dentry_key = DentryKey {
            namespace_id: namespace_id.clone(),
            parent_inode_id: parent_inode_id.clone(),
            name,
        };
        let lease = WriteLease {
            inode_id: inode.inode_id.clone(),
            owner_node_id: caller_id.clone(),
            owner_session_id,
            lease_epoch: 1,
            expires_at_unix_ms: lease_expiry(lease_seconds)?,
        };
        // The handle this create opens is admitted in the same transaction, so
        // the new inode is protected from the moment it exists, without a second
        // Meta round trip. The caller adopts this reference when it builds the
        // handle and releases it like any other.
        let handle_reference = crate::dfs::GcReference {
            node_id: caller_id.clone(),
            session_id: lease.owner_session_id.clone(),
            reference_id: crate::dfs::created_handle_reference_id(&operation_id),
            inode_id: Some(inode.inode_id.clone()),
            publication: false,
            chunks: Vec::new(),
            retired_at_unix_ms: None,
            open_read: None,
            // A new file has no version yet; its creator reads its own writes.
            pinned_version: None,
        };
        // B1: the creator's first write generation is admitted with the
        // file, so its first write needs no admission round trip. Its chunks
        // reach no GC-visible place before this transaction is durable: the
        // creator uploads only after the reply.
        let generation_reference = first_generation
            .as_ref()
            .map(|generation| {
                super::gc::generation_reference_admitted_now(
                    &caller_id,
                    &lease.owner_session_id,
                    &inode.inode_id,
                    generation,
                )
            })
            .transpose()?;
        // ② of the registration, before the transaction that creates `I`.
        let session = lease.owner_session_id.clone();
        let registered = reader.map(|reader| {
            let seq = self.readers.insert(
                &caller_id,
                &lease.owner_session_id,
                reader,
                &inode.inode_id,
                None,
            );
            (reader, seq)
        });
        let created = self
            .create_attempts(
                caller_id.clone(),
                &operation_id,
                request_digest,
                namespace_id,
                parent_inode_id,
                parent,
                dentry_key,
                inode,
                lease,
                handle_reference,
                generation_reference,
                registered.is_some(),
            )
            .await;
        match (created, registered) {
            (Ok((inode, lease)), registered) => {
                Ok((inode, lease, registered.map(|_| self.store.writer_term())))
            }
            (Err(error), Some((reader, seq))) => {
                self.readers.remove_if(&caller_id, &session, reader, seq);
                Err(error)
            }
            (Err(error), None) => Err(error),
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn create_attempts(
        &self,
        caller_id: String,
        operation_id: &OperationId,
        request_digest: [u8; 32],
        namespace_id: NamespaceId,
        parent_inode_id: InodeId,
        mut parent: Option<InodeRecord>,
        dentry_key: DentryKey,
        inode: InodeRecord,
        lease: WriteLease,
        handle_reference: crate::dfs::GcReference,
        generation_reference: Option<crate::dfs::GcReference>,
        soft_handle: bool,
    ) -> Result<(InodeRecord, WriteLease)> {
        let request = RequestKey::new(caller_id, operation_id.0.clone());
        let max_attempts = CREATE_PARENT_DRIFT_RETRY_LIMIT;
        for attempt in 0..max_attempts {
            let outcome = RequestOutcome {
                request: request.clone(),
                operation: StoreOperation::DfsCreate,
                result: namespace_result(
                    request_digest,
                    OperationResult::DfsInodeWithLease {
                        inode: inode.clone(),
                        lease: lease.clone(),
                    },
                ),
            };
            let dentry = Dentry {
                key: dentry_key.clone(),
                inode_id: inode.inode_id.clone(),
            };
            let mut txn = MetaTxn::new(request.clone(), StoreOperation::DfsCreate);
            txn.conditions.extend([
                TxnCondition::RequestAbsent(request.clone()),
                TxnCondition::Missing(MetaKey::DfsDentry(dentry.key.clone())),
                TxnCondition::Missing(MetaKey::DfsInode(inode.inode_id.clone())),
                TxnCondition::Missing(MetaKey::DfsWriteLease(inode.inode_id.clone())),
            ]);
            if !soft_handle {
                txn.conditions
                    .push(TxnCondition::Missing(crate::meta::store::entity_key(
                        &MetaEntity::DfsReference(handle_reference.clone()),
                    )));
                txn.mutations
                    .push(TxnMutation::Put(MetaEntity::DfsReference(
                        handle_reference.clone(),
                    )));
            }
            push_parent_namespace_change(&mut txn, parent.clone(), 0);
            if let Some(reference) = &generation_reference {
                // Like `update_gc_reference`: only the creator's live session
                // admits, and an id already used (or tombstoned) is refused.
                txn.conditions.extend([
                    TxnCondition::NodeSessionCurrent {
                        node_id: reference.node_id.clone(),
                        session_id: reference.session_id.clone(),
                    },
                    TxnCondition::Missing(crate::meta::store::entity_key(
                        &MetaEntity::DfsReference(reference.clone()),
                    )),
                ]);
                txn.mutations
                    .push(TxnMutation::Put(MetaEntity::DfsReference(
                        reference.clone(),
                    )));
            }
            txn.mutations.extend([
                TxnMutation::Put(MetaEntity::DfsDentry(dentry)),
                TxnMutation::Put(MetaEntity::DfsInode(inode.clone())),
                TxnMutation::Put(MetaEntity::DfsWriteLease(lease.clone())),
                TxnMutation::RecordRequestOutcome(outcome),
            ]);
            let outcome = validate_namespace_outcome(
                self.store.compare_and_commit(txn).await?,
                request_digest,
            )?;
            match inode_and_lease_outcome(outcome, StoreOperation::DfsCreate) {
                Ok(created) => return Ok(created),
                Err(error) if error.code() == afs_error::META_DFS_CONFLICT => {
                    if let Some(created) = self
                        .replayed_namespace_inode_and_lease(
                            &request.caller_id,
                            &OperationId::new(request.request_id.clone()),
                            StoreOperation::DfsCreate,
                            request_digest,
                        )
                        .await?
                    {
                        return Ok(created);
                    }
                    let current_parent = self
                        .retryable_create_parent_drift(
                            &namespace_id,
                            &parent_inode_id,
                            &dentry_key,
                            &inode.inode_id,
                            parent.as_ref(),
                        )
                        .await?;
                    let Some(current_parent) = current_parent else {
                        return Err(error);
                    };
                    if attempt + 1 >= max_attempts {
                        return Err(conflict(
                            "DFS create parent changed repeatedly during bounded retry",
                        ));
                    }
                    parent = Some(current_parent);
                }
                Err(error) => return Err(error),
            }
        }
        unreachable!("bounded DFS create retry loop returns on every outcome")
    }

    async fn retryable_create_parent_drift(
        &self,
        namespace_id: &NamespaceId,
        parent_inode_id: &InodeId,
        dentry_key: &DentryKey,
        inode_id: &InodeId,
        previous_parent: Option<&InodeRecord>,
    ) -> Result<Option<InodeRecord>> {
        if self
            .store
            .read(MetaRead::DfsDentry(dentry_key.clone()))
            .await?
            .entity
            .is_some()
            || self
                .store
                .read(MetaRead::DfsInode(inode_id.clone()))
                .await?
                .entity
                .is_some()
            || self
                .store
                .read(MetaRead::DfsWriteLease(inode_id.clone()))
                .await?
                .entity
                .is_some()
        {
            return Ok(None);
        }
        let Some(previous_parent) = previous_parent else {
            return Ok(None);
        };
        let Some(current_parent) = self
            .directory_for_mutation(namespace_id, parent_inode_id)
            .await?
        else {
            return Ok(None);
        };
        if create_parent_retry_drift_only(previous_parent, &current_parent) {
            Ok(Some(current_parent))
        } else {
            Ok(None)
        }
    }

    pub async fn mkdir(&self, request: MkdirRequest) -> Result<InodeRecord> {
        let digest = namespace_request_digest(&request)?;
        require_id(&request.caller_id, "caller_id")?;
        require_id(&request.operation_id.0, "operation_id")?;
        require_id(&request.namespace_id.0, "namespace_id")?;
        require_id(&request.parent_inode_id.0, "parent_inode_id")?;
        let operation = StoreOperation::DfsMkdir;
        if let Some(inode) = self
            .replayed_namespace_inode(&request.caller_id, &request.operation_id, operation, digest)
            .await?
        {
            return Ok(inode);
        }
        validate_name(&request.name)?;
        let key = RequestKey::new(request.caller_id.clone(), request.operation_id.0.clone());
        let inode_id = namespace_inode_id(&request.caller_id, &request.operation_id);
        let dentry = Dentry {
            key: DentryKey {
                namespace_id: request.namespace_id.clone(),
                parent_inode_id: request.parent_inode_id.clone(),
                name: request.name,
            },
            inode_id: inode_id.clone(),
        };
        for _ in 0..NAMESPACE_MKDIR_RETRY_LIMIT {
            let parent = self
                .directory_for_mutation(&dentry.key.namespace_id, &dentry.key.parent_inode_id)
                .await?;
            if let Some(parent) = &parent {
                ensure_directory_create_access(&request.caller, parent)?;
            }
            let mut attributes = request.attributes.clone();
            attributes.uid = request.caller.uid;
            attributes.gid = inherited_child_gid(&request.caller, parent.as_ref());
            if parent
                .as_ref()
                .is_some_and(|parent| parent.attributes.mode & 0o2000 != 0)
            {
                attributes.mode |= 0o2000;
            }
            attributes.nlink = 2;
            let inode = InodeRecord {
                namespace_id: dentry.key.namespace_id.clone(),
                inode_id: inode_id.clone(),
                kind: InodeKind::Directory,
                attributes,
                head_version: None,
                symlink_target: None,
                xattrs: Default::default(),
                revision: 1,
            };
            let mut txn = MetaTxn::new(key.clone(), operation);
            txn.conditions.extend([
                TxnCondition::RequestAbsent(key.clone()),
                TxnCondition::Missing(MetaKey::DfsDentry(dentry.key.clone())),
                TxnCondition::Missing(MetaKey::DfsInode(inode_id.clone())),
            ]);
            push_parent_namespace_change(&mut txn, parent, 1);
            txn.mutations.extend([
                TxnMutation::Put(MetaEntity::DfsDentry(dentry.clone())),
                TxnMutation::Put(MetaEntity::DfsInode(inode.clone())),
                TxnMutation::RecordRequestOutcome(RequestOutcome {
                    request: key.clone(),
                    operation,
                    result: namespace_result(digest, OperationResult::DfsInode(inode)),
                }),
            ]);
            let outcome =
                validate_namespace_outcome(self.store.compare_and_commit(txn).await?, digest)?;
            if !matches!(
                outcome,
                TxnOutcome::ConditionFailed {
                    existing_outcome: None,
                    ..
                }
            ) {
                return inode_outcome(outcome, operation);
            }

            // A sibling mutation also changes the parent inode. Re-read and
            // revalidate rather than treating every failed CAS as a collision.
            if self
                .store
                .read(MetaRead::DfsDentry(dentry.key.clone()))
                .await?
                .entity
                .is_some()
            {
                if let Some(inode) = self
                    .replayed_namespace_inode(
                        &request.caller_id,
                        &request.operation_id,
                        operation,
                        digest,
                    )
                    .await?
                {
                    return Ok(inode);
                }
                return Err(already_exists("DFS mkdir target already exists"));
            }
            tokio::task::yield_now().await;
        }
        Err(Error::coded(
            afs_error::META_DFS_NAMESPACE_RETRY,
            "DFS namespace changed repeatedly during bounded mkdir retry",
        ))
    }

    /// Special inode bytes are interpreted by the Linux VFS, not ChunkSink.
    /// Meta atomically owns the name, mode, type and device number.
    pub async fn mknod(&self, request: MknodRequest) -> Result<InodeRecord> {
        let request_digest = namespace_request_digest(&request)?;
        let MknodRequest {
            caller_id,
            operation_id,
            namespace_id,
            parent_inode_id,
            name,
            kind,
            mut attributes,
            caller,
        } = request;
        require_id(&caller_id, "caller_id")?;
        require_id(&operation_id.0, "operation_id")?;
        require_id(&namespace_id.0, "namespace_id")?;
        require_id(&parent_inode_id.0, "parent_inode_id")?;
        if let Some(inode) = self
            .replayed_namespace_inode(
                &caller_id,
                &operation_id,
                StoreOperation::DfsMknod,
                request_digest,
            )
            .await?
        {
            return Ok(inode);
        }
        validate_name(&name)?;
        let parent = self
            .directory_for_mutation(&namespace_id, &parent_inode_id)
            .await?;
        if let Some(parent) = &parent {
            ensure_directory_create_access(&caller, parent)?;
        }
        if matches!(
            kind,
            SpecialNodeKind::BlockDevice { .. } | SpecialNodeKind::CharDevice { .. }
        ) && caller.uid != 0
        {
            return Err(operation_not_permitted(
                "DFS device inode creation requires a privileged caller",
            ));
        }
        if let SpecialNodeKind::BlockDevice { rdev } | SpecialNodeKind::CharDevice { rdev } = kind
            && u32::try_from(rdev).is_err()
        {
            return Err(invalid(
                "DFS device number exceeds the FUSE representable range",
            ));
        }
        attributes.uid = caller.uid;
        attributes.gid = if let Some(parent) = &parent
            && parent.attributes.mode & 0o2000 != 0
        {
            parent.attributes.gid
        } else {
            caller.gid
        };
        attributes.nlink = 1;
        let now = now_unix_ms();
        attributes.atime_unix_ms = now;
        attributes.mtime_unix_ms = now;
        attributes.ctime_unix_ms = now;
        let inode = InodeRecord {
            namespace_id: namespace_id.clone(),
            inode_id: namespace_inode_id(&caller_id, &operation_id),
            kind: InodeKind::Special(kind),
            attributes,
            head_version: None,
            symlink_target: None,
            xattrs: Default::default(),
            revision: 1,
        };
        let dentry = Dentry {
            key: DentryKey {
                namespace_id,
                parent_inode_id,
                name,
            },
            inode_id: inode.inode_id.clone(),
        };
        let request = RequestKey::new(caller_id, operation_id.0);
        let mut txn = MetaTxn::new(request.clone(), StoreOperation::DfsMknod);
        txn.conditions.extend([
            TxnCondition::RequestAbsent(request.clone()),
            TxnCondition::Missing(MetaKey::DfsDentry(dentry.key.clone())),
            TxnCondition::Missing(MetaKey::DfsInode(inode.inode_id.clone())),
        ]);
        push_parent_namespace_change(&mut txn, parent, 0);
        txn.mutations.extend([
            TxnMutation::Put(MetaEntity::DfsDentry(dentry)),
            TxnMutation::Put(MetaEntity::DfsInode(inode.clone())),
            TxnMutation::RecordRequestOutcome(RequestOutcome {
                request,
                operation: StoreOperation::DfsMknod,
                result: namespace_result(request_digest, OperationResult::DfsInode(inode)),
            }),
        ]);
        inode_outcome(
            validate_namespace_outcome(self.store.compare_and_commit(txn).await?, request_digest)?,
            StoreOperation::DfsMknod,
        )
    }

    /// One page of a directory in name order, strictly after `after` (empty
    /// starts at the beginning), at most `limit` entries capped at
    /// [`READ_DIR_PAGE_MAX`] (0 asks for the cap). Each page reads one consistent view; across pages
    /// a name cursor keeps every entry not changed meanwhile exactly once (R72).
    pub async fn read_dir_page(
        &self,
        namespace_id: NamespaceId,
        parent_inode_id: InodeId,
        after: Vec<u8>,
        limit: usize,
    ) -> Result<DirPage> {
        require_id(&namespace_id.0, "namespace_id")?;
        require_id(&parent_inode_id.0, "parent_inode_id")?;
        let limit = if limit == 0 {
            READ_DIR_PAGE_MAX
        } else {
            limit.min(READ_DIR_PAGE_MAX)
        };
        let view = self.store.read_view().await?;
        let parent = inode_from_view(&view, &namespace_id, parent_inode_id.clone()).await?;
        if parent.kind != InodeKind::Directory {
            return Err(not_directory("DFS readdir target is not a directory"));
        }
        // One extra entry tells whether the page ends the directory.
        let snapshot = view
            .read(MetaRead::DfsDirectoryPage {
                namespace_id: namespace_id.clone(),
                parent_inode_id,
                after,
                limit: limit + 1,
            })
            .await?;
        let eof = snapshot.entities.len() <= limit;
        let mut entries = Vec::with_capacity(snapshot.entities.len().min(limit));
        for entity in snapshot.entities.into_iter().take(limit) {
            let MetaEntity::DfsDentry(dentry) = entity else {
                continue;
            };
            let inode = inode_from_view(&view, &namespace_id, dentry.inode_id).await?;
            entries.push(DirEntryRecord {
                name: dentry.key.name,
                inode_id: inode.inode_id,
                kind: inode.kind,
            });
        }
        Ok(DirPage { entries, eof })
    }

    pub async fn unlink(&self, request: UnlinkRequest) -> Result<InodeRecord> {
        self.remove_namespace_entry(request, false).await
    }

    pub async fn rmdir(&self, request: RmdirRequest) -> Result<InodeRecord> {
        self.remove_namespace_entry(
            UnlinkRequest {
                caller_id: request.caller_id,
                operation_id: request.operation_id,
                namespace_id: request.namespace_id,
                parent_inode_id: request.parent_inode_id,
                name: request.name,
                caller: request.caller,
            },
            true,
        )
        .await
    }

    /// One rename. A commit that lost a race on any inode or dentry it read
    /// (a commit or metadata sync of the source or the replaced file, a sibling
    /// change of a parent) is planned again here against the current state,
    /// rechecking every rule, up to `RENAME_RETRY_LIMIT` attempts (round 2 B6):
    /// the Node no longer has to resend it.
    pub async fn rename(&self, request: RenameRequest) -> Result<RenameOutcome> {
        for _ in 0..RENAME_RETRY_LIMIT {
            if let Some(outcome) = self.rename_attempt(request.clone()).await? {
                return Ok(outcome);
            }
            tokio::task::yield_now().await;
        }
        Err(conflict("DFS metadata condition changed during rename"))
    }

    async fn rename_attempt(&self, request: RenameRequest) -> Result<Option<RenameOutcome>> {
        let request_digest = namespace_request_digest(&request)?;
        let RenameRequest {
            caller_id,
            operation_id,
            namespace_id,
            old_parent_inode_id,
            old_name,
            new_parent_inode_id,
            new_name,
            mode,
            caller,
        } = request;
        require_id(&caller_id, "caller_id")?;
        require_id(&operation_id.0, "operation_id")?;
        require_id(&namespace_id.0, "namespace_id")?;
        require_id(&old_parent_inode_id.0, "old_parent_inode_id")?;
        require_id(&new_parent_inode_id.0, "new_parent_inode_id")?;
        if let Some(outcome) = self
            .replayed_namespace_rename(
                &caller_id,
                &operation_id,
                StoreOperation::DfsRename,
                request_digest,
            )
            .await?
        {
            return Ok(Some(outcome));
        }
        validate_name(&old_name)?;
        validate_name(&new_name)?;
        let (old_parent, new_parent) = self
            .rename_parents(
                &caller,
                &namespace_id,
                &old_parent_inode_id,
                &new_parent_inode_id,
            )
            .await?;
        let old_key = DentryKey {
            namespace_id: namespace_id.clone(),
            parent_inode_id: old_parent_inode_id.clone(),
            name: old_name,
        };
        let new_key = DentryKey {
            namespace_id: namespace_id.clone(),
            parent_inode_id: new_parent_inode_id.clone(),
            name: new_name,
        };
        let view = self.store.read_view().await?;
        let source_dentry = dentry_from_view(&view, old_key.clone())
            .await?
            .ok_or_else(|| not_found("DFS rename source was not found"))?;
        let source_inode =
            inode_from_view(&view, &namespace_id, source_dentry.inode_id.clone()).await?;
        ensure_sticky_parent_allows(&caller, old_parent.as_ref(), &source_inode)?;
        let target_dentry = dentry_from_view(&view, new_key.clone()).await?;
        if old_key == new_key {
            return self
                .record_rename_noop(
                    caller_id,
                    operation_id,
                    source_dentry,
                    source_inode,
                    request_digest,
                )
                .await
                .map(Some);
        }
        if source_inode.kind == InodeKind::Directory
            && directory_is_descendant_or_same(
                &view,
                &namespace_id,
                &source_inode.inode_id,
                &new_parent_inode_id,
            )
            .await?
        {
            return Err(invalid("DFS cannot rename a directory below itself"));
        }
        if target_dentry.is_some() && mode == RenameMode::NoReplace {
            return Err(already_exists("DFS rename target already exists"));
        }
        let mut replaced_inode = None;
        let mut target_directory_empty_condition = None;
        if let Some(target) = &target_dentry {
            match rename_target_rules(
                &view,
                &caller,
                &namespace_id,
                new_parent.as_ref(),
                &source_inode,
                target,
            )
            .await?
            {
                RenameTargetRule::SameInode => {
                    return self
                        .record_rename_noop(
                            caller_id,
                            operation_id,
                            source_dentry,
                            source_inode,
                            request_digest,
                        )
                        .await
                        .map(Some);
                }
                RenameTargetRule::Replace {
                    target_inode,
                    empty_directory,
                } => {
                    target_directory_empty_condition = empty_directory;
                    replaced_inode = Some(*target_inode);
                }
            }
        }

        let same_parent = old_parent_inode_id == new_parent_inode_id;
        let txn = rename_txn(
            &view,
            RenamePlan {
                request: RequestKey::new(caller_id, operation_id.0.clone()),
                request_digest,
                namespace_id,
                old_key,
                new_key,
                new_parent_inode_id,
                source_dentry,
                source_inode,
                target_dentry,
                replaced_inode,
                target_directory_empty_condition,
                old_parent,
                new_parent,
                same_parent,
            },
        )
        .await?;
        match self.store.compare_and_commit(txn).await? {
            // Lost a race on something it read: plan again (the caller retries).
            TxnOutcome::ConditionFailed {
                existing_outcome: None,
                ..
            } => Ok(None),
            outcome => rename_outcome(
                validate_namespace_outcome(outcome, request_digest)?,
                StoreOperation::DfsRename,
            )
            .map(Some),
        }
    }

    /// Reads both parent directories (old first) and checks create access on
    /// each distinct one.
    async fn rename_parents(
        &self,
        caller: &CallerContext,
        namespace_id: &NamespaceId,
        old_parent_inode_id: &InodeId,
        new_parent_inode_id: &InodeId,
    ) -> Result<(Option<InodeRecord>, Option<InodeRecord>)> {
        let old_parent = self
            .directory_for_mutation(namespace_id, old_parent_inode_id)
            .await?;
        let new_parent = self
            .directory_for_mutation(namespace_id, new_parent_inode_id)
            .await?;
        if let Some(parent) = &old_parent {
            ensure_directory_create_access(caller, parent)?;
        }
        if old_parent_inode_id != new_parent_inode_id
            && let Some(parent) = &new_parent
        {
            ensure_directory_create_access(caller, parent)?;
        }
        Ok((old_parent, new_parent))
    }

    async fn remove_namespace_entry(
        &self,
        request: UnlinkRequest,
        directory: bool,
    ) -> Result<InodeRecord> {
        let mut target_inode = None;
        for _ in 0..NAMESPACE_REMOVE_RETRY_LIMIT {
            match self
                .remove_namespace_entry_attempt(request.clone(), directory, &mut target_inode)
                .await?
            {
                NamespaceRemoveAttempt::Removed(inode) => return Ok(inode),
                NamespaceRemoveAttempt::Retry => tokio::task::yield_now().await,
            }
        }
        Err(Error::coded(
            afs_error::META_DFS_NAMESPACE_RETRY,
            "DFS namespace changed repeatedly during bounded removal retry",
        ))
    }

    async fn remove_namespace_entry_attempt(
        &self,
        request: UnlinkRequest,
        directory: bool,
        target_inode: &mut Option<InodeId>,
    ) -> Result<NamespaceRemoveAttempt> {
        let request_digest = namespace_request_digest(&request)?;
        let UnlinkRequest {
            caller_id,
            operation_id,
            namespace_id,
            parent_inode_id,
            name,
            caller,
        } = request;
        require_id(&caller_id, "caller_id")?;
        require_id(&operation_id.0, "operation_id")?;
        require_id(&namespace_id.0, "namespace_id")?;
        require_id(&parent_inode_id.0, "parent_inode_id")?;
        let operation = if directory {
            StoreOperation::DfsRmdir
        } else {
            StoreOperation::DfsUnlink
        };
        if let Some(inode) = self
            .replayed_namespace_inode(&caller_id, &operation_id, operation, request_digest)
            .await?
        {
            return Ok(NamespaceRemoveAttempt::Removed(inode));
        }
        validate_name(&name)?;
        let parent = self
            .directory_for_mutation(&namespace_id, &parent_inode_id)
            .await?;
        if let Some(parent) = &parent {
            ensure_directory_create_access(&caller, parent)?;
        }
        let key = DentryKey {
            namespace_id: namespace_id.clone(),
            parent_inode_id,
            name,
        };
        let view = self.store.read_view().await?;
        let dentry = dentry_from_view(&view, key.clone())
            .await?
            .ok_or_else(|| not_found("DFS namespace entry was not found"))?;
        // Revalidate permissions, current metadata and directory emptiness on every
        // attempt, but never follow a replaced name to a different inode within
        // the same operation. The original request identity/digest stays fixed.
        if target_inode
            .as_ref()
            .is_some_and(|expected| expected != &dentry.inode_id)
        {
            return Err(Error::coded(
                afs_error::META_DFS_NAMESPACE_RETRY,
                "DFS removal target was replaced during retry",
            ));
        }
        *target_inode = Some(dentry.inode_id.clone());
        let mut inode = inode_from_view(&view, &namespace_id, dentry.inode_id.clone()).await?;
        if directory {
            if inode.kind != InodeKind::Directory {
                return Err(not_directory("DFS rmdir target is not a directory"));
            }
            if !directory_entries_from_view(&view, &namespace_id, &inode.inode_id)
                .await?
                .is_empty()
            {
                return Err(directory_not_empty("DFS rmdir target is not empty"));
            }
        } else if inode.kind == InodeKind::Directory {
            return Err(is_directory("DFS unlink target is a directory"));
        }
        ensure_sticky_parent_allows(&caller, parent.as_ref(), &inode)?;
        let original_inode = inode.clone();
        if directory {
            inode.attributes.nlink = 0;
        } else {
            inode.attributes.nlink = inode.attributes.nlink.saturating_sub(1);
        }
        inode.attributes.ctime_unix_ms = now_unix_ms();
        inode.revision = inode.revision.saturating_add(1);
        let request = RequestKey::new(caller_id, operation_id.0.clone());
        let outcome = RequestOutcome {
            request: request.clone(),
            operation,
            result: namespace_result(request_digest, OperationResult::DfsInode(inode.clone())),
        };
        let mut txn = MetaTxn::new(request.clone(), operation);
        txn.conditions.extend([
            TxnCondition::RequestAbsent(request),
            TxnCondition::EntityEquals(MetaEntity::DfsDentry(dentry)),
            TxnCondition::EntityEquals(MetaEntity::DfsInode(original_inode)),
        ]);
        if directory {
            txn.conditions.push(TxnCondition::DfsDirectoryEmpty {
                namespace_id: namespace_id.clone(),
                parent_inode_id: inode.inode_id.clone(),
            });
        }
        txn.mutations
            .push(TxnMutation::Delete(MetaKey::DfsDentry(key)));
        txn.mutations
            .push(TxnMutation::Put(MetaEntity::DfsInode(inode)));
        push_parent_namespace_change(&mut txn, parent, if directory { -1 } else { 0 });
        txn.mutations
            .push(TxnMutation::RecordRequestOutcome(outcome));
        let outcome =
            validate_namespace_outcome(self.store.compare_and_commit(txn).await?, request_digest)?;
        match outcome {
            TxnOutcome::ConditionFailed {
                existing_outcome: None,
                ..
            } => Ok(NamespaceRemoveAttempt::Retry),
            outcome => inode_outcome(outcome, operation).map(NamespaceRemoveAttempt::Removed),
        }
    }

    pub async fn link(&self, request: LinkRequest) -> Result<InodeRecord> {
        let request_digest = namespace_request_digest(&request)?;
        let LinkRequest {
            caller_id,
            operation_id,
            namespace_id,
            existing_inode_id,
            expected_inode_revision,
            parent_inode_id,
            name,
            caller,
        } = request;
        require_id(&caller_id, "caller_id")?;
        require_id(&operation_id.0, "operation_id")?;
        require_id(&namespace_id.0, "namespace_id")?;
        require_id(&existing_inode_id.0, "existing_inode_id")?;
        require_id(&parent_inode_id.0, "parent_inode_id")?;
        if let Some(inode) = self
            .replayed_namespace_inode(
                &caller_id,
                &operation_id,
                StoreOperation::DfsLink,
                request_digest,
            )
            .await?
        {
            return Ok(inode);
        }
        validate_name(&name)?;
        let parent = self
            .directory_for_mutation(&namespace_id, &parent_inode_id)
            .await?;
        if let Some(parent) = &parent {
            ensure_directory_create_access(&caller, parent)?;
        }
        let view = self.store.read_view().await?;
        let mut inode = inode_from_view(&view, &namespace_id, existing_inode_id.clone()).await?;
        if inode.kind == InodeKind::Directory {
            return Err(operation_not_permitted(
                "DFS hardlink target cannot be a directory",
            ));
        }
        if inode.attributes.nlink == 0 {
            return Err(not_found("DFS hardlink target is no longer linked"));
        }
        if inode.revision != expected_inode_revision {
            return Err(conflict("DFS hardlink target inode revision changed"));
        }
        let dentry = Dentry {
            key: DentryKey {
                namespace_id: namespace_id.clone(),
                parent_inode_id,
                name,
            },
            inode_id: inode.inode_id.clone(),
        };
        let original_inode = inode.clone();
        inode.attributes.nlink = inode.attributes.nlink.saturating_add(1);
        inode.attributes.ctime_unix_ms = now_unix_ms();
        inode.revision = inode.revision.saturating_add(1);
        let request = RequestKey::new(caller_id, operation_id.0.clone());
        let outcome = RequestOutcome {
            request: request.clone(),
            operation: StoreOperation::DfsLink,
            result: namespace_result(request_digest, OperationResult::DfsInode(inode.clone())),
        };
        let mut txn = MetaTxn::new(request.clone(), StoreOperation::DfsLink);
        txn.conditions.extend([
            TxnCondition::RequestAbsent(request),
            TxnCondition::Missing(MetaKey::DfsDentry(dentry.key.clone())),
            TxnCondition::EntityEquals(MetaEntity::DfsInode(original_inode)),
        ]);
        push_parent_namespace_change(&mut txn, parent, 0);
        txn.mutations.extend([
            TxnMutation::Put(MetaEntity::DfsDentry(dentry)),
            TxnMutation::Put(MetaEntity::DfsInode(inode)),
            TxnMutation::RecordRequestOutcome(outcome),
        ]);
        inode_outcome(
            validate_namespace_outcome(self.store.compare_and_commit(txn).await?, request_digest)?,
            StoreOperation::DfsLink,
        )
    }

    pub async fn symlink(&self, request: SymlinkRequest) -> Result<InodeRecord> {
        let request_digest = namespace_request_digest(&request)?;
        let SymlinkRequest {
            caller_id,
            operation_id,
            namespace_id,
            parent_inode_id,
            name,
            target,
            mut attributes,
            caller,
        } = request;
        require_id(&caller_id, "caller_id")?;
        require_id(&operation_id.0, "operation_id")?;
        require_id(&namespace_id.0, "namespace_id")?;
        require_id(&parent_inode_id.0, "parent_inode_id")?;
        if let Some(inode) = self
            .replayed_namespace_inode(
                &caller_id,
                &operation_id,
                StoreOperation::DfsSymlink,
                request_digest,
            )
            .await?
        {
            return Ok(inode);
        }
        validate_name(&name)?;
        validate_symlink_target(&target)?;
        let parent = self
            .directory_for_mutation(&namespace_id, &parent_inode_id)
            .await?;
        if let Some(parent) = &parent {
            ensure_directory_create_access(&caller, parent)?;
        }
        attributes.uid = caller.uid;
        attributes.gid = inherited_child_gid(&caller, parent.as_ref());
        attributes.nlink = 1;
        let inode = InodeRecord {
            namespace_id: namespace_id.clone(),
            inode_id: namespace_inode_id(&caller_id, &operation_id),
            kind: InodeKind::Symlink,
            attributes,
            head_version: None,
            symlink_target: Some(target),
            xattrs: Default::default(),
            revision: 1,
        };
        let dentry = Dentry {
            key: DentryKey {
                namespace_id,
                parent_inode_id,
                name,
            },
            inode_id: inode.inode_id.clone(),
        };
        let request = RequestKey::new(caller_id, operation_id.0.clone());
        let outcome = RequestOutcome {
            request: request.clone(),
            operation: StoreOperation::DfsSymlink,
            result: namespace_result(request_digest, OperationResult::DfsInode(inode.clone())),
        };
        let mut txn = MetaTxn::new(request.clone(), StoreOperation::DfsSymlink);
        txn.conditions.extend([
            TxnCondition::RequestAbsent(request),
            TxnCondition::Missing(MetaKey::DfsDentry(dentry.key.clone())),
            TxnCondition::Missing(MetaKey::DfsInode(inode.inode_id.clone())),
        ]);
        push_parent_namespace_change(&mut txn, parent, 0);
        txn.mutations.extend([
            TxnMutation::Put(MetaEntity::DfsDentry(dentry)),
            TxnMutation::Put(MetaEntity::DfsInode(inode)),
            TxnMutation::RecordRequestOutcome(outcome),
        ]);
        inode_outcome(
            validate_namespace_outcome(self.store.compare_and_commit(txn).await?, request_digest)?,
            StoreOperation::DfsSymlink,
        )
    }

    pub async fn read_link(&self, request: ReadLinkRequest) -> Result<Vec<u8>> {
        require_id(&request.namespace_id.0, "namespace_id")?;
        require_id(&request.inode_id.0, "inode_id")?;
        let view = self.store.read_view().await?;
        let inode = inode_from_view(&view, &request.namespace_id, request.inode_id).await?;
        if inode.kind != InodeKind::Symlink {
            return Err(invalid("DFS readlink target is not a symlink"));
        }
        inode
            .symlink_target
            .ok_or_else(|| invalid("DFS symlink inode is missing target metadata"))
    }

    /// Applies one attribute change. Without a client revision the change is
    /// rebuilt on the current inode and a commit that lost a race with another
    /// inode update (a background metadata sync, a commit) is retried here,
    /// up to `SETATTR_RETRY_LIMIT` attempts, instead of failing the caller
    /// with a conflict it cannot act on.
    pub async fn set_inode_attributes(&self, request: SetInodeAttrRequest) -> Result<InodeRecord> {
        require_id(&request.caller_id, "caller_id")?;
        require_id(&request.operation_id.0, "operation_id")?;
        require_id(&request.inode_id.0, "inode_id")?;
        let request_digest = namespace_request_digest(&request)?;
        for _ in 0..SETATTR_RETRY_LIMIT {
            match self
                .set_inode_attributes_attempt(&request, request_digest)
                .await?
            {
                SetAttrAttempt::Applied(inode) => return Ok(inode),
                SetAttrAttempt::Retry => tokio::task::yield_now().await,
            }
        }
        Err(Error::coded(
            afs_error::META_DFS_NAMESPACE_RETRY,
            "DFS inode changed repeatedly during bounded setattr retry",
        ))
    }

    async fn set_inode_attributes_attempt(
        &self,
        request: &SetInodeAttrRequest,
        request_digest: [u8; 32],
    ) -> Result<SetAttrAttempt> {
        let SetInodeAttrRequest {
            caller_id,
            operation_id,
            caller,
            inode_id,
            expected_inode_revision,
            update,
        } = request;
        if let Some(inode) = self
            .replayed_namespace_inode(
                caller_id,
                operation_id,
                StoreOperation::DfsSetInodeAttributes,
                request_digest,
            )
            .await?
        {
            return Ok(SetAttrAttempt::Applied(inode));
        }
        let current = self.get_inode(inode_id.clone()).await?;
        if expected_inode_revision.is_some_and(|expected| current.revision != expected) {
            return Err(conflict("DFS inode revision changed before setattr"));
        }
        validate_attr_update_permission(caller, &current, update)?;
        let mut updated = current.clone();
        let ownership_changed = update.uid.is_some() || update.gid.is_some();
        if let Some(mode) = update.mode {
            updated.attributes.mode = mode;
        }
        if let Some(uid) = update.uid {
            updated.attributes.uid = uid;
        }
        if let Some(gid) = update.gid {
            updated.attributes.gid = gid;
        }
        if ownership_changed && current.kind != InodeKind::Directory {
            updated.attributes.mode &= !0o6000;
        }
        let attr_now = now_unix_ms();
        if let Some(atime) = update.atime_unix_ms {
            updated.attributes.atime_unix_ms = if update.timestamps_now {
                attr_now
            } else {
                atime
            };
        }
        if let Some(mtime) = update.mtime_unix_ms {
            updated.attributes.mtime_unix_ms = if update.timestamps_now {
                attr_now
            } else {
                mtime
            };
        }
        updated.attributes.ctime_unix_ms = attr_now;
        updated.revision = updated.revision.saturating_add(1);
        let applied = updated.clone();
        let key = RequestKey::new(caller_id.clone(), operation_id.0.clone());
        let mut txn = MetaTxn::new(key.clone(), StoreOperation::DfsSetInodeAttributes);
        txn.conditions.extend([
            TxnCondition::RequestAbsent(key.clone()),
            TxnCondition::EntityEquals(MetaEntity::DfsInode(current)),
        ]);
        txn.mutations.extend([
            TxnMutation::Put(MetaEntity::DfsInode(updated.clone())),
            TxnMutation::RecordRequestOutcome(RequestOutcome {
                request: key,
                operation: StoreOperation::DfsSetInodeAttributes,
                result: namespace_result(request_digest, OperationResult::DfsInode(updated)),
            }),
        ]);
        match self.store.compare_and_commit(txn).await? {
            TxnOutcome::Committed { .. } => Ok(SetAttrAttempt::Applied(applied)),
            // A lost reply's retry, or another request committing this id
            // first, is answered from the recorded outcome next attempt.
            TxnOutcome::ConditionFailed {
                existing_outcome: Some(_),
                ..
            } => Ok(SetAttrAttempt::Retry),
            TxnOutcome::ConditionFailed { .. } if expected_inode_revision.is_some() => {
                Err(conflict("DFS metadata condition changed during commit"))
            }
            TxnOutcome::ConditionFailed { .. } => Ok(SetAttrAttempt::Retry),
        }
    }

    pub async fn get_xattr(&self, request: GetXattrRequest) -> Result<Vec<u8>> {
        let name = xattr_key(&request.name)?;
        let inode = self.get_inode(request.inode_id).await?;
        ensure_xattr_permission(&request.caller, &inode, &name, XattrAccess::Read)?;
        inode
            .xattrs
            .get(&name)
            .cloned()
            .ok_or_else(|| no_data("DFS xattr does not exist"))
    }

    pub async fn list_xattr(&self, request: ListXattrRequest) -> Result<Vec<Vec<u8>>> {
        let inode = self.get_inode(request.inode_id).await?;
        ensure_basic_access(&request.caller, &inode, AccessMode::Read)?;
        Ok(inode.xattrs.keys().cloned().collect())
    }

    pub async fn bind_lazy_source(
        &self,
        request: crate::dfs::BindLazySourceRequest,
    ) -> Result<InodeRecord> {
        use crate::dfs::LAZY_FILE_DESCRIPTOR_XATTR;
        let digest = namespace_request_digest(&request)?;
        let operation = StoreOperation::DfsBindLazySource;
        let crate::dfs::BindLazySourceRequest {
            caller_id,
            caller_session_id,
            operation_id,
            inode_id,
            expected_inode_revision,
            source,
        } = request;
        require_id(&caller_id, "caller_id")?;
        require_id(&caller_session_id, "caller_session_id")?;
        require_id(&operation_id.0, "operation_id")?;
        source.validate()?;
        if let Some(inode) = self
            .replayed_namespace_inode(&caller_id, &operation_id, operation, digest)
            .await?
        {
            return Ok(inode);
        }
        let bytes = serde_json::to_vec(&source).map_err(|error| invalid(error.to_string()))?;
        let current = self.get_inode(inode_id.clone()).await?;
        if let Some(existing) = current.xattrs.get(LAZY_FILE_DESCRIPTOR_XATTR) {
            return if *existing == bytes {
                Ok(current)
            } else {
                Err(conflict("lazy source is immutable"))
            };
        }
        if current.revision != expected_inode_revision
            || current.kind != InodeKind::Regular
            || current.attributes.mode & 0o222 != 0
            || current.attributes.nlink != 1
        {
            return Err(conflict("lazy source requires a prepared read-only inode"));
        }
        match current.head_version.clone() {
            Some(id) => {
                let (version, layout) = self.get_file_version(id).await?;
                if version.inode_id != inode_id
                    || layout.file_length != source.length
                    || !layout.inline_extents.is_empty()
                {
                    return Err(conflict(
                        "lazy source requires an empty committed layout of the declared length",
                    ));
                }
            }
            None if source.length == 0 => {}
            None => return Err(conflict("lazy source requires committed length")),
        }
        let Some(MetaEntity::DfsWriteLease(lease)) = self
            .store
            .read(MetaRead::DfsWriteLease(inode_id))
            .await?
            .entity
        else {
            return Err(conflict(
                "lazy source binding requires the current write owner",
            ));
        };
        if lease.owner_node_id != caller_id
            || lease.owner_session_id != caller_session_id
            || lease.expires_at_unix_ms <= now_unix_ms()
        {
            return Err(conflict(
                "lazy source binding requires the current write owner",
            ));
        }
        let key = RequestKey::new(caller_id.clone(), operation_id.0);
        let mut updated = current.clone();
        updated
            .xattrs
            .insert(LAZY_FILE_DESCRIPTOR_XATTR.to_vec(), bytes);
        updated.revision = updated.revision.saturating_add(1);
        let mut txn = MetaTxn::new(key.clone(), operation);
        txn.conditions.extend([
            TxnCondition::RequestAbsent(key.clone()),
            TxnCondition::EntityEquals(MetaEntity::DfsInode(current)),
            TxnCondition::EntityEquals(MetaEntity::DfsWriteLease(lease)),
            TxnCondition::NodeSessionCurrent {
                node_id: caller_id,
                session_id: caller_session_id,
            },
        ]);
        txn.mutations.extend([
            TxnMutation::Put(MetaEntity::DfsInode(updated.clone())),
            TxnMutation::RecordRequestOutcome(RequestOutcome {
                request: key,
                operation,
                result: namespace_result(digest, OperationResult::DfsInode(updated)),
            }),
        ]);
        inode_outcome(
            validate_namespace_outcome(self.store.compare_and_commit(txn).await?, digest)?,
            operation,
        )
    }

    pub async fn set_xattr(&self, request: SetXattrRequest) -> Result<InodeRecord> {
        let SetXattrRequest {
            caller_id,
            operation_id,
            caller,
            inode_id,
            expected_inode_revision,
            name,
            value,
            mode,
        } = request;
        require_id(&caller_id, "caller_id")?;
        require_id(&operation_id.0, "operation_id")?;
        require_id(&inode_id.0, "inode_id")?;
        let name = xattr_key(&name)?;
        if name == crate::dfs::LAZY_FILE_DESCRIPTOR_XATTR {
            return Err(operation_not_permitted("lazy source attribute is reserved"));
        }
        if let Some(inode) = self
            .replayed_inode(&caller_id, &operation_id, StoreOperation::DfsSetXattr)
            .await?
        {
            return Ok(inode);
        }
        let current = self.get_inode(inode_id).await?;
        if current.revision != expected_inode_revision {
            return Err(conflict("DFS inode revision changed before setxattr"));
        }
        ensure_xattr_permission(&caller, &current, &name, XattrAccess::Write)?;
        let exists = current.xattrs.contains_key(&name);
        match mode {
            XattrSetMode::Create if exists => {
                return Err(already_exists("DFS xattr already exists"));
            }
            XattrSetMode::Replace if !exists => {
                return Err(no_data("DFS xattr does not exist"));
            }
            _ => {}
        }
        let mut updated = current.clone();
        updated.xattrs.insert(name, value);
        updated.revision = updated.revision.saturating_add(1);
        self.commit_inode_metadata_mutation(
            caller_id,
            operation_id,
            StoreOperation::DfsSetXattr,
            current,
            updated,
        )
        .await
    }

    pub async fn remove_xattr(&self, request: RemoveXattrRequest) -> Result<InodeRecord> {
        let RemoveXattrRequest {
            caller_id,
            operation_id,
            caller,
            inode_id,
            expected_inode_revision,
            name,
        } = request;
        require_id(&caller_id, "caller_id")?;
        require_id(&operation_id.0, "operation_id")?;
        require_id(&inode_id.0, "inode_id")?;
        let name = xattr_key(&name)?;
        if name == crate::dfs::LAZY_FILE_DESCRIPTOR_XATTR {
            return Err(operation_not_permitted("lazy source attribute is reserved"));
        }
        if let Some(inode) = self
            .replayed_inode(&caller_id, &operation_id, StoreOperation::DfsRemoveXattr)
            .await?
        {
            return Ok(inode);
        }
        let current = self.get_inode(inode_id).await?;
        if current.revision != expected_inode_revision {
            return Err(conflict("DFS inode revision changed before removexattr"));
        }
        ensure_xattr_permission(&caller, &current, &name, XattrAccess::Write)?;
        if !current.xattrs.contains_key(&name) {
            return Err(no_data("DFS xattr does not exist"));
        }
        let mut updated = current.clone();
        updated.xattrs.remove(&name);
        updated.revision = updated.revision.saturating_add(1);
        self.commit_inode_metadata_mutation(
            caller_id,
            operation_id,
            StoreOperation::DfsRemoveXattr,
            current,
            updated,
        )
        .await
    }

    async fn commit_inode_metadata_mutation(
        &self,
        caller_id: String,
        operation_id: OperationId,
        operation: StoreOperation,
        current: InodeRecord,
        updated: InodeRecord,
    ) -> Result<InodeRecord> {
        let request = RequestKey::new(caller_id, operation_id.0.clone());
        let outcome = RequestOutcome {
            request: request.clone(),
            operation,
            result: OperationResult::DfsInode(updated.clone()),
        };
        let mut txn = MetaTxn::new(request.clone(), operation);
        txn.conditions.extend([
            TxnCondition::RequestAbsent(request),
            TxnCondition::EntityEquals(MetaEntity::DfsInode(current)),
        ]);
        txn.mutations.extend([
            TxnMutation::Put(MetaEntity::DfsInode(updated)),
            TxnMutation::RecordRequestOutcome(outcome),
        ]);
        inode_outcome(self.store.compare_and_commit(txn).await?, operation)
    }

    async fn directory_for_mutation(
        &self,
        namespace_id: &NamespaceId,
        parent: &InodeId,
    ) -> Result<Option<InodeRecord>> {
        let inode = if parent.0 == "1" {
            self.ensure_namespace_root(namespace_id).await?
        } else {
            self.get_inode(parent.clone()).await?
        };
        if inode.kind != InodeKind::Directory || inode.namespace_id != *namespace_id {
            return Err(not_directory(
                "DFS parent is not a directory in this namespace",
            ));
        }
        if inode.attributes.nlink == 0 {
            return Err(not_found("DFS namespace parent has been removed"));
        }
        Ok(Some(inode))
    }

    // The current GetInode API and mount root use one global root ID. Persist
    // that configured namespace root once; never alias a different namespace.
    pub(super) async fn ensure_namespace_root(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<InodeRecord> {
        let id = InodeId::new("1");
        if self
            .store
            .read(MetaRead::DfsInode(id.clone()))
            .await?
            .entity
            .is_none()
        {
            let inode = root_inode(namespace_id.clone());
            let request =
                RequestKey::new("afs-meta", format!("dfs-namespace-root:{}", namespace_id.0));
            let mut txn = MetaTxn::new(request.clone(), StoreOperation::DfsInitializeNamespace);
            txn.conditions.extend([
                TxnCondition::RequestAbsent(request.clone()),
                TxnCondition::Missing(MetaKey::DfsInode(id.clone())),
            ]);
            txn.mutations.extend([
                TxnMutation::Put(MetaEntity::DfsInode(inode.clone())),
                TxnMutation::RecordRequestOutcome(RequestOutcome {
                    request,
                    operation: StoreOperation::DfsInitializeNamespace,
                    result: OperationResult::DfsInode(inode),
                }),
            ]);
            // A concurrent initializer can win; the authoritative read below
            // checks the resulting namespace before any namespace mutation.
            self.store.compare_and_commit(txn).await?;
        }
        let inode = self.get_inode(id).await?;
        if inode.namespace_id != *namespace_id {
            return Err(invalid(
                "DFS root already belongs to another configured namespace",
            ));
        }
        Ok(inode)
    }

    async fn record_rename_noop(
        &self,
        caller_id: String,
        operation_id: OperationId,
        source_dentry: Dentry,
        source_inode: InodeRecord,
        request_digest: [u8; 32],
    ) -> Result<RenameOutcome> {
        let request = RequestKey::new(caller_id, operation_id.0);
        let value = RenameOutcome {
            inode: source_inode.clone(),
            replaced_inode: None,
        };
        let outcome = RequestOutcome {
            request: request.clone(),
            operation: StoreOperation::DfsRename,
            result: namespace_result(request_digest, OperationResult::DfsRename(value)),
        };
        let mut txn = MetaTxn::new(request.clone(), StoreOperation::DfsRename);
        txn.conditions.extend([
            TxnCondition::RequestAbsent(request),
            TxnCondition::EntityEquals(MetaEntity::DfsDentry(source_dentry)),
            TxnCondition::EntityEquals(MetaEntity::DfsInode(source_inode)),
        ]);
        txn.mutations
            .push(TxnMutation::RecordRequestOutcome(outcome));
        rename_outcome(
            validate_namespace_outcome(self.store.compare_and_commit(txn).await?, request_digest)?,
            StoreOperation::DfsRename,
        )
    }
}

/// What an existing rename target means for the rename.
enum RenameTargetRule {
    /// The target names the source inode: a recorded no-op.
    SameInode,
    /// The target inode is replaced; an empty directory target is pinned by
    /// a `DfsDirectoryEmpty` condition.
    Replace {
        target_inode: Box<InodeRecord>,
        empty_directory: Option<InodeId>,
    },
}

/// Replacement rules for an existing target: sticky parent, same inode,
/// directory/non-directory type match and empty directory.
async fn rename_target_rules(
    view: &MetaReadView,
    caller: &CallerContext,
    namespace_id: &NamespaceId,
    new_parent: Option<&InodeRecord>,
    source_inode: &InodeRecord,
    target: &Dentry,
) -> Result<RenameTargetRule> {
    let target_inode = inode_from_view(view, namespace_id, target.inode_id.clone()).await?;
    ensure_sticky_parent_allows(caller, new_parent, &target_inode)?;
    if target_inode.inode_id == source_inode.inode_id {
        return Ok(RenameTargetRule::SameInode);
    }
    let mut empty_directory = None;
    if source_inode.kind == InodeKind::Directory || target_inode.kind == InodeKind::Directory {
        if source_inode.kind == InodeKind::Directory && target_inode.kind != InodeKind::Directory {
            return Err(not_directory(
                "DFS rename cannot replace a directory with a non-directory",
            ));
        }
        if source_inode.kind != InodeKind::Directory && target_inode.kind == InodeKind::Directory {
            return Err(is_directory(
                "DFS rename cannot replace a non-directory with a directory",
            ));
        }
        let target_empty =
            directory_entries_from_view(view, namespace_id, &target_inode.inode_id).await?;
        if !target_empty.is_empty() {
            return Err(directory_not_empty(
                "DFS rename cannot replace a non-empty directory",
            ));
        }
        empty_directory = Some(target_inode.inode_id.clone());
    }
    Ok(RenameTargetRule::Replace {
        target_inode: Box::new(target_inode),
        empty_directory,
    })
}

/// Everything the rename transaction is assembled from, all read beforehand
/// except the replaced inode's original record (re-read from the same view).
struct RenamePlan {
    request: RequestKey,
    request_digest: [u8; 32],
    namespace_id: NamespaceId,
    old_key: DentryKey,
    new_key: DentryKey,
    new_parent_inode_id: InodeId,
    source_dentry: Dentry,
    source_inode: InodeRecord,
    target_dentry: Option<Dentry>,
    replaced_inode: Option<InodeRecord>,
    target_directory_empty_condition: Option<InodeId>,
    old_parent: Option<InodeRecord>,
    new_parent: Option<InodeRecord>,
    same_parent: bool,
}

async fn rename_txn(view: &MetaReadView, plan: RenamePlan) -> Result<MetaTxn> {
    let RenamePlan {
        request,
        request_digest,
        namespace_id,
        old_key,
        new_key,
        new_parent_inode_id,
        source_dentry,
        source_inode,
        target_dentry,
        mut replaced_inode,
        target_directory_empty_condition,
        old_parent,
        new_parent,
        same_parent,
    } = plan;
    let mut renamed_inode = source_inode.clone();
    renamed_inode.attributes.ctime_unix_ms = now_unix_ms();
    renamed_inode.revision = renamed_inode.revision.saturating_add(1);
    if let Some(inode) = replaced_inode.as_mut() {
        inode.attributes.nlink = if inode.kind == InodeKind::Directory {
            0
        } else {
            inode.attributes.nlink.saturating_sub(1)
        };
        inode.attributes.ctime_unix_ms = now_unix_ms();
        inode.revision = inode.revision.saturating_add(1);
    }
    let outcome_value = RenameOutcome {
        inode: renamed_inode.clone(),
        replaced_inode: replaced_inode.clone(),
    };
    let outcome = RequestOutcome {
        request: request.clone(),
        operation: StoreOperation::DfsRename,
        result: namespace_result(request_digest, OperationResult::DfsRename(outcome_value)),
    };
    let mut txn = MetaTxn::new(request.clone(), StoreOperation::DfsRename);
    txn.conditions.extend([
        TxnCondition::RequestAbsent(request),
        TxnCondition::EntityEquals(MetaEntity::DfsDentry(source_dentry.clone())),
        TxnCondition::EntityEquals(MetaEntity::DfsInode(source_inode.clone())),
        TxnCondition::Missing(MetaKey::DfsDentry(new_key.clone())),
    ]);
    if let Some(target) = &target_dentry {
        txn.conditions.pop();
        txn.conditions
            .push(TxnCondition::EntityEquals(MetaEntity::DfsDentry(
                target.clone(),
            )));
    }
    if source_inode.kind == InodeKind::Directory {
        txn.conditions.push(TxnCondition::DfsNotDescendant {
            namespace_id: namespace_id.clone(),
            ancestor_inode_id: source_inode.inode_id.clone(),
            child_inode_id: new_parent_inode_id.clone(),
        });
    }
    if let Some(target_inode_id) = target_directory_empty_condition {
        txn.conditions.push(TxnCondition::DfsDirectoryEmpty {
            namespace_id: namespace_id.clone(),
            parent_inode_id: target_inode_id,
        });
    }
    txn.mutations
        .push(TxnMutation::Delete(MetaKey::DfsDentry(old_key)));
    if let Some(target) = &target_dentry {
        txn.mutations
            .push(TxnMutation::Delete(MetaKey::DfsDentry(target.key.clone())));
        if let Some(inode) = replaced_inode.clone() {
            let original = inode_from_view(view, &namespace_id, inode.inode_id.clone()).await?;
            txn.conditions
                .push(TxnCondition::EntityEquals(MetaEntity::DfsInode(original)));
            txn.mutations
                .push(TxnMutation::Put(MetaEntity::DfsInode(inode)));
        }
    }
    txn.mutations
        .push(TxnMutation::Put(MetaEntity::DfsDentry(Dentry {
            key: new_key,
            inode_id: source_inode.inode_id.clone(),
        })));
    txn.mutations
        .push(TxnMutation::Put(MetaEntity::DfsInode(renamed_inode)));
    let moving_directory = source_inode.kind == InodeKind::Directory;
    let replacing_directory = replaced_inode
        .as_ref()
        .is_some_and(|inode| inode.kind == InodeKind::Directory);
    if same_parent {
        push_parent_namespace_change(
            &mut txn,
            old_parent,
            if replacing_directory { -1 } else { 0 },
        );
    } else {
        push_parent_namespace_change(&mut txn, old_parent, if moving_directory { -1 } else { 0 });
        push_parent_namespace_change(
            &mut txn,
            new_parent,
            i32::from(moving_directory) - i32::from(replacing_directory),
        );
    }
    txn.mutations
        .push(TxnMutation::RecordRequestOutcome(outcome));
    Ok(txn)
}
