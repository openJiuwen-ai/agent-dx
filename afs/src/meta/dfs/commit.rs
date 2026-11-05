//! GC fences, FileVersion commit and chunk receipt validation.

use super::*;

impl DfsService {
    pub async fn commit_file_version(
        &self,
        caller_id: String,
        commit: CommitFileVersion,
    ) -> Result<InodeRecord> {
        for _ in 0..FILE_COMMIT_RETRY_LIMIT {
            match self
                .commit_file_version_attempt(caller_id.clone(), commit.clone())
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

    async fn commit_file_version_attempt(
        &self,
        caller_id: String,
        commit: CommitFileVersion,
    ) -> Result<FileCommitAttempt> {
        let request_digest = namespace_request_digest(&commit)?;
        validate_commit_request(&caller_id, &commit)?;
        if let Some(inode) = self
            .replayed_namespace_inode(
                &caller_id,
                &commit.operation_id,
                StoreOperation::DfsCommitFileVersion,
                request_digest,
            )
            .await?
        {
            return Ok(FileCommitAttempt::Committed(inode));
        }
        let gc_snapshot = self.store.read(MetaRead::DfsGc).await?;
        let (current, current_lease) = self.commit_target(&caller_id, &commit).await?;
        let replication = self.current_replication_config().await?;
        let durability_policy = self.current_durability_policy().await?;
        let external_session = self.external_commit_session(&caller_id, &commit).await?;
        let generation_references = self
            .admitted_generation_references(&caller_id, &commit)
            .await?;
        let expected_base_layout = match commit.expected_head_version.as_ref() {
            Some(version_id) => Some(self.get_file_version(version_id.clone()).await?.1),
            None => None,
        };
        let clone_layout = self
            .clone_layout_for(&commit, &current, expected_base_layout.as_ref())
            .await?;
        let accepted_copies = self
            .accepted_receipt_copies(&caller_id, &commit, &replication, &durability_policy)
            .await?;
        validate_layout_extents(
            &commit,
            clone_layout.as_ref(),
            expected_base_layout.as_ref(),
        )?;
        let updated = committed_inode(&current, &commit)?;

        let request = RequestKey::new(caller_id.clone(), commit.operation_id.0.clone());
        let outcome = RequestOutcome {
            request: request.clone(),
            operation: StoreOperation::DfsCommitFileVersion,
            result: namespace_result(request_digest, OperationResult::DfsInode(updated.clone())),
        };
        let mut txn = MetaTxn::new(request.clone(), StoreOperation::DfsCommitFileVersion);
        txn.conditions.extend([
            TxnCondition::RequestAbsent(request),
            TxnCondition::EntityEquals(MetaEntity::DfsInode(current)),
            TxnCondition::EntityEquals(MetaEntity::DfsWriteLease(current_lease)),
            TxnCondition::EntityEquals(MetaEntity::DfsReplicationConfig(replication.clone())),
            TxnCondition::Missing(MetaKey::DfsFileVersion(commit.file_version.id.clone())),
            TxnCondition::Missing(MetaKey::DfsLayoutRoot(commit.layout_root.id.clone())),
        ]);
        push_commit_gc_fence(
            &mut txn,
            &caller_id,
            &commit,
            &generation_references,
            &gc_snapshot,
        );
        if let Some(MetaEntity::DfsGc(gc)) = gc_snapshot.entity
            && commit
                .layout_root
                .inline_extents
                .iter()
                .any(|extent| gc.chunks.contains(&extent.chunk_id))
        {
            return Err(conflict("commit references a chunk pending deletion"));
        }
        push_commit_reference_mutations(&mut txn, generation_references);
        if let Some(session) = external_session {
            txn.conditions.extend([
                TxnCondition::EntityEquals(MetaEntity::DfsDurabilityPolicy(durability_policy)),
                TxnCondition::NodeSessionCurrent {
                    node_id: session.node_id,
                    session_id: session.session_id,
                },
            ]);
        }
        self.push_receipt_mutations(&mut txn, &commit, accepted_copies, &replication);
        txn.mutations.extend([
            TxnMutation::Put(MetaEntity::DfsLayoutRoot(commit.layout_root)),
            TxnMutation::Put(MetaEntity::DfsFileVersion(commit.file_version)),
            TxnMutation::Put(MetaEntity::DfsInode(updated)),
            TxnMutation::RecordRequestOutcome(outcome),
        ]);
        file_commit_attempt(
            validate_namespace_outcome(self.store.compare_and_commit(txn).await?, request_digest)?,
            StoreOperation::DfsCommitFileVersion,
        )
    }

    /// Reads the inode and the presented write lease and checks that the
    /// commit still describes one file at the expected head.
    async fn commit_target(
        &self,
        caller_id: &str,
        commit: &CommitFileVersion,
    ) -> Result<(InodeRecord, WriteLease)> {
        let current = self.get_inode(commit.inode_id.clone()).await?;
        if commit.write_lease.inode_id != commit.inode_id
            || commit.write_lease.owner_node_id != caller_id
        {
            return Err(conflict(
                "DFS commit lease does not belong to caller and inode",
            ));
        }
        let current_lease = self
            .validate_write_lease(caller_id, &commit.inode_id, &commit.write_lease)
            .await?;
        if current.revision < commit.expected_inode_revision
            || current.head_version != commit.expected_head_version
        {
            return Err(conflict("DFS inode head changed before commit"));
        }
        if commit.file_version.inode_id != commit.inode_id
            || commit.file_version.layout_root != commit.layout_root.id
            || commit.file_version.length != commit.layout_root.file_length
            || commit.file_version.parent_version != commit.expected_head_version
        {
            return Err(invalid(
                "FileVersion and LayoutRoot do not describe one file",
            ));
        }
        Ok((current, current_lease))
    }

    async fn external_commit_session(
        &self,
        caller_id: &str,
        commit: &CommitFileVersion,
    ) -> Result<Option<NodeSession>> {
        if !self.is_external_backend() {
            return Ok(None);
        }
        let session = self.current_live_session(caller_id, now_unix_ms()).await?;
        if session.session_id != commit.write_lease.owner_session_id {
            return Err(conflict(
                "external receipt writer does not own the current Node session",
            ));
        }
        Ok(Some(session))
    }

    /// Under a write-generation fence, every generation this commit publishes
    /// must still be admitted to the writer's session. Empty otherwise.
    async fn admitted_generation_references(
        &self,
        caller_id: &str,
        commit: &CommitFileVersion,
    ) -> Result<Vec<crate::dfs::GcReference>> {
        let mut generation_references = Vec::new();
        if commit.gc_fence != Some(crate::dfs::GcFence::WriteGeneration) {
            return Ok(generation_references);
        }
        let session = self.current_live_session(caller_id, now_unix_ms()).await?;
        if session.session_id != commit.write_lease.owner_session_id {
            return Err(conflict("write generation owner session changed"));
        }
        let view = self.store.read_view().await?;
        let mut generations = BTreeSet::from([ChunkObject::generation_id(&commit.operation_id)]);
        for receipt in &commit.chunk_receipts {
            let generation = receipt
                .chunk
                .write_generation()
                .ok_or_else(|| invalid("generation receipts require CRC identities"))?;
            if generation != ChunkObject::generation_id(&receipt.operation_id) {
                return Err(invalid(
                    "receipt operation differs from its birth generation",
                ));
            }
            generations.insert(generation.to_owned());
        }
        for generation in generations {
            let key = MetaKey::DfsReference {
                node_id: caller_id.to_owned(),
                session_id: commit.write_lease.owner_session_id.clone(),
                reference_id: format!("write:{generation}"),
            };
            let Some(MetaEntity::DfsReference(reference)) = view.lookup(&key)?.entity else {
                return Err(Error::coded(
                    afs_error::META_DFS_GC_CHANGED,
                    "write generation is not admitted",
                ));
            };
            if !reference.publication
                || reference.retired_at_unix_ms.is_some()
                || reference.inode_id.as_ref() != Some(&commit.inode_id)
                || reference.node_id != caller_id
                || reference.session_id != commit.write_lease.owner_session_id
            {
                return Err(Error::coded(
                    afs_error::META_DFS_GC_CHANGED,
                    "write generation has been retired",
                ));
            }
            generation_references.push(reference);
        }
        Ok(generation_references)
    }

    /// COW publishes a distinct destination version, sharing immutable chunks.
    /// A caller cannot introduce arbitrary chunk references via this path.
    async fn clone_layout_for(
        &self,
        commit: &CommitFileVersion,
        current: &InodeRecord,
        expected_base_layout: Option<&LayoutRoot>,
    ) -> Result<Option<LayoutRoot>> {
        let Some(range) = &commit.clone_source else {
            return Ok(None);
        };
        let (source, layout) = self.get_file_version(range.version.clone()).await?;
        let source_inode = self.get_inode(source.inode_id.clone()).await?;
        let empty = LayoutRoot {
            id: crate::dfs::LayoutRootId::new("empty"),
            file_length: 0,
            inline_extents: vec![],
        };
        let expected = crate::dfs::clone_file_layout(
            expected_base_layout.unwrap_or(&empty),
            &layout,
            range,
            commit.layout_root.id.clone(),
        )?;
        if source_inode.namespace_id != current.namespace_id
            || source_inode.kind != InodeKind::Regular
            || source.inode_id == current.inode_id
            || expected != commit.layout_root
            || !commit.chunk_receipts.is_empty()
        {
            return Err(invalid(
                "COW clone must preserve the declared same-namespace source range",
            ));
        }
        Ok(Some(expected))
    }

    /// Validates every receipt in order; one accepted copy set per receipt.
    async fn accepted_receipt_copies(
        &self,
        caller_id: &str,
        commit: &CommitFileVersion,
        replication: &ReplicationConfig,
        durability_policy: &DfsDurabilityPolicy,
    ) -> Result<Vec<Vec<CopyRecord>>> {
        let mut accepted_copies = Vec::with_capacity(commit.chunk_receipts.len());
        let mut receipt_chunks = HashSet::new();
        for receipt in &commit.chunk_receipts {
            if receipt.operation_id != commit.operation_id
                && commit.gc_fence != Some(crate::dfs::GcFence::WriteGeneration)
            {
                return Err(invalid("ChunkReceipt operation does not match commit"));
            }
            if !receipt_chunks.insert(receipt.chunk.id.clone()) {
                return Err(invalid("commit contains duplicate ChunkReceipts"));
            }
            accepted_copies.push(if self.is_external_backend() {
                self.validate_external_chunk_receipt(receipt, durability_policy)?
            } else {
                self.validate_chunk_receipt(caller_id, receipt, replication)
                    .await?
            });
        }
        Ok(accepted_copies)
    }

    /// Records each receipt's chunk, copies and placement; registers the
    /// replica debt of an under-replicated Node placement as a Pending task.
    fn push_receipt_mutations(
        &self,
        txn: &mut MetaTxn,
        commit: &CommitFileVersion,
        accepted_copies: Vec<Vec<CopyRecord>>,
        replication: &ReplicationConfig,
    ) {
        for (receipt, copies) in commit.chunk_receipts.iter().zip(accepted_copies) {
            txn.mutations.push(TxnMutation::Put(MetaEntity::DfsChunk(
                receipt.chunk.clone(),
            )));
            for copy in &copies {
                txn.mutations
                    .push(TxnMutation::Put(MetaEntity::DfsCopy(copy.clone())));
            }
            let copy_ids = copies
                .iter()
                .map(|copy| copy.id.clone())
                .collect::<Vec<_>>();
            let health = if copies.len() >= usize::from(replication.desired_copies) {
                PlacementHealth::Satisfied
            } else {
                PlacementHealth::UnderReplicated
            };
            txn.mutations
                .push(TxnMutation::Put(MetaEntity::DfsPlacement(
                    PlacementRecord {
                        chunk_id: receipt.chunk.id.clone(),
                        replica_group_id: if self.is_external_backend() {
                            ReplicaGroupId::new("external")
                        } else {
                            receipt.replica_group_id.clone()
                        },
                        placement_epoch: if self.is_external_backend() {
                            1
                        } else {
                            receipt.placement_epoch
                        },
                        desired_copies: replication.desired_copies,
                        copies: copy_ids.clone(),
                        health,
                    },
                )));
            if !self.is_external_backend() && copies.len() < usize::from(replication.desired_copies)
            {
                txn.mutations
                    .push(TxnMutation::Put(MetaEntity::DfsReplicationTask(
                        pending_repair_task(
                            &receipt.chunk.id,
                            receipt.placement_epoch,
                            replication.desired_copies,
                            copy_ids,
                            now_unix_ms(),
                        ),
                    )));
            }
        }
    }

    fn validate_external_chunk_receipt(
        &self,
        receipt: &crate::dfs::ChunkReceipt,
        policy: &DfsDurabilityPolicy,
    ) -> Result<Vec<CopyRecord>> {
        let DfsDurabilityPolicy::ExternalBackend {
            store_id,
            generation,
        } = policy
        else {
            return Err(invalid(
                "external receipt requires the external durability policy",
            ));
        };
        let external = receipt
            .external_commit
            .as_ref()
            .ok_or_else(|| invalid("external durability requires an external commit receipt"))?;
        let key = crate::dfs::external_object_key(store_id, generation, &receipt.chunk.id)?;
        let canonical_chunk = format!(
            "blake3-{}-{}",
            blake3::Hash::from(receipt.chunk.content_digest.bytes).to_hex(),
            receipt.chunk.length
        );
        if !receipt.durable_acks.is_empty()
            || receipt.placement_revision != 0
            || receipt.placement_epoch != 0
            || !receipt.replica_group_id.0.is_empty()
            || external.store_id != *store_id
            || external.generation != *generation
            || external.object_key != key
            || external.persisted_bytes != receipt.chunk.length
            || external.verified_digest != receipt.chunk.content_digest
            || receipt.chunk.id.0 != canonical_chunk
            || receipt.chunk.length == 0
        {
            return Err(invalid(
                "external receipt does not prove the configured immutable Chunk",
            ));
        }
        // Authenticated service Nodes are trusted to issue this receipt only
        // after the configured PUT succeeds, just as replica Nodes issue ACKs
        // only after persistence. No arbitrary store, key or Node ACK is accepted.
        Ok(vec![CopyRecord {
            id: CopyId::new(format!(
                "external:{store_id}:{generation}:{}",
                receipt.chunk.id.0
            )),
            chunk_id: receipt.chunk.id.clone(),
            role: CopyRole::ExternalCommitted,
            location: CopyLocation::External {
                store_id: store_id.clone(),
                object_key: key,
                object_revision: generation.clone(),
            },
            state: CopyState::Ready,
            persisted_bytes: receipt.chunk.length,
            verified_digest: receipt.chunk.content_digest.clone(),
        }])
    }

    /// Re-reads the current replication config at commit, not the snapshot the
    /// writer used. That is sound only while effective equals declared: once a
    /// later stage can lower the effective local-copy rule, a receipt written
    /// under one value could be judged under another. That stage must bind the
    /// value to the receipt's placement epoch, which already mixes it in.
    pub(super) async fn validate_chunk_receipt(
        &self,
        caller_id: &str,
        receipt: &crate::dfs::ChunkReceipt,
        replication: &ReplicationConfig,
    ) -> Result<Vec<CopyRecord>> {
        if receipt.external_commit.is_some() {
            return Err(invalid(
                "Node replication does not accept external commit receipts",
            ));
        }
        if receipt.placement_revision == 0
            || receipt.placement_epoch == 0
            || receipt.durable_acks.len() < usize::from(replication.sync_required_copies)
        {
            return Err(invalid(
                "ChunkReceipt does not satisfy the initialized replication config",
            ));
        }

        let mut nodes = HashSet::new();
        let mut failure_domains = HashSet::new();
        let mut copies = Vec::with_capacity(receipt.durable_acks.len());
        let placement = self.placement_snapshot(caller_id.to_owned()).await?;
        if placement.replication != *replication || receipt.placement_revision > placement.revision
        {
            return Err(conflict(
                "ChunkReceipt does not match the current replication authority",
            ));
        }
        let group = placement
            .replica_groups
            .iter()
            .find(|group| group.id == receipt.replica_group_id)
            .filter(|group| group.placement_epoch == receipt.placement_epoch)
            .ok_or_else(|| conflict("ChunkReceipt references a stale ReplicaGroup"))?;
        for ack in &receipt.durable_acks {
            if ack.operation_id != receipt.operation_id
                || ack.chunk_id != receipt.chunk.id
                || ack.placement_revision != receipt.placement_revision
                || ack.placement_epoch != receipt.placement_epoch
                || ack.persisted_bytes != receipt.chunk.length
                || ack.verified_digest != receipt.chunk.content_digest
            {
                return Err(invalid("ReplicaAck does not prove its ChunkReceipt"));
            }
            if !nodes.insert(ack.node_id.clone()) {
                return Err(invalid(
                    "ChunkReceipt contains duplicate Node acknowledgements",
                ));
            }
            group
                .targets
                .iter()
                .find(|target| {
                    target.node_id == ack.node_id
                        && target.node_epoch == ack.node_epoch
                        && target.device.device_id == ack.device_id
                        && target.device.device_epoch == ack.device_epoch
                })
                .ok_or_else(|| {
                    conflict("ReplicaAck target is not assigned by the current ReplicaGroup")
                })?;
            // This snapshot may include unrelated catalog advances after the
            // write grant. The receiver enforces the frozen grant's floor;
            // comparing that ACK against today's floor would reject valid data.
            let session_snapshot = self
                .store
                .read(MetaRead::CurrentNodeSession {
                    node_id: ack.node_id.clone(),
                })
                .await?;
            let session = match session_snapshot.entity {
                Some(MetaEntity::NodeSession(session))
                    if session.lease_epoch == ack.node_epoch
                        && session.is_live_at_unix_ms(now_unix_ms()) =>
                {
                    session
                }
                _ => return Err(conflict("ReplicaAck references a stale Node epoch")),
            };
            let device = session
                .storage_devices
                .iter()
                .find(|device| {
                    device.device_id == ack.device_id && device.device_epoch == ack.device_epoch
                })
                .ok_or_else(|| conflict("ReplicaAck references a stale storage device"))?;
            failure_domains.insert(device.failure_domain.clone());
            copies.push(CopyRecord {
                id: CopyId::new(format!(
                    "{}:{}:{}:{}",
                    ack.node_id, ack.node_epoch, ack.device_id, receipt.chunk.id.0
                )),
                chunk_id: receipt.chunk.id.clone(),
                role: CopyRole::DurableReplica,
                location: CopyLocation::Node {
                    node_id: ack.node_id.clone(),
                    node_epoch: ack.node_epoch,
                    device_id: ack.device_id.clone(),
                    device_epoch: ack.device_epoch,
                    catalog_revision: ack.catalog_revision,
                },
                state: CopyState::Ready,
                persisted_bytes: ack.persisted_bytes,
                verified_digest: ack.verified_digest.clone(),
            });
        }

        if nodes.len() < usize::from(replication.min_distinct_nodes)
            || failure_domains.len() < usize::from(replication.min_distinct_failure_domains)
            || (EffectiveLocalCopy::declared(replication).requires_local()
                && !nodes.contains(caller_id))
        {
            return Err(invalid(
                "ChunkReceipt does not satisfy Node, failure-domain or local-copy constraints",
            ));
        }
        Ok(copies)
    }
}

fn extent_is_inherited(extent: &crate::dfs::Extent, base: &LayoutRoot) -> bool {
    let Some(extent_file_end) = extent.file_offset.checked_add(extent.length) else {
        return false;
    };
    base.inline_extents.iter().any(|candidate| {
        let Some(candidate_file_end) = candidate.file_offset.checked_add(candidate.length) else {
            return false;
        };
        if candidate.chunk_id != extent.chunk_id
            || extent.file_offset < candidate.file_offset
            || extent_file_end > candidate_file_end
        {
            return false;
        }
        candidate
            .chunk_offset
            .checked_add(extent.file_offset - candidate.file_offset)
            == Some(extent.chunk_offset)
    })
}

fn validate_commit_request(caller_id: &str, commit: &CommitFileVersion) -> Result<()> {
    require_id(caller_id, "caller_id")?;
    require_id(&commit.operation_id.0, "operation_id")?;
    require_id(&commit.inode_id.0, "inode_id")?;
    require_id(
        &commit.write_lease.owner_node_id,
        "write_lease.owner_node_id",
    )?;
    require_id(
        &commit.write_lease.owner_session_id,
        "write_lease.owner_session_id",
    )?;
    require_id(&commit.file_version.id.0, "file_version.id")?;
    require_id(&commit.layout_root.id.0, "layout_root.id")?;
    Ok(())
}

/// Every extent is non-empty, ordered, inside the file, and backed either by
/// a newly durable receipt chunk or by an inherited clone/base range.
fn validate_layout_extents(
    commit: &CommitFileVersion,
    clone_layout: Option<&LayoutRoot>,
    expected_base_layout: Option<&LayoutRoot>,
) -> Result<()> {
    let mut previous_end = 0;
    for extent in &commit.layout_root.inline_extents {
        let file_end = extent
            .file_offset
            .checked_add(extent.length)
            .ok_or_else(|| invalid("DFS extent file range overflows"))?;
        if extent.length == 0
            || extent.file_offset < previous_end
            || file_end > commit.file_version.length
        {
            return Err(invalid(
                "LayoutRoot extents must be non-empty, ordered and non-overlapping",
            ));
        }
        let chunk_end = extent
            .chunk_offset
            .checked_add(extent.length)
            .ok_or_else(|| invalid("DFS extent chunk range overflows"))?;
        if let Some(receipt) = commit
            .chunk_receipts
            .iter()
            .find(|receipt| receipt.chunk.id == extent.chunk_id)
        {
            if chunk_end > receipt.chunk.length {
                return Err(invalid("DFS extent exceeds its referenced Chunk"));
            }
        } else if !clone_layout.is_some_and(|layout| extent_is_inherited(extent, layout))
            && !expected_base_layout.is_some_and(|layout| extent_is_inherited(extent, layout))
        {
            return Err(invalid(
                "LayoutRoot references neither a newly durable Chunk nor a legal expected-base range",
            ));
        }
        previous_end = file_end;
    }
    Ok(())
}

/// The inode as it becomes once this version is the head.
fn committed_inode(current: &InodeRecord, commit: &CommitFileVersion) -> Result<InodeRecord> {
    let mut updated = current.clone();
    updated.head_version = Some(commit.file_version.id.clone());
    updated.revision = updated.revision.saturating_add(1);
    match commit.metadata_delta.mode {
        CommitMetadataMode::DataOnly => {}
        CommitMetadataMode::Full => {
            updated.attributes.mtime_unix_ms = commit
                .metadata_delta
                .mtime_unix_ms
                .ok_or_else(|| invalid("DFS full commit requires mtime"))?;
            updated.attributes.ctime_unix_ms = commit
                .metadata_delta
                .ctime_unix_ms
                .ok_or_else(|| invalid("DFS full commit requires ctime"))?;
        }
    }
    if commit.metadata_delta.kill_suidgid {
        let ctime = commit
            .metadata_delta
            .ctime_unix_ms
            .ok_or_else(|| invalid("DFS killpriv commit requires ctime"))?;
        let cleared = cleared_write_privileges(updated.attributes.mode);
        if cleared != updated.attributes.mode {
            updated.attributes.mode = cleared;
            updated.attributes.ctime_unix_ms = ctime;
        }
    }
    Ok(updated)
}

/// The GC fence of a commit: admitted generation references plus the writer
/// session, otherwise the GC state revision (or its absence).
fn push_commit_gc_fence(
    txn: &mut MetaTxn,
    caller_id: &str,
    commit: &CommitFileVersion,
    generation_references: &[crate::dfs::GcReference],
    gc_snapshot: &super::super::store::MetaSnapshot,
) {
    if !generation_references.is_empty() {
        for reference in generation_references {
            txn.conditions
                .push(TxnCondition::EntityEquals(MetaEntity::DfsReference(
                    reference.clone(),
                )));
        }
        txn.conditions.push(TxnCondition::NodeSessionCurrent {
            node_id: caller_id.to_owned(),
            session_id: commit.write_lease.owner_session_id.clone(),
        });
    } else if gc_snapshot.entity.is_some() {
        txn.conditions.push(TxnCondition::RevisionEquals {
            key: MetaKey::DfsGc,
            revision: gc_snapshot.revision,
        });
    } else {
        txn.conditions.push(TxnCondition::Missing(MetaKey::DfsGc));
    }
}

fn push_commit_reference_mutations(
    txn: &mut MetaTxn,
    generation_references: Vec<crate::dfs::GcReference>,
) {
    for mut reference in generation_references {
        // The version becomes reachable in this same transaction. Retain a
        // retirement marker through the maximum delayed-admission window.
        reference.retired_at_unix_ms = Some(now_unix_ms());
        txn.mutations
            .push(TxnMutation::Put(MetaEntity::DfsReference(reference)));
    }
}
