//! Reachability GC and durable deletion barriers. A lineage pointer is not ownership.
use super::*;
use crate::dfs::{ChunkId, GcReference, GcState, UpdateGcReference};
use std::{
    collections::{BTreeSet, hash_map::DefaultHasher},
    hash::{Hash, Hasher},
};
static GC_SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
const MAX_CHUNKS: usize = 1024;
#[derive(Default)]
struct GcPlan {
    keys: Vec<MetaKey>,
    chunks: Vec<ChunkId>,
}
fn plan_collection(
    entities: &[MetaEntity],
    references: &[GcReference],
    now: u64,
    grace: u64,
) -> GcPlan {
    let protected: BTreeSet<_> = references
        .iter()
        .filter(|reference| {
            reference
                .retired_at_unix_ms
                .is_none_or(|time| time.saturating_add(grace) > now)
        })
        .filter_map(|reference| reference.inode_id.clone())
        .collect();
    let mut protected_inodes = protected;
    for entity in entities {
        if let MetaEntity::DfsWriteLease(lease) = entity
            && lease.expires_at_unix_ms > now
        {
            protected_inodes.insert(lease.inode_id.clone());
        }
    }
    let heads: BTreeSet<_> = entities
        .iter()
        .filter_map(|entity| match entity {
            MetaEntity::DfsInode(inode)
                if inode.attributes.nlink > 0 || protected_inodes.contains(&inode.inode_id) =>
            {
                inode.head_version.clone()
            }
            _ => None,
        })
        .collect();
    let mut versions = BTreeSet::new();
    let mut layouts = BTreeSet::new();
    for entity in entities {
        if let MetaEntity::DfsFileVersion(version) = entity
            && (heads.contains(&version.id) || protected_inodes.contains(&version.inode_id))
        {
            versions.insert(version.id.clone());
            layouts.insert(version.layout_root.clone());
        }
    }
    let mut chunks = BTreeSet::new();
    for entity in entities {
        if let MetaEntity::DfsLayoutRoot(layout) = entity
            && layouts.contains(&layout.id)
        {
            chunks.extend(
                layout
                    .inline_extents
                    .iter()
                    .map(|extent| extent.chunk_id.clone()),
            );
        }
        if let MetaEntity::DfsReplicationTask(task) = entity
            && task
                .claim
                .as_ref()
                .is_some_and(|claim| claim.expires_at_unix_ms > now)
        {
            chunks.insert(task.chunk_id.clone());
        }
    }
    for reference in references.iter().filter(|reference| {
        reference
            .retired_at_unix_ms
            .is_none_or(|time| time.saturating_add(grace) > now)
    }) {
        chunks.extend(reference.chunks.iter().map(|chunk| chunk.id.clone()));
    }
    // Inventory can arrive from any replica before CommitFileVersion. One
    // admitted write generation protects all of its independently named objects.
    let generations: BTreeSet<_> = references
        .iter()
        .filter(|reference| {
            reference.publication
                && reference
                    .retired_at_unix_ms
                    .is_none_or(|time| time.saturating_add(grace) > now)
        })
        .filter_map(|reference| {
            reference
                .reference_id
                .strip_prefix("write:")
                .map(str::to_owned)
                .or_else(|| {
                    reference
                        .reference_id
                        .strip_prefix("publish:")
                        .map(|operation| ChunkObject::generation_id(&OperationId::new(operation)))
                })
        })
        .collect();
    for entity in entities {
        if let MetaEntity::DfsChunk(chunk) = entity
            && chunk
                .write_generation()
                .is_some_and(|generation| generations.contains(generation))
        {
            chunks.insert(chunk.id.clone());
        }
    }
    let reclaim: BTreeSet<_> = entities
        .iter()
        .filter_map(|entity| match entity {
            MetaEntity::DfsChunk(chunk) if !chunks.contains(&chunk.id) => Some(chunk.id.clone()),
            _ => None,
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .take(MAX_CHUNKS)
        .collect();
    let mut plan = GcPlan {
        keys: Vec::new(),
        chunks: reclaim.iter().cloned().collect(),
    };
    for entity in entities {
        let dead = match entity {
            MetaEntity::DfsInode(inode) => {
                inode.attributes.nlink == 0 && !protected_inodes.contains(&inode.inode_id)
            }
            MetaEntity::DfsFileVersion(version) => !versions.contains(&version.id),
            MetaEntity::DfsLayoutRoot(layout) => !layouts.contains(&layout.id),
            MetaEntity::DfsChunk(chunk) => reclaim.contains(&chunk.id),
            MetaEntity::DfsPlacement(placement) => reclaim.contains(&placement.chunk_id),
            MetaEntity::DfsCopy(copy) => reclaim.contains(&copy.chunk_id),
            MetaEntity::DfsReplicationTask(task) => reclaim.contains(&task.chunk_id),
            MetaEntity::DfsWriteLease(lease) => lease.expires_at_unix_ms <= now,
            MetaEntity::DfsReference(reference) => {
                reference.retired_at_unix_ms.is_some_and(|time| {
                    time.saturating_add(if reference.reference_id.starts_with("write:") {
                        grace.max(120_000)
                    } else {
                        grace
                    }) <= now
                })
            }
            _ => false,
        };
        if dead {
            plan.keys.push(crate::meta::store::entity_key(entity));
        }
    }
    plan
}
fn reference_entity(
    view: &crate::meta::store::MetaReadView,
    key: MetaKey,
    conditions: &mut Vec<TxnCondition>,
) -> Result<Option<MetaEntity>> {
    let snapshot = view.lookup(&key)?;
    conditions.push(if snapshot.entity.is_some() {
        TxnCondition::RevisionEquals {
            key,
            revision: snapshot.revision,
        }
    } else {
        TxnCondition::Missing(key)
    });
    Ok(snapshot.entity)
}
fn ref_key(node: &str, session: &str, id: &str) -> MetaKey {
    MetaKey::DfsReference {
        node_id: node.into(),
        session_id: session.into(),
        reference_id: id.into(),
    }
}
fn gc_state(entities: &[MetaEntity]) -> GcState {
    entities
        .iter()
        .find_map(|entity| match entity {
            MetaEntity::DfsGc(state) => Some(state.clone()),
            _ => None,
        })
        .unwrap_or_default()
}
fn live_caller(entities: &[MetaEntity], node: &str, session: &str) -> Result<()> {
    let current = entities
        .iter()
        .filter_map(|entity| match entity {
            MetaEntity::NodeSession(value) if value.node_id == node => Some(value),
            _ => None,
        })
        .max_by_key(|value| value.lease_epoch);
    if current
        .is_some_and(|value| value.session_id == session && value.is_live_at_unix_ms(now_unix_ms()))
    {
        Ok(())
    } else {
        Err(permission_denied(
            "GC requires the current live Node session",
        ))
    }
}
impl DfsService {
    #[cfg(test)]
    fn with_gc_grace_ms(mut self, grace: u64) -> Self {
        self.gc_grace_ms = grace;
        self
    }
    pub async fn update_gc_reference(&self, mut update: UpdateGcReference) -> Result<()> {
        require_id(&update.node_id, "node_id")?;
        require_id(&update.session_id, "session_id")?;
        require_id(&update.reference_id, "reference_id")?;
        if self.is_external_backend() {
            return Err(invalid(
                "native GC references require NodeReplicated storage",
            ));
        }
        // On the wire the original timestamped operation fixes admission age.
        // Persist the hash key that immutable chunk identities can address.
        let write_operation = update
            .reference_id
            .strip_prefix("write:")
            .map(str::to_owned);
        if let Some(operation) = &write_operation {
            update.reference_id = format!(
                "write:{}",
                ChunkObject::generation_id(&OperationId::new(operation))
            );
        }
        let key = ref_key(&update.node_id, &update.session_id, &update.reference_id);
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        // Concurrent uploads append to one publication reference. Queue them
        // fairly instead of making each append lose up to 64 CAS attempts.
        // Fixed shards bound memory; durable conditions still fence GC, other
        // service instances, session replacement and cancelled requests.
        let shard = hasher.finish() as usize % self.gc_reference_updates.len();
        let _reference_update = self.gc_reference_updates[shard].lock().await;
        for _ in 0..64 {
            let view = self.store.read_view().await?;
            // Keep all decisions on one pinned view. Reference updates touch a
            // few keys; cloning every layout/chunk here made admission scale
            // with the entire namespace, including each CAS retry.
            let caller = view
                .lookup(&MetaKey::CurrentNodeSession {
                    node_id: update.node_id.clone(),
                })?
                .entity;
            live_caller(caller.as_slice(), &update.node_id, &update.session_id)?;
            let mut conditions = vec![TxnCondition::NodeSessionCurrent {
                node_id: update.node_id.clone(),
                session_id: update.session_id.clone(),
            }];
            let existing = match reference_entity(&view, key.clone(), &mut conditions)? {
                Some(MetaEntity::DfsReference(reference)) => Some(reference),
                _ => None,
            };
            let mut reference = if let Some(reference) = existing {
                reference
            } else if update.release {
                // Persist a tombstone even when admission has not become visible.
                // This CAS fences a queued/lost-reply retain on the same key.
                let tombstone = GcReference {
                    node_id: update.node_id.clone(),
                    session_id: update.session_id.clone(),
                    reference_id: update.reference_id.clone(),
                    inode_id: None,
                    publication: false,
                    chunks: Vec::new(),
                    retired_at_unix_ms: Some(now_unix_ms()),
                };
                if self
                    .gc_commit_checked(
                        conditions,
                        StoreOperation::DfsUpdateReference,
                        vec![TxnMutation::Put(MetaEntity::DfsReference(tombstone))],
                    )
                    .await?
                {
                    return Ok(());
                }
                continue;
            } else {
                let now = now_unix_ms();
                if update.admission_deadline_unix_ms <= now {
                    return Err(afs_error::Error::coded(
                        afs_error::META_DFS_REFERENCE_ADMISSION_EXPIRED,
                        "reference admission deadline expired",
                    ));
                }
                if let Some(operation) = &write_operation {
                    let created = operation
                        .strip_prefix("afs1-")
                        .and_then(|value| value.split_once('-'))
                        .and_then(|(time, _)| time.parse::<u64>().ok())
                        .ok_or_else(|| {
                            invalid("write generation requires a timestamped operation identity")
                        })?;
                    if created > now.saturating_add(60_000)
                        || update.admission_deadline_unix_ms > created.saturating_add(60_000)
                    {
                        return Err(afs_error::Error::coded(
                            afs_error::META_DFS_REFERENCE_ADMISSION_EXPIRED,
                            "write generation admission deadline cannot be refreshed",
                        ));
                    }
                }
                if update.admission_deadline_unix_ms > now.saturating_add(60_000) {
                    return Err(invalid("reference admission exceeds the 60s cleanup fence"));
                }
                let inode_id = update
                    .inode_id
                    .clone()
                    .ok_or_else(|| conflict("publication reference was already retired"))?;
                if !matches!(
                    reference_entity(&view, MetaKey::DfsInode(inode_id.clone()), &mut conditions)?,
                    Some(MetaEntity::DfsInode(_))
                ) {
                    return Err(not_found("cannot retain a collected inode"));
                }
                if let Some(version) = &update.version_id
                    && !matches!(reference_entity(&view, MetaKey::DfsFileVersion(version.clone()), &mut conditions)?,
                        Some(MetaEntity::DfsFileVersion(value)) if value.inode_id == inode_id)
                {
                    return Err(not_found(
                        "open version was collected before reference admission",
                    ));
                }
                GcReference {
                    node_id: update.node_id.clone(),
                    session_id: update.session_id.clone(),
                    reference_id: update.reference_id.clone(),
                    inode_id: Some(inode_id),
                    publication: update.publication,
                    chunks: Vec::new(),
                    retired_at_unix_ms: None,
                }
            };
            if reference.retired_at_unix_ms.is_some() {
                return if update.release {
                    Ok(())
                } else {
                    Err(conflict("retired reference cannot be reused"))
                };
            }
            if !update.release
                && (update
                    .inode_id
                    .as_ref()
                    .is_some_and(|inode| reference.inode_id.as_ref() != Some(inode))
                    || update.publication != reference.publication)
            {
                return Err(invalid("reference identity was reused for another access"));
            }
            let gc = match reference_entity(&view, MetaKey::DfsGc, &mut conditions)? {
                Some(MetaEntity::DfsGc(gc)) => gc,
                _ => GcState::default(),
            };
            if update
                .chunks
                .iter()
                .any(|chunk| gc.chunks.contains(&chunk.id))
            {
                return Err(afs_error::Error::coded(
                    afs_error::META_DFS_GC_BUSY,
                    "chunk deletion must finish before publication",
                ));
            }
            if !update.chunks.is_empty() && !reference.publication {
                return Err(invalid("chunks require a publication reference"));
            }
            let mut mutations = Vec::new();
            for chunk in &update.chunks {
                if let Some(MetaEntity::DfsChunk(known)) =
                    reference_entity(&view, MetaKey::DfsChunk(chunk.id.clone()), &mut conditions)?
                {
                    if &known != chunk {
                        return Err(invalid("immutable chunk identity changed"));
                    }
                } else {
                    mutations.push(TxnMutation::Put(MetaEntity::DfsChunk(chunk.clone())));
                }
                if let Some(known) = reference.chunks.iter().find(|known| known.id == chunk.id) {
                    if known != chunk {
                        return Err(invalid("publication chunk identity changed"));
                    }
                } else {
                    reference.chunks.push(chunk.clone());
                }
            }
            if update.release {
                reference.retired_at_unix_ms = Some(now_unix_ms());
            }
            mutations.push(TxnMutation::Put(MetaEntity::DfsReference(reference)));
            if self
                .gc_commit_checked(conditions, StoreOperation::DfsUpdateReference, mutations)
                .await?
            {
                return Ok(());
            }
        }
        Err(conflict("GC reference admission contention"))
    }
    async fn gc_commit(
        &self,
        revision: super::super::store::StoreRevision,
        operation: StoreOperation,
        mutations: Vec<TxnMutation>,
    ) -> Result<bool> {
        self.gc_commit_checked(
            vec![TxnCondition::StoreRevisionEquals(revision)],
            operation,
            mutations,
        )
        .await
    }
    async fn gc_commit_checked(
        &self,
        conditions: Vec<TxnCondition>,
        operation: StoreOperation,
        mut mutations: Vec<TxnMutation>,
    ) -> Result<bool> {
        let key = RequestKey::new(
            "afs-meta",
            super::super::store::timed_request_id(format!(
                "gc-{}-{}",
                std::process::id(),
                GC_SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            )),
        );
        let mut txn = MetaTxn::new(key.clone(), operation);
        txn.conditions = conditions;
        txn.conditions
            .push(TxnCondition::RequestAbsent(key.clone()));
        mutations.push(TxnMutation::RecordRequestOutcome(RequestOutcome {
            request: key,
            operation,
            result: OperationResult::Empty,
        }));
        txn.mutations = mutations;
        Ok(matches!(
            self.store.compare_and_commit(txn).await?,
            TxnOutcome::Committed { .. }
        ))
    }
    pub async fn collect_garbage(
        &self,
        node: String,
        session: String,
        completed: u64,
        inventory: Vec<ChunkObject>,
    ) -> Result<GcState> {
        if self.is_external_backend() {
            return Err(invalid("native collection requires NodeReplicated storage"));
        }
        if inventory.len() > MAX_CHUNKS {
            return Err(invalid("GC inventory batch too large"));
        }
        for _ in 0..64 {
            let view = self.store.read_view().await?;
            let (revision, mut entities) = view.snapshot()?;
            live_caller(&entities, &node, &session)?;
            let mut gc = gc_state(&entities);
            // A new session fences the old process. Keep its pins through the old
            // authority deadline plus the normal read-grant grace; heartbeat loss
            // alone never retires a reader.
            let mut mutations = Vec::new();
            let now = now_unix_ms();
            let old_deadlines = entities
                .iter()
                .filter_map(|entity| match entity {
                    MetaEntity::NodeSession(value)
                        if value.node_id == node && value.session_id != session =>
                    {
                        Some((value.session_id.clone(), value.expires_at_unix_ms))
                    }
                    _ => None,
                })
                .collect::<std::collections::BTreeMap<_, _>>();
            for entity in &mut entities {
                if let MetaEntity::DfsReference(reference) = entity
                    && reference.node_id == node
                    && reference.session_id != session
                    && reference.retired_at_unix_ms.is_none()
                {
                    reference.retired_at_unix_ms = Some(
                        old_deadlines
                            .get(&reference.session_id)
                            .copied()
                            .unwrap_or(now)
                            .max(now),
                    );
                    mutations.push(TxnMutation::Put(entity.clone()));
                }
            }
            if completed > gc.generation {
                return Err(conflict(
                    "Meta GC generation is older than durable Node progress",
                ));
            }
            if !gc.waiting_nodes.is_empty() {
                if completed == gc.generation && gc.waiting_nodes.contains(&node) {
                    gc.waiting_nodes.retain(|value| value != &node);
                    if gc.waiting_nodes.is_empty() {
                        gc.chunks.clear();
                    }
                    mutations.push(TxnMutation::Put(MetaEntity::DfsGc(gc.clone())));
                }
                if !mutations.is_empty()
                    && !self
                        .gc_commit(revision, StoreOperation::DfsCollectGarbage, mutations)
                        .await?
                {
                    continue;
                }
                return Ok(gc);
            }
            for chunk in &inventory {
                if !chunk.valid_immutable_identity() {
                    return Err(invalid(
                        "GC inventory requires canonical content identities",
                    ));
                }
                if !entities.iter().any(
                    |entity| matches!(entity,MetaEntity::DfsChunk(value) if value.id==chunk.id),
                ) {
                    let value = MetaEntity::DfsChunk(chunk.clone());
                    mutations.push(TxnMutation::Put(value.clone()));
                    entities.push(value);
                }
            }
            let references = entities
                .iter()
                .filter_map(|entity| match entity {
                    MetaEntity::DfsReference(value) => Some(value.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>();
            let plan = plan_collection(&entities, &references, now_unix_ms(), self.gc_grace_ms);
            mutations.extend(plan.keys.into_iter().map(TxnMutation::Delete));
            if mutations.is_empty() {
                return Ok(gc);
            }
            gc.generation = gc
                .generation
                .checked_add(1)
                .ok_or_else(|| conflict("GC generation exhausted"))?;
            gc.chunks = plan.chunks;
            if !gc.chunks.is_empty() {
                gc.waiting_nodes = entities
                    .iter()
                    .filter_map(|entity| match entity {
                        MetaEntity::NodeSession(value) => Some(value.node_id.clone()),
                        _ => None,
                    })
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect();
            }
            mutations.push(TxnMutation::Put(MetaEntity::DfsGc(gc.clone())));
            if self
                .gc_commit(revision, StoreOperation::DfsCollectGarbage, mutations)
                .await?
            {
                return Ok(gc);
            }
        }
        Err(conflict("garbage collection contention"))
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::dfs::{ChunkEncoding, ChunkId, ContentDigest, DigestAlgorithm, Extent};
    use std::collections::BTreeMap;
    fn inode(id: &str, nlink: u32, head: &str) -> MetaEntity {
        MetaEntity::DfsInode(InodeRecord {
            namespace_id: NamespaceId::new("ns"),
            inode_id: InodeId::new(id),
            kind: InodeKind::Regular,
            attributes: InodeAttributes {
                mode: 0o600,
                uid: 0,
                gid: 0,
                nlink,
                atime_unix_ms: 0,
                mtime_unix_ms: 0,
                ctime_unix_ms: 0,
            },
            head_version: Some(FileVersionId::new(head)),
            symlink_target: None,
            xattrs: BTreeMap::new(),
            revision: 1,
        })
    }
    fn version(id: &str, owner: &str, chunk: &str) -> Vec<MetaEntity> {
        vec![
            MetaEntity::DfsFileVersion(FileVersion {
                id: FileVersionId::new(id),
                inode_id: InodeId::new(owner),
                parent_version: None,
                length: 4,
                layout_root: LayoutRootId::new(id),
                created_at_unix_ms: 0,
            }),
            MetaEntity::DfsLayoutRoot(LayoutRoot {
                id: LayoutRootId::new(id),
                file_length: 4,
                inline_extents: vec![Extent {
                    file_offset: 0,
                    length: 4,
                    chunk_id: ChunkId::new(chunk),
                    chunk_offset: 0,
                }],
            }),
        ]
    }
    fn chunk(id: &str) -> MetaEntity {
        MetaEntity::DfsChunk(ChunkObject {
            id: ChunkId::new(id),
            length: 4,
            encoding: ChunkEncoding::Raw,
            content_digest: ContentDigest {
                algorithm: DigestAlgorithm::Blake3,
                bytes: [0; 32],
            },
        })
    }
    fn pin(id: &str) -> GcReference {
        GcReference {
            node_id: "reader".into(),
            session_id: "session".into(),
            reference_id: "open".into(),
            inode_id: Some(InodeId::new(id)),
            publication: false,
            chunks: vec![],
            retired_at_unix_ms: None,
        }
    }
    #[test]
    fn cow_clone_retains_shared_chunks_after_source_is_unlinked() {
        let mut entities = vec![
            inode("source", 0, "v1"),
            inode("clone", 1, "v2"),
            chunk("shared"),
            chunk("garbage"),
        ];
        entities.extend(version("v1", "source", "shared"));
        entities.extend(version("v2", "clone", "shared"));
        let plan = plan_collection(&entities, &[], 100, 10);
        assert!(
            plan.keys
                .contains(&MetaKey::DfsFileVersion(FileVersionId::new("v1")))
        );
        assert!(
            !plan
                .keys
                .contains(&MetaKey::DfsFileVersion(FileVersionId::new("v2")))
        );
        assert_eq!(plan.chunks, vec![ChunkId::new("garbage")]);
    }
    #[test]
    fn open_inode_retains_old_versions_until_last_reference_and_grace_are_gone() {
        let mut entities = vec![inode("mapped", 0, "v2"), chunk("old"), chunk("new")];
        entities.extend(version("v1", "mapped", "old"));
        entities.extend(version("v2", "mapped", "new"));
        let mut reference = pin("mapped");
        assert!(
            plan_collection(&entities, &[reference.clone()], 100, 10)
                .chunks
                .is_empty()
        );
        reference.retired_at_unix_ms = Some(95);
        assert!(
            plan_collection(&entities, &[reference.clone()], 100, 10)
                .chunks
                .is_empty()
        );
        let plan = plan_collection(&entities, &[reference], 106, 10);
        assert_eq!(
            plan.chunks.len(),
            2,
            "both versions become unreachable only after close and grace"
        );
    }
    fn generation_chunk(operation: &str, suffix: char) -> MetaEntity {
        let generation = blake3::hash(operation.as_bytes()).to_hex()[..32].to_string();
        let mut staged = crate::node::chunk::StagedChunk::new_crc32c(
            OperationId::new(operation),
            b"uncommitted inventory".to_vec(),
        )
        .unwrap();
        staged.chunk.id = ChunkId::new(format!(
            "crc32c-v2-{generation}-{}",
            suffix.to_string().repeat(32)
        ));
        MetaEntity::DfsChunk(staged.chunk)
    }

    #[test]
    fn generation_reference_protects_unregistered_inventory_on_all_replicas() {
        let mut reference = pin("writer");
        reference.publication = true;
        reference.reference_id = "publish:generation-A".into();
        let entities = vec![
            generation_chunk("generation-A", 'a'),
            generation_chunk("generation-A", 'b'),
        ];
        let plan = plan_collection(&entities, &[reference], 100, 10);
        assert!(
            plan.chunks.is_empty(),
            "one generation admission must protect new replica inventories without per-chunk registration"
        );
    }

    #[test]
    fn generation_reference_does_not_protect_another_write() {
        let mut reference = pin("writer");
        reference.publication = true;
        reference.reference_id = "publish:generation-A".into();
        let a = generation_chunk("generation-A", 'a');
        let b = generation_chunk("generation-B", 'b');
        let MetaEntity::DfsChunk(expected) = &b else {
            unreachable!()
        };
        let plan = plan_collection(&[a, b.clone()], &[reference], 100, 10);
        assert_eq!(
            plan.chunks,
            vec![expected.id.clone()],
            "unrelated orphan still must be reclaimable"
        );
    }

    #[test]
    fn active_publication_retains_uploaded_but_uncommitted_chunks() {
        let entities = vec![chunk("pending")];
        let mut reference = pin("writer");
        reference.publication = true;
        let MetaEntity::DfsChunk(value) = chunk("pending") else {
            unreachable!()
        };
        reference.chunks.push(value);
        assert!(
            plan_collection(&entities, &[reference], 100, 10)
                .chunks
                .is_empty()
        );
        assert_eq!(
            plan_collection(&entities, &[], 100, 10).chunks,
            vec![ChunkId::new("pending")]
        );
    }
    async fn seeded_store(
        entities: Vec<MetaEntity>,
    ) -> (
        Arc<crate::meta::store::Store>,
        Arc<crate::meta::store::journal::MemoryLog>,
    ) {
        use crate::meta::store::{NodeSessionLease, Store, journal::MemoryLog};
        let backend = Arc::new(MemoryLog::new(Default::default()).unwrap());
        let store = Arc::new(Store::open_log(backend.clone()).await.unwrap());
        store
            .register_node_session(
                RequestKey::new("n", "session"),
                NodeSessionLease {
                    node_id: "n".into(),
                    session_id: "s".into(),
                    grpc_addr: "http://n:1".into(),
                    data_addr: "http://n:2".into(),
                    rest_addr: "http://n:3".into(),
                    storage_devices: vec![],
                    lease_ttl: std::time::Duration::from_secs(30),
                },
            )
            .await
            .unwrap();
        let key = RequestKey::new("seed", "entities");
        let mut txn = MetaTxn::new(key.clone(), StoreOperation::DfsCreate);
        txn.conditions
            .push(TxnCondition::RequestAbsent(key.clone()));
        txn.mutations = entities.into_iter().map(TxnMutation::Put).collect();
        txn.mutations
            .push(TxnMutation::RecordRequestOutcome(RequestOutcome {
                request: key,
                operation: StoreOperation::DfsCreate,
                result: OperationResult::Empty,
            }));
        store.compare_and_commit(txn).await.unwrap();
        (store, backend)
    }
    fn canonical_chunk() -> ChunkObject {
        ChunkObject::from_content_addressed_id(&ChunkId::new(format!(
            "blake3-{}-4",
            blake3::hash(b"data").to_hex()
        )))
        .unwrap()
    }
    fn update(id: &str) -> UpdateGcReference {
        UpdateGcReference {
            node_id: "n".into(),
            session_id: "s".into(),
            reference_id: id.into(),
            inode_id: Some(InodeId::new("i")),
            version_id: None,
            publication: true,
            chunks: vec![],
            release: false,
            admission_deadline_unix_ms: now_unix_ms().saturating_add(30_000),
        }
    }
    fn pending_reference(id: &str) -> MetaEntity {
        MetaEntity::DfsReference(GcReference {
            node_id: "n".into(),
            session_id: "s".into(),
            reference_id: id.into(),
            inode_id: Some(InodeId::new("i")),
            publication: true,
            chunks: vec![],
            retired_at_unix_ms: None,
        })
    }
    #[tokio::test]
    async fn concurrent_publication_reservations_keep_every_chunk_without_retry_exhaustion() {
        let (store, _) = seeded_store(vec![inode("i", 1, "v")]).await;
        let service = DfsService::new(store.clone());
        service
            .update_gc_reference(update("publish:parallel"))
            .await
            .unwrap();
        let start = Arc::new(tokio::sync::Barrier::new(256));
        let mut tasks = tokio::task::JoinSet::new();
        for index in 0..256 {
            let service = service.clone();
            let start = start.clone();
            tasks.spawn(async move {
                let bytes = format!("parallel-{index}");
                let chunk = ChunkObject::from_content_addressed_id(&ChunkId::new(format!(
                    "blake3-{}-{}",
                    blake3::hash(bytes.as_bytes()).to_hex(),
                    bytes.len()
                )))
                .unwrap();
                let mut reservation = update("publish:parallel");
                reservation.inode_id = None;
                reservation.chunks = vec![chunk];
                start.wait().await;
                service.update_gc_reference(reservation).await
            });
        }
        let mut errors = Vec::new();
        while let Some(result) = tasks.join_next().await {
            if let Err(error) = result.unwrap() {
                errors.push(error.to_string());
            }
        }
        assert!(
            errors.is_empty(),
            "{} publication reservations failed; first: {:?}",
            errors.len(),
            errors.first()
        );
        let reference = store
            .read_view()
            .await
            .unwrap()
            .lookup(&ref_key("n", "s", "publish:parallel"))
            .unwrap();
        let Some(MetaEntity::DfsReference(reference)) = reference.entity else {
            panic!("publication protection missing");
        };
        assert_eq!(reference.chunks.len(), 256);
        assert_eq!(
            reference
                .chunks
                .iter()
                .map(|chunk| &chunk.id)
                .collect::<BTreeSet<_>>()
                .len(),
            256,
            "every distinct uploaded chunk must remain protected"
        );
    }
    fn admission_conditions(
        view: &crate::meta::store::MetaReadView,
        id: &str,
    ) -> Vec<TxnCondition> {
        let mut conditions = vec![TxnCondition::NodeSessionCurrent {
            node_id: "n".into(),
            session_id: "s".into(),
        }];
        for key in [
            ref_key("n", "s", id),
            MetaKey::DfsInode(InodeId::new("i")),
            MetaKey::DfsGc,
        ] {
            reference_entity(view, key, &mut conditions).unwrap();
        }
        conditions
    }
    #[tokio::test]
    async fn reference_conditions_ignore_unrelated_commits_but_invalidate_stale_collection() {
        let (store, _) = seeded_store(vec![inode("i", 1, "v")]).await;
        let service = DfsService::new(store.clone());
        let view = store.read_view().await.unwrap();
        let conditions = admission_conditions(&view, "reader");
        service
            .update_gc_reference(update("unrelated-reference"))
            .await
            .unwrap();
        let collection_revision = store.read_view().await.unwrap().snapshot().unwrap().0;
        assert!(
            service
                .gc_commit_checked(
                    conditions,
                    StoreOperation::DfsUpdateReference,
                    vec![TxnMutation::Put(pending_reference("reader"))]
                )
                .await
                .unwrap()
        );
        assert!(
            !service
                .gc_commit(
                    collection_revision,
                    StoreOperation::DfsCollectGarbage,
                    vec![TxnMutation::Delete(MetaKey::DfsInode(InodeId::new("i")))]
                )
                .await
                .unwrap(),
            "a collector prepared before admission must retry its reachability scan"
        );
        assert!(
            store
                .read(MetaRead::DfsInode(InodeId::new("i")))
                .await
                .unwrap()
                .entity
                .is_some()
        );
    }
    #[tokio::test]
    async fn reference_conditions_reject_release_collection_and_session_fences() {
        for fence in ["release", "collection", "session"] {
            let (store, _) = seeded_store(vec![inode("i", 0, "v")]).await;
            let service = DfsService::new(store.clone()).with_gc_grace_ms(0);
            let view = store.read_view().await.unwrap();
            let conditions = admission_conditions(&view, "late-reader");
            match fence {
                "release" => {
                    let mut release = update("late-reader");
                    release.release = true;
                    service.update_gc_reference(release).await.unwrap();
                }
                "collection" => {
                    service
                        .collect_garbage("n".into(), "s".into(), 0, vec![])
                        .await
                        .unwrap();
                }
                _ => {
                    store
                        .register_node_session(
                            RequestKey::new("n", "replace"),
                            crate::meta::store::NodeSessionLease {
                                node_id: "n".into(),
                                session_id: "new-session".into(),
                                grpc_addr: "http://n:1".into(),
                                data_addr: "http://n:2".into(),
                                rest_addr: "http://n:3".into(),
                                storage_devices: vec![],
                                lease_ttl: std::time::Duration::from_secs(30),
                            },
                        )
                        .await
                        .unwrap();
                }
            }
            assert!(
                !service
                    .gc_commit_checked(
                        conditions,
                        StoreOperation::DfsUpdateReference,
                        vec![TxnMutation::Put(pending_reference("late-reader"))]
                    )
                    .await
                    .unwrap(),
                "{fence}"
            );
        }
    }
    #[tokio::test]
    async fn publication_conditions_reject_reserved_chunk_reclamation() {
        let object = canonical_chunk();
        let (store, _) = seeded_store(vec![
            inode("i", 1, "v"),
            MetaEntity::DfsChunk(object.clone()),
        ])
        .await;
        let service = DfsService::new(store.clone()).with_gc_grace_ms(0);
        let view = store.read_view().await.unwrap();
        let mut conditions = admission_conditions(&view, "publication");
        reference_entity(&view, MetaKey::DfsChunk(object.id.clone()), &mut conditions).unwrap();
        let barrier = service
            .collect_garbage("n".into(), "s".into(), 0, vec![])
            .await
            .unwrap();
        assert!(barrier.chunks.contains(&object.id));
        let mut reference = pending_reference("publication");
        if let MetaEntity::DfsReference(value) = &mut reference {
            value.chunks.push(object);
        }
        assert!(
            !service
                .gc_commit_checked(
                    conditions,
                    StoreOperation::DfsUpdateReference,
                    vec![TxnMutation::Put(reference)]
                )
                .await
                .unwrap()
        );
    }
    #[tokio::test]
    async fn reference_admission_requires_its_inode_version_and_current_session() {
        let mut entities = vec![inode("i", 1, "v")];
        entities.extend(version("v", "another-inode", "chunk"));
        let (store, _) = seeded_store(entities).await;
        let service = DfsService::new(store.clone());
        let mut missing = update("missing-inode");
        missing.inode_id = Some(InodeId::new("collected"));
        assert!(service.update_gc_reference(missing).await.is_err());
        let mut wrong_version = update("wrong-version");
        wrong_version.version_id = Some(FileVersionId::new("v"));
        assert!(service.update_gc_reference(wrong_version).await.is_err());
        service
            .update_gc_reference(update("valid-inode"))
            .await
            .unwrap();
        store
            .register_node_session(
                RequestKey::new("n", "replace-session"),
                crate::meta::store::NodeSessionLease {
                    node_id: "n".into(),
                    session_id: "s2".into(),
                    grpc_addr: "http://n:1".into(),
                    data_addr: "http://n:2".into(),
                    rest_addr: "http://n:3".into(),
                    storage_devices: vec![],
                    lease_ttl: std::time::Duration::from_secs(30),
                },
            )
            .await
            .unwrap();
        assert!(
            service
                .update_gc_reference(update("stale-session"))
                .await
                .is_err()
        );
        let mut current = update("current-session");
        current.session_id = "s2".into();
        service.update_gc_reference(current).await.unwrap();
        let view = store.read_view().await.unwrap();
        for id in ["missing-inode", "wrong-version", "stale-session"] {
            assert!(
                view.lookup(&ref_key("n", "s", id))
                    .unwrap()
                    .entity
                    .is_none()
            );
        }
    }
    #[tokio::test]
    async fn publication_and_lost_gc_ack_are_fenced_across_meta_reopen() {
        use crate::meta::store::Store;
        let chunk = canonical_chunk();
        let (store, backend) = seeded_store(vec![inode("i", 1, "v")]).await;
        let service = DfsService::with_replication_config(
            store.clone(),
            ReplicationConfig::local_single_copy(),
        )
        .with_gc_grace_ms(0);
        service
            .update_gc_reference(update("publish:p"))
            .await
            .unwrap();
        let mut reserve = update("publish:p");
        reserve.inode_id = None;
        reserve.chunks = vec![chunk.clone()];
        service.update_gc_reference(reserve.clone()).await.unwrap();
        assert!(
            service
                .collect_garbage("n".into(), "s".into(), 0, vec![chunk.clone()])
                .await
                .unwrap()
                .chunks
                .is_empty()
        );
        let mut release = update("publish:p");
        release.release = true;
        service.update_gc_reference(release).await.unwrap();
        let ticket = service
            .collect_garbage("n".into(), "s".into(), 0, vec![chunk.clone()])
            .await
            .unwrap();
        assert_eq!(ticket.chunks, vec![chunk.id.clone()]);
        drop(service);
        drop(store);
        // The backend image includes pending deletion and reference records.
        let reopened = Arc::new(Store::open_log(backend).await.unwrap());
        let service =
            DfsService::with_replication_config(reopened, ReplicationConfig::local_single_copy())
                .with_gc_grace_ms(0);
        service
            .update_gc_reference(update("publish:new"))
            .await
            .unwrap();
        reserve.reference_id = "publish:new".into();
        assert_eq!(
            service
                .update_gc_reference(reserve.clone())
                .await
                .unwrap_err()
                .code(),
            afs_error::META_DFS_GC_BUSY
        );
        let repeated = service
            .collect_garbage("n".into(), "s".into(), 0, vec![])
            .await
            .unwrap();
        assert_eq!(repeated, ticket);
        let acknowledged = service
            .collect_garbage("n".into(), "s".into(), ticket.generation, vec![])
            .await
            .unwrap();
        assert!(acknowledged.chunks.is_empty());
        service
            .collect_garbage("n".into(), "s".into(), ticket.generation, vec![])
            .await
            .unwrap();
        service.update_gc_reference(reserve).await.unwrap();
    }
    #[tokio::test]
    async fn session_replacement_retires_only_fenced_refs_after_the_old_authority_deadline() {
        let chunk = canonical_chunk();
        let mut reference = pin("i");
        reference.node_id = "n".into();
        reference.session_id = "old".into();
        reference.chunks = vec![chunk.clone()];
        let deadline = now_unix_ms() + 30_000;
        let old = MetaEntity::NodeSession(NodeSession {
            node_id: "n".into(),
            session_id: "old".into(),
            grpc_addr: "http://n:1".into(),
            data_addr: "http://n:2".into(),
            rest_addr: "http://n:3".into(),
            storage_devices: vec![],
            lease_epoch: 0,
            expires_at_unix_ms: deadline,
        });
        let (store, _) = seeded_store(vec![
            inode("i", 0, "v"),
            MetaEntity::DfsChunk(chunk.clone()),
            MetaEntity::DfsReference(reference),
            old,
        ])
        .await;
        let service = DfsService::new(store.clone());
        let ticket = service
            .collect_garbage("n".into(), "s".into(), 0, vec![])
            .await
            .unwrap();
        assert!(ticket.chunks.is_empty());
        let (_, entities) = store.read_view().await.unwrap().snapshot().unwrap();
        let retired = entities
            .iter()
            .find_map(|entity| match entity {
                MetaEntity::DfsReference(reference) => Some(reference),
                _ => None,
            })
            .unwrap();
        assert!(retired.retired_at_unix_ms.unwrap() >= deadline);
        assert!(
            plan_collection(
                &entities,
                std::slice::from_ref(retired),
                deadline + 59_999,
                60_000
            )
            .chunks
            .is_empty()
        );
        assert_eq!(
            plan_collection(
                &entities,
                std::slice::from_ref(retired),
                deadline + 60_001,
                60_000
            )
            .chunks,
            vec![chunk.id]
        );
        let mut wrong = update("open:stale");
        wrong.session_id = "old".into();
        assert!(service.update_gc_reference(wrong).await.is_err());
    }
    #[test]
    fn heartbeat_expiry_without_session_replacement_never_retires_an_open_reader() {
        let mut reference = pin("i");
        let MetaEntity::DfsChunk(object) = chunk("live") else {
            unreachable!()
        };
        reference.chunks = vec![object];
        let entities = vec![inode("i", 0, "v"), chunk("live")];
        assert!(
            plan_collection(&entities, &[reference], u64::MAX - 60_000, 60_000)
                .chunks
                .is_empty()
        );
    }
    #[tokio::test]
    async fn release_before_delayed_admission_is_a_durable_rejection() {
        use crate::meta::store::Store;
        let (store, backend) = seeded_store(vec![inode("i", 1, "v")]).await;
        let service = DfsService::new(store.clone());
        let mut release = update("late-open");
        release.release = true;
        release.inode_id = None;
        service.update_gc_reference(release).await.unwrap();
        drop(service);
        drop(store);
        let service = DfsService::new(Arc::new(Store::open_log(backend).await.unwrap()));
        assert!(
            service
                .update_gc_reference(update("late-open"))
                .await
                .is_err(),
            "a delayed lost-reply admission cannot resurrect an already released reference"
        );
    }

    #[tokio::test]
    async fn expired_delayed_admission_cannot_reappear_after_tombstone_collection() {
        let (store, _) = seeded_store(vec![inode("i", 1, "v")]).await;
        let service = DfsService::new(store.clone()).with_gc_grace_ms(0);
        let mut release = update("late-open");
        release.release = true;
        release.inode_id = None;
        service.update_gc_reference(release).await.unwrap();
        service
            .collect_garbage("n".into(), "s".into(), 0, vec![])
            .await
            .unwrap();
        let (_, entities) = store.read_view().await.unwrap().snapshot().unwrap();
        assert!(
            !entities
                .iter()
                .any(|entity| matches!(entity, MetaEntity::DfsReference(_))),
            "retired tombstones must also converge"
        );
        let mut late = update("late-open");
        late.admission_deadline_unix_ms = now_unix_ms().saturating_sub(1);
        assert_eq!(
            service.update_gc_reference(late).await.unwrap_err().code(),
            afs_error::META_DFS_REFERENCE_ADMISSION_EXPIRED
        );
    }
    #[tokio::test]
    async fn active_publication_outlives_admission_but_cannot_change_identity() {
        let (store, _) = seeded_store(vec![inode("i", 1, "v")]).await;
        let service = DfsService::new(store);
        let mut invalid = update("too-far");
        invalid.admission_deadline_unix_ms = now_unix_ms() + 120_000;
        assert!(service.update_gc_reference(invalid).await.is_err());
        service.update_gc_reference(update("writer")).await.unwrap();
        let mut chunks = update("writer");
        chunks.inode_id = None;
        chunks.admission_deadline_unix_ms = 0;
        chunks.chunks = vec![canonical_chunk()];
        service.update_gc_reference(chunks).await.unwrap();
        let mut reused = update("writer");
        reused.inode_id = Some(InodeId::new("different"));
        assert!(service.update_gc_reference(reused).await.is_err());
    }
}
