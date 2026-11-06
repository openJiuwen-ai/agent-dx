//! Reachability GC and durable deletion barriers. A lineage pointer is not ownership.
use super::*;
use crate::dfs::{ChunkId, GcReference, GcState, UpdateGcReference};
use std::{
    collections::{BTreeSet, hash_map::DefaultHasher},
    hash::{Hash, Hasher},
};
static GC_SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
const MAX_CHUNKS: usize = 1024;
/// A pass or generation ranges from milliseconds to minutes under load.
const GC_DURATION_BUCKETS: [f64; 12] = [
    0.001, 0.01, 0.05, 0.1, 0.5, 1.0, 2.5, 5.0, 15.0, 60.0, 300.0, 1800.0,
];
/// Most references one release call retires. One call is one transaction, so
/// this also bounds the size of its log record.
pub const MAX_RELEASE_BATCH: usize = 1024;
/// Maximum admission horizon update_gc_reference accepts beyond the current
/// time (the "60s cleanup fence"). A replay of an abandoned admission can
/// only be accepted while its original deadline is still in this window.
const ADMISSION_HORIZON_MS: u64 = 60_000;
/// Clock-skew allowance between node clocks (which stamp admission deadlines
/// and write identities) and the meta clock (which validates them). Combined
/// with ADMISSION_HORIZON_MS it bounds when a replay can still pass the
/// deadline gate after the tombstone's retirement.
const ADMISSION_CLOCK_SKEW_MS: u64 = 60_000;
/// Minimum time a retired reference record must linger as a replay tombstone:
/// admission horizon + clock skew, the window in which a late arrival can
/// still legally pass its deadline gate. Records additionally live as long
/// as the configured data grace protects their content (plan_collection
/// deletes them at max(grace, this value)), so a long grace is honored.
const REFERENCE_RECORD_RETENTION_MS: u64 = ADMISSION_HORIZON_MS + ADMISSION_CLOCK_SKEW_MS;
/// Process-local GC progress for operators. A generation that stays open with
/// waiting nodes is the stall to alert on (`generation_age_seconds`); it is
/// refreshed by every collection call, and any live storage node calls every
/// few seconds. Restarting Meta restarts the age.
#[derive(Clone)]
pub(crate) struct GcMetrics {
    pub(crate) generation: afs_metrics::IntGauge,
    pub(crate) waiting_nodes: afs_metrics::IntGauge,
    pub(crate) pending_chunks: afs_metrics::IntGauge,
    pub(crate) generation_age_seconds: afs_metrics::IntGauge,
    pub(crate) dropped_nodes: afs_metrics::IntCounter,
    pub(crate) retired_references: afs_metrics::IntCounter,
    /// One per `collect_garbage` call.
    pub(crate) passes: afs_metrics::IntCounter,
    /// One per snapshot-and-plan attempt inside a pass.
    pub(crate) attempts: afs_metrics::IntCounter,
    /// Attempts whose commit lost a conflict and had to start over.
    pub(crate) conflicts: afs_metrics::IntCounter,
    /// Passes that gave up after the last attempt ("contention").
    pub(crate) exhausted_passes: afs_metrics::IntCounter,
    pub(crate) pass_duration: afs_metrics::Histogram,
    pub(crate) generation_duration: afs_metrics::Histogram,
    /// References that still protect their inode without a retirement time.
    pub(crate) live_references: afs_metrics::IntGauge,
    /// Retired reference records kept for grace and replay fencing.
    pub(crate) retired_reference_records: afs_metrics::IntGauge,
    /// Authority entities one planning attempt had to look at.
    pub(crate) planned_entities: afs_metrics::IntGauge,
    open_generation: Arc<std::sync::Mutex<Option<(u64, std::time::Instant)>>>,
}

impl GcMetrics {
    pub(crate) fn new() -> std::result::Result<Self, afs_metrics::MetricsError> {
        Ok(Self {
            generation: afs_metrics::IntGauge::new(
                "afs_meta_dfs_gc_generation",
                "Current DFS GC generation",
            )?,
            waiting_nodes: afs_metrics::IntGauge::new(
                "afs_meta_dfs_gc_waiting_nodes",
                "Live storage nodes that have not finished the current DFS GC generation",
            )?,
            pending_chunks: afs_metrics::IntGauge::new(
                "afs_meta_dfs_gc_pending_chunks",
                "Chunks behind the current DFS GC deletion barrier",
            )?,
            generation_age_seconds: afs_metrics::IntGauge::new(
                "afs_meta_dfs_gc_generation_age_seconds",
                "How long this Meta process has seen the current DFS GC generation waiting; 0 when none waits",
            )?,
            dropped_nodes: afs_metrics::IntCounter::new(
                "afs_meta_dfs_gc_dropped_nodes_total",
                "Nodes removed from a DFS GC generation's waiting set because their session expired",
            )?,
            retired_references: afs_metrics::IntCounter::new(
                "afs_meta_dfs_gc_retired_references_total",
                "GC references retired because their node session is no longer current and live",
            )?,
            passes: afs_metrics::IntCounter::new(
                "afs_meta_dfs_gc_passes_total",
                "DFS GC collection calls",
            )?,
            attempts: afs_metrics::IntCounter::new(
                "afs_meta_dfs_gc_attempts_total",
                "DFS GC planning attempts, including those repeated after a conflict",
            )?,
            conflicts: afs_metrics::IntCounter::new(
                "afs_meta_dfs_gc_conflicts_total",
                "DFS GC commits that lost a conflict and started over",
            )?,
            exhausted_passes: afs_metrics::IntCounter::new(
                "afs_meta_dfs_gc_exhausted_passes_total",
                "DFS GC collection calls that gave up after repeated conflicts",
            )?,
            pass_duration: afs_metrics::Histogram::with_opts(
                afs_metrics::HistogramOpts::new(
                    "afs_meta_dfs_gc_pass_duration_seconds",
                    "Duration of one DFS GC collection call",
                )
                .buckets(GC_DURATION_BUCKETS.to_vec()),
            )?,
            generation_duration: afs_metrics::Histogram::with_opts(
                afs_metrics::HistogramOpts::new(
                    "afs_meta_dfs_gc_generation_duration_seconds",
                    "How long this Meta process saw a DFS GC generation waiting for nodes",
                )
                .buckets(GC_DURATION_BUCKETS.to_vec()),
            )?,
            live_references: afs_metrics::IntGauge::new(
                "afs_meta_dfs_gc_live_references",
                "GC references without a retirement time, as of the last planning attempt",
            )?,
            retired_reference_records: afs_metrics::IntGauge::new(
                "afs_meta_dfs_gc_retired_reference_records",
                "Retired GC reference records still kept, as of the last planning attempt",
            )?,
            planned_entities: afs_metrics::IntGauge::new(
                "afs_meta_dfs_gc_planned_entities",
                "Authority entities read by the last DFS GC planning attempt",
            )?,
            open_generation: Arc::new(std::sync::Mutex::new(None)),
        })
    }

    pub(crate) fn register(
        &self,
        registry: &afs_metrics::Registry,
    ) -> std::result::Result<(), afs_metrics::MetricsError> {
        afs_metrics::register_collector(registry, &self.generation)?;
        afs_metrics::register_collector(registry, &self.waiting_nodes)?;
        afs_metrics::register_collector(registry, &self.pending_chunks)?;
        afs_metrics::register_collector(registry, &self.generation_age_seconds)?;
        afs_metrics::register_collector(registry, &self.dropped_nodes)?;
        afs_metrics::register_collector(registry, &self.retired_references)?;
        afs_metrics::register_collector(registry, &self.passes)?;
        afs_metrics::register_collector(registry, &self.attempts)?;
        afs_metrics::register_collector(registry, &self.conflicts)?;
        afs_metrics::register_collector(registry, &self.exhausted_passes)?;
        afs_metrics::register_collector(registry, &self.pass_duration)?;
        afs_metrics::register_collector(registry, &self.generation_duration)?;
        afs_metrics::register_collector(registry, &self.live_references)?;
        afs_metrics::register_collector(registry, &self.retired_reference_records)?;
        afs_metrics::register_collector(registry, &self.planned_entities)
    }

    fn observe_references(&self, entities: &[MetaEntity]) {
        let (mut live, mut retired) = (0_i64, 0_i64);
        for entity in entities {
            if let MetaEntity::DfsReference(reference) = entity {
                if reference.retired_at_unix_ms.is_some() {
                    retired += 1;
                } else {
                    live += 1;
                }
            }
        }
        self.live_references.set(live);
        self.retired_reference_records.set(retired);
        self.planned_entities
            .set(i64::try_from(entities.len()).unwrap_or(i64::MAX));
    }

    fn observe(&self, gc: &GcState) {
        self.generation
            .set(i64::try_from(gc.generation).unwrap_or(i64::MAX));
        self.waiting_nodes
            .set(i64::try_from(gc.waiting_nodes.len()).unwrap_or(i64::MAX));
        self.pending_chunks
            .set(i64::try_from(gc.chunks.len()).unwrap_or(i64::MAX));
        let Ok(mut open) = self.open_generation.lock() else {
            return;
        };
        if gc.waiting_nodes.is_empty() {
            if let Some((_, since)) = open.take() {
                self.generation_duration
                    .observe(since.elapsed().as_secs_f64());
            }
            self.generation_age_seconds.set(0);
            return;
        }
        let since = match *open {
            Some((generation, since)) if generation == gc.generation => since,
            previous => {
                if let Some((_, since)) = previous {
                    self.generation_duration
                        .observe(since.elapsed().as_secs_f64());
                }
                let since = std::time::Instant::now();
                *open = Some((gc.generation, since));
                since
            }
        };
        self.generation_age_seconds
            .set(i64::try_from(since.elapsed().as_secs()).unwrap_or(i64::MAX));
    }
}

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
                    // The record must live as long as it still protects data
                    // (a configured grace can exceed the tombstone retention)
                    // and never shorter than the replay-fencing window.
                    time.saturating_add(grace.max(REFERENCE_RECORD_RETENTION_MS)) <= now
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
/// On the wire a `write:` reference names its timestamped operation, which fixes
/// admission age. The stored id is the hash immutable chunk identities address.
fn stored_reference_id(id: &str) -> String {
    match id.strip_prefix("write:") {
        Some(operation) => format!(
            "write:{}",
            ChunkObject::generation_id(&OperationId::new(operation))
        ),
        None => id.to_owned(),
    }
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
    pub fn with_gc_grace_ms(mut self, grace: u64) -> Self {
        self.gc_grace_ms = grace;
        self
    }

    pub(crate) fn gc_metrics(&self) -> &GcMetrics {
        &self.gc_metrics
    }
    pub async fn update_gc_reference(&self, mut update: UpdateGcReference) -> Result<()> {
        require_id(&update.node_id, "node_id")?;
        require_id(&update.session_id, "session_id")?;
        require_id(&update.reference_id, "reference_id")?;
        // On the wire the original timestamped operation fixes admission age.
        // Persist the hash key that immutable chunk identities can address.
        let write_operation = update
            .reference_id
            .strip_prefix("write:")
            .map(str::to_owned);
        update.reference_id = stored_reference_id(&update.reference_id);
        let key = ref_key(&update.node_id, &update.session_id, &update.reference_id);
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        // Serialize updates of one reference key fairly instead of letting each
        // lose up to 64 CAS attempts. Fixed shards bound memory; durable
        // conditions still fence GC, other service instances, session
        // replacement and cancelled requests.
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
            let reference = if let Some(reference) = existing {
                reference
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
                    open_read: None,
                }
            };
            if reference.retired_at_unix_ms.is_some() {
                return Err(conflict("retired reference cannot be reused"));
            }
            if update
                .inode_id
                .as_ref()
                .is_some_and(|inode| reference.inode_id.as_ref() != Some(inode))
                || update.publication != reference.publication
            {
                return Err(invalid("reference identity was reused for another access"));
            }
            let mutations = vec![TxnMutation::Put(MetaEntity::DfsReference(reference))];
            if self
                .gc_commit_checked(conditions, StoreOperation::DfsUpdateReference, mutations)
                .await?
            {
                return Ok(());
            }
        }
        Err(conflict("GC reference admission contention"))
    }
    /// Retires `reference_ids` of this session in one transaction: one log
    /// record however many there are. A reference Meta has not seen gets a
    /// tombstone that fences a queued or lost-reply admission of the same key;
    /// one already retired stays as it is, so a retried batch is idempotent.
    /// Keys are not shard-locked: a racing admission of the same key loses on
    /// the per-key conditions, whichever transaction commits first.
    pub async fn release_gc_references(
        &self,
        node_id: String,
        session_id: String,
        reference_ids: Vec<String>,
    ) -> Result<()> {
        require_id(&node_id, "node_id")?;
        require_id(&session_id, "session_id")?;
        if reference_ids.len() > MAX_RELEASE_BATCH {
            return Err(invalid("GC release batch too large"));
        }
        let mut keys = BTreeSet::new();
        for id in &reference_ids {
            require_id(id, "reference_id")?;
            keys.insert(ref_key(&node_id, &session_id, &stored_reference_id(id)));
        }
        if keys.is_empty() {
            return Ok(());
        }
        for _ in 0..64 {
            let view = self.store.read_view().await?;
            let caller = view
                .lookup(&MetaKey::CurrentNodeSession {
                    node_id: node_id.clone(),
                })?
                .entity;
            live_caller(caller.as_slice(), &node_id, &session_id)?;
            let mut conditions = vec![TxnCondition::NodeSessionCurrent {
                node_id: node_id.clone(),
                session_id: session_id.clone(),
            }];
            let now = now_unix_ms();
            let mut mutations = Vec::new();
            for key in &keys {
                let reference = match reference_entity(&view, key.clone(), &mut conditions)? {
                    Some(MetaEntity::DfsReference(reference)) => reference,
                    Some(_) => return Err(invalid("GC reference key holds another entity")),
                    None => {
                        let MetaKey::DfsReference { reference_id, .. } = key else {
                            return Err(invalid("GC release key is not a reference"));
                        };
                        GcReference {
                            open_read: None,
                            node_id: node_id.clone(),
                            session_id: session_id.clone(),
                            reference_id: reference_id.clone(),
                            inode_id: None,
                            publication: false,
                            chunks: Vec::new(),
                            retired_at_unix_ms: None,
                        }
                    }
                };
                if reference.retired_at_unix_ms.is_none() {
                    mutations.push(TxnMutation::Put(MetaEntity::DfsReference(GcReference {
                        retired_at_unix_ms: Some(now),
                        ..reference
                    })));
                }
            }
            if mutations.is_empty()
                || self
                    .gc_commit_checked(conditions, StoreOperation::DfsUpdateReference, mutations)
                    .await?
            {
                return Ok(());
            }
        }
        Err(conflict("GC reference release contention"))
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
            crate::clock::timed_request_id(format!(
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
        self.gc_metrics.passes.inc();
        let started = std::time::Instant::now();
        let result = self
            .collect_garbage_attempts(node, session, completed, inventory)
            .await;
        self.gc_metrics
            .pass_duration
            .observe(started.elapsed().as_secs_f64());
        let gc = result?;
        self.gc_metrics.observe(&gc);
        Ok(gc)
    }

    /// Whether `node` should list the external object store and report a page
    /// as inventory: one node at a time, the same one that deletes (R05).
    pub async fn lists_external_inventory(&self, node: &str) -> Result<bool> {
        if !self.is_external_backend() {
            return Ok(false);
        }
        let sessions = self.store.read(MetaRead::CurrentNodeSessions).await?;
        let participants = gc_participants(&sessions.entities, now_unix_ms());
        Ok(participants.first().map(String::as_str) == Some(node))
    }

    async fn collect_garbage_attempts(
        &self,
        node: String,
        session: String,
        completed: u64,
        inventory: Vec<ChunkObject>,
    ) -> Result<GcState> {
        if inventory.len() > MAX_CHUNKS {
            return Err(invalid("GC inventory batch too large"));
        }
        for _ in 0..64 {
            let view = self.store.read_view().await?;
            let (revision, mut entities) = view.snapshot()?;
            self.gc_metrics.attempts.inc();
            self.gc_metrics.observe_references(&entities);
            live_caller(&entities, &node, &session)?;
            let mut gc = gc_state(&entities);
            let now = now_unix_ms();
            let participants = gc_participants(&entities, now);
            let mut mutations = Vec::new();
            let retired = retire_departed_references(&mut entities, now, &mut mutations);
            if completed > gc.generation {
                return Err(conflict(
                    "Meta GC generation is older than durable Node progress",
                ));
            }
            if !gc.waiting_nodes.is_empty() {
                // A listed page is a cursor position that will not repeat soon;
                // native inventory simply comes back next pass.
                if self.is_external_backend() {
                    register_inventory(&inventory, &mut entities, &mut mutations)?;
                }
                let waiting = gc.waiting_nodes.len();
                gc.waiting_nodes
                    .retain(|value| participants.contains(value));
                let dropped = waiting - gc.waiting_nodes.len();
                if completed == gc.generation {
                    gc.waiting_nodes.retain(|value| value != &node);
                }
                if gc.waiting_nodes.len() != waiting {
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
                    self.gc_metrics.conflicts.inc();
                    continue;
                }
                self.gc_metrics.dropped_nodes.inc_by(dropped as u64);
                self.gc_metrics.retired_references.inc_by(retired);
                return Ok(gc);
            }
            register_inventory(&inventory, &mut entities, &mut mutations)?;
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
            gc.waiting_nodes = if gc.chunks.is_empty() {
                Vec::new()
            } else if self.is_external_backend() {
                // One copy of each object: one Node deletes. If it is lost the
                // objects stay in the store and reconciliation finds them.
                participants.into_iter().take(1).collect()
            } else {
                participants.into_iter().collect()
            };
            if gc.waiting_nodes.is_empty() {
                // No live storage node holds local copies to retire.
                gc.chunks.clear();
            }
            mutations.push(TxnMutation::Put(MetaEntity::DfsGc(gc.clone())));
            if self
                .gc_commit(revision, StoreOperation::DfsCollectGarbage, mutations)
                .await?
            {
                self.gc_metrics.retired_references.inc_by(retired);
                return Ok(gc);
            }
            self.gc_metrics.conflicts.inc();
        }
        self.gc_metrics.exhausted_passes.inc();
        Err(conflict("garbage collection contention"))
    }
}

/// Registers objects a Node found that Meta does not know, so the one plan
/// decides them (a live write generation protects them; otherwise collected).
fn register_inventory(
    inventory: &[ChunkObject],
    entities: &mut Vec<MetaEntity>,
    mutations: &mut Vec<TxnMutation>,
) -> Result<()> {
    if inventory.is_empty() {
        return Ok(());
    }
    let known = entities
        .iter()
        .filter_map(|entity| match entity {
            MetaEntity::DfsChunk(value) => Some(value.id.clone()),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    for chunk in inventory {
        if !chunk.valid_immutable_identity() {
            return Err(invalid(
                "GC inventory requires canonical content identities",
            ));
        }
        if !known.contains(&chunk.id) {
            let value = MetaEntity::DfsChunk(chunk.clone());
            mutations.push(TxnMutation::Put(value.clone()));
            entities.push(value);
        }
    }
    Ok(())
}

/// Each node's current session: the highest lease epoch among its sessions.
fn current_sessions(entities: &[MetaEntity]) -> std::collections::BTreeMap<&str, &NodeSession> {
    let mut current = std::collections::BTreeMap::<&str, &NodeSession>::new();
    for entity in entities {
        if let MetaEntity::NodeSession(value) = entity {
            let slot = current.entry(value.node_id.as_str()).or_insert(value);
            if value.lease_epoch > slot.lease_epoch {
                *slot = value;
            }
        }
    }
    current
}

/// The nodes a GC generation waits for (R55): a node whose current session is
/// live at `now` and registered a storage device, the same rule as repair
/// placement. A node that is not live has fail-closed (heartbeat loss stops it
/// before its lease ends) and serves no copy; when it returns, its leftover
/// files are unknown to Meta, come back through its inventory and are
/// collected in a later generation. A node without storage (OwnerFs only) runs
/// no Collector and is never waited for.
fn gc_participants(entities: &[MetaEntity], now: u64) -> BTreeSet<String> {
    current_sessions(entities)
        .into_iter()
        .filter(|(_, session)| {
            session.is_live_at_unix_ms(now) && !session.storage_devices.is_empty()
        })
        .map(|(node, _)| node.to_owned())
        .collect()
}

/// Retires every unretired reference whose session is no longer its node's
/// current live session: the node restarted (the old process is fenced) or
/// its session expired (the process fail-closed). Retirement keeps protecting
/// the inode through the old session's deadline plus the normal read grace.
fn retire_departed_references(
    entities: &mut [MetaEntity],
    now: u64,
    mutations: &mut Vec<TxnMutation>,
) -> u64 {
    let live_sessions = current_sessions(entities)
        .into_iter()
        .filter(|(_, session)| session.is_live_at_unix_ms(now))
        .map(|(node, session)| (node.to_owned(), session.session_id.clone()))
        .collect::<std::collections::BTreeMap<_, _>>();
    let deadlines = entities
        .iter()
        .filter_map(|entity| match entity {
            MetaEntity::NodeSession(value) => Some((
                (value.node_id.clone(), value.session_id.clone()),
                value.expires_at_unix_ms,
            )),
            _ => None,
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    let mut retired = 0;
    for entity in entities {
        if let MetaEntity::DfsReference(reference) = entity
            && reference.retired_at_unix_ms.is_none()
            && match live_sessions.get(&reference.node_id) {
                Some(session) => session != &reference.session_id,
                None => deadlines.keys().any(|(node, _)| node == &reference.node_id),
            }
        {
            let deadline = deadlines
                .get(&(reference.node_id.clone(), reference.session_id.clone()))
                .copied()
                .unwrap_or(now);
            reference.retired_at_unix_ms = Some(deadline.max(now));
            mutations.push(TxnMutation::Put(entity.clone()));
            retired += 1;
        }
    }
    retired
}

#[cfg(test)]
#[path = "gc_tests.rs"]
mod tests;
