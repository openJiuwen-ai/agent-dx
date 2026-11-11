//! DFS replication coordination above the local immutable chunk store.
//!
//! The filesystem-wide `ReplicationConfig` is immutable after initialization.
//! Placement can still change as nodes and devices change; Nodes cache that
//! authority snapshot and derive one-operation `ReplicationPlan` values.

use std::{collections::HashSet, sync::Arc};

use afs_error::{Error, ErrorKind, Result};

use crate::{
    dfs::{
        ChunkId, ChunkReceipt, EffectiveLocalCopy, OperationId, PlacementSnapshot, ReplicaAck,
        ReplicaGroup, ReplicaGroupId, ReplicaTarget, ReplicaWriteGrant, ReplicationConfig,
    },
    node::{
        chunk::{
            BodyRelease, ChunkSink, LocalChunkStore, PutIntent, QuarantineTrigger, StagedChunk,
            StoredChunk,
        },
        vfs::types::FilesystemCapacity,
    },
};

// Background repair lives in the fault layer; the names stay reachable here.
#[doc(inline)]
pub use crate::node::fault::repair::{ReplicationTaskAuthority, ReplicationWorker};

pub trait PlacementProvider: Send + Sync {
    fn snapshot(&self) -> Result<Arc<PlacementSnapshot>>;
    fn refresh(&self, minimum_revision: u64) -> Result<Arc<PlacementSnapshot>>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplicaTransferMode {
    GrpcStream,
    RdmaOneSided,
}

/// Transport adapter used only for remote replica bytes.
///
/// gRPC can implement this with request frames. RDMA implements the same
/// command with a negotiated memory descriptor and one-sided transfer; the
/// replication state machine never observes either transport's wire details.
pub trait ReplicaDataPlane: Send + Sync {
    fn mode(&self) -> ReplicaTransferMode;

    /// Validates structural transport setup before replica side effects.
    /// A lazy channel is not a completed handshake or receiver authorization;
    /// those checks occur at the receiving RPC before persistence.
    fn prepare_peer(&self, op: &ReplicaPeerOp) -> Result<()>;

    fn put_peer_replica(&self, op: &ReplicaPeerOp, staged: &StagedChunk)
    -> Result<Vec<ReplicaAck>>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicationPlan {
    pub chunk_id: ChunkId,
    pub placement_revision: u64,
    pub placement_epoch: u64,
    pub replica_group_id: ReplicaGroupId,
    pub config: ReplicationConfig,
    pub local_copy: EffectiveLocalCopy,
    pub ack_requirement: AckRequirement,
    pub ordered_targets: Vec<ReplicaTarget>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaPeerOp {
    pub chunk_id: ChunkId,
    pub placement_revision: u64,
    pub placement_epoch: u64,
    pub replica_group_id: ReplicaGroupId,
    pub initiator_node_id: String,
    pub initiator_node_epoch: u64,
    /// Complete authoritative chain; only its synchronous prefix is executed.
    pub ordered_targets: Vec<ReplicaTarget>,
    pub sync_target_count: usize,
    pub target_index: usize,
    pub target: ReplicaTarget,
    pub chain_tail: Vec<ReplicaTarget>,
    /// Persisted Meta task authority, carried unchanged at every repair hop.
    pub repair_claim: Option<crate::dfs::ReplicationClaim>,
}

impl ReplicaPeerOp {
    /// Build receiver execution only from an authenticated Meta response.
    pub fn from_grant(grant: &ReplicaWriteGrant) -> Result<Self> {
        if !grant.replication.is_valid() || grant.token.is_empty() {
            return Err(invalid("replica write grant has no valid authority"));
        }
        // A grant carries no effective value yet; a stage that lowers it must
        // add one, or receivers will judge the initiator by the declared rule.
        let local_copy = EffectiveLocalCopy::declared(&grant.replication);
        let plan = ReplicationPlan {
            chunk_id: grant.chunk_id.clone(),
            placement_revision: grant.placement_revision,
            placement_epoch: grant.placement_epoch,
            replica_group_id: grant.replica_group_id.clone(),
            config: grant.replication.clone(),
            local_copy,
            ack_requirement: AckRequirement::for_writes(
                &grant.replication,
                grant.failure,
                local_copy,
            ),
            ordered_targets: grant.replica_group.targets.clone(),
        };
        plan.validate_initiator(&grant.initiator_node_id)?;
        if grant.replica_group.id != grant.replica_group_id
            || grant.replica_group.placement_epoch != grant.placement_epoch
        {
            return Err(invalid("replica write grant group identity differs"));
        }
        let sync_target_count = plan.sync_target_count()?;
        let target_index = grant.target_index as usize;
        if target_index >= sync_target_count {
            return Err(invalid("receiver is not part of synchronous chain"));
        }
        let target = plan.ordered_targets[target_index].clone();
        if target.node_id != grant.requester_node_id
            || target.node_epoch != grant.requester_node_epoch
        {
            return Err(invalid("replica write grant is not for this receiver"));
        }
        let result = Self {
            chunk_id: grant.chunk_id.clone(),
            placement_revision: grant.placement_revision,
            placement_epoch: grant.placement_epoch,
            replica_group_id: grant.replica_group_id.clone(),
            initiator_node_id: grant.initiator_node_id.clone(),
            initiator_node_epoch: grant.initiator_node_epoch,
            ordered_targets: plan.ordered_targets.clone(),
            sync_target_count,
            target_index,
            target,
            chain_tail: plan.ordered_targets[target_index + 1..sync_target_count].to_vec(),
            repair_claim: None,
        };
        result.validate_shape()?;
        Ok(result)
    }

    pub fn validate_sender(&self, authenticated_peer: &str) -> Result<()> {
        self.validate_shape()?;
        let expected = if self.target_index == 0 {
            self.initiator_node_id.as_str()
        } else {
            self.ordered_targets[self.target_index - 1].node_id.as_str()
        };
        if authenticated_peer != expected {
            return Err(Error::coded(
                afs_error::IO_PERMISSION_DENIED,
                "replica writer is not the authorized predecessor",
            ));
        }
        Ok(())
    }

    /// Validate domain/wire structure before asking authority or creating files.
    /// Authority still has to validate every epoch, endpoint and assignment.
    pub fn validate_shape(&self) -> Result<()> {
        if self.initiator_node_id.is_empty()
            || self.initiator_node_epoch == 0
            || self.placement_revision == 0
            || self.placement_epoch == 0
            || self.sync_target_count == 0
            || self.sync_target_count > self.ordered_targets.len()
            || self.target_index >= self.sync_target_count
            || self.ordered_targets.get(self.target_index) != Some(&self.target)
            || self.chain_tail
                != self.ordered_targets[self.target_index + 1..self.sync_target_count]
        {
            return Err(invalid(
                "replica operation has an invalid chain/initiator identity",
            ));
        }
        let mut nodes = HashSet::new();
        for target in &self.ordered_targets {
            if target.node_id.is_empty()
                || target.node_epoch == 0
                || target.device.device_id.is_empty()
                || target.device.device_epoch == 0
                || target.data_endpoint.is_empty()
                || !nodes.insert(target.node_id.as_str())
            {
                return Err(invalid(
                    "replica operation has an invalid or duplicate target",
                ));
            }
        }
        if let Some(claim) = &self.repair_claim
            && (claim.chunk.id != self.chunk_id
                || claim.worker_node_id != self.initiator_node_id
                || claim.worker_node_epoch != self.initiator_node_epoch
                || claim.placement_revision != self.placement_revision
                || claim.replica_group.id != self.replica_group_id
                || claim.replica_group.placement_epoch != self.placement_epoch
                || claim.replica_group.targets != self.ordered_targets
                || self.sync_target_count != self.ordered_targets.len())
        {
            return Err(invalid("repair operation differs from its frozen claim"));
        }
        Ok(())
    }

    pub fn next_hop(&self) -> Result<Option<Self>> {
        self.validate_shape()?;
        let target_index = self.target_index + 1;
        if target_index == self.sync_target_count {
            return Ok(None);
        }
        let mut next = self.clone();
        next.target_index = target_index;
        next.target = self.ordered_targets[target_index].clone();
        next.chain_tail = self.ordered_targets[target_index + 1..self.sync_target_count].to_vec();
        Ok(Some(next))
    }
}

impl ReplicationPlan {
    fn derive(staged: &StagedChunk, snapshot: &PlacementSnapshot) -> Result<Self> {
        if !snapshot.replication.is_valid() {
            return Err(invalid("Meta returned an invalid DFS replication config"));
        }
        let group = select_replica_group(&staged.chunk.id, snapshot)
            .ok_or_else(|| invalid("placement snapshot contains no replica group"))?;
        Self::from_group(staged, snapshot, group)
    }

    fn from_group(
        staged: &StagedChunk,
        snapshot: &PlacementSnapshot,
        group: &ReplicaGroup,
    ) -> Result<Self> {
        let ack_requirement = AckRequirement::for_writes(
            &snapshot.replication,
            snapshot.failure,
            snapshot.local_copy,
        );
        if group.targets.len() < ack_requirement.fewest_acks() {
            return Err(Error::coded(
                afs_error::NODE_VFS_UNIMPLEMENTED,
                "placement has fewer targets than the configured synchronous copy count",
            ));
        }
        Ok(Self {
            chunk_id: staged.chunk.id.clone(),
            placement_revision: snapshot.revision,
            placement_epoch: group.placement_epoch,
            replica_group_id: group.id.clone(),
            config: snapshot.replication.clone(),
            local_copy: snapshot.local_copy,
            ack_requirement,
            ordered_targets: group.targets.clone(),
        })
    }

    fn local_target(&self, local_node_id: &str) -> Option<&ReplicaTarget> {
        self.ordered_targets
            .iter()
            .find(|target| target.node_id == local_node_id)
    }

    pub(crate) fn sync_target_count(&self) -> Result<usize> {
        let minimum = self
            .ack_requirement
            .min_acks
            .max(self.ack_requirement.min_nodes);
        let mut domains = HashSet::new();
        for (index, target) in self.ordered_targets.iter().enumerate() {
            domains.insert(target.device.failure_domain.as_str());
            if index + 1 >= minimum && domains.len() >= self.ack_requirement.min_domains {
                return Ok(index + 1);
            }
        }
        // A degraded group (too few live Nodes for a complete one) is written
        // in full: every target it has is synchronous.
        if self
            .ack_requirement
            .degraded_floor
            .is_some_and(|floor| self.ordered_targets.len() >= floor)
        {
            return Ok(self.ordered_targets.len());
        }
        Err(invalid(
            "placement cannot satisfy synchronous node/failure-domain constraints",
        ))
    }

    pub(crate) fn validate_initiator(&self, local_node_id: &str) -> Result<()> {
        let mut nodes = HashSet::new();
        if self
            .ordered_targets
            .iter()
            .any(|target| !nodes.insert(target.node_id.as_str()))
        {
            return Err(invalid(
                "placement contains multiple targets on the same node",
            ));
        }
        if self.local_copy.requires_local()
            && self
                .ordered_targets
                .first()
                .is_none_or(|target| target.node_id != local_node_id)
        {
            return Err(invalid(
                "local-required placement must start at the writer node",
            ));
        }
        self.sync_target_count()?;
        Ok(())
    }

    fn selected_peer_op(
        &self,
        local_node_id: &str,
        local_node_epoch: u64,
    ) -> Result<Option<ReplicaPeerOp>> {
        let sync_target_count = self.sync_target_count()?;
        let local_head = self
            .ordered_targets
            .first()
            .is_some_and(|target| target.node_id == local_node_id);
        let target_index = usize::from(local_head);
        if target_index >= sync_target_count {
            return Ok(None);
        }
        let target = self
            .ordered_targets
            .get(target_index)
            .ok_or_else(|| invalid("replica first hop is absent"))?;
        Ok(Some(ReplicaPeerOp {
            chunk_id: self.chunk_id.clone(),
            placement_revision: self.placement_revision,
            placement_epoch: self.placement_epoch,
            replica_group_id: self.replica_group_id.clone(),
            initiator_node_id: local_node_id.into(),
            initiator_node_epoch: local_node_epoch,
            ordered_targets: self.ordered_targets.clone(),
            sync_target_count,
            target_index,
            target: target.clone(),
            chain_tail: self.ordered_targets[target_index + 1..sync_target_count].to_vec(),
            repair_claim: None,
        }))
    }

    fn shares_batch_group(&self, other: &Self) -> bool {
        self.placement_revision == other.placement_revision
            && self.placement_epoch == other.placement_epoch
            && self.replica_group_id == other.replica_group_id
            && self.config == other.config
            && self.local_copy == other.local_copy
            && self.ack_requirement == other.ack_requirement
            && self.ordered_targets == other.ordered_targets
    }
}

#[derive(Debug)]
struct ReplicationPlanGroup {
    plan: ReplicationPlan,
    indexes: Vec<usize>,
}

/// The ordered synchronous write: persist on this node when it heads the chain,
/// forward to the peers, then prove the result against the plan's
/// [`AckRequirement`]. Normal writes and repair share it; repair only adds the
/// Meta claim that every hop carries.
pub(crate) struct ReplicaChain {
    node_id: String,
    node_epoch: u64,
    local: Arc<LocalChunkStore>,
    data_plane: Arc<dyn ReplicaDataPlane>,
}

impl ReplicaChain {
    pub(crate) fn new(
        node_id: String,
        node_epoch: u64,
        local: Arc<LocalChunkStore>,
        data_plane: Arc<dyn ReplicaDataPlane>,
    ) -> Self {
        Self {
            node_id,
            node_epoch,
            local,
            data_plane,
        }
    }

    /// Structural peer setup, before any durable side effect.
    fn prepare(
        &self,
        plan: &ReplicationPlan,
        repair_claim: Option<&crate::dfs::ReplicationClaim>,
    ) -> Result<Option<ReplicaPeerOp>> {
        let mut op = plan.selected_peer_op(&self.node_id, self.node_epoch)?;
        if let Some(op) = op.as_mut() {
            op.repair_claim = repair_claim.cloned();
            self.data_plane.prepare_peer(op)?;
        }
        Ok(op)
    }

    /// Durable on this node if it heads the chain. A later hop failing does not
    /// roll this back; the unpublished copy is left for GC to collect.
    fn persist_head(
        &self,
        plan: &ReplicationPlan,
        staged: &[StagedChunk],
    ) -> Result<Vec<ReplicaAck>> {
        match plan
            .ordered_targets
            .first()
            .filter(|target| target.node_id == self.node_id)
        {
            Some(target) => self.local.persist_batch(
                staged,
                target,
                plan.placement_revision,
                plan.placement_epoch,
            ),
            None => Ok(Vec::new()),
        }
    }

    /// Sends the chunk down the chain. Under `degrade`, a failed hop keeps the
    /// copies already held when they meet the floor; `skip_remote` means an
    /// earlier chunk of the same group already did, so its chain is not tried
    /// again. Returns the ACKs and whether the write was degraded.
    fn forward(
        &self,
        plan: &ReplicationPlan,
        op: Option<&ReplicaPeerOp>,
        staged: &StagedChunk,
        local_ack: Option<ReplicaAck>,
        skip_remote: bool,
    ) -> Result<(Vec<ReplicaAck>, bool)> {
        let mut durable_acks = local_ack.into_iter().collect::<Vec<_>>();
        let mut degraded = false;
        if let Some(op) = op {
            let keep = plan.ack_requirement.may_keep(&durable_acks, &self.node_id);
            if skip_remote && keep {
                degraded = true;
            } else {
                match self.data_plane.put_peer_replica(op, staged) {
                    Ok(acks) => durable_acks.extend(acks),
                    Err(error)
                        if keep
                            && crate::node::fault::classify::replica_peer_failure_degrades(
                                &error,
                            ) =>
                    {
                        afs_logging::warn!("dfs.replica_write_degraded";
                            "chunk_id" => staged.chunk.id.0.as_str(),
                            "kept_copies" => durable_acks.len(),
                            "error" => error.to_string());
                        degraded = true;
                    }
                    Err(error) => return Err(error),
                }
            }
        }
        validate_acks(plan, staged, &durable_acks, &self.node_id)?;
        Ok((durable_acks, degraded))
    }

    /// A local-head strict pair has exactly one remote hop. An alternative
    /// group from this same Meta snapshot can replace that hop. Persisting
    /// the same immutable chunk reuses its local body and obtains a receipt
    /// for the replacement placement epoch.
    fn retry_strict_pair(
        &self,
        snapshot: &PlacementSnapshot,
        failed: &ReplicationPlan,
        staged: &StagedChunk,
        error: &Error,
    ) -> Result<Option<(ReplicationPlan, Vec<ReplicaAck>)>> {
        if snapshot.failure.policy != crate::dfs::ReplicaFailurePolicy::Strict
            || !matches!(
                error.kind(),
                ErrorKind::Unavailable | ErrorKind::DeadlineExceeded | ErrorKind::ResourceExhausted
            )
            || failed.sync_target_count()? != 2
        {
            return Ok(None);
        }
        let head = &failed.ordered_targets[0];
        let failed_peer = &failed.ordered_targets[1];
        if head.node_id != self.node_id {
            return Ok(None);
        }
        let mut last_error = None;
        for group in &snapshot.replica_groups {
            if !matches!(group.targets.as_slice(), [local, peer, ..]
                if local == head && peer.node_id != failed_peer.node_id)
            {
                continue;
            }
            let plan = ReplicationPlan::from_group(staged, snapshot, group)?;
            if plan.sync_target_count()? != 2 {
                continue;
            }
            let op = self.prepare(&plan, None)?;
            let local_ack = self
                .persist_head(&plan, std::slice::from_ref(staged))?
                .into_iter()
                .next();
            let acks = match self.forward(&plan, op.as_ref(), staged, local_ack, false) {
                Ok((acks, _)) => acks,
                Err(error)
                    if matches!(
                        error.kind(),
                        ErrorKind::Unavailable
                            | ErrorKind::DeadlineExceeded
                            | ErrorKind::ResourceExhausted
                    ) =>
                {
                    last_error = Some(error);
                    continue;
                }
                Err(error) => return Err(error),
            };
            afs_logging::info!("dfs.replica_write_replaced";
                "chunk_id" => staged.chunk.id.0.as_str(),
                "failed_node" => failed_peer.node_id.as_str(),
                "replacement_node" => plan.ordered_targets[1].node_id.as_str(),
                "replica_group_id" => plan.replica_group_id.0.as_str()
            );
            return Ok(Some((plan, acks)));
        }
        match last_error {
            Some(error) => Err(error),
            None => Ok(None),
        }
    }

    fn put_batch(
        &self,
        staged: Vec<StagedChunk>,
        snapshot: &PlacementSnapshot,
        retry_strict_pair: bool,
    ) -> Result<Vec<StoredChunk>> {
        let plans = staged
            .iter()
            .map(|item| ReplicationPlan::derive(item, snapshot))
            .collect::<Result<Vec<_>>>()?;
        let groups = group_plans(&plans);

        // Check the whole batch before creating local durable side effects.
        let mut ops = Vec::with_capacity(plans.len());
        for plan in &plans {
            plan.validate_initiator(&self.node_id)?;
            ops.push(self.prepare(plan, None)?);
        }

        let mut receipts = vec![None; staged.len()];
        for group in groups {
            let group_staged = group
                .indexes
                .iter()
                .map(|index| staged[*index].clone())
                .collect::<Vec<_>>();
            let local_acks = self.persist_head(&group.plan, &group_staged)?;
            // One failed hop degrades the rest of this group without waiting
            // for the same peer once per chunk.
            let mut degraded = false;
            for ((index, item), local_ack) in group.indexes.iter().copied().zip(group_staged).zip(
                local_acks
                    .into_iter()
                    .map(Some)
                    .chain(std::iter::repeat(None)),
            ) {
                let plan = &plans[index];
                let alternate;
                let (plan, durable_acks) =
                    match self.forward(plan, ops[index].as_ref(), &item, local_ack, degraded) {
                        Ok((acks, this_degraded)) => {
                            degraded |= this_degraded;
                            (plan, acks)
                        }
                        Err(error) if retry_strict_pair => {
                            let Some((replacement, acks)) =
                                self.retry_strict_pair(snapshot, plan, &item, &error)?
                            else {
                                return Err(error);
                            };
                            alternate = replacement;
                            (&alternate, acks)
                        }
                        Err(error) => return Err(error),
                    };
                receipts[index] = Some(StoredChunk::unreleased(ChunkReceipt {
                    external_commit: None,
                    operation_id: item.operation_id,
                    chunk: item.chunk,
                    placement_revision: plan.placement_revision,
                    placement_epoch: plan.placement_epoch,
                    replica_group_id: plan.replica_group_id.clone(),
                    durable_acks,
                }));
            }
        }
        receipts
            .into_iter()
            .map(|receipt| receipt.ok_or_else(|| invalid("replication receipt was not produced")))
            .collect()
    }

    /// One chunk along a repair plan the caller has checked is source-first.
    pub(crate) fn put_repair(
        &self,
        plan: &ReplicationPlan,
        staged: &StagedChunk,
        claim: &crate::dfs::ReplicationClaim,
    ) -> Result<Vec<ReplicaAck>> {
        let op = self.prepare(plan, Some(claim))?;
        let local_ack = self
            .persist_head(plan, std::slice::from_ref(staged))?
            .into_iter()
            .next();
        // A repair plan is strict, so it never degrades.
        self.forward(plan, op.as_ref(), staged, local_ack, false)
            .map(|(acks, _)| acks)
    }
}

/// One synchronous copy on this node: persist locally, nothing to forward.
/// `DfsChunkSink` only routes here when the placement is a local fast path.
struct LocalR1Path {
    node_id: String,
    local: Arc<LocalChunkStore>,
}

impl LocalR1Path {
    fn put_batch(
        &self,
        staged: Vec<StagedChunk>,
        snapshot: &PlacementSnapshot,
    ) -> Result<Vec<StoredChunk>> {
        if staged.is_empty() {
            return Ok(Vec::new());
        }
        let plans = staged
            .iter()
            .map(|item| ReplicationPlan::derive(item, snapshot))
            .collect::<Result<Vec<_>>>()?;
        for plan in &plans {
            plan.validate_initiator(&self.node_id)?;
        }
        let mut receipts = vec![None; staged.len()];
        for group in group_plans(&plans) {
            let target = group
                .plan
                .local_target(&self.node_id)
                .ok_or_else(|| invalid("local R1 placement does not contain this node"))?;
            let group_staged = group
                .indexes
                .iter()
                .map(|index| staged[*index].clone())
                .collect::<Vec<_>>();
            let local_acks = self.local.persist_batch(
                &group_staged,
                target,
                group.plan.placement_revision,
                group.plan.placement_epoch,
            )?;
            for ((index, item), ack) in group
                .indexes
                .iter()
                .copied()
                .zip(group_staged)
                .zip(local_acks)
            {
                receipts[index] = Some(StoredChunk::unreleased(ChunkReceipt {
                    external_commit: None,
                    operation_id: item.operation_id,
                    chunk: item.chunk,
                    placement_revision: group.plan.placement_revision,
                    placement_epoch: group.plan.placement_epoch,
                    replica_group_id: group.plan.replica_group_id.clone(),
                    durable_acks: vec![ack],
                }));
            }
        }
        receipts
            .into_iter()
            .map(|receipt| receipt.ok_or_else(|| invalid("local receipt was not produced")))
            .collect()
    }
}

/// The native [`ChunkSink`]: routes each batch to [`LocalR1Path`] or
/// [`ReplicaChain`] and decides the resulting [`BodyRelease`].
pub struct DfsChunkSink {
    local_node_id: String,
    local_node_epoch: Option<u64>,
    local: Arc<LocalChunkStore>,
    placement: Arc<dyn PlacementProvider>,
    local_r1: LocalR1Path,
    chain: ReplicaChain,
}

impl DfsChunkSink {
    pub fn new(
        local_node_id: String,
        local: Arc<LocalChunkStore>,
        placement: Arc<dyn PlacementProvider>,
        data_plane: Arc<dyn ReplicaDataPlane>,
    ) -> Self {
        Self::assemble(local_node_id, None, local, placement, data_plane)
    }

    pub fn new_with_epoch(
        local_node_id: String,
        local_node_epoch: u64,
        local: Arc<LocalChunkStore>,
        placement: Arc<dyn PlacementProvider>,
        data_plane: Arc<dyn ReplicaDataPlane>,
    ) -> Self {
        Self::assemble(
            local_node_id,
            Some(local_node_epoch),
            local,
            placement,
            data_plane,
        )
    }

    fn assemble(
        local_node_id: String,
        local_node_epoch: Option<u64>,
        local: Arc<LocalChunkStore>,
        placement: Arc<dyn PlacementProvider>,
        data_plane: Arc<dyn ReplicaDataPlane>,
    ) -> Self {
        Self {
            local_r1: LocalR1Path {
                node_id: local_node_id.clone(),
                local: local.clone(),
            },
            chain: ReplicaChain::new(
                local_node_id.clone(),
                local_node_epoch.unwrap_or(0),
                local.clone(),
                data_plane,
            ),
            local_node_id,
            local_node_epoch,
            local,
            placement,
        }
    }

    /// The chain always writes under freshly refreshed placement; an empty
    /// batch asks Meta nothing.
    fn put_replicated_batch(
        &self,
        staged: Vec<StagedChunk>,
        minimum_revision: u64,
        intent: PutIntent,
    ) -> Result<Vec<StoredChunk>> {
        if staged.is_empty() {
            return Ok(Vec::new());
        }
        let snapshot = self.placement.refresh(minimum_revision)?;
        self.chain
            .put_batch(staged, &snapshot, intent == PutIntent::PipelineUpload)
    }

    fn validate_local_capacity_authority(&self, snapshot: &PlacementSnapshot) -> Result<()> {
        if !snapshot.is_local_fast_path() {
            return Err(unsupported_capacity(
                "DFS capacity requires local R1 placement authority",
            ));
        }
        if snapshot.replica_groups.is_empty() {
            return Err(unsupported_capacity(
                "DFS capacity requires a local placement authority",
            ));
        }
        let local = self.local.device_descriptor()?;
        for group in &snapshot.replica_groups {
            if group.targets.len() != 1 {
                return Err(unsupported_capacity(
                    "DFS capacity is unsupported for replicated placement",
                ));
            }
            let target = &group.targets[0];
            if target.node_id != self.local_node_id
                || self
                    .local_node_epoch
                    .is_some_and(|epoch| target.node_epoch != epoch)
            {
                return Err(unsupported_capacity(
                    "DFS capacity requires local node placement authority",
                ));
            }
            if target.device.device_id != local.device_id
                || target.device.device_epoch != local.device_epoch
                || target.device.failure_domain != local.failure_domain
            {
                return Err(invalid(
                    "DFS capacity placement does not match the local storage device authority",
                ));
            }
        }
        Ok(())
    }

    /// Under an effective Required policy, an uploaded body may be released once
    /// this node's exact durable copy of it is proven.
    fn confirm_local_required_copies(&self, stored: &mut [StoredChunk]) -> Result<()> {
        let device = self.local.device_descriptor()?;
        for stored in stored {
            let receipt = &stored.receipt;
            let local_ack = receipt
                .durable_acks
                .iter()
                .find(|ack| {
                    ack.node_id == self.local_node_id
                        && self
                            .local_node_epoch
                            .is_none_or(|epoch| ack.node_epoch == epoch)
                        && ack.device_id == device.device_id
                        && ack.device_epoch == device.device_epoch
                })
                .ok_or_else(|| {
                    invalid("native Required receipt has no matching local durable ACK")
                })?;
            if local_ack.operation_id != receipt.operation_id
                || local_ack.chunk_id != receipt.chunk.id
                || local_ack.placement_revision != receipt.placement_revision
                || local_ack.placement_epoch != receipt.placement_epoch
                || local_ack.persisted_bytes != receipt.chunk.length
                || local_ack.verified_digest != receipt.chunk.content_digest
            {
                return Err(invalid("native local ACK does not match its exact receipt"));
            }
            // Pinning checks catalog and content-addressed identity without
            // another full scan. Every later local read verifies its groups.
            self.local.open_receipt_reader(&receipt.chunk)?;
            stored.body_release = BodyRelease::ReleaseAfterUpload;
        }
        Ok(())
    }
}

impl ChunkSink for DfsChunkSink {
    fn put_chunks(&self, staged: Vec<StagedChunk>, intent: PutIntent) -> Result<Vec<StoredChunk>> {
        // Native writes fence their own write generation at commit; chunks
        // are never registered per publication (S1a-7, F6; R33 removed the
        // `publish:` reservation that only ever saw empty batches).
        let snapshot = self.placement.snapshot()?;
        let mut stored = if snapshot.is_local_fast_path() {
            self.local_r1.put_batch(staged, &snapshot)?
        } else {
            self.put_replicated_batch(staged, snapshot.revision, intent)?
        };
        if intent == PutIntent::PipelineUpload && snapshot.local_copy.requires_local() {
            self.confirm_local_required_copies(&mut stored)?;
        }
        Ok(stored)
    }

    fn descriptor_reservation_bytes(&self, operation_id: &OperationId) -> Result<usize> {
        let snapshot = self.placement.snapshot()?;
        if !snapshot.replication.is_valid() || snapshot.replica_groups.is_empty() {
            return Err(invalid(
                "native descriptor admission needs valid placement authority",
            ));
        }
        let synchronous = snapshot
            .failure
            .degraded_floor(&snapshot.replication)
            .unwrap_or(usize::from(snapshot.replication.sync_required_copies));
        let mut receipt = 0usize;
        for group in &snapshot.replica_groups {
            if group.targets.len() < synchronous {
                return Err(invalid("native placement has too few synchronous targets"));
            }
            let ack_bytes = group.targets.iter().try_fold(0usize, |sum, target| {
                sum.checked_add(
                    std::mem::size_of::<ReplicaAck>()
                        + operation_id.0.capacity()
                        + 128
                        + target.node_id.capacity()
                        + target.device.device_id.capacity(),
                )
                .ok_or_else(|| invalid("native descriptor capacity overflow"))
            })?;
            receipt = receipt.max(
                std::mem::size_of::<ChunkReceipt>()
                    + operation_id.0.capacity()
                    + 128
                    + group.id.0.capacity()
                    + ack_bytes,
            );
        }
        // This is a pre-admission reservation, not an actual-capacity metric.
        // It includes owned descriptor state, layout/domain clones and bounded
        // publication scratch while keeping the original 4MiB envelope fixed.
        receipt
            .checked_mul(8)
            .and_then(|bytes| bytes.checked_add(8192))
            .ok_or_else(|| invalid("native descriptor reservation overflow"))
    }

    fn read_uploaded_at(
        &self,
        receipt: &ChunkReceipt,
        offset: u64,
        out: &mut [u8],
    ) -> Result<usize> {
        let reader = self.local.open_receipt_reader(&receipt.chunk)?;
        let result = reader.read_at(offset, out);
        if result.is_err() {
            // This rechecks the actual local object before quarantining; a
            // caller's expected-root mismatch above never quarantines it. A
            // failed quarantine is logged (R52) and the read error still wins.
            self.local
                .lifecycle()
                .quarantine(&receipt.chunk.id, QuarantineTrigger::UploadedReadBack);
        }
        result
    }

    fn capacity(&self) -> Result<FilesystemCapacity> {
        let snapshot = self.placement.snapshot()?;
        self.validate_local_capacity_authority(&snapshot)?;
        self.local.capacity()
    }
}

fn group_plans(plans: &[ReplicationPlan]) -> Vec<ReplicationPlanGroup> {
    let mut groups: Vec<ReplicationPlanGroup> = Vec::new();
    for (index, plan) in plans.iter().enumerate() {
        if let Some(group) = groups
            .iter_mut()
            .find(|group| group.plan.shares_batch_group(plan))
        {
            group.indexes.push(index);
            continue;
        }
        groups.push(ReplicationPlanGroup {
            plan: plan.clone(),
            indexes: vec![index],
        });
    }
    groups
}

fn select_replica_group<'a>(
    chunk_id: &ChunkId,
    snapshot: &'a PlacementSnapshot,
) -> Option<&'a ReplicaGroup> {
    if snapshot.replica_groups.is_empty() {
        return None;
    }
    let index = stable_group_index(chunk_id, snapshot.replica_groups.len());
    snapshot.replica_groups.get(index)
}

fn stable_group_index(chunk_id: &ChunkId, group_count: usize) -> usize {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in chunk_id.0.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    (hash as usize) % group_count
}

fn invalid(message: impl Into<String>) -> Error {
    Error::coded(afs_error::NODE_STORAGE_INVALID, message)
}

fn unsupported_capacity(message: impl Into<String>) -> Error {
    Error::coded(afs_error::NODE_VFS_UNIMPLEMENTED, message)
}

mod acks;
#[cfg(test)]
mod tests;

pub use acks::AckRequirement;
use acks::validate_acks;
pub(crate) use acks::validate_peer_acks;
