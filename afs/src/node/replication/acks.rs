//! What a replica chain must prove: the requirement and the ACK checks.

use std::collections::HashSet;

use afs_error::Result;

use super::{ReplicaPeerOp, ReplicationPlan, invalid};
use crate::{
    dfs::{EffectiveLocalCopy, ReplicaAck, ReplicaFailureConfig, ReplicationConfig},
    node::chunk::StagedChunk,
};

/// What a replica chain must prove before a write counts as durable.
///
/// A degraded write (R12) keeps the copies this Node holds when a later hop
/// fails: the chain is cut at its head, so the copies are this Node's own.
/// A failed hop further down still fails the whole tail, because
/// [`validate_peer_acks`] demands the exact tail and no chain reply carries a
/// partial prefix. Repair is always strict.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AckRequirement {
    pub(super) min_acks: usize,
    pub(super) min_nodes: usize,
    pub(super) min_domains: usize,
    pub(super) require_initiator: bool,
    /// Fewest copies a degraded write may keep; `None` is strict.
    pub(super) degraded_floor: Option<usize>,
}

impl AckRequirement {
    pub fn strict(config: &ReplicationConfig, local_copy: EffectiveLocalCopy) -> Self {
        Self {
            min_acks: usize::from(config.sync_required_copies),
            min_nodes: usize::from(config.min_distinct_nodes),
            min_domains: usize::from(config.min_distinct_failure_domains),
            require_initiator: local_copy.requires_local(),
            degraded_floor: None,
        }
    }

    /// An ordinary write under the filesystem's replica failure contract.
    pub fn for_writes(
        config: &ReplicationConfig,
        failure: ReplicaFailureConfig,
        local_copy: EffectiveLocalCopy,
    ) -> Self {
        Self {
            degraded_floor: failure.degraded_floor(config),
            ..Self::strict(config, local_copy)
        }
    }

    pub(super) fn fewest_acks(self) -> usize {
        self.degraded_floor.unwrap_or(self.min_acks)
    }

    /// The copies already held suffice for a degraded commit.
    pub(super) fn may_keep(self, acks: &[ReplicaAck], initiator: &str) -> bool {
        self.degraded_floor.is_some_and(|floor| acks.len() >= floor)
            && (!self.require_initiator || acks.iter().any(|ack| ack.node_id == initiator))
    }
}

/// Validate an ordered synchronous tail before acknowledging its predecessor.
pub(crate) fn validate_peer_acks(
    op: &ReplicaPeerOp,
    staged: &StagedChunk,
    acks: &[ReplicaAck],
) -> Result<()> {
    op.validate_shape()?;
    let targets = &op.ordered_targets[op.target_index..op.sync_target_count];
    if acks.len() != targets.len() {
        return Err(invalid("replica acknowledgement tail is incomplete"));
    }
    for (ack, target) in acks.iter().zip(targets) {
        if ack.operation_id != staged.operation_id
            || ack.chunk_id != staged.chunk.id
            || ack.placement_revision != op.placement_revision
            || ack.placement_epoch != op.placement_epoch
            || ack.node_id != target.node_id
            || ack.node_epoch != target.node_epoch
            || ack.device_id != target.device.device_id
            || ack.device_epoch != target.device.device_epoch
            || ack.catalog_revision < target.device.catalog_revision
            || ack.persisted_bytes != staged.chunk.length
            || ack.verified_digest != staged.chunk.content_digest
        {
            return Err(invalid(
                "replica acknowledgement differs from authorized immutable tail",
            ));
        }
    }
    Ok(())
}

pub(super) fn validate_acks(
    plan: &ReplicationPlan,
    staged: &StagedChunk,
    durable_acks: &[ReplicaAck],
    initiator: &str,
) -> Result<()> {
    let mut acknowledged_targets = HashSet::new();
    let mut domains = HashSet::new();
    for ack in durable_acks {
        if ack.operation_id != staged.operation_id
            || ack.chunk_id != staged.chunk.id
            || ack.placement_revision != plan.placement_revision
            || ack.placement_epoch != plan.placement_epoch
            || ack.persisted_bytes != staged.chunk.length
            || ack.verified_digest != staged.chunk.content_digest
        {
            return Err(invalid(
                "replica acknowledgement does not prove the staged Chunk",
            ));
        }
        let assigned = plan.ordered_targets.iter().any(|target| {
            target.node_id == ack.node_id
                && target.node_epoch == ack.node_epoch
                && target.device.device_id == ack.device_id
                && target.device.device_epoch == ack.device_epoch
                && ack.catalog_revision >= target.device.catalog_revision
        });
        if !assigned {
            return Err(invalid(
                "replica acknowledgement is not part of the ReplicationPlan",
            ));
        }
        if !acknowledged_targets.insert(ack.node_id.clone()) {
            return Err(invalid(
                "replica acknowledgements contain a duplicate target",
            ));
        }
        let target = plan
            .ordered_targets
            .iter()
            .find(|target| target.node_id == ack.node_id)
            .expect("assigned acknowledgement checked above");
        domains.insert(target.device.failure_domain.as_str());
    }
    let requirement = plan.ack_requirement;
    // Node and domain minima describe a complete placement; a degraded write
    // keeps only the local-copy rule and its floor.
    if requirement.may_keep(durable_acks, initiator) {
        return Ok(());
    }
    if acknowledged_targets.len() < requirement.min_nodes
        || domains.len() < requirement.min_domains
        || (requirement.require_initiator && !acknowledged_targets.contains(initiator))
    {
        return Err(invalid(
            "replica acknowledgements violate node, failure-domain or local-copy constraints",
        ));
    }
    if acknowledged_targets.len() < requirement.min_acks {
        return Err(invalid(
            "replica acknowledgements do not satisfy the synchronous copy count",
        ));
    }
    Ok(())
}
