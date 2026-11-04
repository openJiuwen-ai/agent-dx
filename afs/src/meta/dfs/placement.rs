//! Placement snapshots, replica write validation and replica group construction.

use super::*;

impl DfsService {
    pub async fn placement_snapshot(&self, caller_id: String) -> Result<PlacementSnapshot> {
        if self.is_external_backend() {
            return Err(invalid(
                "external durability does not use Node replica placement",
            ));
        }
        require_id(&caller_id, "caller_id")?;
        let config_snapshot = self.store.read(MetaRead::DfsReplicationConfig).await?;
        let replication = match config_snapshot.entity {
            Some(MetaEntity::DfsReplicationConfig(config)) => config,
            _ => return Err(invalid("DFS replication config is not initialized")),
        };
        if !replication.is_valid() {
            return Err(invalid("DFS replication config is invalid"));
        }

        let sessions_snapshot = self.store.read(MetaRead::CurrentNodeSessions).await?;
        let now = now_unix_ms();
        let mut sessions = sessions_snapshot
            .entities
            .into_iter()
            .filter_map(|entity| match entity {
                MetaEntity::NodeSession(session) if session.is_live_at_unix_ms(now) => {
                    Some(session)
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        sessions.sort_by(|left, right| left.node_id.cmp(&right.node_id));

        let caller_session = sessions.iter().find(|session| session.node_id == caller_id);
        if caller_session.is_none() {
            return Err(conflict(
                "DFS placement requires a live caller Node session",
            ));
        }
        let storage_sessions = sessions
            .iter()
            .filter(|session| !session.storage_devices.is_empty())
            .cloned()
            .collect::<Vec<_>>();

        let local_copy = EffectiveLocalCopy::declared(&replication);
        let replica_groups =
            build_replica_groups(&caller_id, &replication, local_copy, &storage_sessions)?;
        Ok(PlacementSnapshot {
            revision: config_snapshot.revision.0.max(sessions_snapshot.revision.0),
            replication,
            replica_groups,
            local_copy,
        })
    }

    /// Authorizes one receiver to durably accept one chunk replica for a
    /// current placement group. The requester is the authenticated receiver,
    /// while the initiator is the node that selected the chain. Meta rebuilds
    /// the initiator-scoped placement and never trusts a receiver-local
    /// caller-specific snapshot.
    pub async fn validate_replica_write(
        &self,
        request: ValidateReplicaWriteRequest,
    ) -> Result<ReplicaWriteGrant> {
        if self.is_external_backend() {
            return Err(invalid(
                "external durability does not accept Node replica writes",
            ));
        }
        require_id(&request.requester_node_id, "requester_node_id")?;
        require_id(&request.initiator_node_id, "initiator_node_id")?;
        require_id(&request.operation_id.0, "operation_id")?;
        require_id(&request.chunk_id.0, "chunk_id")?;
        require_id(&request.replica_group_id.0, "replica_group_id")?;
        if request.requester_node_epoch == 0 || request.initiator_node_epoch == 0 {
            return Err(invalid("DFS replica write node epochs are required"));
        }
        if request.chunk_length == 0 {
            return Err(invalid(
                "DFS replica write chunk_length must be greater than zero",
            ));
        }
        if request.content_digest.bytes == [0; 32] {
            return Err(invalid("DFS replica write content digest is required"));
        }
        if request.placement_revision == 0 || request.placement_epoch == 0 {
            return Err(invalid("DFS replica write placement identity is required"));
        }
        if request.ordered_targets.is_empty() {
            return Err(invalid("DFS replica write ordered_targets are required"));
        }
        if request.repair_claim.is_some() {
            return self.validate_repair_replica_write(request).await;
        }

        let initiator_snapshot = self
            .store
            .read(MetaRead::CurrentNodeSession {
                node_id: request.initiator_node_id.clone(),
            })
            .await?;
        match initiator_snapshot.entity {
            Some(MetaEntity::NodeSession(session))
                if session.lease_epoch == request.initiator_node_epoch
                    && session.is_live_at_unix_ms(now_unix_ms()) => {}
            _ => return Err(conflict("DFS replica write initiator epoch is not live")),
        }

        let placement = self
            .placement_snapshot(request.initiator_node_id.clone())
            .await?;
        if request.placement_revision > placement.revision {
            return Err(conflict(
                "DFS replica write references a future placement revision",
            ));
        }
        let group = placement
            .replica_groups
            .iter()
            .find(|group| group.id == request.replica_group_id)
            .filter(|group| group.placement_epoch == request.placement_epoch)
            .ok_or_else(|| conflict("DFS replica write references a stale ReplicaGroup"))?;
        if !replica_targets_match_requested_floor(&request.ordered_targets, &group.targets) {
            return Err(conflict(
                "DFS replica write target order does not match current placement",
            ));
        }
        let target_index = usize::try_from(request.target_index)
            .map_err(|_| invalid("DFS replica write target_index is invalid"))?;
        let target = group
            .targets
            .get(target_index)
            .ok_or_else(|| invalid("DFS replica write target_index is out of range"))?;
        if target.node_id != request.requester_node_id
            || target.node_epoch != request.requester_node_epoch
        {
            return Err(permission_denied(
                "DFS replica write requester is not the assigned placement target",
            ));
        }

        let expires_at_unix_ms = now_unix_ms().saturating_add(30_000);
        let fence = placement.revision;
        let token = replica_write_token(&request, fence, expires_at_unix_ms);
        Ok(ReplicaWriteGrant {
            requester_node_id: request.requester_node_id,
            requester_node_epoch: request.requester_node_epoch,
            initiator_node_id: request.initiator_node_id,
            initiator_node_epoch: request.initiator_node_epoch,
            operation_id: request.operation_id,
            chunk_id: request.chunk_id,
            chunk_length: request.chunk_length,
            content_digest: request.content_digest,
            placement_revision: request.placement_revision,
            placement_epoch: request.placement_epoch,
            replica_group_id: request.replica_group_id,
            target_index: request.target_index,
            replication: placement.replication.clone(),
            // Authority has checked the frozen order and device identities
            // against current placement. Keep the caller's validated catalog
            // floors so granting a retry cannot rewrite its transfer identity.
            replica_group: ReplicaGroup {
                id: group.id.clone(),
                placement_epoch: group.placement_epoch,
                targets: request.ordered_targets,
            },
            expires_at_unix_ms,
            fence,
            token,
        })
    }
}

pub(super) fn build_replica_groups(
    caller_id: &str,
    replication: &ReplicationConfig,
    local_copy: EffectiveLocalCopy,
    sessions: &[NodeSession],
) -> Result<Vec<ReplicaGroup>> {
    let mut targets = sessions
        .iter()
        .filter_map(preferred_replica_target)
        .collect::<Vec<_>>();
    targets.sort_by(|left, right| {
        left.node_id
            .cmp(&right.node_id)
            .then(left.node_epoch.cmp(&right.node_epoch))
            .then(left.device.device_id.cmp(&right.device.device_id))
            .then(left.device.device_epoch.cmp(&right.device.device_epoch))
    });
    targets.dedup_by(|left, right| left.node_id == right.node_id);

    let caller_target = targets
        .iter()
        .find(|target| target.node_id == caller_id)
        .cloned();
    if local_copy.requires_local() && caller_target.is_none() {
        return Err(conflict(
            "DFS placement requires a caller-local storage target for local-copy=required",
        ));
    }

    let required_targets = usize::from(replication.sync_required_copies)
        .max(usize::from(replication.min_distinct_nodes))
        .max(usize::from(replication.min_distinct_failure_domains));
    if targets.len() < required_targets {
        return Err(conflict(
            "DFS placement has too few live storage nodes for the configured policy",
        ));
    }
    let target_count = usize::from(replication.desired_copies).min(targets.len());
    if target_count < required_targets {
        return Err(conflict(
            "DFS placement cannot satisfy sync, node or failure-domain minima",
        ));
    }

    let mut groups = Vec::new();
    match caller_target.filter(|_| local_copy.prefers_local_head()) {
        Some(head) => {
            let tail = targets
                .iter()
                .filter(|target| target.node_id != head.node_id)
                .cloned()
                .collect::<Vec<_>>();
            let rotations = tail.len().max(1);
            for seed in 0..rotations {
                if let Some(group) = build_replica_group(
                    replication,
                    local_copy,
                    Some(head.clone()),
                    &tail,
                    target_count,
                    seed,
                ) {
                    groups.push(group);
                }
            }
        }
        None => {
            for seed in 0..targets.len() {
                if let Some(group) =
                    build_replica_group(replication, local_copy, None, &targets, target_count, seed)
                {
                    groups.push(group);
                }
            }
        }
    }

    groups.sort_by(|left, right| left.id.0.cmp(&right.id.0));
    groups.dedup_by(|left, right| left.id == right.id);
    if groups.is_empty() {
        return Err(conflict(
            "DFS placement could not build a replica group satisfying the configured policy",
        ));
    }
    Ok(groups)
}

fn preferred_replica_target(session: &NodeSession) -> Option<ReplicaTarget> {
    let mut devices = session.storage_devices.clone();
    devices.sort_by(|left, right| {
        left.device_id
            .cmp(&right.device_id)
            .then(left.device_epoch.cmp(&right.device_epoch))
            .then(left.failure_domain.cmp(&right.failure_domain))
    });
    devices.into_iter().next().map(|device| ReplicaTarget {
        node_id: session.node_id.clone(),
        node_epoch: session.lease_epoch,
        data_endpoint: session.data_addr.clone(),
        device,
    })
}

fn build_replica_group(
    replication: &ReplicationConfig,
    local_copy: EffectiveLocalCopy,
    head: Option<ReplicaTarget>,
    candidates: &[ReplicaTarget],
    target_count: usize,
    seed: usize,
) -> Option<ReplicaGroup> {
    let mut selected = Vec::with_capacity(target_count);
    let mut nodes = HashSet::new();
    let mut domains = HashSet::new();
    if let Some(head) = head {
        nodes.insert(head.node_id.clone());
        domains.insert(head.device.failure_domain.clone());
        selected.push(head);
    }

    let rotated = (0..candidates.len())
        .map(|offset| candidates[(seed + offset) % candidates.len()].clone())
        .collect::<Vec<_>>();
    for prefer_new_domain in [true, false] {
        for target in &rotated {
            if selected.len() >= target_count {
                break;
            }
            if nodes.contains(&target.node_id) {
                continue;
            }
            let new_domain = !domains.contains(&target.device.failure_domain);
            if prefer_new_domain && !new_domain {
                continue;
            }
            nodes.insert(target.node_id.clone());
            domains.insert(target.device.failure_domain.clone());
            selected.push(target.clone());
        }
    }

    if selected.len() < usize::from(replication.sync_required_copies)
        || nodes.len() < usize::from(replication.min_distinct_nodes)
        || domains.len() < usize::from(replication.min_distinct_failure_domains)
    {
        return None;
    }
    let placement_epoch = effective_placement_hash(replication, local_copy, &selected);
    Some(ReplicaGroup {
        id: ReplicaGroupId::new(format!("group:{placement_epoch:016x}")),
        placement_epoch,
        targets: selected,
    })
}

fn replica_targets_match_requested_floor(
    requested: &[ReplicaTarget],
    current: &[ReplicaTarget],
) -> bool {
    requested.len() == current.len()
        && requested.iter().zip(current).all(|(requested, current)| {
            requested.node_id == current.node_id
                && requested.node_epoch == current.node_epoch
                && requested.data_endpoint == current.data_endpoint
                && requested.device.device_id == current.device.device_id
                && requested.device.device_epoch == current.device.device_epoch
                && requested.device.failure_domain == current.device.failure_domain
                && requested.device.catalog_revision <= current.device.catalog_revision
        })
}

pub(super) fn replica_write_token(
    request: &ValidateReplicaWriteRequest,
    fence: u64,
    expires_at_unix_ms: u64,
) -> String {
    let mut hash = StableHash::new();
    hash.write_str(&request.requester_node_id);
    hash.write_u64(request.requester_node_epoch);
    hash.write_str(&request.initiator_node_id);
    hash.write_u64(request.initiator_node_epoch);
    hash.write_str(&request.operation_id.0);
    hash.write_str(&request.chunk_id.0);
    hash.write_u64(request.chunk_length);
    hash.write_bytes(&request.content_digest.bytes);
    hash.write_u64(request.placement_revision);
    hash.write_u64(request.placement_epoch);
    hash.write_str(&request.replica_group_id.0);
    hash.write_u64(u64::from(request.target_index));
    for target in &request.ordered_targets {
        hash.write_str(&target.node_id);
        hash.write_u64(target.node_epoch);
        hash.write_str(&target.device.device_id);
        hash.write_u64(target.device.device_epoch);
        hash.write_u64(target.device.catalog_revision);
    }
    hash.write_u64(fence);
    hash.write_u64(expires_at_unix_ms);
    format!("dfs-replica-write:{:016x}", hash.finish_nonzero())
}

/// `placement_hash` of a placement that executes its declared local-copy rule.
#[cfg(test)]
pub(super) fn placement_hash(replication: &ReplicationConfig, targets: &[ReplicaTarget]) -> u64 {
    effective_placement_hash(
        replication,
        EffectiveLocalCopy::declared(replication),
        targets,
    )
}

/// Mixes the effective local-copy rule, so a placement whose rule changes gets
/// a new epoch and its old groups stop matching. While effective equals
/// declared the output is identical to hashing the declared rule.
pub(super) fn effective_placement_hash(
    replication: &ReplicationConfig,
    local_copy: EffectiveLocalCopy,
    targets: &[ReplicaTarget],
) -> u64 {
    let mut hash = StableHash::new();
    hash.write_u64(u64::from(replication.desired_copies));
    hash.write_u64(u64::from(replication.sync_required_copies));
    hash.write_u64(u64::from(replication.min_distinct_nodes));
    hash.write_u64(u64::from(replication.min_distinct_failure_domains));
    hash.write_u64(match local_copy.effective_policy() {
        LocalCopyPolicy::Required => 1,
        LocalCopyPolicy::Preferred => 2,
        LocalCopyPolicy::NotRequired => 3,
    });
    for target in targets {
        hash.write_str(&target.node_id);
        hash.write_u64(target.node_epoch);
        hash.write_str(&target.data_endpoint);
        hash.write_str(&target.device.device_id);
        hash.write_u64(target.device.device_epoch);
        hash.write_str(&target.device.failure_domain);
    }
    hash.finish_nonzero()
}

struct StableHash(u64);

impl StableHash {
    fn new() -> Self {
        Self(0xcbf2_9ce4_8422_2325)
    }

    fn write_str(&mut self, value: &str) {
        self.write_bytes(value.as_bytes());
        self.write_bytes(&[0xff]);
    }

    fn write_u64(&mut self, value: u64) {
        self.write_bytes(&value.to_le_bytes());
    }

    fn write_bytes(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 ^= u64::from(*byte);
            self.0 = self.0.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }

    fn finish_nonzero(self) -> u64 {
        if self.0 == 0 { 1 } else { self.0 }
    }
}
