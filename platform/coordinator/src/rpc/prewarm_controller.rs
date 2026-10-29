//! Bounded durable reconciliation for immutable artifact replicas. All network
//! operations run outside the scheduler lock; uncertain objects retain capacity.
use super::*;
use adx_core::prewarm::{management::*, *};

impl CoordinatorRpc {
    pub(super) async fn prewarm_nodes(&self) -> std::result::Result<Vec<StoredNode>, Status> {
        let state = self.0.state.lock().await;
        state.healthy().map_err(status)?;
        Ok(state
            .nodes
            .values()
            .filter(|node| {
                state.live.get(&node.node.id).is_some_and(|live| {
                    !live.expired
                        && live.inspected
                        && live.last_seen.elapsed() < self.0.heartbeat_timeout
                        && node.session.as_ref().is_some_and(|s| s.id == live.session)
                })
            })
            .cloned()
            .collect())
    }
    pub(super) async fn artifact_rpc(
        &self,
        node: &StoredNode,
        command: &ArtifactCommand,
    ) -> Result<ArtifactObservation> {
        let session = node.session.as_ref().ok_or(Error::Conflict)?;
        let endpoint = self
            .0
            .node_tls
            .endpoint(&node.address)
            .map_err(|_| Error::Invalid("invalid preparation node address".into()))?
            .connect_timeout(self.0.timeout)
            .timeout(self.0.timeout);
        let channel = endpoint
            .connect()
            .await
            .map_err(|_| Error::Unavailable("preparation node connection unavailable".into()))?;
        let result = pb::preparation_node_service_client::PreparationNodeServiceClient::new(
            self.0.node_tls.wrap(channel),
        )
        .artifact(adx_observability::trace::inject(pb::ArtifactRequest {
            node_session_id: session.id.clone(),
            command_json: serde_json::to_vec(command)
                .map_err(|_| Error::Invalid("invalid artifact command".into()))?,
        }))
        .await
        .map_err(|e| {
            if e.code() == tonic::Code::Unimplemented {
                unsupported()
            } else {
                Error::Unavailable("artifact side effect unconfirmed".into())
            }
        })?
        .into_inner();
        serde_json::from_slice(&result.observation_json)
            .map_err(|_| Error::Unavailable("invalid artifact observation".into()))
    }

    /// One bounded sweep, safe to repeat after cancellation or process restart.
    /// Slots are never accepted here; production drivers only qualify EROFS artifacts.
    pub async fn reconcile_prewarm(&self) -> Result<usize> {
        let session = self
            .prewarm_session()
            .await
            .map_err(|_| Error::Unavailable("prewarm authority unavailable".into()))?;
        let nodes = self
            .prewarm_nodes()
            .await
            .map_err(|_| Error::Unavailable("prewarm node inventory unavailable".into()))?;
        let mut completed = 0;
        for pool in session.retained_prewarm_pools().await? {
            let mut pool = pool;
            let now = super::recovery::now()?;
            let old = pool.clone();
            let mut keep = 0u32;
            for artifact in &mut pool.artifacts {
                if artifact.state == SlotState::Ready {
                    if keep >= pool.policy.capacity.target_ready {
                        artifact.state = SlotState::Retiring;
                    } else {
                        keep += 1;
                    }
                }
            }
            if pool != old && !session.update_pool_inventory(&old, &pool).await? {
                continue;
            }
            for index in 0..pool.artifacts.len() {
                let old = pool.clone();
                let artifact = &pool.artifacts[index];
                if artifact.state == SlotState::Deleted || now < artifact.retry_at {
                    continue;
                }
                let node = nodes.iter().find(|node| node.node.id == artifact.node_id);
                let current_session = node.and_then(|n| n.session.as_ref()).map(|s| s.id.as_str());
                let retiring = matches!(pool.phase, PoolPhase::Draining | PoolPhase::Deleting)
                    || artifact.state == SlotState::Quarantined
                    || (artifact.expires_at > 0 && now >= artifact.expires_at)
                    || current_session != Some(artifact.node_session.as_str());
                if retiring && artifact.state != SlotState::Retiring {
                    pool.artifacts[index].state = SlotState::Retiring;
                    if !session.update_pool_inventory(&old, &pool).await? {
                        break;
                    }
                }
                let Some(node) = node else {
                    continue;
                };
                let artifact = &pool.artifacts[index];
                let command = match artifact.state {
                    SlotState::Retiring => ArtifactCommand::Destroy {
                        id: artifact.id.clone(),
                    },
                    SlotState::Preparing => ArtifactCommand::Prepare {
                        id: artifact.id.clone(),
                        profile: Box::new(pool.profile.clone()),
                        max_bytes: artifact.size_bytes,
                        expected_recipe_digest: pool.artifact_recipe_digest.clone(),
                    },
                    SlotState::Ready
                        if now.saturating_sub(artifact.checked_at)
                            >= pool.policy.health.interval_seconds =>
                    {
                        ArtifactCommand::Observe {
                            id: artifact.id.clone(),
                        }
                    }
                    _ => continue,
                };
                let before = pool.clone();
                let preparation_started = std::time::Instant::now();
                let result = self.artifact_rpc(node, &command).await;
                if matches!(command, ArtifactCommand::Prepare { .. }) {
                    self.0.prewarm_metrics.prepare(
                        result.as_ref().is_ok_and(|o| o.ready),
                        preparation_started.elapsed(),
                    );
                }
                let finished_at = super::recovery::now()?;
                let artifact = &mut pool.artifacts[index];
                match result {
                    Ok(observation) if matches!(command, ArtifactCommand::Destroy { .. }) => {
                        if observation.in_use {
                            continue;
                        }
                        artifact.state = SlotState::Deleted;
                        artifact.size_bytes = 0;
                        completed += 1;
                    }
                    Ok(observation)
                        if observation.ready
                            && observation.size_bytes <= artifact.size_bytes
                            && observation.recipe_digest == pool.artifact_recipe_digest =>
                    {
                        pool.refill_failures = 0;
                        pool.refill_retry_at = 0;
                        artifact.state = SlotState::Ready;
                        artifact.checked_at = finished_at;
                        artifact.failures = 0;
                        if artifact.expires_at == 0 {
                            artifact.expires_at = finished_at
                                .checked_add(pool.policy.ready_ttl_seconds)
                                .ok_or(Error::Conflict)?;
                        }
                    }
                    _ => {
                        if artifact.state != SlotState::Retiring {
                            artifact.state = SlotState::Quarantined;
                        }
                        artifact.failures = artifact.failures.saturating_add(1);
                        let factor = 1u64
                            .checked_shl(artifact.failures.min(20))
                            .unwrap_or(u64::MAX);
                        let backoff = pool
                            .policy
                            .refill
                            .initial_backoff_seconds
                            .saturating_mul(factor)
                            .min(pool.policy.refill.max_backoff_seconds);
                        artifact.retry_at = finished_at.saturating_add(backoff);
                        if matches!(command, ArtifactCommand::Prepare { .. }) {
                            pool.refill_failures = pool.refill_failures.saturating_add(1);
                            pool.refill_retry_at = artifact.retry_at;
                        }
                    }
                }
                if !session.update_pool_inventory(&before, &pool).await? {
                    break;
                }
            }
            // Re-read policy after physical calls: no stale policy may replenish a drained pool.
            let fresh = session.get_pool(&pool.tenant, &pool.pool_id).await;
            let mut pool = match fresh {
                Ok(pool) => pool,
                Err(PoolError::NotFound) => continue,
                Err(PoolError::Control(e)) => return Err(e),
                Err(_) => return Err(Error::Conflict),
            };
            let old = pool.clone();
            pool.observed_revision = pool.revision;
            pool.observed_at = now;
            if !session.update_pool_inventory(&old, &pool).await? {
                continue;
            }
            if pool.phase == PoolPhase::Deleting
                && !pool
                    .artifacts
                    .iter()
                    .any(PreparationArtifact::counts_as_live)
            {
                let old = pool.clone();
                pool.phase = PoolPhase::Deleted;
                session.update_pool_inventory(&old, &pool).await?;
                continue;
            }
            if pool.phase != PoolPhase::Active || !pool.policy.enabled || now < pool.refill_retry_at
            {
                continue;
            }
            for node in &nodes {
                let live = pool.artifacts.iter().filter(|a| a.counts_as_live()).count();
                let pending = pool
                    .artifacts
                    .iter()
                    .filter(|a| a.state == SlotState::Preparing)
                    .count();
                let ready = pool
                    .artifacts
                    .iter()
                    .filter(|a| a.claimable(now, &pool.policy))
                    .count();
                if ready + pending >= pool.policy.capacity.target_ready as usize
                    || live >= pool.policy.capacity.max_live as usize
                    || pending >= pool.policy.refill.prepare_concurrency as usize
                {
                    break;
                }
                if !node.node.available
                    || pool
                        .artifacts
                        .iter()
                        .any(|a| a.node_id == node.node.id && a.counts_as_live())
                {
                    continue;
                }
                let observation = match self
                    .artifact_rpc(
                        node,
                        &ArtifactCommand::Qualify {
                            profile: Box::new(pool.profile.clone()),
                        },
                    )
                    .await
                {
                    Ok(value) => value,
                    Err(_) => continue,
                };
                if observation.recipe_digest != pool.artifact_recipe_digest {
                    continue;
                }
                let used = pool
                    .artifacts
                    .iter()
                    .filter(|a| a.counts_as_live())
                    .try_fold(0u64, |n, a| {
                        n.checked_add(a.size_bytes).ok_or(Error::NoCapacity)
                    })?;
                if used
                    .checked_add(observation.size_bytes)
                    .is_none_or(|n| n > pool.policy.idle_budget.disk_bytes)
                {
                    continue;
                }
                let old = pool.clone();
                pool.artifacts.retain(|a| a.state != SlotState::Deleted);
                pool.artifacts.push(PreparationArtifact {
                    id: format!("artifact-{}", uuid::Uuid::new_v4()),
                    node_id: node.node.id.clone(),
                    node_session: node.session.as_ref().ok_or(Error::Conflict)?.id.clone(),
                    profile_digest: pool.profile_digest.clone(),
                    state: SlotState::Preparing,
                    size_bytes: observation.size_bytes,
                    created_at: now,
                    checked_at: 0,
                    expires_at: 0,
                    retry_at: now,
                    failures: 0,
                });
                if !session.update_pool_inventory(&old, &pool).await? {
                    break;
                }
            }
        }
        self.0
            .prewarm_metrics
            .observe(&session.retained_prewarm_pools().await?)?;
        Ok(completed)
    }
}
