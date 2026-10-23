//! Retryable artifact deletion, independent of the retired Environment lifecycle.
use super::*;

impl CoordinatorRpc {
    /// Attempt at most sixteen durable tasks; no scheduler lock spans a node RPC.
    pub async fn collect_checkpoint_cleanups(&self) -> Result<usize> {
        let (session, candidates) = {
            let mut state = self.0.state.lock().await;
            state.recover_authoritative_state().await?;
            state.healthy()?;
            let tasks = state.session.checkpoint_cleanups().await?;
            let cursor = state.checkpoint_cleanup_cursor.clone().unwrap_or_default();
            let tasks: Vec<_> = tasks
                .iter()
                .filter(|t| t.id() > cursor)
                .chain(tasks.iter().filter(|t| t.id() <= cursor))
                .take(16)
                .cloned()
                .collect();
            state.checkpoint_cleanup_cursor = tasks.last().map(|t| t.id());
            let mut candidates = vec![];
            for task in tasks {
                if !state
                    .session
                    .checkpoint_cleanup_unreferenced(&task, None)
                    .await?
                {
                    continue;
                }
                if task
                    .artifacts
                    .iter()
                    .all(|artifact| artifact.storage != "local")
                {
                    // Shared stores are reclaimed by the Coordinator.  Their
                    // physical bytes must never be acknowledged from a node's
                    // cache-release path, and they do not depend on a source
                    // node remaining reachable.
                    candidates.push((task, None));
                    continue;
                }
                let owner = state.live.get(&task.assignment.node_id);
                let node = state.nodes.get(&task.assignment.node_id);
                if let Some((_, node, node_session)) = owner
                    .zip(node)
                    .and_then(|(live, node)| {
                        node.session.as_ref().map(|session| (live, node, session))
                    })
                    .filter(|(live, _, node_session)| {
                        node_session.routable
                            && node_session.id == live.session
                            && live.inspected
                            && !live.expired
                            && !live.report.reconciling
                            && live.last_seen.elapsed() < self.0.heartbeat_timeout
                    })
                {
                    candidates.push((task, Some((node.address.clone(), node_session.id.clone()))));
                }
            }
            (state.session.clone(), candidates)
        };
        let mut completed = 0;
        let mut failure = None;
        for (task, local_collector) in candidates {
            let result = async {
                if !session.checkpoint_cleanups().await?.contains(&task)
                    || !session.checkpoint_cleanup_unreferenced(&task, None).await?
                {
                    return Err(Error::Conflict);
                }
                if let Some((address, node_session_id)) = local_collector {
                    let local = adx_core::CheckpointCleanup {
                        assignment: task.assignment.clone(),
                        revision: task.revision,
                        artifacts: task
                            .artifacts
                            .iter()
                            .filter(|artifact| artifact.storage == "local")
                            .cloned()
                            .collect(),
                    };
                    local.validate()?;
                    let channel = self
                        .0
                        .node_tls
                        .endpoint(&address)
                        .map_err(|_| Error::Invalid("invalid cleanup collector address".into()))?
                        .connect_timeout(self.0.timeout)
                        .timeout(self.0.timeout)
                        .connect()
                        .await
                        .map_err(|_| {
                            Error::Unavailable("checkpoint collector unavailable".into())
                        })?;
                    let ack = pb::node_service_client::NodeServiceClient::new(
                        self.0.node_tls.wrap(channel),
                    )
                    .collect_checkpoint_cleanup(pb::CollectCheckpointCleanupRequest {
                        cleanup: Some(local.try_into()?),
                        node_session_id,
                        delegated: false,
                    })
                    .await
                    .map_err(adx_protocol::dependency_status)?
                    .into_inner();
                    if ack.environment_id != task.assignment.environment_id
                        || ack.generation != task.assignment.generation
                        || ack.revision != task.revision
                    {
                        return Err(Error::Conflict);
                    }
                    if !session.checkpoint_cleanups().await?.contains(&task)
                        || !session.checkpoint_cleanup_unreferenced(&task, None).await?
                    {
                        return Err(Error::Conflict);
                    }
                }
                let reclaimers = self
                    .0
                    .reclaimers
                    .read()
                    .map_err(|_| {
                        Error::Unavailable("checkpoint reclaimer registry unavailable".into())
                    })?
                    .clone();
                for artifact in task
                    .artifacts
                    .iter()
                    .filter(|artifact| artifact.storage != "local")
                {
                    let _deletion = session.claim_artifact_deletion(artifact, None).await?;
                    reclaimers.remove(artifact).await?;
                    session.forget_artifact(artifact).await?;
                }
                session.finish_checkpoint_cleanup(&task).await
            }
            .await;
            match result {
                Ok(()) => completed += 1,
                Err(error) => failure = Some(error),
            }
        }
        if let Some(error) = failure {
            return Err(error);
        }
        Ok(completed)
    }
}
