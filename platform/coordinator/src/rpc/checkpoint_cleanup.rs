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
                let eligible = |id: &String, live: &LiveNode| {
                    let node = state.nodes.get(id)?;
                    let session = node.session.as_ref()?;
                    if !session.routable
                        || session.id != live.session
                        || !live.inspected
                        || live.expired
                        || live.report.reconciling
                        || live.last_seen.elapsed() >= self.0.heartbeat_timeout
                    {
                        return None;
                    }
                    let delegated = id != &task.assignment.node_id;
                    if task.artifacts.iter().any(|a| {
                        a.storage != "local"
                            && !a.scope.is_empty()
                            && (!node.checkpoint_scopes.contains(&a.scope)
                                || !live.report.checkpoint_scopes.contains(&a.scope))
                    }) {
                        return None;
                    }
                    if delegated
                        && !task.artifacts.iter().all(|a| {
                            a.storage != "local"
                                && !a.scope.is_empty()
                                && node.checkpoint_scopes.contains(&a.scope)
                                && live.report.checkpoint_scopes.contains(&a.scope)
                        })
                    {
                        return None;
                    }
                    Some((node.address.clone(), live.session.clone(), delegated))
                };
                let owner = state
                    .live
                    .get_key_value(&task.assignment.node_id)
                    .and_then(|(id, live)| eligible(id, live));
                let collector =
                    owner.or_else(|| state.live.iter().find_map(|(id, live)| eligible(id, live)));
                if let Some((address, node_session_id, delegated)) = collector {
                    candidates.push((task, address, node_session_id, delegated));
                }
            }
            (state.session.clone(), candidates)
        };
        let mut completed = 0;
        let mut failure = None;
        for (task, address, node_session_id, delegated) in candidates {
            let result = async {
                if !session.checkpoint_cleanups().await?.contains(&task)
                    || !session.checkpoint_cleanup_unreferenced(&task, None).await?
                {
                    return Err(Error::Conflict);
                }
                let channel = self
                    .0
                    .node_tls
                    .endpoint(&address)
                    .map_err(|_| Error::Invalid("invalid cleanup collector address".into()))?
                    .connect_timeout(self.0.timeout)
                    .timeout(self.0.timeout)
                    .connect()
                    .await
                    .map_err(|_| Error::Unavailable("checkpoint collector unavailable".into()))?;
                let ack =
                    pb::node_service_client::NodeServiceClient::new(self.0.node_tls.wrap(channel))
                        .collect_checkpoint_cleanup(pb::CollectCheckpointCleanupRequest {
                            cleanup: Some(task.clone().try_into()?),
                            node_session_id,
                            delegated,
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
