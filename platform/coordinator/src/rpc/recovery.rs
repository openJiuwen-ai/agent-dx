//! Recover invalidated executions through normal ShardScheduler placement and a durable
//! Paused record at a new generation. No image-start fallback is permitted.
use super::*;
use adx_core::scheduling::{LabelRequirement, LabelSelector, SelectorOp};
use futures_util::{stream, StreamExt};

pub(super) fn now() -> Result<u64> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .map_err(|_| Error::Unavailable("system clock before epoch".into()))
}
impl CoordinatorRpc {
    pub async fn recover_environments(&self) -> Result<usize> {
        let work = {
            let mut state = self.0.state.lock().await;
            state.healthy()?;
            let now = now()?;
            let upgrade = state.session.upgrade_task().await?;
            let upgrade_owned = |id: &str| {
                upgrade.as_ref().is_some_and(|task| {
                    task.active()
                        && task
                            .operations
                            .values()
                            .any(|op| op.environment_id == id && !op.settled())
                })
            };
            let candidates: Vec<_> = state
                .environments
                .values()
                .filter(|i| !upgrade_owned(&i.spec.id) && i.recovery_point(now).is_some())
                .cloned()
                .collect();
            for stored in candidates {
                let domains = self.volume_domains(&state)?;
                state.prepare_volumes(&stored.spec, &domains).await?;
                let alias = stored
                    .recovery_point(now)
                    .ok_or(Error::Conflict)?
                    .artifact
                    .storage
                    .clone();
                let mut spec = stored.spec;

                if spec.scheduling.required_node.is_empty() {
                    spec.scheduling.required_node.push(LabelSelector::default());
                }
                for alternative in &mut spec.scheduling.required_node {
                    alternative.expressions.push(LabelRequirement {
                        key: "ADX_CHECKPOINT_STORAGE".into(),
                        op: SelectorOp::In,
                        values: vec![alias.clone()],
                    });
                    alternative.expressions.push(LabelRequirement {
                        key: "NODE_ID".into(),
                        op: SelectorOp::NotIn,
                        values: vec![stored.assignment.node_id.clone()],
                    });
                }
                state.scheduler.submit_recovery(spec)?;
            }
            // Each ShardScheduler round already has its own attempt/time budget.
            for _ in 0..state.scheduler.shards.len() {
                if state.drive().await? {
                    self.0.changed.notify_waiters();
                }
            }
            let cursor = state.recovery_cursor.clone().unwrap_or_default();
            let work: Vec<_> = state
                .environments
                .values()
                .filter(|i| i.spec.id > cursor)
                .chain(state.environments.values().filter(|i| i.spec.id <= cursor))
                .filter(|i| {
                    !upgrade_owned(&i.spec.id)
                        && !i.invalidated
                        && i.recovery.as_ref().is_some_and(|r| r.pending)
                })
                .filter_map(|i| {
                    let node = state.nodes.get(&i.assignment.node_id)?;
                    let live = state.live.get(&i.assignment.node_id)?;
                    if !live.inspected
                        || live.report.reconciling
                        || live.expired
                        || live.last_seen.elapsed() >= self.0.heartbeat_timeout
                    {
                        return None;
                    }
                    Some((
                        node.clone(),
                        i.result.clone()?,
                        i.resume
                            .as_ref()
                            .and_then(|intent| intent.run_timeout_seconds),
                    ))
                })
                .take(16)
                .collect();
            state.recovery_cursor = work.last().map(|(_, record, _)| record.spec.id.clone());
            work
        };
        let results = stream::iter(work.into_iter().map(|(node, record, timeout)| {
            self.recover_record(node, record, timeout, self.0.timeout, false)
        }))
        .buffer_unordered(4)
        .collect::<Vec<_>>()
        .await;
        let completed = results.iter().filter(|r| r.is_ok()).count();
        if let Some(error) = results.into_iter().find_map(Result::err) {
            return Err(error);
        }
        Ok(completed)
    }
    /// Both planned and failure recovery restore the same committed checkpoint;
    /// a planned transfer additionally requires a published Running acknowledgement.
    pub(super) fn recover_record(
        &self,
        node: StoredNode,
        record: EnvironmentRecord,
        run_timeout_seconds: Option<u64>,
        timeout: Duration,
        require_running: bool,
    ) -> RpcFuture<'_, EnvironmentRecord> {
        Box::pin(async move {
            let assignment = record.assignment.clone();
            let channel = self
                .0
                .node_tls
                .endpoint(&node.address)
                .map_err(|_| Error::Conflict)?
                .connect_timeout(self.0.timeout)
                .timeout(timeout)
                .connect()
                .await
                .map_err(|_| Error::Unavailable("recovery node unavailable".into()))?;
            let session = node.session.ok_or(Error::Conflict)?.id;
            let result =
                pb::node_service_client::NodeServiceClient::new(self.0.node_tls.wrap(channel))
                    .recover_environment(pb::RecoverEnvironmentRequest {
                        run_timeout_seconds,
                        record: Some(record.try_into()?),
                        node_session_id: session.clone(),
                    })
                    .await
                    .map_err(adx_protocol::dependency_status)?
                    .into_inner();
            if result.durability != pb::Durability::Published as i32 {
                return Err(Error::Unavailable(
                    "recovery result publication pending".into(),
                ));
            }
            let record: EnvironmentRecord = result.record.ok_or(Error::Conflict)?.try_into()?;
            if record.assignment != assignment
                || (require_running
                    && (record.state != EnvironmentState::Running
                        || !record.resources_held
                        || record.runtime.ip.is_none()))
            {
                return Err(Error::Unavailable(
                    "target restore is not published Running; retain source checkpoint".into(),
                ));
            }
            self.commit(record, session).await
        })
    }
}
