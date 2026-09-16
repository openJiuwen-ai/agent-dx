//! Recover invalidated executions through normal Domain placement and a durable
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
impl MasterRpc {
    pub async fn recover_instances(&self) -> Result<usize> {
        let work = {
            let mut state = self.0.state.lock().await;
            state.healthy()?;
            let now = now()?;
            let candidates: Vec<_> = state
                .instances
                .values()
                .filter(|i| i.recovery_point(now).is_some())
                .cloned()
                .collect();
            for stored in candidates {
                let mut spec = stored.spec;
                if spec.scheduling.required_node.is_empty() {
                    spec.scheduling.required_node.push(LabelSelector::default());
                }
                for alternative in &mut spec.scheduling.required_node {
                    alternative.expressions.push(LabelRequirement {
                        key: "NODE_ID".into(),
                        op: SelectorOp::NotIn,
                        values: vec![stored.assignment.node_id.clone()],
                    });
                }
                state.scheduler.submit_recovery(spec)?;
            }
            // Each Domain round already has its own attempt/time budget.
            for _ in 0..state.scheduler.domains.len() {
                if state.drive().await? {
                    self.0.changed.notify_waiters();
                }
            }
            let cursor = state.recovery_cursor.clone().unwrap_or_default();
            let work: Vec<_> = state
                .instances
                .values()
                .filter(|i| i.spec.id > cursor)
                .chain(state.instances.values().filter(|i| i.spec.id <= cursor))
                .filter(|i| !i.invalidated && i.recovery.as_ref().is_some_and(|r| r.pending))
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
                    Some((node.clone(), i.result.clone()?))
                })
                .take(16)
                .collect();
            state.recovery_cursor = work.last().map(|(_, record)| record.spec.id.clone());
            work
        };
        let results = stream::iter(work.into_iter().map(|(node, record)| async move {
            let endpoint = Endpoint::from_shared(format!("https://{}", node.address))
                .map_err(|_| Error::Conflict)?
                .tls_config(self.0.node_tls.clone())
                .map_err(|_| Error::Conflict)?
                .connect_timeout(self.0.timeout)
                .timeout(self.0.timeout);
            let channel = endpoint
                .connect()
                .await
                .map_err(|_| Error::Unavailable("recovery node unavailable".into()))?;
            let session = node.session.ok_or(Error::Conflict)?.id;
            let response = pb::node_service_client::NodeServiceClient::new(channel)
                .recover_instance(pb::RecoverInstanceRequest {
                    record: Some(record.try_into()?),
                    node_session_id: session.clone(),
                })
                .await
                .map_err(adx_protocol::dependency_status)?
                .into_inner();
            if response.durability != pb::Durability::Published as i32 {
                return Err(Error::Unavailable(
                    "recovery result publication pending".into(),
                ));
            }
            let record = response.record.ok_or(Error::Conflict)?.try_into()?;
            self.commit(record, session).await?;
            Ok::<_, Error>(())
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
}
