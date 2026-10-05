//! Bounded terminal-failure collection; node cleanup stays in the serial lifecycle task.
use super::*;

impl CoordinatorRpc {
    /// Collect at most sixteen terminal failures, preserving pending restarts and recovery.
    /// Cleanup or persistence failures retain the record for a later collection pass.
    pub async fn collect_failed(&self, retention: Duration) -> Result<usize> {
        let now = recovery::now()?;
        let work = {
            let mut state = self.0.state.lock().await;
            state.recover_authoritative_state().await?;
            state.healthy()?;
            let cursor = state.collection_cursor.clone().unwrap_or_default();
            let ids: Vec<_> = state
                .failed_environments
                .range((
                    std::ops::Bound::Excluded(cursor.clone()),
                    std::ops::Bound::Unbounded,
                ))
                .chain(state.failed_environments.range(..=cursor))
                .take(16)
                .cloned()
                .collect();
            state.collection_cursor = ids.last().cloned();
            ids
        };
        let mut completed = 0;
        let mut error = None;
        for id in work {
            match self.collect_one_failed(&id, now, retention.as_secs()).await {
                Ok(true) => completed += 1,
                Ok(false) | Err(Error::Conflict | Error::NotFound) => {}
                Err(failure) => error = Some(failure),
            }
        }
        if let Some(error) = error {
            return Err(error);
        }
        Ok(completed)
    }

    async fn collect_one_failed(&self, id: &str, now: u64, retention: u64) -> Result<bool> {
        let (stored, node, session_id) = {
            let mut state = self.0.state.lock().await;
            state.healthy()?;
            let stored = state.session.observe_failed(id, now).await?;
            state.remember_environment(stored.clone());
            if !stored.failed_collectable(now, retention) {
                return Ok(false);
            }
            let live = state.live.get(&stored.assignment.node_id).filter(|live| {
                !live.expired
                    && live.inspected
                    && !live.report.reconciling
                    && live.last_seen.elapsed() < self.0.heartbeat_timeout
            });
            if live.is_none() {
                if !stored.invalidated || stored.resources_held() {
                    return Ok(false);
                }
                // Any failure after the durable retirement must rebuild the in-memory
                // ledger before serving another placement or collection request.
                state.needs_recovery = true;
                // This is persisted ownership, not a pending scheduling request.
                // If retirement loses its response, do not requeue the old spec as
                // an absent, never-persisted request during authoritative recovery.
                state.specs.remove(id);
                let deleted = state
                    .session
                    .retire_invalidated_failed(&stored, now)
                    .await?;
                if let Some(snapshot_id) = &deleted.spec.snapshot_id {
                    state
                        .session
                        .release_snapshot(
                            snapshot_id,
                            adx_core::snapshots::Reference::Restore {
                                environment_id: id.into(),
                            },
                        )
                        .await?;
                }
                state.scheduler.forget_deleted(id)?;
                state.environments.remove(id);
                state.failed_environments.remove(id);
                state.specs.remove(id);
                state.scheduling_deadlines.remove(id);
                state.needs_recovery = false;
                self.0.changed.notify_waiters();
                return Ok(true);
            }
            let session_id = live.ok_or(Error::Conflict)?.session.clone();
            let node = state
                .nodes
                .get(&stored.assignment.node_id)
                .ok_or(Error::Conflict)?
                .clone();
            (stored, node, session_id)
        };
        let record = stored.result.ok_or(Error::Conflict)?;
        let channel = self
            .0
            .node_tls
            .endpoint(&node.address)
            .map_err(|_| Error::Invalid("invalid node address".into()))?
            .connect_timeout(self.0.timeout)
            .timeout(self.0.timeout)
            .connect()
            .await
            .map_err(|_| Error::Unavailable("failed collector node unavailable".into()))?;
        let response =
            pb::node_service_client::NodeServiceClient::new(self.0.node_tls.wrap(channel))
                .delete_environment(pb::DeleteEnvironmentRequest {
                    assignment: Some(record.assignment.clone().try_into()?),
                    caller: None,
                    failed_revision: Some(record.revision),
                    node_session_id: session_id.clone(),
                })
                .await
                .map_err(adx_protocol::dependency_status)?
                .into_inner();
        if response.durability != pb::Durability::Published as i32 {
            return Err(Error::Unavailable(
                "failed deletion publication pending".into(),
            ));
        }
        let deleted: EnvironmentRecord = response.record.ok_or(Error::Conflict)?.try_into()?;
        if deleted.state != EnvironmentState::Deleted
            || deleted.resources_held
            || deleted.assignment != record.assignment
        {
            return Err(Error::Conflict);
        }
        self.commit(deleted, session_id).await?;
        Ok(true)
    }
}
