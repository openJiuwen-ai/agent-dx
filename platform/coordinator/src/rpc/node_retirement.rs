//! Offline registration GC and authenticated planned departure.
use super::*;

impl State {
    async fn retire_empty_node(&mut self, id: &str, now: u64, retention: u64) -> Result<bool> {
        if self.session.process_node_protected(id).await? {
            return Ok(false);
        }
        let Some(node) = self.nodes.get(id) else {
            return Ok(false);
        };
        let Some(session) = node.session.as_ref() else {
            return Ok(false);
        };
        if self
            .environments
            .values()
            .any(|e| e.assignment.node_id == id)
            || self
                .scheduler
                .snapshot()
                .environments()
                .values()
                .any(|e| e.node_id == id)
        {
            return Ok(false);
        }
        let session_id = session.id.clone();
        self.needs_recovery = true;
        let retired = self
            .session
            .retire_expired_node(id, &session_id, now, retention)
            .await?;
        if retired {
            self.scheduler.forget_node(id)?;
            self.nodes.remove(id);
            self.live.remove(id);
            self.recovering.remove(id);
        }
        self.needs_recovery = false;
        Ok(retired)
    }
}
impl CoordinatorRpc {
    /// Bound durable retirement to sixteen empty offline nodes per maintenance pass.
    pub async fn collect_retired_nodes(&self, retention: Duration) -> Result<usize> {
        if retention.is_zero() {
            return Err(Error::Invalid("node retention must be positive".into()));
        }
        let now = super::recovery::now()?;
        let mut state = self.0.state.lock().await;
        state.recover_authoritative_state().await?;
        state.healthy()?;
        let eligible: Vec<_> = state
            .nodes
            .iter()
            .filter(|(id, node)| {
                node.retirement.as_ref().is_some_and(|r| {
                    r.unregistered
                        || now.saturating_sub(r.since_unix_seconds) >= retention.as_secs()
                }) && state.live.get(*id).is_none_or(|live| live.expired)
            })
            .map(|(id, _)| id.clone())
            .collect();
        if eligible.is_empty() {
            return Ok(0);
        }
        // Scan Environment references once, rather than once per offline node.
        let referenced: BTreeSet<_> = state
            .environments
            .values()
            .map(|e| e.assignment.node_id.as_str())
            .collect();
        let candidates: Vec<_> = eligible
            .into_iter()
            .filter(|id| !referenced.contains(id.as_str()))
            .take(16)
            .collect();
        let mut completed = 0;
        for id in candidates {
            completed += usize::from(
                state
                    .retire_empty_node(&id, now, retention.as_secs())
                    .await?,
            );
        }
        if completed > 0 {
            self.0.changed.notify_waiters();
        }
        Ok(completed)
    }

    pub(super) async fn unregister_node_request(
        &self,
        request: Request<pb::UnregisterNodeRequest>,
    ) -> std::result::Result<Response<pb::UnregisterNodeResponse>, Status> {
        let principal = self.0.peers.authenticate(&request)?;
        let r = request.into_inner();
        if r.node_id.is_empty() || r.session_id.is_empty() {
            return Err(Status::invalid_argument("node and session required"));
        }
        if principal != Principal::Node(r.node_id.clone()) {
            return Err(Status::permission_denied(
                "node identity does not match certificate",
            ));
        }
        let mut state = self.0.state.lock().await;
        state.recover_authoritative_state().await.map_err(status)?;
        state.healthy().map_err(status)?;
        if state
            .session
            .process_node_protected(&r.node_id)
            .await
            .map_err(status)?
        {
            return Err(Status::failed_precondition(
                "process maintenance protects node departure",
            ));
        }
        if !state.nodes.contains_key(&r.node_id) {
            if state
                .session
                .is_unregistered_node(&r.node_id, &r.session_id)
                .await
                .map_err(status)?
            {
                return Ok(Response::new(pb::UnregisterNodeResponse {}));
            }
            return Err(Status::failed_precondition(
                "node session no longer registered",
            ));
        }
        if state.nodes[&r.node_id]
            .session
            .as_ref()
            .is_none_or(|s| s.id != r.session_id)
        {
            return Err(Status::failed_precondition("node session replaced"));
        }
        state
            .invalidate_node_with_departure(&r.node_id, true)
            .await
            .map_err(status)?;
        state
            .retired_sessions
            .insert((r.node_id.clone(), r.session_id));
        self.0.changed.notify_waiters();
        // Existing Environment references retain their recovery/cleanup context;
        // an already drained node can leave the directory immediately.
        state
            .retire_empty_node(&r.node_id, super::recovery::now().map_err(status)?, 0)
            .await
            .map_err(status)?;
        Ok(Response::new(pb::UnregisterNodeResponse {}))
    }
}
