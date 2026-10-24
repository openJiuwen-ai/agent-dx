//! One durable on-demand restore path for public resume and Ingress demand.
use super::*;
use adx_core::scheduling::{LabelRequirement, LabelSelector, SelectorOp};

impl CoordinatorRpc {
    pub(super) async fn retire_dormant(
        &self,
        request: pb::GetEnvironmentRequest,
    ) -> std::result::Result<pb::EnvironmentResult, Status> {
        let mut state = self.0.state.lock().await;
        state.recover_authoritative_state().await.map_err(status)?;
        let stored = state
            .session
            .get(&request.environment_id)
            .await
            .map_err(status)?;
        tenant(request.caller.as_ref(), &stored.spec.tenant_id)?;
        state.needs_recovery = true;
        // A lost retirement reply must not requeue the removed durable record
        // as an in-memory creation that has not reached Redis yet.
        state.specs.remove(&request.environment_id);
        let deleted = state
            .session
            .retire_dormant(&stored)
            .await
            .map_err(status)?;
        if let Some(snapshot) = &stored.spec.snapshot_id {
            state
                .session
                .release_snapshot(
                    snapshot,
                    adx_core::snapshots::Reference::Restore {
                        environment_id: stored.spec.id.clone(),
                    },
                )
                .await
                .map_err(status)?;
        }
        state
            .scheduler
            .retire_claim(&request.environment_id)
            .map_err(status)?;
        state
            .scheduler
            .forget_deleted(&request.environment_id)
            .map_err(status)?;
        state.environments.remove(&request.environment_id);
        state.specs.remove(&request.environment_id);
        state.needs_recovery = false;
        self.0.changed.notify_waiters();
        Ok(pb::EnvironmentResult {
            record: Some(deleted.try_into().map_err(status)?),
            durability: pb::Durability::Published as i32,
        })
    }
    pub async fn expire_dormant(&self) -> Result<usize> {
        let mut state = self.0.state.lock().await;
        state.recover_authoritative_state().await?;
        let now = recovery::now()?;
        let candidates = state
            .environments
            .values()
            .filter(|s| {
                s.invalidated
                    && s.result.as_ref().is_some_and(|r| {
                        r.state == EnvironmentState::Paused
                            && r.checkpoint.as_ref().is_some_and(|cp| cp.expired(now))
                    })
            })
            .take(16)
            .cloned()
            .collect::<Vec<_>>();
        let mut completed = 0;
        for stored in candidates {
            state.needs_recovery = true;
            state.specs.remove(&stored.spec.id);
            match state.session.retire_dormant(&stored).await {
                Ok(_) => {
                    if let Some(snapshot) = &stored.spec.snapshot_id {
                        state
                            .session
                            .release_snapshot(
                                snapshot,
                                adx_core::snapshots::Reference::Restore {
                                    environment_id: stored.spec.id.clone(),
                                },
                            )
                            .await?;
                    }
                    state.scheduler.retire_claim(&stored.spec.id)?;
                    state.scheduler.forget_deleted(&stored.spec.id)?;
                    state.environments.remove(&stored.spec.id);
                    state.specs.remove(&stored.spec.id);
                    state.needs_recovery = false;
                    completed += 1;
                }
                Err(Error::Conflict | Error::NotFound) => (),
                Err(error) => return Err(error),
            }
        }
        Ok(completed)
    }
    pub(super) async fn ensure_running(
        &self,
        request: pb::EnsureRunningRequest,
    ) -> std::result::Result<pb::GetEnvironmentResponse, Status> {
        let seconds = if request.timeout_seconds == 0 {
            300
        } else {
            request.timeout_seconds
        };
        if seconds > 3600 {
            return Err(Status::invalid_argument(
                "resume timeout exceeds 3600 seconds",
            ));
        }
        let lock = {
            let mut locks = self
                .0
                .resume_locks
                .lock()
                .map_err(|_| Status::internal("resume registry unavailable"))?;
            locks.retain(|_, lock| lock.strong_count() > 0);
            match locks
                .get(&request.environment_id)
                .and_then(std::sync::Weak::upgrade)
            {
                Some(lock) => lock,
                None => {
                    let lock = Arc::new(Mutex::new(()));
                    locks.insert(request.environment_id.clone(), Arc::downgrade(&lock));
                    lock
                }
            }
        };
        tokio::time::timeout(Duration::from_secs(seconds), async {
            let _guard = lock.lock().await;
            {
                let mut state = self.0.state.lock().await;
                state.recover_authoritative_state().await.map_err(status)?;
                let stored = state
                    .session
                    .get(&request.environment_id)
                    .await
                    .map_err(status)?;
                tenant(request.caller.as_ref(), &stored.spec.tenant_id)?;
                if !stored.invalidated
                    && stored.resume.is_none()
                    && stored.result.as_ref().is_some_and(|r| {
                        r.state == EnvironmentState::Running
                            && !request.expected_runtime_id.is_empty()
                            && r.runtime.id != request.expected_runtime_id
                    })
                {
                    let node = state
                        .nodes
                        .get(&stored.assignment.node_id)
                        .ok_or_else(|| Status::unavailable("owner node missing"))?;
                    return Ok(pb::GetEnvironmentResponse {
                        record: Some(
                            stored
                                .result
                                .ok_or_else(|| Status::data_loss("result missing"))?
                                .try_into()
                                .map_err(status)?,
                        ),
                        node_address: node.address.clone(),
                        relay_address: node.proxy_address.clone(),
                    });
                }
                // A cancelled or ambiguous CAS must reload before cached sweeps
                // decide that no durable wake intent remains.
                state.needs_recovery = true;
                let demanded = state
                    .session
                    .request_resume(&request.environment_id, recovery::now().map_err(status)?)
                    .await
                    .map_err(status)?;
                state.remember_environment(demanded);
                state.needs_recovery = false;
            }
            self.drive_resume(&request.environment_id).await
        })
        .await
        .map_err(|_| {
            Status::deadline_exceeded("resume remains pending; retry the same Environment")
        })?
    }

    async fn drive_resume(
        &self,
        id: &str,
    ) -> std::result::Result<pb::GetEnvironmentResponse, Status> {
        loop {
            let work = {
                let mut state = self.0.state.lock().await;
                state.healthy().map_err(status)?;
                let mut stored = state.session.get(id).await.map_err(status)?;
                if stored.resume.is_some() {
                    let was_rejected = stored.resume.as_ref().is_some_and(|r| r.local_rejected);
                    state.needs_recovery = true;
                    stored = state
                        .session
                        .request_resume(id, recovery::now().map_err(status)?)
                        .await
                        .map_err(status)?;
                    if was_rejected && !stored.resume.as_ref().is_some_and(|r| r.local_rejected) {
                        state.scheduler.cancel_pending(id);
                    }
                }
                state.remember_environment(stored.clone());
                state.needs_recovery = false;
                if stored
                    .resume_point(recovery::now().map_err(status)?)
                    .is_some()
                {
                    // Only an absent execution or a definitive admission rejection
                    // permits transfer. An ambiguous RPC never enters this branch.
                    let mut spec = stored.spec.clone();
                    if spec.scheduling.required_node.is_empty() {
                        spec.scheduling.required_node.push(LabelSelector::default());
                    }
                    for selector in &mut spec.scheduling.required_node {
                        let alias = stored
                            .result
                            .as_ref()
                            .and_then(|r| r.checkpoint.as_ref().or(r.snapshot_source.as_ref()))
                            .ok_or_else(|| {
                                Status::failed_precondition("resume checkpoint missing")
                            })?
                            .artifact
                            .storage
                            .clone();
                        selector.expressions.push(LabelRequirement {
                            key: "ADX_CHECKPOINT_STORAGE".into(),
                            op: SelectorOp::In,
                            values: vec![alias],
                        });
                        selector.expressions.push(LabelRequirement {
                            key: "NODE_ID".into(),
                            op: SelectorOp::NotIn,
                            values: stored
                                .resume
                                .as_ref()
                                .map(|r| {
                                    r.rejected_nodes
                                        .iter()
                                        .cloned()
                                        .chain(std::iter::once(stored.assignment.node_id.clone()))
                                        .collect::<std::collections::BTreeSet<_>>()
                                        .into_iter()
                                        .collect()
                                })
                                .unwrap_or_else(|| vec![stored.assignment.node_id.clone()]),
                        });
                    }
                    if state
                        .scheduler
                        .requests
                        .get(id)
                        .is_none_or(|(_, queued)| queued != &spec)
                    {
                        state.scheduler.retire_claim(id).map_err(status)?;
                        state.scheduler.submit_recovery(spec).map_err(status)?;
                    }
                    for _ in 0..state.scheduler.shards.len() {
                        state.drive().await.map_err(status)?;
                    }
                    stored = state.session.get(id).await.map_err(status)?;
                    state.remember_environment(stored.clone());
                }
                let record = stored
                    .result
                    .clone()
                    .ok_or_else(|| Status::failed_precondition("execution is not resumable"))?;
                if !matches!(
                    record.state,
                    EnvironmentState::Running | EnvironmentState::Paused
                ) {
                    return Err(Status::failed_precondition("execution is not resumable"));
                }
                let node = state
                    .nodes
                    .get(&stored.assignment.node_id)
                    .cloned()
                    .ok_or_else(|| Status::unavailable("owner node missing"))?;
                if record.state == EnvironmentState::Running
                    && stored.resume.is_none()
                    && !stored.invalidated
                {
                    return Ok(pb::GetEnvironmentResponse {
                        record: Some(record.try_into().map_err(status)?),
                        node_address: node.address,
                        relay_address: node.proxy_address,
                    });
                }
                if stored.invalidated
                    && record
                        .checkpoint
                        .as_ref()
                        .is_some_and(|cp| cp.artifact.storage == "local")
                {
                    return Err(Status::failed_precondition(
                        "local restore point is unavailable on another node",
                    ));
                }
                // Existing ownership can outlive administrative admission. A
                // paused execution must not reacquire capacity on an operator-paused node.
                let available = !node.scheduling_paused
                    && state.live.get(&node.node.id).is_some_and(|live| {
                        live.inspected
                            && !live.expired
                            && !live.report.reconciling
                            && live.last_seen.elapsed() < self.0.heartbeat_timeout
                    })
                    && node.session.as_ref().is_some_and(|s| s.routable);
                if available
                    && !stored.invalidated
                    && !stored.resume.as_ref().is_some_and(|r| r.local_rejected)
                {
                    Some((stored, node, record))
                } else {
                    None
                }
            };
            let Some((stored, node, record)) = work else {
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            };
            let intent = stored.resume.clone().ok_or_else(|| {
                Status::failed_precondition("resume intent completed without a running execution")
            })?;
            let session = node
                .session
                .as_ref()
                .ok_or_else(|| Status::unavailable("node session missing"))?
                .id
                .clone();
            let endpoint = self
                .0
                .node_tls
                .endpoint(&node.address)
                .map_err(|_| Status::unavailable("invalid resume owner endpoint"))?
                .connect_timeout(self.0.timeout)
                .timeout(self.0.timeout);
            let channel = match endpoint.connect().await {
                Ok(channel) => channel,
                Err(_) => {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
            };
            let mut client =
                pb::node_service_client::NodeServiceClient::new(self.0.node_tls.wrap(channel));
            let transferred = stored.recovery.as_ref().is_some_and(|r| r.pending);
            let result = if transferred {
                client
                    .recover_environment(pb::RecoverEnvironmentRequest {
                        record: Some(record.try_into().map_err(status)?),
                        node_session_id: session.clone(),
                    })
                    .await
            } else {
                client
                    .ensure_environment_running(pb::EnsureEnvironmentRunningRequest {
                        assignment: Some(stored.assignment.clone().try_into().map_err(status)?),
                        operation_id: intent.operation_id.clone(),
                        node_session_id: session.clone(),
                    })
                    .await
            };
            match result {
                Ok(result) => {
                    let result = result.into_inner();
                    if result.durability != pb::Durability::Published as i32 {
                        return Err(Status::unavailable("resume publication pending"));
                    }
                    let restored: EnvironmentRecord = result
                        .record
                        .ok_or_else(|| Status::data_loss("resume result missing"))?
                        .try_into()
                        .map_err(status)?;
                    self.commit(restored, session).await.map_err(status)?;
                }
                Err(error) if error.code() == tonic::Code::ResourceExhausted => {
                    let mut state = self.0.state.lock().await;
                    state.needs_recovery = true;
                    let rejected = state
                        .session
                        .reject_local_resume(
                            &stored.assignment,
                            &intent.operation_id,
                            recovery::now().map_err(status)?,
                        )
                        .await
                        .map_err(status)?;
                    state.remember_environment(rejected.clone());
                    state.needs_recovery = false;
                    if rejected
                        .result
                        .as_ref()
                        .and_then(|r| r.checkpoint.as_ref().or(r.snapshot_source.as_ref()))
                        .is_some_and(|cp| cp.artifact.storage == "local")
                    {
                        return Err(error);
                    }
                }
                // An owner RPC has a shorter budget than the whole restore.
                // Keep waiting on that owner after an ambiguous transport or
                // publication result; the caller's outer deadline bounds this loop.
                Err(error)
                    if matches!(
                        error.code(),
                        tonic::Code::Unavailable
                            | tonic::Code::DeadlineExceeded
                            | tonic::Code::Cancelled
                            | tonic::Code::Unknown
                    ) =>
                {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(error) => return Err(error),
            }
        }
    }

    pub async fn resume_pending(&self) -> Result<usize> {
        let pending = {
            let mut state = self.0.state.lock().await;
            state.recover_authoritative_state().await?;
            state.healthy()?;
            let cursor = state.resume_cursor.clone().unwrap_or_default();
            let pending = state
                .environments
                .values()
                .filter(|e| e.spec.id > cursor)
                .chain(state.environments.values().filter(|e| e.spec.id <= cursor))
                .filter(|e| e.resume.is_some())
                .take(16)
                .cloned()
                .collect::<Vec<_>>();
            state.resume_cursor = pending.last().map(|e| e.spec.id.clone());
            pending
        };
        let mut count = 0;
        for stored in pending {
            let request = pb::EnsureRunningRequest {
                expected_runtime_id: String::new(),
                environment_id: stored.spec.id,
                caller: Some(pb::CallerContext {
                    tenant_id: stored.spec.tenant_id,
                    administrator: false,
                }),
                timeout_seconds: 1,
            };
            if self.ensure_running(request).await.is_ok() {
                count += 1;
            }
        }
        Ok(count)
    }
}
