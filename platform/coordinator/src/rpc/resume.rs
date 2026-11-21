//! One durable on-demand restore path for public resume and Ingress demand.
use super::*;
use crate::storage::ResumeAdmission;

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
            newly_created: false,
            record: Some(deleted.try_into().map_err(status)?),
            durability: pb::Durability::Published as i32,
            ..Default::default()
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
        // Keep the restore payload off the caller's poll frame: background
        // recovery also drives this path on the default Tokio test/thread stack.
        tokio::time::timeout(
            Duration::from_secs(seconds),
            Box::pin(async {
                let _guard = lock.lock().await;
                loop {
                    let waiting_for_upgrade = {
                        let mut state = self.0.state.lock().await;
                        state.recover_authoritative_state().await.map_err(status)?;
                        let stored = state
                            .session
                            .get(&request.environment_id)
                            .await
                            .map_err(status)?;
                        tenant(request.caller.as_ref(), &stored.spec.tenant_id)?;
                        // A cancelled/ambiguous CAS must reload. A known read-only
                        // upgrade fence must not poison the scheduler health gate.
                        state.needs_recovery = true;
                        let admission = state
                            .session
                            .admit_resume(
                                &request.environment_id,
                                recovery::now().map_err(status)?,
                                &request.expected_runtime_id,
                                request.run_timeout_seconds,
                            )
                            .await
                            .map_err(status)?;
                        state.needs_recovery = false;
                        match admission {
                            ResumeAdmission::Upgrading => true,
                            ResumeAdmission::Running(stored) => {
                                let node = state
                                    .nodes
                                    .get(&stored.assignment.node_id)
                                    .ok_or_else(|| Status::unavailable("owner node missing"))?;
                                return Ok(pb::GetEnvironmentResponse {
                                    record: Some((&stored).try_into().map_err(status)?),
                                    node_address: node.address.clone(),
                                    relay_address: node.proxy_address.clone(),
                                });
                            }
                            ResumeAdmission::Demanded(stored) => {
                                state.remember_environment(stored);
                                false
                            }
                        }
                    };
                    if !waiting_for_upgrade {
                        break;
                    }
                    // Never hold the state mutex while migration/rollback needs it.
                    // Polling also covers durable task updates without a notification;
                    // the single outer deadline bounds every wait and lock acquisition.
                    tokio::select! {
                        _ = self.0.changed.notified() => {},
                        _ = tokio::time::sleep(Duration::from_millis(50)) => {},
                    }
                }
                self.drive_resume(&request.environment_id).await
            }),
        )
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
            let changed = self.0.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let mut waiting_for_volume = false;
            let work = {
                let mut state = self.0.state.lock().await;
                state.healthy().map_err(status)?;
                let coordinator_epoch = state.session.epoch();
                let mut stored = state.session.get(id).await.map_err(status)?;
                if stored.resume.is_some() {
                    let was_rejected = stored.resume.as_ref().is_some_and(|r| r.local_rejected);
                    state.needs_recovery = true;
                    let admission = state
                        .session
                        .admit_resume(id, recovery::now().map_err(status)?, "", None)
                        .await
                        .map_err(status)?;
                    state.needs_recovery = false;
                    stored = match admission {
                        ResumeAdmission::Demanded(stored) | ResumeAdmission::Running(stored) => {
                            stored
                        }
                        ResumeAdmission::Upgrading => {
                            drop(state);
                            tokio::time::sleep(Duration::from_millis(50)).await;
                            continue;
                        }
                    };
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
                    let domains = self.volume_domains(&state).map_err(status)?;
                    state
                        .prepare_volumes(&stored.spec, &domains)
                        .await
                        .map_err(status)?;
                    let point = stored
                        .result
                        .as_ref()
                        .and_then(|record| {
                            record
                                .checkpoint
                                .as_ref()
                                .or(record.snapshot_source.as_ref())
                        })
                        .ok_or_else(|| Status::failed_precondition("resume checkpoint missing"))?;
                    let point = state.session.ordinary_recovery_point(&stored.spec, point);
                    let excluded = stored
                        .resume
                        .as_ref()
                        .map(|intent| intent.rejected_nodes.clone())
                        .unwrap_or_default();
                    let spec =
                        recovery::checkpoint_spec(&stored, Some(&point), None, &excluded, None)
                            .map_err(status)?;
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
                if record.state == EnvironmentState::Paused && !stored.resources_held() {
                    let domains = self.volume_domains(&state).map_err(status)?;
                    state
                        .prepare_volumes(&stored.spec, &domains)
                        .await
                        .map_err(status)?;
                    waiting_for_volume = state
                        .scheduler
                        .volume_placement
                        .get(id)
                        .is_some_and(|placement| placement.waiting_for.is_some());
                }
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
                        record: Some((&stored).try_into().map_err(status)?),
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
                    && !waiting_for_volume
                {
                    Some((stored, node, record, coordinator_epoch))
                } else {
                    None
                }
            };
            let Some((stored, node, record, coordinator_epoch)) = work else {
                if waiting_for_volume {
                    changed.await;
                } else {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
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
                        run_timeout_seconds: intent.run_timeout_seconds,
                        record: Some(record.try_into().map_err(status)?),
                        node_session_id: session.clone(),
                        coordinator_epoch,
                    })
                    .await
            } else {
                client
                    .ensure_environment_running(pb::EnsureEnvironmentRunningRequest {
                        run_timeout_seconds: intent.run_timeout_seconds,
                        assignment: Some(stored.assignment.clone().try_into().map_err(status)?),
                        operation_id: intent.operation_id.clone(),
                        node_session_id: session.clone(),
                        coordinator_epoch,
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
                Err(error)
                    if matches!(
                        adx_protocol::dependency_status(error.clone()),
                        Error::VolumeBusy(_)
                    ) =>
                {
                    // A competing claimant won after the preflight. Keep the
                    // existing owner and recheck the same Volume wait condition.
                    continue;
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
                run_timeout_seconds: None,
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
