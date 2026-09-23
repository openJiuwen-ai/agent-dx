//! Local-first ownership coordination; the same mutex guards center scheduling.
use super::*;
use crate::storage::{ClaimOutcome, LocalClaim};
use adx_core::snapshots::Reference;
use pb::claim_environment_response::Outcome;

impl State {
    pub(super) async fn recover_claim_write(&mut self) -> Result<()> {
        if !self.claim_recovery {
            return Ok(());
        }
        // The failed writer has returned. Advancing the header prevents its last
        // in-flight CAS from materializing after the authoritative snapshot.
        self.session.claim_barrier().await?;
        let saved = self.session.snapshot().await?;
        let pending: Vec<_> = self
            .specs
            .values()
            .filter(|s| !saved.environments.contains_key(&s.id))
            .cloned()
            .collect();
        let mut scheduler = Coordinator::restore(&saved, self.placement)?;
        for node in saved.nodes.values() {
            let mut candidate = node.node.clone();
            candidate.available &= self.live.get(&candidate.id).is_some_and(|live| {
                !live.expired
                    && live.inspected
                    && node
                        .session
                        .as_ref()
                        .is_some_and(|s| s.id == live.session && s.routable)
            });
            scheduler.register(candidate)?;
        }
        for spec in pending {
            scheduler.submit(spec)?;
        }
        self.scheduler = scheduler;
        self.nodes = saved.nodes;
        self.environments = saved.environments;
        for (id, record) in &self.environments {
            self.specs.insert(id.clone(), record.spec.clone());
        }
        self.claim_recovery = false;
        self.needs_recovery = false;
        Ok(())
    }
    pub(super) fn accept_stored_claim(&mut self, record: StoredEnvironment) -> Result<()> {
        let synchronized = if record.resources_held() && !record.invalidated {
            self.scheduler
                .accept_claim(&record.spec, &record.assignment)
        } else {
            self.scheduler.retire_claim(&record.spec.id)
        };
        if let Err(error) = synchronized {
            self.needs_recovery = true;
            self.claim_recovery = true;
            return Err(error);
        }
        self.scheduler.generation = self.scheduler.generation.max(record.assignment.generation);
        self.specs
            .insert(record.spec.id.clone(), record.spec.clone());
        self.environments.insert(record.spec.id.clone(), record);
        Ok(())
    }
    pub(super) fn live_claimant(&self, id: &str, session: &str, timeout: Duration) -> Result<()> {
        if session.is_empty()
            || self.live.get(id).is_none_or(|live| {
                live.expired
                    || !live.inspected
                    || live.session != session
                    || live.last_seen.elapsed() >= timeout
            })
            || self
                .nodes
                .get(id)
                .and_then(|n| n.session.as_ref())
                .is_none_or(|s| s.id != session || !s.routable)
        {
            return Err(Error::Conflict);
        }
        Ok(())
    }
    pub(super) fn environment_response(
        &self,
        stored: &StoredEnvironment,
    ) -> Result<pb::GetEnvironmentResponse> {
        let node = self
            .nodes
            .get(&stored.assignment.node_id)
            .ok_or(Error::NotFound)?;
        let record = stored.result.clone().unwrap_or_else(|| EnvironmentRecord {
            spec: stored.spec.clone(),
            assignment: stored.assignment.clone(),
            runtime: adx_core::Runtime {
                id: format!("{}-{}", stored.spec.id, stored.assignment.generation),
                ip: None,
            },
            state: EnvironmentState::Pending,
            revision: 0,
            resources_held: true,
            checkpoint: None,
            last_operation: None,
            restart_attempts: 0,
            restart_pending: false,
        });
        Ok(pb::GetEnvironmentResponse {
            record: Some(record.try_into()?),
            node_address: node.address.clone(),
            relay_address: node.proxy_address.clone(),
        })
    }
}
impl CoordinatorRpc {
    pub(super) async fn prepare_local(
        &self,
        node: &str,
        request: pb::LocalEnvironmentCreateRequest,
    ) -> std::result::Result<pb::PreparedEnvironment, Status> {
        let create = request
            .create
            .ok_or_else(|| Status::invalid_argument("create required"))?;
        let raw = create
            .spec
            .ok_or_else(|| Status::invalid_argument("spec required"))?;
        tenant(create.caller.as_ref(), &raw.tenant_id)?;
        let mut state = self.0.state.lock().await;
        state.recover_claim_write().await.map_err(status)?;
        state.healthy().map_err(status)?;
        state
            .live_claimant(node, &request.node_session_id, self.0.heartbeat_timeout)
            .map_err(status)?;
        let spec = cloning::normalize(&state.session, raw)
            .await
            .map_err(status)?;
        let snapshot = match &spec.snapshot_id {
            Some(id) => Some(
                state
                    .session
                    .get_snapshot(id)
                    .await
                    .map_err(status)?
                    .try_into()
                    .map_err(status)?,
            ),
            None => None,
        };
        Ok(pb::PreparedEnvironment {
            spec: Some(spec.into()),
            snapshot,
        })
    }
    pub(super) async fn claim_local(
        &self,
        node: String,
        request: pb::ClaimEnvironmentRequest,
    ) -> std::result::Result<pb::ClaimEnvironmentResponse, Status> {
        let spec: EnvironmentSpec = request
            .spec
            .ok_or_else(|| Status::invalid_argument("spec required"))?
            .try_into()
            .map_err(status)?;
        tenant(request.caller.as_ref(), &spec.tenant_id)?;
        let candidate = LocalClaim {
            node_id: node.clone(),
            node_session_id: request.node_session_id,
            devices: request
                .devices
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_>>()
                .map_err(status)?,
        };
        let mut state = self.0.state.lock().await;
        state.recover_claim_write().await.map_err(status)?;
        state.healthy().map_err(status)?;
        state
            .live_claimant(&node, &candidate.node_session_id, self.0.heartbeat_timeout)
            .map_err(status)?;
        if let Some(existing) = state.specs.get(&spec.id) {
            if existing != &spec {
                return Err(Status::already_exists("environment specification conflict"));
            }
            if !state.environments.contains_key(&spec.id) {
                return Ok(pb::ClaimEnvironmentResponse {
                    outcome: Some(Outcome::Fallback(true)),
                });
            }
        }
        // A stored owner is authoritative even when this candidate no longer fits.
        let existing = match state.session.get(&spec.id).await {
            Ok(record) => Some(record),
            Err(Error::NotFound) => None,
            Err(e) => return Err(status(e)),
        };
        if let Some(record) = &existing {
            if record.spec != spec {
                return Err(Status::already_exists("environment specification conflict"));
            }
        } else if !state
            .scheduler
            .local_candidate(&spec, &node, &candidate.devices)
            .map_err(status)?
        {
            return Ok(pb::ClaimEnvironmentResponse {
                outcome: Some(Outcome::Fallback(true)),
            });
        }
        let snapshot = if let Some(id) = &spec.snapshot_id {
            if existing.is_none() {
                let normalized = cloning::normalize(&state.session, spec.clone().into())
                    .await
                    .map_err(status)?;
                if normalized != spec {
                    return Err(Status::invalid_argument(
                        "snapshot specification must be normalized",
                    ));
                }
                state
                    .session
                    .acquire_snapshot(
                        id,
                        &spec.tenant_id,
                        Reference::Restore {
                            environment_id: spec.id.clone(),
                        },
                    )
                    .await
                    .map_err(status)?;
            }
            Some(state.session.get_snapshot(id).await.map_err(status)?)
        } else {
            None
        };
        let outcome = match state.session.claim(spec.clone(), &candidate).await {
            Ok(outcome) => outcome,
            Err(e @ Error::Unavailable(_)) => {
                state.claim_recovery = true;
                state.needs_recovery = true;
                self.0.changed.notify_waiters();
                return Err(status(e));
            }
            Err(e) => return Err(status(e)),
        };
        let record = outcome.record().clone();
        state.accept_stored_claim(record.clone()).map_err(status)?;
        state
            .live_claimant(&node, &candidate.node_session_id, self.0.heartbeat_timeout)
            .map_err(status)?;
        if matches!(outcome, ClaimOutcome::Owned(_)) {
            adx_observability::info!(event="local_environment_claim", environment_id=%record.spec.id,
                node_id=%record.assignment.node_id, generation=record.assignment.generation,
                "local ownership persisted and scheduler ledger synchronized");
        }
        let outcome = match outcome {
            ClaimOutcome::Owned(_) => {
                Outcome::Owned(Box::new(pb::StartAssignedEnvironmentRequest {
                    spec: Some(record.spec.into()),
                    assignment: Some(record.assignment.try_into().map_err(status)?),
                    node_session_id: candidate.node_session_id,
                    snapshot: snapshot
                        .map(TryInto::try_into)
                        .transpose()
                        .map_err(status)?,
                }))
            }
            ClaimOutcome::Existing(_) => Outcome::Existing(Box::new(
                state.environment_response(&record).map_err(status)?,
            )),
        };
        self.0.changed.notify_waiters();
        Ok(pb::ClaimEnvironmentResponse {
            outcome: Some(outcome),
        })
    }
}
