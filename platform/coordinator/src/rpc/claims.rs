//! Local-first ownership coordination; the same mutex guards center scheduling.
use super::*;
use crate::storage::{ClaimOutcome, LocalClaim};
use adx_core::snapshots::Reference;
use pb::claim_environment_response::Outcome;

impl State {
    pub(super) async fn recover_authoritative_state(&mut self) -> Result<()> {
        if !self.needs_recovery && self.session.inspection_ready().await {
            return Ok(());
        }
        self.needs_recovery = true;
        // The failed writer has returned. Fence only optional claims before
        // loading their authoritative outcomes; lifecycle objects are independent.
        self.session.claim_barrier().await?;
        let mut saved = self.session.snapshot().await?;
        self.session.refresh_inspection_inventory().await?;
        // An in-process catalog rebuild must retain local work already handed
        // to an adxlet. Requeuing it would have no central task to start it.
        // These reservations remain memory-only and keep their original expiry;
        // a new Coordinator process recovers only the durable snapshot.
        for (id, (assignment, _)) in &self.local_reservation_deadlines {
            if !saved.environments.contains_key(id) {
                if let Some(record) = self.environments.get(id).filter(|record| {
                    record.assignment == *assignment
                        && record.result.is_none()
                        && !record.invalidated
                        && record.recovery.is_none()
                }) {
                    saved.generation = saved.generation.max(assignment.generation);
                    saved.environments.insert(id.clone(), record.clone());
                }
            }
        }
        let pending: Vec<_> = self
            .specs
            .values()
            .filter(|s| {
                !saved.environments.contains_key(&s.id)
                    && self
                        .environments
                        .get(&s.id)
                        .is_none_or(|e| e.result.is_none())
            })
            .cloned()
            .collect();
        let mut scheduler = Coordinator::restore(&saved, self.placement)?;
        scheduler.use_generation_source(self.session.clone());
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
        self.specs.clear();
        for spec in pending {
            if let Some(placement) = self.scheduler.volume_placement.get(&spec.id).cloned() {
                scheduler.set_volume_placement(&spec.id, placement);
            }
            scheduler.submit(spec.clone())?;
            self.specs.insert(spec.id.clone(), spec);
        }
        self.scheduler = scheduler;
        self.nodes = saved.nodes;
        self.environments = saved.environments;
        self.failed_environments = self
            .environments
            .iter()
            .filter(|(_, i)| {
                i.result
                    .as_ref()
                    .is_some_and(|r| r.state == EnvironmentState::Failed)
            })
            .map(|(id, _)| id.clone())
            .collect();
        for (id, record) in &self.environments {
            self.specs.insert(id.clone(), record.spec.clone());
        }
        self.needs_recovery = false;
        self.refresh_volume_waiters().await?;
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
            return Err(error);
        }
        self.scheduler.generation = self.scheduler.generation.max(record.assignment.generation);
        self.specs
            .insert(record.spec.id.clone(), record.spec.clone());
        self.remember_environment(record);
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
        let record = stored.effective_record();
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
        state.recover_authoritative_state().await.map_err(status)?;
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
        persistent: bool,
    ) -> std::result::Result<pb::ClaimEnvironmentResponse, Status> {
        let deadline = if persistent && request.create_deadline_unix_millis == 0 {
            None
        } else {
            Some(adx_transport::deadline::Deadline::from_unix_millis(
                request.create_deadline_unix_millis,
            )?)
        };
        let request_id = request.request_id.clone();
        let spec: EnvironmentSpec = request
            .spec
            .ok_or_else(|| Status::invalid_argument("spec required"))?
            .try_into()
            .map_err(status)?;
        tenant(request.caller.as_ref(), &spec.tenant_id)?;
        let _publication = self.create_guard(format!("publication:{}", spec.id)).await;
        let (session, cached) = {
            let mut state = self.0.state.lock().await;
            state.recover_authoritative_state().await.map_err(status)?;
            state.healthy().map_err(status)?;
            (
                state.session.clone(),
                state.environments.get(&spec.id).cloned(),
            )
        };
        let cached = if cached
            .as_ref()
            .is_some_and(|stored| stored.result.is_some())
        {
            let mut state = self.0.state.lock().await;
            state.recover_authoritative_state().await.map_err(status)?;
            state.healthy().map_err(status)?;
            match state.session.get(&spec.id).await {
                Ok(current) => {
                    state.accept_stored_claim(current.clone()).map_err(status)?;
                    Some(current)
                }
                Err(Error::NotFound) => {
                    if state.environments.get(&spec.id).is_some_and(|old| {
                        cached
                            .as_ref()
                            .is_some_and(|cached| old.assignment == cached.assignment)
                    }) {
                        state.scheduler.retire_claim(&spec.id).map_err(status)?;
                        state.scheduler.forget_deleted(&spec.id).map_err(status)?;
                        state.environments.remove(&spec.id);
                        state.specs.remove(&spec.id);
                        state.create_requests.remove(&spec.id);
                    }
                    None
                }
                Err(error) => return Err(status(error)),
            }
        } else {
            cached
        };
        let published = match session
            .published_request(&spec.tenant_id, &request_id)
            .await
            .map_err(status)?
        {
            Some(record) => Some(record),
            None => match cached {
                Some(stored) if stored.spec.tenant_id != spec.tenant_id => {
                    return Err(Status::not_found("environment not found"));
                }
                Some(stored) => stored.result,
                None => None,
            },
        };
        if let Some(record) = published {
            let record = if record.state == EnvironmentState::Running {
                session
                    .publish_existing(record, &request_id)
                    .await
                    .map_err(status)?
            } else {
                record
            };
            let state = self.0.state.lock().await;
            let owner = state
                .nodes
                .get(&record.assignment.node_id)
                .ok_or_else(|| Status::unavailable("confirmed owner is unavailable"))?;
            return Ok(pb::ClaimEnvironmentResponse {
                outcome: Some(Outcome::Existing(Box::new(pb::GetEnvironmentResponse {
                    record: Some(record.try_into().map_err(status)?),
                    node_address: owner.address.clone(),
                    relay_address: owner.proxy_address.clone(),
                }))),
            });
        }
        let candidate = LocalClaim {
            request_id: request.request_id,
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
        state.recover_authoritative_state().await.map_err(status)?;
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
        // While healthy, the in-memory catalog is synchronized after every
        // successful write and rebuilt before `needs_recovery` is cleared. The
        // storage claim below still performs the authoritative atomic read/CAS;
        // avoid a redundant Redis round trip only used to choose admission.
        // A known owner remains authoritative even when this candidate no
        // longer fits the current placement snapshot.
        let existing = state.environments.get(&spec.id).cloned();
        if let Some(record) = &existing {
            if record.spec != spec {
                return Err(Status::already_exists("environment specification conflict"));
            }
        } else {
            let domains = self.volume_domains(&state).map_err(status)?;
            state
                .prepare_volumes(&spec, &domains)
                .await
                .map_err(status)?;
            if !state
                .scheduler
                .local_candidate(&spec, &node, &candidate.devices)
                .map_err(status)?
            {
                return Ok(pb::ClaimEnvironmentResponse {
                    outcome: Some(Outcome::Fallback(true)),
                });
            }
        }
        if let Some(record) = &existing {
            // A center-selected assignment is memory-only. A competing optional
            // Claim must reuse it rather than install a second durable owner.
            // Forwarding through the center also replays a durable pending Claim.
            if record.result.is_none() && !record.invalidated && record.recovery.is_none() {
                state
                    .create_requests
                    .entry(spec.id.clone())
                    .or_default()
                    .insert(request_id);
                return Ok(pb::ClaimEnvironmentResponse {
                    outcome: Some(Outcome::Existing(Box::new(
                        state.environment_response(record).map_err(status)?,
                    ))),
                });
            }
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
        if let Some(deadline) = deadline {
            deadline.remaining()?;
        }
        state
            .create_requests
            .entry(spec.id.clone())
            .or_default()
            .insert(request_id);
        if !persistent && !crate::storage::has_volumes(&spec) {
            let deadline = deadline
                .ok_or_else(|| Status::invalid_argument("local reservation deadline required"))?;
            let generation = state.session.next_generation().map_err(status)?;
            let shard_id = state
                .nodes
                .get(&node)
                .ok_or_else(|| Status::unavailable("local node disappeared"))?
                .shard_id;
            let assignment = adx_core::Assignment {
                environment_id: spec.id.clone(),
                node_id: node.clone(),
                shard_id,
                generation,
                devices: candidate.devices.clone(),
            };
            let record = StoredEnvironment {
                creation_generation: generation,
                creation: Some(
                    crate::storage::CreationSummary::new(&candidate.request_id).map_err(status)?,
                ),
                last_interruption: None,
                volume_execution: None,
                spec: spec.clone(),
                assignment: assignment.clone(),
                result: None,
                resume: None,
                recovery: None,
                invalidated: false,
                failed_since_unix_seconds: None,
            };
            state.accept_stored_claim(record).map_err(status)?;
            state
                .local_reservation_deadlines
                .insert(spec.id.clone(), (assignment.clone(), deadline.instant()));
            self.0.changed.notify_waiters();
            adx_observability::info!(event="local_environment_reservation", environment_id=%spec.id,
                node_id=%node, generation, "local execution reserved in memory");
            return Ok(pb::ClaimEnvironmentResponse {
                outcome: Some(Outcome::Owned(Box::new(
                    pb::StartAssignedEnvironmentRequest {
                        create_deadline_unix_millis: deadline.unix_millis(),
                        spec: Some(spec.into()),
                        assignment: Some(assignment.try_into().map_err(status)?),
                        node_session_id: candidate.node_session_id,
                        snapshot: snapshot
                            .map(TryInto::try_into)
                            .transpose()
                            .map_err(status)?,
                    },
                ))),
            });
        }
        *state.claims_in_flight.entry(spec.id.clone()).or_default() += 1;
        drop(state);
        let outcome = self.0.claim(spec.clone(), candidate.clone()).await;
        let mut state = self.0.state.lock().await;
        let in_flight = state
            .claims_in_flight
            .get_mut(&spec.id)
            .expect("claim registered before releasing the state lock");
        *in_flight -= 1;
        if *in_flight == 0 {
            state.claims_in_flight.remove(&spec.id);
        }
        self.0.changed.notify_waiters();
        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(Error::VolumeBusy(_)) => {
                return Ok(pb::ClaimEnvironmentResponse {
                    outcome: Some(Outcome::Fallback(true)),
                })
            }
            Err(e @ Error::Unavailable(_)) => {
                state.needs_recovery = true;
                self.0.changed.notify_waiters();
                return Err(status(e));
            }
            Err(e) => return Err(status(e)),
        };
        state.recover_authoritative_state().await.map_err(status)?;
        state.healthy().map_err(status)?;
        let record = outcome.record().clone();
        state.accept_stored_claim(record.clone()).map_err(status)?;
        if let Some(deadline) = deadline.filter(|_| matches!(outcome, ClaimOutcome::Owned(_))) {
            state
                .local_reservation_deadlines
                .entry(spec.id.clone())
                .or_insert((record.assignment.clone(), deadline.instant()));
        }
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
                    create_deadline_unix_millis: deadline
                        .map_or(0, |deadline| deadline.unix_millis()),
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
