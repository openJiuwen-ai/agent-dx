use super::*;
use adx_transport::deadline::remaining;
use pb::claim_environment_response::Outcome;
#[derive(Clone)]
pub(super) struct PendingLocalCreate {
    request: pb::LocalEnvironmentCreateRequest,
    deadline: tokio::time::Instant,
    reservation: crate::local::LocalReservation,
}
impl NodeRpc {
    /// Retry idle, unconfirmed holds with the original identity and deadline.
    pub async fn retry_local_claims(&self) {
        let pending: Vec<_> = {
            let holds = self
                .manager
                .local_holds
                .lock()
                .expect("local hold registry poisoned");
            let mut retries = self
                .local_retries
                .lock()
                .expect("local retry registry poisoned");
            retries.retain(|id, retry| {
                holds
                    .get(id)
                    .is_some_and(|hold| retry.reservation.token.as_ref() == Some(&hold.token))
            });
            retries
                .iter()
                .map(|(id, retry)| (id.clone(), retry.clone()))
                .collect()
        };
        for (id, retry) in pending {
            let busy = self
                .entry_locks
                .lock()
                .expect("entry lock registry poisoned")
                .get(&id)
                .is_some_and(|lock| lock.strong_count() > 0);
            if busy {
                continue;
            }
            if retry.deadline <= tokio::time::Instant::now() {
                // This hold has not become a Controller. No execution was started
                // by it; a later central assignment performs normal local admission.
                if let Err(error) = self.manager.release_local(&id, &retry.reservation) {
                    adx_observability::warn!(environment_id=%id, %error, "expired local hold release failed");
                }
                continue;
            }
            if let Err(error) = self.create_local(retry.request, retry.deadline).await {
                adx_observability::warn!(code=?error.code(), "local reservation still requires retry or reconciliation");
            }
        }
    }

    pub(super) async fn create_local(
        &self,
        mut request: pb::LocalEnvironmentCreateRequest,
        deadline: tokio::time::Instant,
    ) -> std::result::Result<Response<pb::EnvironmentResult>, Status> {
        if request.node_session_id != self.session_id {
            return Err(Status::failed_precondition("entry node session changed"));
        }
        let raw = request
            .create
            .as_ref()
            .and_then(|c| c.spec.as_ref())
            .ok_or_else(|| Status::invalid_argument("spec required"))?;
        let caller = request
            .create
            .as_ref()
            .and_then(|c| c.caller.as_ref())
            .cloned();
        tenant(caller.as_ref(), &raw.tenant_id)?;
        let sink = self
            .coordinator
            .as_ref()
            .ok_or_else(|| Status::unavailable("local-first entry is not configured"))?;
        if request
            .create
            .as_ref()
            .is_some_and(|c| c.preparation_context.is_some())
        {
            return self.forward_local(sink, request, deadline).await;
        }
        let lock = {
            let mut locks = self.entry_locks.lock().expect("shared state lock poisoned");
            locks.retain(|_, lock| lock.strong_count() > 0);
            if let Some(lock) = locks.get(&raw.id).and_then(std::sync::Weak::upgrade) {
                lock
            } else {
                let lock = Arc::new(tokio::sync::Mutex::new(()));
                locks.insert(raw.id.clone(), Arc::downgrade(&lock));
                lock
            }
        };
        let _serial = lock.lock().await;
        let gate = self.manager.lifecycle_ready.read().await;
        if !*gate || self.manager.is_draining() {
            return Err(Status::unavailable("node is reconciling or draining"));
        }
        if raw.snapshot_id.is_some() {
            drop(gate);
            drop(_serial);
            return self.forward_local(sink, request, deadline).await;
        }
        let spec: adx_core::EnvironmentSpec = raw.clone().try_into().map_err(status)?;
        let reservation = match self.manager.reserve_local(&spec) {
            Ok(held) => held,
            Err(Error::NoCapacity | Error::Conflict) => {
                drop(gate);
                drop(_serial);
                return self.forward_local(sink, request, deadline).await;
            }
            Err(e) => return Err(status(e)),
        };
        if reservation.token.is_some() {
            self.local_retries
                .lock()
                .expect("local retry registry poisoned")
                .entry(spec.id.clone())
                .and_modify(|retry| {
                    if retry.reservation.token != reservation.token {
                        *retry = PendingLocalCreate {
                            request: request.clone(),
                            deadline,
                            reservation: reservation.clone(),
                        };
                    }
                })
                .or_insert_with(|| PendingLocalCreate {
                    request: request.clone(),
                    deadline,
                    reservation: reservation.clone(),
                });
        }
        let claim = pb::ClaimEnvironmentRequest {
            create_deadline_unix_millis: request
                .create
                .as_ref()
                .map_or(0, |create| create.create_deadline_unix_millis),
            request_id: request
                .create
                .as_ref()
                .map(|r| r.request_id.clone())
                .unwrap_or_default(),
            spec: Some(spec.clone().into()),
            caller,
            node_session_id: self.session_id.clone(),
            devices: reservation
                .devices
                .clone()
                .into_iter()
                .map(Into::into)
                .collect(),
        };
        let mut answer = Err(Status::unavailable("claim unavailable"));
        for _ in 0..2 {
            let mut client = sink
                .client
                .read()
                .expect("shared state lock poisoned")
                .clone();
            answer = match tokio::time::timeout(
                remaining(deadline, sink.timeout).ok_or_else(|| {
                    Status::deadline_exceeded("create deadline exceeded during ownership claim")
                })?,
                async {
                    if request.persist_claim {
                        client
                            .claim_environment(adx_observability::trace::inject(claim.clone()))
                            .await
                    } else {
                        client
                            .reserve_local_environment(adx_observability::trace::inject(
                                claim.clone(),
                            ))
                            .await
                    }
                },
            )
            .await
            {
                Ok(result) => result.map(Response::into_inner),
                Err(_) => Err(Status::unavailable(
                    "claim result unknown; retry same Environment ID",
                )),
            };
            if !answer.as_ref().is_err_and(|e| {
                matches!(
                    e.code(),
                    tonic::Code::Unavailable | tonic::Code::DeadlineExceeded
                )
            }) {
                break;
            }
        }
        // A different immutable specification has definitively won. This
        // token cannot belong to it. Other errors may follow a committed write.
        if answer
            .as_ref()
            .is_err_and(|e| e.code() == tonic::Code::AlreadyExists)
        {
            self.manager
                .release_local(&spec.id, &reservation)
                .map_err(status)?;
        }
        match answer?
            .outcome
            .ok_or_else(|| Status::data_loss("claim outcome missing"))?
        {
            Outcome::Owned(owned) => {
                let owner: adx_core::Assignment = owned
                    .assignment
                    .ok_or_else(|| Status::data_loss("assignment missing"))?
                    .try_into()
                    .map_err(status)?;
                let got: adx_core::EnvironmentSpec = owned
                    .spec
                    .ok_or_else(|| Status::data_loss("spec missing"))?
                    .try_into()
                    .map_err(status)?;
                if got != spec
                    || owner.node_id != self.manager.node_id
                    || owner.devices != reservation.devices
                    || owned.node_session_id != self.session_id
                {
                    return Err(Status::data_loss("claim response identity changed"));
                }
                let handle = self.manager.environment(spec, owner).map_err(status)?;
                let result = match owned.snapshot {
                    Some(snapshot) => {
                        handle
                            .create_from_snapshot_with_deadline(
                                snapshot.try_into().map_err(status)?,
                                deadline,
                            )
                            .await
                    }
                    None => handle.create_with_deadline(deadline).await,
                };
                response(result.map_err(|error| creation_error(&handle, error))?).map_err(status)
            }
            Outcome::Existing(existing) => {
                let record: EnvironmentRecord = existing
                    .record
                    .ok_or_else(|| Status::data_loss("existing record missing"))?
                    .try_into()
                    .map_err(status)?;
                if record.spec.tenant_id != spec.tenant_id {
                    self.manager
                        .release_local(&spec.id, &reservation)
                        .map_err(status)?;
                    return Err(Status::not_found("environment not found"));
                }
                if let Some(create) = request.create.as_mut() {
                    create.expected_assignment =
                        Some(record.assignment.clone().try_into().map_err(status)?);
                }
                self.manager
                    .release_local(&spec.id, &reservation)
                    .map_err(status)?;
                if record.state == adx_core::EnvironmentState::Running {
                    response(crate::OperationResult {
                        newly_created: false,
                        record,
                        durability: Durability::Published,
                    })
                    .map_err(status)
                } else {
                    drop(gate);
                    drop(_serial);
                    self.forward_local(sink, request, deadline).await
                }
            }
            Outcome::Fallback(true) => {
                self.manager
                    .release_local(&spec.id, &reservation)
                    .map_err(status)?;
                drop(gate);
                drop(_serial);
                self.forward_local(sink, request, deadline).await
            }
            Outcome::Fallback(false) => Err(Status::data_loss("invalid fallback decision")),
        }
    }
    async fn forward_local(
        &self,
        sink: &CoordinatorStateSink,
        request: pb::LocalEnvironmentCreateRequest,
        deadline: tokio::time::Instant,
    ) -> std::result::Result<Response<pb::EnvironmentResult>, Status> {
        if let Some(spec) = request
            .create
            .as_ref()
            .and_then(|create| create.spec.as_ref())
        {
            adx_observability::info!(event="local_environment_fallback",
                environment_id=%spec.id, node_id=%self.manager.node_id,
                "entry node forwarded create to central scheduling");
        }
        let mut client = sink
            .client
            .read()
            .expect("shared state lock poisoned")
            .clone();
        let timeout = remaining(deadline, Duration::MAX).ok_or_else(|| {
            Status::deadline_exceeded("create deadline exceeded before central scheduling")
        })?;
        let timeout = adx_transport::deadline::create_response_timeout(timeout);
        let mut request = adx_observability::trace::inject(request);
        request.set_timeout(timeout);
        tokio::time::timeout(timeout, client.forward_create(request))
            .await
            .map_err(|_| {
                Status::unavailable("forwarded creation result unknown; retry same Environment ID")
            })?
    }
}

#[cfg(test)]
mod timeout_tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn forwarded_create_inherits_the_remaining_deadline() {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
        tokio::time::advance(Duration::from_secs(10)).await;
        assert_eq!(
            remaining(deadline, Duration::MAX).unwrap(),
            Duration::from_secs(80)
        );
        assert_eq!(
            remaining(deadline, Duration::from_secs(30)).unwrap(),
            Duration::from_secs(30)
        );
        tokio::time::advance(Duration::from_secs(80)).await;
        assert_eq!(remaining(deadline, Duration::MAX), None);
    }
}
