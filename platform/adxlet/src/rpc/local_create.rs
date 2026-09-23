use super::*;
use adx_transport::deadline::remaining;
use pb::claim_environment_response::Outcome;
impl NodeRpc {
    /// Retry only idle, unconfirmed holds. HTTP callers may have disappeared;
    /// ownership must still converge before these resources can be released.
    pub async fn retry_local_claims(&self) {
        let specs: Vec<_> = self
            .manager
            .local_holds
            .lock()
            .expect("shared state lock poisoned")
            .values()
            .map(|hold| hold.spec.clone())
            .collect();
        for spec in specs {
            let busy = self
                .entry_locks
                .lock()
                .expect("shared state lock poisoned")
                .get(&spec.id)
                .is_some_and(|lock| lock.strong_count() > 0);
            if busy {
                continue;
            }
            let request = pb::LocalEnvironmentCreateRequest {
                create: Some(pb::CreateEnvironmentRequest {
                    caller: Some(pb::CallerContext {
                        tenant_id: spec.tenant_id.clone(),
                        administrator: false,
                    }),
                    spec: Some(spec.into()),
                    schedule_timeout_seconds: 30,
                    create_timeout_seconds: 90,
                }),
                node_session_id: self.session_id.clone(),
            };
            let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
            if let Err(error) = self.create_local(request, deadline).await {
                adx_observability::warn!(code=?error.code(), "local claim still requires retry or reconciliation");
            }
        }
    }

    pub(super) async fn create_local(
        &self,
        request: pb::LocalEnvironmentCreateRequest,
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
        let spec: adx_core::EnvironmentSpec = if raw.snapshot_id.is_some() {
            let mut client = sink
                .client
                .read()
                .expect("shared state lock poisoned")
                .clone();
            tokio::time::timeout(
                remaining(deadline, sink.timeout)
                    .ok_or_else(|| Status::deadline_exceeded("create deadline exceeded"))?,
                client.prepare_create(adx_observability::trace::inject(request.clone())),
            )
            .await
            .map_err(|_| Status::unavailable("prepare create timed out"))??
            .into_inner()
            .spec
            .ok_or_else(|| Status::data_loss("prepared specification missing"))?
            .try_into()
            .map_err(status)?
        } else {
            raw.clone().try_into().map_err(status)?
        };
        let reservation = match self.manager.reserve_local(&spec) {
            Ok(held) => held,
            Err(Error::NoCapacity) => {
                drop(gate);
                return self.forward_local(sink, request, deadline).await;
            }
            Err(e) => return Err(status(e)),
        };
        let claim = pb::ClaimEnvironmentRequest {
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
                remaining(deadline, sink.timeout)
                    .ok_or_else(|| Status::deadline_exceeded("create deadline exceeded"))?,
                client.claim_environment(adx_observability::trace::inject(claim.clone())),
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
                            .create_from_snapshot(snapshot.try_into().map_err(status)?)
                            .await
                    }
                    None => handle.create().await,
                };
                response(result.map_err(status)?).map_err(status)
            }
            Outcome::Existing(existing) => {
                let record: EnvironmentRecord = existing
                    .record
                    .ok_or_else(|| Status::data_loss("existing record missing"))?
                    .try_into()
                    .map_err(status)?;
                if record.spec != spec {
                    return Err(Status::already_exists("existing specification changed"));
                }
                self.manager
                    .release_local(&spec.id, &reservation)
                    .map_err(status)?;
                if record.state == adx_core::EnvironmentState::Running {
                    response(crate::OperationResult {
                        record,
                        durability: Durability::Published,
                    })
                    .map_err(status)
                } else {
                    drop(gate);
                    self.forward_local(sink, request, deadline).await
                }
            }
            Outcome::Fallback(true) => {
                self.manager
                    .release_local(&spec.id, &reservation)
                    .map_err(status)?;
                drop(gate);
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
        let mut client = sink
            .client
            .read()
            .expect("shared state lock poisoned")
            .clone();
        let timeout = remaining(deadline, Duration::MAX)
            .ok_or_else(|| Status::deadline_exceeded("create deadline exceeded"))?;
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
