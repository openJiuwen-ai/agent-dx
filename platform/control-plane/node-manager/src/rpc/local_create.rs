use super::*;
use pb::claim_instance_response::Outcome;
impl NodeRpc {
    /// Retry only idle, unconfirmed holds. HTTP callers may have disappeared;
    /// ownership must still converge before these resources can be released.
    pub async fn retry_local_claims(&self) {
        let specs: Vec<_> = self
            .manager
            .local_holds
            .lock()
            .unwrap()
            .values()
            .map(|hold| hold.spec.clone())
            .collect();
        for spec in specs {
            let busy = self
                .entry_locks
                .lock()
                .unwrap()
                .get(&spec.id)
                .is_some_and(|lock| lock.strong_count() > 0);
            if busy {
                continue;
            }
            let request = pb::LocalCreateRequest {
                create: Some(pb::CreateInstanceRequest {
                    caller: Some(pb::CallerContext {
                        tenant_id: spec.tenant_id.clone(),
                        administrator: false,
                    }),
                    spec: Some(spec.into()),
                }),
                node_session_id: self.session_id.clone(),
            };
            if let Err(error) = self.create_local(request).await {
                adx_observability::warn!(code=?error.code(), "local claim still requires retry or reconciliation");
            }
        }
    }

    pub(super) async fn create_local(
        &self,
        request: pb::LocalCreateRequest,
    ) -> std::result::Result<Response<pb::InstanceResult>, Status> {
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
            .master
            .as_ref()
            .ok_or_else(|| Status::unavailable("local-first entry is not configured"))?;
        let lock = {
            let mut locks = self.entry_locks.lock().unwrap();
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
        let spec: adx_core::InstanceSpec = if raw.snapshot_id.is_some() {
            let mut client = sink.client.read().unwrap().clone();
            tokio::time::timeout(
                sink.timeout,
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
                return self.forward_local(sink, request).await;
            }
            Err(e) => return Err(status(e)),
        };
        let claim = pb::ClaimInstanceRequest {
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
            let mut client = sink.client.read().unwrap().clone();
            answer = match tokio::time::timeout(
                sink.timeout,
                client.claim_instance(adx_observability::trace::inject(claim.clone())),
            )
            .await
            {
                Ok(result) => result.map(Response::into_inner),
                Err(_) => Err(Status::unavailable(
                    "claim result unknown; retry same Instance ID",
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
                let got: adx_core::InstanceSpec = owned
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
                let handle = self.manager.instance(spec, owner).map_err(status)?;
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
                let record: InstanceRecord = existing
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
                if record.state == adx_core::InstanceState::Running {
                    response(crate::OperationResult {
                        record,
                        durability: Durability::Published,
                    })
                    .map_err(status)
                } else {
                    drop(gate);
                    self.forward_local(sink, request).await
                }
            }
            Outcome::Fallback(true) => {
                self.manager
                    .release_local(&spec.id, &reservation)
                    .map_err(status)?;
                drop(gate);
                self.forward_local(sink, request).await
            }
            Outcome::Fallback(false) => Err(Status::data_loss("invalid fallback decision")),
        }
    }
    async fn forward_local(
        &self,
        sink: &MasterStateSink,
        request: pb::LocalCreateRequest,
    ) -> std::result::Result<Response<pb::InstanceResult>, Status> {
        let mut client = sink.client.read().unwrap().clone();
        tokio::time::timeout(
            sink.timeout,
            client.forward_create(adx_observability::trace::inject(request)),
        )
        .await
        .map_err(|_| {
            Status::unavailable("forwarded creation result unknown; retry same Instance ID")
        })?
    }
}
