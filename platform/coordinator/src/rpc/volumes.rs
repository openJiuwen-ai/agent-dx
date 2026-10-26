use super::*;
use adx_core::volumes::{StorageDomain, VolumeSpec};

impl State {
    pub(super) async fn prepare_volumes(
        &mut self,
        spec: &EnvironmentSpec,
        domains: &BTreeMap<String, StorageDomain>,
    ) -> Result<()> {
        if !self.environments.contains_key(&spec.id) {
            self.session.pin_volumes(spec).await?;
        }
        let records = self.session.resolve_volumes(spec).await?;
        if records.is_empty() {
            return Ok(());
        }
        let mut storage_domains = BTreeSet::new();
        let mut waiting_for = None;
        for record in records {
            let adx_core::volumes::VolumeSource::Directory { storage_domain, .. } =
                &record.volume.spec.source;
            let domain = domains
                .get(storage_domain)
                .ok_or_else(|| Error::Invalid("unknown Volume storage domain".into()))?;
            if domain.backend_id != record.backend_id {
                return Err(Error::Conflict);
            }
            storage_domains.insert(storage_domain.clone());
            if record.occupancy.is_some() {
                waiting_for = Some(record.volume.uid);
            }
        }
        if let Some(uid) = &waiting_for {
            self.volume_waiters
                .entry(uid.clone())
                .or_default()
                .insert(spec.id.clone());
        }
        self.scheduler.set_volume_placement(
            &spec.id,
            crate::VolumePlacement {
                nodes: accessible_nodes(&storage_domains, domains)?,
                storage_domains,
                waiting_for,
            },
        );
        Ok(())
    }

    /// Recompute cached topology without changing occupancy waits or queue deadlines.
    pub(super) fn refresh_volume_topology(
        &mut self,
        domains: &BTreeMap<String, StorageDomain>,
    ) -> Result<()> {
        let mut changed = Vec::new();
        for (id, old) in &self.scheduler.volume_placement {
            let nodes = accessible_nodes(&old.storage_domains, domains)?;
            if nodes != old.nodes {
                let mut placement = old.clone();
                placement.nodes = nodes;
                changed.push((id.clone(), placement));
            }
        }
        for (id, placement) in changed {
            self.scheduler.set_volume_placement(&id, placement);
        }
        Ok(())
    }

    pub(super) async fn refresh_volume_waiters(&mut self) -> Result<bool> {
        if self.volume_waiters.is_empty() {
            return Ok(false);
        }
        let owners = self.session.volume_owners().await?;
        let released: Vec<_> = self
            .volume_waiters
            .keys()
            .filter(|uid| owners.get(*uid).is_some_and(Option::is_none))
            .cloned()
            .collect();
        let mut changed = false;
        for uid in released {
            if let Some(waiters) = self.volume_waiters.remove(&uid) {
                for id in waiters {
                    if let Some(mut placement) = self.scheduler.volume_placement.get(&id).cloned() {
                        if placement.waiting_for.as_deref() == Some(&uid) {
                            placement.waiting_for = None;
                            self.scheduler.set_volume_placement(&id, placement);
                            changed = true;
                        }
                    }
                }
            }
        }
        Ok(changed)
    }

    pub(super) fn forget_volume_waiter(&mut self, id: &str) {
        self.volume_waiters.retain(|_, waiters| {
            waiters.remove(id);
            !waiters.is_empty()
        });
    }
}

impl CoordinatorRpc {
    pub(super) fn volume_domains(&self, state: &State) -> Result<BTreeMap<String, StorageDomain>> {
        let mut domains = self
            .0
            .volume_domains
            .read()
            .map_err(|_| Error::Unavailable("volume domain configuration unavailable".into()))?
            .clone();
        for (id, domain) in &mut domains {
            if domain.dynamic_nodes {
                domain.node_roots = state
                    .live
                    .iter()
                    .filter_map(|(node, live)| {
                        if live.expired || !live.inspected || live.report.reconciling {
                            return None;
                        }
                        let mount = live.report.storage_mounts.get(id)?;
                        (mount.backend_id == domain.backend_id)
                            .then(|| (node.clone(), mount.root.clone()))
                    })
                    .collect();
            }
        }
        Ok(domains)
    }

    pub(super) fn watch_volume_changes(
        &self,
        mut changes: tokio::sync::broadcast::Receiver<crate::storage::ControlChange>,
    ) {
        let owner = Arc::downgrade(&self.0);
        tokio::spawn(async move {
            let mut repair = tokio::time::interval(Duration::from_secs(30));
            loop {
                tokio::select! {
                    change = changes.recv() => match change {
                        Ok(change) if change.fields.iter().all(|field| !field.starts_with("volume:")) => continue,
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                        _ => (),
                    },
                    _ = repair.tick() => (),
                }
                let Some(owner) = owner.upgrade() else {
                    break;
                };
                let mut state = owner.state.lock().await;
                if let Err(error) = async {
                    state.healthy()?;
                    if state.refresh_volume_waiters().await? {
                        owner.changed.notify_waiters();
                    }
                    Ok::<_, Error>(())
                }
                .await
                {
                    adx_observability::warn!("volume wait reconciliation unavailable: {error}");
                }
            }
        });
    }
    /// Install validated deployment storage mappings; no directory is accessed here.
    pub fn with_volume_domains(self, domains: BTreeMap<String, StorageDomain>) -> Result<Self> {
        for (id, domain) in &domains {
            if id.trim().is_empty() || id.len() > 256 || id.chars().any(char::is_control) {
                return Err(Error::Invalid("invalid volume storage domain ID".into()));
            }
            domain.validate()?;
        }
        *self
            .0
            .volume_domains
            .write()
            .map_err(|_| Error::Unavailable("volume domain configuration unavailable".into()))? =
            domains;
        Ok(self)
    }

    async fn volume_session(&self) -> std::result::Result<Session, Status> {
        let state = self.0.state.lock().await;
        state.healthy().map_err(status)?;
        Ok(state.session.clone())
    }
}

fn accessible_nodes(
    storage_domains: &BTreeSet<String>,
    domains: &BTreeMap<String, StorageDomain>,
) -> Result<BTreeSet<String>> {
    let mut nodes: Option<BTreeSet<String>> = None;
    for id in storage_domains {
        let domain = domains
            .get(id)
            .ok_or_else(|| Error::Invalid("unknown Volume storage domain".into()))?;
        let accessible = domain.node_roots.keys().cloned().collect();
        nodes = Some(match nodes {
            Some(nodes) => nodes.intersection(&accessible).cloned().collect(),
            None => accessible,
        });
    }
    Ok(nodes.unwrap_or_default())
}

fn info(record: crate::storage::StoredVolume) -> Result<pb::VolumeInfo> {
    use crate::storage::Initialization;
    use pb::VolumeInitializationState as State;
    let (initialization_state, initialization_error) = match record.initialization {
        Initialization::NotRequested => (State::NotRequested, String::new()),
        Initialization::Uninitialized => (State::Uninitialized, String::new()),
        Initialization::Pending { .. } => (State::Pending, String::new()),
        Initialization::Ready { .. } => (State::Ready, String::new()),
        Initialization::NeedsRepair { error } => (State::NeedsRepair, error),
    };
    Ok(pb::VolumeInfo {
        initialization_state: initialization_state as i32,
        initialization_error,
        volume: Some(record.volume.into()),
        environment_ids: record.references.into_iter().collect(),
        owner_ids: record.owner_ids.into_iter().collect(),
        snapshot_ids: record.snapshot_ids.into_iter().collect(),
        occupancy: record.occupancy.map(TryInto::try_into).transpose()?,
    })
}

fn owner<'a>(caller: Option<&'a pb::CallerContext>, requested: &'a str) -> &'a str {
    if requested.is_empty() {
        caller.map_or("", |caller| caller.tenant_id.as_str())
    } else {
        requested
    }
}

impl CoordinatorRpc {
    async fn initialization_session(
        &self,
        record: &EnvironmentRecord,
        session: &str,
    ) -> Result<Session> {
        let mut state = self.0.state.lock().await;
        state.recover_authoritative_state().await?;
        state.healthy()?;
        let node = &record.assignment.node_id;
        if state.overdue(node, self.0.heartbeat_timeout) {
            state.invalidate_node(node).await?;
        }
        if state
            .live
            .get(node)
            .is_none_or(|live| live.expired || live.session != session)
            || state
                .nodes
                .get(node)
                .and_then(|node| node.session.as_ref())
                .is_none_or(|node| node.id != session)
        {
            return Err(Error::Conflict);
        }
        Ok(state.session.clone())
    }
}

#[tonic::async_trait]
impl pb::volume_execution_service_server::VolumeExecutionService for CoordinatorRpc {
    async fn authorize_execution(
        &self,
        request: Request<pb::AuthorizeVolumeExecutionRequest>,
    ) -> std::result::Result<Response<pb::VolumeExecutionGrant>, Status> {
        let trace = adx_observability::trace::Trace::rpc("coordinator.volume.authorize", &request);
        trace
            .run_result(async {
                let principal = self.0.peers.authenticate(&request)?;
                let request = request.into_inner();
                let record: EnvironmentRecord = request
                    .record
                    .ok_or_else(|| Status::invalid_argument("execution record required"))?
                    .try_into()
                    .map_err(status)?;
                if principal != Principal::Node(record.assignment.node_id.clone()) {
                    return Err(Status::permission_denied(
                        "only owning node may authorize execution",
                    ));
                }
                let mut state = self.0.state.lock().await;
                state.recover_authoritative_state().await.map_err(status)?;
                state.healthy().map_err(status)?;
                state
                    .live_claimant(
                        &record.assignment.node_id,
                        &request.node_session_id,
                        self.0.heartbeat_timeout,
                    )
                    .map_err(status)?;
                let domains = self.volume_domains(&state).map_err(status)?;
                let definitions = state
                    .session
                    .resolve_volumes(&record.spec)
                    .await
                    .map_err(status)?;
                let mut granted = Vec::new();
                for definition in definitions {
                    if definition
                        .occupancy
                        .as_ref()
                        .is_some_and(|slot| slot.assignment != record.assignment)
                    {
                        return Err(status(Error::VolumeBusy(definition.volume.uid)));
                    }
                    let adx_core::volumes::VolumeSource::Directory { storage_domain, .. } =
                        &definition.volume.spec.source;
                    let domain = domains.get(storage_domain).ok_or_else(|| {
                        Status::failed_precondition("Volume storage domain unavailable")
                    })?;
                    if domain.backend_id != definition.backend_id {
                        return Err(Status::failed_precondition(
                            "Volume storage identity changed",
                        ));
                    }
                    let root = domain
                        .node_roots
                        .get(&record.assignment.node_id)
                        .ok_or_else(|| {
                            Status::resource_exhausted("Volume is not accessible on this node")
                        })?;
                    granted.push(pb::GrantedVolume {
                        volume: Some(definition.volume.into()),
                        backend_id: domain.backend_id.clone(),
                        root: root.clone(),
                    });
                }
                let current = state.session.get(&record.spec.id).await.map_err(status)?;
                if !current.resources_held()
                    && !state
                        .scheduler
                        .local_candidate(
                            &record.spec,
                            &record.assignment.node_id,
                            &record.assignment.devices,
                        )
                        .map_err(status)?
                {
                    return Err(Status::resource_exhausted(
                        "node compute capacity unavailable",
                    ));
                }
                state
                    .session
                    .authorize_volumes(
                        &record,
                        &request.previous_runtime_id,
                        &request.node_session_id,
                    )
                    .await
                    .map_err(status)?;
                let saved = state.session.get(&record.spec.id).await.map_err(status)?;
                state.accept_stored_claim(saved).map_err(|_| {
                    Status::unavailable("execution authorized; scheduler reconciliation required")
                })?;
                self.0.changed.notify_waiters();
                Ok(Response::new(pb::VolumeExecutionGrant {
                    assignment: Some(record.assignment.try_into().map_err(status)?),
                    runtime_id: record.runtime.id,
                    node_session_id: request.node_session_id,
                    volumes: granted,
                }))
            })
            .await
    }

    async fn begin_initialization(
        &self,
        request: Request<pb::VolumeInitializationRequest>,
    ) -> std::result::Result<Response<pb::VolumeInitializationResponse>, Status> {
        let trace =
            adx_observability::trace::Trace::rpc("coordinator.volume.initialize.begin", &request);
        trace
            .run_result(async {
                let principal = self.0.peers.authenticate(&request)?;
                let request = request.into_inner();
                let record: EnvironmentRecord = request
                    .record
                    .ok_or_else(|| Status::invalid_argument("execution record required"))?
                    .try_into()
                    .map_err(status)?;
                if principal != Principal::Node(record.assignment.node_id.clone()) {
                    return Err(Status::permission_denied(
                        "only owning node may initialize Volume",
                    ));
                }
                loop {
                    let changed = self.0.changed.notified();
                    tokio::pin!(changed);
                    changed.as_mut().enable();
                    let session = self
                        .initialization_session(&record, &request.node_session_id)
                        .await
                        .map_err(status)?;
                    match session
                        .begin_volume_initialization(
                            &record,
                            &request.node_session_id,
                            &request.volume_uid,
                        )
                        .await
                    {
                        Ok(action) => {
                            use crate::storage::InitializationAction;
                            return Ok(Response::new(pb::VolumeInitializationResponse {
                                initialize: action != InitializationAction::Ready,
                                verify_existing: action == InitializationAction::Verify,
                            }));
                        }
                        Err(Error::VolumeBusy(_)) => changed.await,
                        Err(error) => return Err(status(error)),
                    }
                }
            })
            .await
    }

    async fn complete_initialization(
        &self,
        request: Request<pb::CompleteVolumeInitializationRequest>,
    ) -> std::result::Result<Response<pb::DeleteVolumeResponse>, Status> {
        let trace = adx_observability::trace::Trace::rpc(
            "coordinator.volume.initialize.complete",
            &request,
        );
        trace
            .run_result(async {
                let principal = self.0.peers.authenticate(&request)?;
                let request = request.into_inner();
                let execution = request
                    .execution
                    .ok_or_else(|| Status::invalid_argument("initialization identity required"))?;
                let record: EnvironmentRecord = execution
                    .record
                    .ok_or_else(|| Status::invalid_argument("execution record required"))?
                    .try_into()
                    .map_err(status)?;
                if principal != Principal::Node(record.assignment.node_id.clone()) {
                    return Err(Status::permission_denied(
                        "only owning node may complete initialization",
                    ));
                }
                let session = self
                    .initialization_session(&record, &execution.node_session_id)
                    .await
                    .map_err(status)?;
                session
                    .complete_volume_initialization(
                        &record,
                        &execution.node_session_id,
                        &execution.volume_uid,
                        (!request.success).then_some(request.error.as_str()),
                    )
                    .await
                    .map_err(status)?;
                self.0.changed.notify_waiters();
                Ok(Response::new(pb::DeleteVolumeResponse {}))
            })
            .await
    }
}

#[tonic::async_trait]
impl pb::volume_service_server::VolumeService for CoordinatorRpc {
    async fn release_owner(
        &self,
        request: Request<pb::ReleaseVolumeOwnerRequest>,
    ) -> std::result::Result<Response<pb::DeleteVolumeResponse>, Status> {
        let trace =
            adx_observability::trace::Trace::rpc("coordinator.volume.release_owner", &request);
        trace
            .run_result(async {
                if self.0.peers.authenticate(&request)? != Principal::ApiServer {
                    return Err(Status::permission_denied("API Server identity required"));
                }
                let request = request.into_inner();
                tenant(request.caller.as_ref(), &request.tenant_id)?;
                let mut state = self.0.state.lock().await;
                state.recover_authoritative_state().await.map_err(status)?;
                state.healthy().map_err(status)?;
                if let Some(spec) = state.specs.get(&request.owner_id) {
                    tenant(request.caller.as_ref(), &spec.tenant_id)?;
                    if spec.tenant_id != request.tenant_id {
                        return Err(Status::permission_denied("Volume owner tenant mismatch"));
                    }
                }
                if state
                    .claims_in_flight
                    .get(&request.owner_id)
                    .copied()
                    .unwrap_or_default()
                    != 0
                {
                    return Err(Status::unavailable(
                        "creation outcome unsettled; retry owner cleanup",
                    ));
                }
                match state.session.get(&request.owner_id).await {
                    Ok(_) => {
                        return Err(Status::failed_precondition(
                            "delete the owner Environment before releasing Volumes",
                        ))
                    }
                    Err(Error::NotFound) => (),
                    Err(error) => return Err(status(error)),
                }
                state.session.claim_barrier().await.map_err(status)?;
                state
                    .session
                    .release_volume_owner(&request.tenant_id, &request.owner_id)
                    .await
                    .map_err(status)?;
                // Storage checks persisted reference ownership before any queued
                // request is removed. The state lock also fences new admissions.
                if state.scheduler.cancel_pending(&request.owner_id) {
                    state.specs.remove(&request.owner_id);
                    state.scheduling_deadlines.remove(&request.owner_id);
                }
                state.forget_volume_waiter(&request.owner_id);
                self.0.changed.notify_waiters();
                Ok(Response::new(pb::DeleteVolumeResponse {}))
            })
            .await
    }

    async fn ensure_volume(
        &self,
        request: Request<pb::EnsureVolumeRequest>,
    ) -> std::result::Result<Response<pb::Volume>, Status> {
        let trace = adx_observability::trace::Trace::rpc("coordinator.volume.ensure", &request);
        trace
            .run_result(async {
                if self.0.peers.authenticate(&request)? != Principal::ApiServer {
                    return Err(Status::permission_denied("API Server identity required"));
                }
                let request = request.into_inner();
                let spec: VolumeSpec = request
                    .spec
                    .ok_or_else(|| Status::invalid_argument("volume definition required"))?
                    .try_into()
                    .map_err(status)?;
                tenant(request.caller.as_ref(), &spec.tenant_id)?;
                let domains = self
                    .0
                    .volume_domains
                    .read()
                    .map_err(|_| Status::unavailable("volume domain configuration unavailable"))?
                    .clone();
                let volume = self
                    .volume_session()
                    .await?
                    .ensure_volume_for_owner(spec, &domains, &request.owner_id)
                    .await
                    .map_err(status)?;
                Ok(Response::new(volume.into()))
            })
            .await
    }

    async fn get_volume(
        &self,
        request: Request<pb::GetVolumeRequest>,
    ) -> std::result::Result<Response<pb::VolumeInfo>, Status> {
        let trace = adx_observability::trace::Trace::rpc("coordinator.volume.get", &request);
        trace
            .run_result(async {
                if self.0.peers.authenticate(&request)? != Principal::ApiServer {
                    return Err(Status::permission_denied("API Server identity required"));
                }
                let request = request.into_inner();
                let owner = owner(request.caller.as_ref(), &request.tenant_id);
                tenant(request.caller.as_ref(), owner)?;
                let record = self
                    .volume_session()
                    .await?
                    .get_volume(owner, &request.id)
                    .await
                    .map_err(status)?;
                Ok(Response::new(info(record).map_err(status)?))
            })
            .await
    }

    async fn list_volumes(
        &self,
        request: Request<pb::ListVolumesRequest>,
    ) -> std::result::Result<Response<pb::ListVolumesResponse>, Status> {
        let trace = adx_observability::trace::Trace::rpc("coordinator.volume.list", &request);
        trace
            .run_result(async {
                if self.0.peers.authenticate(&request)? != Principal::ApiServer {
                    return Err(Status::permission_denied("API Server identity required"));
                }
                let request = request.into_inner();
                let owner = owner(request.caller.as_ref(), &request.tenant_id);
                tenant(request.caller.as_ref(), owner)?;
                if request.page_size > 1000
                    || request.page_token.len() > 256
                    || request.page_token.chars().any(char::is_control)
                {
                    return Err(Status::invalid_argument("invalid volume page"));
                }
                let size = if request.page_size == 0 {
                    100
                } else {
                    request.page_size
                } as usize;
                let mut page: Vec<_> = self
                    .volume_session()
                    .await?
                    .list_volume_records(owner)
                    .await
                    .map_err(status)?
                    .into_iter()
                    .filter(|record| record.volume.spec.id > request.page_token)
                    .take(size + 1)
                    .collect();
                let next_page_token = if page.len() > size {
                    page.pop();
                    page.last()
                        .map(|record| record.volume.spec.id.clone())
                        .unwrap_or_default()
                } else {
                    String::new()
                };
                Ok(Response::new(pb::ListVolumesResponse {
                    volumes: page
                        .into_iter()
                        .map(info)
                        .collect::<Result<_>>()
                        .map_err(status)?,
                    next_page_token,
                }))
            })
            .await
    }

    async fn delete_volume(
        &self,
        request: Request<pb::GetVolumeRequest>,
    ) -> std::result::Result<Response<pb::DeleteVolumeResponse>, Status> {
        let trace = adx_observability::trace::Trace::rpc("coordinator.volume.delete", &request);
        trace
            .run_result(async {
                if self.0.peers.authenticate(&request)? != Principal::ApiServer {
                    return Err(Status::permission_denied("API Server identity required"));
                }
                let request = request.into_inner();
                let owner = owner(request.caller.as_ref(), &request.tenant_id);
                tenant(request.caller.as_ref(), owner)?;
                self.volume_session()
                    .await?
                    .delete_volume(owner, &request.id)
                    .await
                    .map_err(status)?;
                Ok(Response::new(pb::DeleteVolumeResponse {}))
            })
            .await
    }
}
