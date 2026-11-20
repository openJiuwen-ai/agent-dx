//! Single in-memory handoff with acknowledged per-environment update journals.
//! Old dependencies and aggregate idle claims remain fenced pending Child evidence.
use super::*;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Beginning,
    Installing,
    Installed,
    Opening,
    ActiveFenced,
    Completed,
    Cancelled,
}
impl Phase {
    fn installed(self) -> bool {
        matches!(
            self,
            Self::Installed | Self::Opening | Self::ActiveFenced | Self::Completed
        )
    }
    fn may_serve(self) -> bool {
        matches!(self, Self::Opening | Self::ActiveFenced | Self::Completed)
    }
    fn name(self) -> &'static str {
        match self {
            Self::Beginning => "beginning",
            Self::Installing => "installing",
            Self::Installed => "installed",
            Self::Opening => "opening",
            Self::ActiveFenced => "active_fenced",
            Self::Completed => "completed",
            Self::Cancelled => "cancelled",
        }
    }
}

pub(super) struct Task {
    identity: pb::RelayHandoffRequest,
    phase: Phase,
    snapshot: Option<pb::BindingSnapshot>,
    old_pending: BTreeMap<String, UpdateBindingRequest>,
    // ACK removes pending work, but must not forget an old-endpoint fence.
    old_retired: BTreeMap<String, (u64, u64)>,
    candidate_pending: BTreeMap<String, UpdateBindingRequest>,
    proof: Option<pb::RelayHandoffProof>,
    replacement: Option<pb::RelayReplacementRequest>,
}
impl Task {
    pub(super) fn is_completed(&self) -> bool {
        matches!(self.phase, Phase::Completed | Phase::Cancelled)
    }
    fn same_reservation(&self, request: &pb::RelayHandoffRequest) -> bool {
        let mut identity = self.identity.clone();
        identity.candidate_proxy_session_id = request.candidate_proxy_session_id.clone();
        identity == *request
    }
    fn new(identity: pb::RelayHandoffRequest) -> Self {
        Self {
            identity,
            phase: Phase::Beginning,
            snapshot: None,
            old_pending: BTreeMap::new(),
            old_retired: BTreeMap::new(),
            candidate_pending: BTreeMap::new(),
            proof: None,
            replacement: None,
        }
    }
    fn freeze_snapshot(&mut self, local: &Local) -> &pb::BindingSnapshot {
        if self.snapshot.is_none() {
            // Pre-snapshot changes are included in the exact frozen request.
            self.candidate_pending.clear();
        }
        let session = &self.identity.candidate_proxy_session_id;
        let epoch = 1; // Fresh candidate's first Begin, including a replacement.
        self.snapshot.get_or_insert_with(|| pb::BindingSnapshot {
            proxy_session_id: session.clone(),
            sync_epoch: epoch,
            bindings: local
                .bindings
                .values()
                .cloned()
                .map(|mut binding| {
                    binding.proxy_session_id = session.clone();
                    binding.sync_epoch = epoch;
                    binding
                })
                .collect(),
        })
    }
    fn queue(&mut self, old: Option<&UpdateBindingRequest>, next: &UpdateBindingRequest) {
        self.candidate_pending
            .insert(next.environment_id.clone(), next.clone());
        let availability = old.is_some_and(|old| {
            matches!((&old.binding, &next.binding),
                (Some(Binding::Active(a)), Some(Binding::Suspended(b)))
                | (Some(Binding::Suspended(a)), Some(Binding::Active(b))) if a == b)
        });
        if matches!(next.binding, Some(Binding::Retired(_)))
            || self.old_retired.contains_key(&next.environment_id)
            || old.is_some_and(|old| {
                old.ownership_generation != next.ownership_generation
                    || (old.binding != next.binding && !availability)
            })
        {
            let version = (next.ownership_generation, next.binding_revision);
            let version = self
                .old_retired
                .entry(next.environment_id.clone())
                .and_modify(|saved| *saved = (*saved).max(version))
                .or_insert(version);
            let mut retired = next.clone();
            (retired.ownership_generation, retired.binding_revision) = *version;
            retired.binding = Some(Binding::Retired(Retired {}));
            self.old_pending
                .insert(next.environment_id.clone(), retired);
        } else if availability {
            // The candidate follows availability; the draining old endpoint
            // stays unavailable without retiring this still-owned version.
            let mut suspended = next.clone();
            if let Some(Binding::Active(target)) = &next.binding {
                suspended.binding = Some(Binding::Suspended(target.clone()));
            }
            self.old_pending
                .insert(next.environment_id.clone(), suspended);
        }
    }
    fn ack(pending: &mut BTreeMap<String, UpdateBindingRequest>, sent: &UpdateBindingRequest) {
        if pending.get(&sent.environment_id) == Some(sent) {
            pending.remove(&sent.environment_id);
        }
    }
    fn matches_candidate(&self, state: &pb::BindingState) -> bool {
        state.proxy_session_id == self.identity.candidate_proxy_session_id
            && state.controller_session_id == self.identity.old_controller_session_id
            && state.sync_epoch == 1
    }
    fn state(&self) -> pb::RelayHandoffState {
        pb::RelayHandoffState {
            identity: Some(self.identity.clone()),
            phase: self.phase.name().into(),
            candidate_sync_epoch: 1,
        }
    }
}
impl UdsRoutes {
    pub async fn replace_failed_candidate(
        &self,
        request: pb::RelayReplacementRequest,
    ) -> Result<pb::RelayHandoffState> {
        let _topology = self.topology.write().await;
        let mut slot = self.handoff.lock().await;
        let task = slot.as_mut().ok_or(Error::NotFound)?;
        if let Some(previous) = task.replacement.as_ref() {
            if previous != &request {
                return Err(Error::Conflict);
            }
            return Ok(task.state());
        }
        let path = PathBuf::from(&request.replacement_socket);
        if request.identity.as_ref().is_none_or(|identity| {
            !task.same_reservation(identity)
                || (!task.identity.candidate_proxy_session_id.is_empty()
                    && identity.candidate_proxy_session_id
                        != task.identity.candidate_proxy_session_id)
        }) || request.failed_pid == 0
            || !request.failed_exited
            || task.phase == Phase::Cancelled
            || !path.is_absolute()
            || path.as_os_str().len() > 100
            || request.replacement_socket == task.identity.old_socket
            || request.replacement_socket == task.identity.candidate_socket
        {
            return Err(Error::Conflict);
        }
        // Retain original-generation revocations and proof while fencing every
        // delayed Open/Switch addressed to the dead candidate identity.
        task.identity.candidate_socket = request.replacement_socket.clone();
        task.identity.candidate_proxy_session_id.clear();
        if let Some(proof) = task.proof.as_mut() {
            proof.identity = Some(task.identity.clone());
        }
        task.phase = Phase::Beginning;
        task.snapshot = None;
        task.candidate_pending.clear();
        task.replacement = Some(request);
        self.activity.disconnect();
        Ok(task.state())
    }
    pub(super) async fn require_stable_topology(&self) -> Result<()> {
        if self
            .handoff
            .lock()
            .await
            .as_ref()
            .is_some_and(|task| !task.is_completed())
        {
            return Err(unavailable("Relay handoff retains old-generation dependencies; admission, reconcile and idle claims fenced"));
        }
        Ok(())
    }
    pub async fn reserve_handoff(
        &self,
        request: pb::RelayHandoffRequest,
    ) -> Result<pb::RelayHandoffState> {
        let _topology = self.topology.write().await;
        let mut slot = self.handoff.lock().await;
        if let Some(task) = slot.as_ref() {
            if task.same_reservation(&request) {
                return Ok(task.state());
            }
            if !task.is_completed() || task.identity.task_id == request.task_id {
                return Err(Error::Conflict);
            }
        }
        if request.task_id.is_empty()
            || !PathBuf::from(&request.old_socket).is_absolute()
            || !PathBuf::from(&request.candidate_socket).is_absolute()
            || request.old_socket == request.candidate_socket
            || std::path::Path::new(&request.old_socket) != self.path.borrow().as_path()
            || request.old_controller_session_id != self.owner
            || request.old_proxy_session_id.is_empty()
            || !request.candidate_proxy_session_id.is_empty()
        {
            return Err(Error::Conflict);
        }
        let local = self.local.lock().await;
        if !local.initialized
            || local.buffering
            || !local.admissions.is_empty()
            || !local.unconfirmed.is_empty()
            || local.session.as_ref().is_none_or(|s| {
                !s.ready
                    || s.proxy_session_id != request.old_proxy_session_id
                    || s.controller_session_id != request.old_controller_session_id
                    || s.sync_epoch != request.old_sync_epoch
            })
        {
            return Err(Error::Conflict);
        }
        let mut old = Self::connect_at(PathBuf::from(&request.old_socket), self.timeout).await?;
        let state = self.remote(&mut old).await?;
        if local.session.as_ref() != Some(&state) {
            return Err(Error::Conflict);
        }
        *slot = Some(Task::new(request));
        self.activity.disconnect();
        Ok(slot.as_ref().ok_or(Error::Conflict)?.state())
    }
    pub async fn cancel_handoff(
        &self,
        request: pb::RelayHandoffRequest,
    ) -> Result<pb::RelayHandoffState> {
        let _topology = self.topology.write().await;
        let mut slot = self.handoff.lock().await;
        let task = slot.as_mut().ok_or(Error::NotFound)?;
        if !task.same_reservation(&request)
            || task.proof.is_some()
            || task.phase.may_serve()
            || !matches!(task.phase, Phase::Beginning | Phase::Cancelled)
        {
            return Err(Error::Conflict);
        }
        task.phase = Phase::Cancelled;
        self.path.send_modify(|_| {});
        Ok(task.state())
    }
    pub async fn prove_handoff(
        &self,
        proof: pb::RelayHandoffProof,
    ) -> Result<pb::RelayHandoffState> {
        let _topology = self.topology.write().await;
        let mut slot = self.handoff.lock().await;
        let task = slot.as_mut().ok_or(Error::NotFound)?;
        if proof
            .identity
            .as_ref()
            .is_none_or(|identity| !task.same_reservation(identity))
            || proof.old_pid == 0
            || proof.identity.as_ref().is_some_and(|identity| {
                !identity.candidate_proxy_session_id.is_empty()
                    && !task.identity.candidate_proxy_session_id.is_empty()
                    && identity.candidate_proxy_session_id
                        != task.identity.candidate_proxy_session_id
            })
            || !(proof.listeners_released || (proof.begin_accepted && proof.old_exited))
            || task.proof.as_ref().is_some_and(|old| {
                old.old_pid != proof.old_pid
                    || (old.old_exited && !proof.old_exited)
                    || (old.listeners_released && !proof.listeners_released)
                    || (old.begin_accepted && !proof.begin_accepted)
            })
        {
            return Err(Error::Conflict);
        }
        if proof.old_exited {
            task.old_pending.clear();
        }
        task.proof = Some(proof);
        Ok(task.state())
    }
    pub async fn finish_handoff(
        &self,
        proof: pb::RelayHandoffProof,
    ) -> Result<pb::RelayHandoffState> {
        self.prove_handoff(proof.clone()).await?;
        let _topology = self.topology.write().await;
        let mut slot = self.handoff.lock().await;
        let task = slot.as_mut().ok_or(Error::NotFound)?;
        if task.phase == Phase::Completed {
            return Ok(task.state());
        }
        if task.phase != Phase::ActiveFenced
            || !proof.old_exited
            || !task.candidate_pending.is_empty()
        {
            return Err(Error::Conflict);
        }
        {
            let mut local = self.local.lock().await;
            if let Some(session) = local.session.clone() {
                local.full_ack(&session);
            }
        }
        task.phase = Phase::Completed;
        self.activity.disconnect();
        // A same-path notification still restarts WatchActivity, obtaining reset.
        self.path.send_modify(|_| {});
        Ok(task.state())
    }
    async fn update_at(
        &self,
        path: &str,
        session: &pb::BindingState,
        mut binding: UpdateBindingRequest,
    ) -> Result<()> {
        let mut client = Self::connect_at(PathBuf::from(path), self.timeout).await?;
        binding.proxy_session_id = session.proxy_session_id.clone();
        binding.sync_epoch = session.sync_epoch;
        let expected = (binding.ownership_generation, binding.binding_revision);
        let ack = client
            .update_binding(self.request(binding))
            .await
            .map_err(rpc_error)?
            .into_inner();
        if (ack.ownership_generation, ack.binding_revision) != expected {
            return Err(Error::Conflict);
        }
        Ok(())
    }
    async fn flush_endpoint(
        &self,
        path: &str,
        session: &pb::BindingState,
        pending: &mut BTreeMap<String, UpdateBindingRequest>,
        environment: Option<&str>,
        deadline: tokio::time::Instant,
    ) -> Result<()> {
        for binding in pending
            .values()
            .filter(|binding| environment.is_none_or(|id| binding.environment_id == id))
            .take(32)
            .cloned()
            .collect::<Vec<_>>()
        {
            tokio::time::timeout_at(deadline, self.update_at(path, session, binding.clone()))
                .await
                .map_err(|_| unavailable("handoff flush budget exhausted"))??;
            Task::ack(pending, &binding);
        }
        Ok(())
    }
    async fn flush_handoff(&self, task: &mut Task, environment: Option<&str>) -> Result<()> {
        // One deadline for both sides, including connection establishment. Exact
        // ACKs are committed individually before the next await can time out.
        let deadline = tokio::time::Instant::now() + Duration::from_millis(500);
        let old = pb::BindingState {
            proxy_session_id: task.identity.old_proxy_session_id.clone(),
            controller_session_id: task.identity.old_controller_session_id.clone(),
            sync_epoch: task.identity.old_sync_epoch,
            ready: true,
        };
        let candidate = pb::BindingState {
            proxy_session_id: task.identity.candidate_proxy_session_id.clone(),
            controller_session_id: task.identity.old_controller_session_id.clone(),
            sync_epoch: 1,
            ready: true,
        };
        let (old_result, candidate_result) = tokio::join!(
            async {
                if task.proof.as_ref().is_some_and(|proof| proof.old_exited) {
                    return Ok(());
                }
                self.flush_endpoint(
                    &task.identity.old_socket,
                    &old,
                    &mut task.old_pending,
                    environment,
                    deadline,
                )
                .await
            },
            async {
                if task.phase.installed() {
                    self.flush_endpoint(
                        &task.identity.candidate_socket,
                        &candidate,
                        &mut task.candidate_pending,
                        environment,
                        deadline,
                    )
                    .await
                } else {
                    Ok(())
                }
            }
        );
        // Join both endpoints before propagating either failure: both journals
        // must retain actual ACK progress even when the other side times out.
        old_result?;
        candidate_result?;
        let pending = |rows: &BTreeMap<String, UpdateBindingRequest>| match environment {
            Some(id) => rows.contains_key(id),
            None => !rows.is_empty(),
        };
        if pending(&task.old_pending) || pending(&task.candidate_pending) {
            return Err(unavailable(
                "handoff updates pending; retry same task or binding",
            ));
        }
        Ok(())
    }
    pub(super) async fn send_handoff(
        &self,
        task: &mut Task,
        mut next: UpdateBindingRequest,
    ) -> Result<()> {
        {
            let mut local = self.local.lock().await;
            let old = local.bindings.get(&next.environment_id);
            if let Some(old) = old {
                if matches!(next.binding, Some(Binding::Retired(_)))
                    && (old.ownership_generation > next.ownership_generation
                        || (old.ownership_generation == next.ownership_generation
                            && old.binding_revision > next.binding_revision
                            && matches!(old.binding, Some(Binding::Retired(_)))))
                {
                    next = old.clone();
                }
                let a = (old.ownership_generation, old.binding_revision);
                let b = (next.ownership_generation, next.binding_revision);
                let availability = matches!((&old.binding, &next.binding),
                    (Some(Binding::Active(a)), Some(Binding::Suspended(b)))
                    | (Some(Binding::Suspended(a)), Some(Binding::Active(b))) if a == b);
                if b < a || (b == a && old.binding != next.binding && !availability) {
                    return Err(Error::Conflict);
                }
            }
            task.queue(old, &next);
            if task.proof.as_ref().is_some_and(|proof| proof.old_exited) {
                task.old_pending.clear();
            }
            local
                .bindings
                .insert(next.environment_id.clone(), next.clone());
        }
        self.flush_handoff(task, Some(&next.environment_id)).await?;
        if !matches!(task.phase, Phase::ActiveFenced | Phase::Completed) {
            return Err(unavailable(
                "candidate synchronization pending; canonical update retained",
            ));
        }
        self.retired_admission(&next).await;
        Ok(())
    }
    pub async fn switch_handoff(
        &self,
        request: pb::RelayHandoffRequest,
    ) -> Result<pb::RelayHandoffState> {
        let _topology = self.topology.write().await;
        let mut slot = self.handoff.lock().await;
        if slot.as_ref().is_none_or(|task| task.proof.is_none()) {
            return Err(Error::Conflict);
        }
        if let Some(task) = slot.as_mut() {
            if !task.same_reservation(&request) {
                return Err(Error::Conflict);
            }
            if task.phase == Phase::Cancelled {
                return Err(Error::Conflict);
            }
            if task.identity.candidate_proxy_session_id.is_empty() {
                if request.candidate_proxy_session_id.is_empty() {
                    return Err(Error::Conflict);
                }
                task.identity.candidate_proxy_session_id =
                    request.candidate_proxy_session_id.clone();
            } else if task.identity != request {
                return Err(Error::Conflict);
            }
        }
        if let Some(task) = slot.as_ref() {
            if task.phase == Phase::Completed {
                return Ok(task.state());
            }
            if task.phase == Phase::ActiveFenced {
                let task = slot.as_mut().ok_or(Error::Conflict)?;
                self.flush_handoff(task, None).await?;
                return Ok(task.state());
            }
        }
        let channel =
            Self::channel_at(PathBuf::from(&request.candidate_socket), self.timeout).await?;
        let mut candidate = RelayServiceClient::new(channel.clone())
            .max_encoding_message_size(64 * 1024 * 1024)
            .max_decoding_message_size(64 * 1024 * 1024);
        let mut candidate_upgrade =
            pb::relay_upgrade_service_client::RelayUpgradeServiceClient::new(channel)
                .max_encoding_message_size(64 * 1024 * 1024)
                .max_decoding_message_size(64 * 1024 * 1024);
        let mut state = self.remote(&mut candidate).await?;
        let task = slot.as_mut().ok_or(Error::Conflict)?;
        if !task.matches_candidate(&state) {
            if task.phase != Phase::Beginning {
                return Err(Error::Conflict);
            }
            if state.proxy_session_id != request.candidate_proxy_session_id
                || state.ready
                || state.sync_epoch != 0
                || !state.controller_session_id.is_empty()
            {
                return Err(Error::Conflict);
            }
            state.controller_session_id = request.old_controller_session_id.clone();
            state = candidate
                .begin_bindings(self.request(state))
                .await
                .map_err(rpc_error)?
                .into_inner();
            if !task.matches_candidate(&state) || state.ready {
                return Err(Error::Conflict);
            }
        }
        if state.ready {
            if !task.phase.may_serve() {
                return Err(Error::Conflict);
            }
            task.snapshot = None;
        } else if !task.phase.installed() {
            task.phase = Phase::Installing;
            let local = self.local.lock().await;
            let snapshot = task.freeze_snapshot(&local).clone();
            drop(local);
            state = candidate_upgrade
                .install_candidate_bindings(self.request(snapshot))
                .await
                .map_err(rpc_error)?
                .into_inner();
            if !task.matches_candidate(&state) || state.ready {
                return Err(Error::Conflict);
            }
            task.snapshot = None;
            task.phase = Phase::Installed;
        }
        // Bounded, acknowledged progress; retries never restart a whole catalog replay.
        self.flush_handoff(task, None).await?;
        if !state.ready {
            // Record uncertainty before Open. A lost reply is resolved by the
            // exact Get above; a closed candidate first receives latest deltas.
            task.phase = Phase::Opening;
            state = candidate_upgrade
                .open_candidate_bindings(self.request(state))
                .await
                .map_err(rpc_error)?
                .into_inner();
            if !task.matches_candidate(&state) || !state.ready {
                return Err(Error::Conflict);
            }
        }
        self.local.lock().await.session = Some(state);
        self.path
            .send_replace(PathBuf::from(&request.candidate_socket));
        self.activity.disconnect();
        task.phase = Phase::ActiveFenced;
        Ok(task.state())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct HandoffEndpoints {
        routes: UdsRoutes,
        _directory: tempfile::TempDir,
        servers: Vec<tokio::task::JoinHandle<()>>,
    }

    impl HandoffEndpoints {
        async fn new(initial: &UpdateBindingRequest) -> Self {
            use data_plane_gateway::node::route_control::{proto as relay_pb, BindingService};
            use data_plane_gateway::{common::protocol::GatewayPolicy, node::Relay};
            use tokio_stream::wrappers::UnixListenerStream;

            let directory = tempfile::tempdir().unwrap();
            let mut identity = request();
            let mut servers = Vec::new();
            for old in [true, false] {
                let service = Arc::new(BindingService::new(Arc::new(
                    Relay::new(GatewayPolicy::new(vec![])).with_route_enforcement(),
                )));
                let mut state = service.inspect().await;
                state.controller_session_id = identity.old_controller_session_id.clone();
                let state = service.begin(state).await.unwrap();
                service
                    .replace(relay_pb::BindingSnapshot {
                        proxy_session_id: state.proxy_session_id.clone(),
                        sync_epoch: state.sync_epoch,
                        bindings: vec![],
                    })
                    .await
                    .unwrap();
                let path = directory
                    .path()
                    .join(if old { "old.sock" } else { "new.sock" });
                let listener = tokio::net::UnixListener::bind(&path).unwrap();
                if old {
                    identity.old_socket = path.to_str().unwrap().into();
                    identity.old_proxy_session_id = state.proxy_session_id;
                    identity.old_sync_epoch = state.sync_epoch;
                } else {
                    identity.candidate_socket = path.to_str().unwrap().into();
                    identity.candidate_proxy_session_id = state.proxy_session_id;
                    assert_eq!(state.sync_epoch, 1);
                }
                servers.push(tokio::spawn(async move {
                    tonic::transport::Server::builder()
                        .add_service(
                            relay_pb::relay_service_server::RelayServiceServer::from_arc(service),
                        )
                        .serve_with_incoming(UnixListenerStream::new(listener))
                        .await
                        .unwrap();
                }));
            }
            let routes =
                UdsRoutes::new(identity.old_socket.clone().into(), Duration::from_secs(2)).unwrap();
            let mut task = Task::new(identity);
            task.phase = Phase::ActiveFenced;
            routes
                .local
                .lock()
                .await
                .bindings
                .insert(initial.environment_id.clone(), initial.clone());
            task.freeze_snapshot(&*routes.local.lock().await);
            for old in [true, false] {
                let state = pb::BindingState {
                    proxy_session_id: if old {
                        &task.identity.old_proxy_session_id
                    } else {
                        &task.identity.candidate_proxy_session_id
                    }
                    .clone(),
                    controller_session_id: task.identity.old_controller_session_id.clone(),
                    sync_epoch: if old { task.identity.old_sync_epoch } else { 1 },
                    ready: true,
                };
                let path = if old {
                    &task.identity.old_socket
                } else {
                    &task.identity.candidate_socket
                };
                routes
                    .update_at(path, &state, initial.clone())
                    .await
                    .unwrap();
            }
            *routes.handoff.lock().await = Some(task);
            Self {
                routes,
                _directory: directory,
                servers,
            }
        }

        async fn old_update(&self, binding: UpdateBindingRequest) -> Result<()> {
            let slot = self.routes.handoff.lock().await;
            let task = slot.as_ref().unwrap();
            let state = pb::BindingState {
                proxy_session_id: task.identity.old_proxy_session_id.clone(),
                controller_session_id: task.identity.old_controller_session_id.clone(),
                sync_epoch: task.identity.old_sync_epoch,
                ready: true,
            };
            self.routes
                .update_at(&task.identity.old_socket, &state, binding)
                .await
        }

        async fn assert_acked(&self) {
            let slot = self.routes.handoff.lock().await;
            let task = slot.as_ref().unwrap();
            assert!(!task.is_completed());
            assert!(
                task.old_pending.is_empty(),
                "real old Relay ACK is required"
            );
            assert!(
                task.candidate_pending.is_empty(),
                "real candidate Relay ACK is required"
            );
        }
    }

    impl Drop for HandoffEndpoints {
        fn drop(&mut self) {
            for server in &self.servers {
                server.abort();
            }
        }
    }

    fn compatibility_binding() -> UpdateBindingRequest {
        UpdateBindingRequest {
            environment_id: "env".into(),
            ownership_generation: 2,
            binding_revision: 2,
            binding: Some(Binding::Active(RuntimeTarget {
                runtime_id: "runtime".into(),
                ip: "10.0.0.1".into(),
            })),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn rebase_compat_handoff_availability_acks_both_relays_and_keeps_retirement_fence() {
        let active = compatibility_binding();
        let endpoints = HandoffEndpoints::new(&active).await;
        let mut suspended = active.clone();
        let Some(Binding::Active(target)) = active.binding.clone() else {
            panic!("active fixture")
        };
        suspended.binding = Some(Binding::Suspended(target));
        endpoints.routes.send(suspended.clone()).await.unwrap();
        endpoints.assert_acked().await;
        endpoints.old_update(suspended.clone()).await.unwrap();
        endpoints.routes.send(active.clone()).await.unwrap();
        endpoints.assert_acked().await;
        assert_eq!(endpoints.routes.local.lock().await.bindings["env"], active);
        let mut conflicting = suspended.clone();
        let Some(Binding::Suspended(target)) = conflicting.binding.as_mut() else {
            panic!("suspended fixture")
        };
        target.runtime_id = "wrong-runtime".into();
        assert!(matches!(
            endpoints.routes.send(conflicting).await,
            Err(Error::Conflict)
        ));
        let mut retired = active.clone();
        retired.binding_revision += 1;
        retired.binding = Some(Binding::Retired(Retired {}));
        endpoints.routes.send(retired).await.unwrap();
        endpoints.assert_acked().await;
        assert!(matches!(
            endpoints.routes.send(active).await,
            Err(Error::Conflict)
        ));
        assert!(matches!(
            endpoints.routes.send(suspended.clone()).await,
            Err(Error::Conflict)
        ));
        assert!(matches!(
            endpoints.old_update(suspended).await,
            Err(Error::Conflict)
        ));
    }

    #[tokio::test]
    async fn rebase_compat_handoff_availability_preserves_an_acked_old_target_retirement() {
        let initial = compatibility_binding();
        let endpoints = HandoffEndpoints::new(&initial).await;
        let mut replacement = initial.clone();
        replacement.binding_revision += 2;
        let Some(Binding::Active(target)) = replacement.binding.as_mut() else {
            panic!("active fixture")
        };
        target.runtime_id = "replacement".into();
        endpoints.routes.send(replacement.clone()).await.unwrap();
        endpoints.assert_acked().await;
        let mut suspended = replacement.clone();
        let Some(Binding::Active(target)) = replacement.binding.clone() else {
            panic!("active fixture")
        };
        suspended.binding = Some(Binding::Suspended(target));
        endpoints.routes.send(suspended.clone()).await.unwrap();
        endpoints.assert_acked().await;
        // ACK removed old_pending, but the old Relay's same-version tombstone survives.
        assert!(matches!(
            endpoints.old_update(suspended.clone()).await,
            Err(Error::Conflict)
        ));
        endpoints.routes.send(replacement.clone()).await.unwrap();
        endpoints.assert_acked().await;
        assert!(matches!(
            endpoints.old_update(replacement.clone()).await,
            Err(Error::Conflict)
        ));
        assert_eq!(
            endpoints.routes.local.lock().await.bindings["env"],
            replacement
        );
    }

    #[tokio::test]
    async fn rebase_compat_handoff_stale_retirement_confirms_new_owner_without_overwrite() {
        let current = compatibility_binding();
        let endpoints = HandoffEndpoints::new(&current).await;
        let mut stale = current.clone();
        stale.ownership_generation -= 1;
        stale.binding_revision = u64::MAX;
        stale.binding = Some(Binding::Retired(Retired {}));
        endpoints.routes.send(stale.clone()).await.unwrap();
        endpoints.assert_acked().await;
        assert_eq!(endpoints.routes.local.lock().await.bindings["env"], current);
        let mut retired = current.clone();
        retired.binding_revision += 1;
        retired.binding = Some(Binding::Retired(Retired {}));
        endpoints.routes.send(retired.clone()).await.unwrap();
        stale = retired.clone();
        stale.binding_revision -= 1;
        endpoints.routes.send(stale).await.unwrap();
        endpoints.assert_acked().await;
        assert_eq!(endpoints.routes.local.lock().await.bindings["env"], retired);
        assert!(matches!(
            endpoints.routes.send(current).await,
            Err(Error::Conflict)
        ));
    }

    #[tokio::test]
    async fn replacement_fences_late_failed_candidate_and_replays_exactly() {
        let routes = UdsRoutes::new("/run/old.sock".into(), Duration::from_secs(1)).unwrap();
        let mut identity = request();
        identity.candidate_proxy_session_id = "failed-candidate".into();
        *routes.handoff.lock().await = Some(Task::new(identity.clone()));
        let proof = pb::RelayHandoffProof {
            identity: Some(identity.clone()),
            old_pid: 7,
            begin_accepted: true,
            listeners_released: true,
            old_exited: false,
        };
        routes.prove_handoff(proof.clone()).await.unwrap();
        routes.handoff.lock().await.as_mut().unwrap().phase = Phase::Opening;
        let replacement = pb::RelayReplacementRequest {
            identity: Some(identity.clone()),
            failed_pid: 42,
            failed_exited: true,
            replacement_socket: "/run/rollback.sock".into(),
        };
        let first = routes
            .replace_failed_candidate(replacement.clone())
            .await
            .unwrap();
        let mut replacement_identity = identity.clone();
        replacement_identity.candidate_socket = "/run/rollback.sock".into();
        replacement_identity.candidate_proxy_session_id.clear();
        assert_eq!(first.identity.as_ref(), Some(&replacement_identity));
        assert_eq!(
            routes.replace_failed_candidate(replacement).await.unwrap(),
            first
        );
        let mut replacement_proof = proof;
        replacement_proof.identity = Some(replacement_identity.clone());
        assert_eq!(
            routes.handoff.lock().await.as_ref().unwrap().proof.as_ref(),
            Some(&replacement_proof)
        );
        assert!(matches!(
            routes.switch_handoff(identity).await,
            Err(Error::Conflict)
        ));
        let slot = routes.handoff.lock().await;
        let task = slot.as_ref().unwrap();
        assert_eq!(task.identity, replacement_identity);
        assert_eq!(task.proof.as_ref(), Some(&replacement_proof));
        assert!(matches!(task.phase, Phase::Beginning));
    }

    #[tokio::test]
    async fn unbegun_reservation_can_be_cancelled_and_replayed() {
        let routes = UdsRoutes::new("/run/old.sock".into(), Duration::from_secs(1)).unwrap();
        let identity = request();
        *routes.handoff.lock().await = Some(Task::new(identity.clone()));
        assert_eq!(
            routes.cancel_handoff(identity.clone()).await.unwrap().phase,
            "cancelled"
        );
        assert_eq!(
            routes.cancel_handoff(identity).await.unwrap().phase,
            "cancelled"
        );
        routes.require_stable_topology().await.unwrap();
    }

    #[tokio::test]
    async fn invalid_next_reserve_keeps_previous_terminal_receipt() {
        let routes = UdsRoutes::new("/run/new.sock".into(), Duration::from_millis(10)).unwrap();
        let identity = request();
        let mut task = Task::new(identity.clone());
        task.phase = Phase::Completed;
        *routes.handoff.lock().await = Some(task);
        let mut next = identity.clone();
        next.task_id = "second".into();
        next.candidate_proxy_session_id.clear();
        assert!(routes.reserve_handoff(next).await.is_err());
        assert_eq!(
            routes.handoff.lock().await.as_ref().unwrap().phase.name(),
            "completed"
        );
    }

    #[tokio::test]
    async fn exit_proof_is_task_exact_and_finish_is_replayable() {
        let routes = UdsRoutes::new("/run/old.sock".into(), Duration::from_secs(1)).unwrap();
        let identity = request();
        let mut task = Task::new(identity.clone());
        task.phase = Phase::ActiveFenced;
        *routes.handoff.lock().await = Some(task);
        let mut proof = pb::RelayHandoffProof {
            identity: Some(identity.clone()),
            old_pid: 42,
            begin_accepted: true,
            listeners_released: true,
            old_exited: true,
        };
        proof.identity.as_mut().unwrap().task_id = "wrong".into();
        assert!(routes.finish_handoff(proof.clone()).await.is_err());
        proof.identity = Some(identity.clone());
        assert_eq!(
            routes.finish_handoff(proof.clone()).await.unwrap().phase,
            "completed"
        );
        assert_eq!(
            routes.finish_handoff(proof).await.unwrap().phase,
            "completed"
        );
        routes.require_stable_topology().await.unwrap();
    }

    #[tokio::test]
    async fn stalled_generation_does_not_starve_other_generation_ack() {
        use data_plane_gateway::node::route_control::{proto as relay_pb, BindingService};
        use data_plane_gateway::{common::protocol::GatewayPolicy, node::Relay};
        use tokio_stream::wrappers::UnixListenerStream;
        for stall_old in [true, false] {
            let directory = tempfile::tempdir().unwrap();
            let stalled_path = directory.path().join("stalled.sock");
            // Accept transport connections but never drive HTTP/2 or gRPC.
            let stalled = tokio::net::UnixListener::bind(&stalled_path).unwrap();
            let stalled_server = tokio::spawn(async move {
                let mut sockets = Vec::new();
                loop {
                    sockets.push(stalled.accept().await.unwrap().0);
                }
            });
            let bindings = Arc::new(BindingService::new(Arc::new(
                Relay::new(GatewayPolicy::new(vec![])).with_route_enforcement(),
            )));
            let mut initial = bindings.inspect().await;
            initial.controller_session_id = "controller".into();
            let state = bindings.begin(initial).await.unwrap();
            bindings
                .replace(relay_pb::BindingSnapshot {
                    proxy_session_id: state.proxy_session_id.clone(),
                    sync_epoch: state.sync_epoch,
                    bindings: vec![],
                })
                .await
                .unwrap();
            let live_path = directory.path().join("live.sock");
            let live = tokio::net::UnixListener::bind(&live_path).unwrap();
            let live_server = tokio::spawn(async move {
                tonic::transport::Server::builder()
                    .add_service(
                        relay_pb::relay_service_server::RelayServiceServer::from_arc(bindings),
                    )
                    .serve_with_incoming(UnixListenerStream::new(live))
                    .await
                    .unwrap();
            });
            let routes = UdsRoutes::new(live_path.clone(), Duration::from_secs(2)).unwrap();
            let mut identity = request();
            let live = live_path.to_str().unwrap().to_string();
            let stalled = stalled_path.to_str().unwrap().to_string();
            if stall_old {
                identity.old_socket = stalled;
                identity.candidate_socket = live;
                identity.candidate_proxy_session_id = state.proxy_session_id;
            } else {
                identity.old_socket = live;
                identity.old_proxy_session_id = state.proxy_session_id;
                identity.old_sync_epoch = state.sync_epoch;
                identity.candidate_socket = stalled;
            }
            let mut task = Task::new(identity);
            task.phase = Phase::Installed;
            let retired = UpdateBindingRequest {
                environment_id: "env".into(),
                ownership_generation: 1,
                binding_revision: 3,
                binding: Some(Binding::Retired(Retired {})),
                ..Default::default()
            };
            task.old_pending.insert("env".into(), retired.clone());
            task.candidate_pending.insert("env".into(), retired.clone());
            assert!(routes.flush_handoff(&mut task, None).await.is_err());
            if stall_old {
                assert_eq!(task.old_pending["env"], retired);
                assert!(task.candidate_pending.is_empty());
            } else {
                assert!(task.old_pending.is_empty());
                assert_eq!(task.candidate_pending["env"], retired);
            }
            stalled_server.abort();
            live_server.abort();
        }
    }

    fn request() -> pb::RelayHandoffRequest {
        pb::RelayHandoffRequest {
            task_id: "release".into(),
            old_socket: "/run/old.sock".into(),
            old_proxy_session_id: "old".into(),
            old_controller_session_id: "controller".into(),
            old_sync_epoch: 7,
            candidate_socket: "/run/new.sock".into(),
            candidate_proxy_session_id: "new".into(),
        }
    }

    #[test]
    fn installation_uncertainty_does_not_imply_open_uncertainty() {
        let mut task = Task::new(request());
        let local = Local::default();
        task.freeze_snapshot(&local);
        assert!(!task.phase.may_serve());
        assert!(task.snapshot.is_some());
        let mut state = pb::BindingState {
            proxy_session_id: "new".into(),
            sync_epoch: 1,
            controller_session_id: "controller".into(),
            ready: true,
        };
        assert!(task.matches_candidate(&state));
        task.phase = Phase::Opening; // Recorded before issuing Open, including a lost reply.
        assert!(task.matches_candidate(&state) && task.phase.may_serve());
        state.sync_epoch += 1;
        assert!(!task.matches_candidate(&state));
    }

    #[tokio::test]
    async fn failed_read_only_preflight_does_not_reserve_task() {
        let routes = UdsRoutes::new(
            "/nonexistent-adx-handoff/old.sock".into(),
            Duration::from_millis(10),
        )
        .unwrap();
        let mut identity = request();
        identity.old_socket = "/nonexistent-adx-handoff/old.sock".into();
        identity.old_controller_session_id = routes.owner.clone();
        {
            let mut local = routes.local.lock().await;
            local.initialized = true;
            local.session = Some(pb::BindingState {
                proxy_session_id: identity.old_proxy_session_id.clone(),
                controller_session_id: routes.owner.clone(),
                sync_epoch: identity.old_sync_epoch,
                ready: true,
            });
        }
        identity.candidate_proxy_session_id.clear();
        assert!(routes.reserve_handoff(identity).await.is_err());
        assert!(routes.handoff.lock().await.is_none());
        routes.require_stable_topology().await.unwrap();
    }

    #[test]
    fn candidate_recovery_requires_exact_frozen_owner_epoch_and_session() {
        let task = Task::new(request());
        let mut state = pb::BindingState {
            proxy_session_id: "new".into(),
            sync_epoch: 1,
            controller_session_id: "controller".into(),
            ready: false,
        };
        assert!(task.matches_candidate(&state)); // Lost Begin reply can resume.
        state.ready = true;
        assert!(task.matches_candidate(&state)); // Lost Open reply must not reinstall.
        state.sync_epoch = 2;
        assert!(!task.matches_candidate(&state));
        state.sync_epoch = 1;
        state.controller_session_id = "other".into();
        assert!(!task.matches_candidate(&state));
        state.controller_session_id = "controller".into();
        state.proxy_session_id = "restarted".into();
        assert!(!task.matches_candidate(&state));
    }

    #[test]
    fn replace_retry_preserves_original_snapshot_and_only_journals_later_changes() {
        let mut task = Task::new(request());
        let mut local = Local::default();
        let original = UpdateBindingRequest {
            environment_id: "env".into(),
            ownership_generation: 1,
            binding_revision: 2,
            binding: Some(Binding::Active(RuntimeTarget {
                runtime_id: "runtime".into(),
                ip: "10.0.0.1".into(),
            })),
            ..Default::default()
        };
        local.bindings.insert("env".into(), original.clone());
        let snapshot = task.freeze_snapshot(&local).clone();
        assert!(task.candidate_pending.is_empty());
        let mut retired = original.clone();
        retired.binding_revision = 3;
        retired.binding = Some(Binding::Retired(Retired {}));
        task.queue(Some(&original), &retired);
        local.bindings.insert("env".into(), retired.clone());
        assert_eq!(task.freeze_snapshot(&local), &snapshot);
        assert_eq!(snapshot.bindings[0].binding_revision, 2);
        assert_eq!(task.candidate_pending["env"], retired);
    }

    #[tokio::test]
    async fn environment_flush_ignores_unrelated_pending_retirement() {
        let routes = UdsRoutes::new(
            "/nonexistent-adx-handoff/old.sock".into(),
            Duration::from_millis(10),
        )
        .unwrap();
        let mut task = Task::new(request());
        task.phase = Phase::Opening;
        let retired = UpdateBindingRequest {
            environment_id: "other".into(),
            ownership_generation: 1,
            binding_revision: 3,
            binding: Some(Binding::Retired(Retired {})),
            ..Default::default()
        };
        task.old_pending.insert("other".into(), retired.clone());
        task.candidate_pending
            .insert("other".into(), retired.clone());
        routes.flush_handoff(&mut task, Some("env")).await.unwrap();
        assert_eq!(task.old_pending["other"], retired);
        assert_eq!(task.candidate_pending["other"], retired);
    }

    #[tokio::test]
    async fn task_slot_keeps_idle_fenced_and_rejects_other_identity() {
        let routes = UdsRoutes::new("/run/old.sock".into(), Duration::from_secs(1)).unwrap();
        let request = request();
        *routes.handoff.lock().await = Some(Task::new(request.clone()));
        assert!(routes.require_stable_topology().await.is_err());
        let mut other = request.clone();
        other.candidate_proxy_session_id = "other".into();
        assert!(routes.switch_handoff(other).await.is_err());
    }

    #[test]
    fn pending_retirement_survives_same_version_retry_and_stale_ack() {
        let mut task = Task::new(request());
        task.phase = Phase::Opening;
        let active = UpdateBindingRequest {
            environment_id: "env".into(),
            ownership_generation: 1,
            binding_revision: 2,
            binding: Some(Binding::Active(RuntimeTarget {
                runtime_id: "a".into(),
                ip: "10.0.0.1".into(),
            })),
            ..Default::default()
        };
        let mut replacement = active.clone();
        replacement.binding_revision = 3;
        replacement.binding = Some(Binding::Active(RuntimeTarget {
            runtime_id: "b".into(),
            ip: "10.0.0.2".into(),
        }));
        task.queue(Some(&active), &replacement);
        let tombstone = task.old_pending["env"].clone();
        assert!(matches!(tombstone.binding, Some(Binding::Retired(_))));
        task.queue(Some(&replacement), &replacement);
        assert_eq!(task.old_pending["env"], tombstone);
        let mut suspended = replacement.clone();
        let Some(Binding::Active(target)) = replacement.binding.clone() else {
            panic!("active replacement")
        };
        suspended.binding = Some(Binding::Suspended(target));
        task.queue(Some(&replacement), &suspended);
        assert_eq!(task.old_pending["env"], tombstone);
        task.queue(Some(&suspended), &replacement);
        assert_eq!(task.old_pending["env"], tombstone);
        let mut retired = replacement.clone();
        retired.binding_revision = 4;
        retired.binding = Some(Binding::Retired(Retired {}));
        task.queue(Some(&replacement), &retired);
        Task::ack(&mut task.old_pending, &tombstone);
        assert!(task.old_pending.contains_key("env"));
        let latest = task.old_pending["env"].clone();
        Task::ack(&mut task.old_pending, &latest);
        assert!(task.old_pending.is_empty());
        assert_eq!(task.candidate_pending["env"], retired);
    }

    #[test]
    fn admission_only_latest_matching_open_ack_releases_blocker() {
        let mut local = Local::default();
        let key = ("env".to_string(), "runtime".to_string(), 1);
        local.admissions.insert(key.clone(), 2);
        local.admission_ack(&key, 1, false);
        assert!(local.admissions.contains_key(&key));
        local.admission_ack(&key, 2, true);
        assert!(local.admissions.contains_key(&key));
        local.admission_ack(&key, 2, false);
        assert!(local.admissions.is_empty());
    }

    #[tokio::test]
    async fn failed_retirement_keeps_canonical_and_both_pending_until_ack() {
        let routes = UdsRoutes::new(
            "/nonexistent-adx-handoff/old.sock".into(),
            Duration::from_millis(10),
        )
        .unwrap();
        let mut identity = request();
        identity.old_socket = "/nonexistent-adx-handoff/old.sock".into();
        identity.candidate_socket = "/nonexistent-adx-handoff/new.sock".into();
        let mut task = Task::new(identity);
        task.phase = Phase::ActiveFenced;
        *routes.handoff.lock().await = Some(task);
        let retired = UpdateBindingRequest {
            environment_id: "env".into(),
            ownership_generation: 1,
            binding_revision: 3,
            binding: Some(Binding::Retired(Retired {})),
            ..Default::default()
        };
        for _ in 0..2 {
            assert!(routes.send(retired.clone()).await.is_err());
            assert_eq!(routes.local.lock().await.bindings["env"], retired);
            let slot = routes.handoff.lock().await;
            let task = slot.as_ref().unwrap();
            assert_eq!(task.old_pending["env"], retired);
            assert_eq!(task.candidate_pending["env"], retired);
        }
    }
}
