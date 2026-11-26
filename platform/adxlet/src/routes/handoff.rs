//! Single in-memory handoff with acknowledged per-environment update journals.
//! Old dependencies and aggregate idle claims remain fenced pending Child evidence.
use super::*;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Beginning,
    ActiveFenced,
    Completed,
    Cancelled,
}
impl Phase {
    fn may_serve(self) -> bool {
        matches!(self, Self::ActiveFenced | Self::Completed)
    }
    fn name(self) -> &'static str {
        match self {
            Self::Beginning => "beginning",
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
    proof: Option<pb::RelayHandoffProof>,
    replacement: Option<(pb::RelayHandoffRequest, pb::RelayReplacement)>,
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
            proof: None,
            replacement: None,
        }
    }
    fn freeze_snapshot(&mut self, local: &mut Local) -> &pb::BindingSnapshot {
        if self.snapshot.is_none() {
            // Pre-snapshot changes are included in the exact frozen request.
            local.unconfirmed.clear();
        }
        let session = &self.identity.candidate_proxy_session_id;
        let epoch = 1; // Fresh candidate's first BeginBindings, including a replacement.
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
    fn accept_proof(&mut self, proof: pb::RelayHandoffProof) -> Result<()> {
        let task = self;
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
            || !(proof.listeners_released || proof.old_exited)
            || task.proof.as_ref().is_some_and(|old| {
                old.old_pid != proof.old_pid
                    || (old.old_exited && !proof.old_exited)
                    || (old.listeners_released && !proof.listeners_released)
            })
        {
            return Err(Error::Conflict);
        }
        if proof.old_exited {
            task.old_pending.clear();
        }
        task.proof = Some(proof);
        Ok(())
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
    pub async fn finish_handoff(
        &self,
        proof: pb::RelayHandoffProof,
    ) -> Result<pb::RelayHandoffState> {
        let _topology = self.topology.write().await;
        let mut slot = self.handoff.lock().await;
        let task = slot.as_mut().ok_or(Error::NotFound)?;
        task.accept_proof(proof.clone())?;
        if task.phase == Phase::Completed {
            return Ok(task.state());
        }
        if task.phase != Phase::ActiveFenced
            || !proof.old_exited
            || !self.local.lock().await.unconfirmed.is_empty()
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
        let old_exited = task.proof.as_ref().is_some_and(|proof| proof.old_exited);
        let candidate_may_serve = task.phase.may_serve() || task.snapshot.is_some();
        let mut local = self.local.lock().await;
        let pending = |rows: &BTreeMap<String, UpdateBindingRequest>| match environment {
            Some(id) => rows.contains_key(id),
            None => !rows.is_empty(),
        };
        let (old_result, candidate_result) = tokio::join!(
            async {
                if old_exited {
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
            tokio::time::timeout_at(deadline, async {
                if !pending(&local.unconfirmed) {
                    return Ok(());
                }
                if candidate_may_serve {
                    let mut client = Self::connect_at(
                        PathBuf::from(&task.identity.candidate_socket),
                        self.timeout,
                    )
                    .await?;
                    let state = self.remote(&mut client).await?;
                    if state != candidate {
                        return Err(unavailable("candidate full synchronization pending"));
                    }
                    self.flush_endpoint(
                        &task.identity.candidate_socket,
                        &candidate,
                        &mut local.unconfirmed,
                        environment,
                        deadline,
                    )
                    .await
                } else {
                    Ok(())
                }
            })
        );
        // Join both endpoints before propagating either failure: both journals
        // must retain actual ACK progress even when the other side times out.
        old_result?;
        candidate_result.map_err(|_| unavailable("handoff flush budget exhausted"))??;
        if pending(&task.old_pending) || pending(&local.unconfirmed) {
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
                .unconfirmed
                .insert(next.environment_id.clone(), next.clone());
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
        request: pb::RelaySwitchRequest,
    ) -> Result<pb::RelayHandoffState> {
        let proof = request.proof.ok_or(Error::Conflict)?;
        let identity = proof.identity.as_ref().ok_or(Error::Conflict)?.clone();
        let _topology = self.topology.write().await;
        let mut slot = self.handoff.lock().await;
        let task = slot.as_mut().ok_or(Error::NotFound)?;
        if task.phase == Phase::Cancelled {
            return Err(Error::Conflict);
        }
        if let Some(replacement) = request.replacement {
            if let Some(previous) = &task.replacement {
                if previous != &(identity, replacement)
                    || task
                        .proof
                        .as_ref()
                        .is_none_or(|old| old.old_pid != proof.old_pid)
                {
                    return Err(Error::Conflict);
                }
                // Replay is a historical receipt, not new Child liveness evidence.
                return Ok(task.state());
            }
            task.accept_proof(proof)?;
            let path = PathBuf::from(&replacement.replacement_socket);
            if replacement.failed_pid == 0
                || !replacement.failed_exited
                || !path.is_absolute()
                || path.as_os_str().len() > 100
                || replacement.replacement_socket == task.identity.old_socket
                || replacement.replacement_socket == task.identity.candidate_socket
            {
                return Err(Error::Conflict);
            }
            // The reaped Child can never own the replacement identity.
            task.identity.candidate_socket = replacement.replacement_socket.clone();
            task.identity.candidate_proxy_session_id.clear();
            if let Some(proof) = task.proof.as_mut() {
                proof.identity = Some(task.identity.clone());
            }
            task.phase = Phase::Beginning;
            task.snapshot = None;
            task.replacement = Some((identity, replacement));
            self.local.lock().await.unconfirmed.clear();
            self.activity.disconnect();
            return Ok(task.state());
        }
        task.accept_proof(proof)?;
        if task.identity.candidate_proxy_session_id.is_empty() {
            if identity.candidate_proxy_session_id.is_empty() {
                return Err(Error::Conflict);
            }
            task.identity.candidate_proxy_session_id = identity.candidate_proxy_session_id.clone();
        } else if task.identity != identity {
            return Err(Error::Conflict);
        }
        if task.phase == Phase::Completed {
            return Ok(task.state());
        }
        let mut candidate =
            Self::connect_at(PathBuf::from(&identity.candidate_socket), self.timeout).await?;
        let mut state = self.remote(&mut candidate).await?;
        if !task.matches_candidate(&state) {
            if state.proxy_session_id != identity.candidate_proxy_session_id
                || state.ready
                || state.sync_epoch != 0
                || !state.controller_session_id.is_empty()
                || task.snapshot.is_some()
                || task.phase.may_serve()
            {
                return Err(Error::Conflict);
            }
            state.controller_session_id = identity.old_controller_session_id.clone();
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
            // An uncertain Replace may already have opened this exact identity.
            if task.snapshot.is_none() && !task.phase.may_serve() {
                return Err(Error::Conflict);
            }
        } else {
            if task.phase.may_serve() {
                return Err(Error::Conflict);
            }
            let mut local = self.local.lock().await;
            let snapshot = task.freeze_snapshot(&mut local).clone();
            drop(local);
            state = candidate
                .replace_bindings(self.request(snapshot))
                .await
                .map_err(rpc_error)?
                .into_inner();
            if !task.matches_candidate(&state) || !state.ready {
                return Err(Error::Conflict);
            }
        }
        // A ready candidate receives only latest canonical deltas, never another full install.
        self.flush_handoff(task, None).await?;
        self.local.lock().await.session = Some(state);
        self.path
            .send_replace(PathBuf::from(&identity.candidate_socket));
        self.activity.disconnect();
        task.snapshot = None;
        task.phase = Phase::ActiveFenced;
        Ok(task.state())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct RelayEndpoint {
        socket: String,
        state: pb::BindingState,
        server: tokio::task::JoinHandle<()>,
    }

    impl RelayEndpoint {
        async fn new(directory: &std::path::Path, name: &str, controller: &str) -> Self {
            use data_plane_gateway::node::route_control::{proto as relay_pb, BindingService};
            use data_plane_gateway::{common::protocol::GatewayPolicy, node::Relay};
            use tokio_stream::wrappers::UnixListenerStream;

            let service = Arc::new(BindingService::new(Arc::new(
                Relay::new(GatewayPolicy::new(vec![])).with_route_enforcement(),
            )));
            let mut state = service.inspect().await;
            state.controller_session_id = controller.into();
            let state = service.begin(state).await.unwrap();
            service
                .replace(relay_pb::BindingSnapshot {
                    proxy_session_id: state.proxy_session_id.clone(),
                    sync_epoch: state.sync_epoch,
                    bindings: vec![],
                })
                .await
                .unwrap();
            let socket = directory.join(name);
            let listener = tokio::net::UnixListener::bind(&socket).unwrap();
            let server = tokio::spawn(async move {
                tonic::transport::Server::builder()
                    .add_service(
                        relay_pb::relay_service_server::RelayServiceServer::from_arc(service),
                    )
                    .serve_with_incoming(UnixListenerStream::new(listener))
                    .await
                    .unwrap();
            });
            Self {
                socket: socket.to_str().unwrap().into(),
                state: pb::BindingState {
                    proxy_session_id: state.proxy_session_id,
                    controller_session_id: state.controller_session_id,
                    sync_epoch: state.sync_epoch,
                    ready: true,
                },
                server,
            }
        }
    }

    impl Drop for RelayEndpoint {
        fn drop(&mut self) {
            self.server.abort();
        }
    }

    struct HandoffEndpoints {
        routes: UdsRoutes,
        old: RelayEndpoint,
        _candidate: RelayEndpoint,
        _directory: tempfile::TempDir,
    }

    impl HandoffEndpoints {
        async fn new(initial: &UpdateBindingRequest) -> Self {
            let directory = tempfile::tempdir().unwrap();
            let mut identity = request();
            let old = RelayEndpoint::new(
                directory.path(),
                "old.sock",
                &identity.old_controller_session_id,
            )
            .await;
            let candidate = RelayEndpoint::new(
                directory.path(),
                "new.sock",
                &identity.old_controller_session_id,
            )
            .await;
            identity.old_socket = old.socket.clone();
            identity.old_proxy_session_id = old.state.proxy_session_id.clone();
            identity.old_sync_epoch = old.state.sync_epoch;
            identity.candidate_socket = candidate.socket.clone();
            identity.candidate_proxy_session_id = candidate.state.proxy_session_id.clone();
            assert_eq!(candidate.state.sync_epoch, 1);
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
            task.freeze_snapshot(&mut *routes.local.lock().await);
            for endpoint in [&old, &candidate] {
                routes
                    .update_at(&endpoint.socket, &endpoint.state, initial.clone())
                    .await
                    .unwrap();
            }
            *routes.handoff.lock().await = Some(task);
            Self {
                routes,
                _directory: directory,
                old,
                _candidate: candidate,
            }
        }

        async fn old_update(&self, binding: UpdateBindingRequest) -> Result<()> {
            self.routes
                .update_at(&self.old.socket, &self.old.state, binding)
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
                self.routes.local.lock().await.unconfirmed.is_empty(),
                "real candidate Relay ACK is required"
            );
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
            listeners_released: true,
            old_exited: false,
        };
        let replacement = pb::RelaySwitchRequest {
            proof: Some(proof.clone()),
            replacement: Some(pb::RelayReplacement {
                failed_pid: 42,
                failed_exited: true,
                replacement_socket: "/run/rollback.sock".into(),
            }),
        };
        let first = routes.switch_handoff(replacement.clone()).await.unwrap();
        let mut replacement_identity = identity.clone();
        replacement_identity.candidate_socket = "/run/rollback.sock".into();
        replacement_identity.candidate_proxy_session_id.clear();
        assert_eq!(first.identity.as_ref(), Some(&replacement_identity));
        let mut wrong_pid = replacement.clone();
        wrong_pid.proof.as_mut().unwrap().old_pid += 1;
        assert!(matches!(
            routes.switch_handoff(wrong_pid).await,
            Err(Error::Conflict)
        ));
        assert_eq!(
            routes.switch_handoff(replacement.clone()).await.unwrap(),
            first
        );
        let mut replacement_proof = proof;
        replacement_proof.identity = Some(replacement_identity.clone());
        assert_eq!(
            routes.handoff.lock().await.as_ref().unwrap().proof.as_ref(),
            Some(&replacement_proof)
        );
        replacement_proof.old_exited = true;
        routes
            .handoff
            .lock()
            .await
            .as_mut()
            .unwrap()
            .accept_proof(replacement_proof.clone())
            .unwrap();
        assert_eq!(routes.switch_handoff(replacement).await.unwrap(), first);
        assert!(matches!(
            routes.switch_handoff(switch_request(identity)).await,
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
    async fn reaped_old_relay_without_begin_reply_is_task_exact_and_finish_is_replayable() {
        let routes = UdsRoutes::new("/run/old.sock".into(), Duration::from_secs(1)).unwrap();
        let identity = request();
        let mut task = Task::new(identity.clone());
        task.phase = Phase::ActiveFenced;
        *routes.handoff.lock().await = Some(task);
        let mut proof = pb::RelayHandoffProof {
            identity: Some(identity.clone()),
            old_pid: 42,
            listeners_released: false,
            old_exited: true,
        };
        proof.identity.as_mut().unwrap().task_id = "wrong".into();
        assert!(routes.finish_handoff(proof.clone()).await.is_err());
        proof.identity = Some(identity.clone());
        assert_eq!(
            routes.finish_handoff(proof.clone()).await.unwrap().phase,
            "completed"
        );
        let recorded = routes
            .handoff
            .lock()
            .await
            .as_ref()
            .unwrap()
            .proof
            .clone()
            .unwrap();
        assert!(recorded.old_exited);
        assert!(!recorded.listeners_released);
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
            let live = RelayEndpoint::new(directory.path(), "live.sock", "controller").await;
            let state = &live.state;
            let routes =
                UdsRoutes::new(live.socket.clone().into(), Duration::from_secs(2)).unwrap();
            let mut identity = request();
            let live_socket = live.socket.clone();
            let stalled = stalled_path.to_str().unwrap().to_string();
            if stall_old {
                identity.old_socket = stalled;
                identity.candidate_socket = live_socket;
                identity.candidate_proxy_session_id = state.proxy_session_id.clone();
            } else {
                identity.old_socket = live_socket;
                identity.old_proxy_session_id = state.proxy_session_id.clone();
                identity.old_sync_epoch = state.sync_epoch;
                identity.candidate_socket = stalled;
            }
            let mut task = Task::new(identity);
            task.phase = Phase::ActiveFenced;
            let retired = UpdateBindingRequest {
                environment_id: "env".into(),
                ownership_generation: 1,
                binding_revision: 3,
                binding: Some(Binding::Retired(Retired {})),
                ..Default::default()
            };
            task.old_pending.insert("env".into(), retired.clone());
            routes
                .local
                .lock()
                .await
                .unconfirmed
                .insert("env".into(), retired.clone());
            assert!(routes.flush_handoff(&mut task, None).await.is_err());
            if stall_old {
                assert_eq!(task.old_pending["env"], retired);
                assert!(routes.local.lock().await.unconfirmed.is_empty());
            } else {
                assert!(task.old_pending.is_empty());
                assert_eq!(routes.local.lock().await.unconfirmed["env"], retired);
            }
            stalled_server.abort();
        }
    }

    fn switch_request(identity: pb::RelayHandoffRequest) -> pb::RelaySwitchRequest {
        pb::RelaySwitchRequest {
            proof: Some(pb::RelayHandoffProof {
                identity: Some(identity),
                old_pid: 42,
                listeners_released: true,
                old_exited: false,
            }),
            replacement: None,
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
        assert!(task.matches_candidate(&state)); // Lost BeginBindings reply can resume.
        state.ready = true;
        assert!(task.matches_candidate(&state)); // Lost Replace reply must not reinstall.
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
        let snapshot = task.freeze_snapshot(&mut local).clone();
        assert!(local.unconfirmed.is_empty());
        let mut retired = original.clone();
        retired.binding_revision = 3;
        retired.binding = Some(Binding::Retired(Retired {}));
        task.queue(Some(&original), &retired);
        local.bindings.insert("env".into(), retired.clone());
        local.unconfirmed.insert("env".into(), retired.clone());
        assert_eq!(task.freeze_snapshot(&mut local), &snapshot);
        assert_eq!(snapshot.bindings[0].binding_revision, 2);
        assert_eq!(local.unconfirmed["env"], retired);
    }

    #[tokio::test]
    async fn environment_flush_ignores_unrelated_pending_retirement() {
        let routes = UdsRoutes::new(
            "/nonexistent-adx-handoff/old.sock".into(),
            Duration::from_millis(10),
        )
        .unwrap();
        let mut task = Task::new(request());
        task.phase = Phase::ActiveFenced;
        let retired = UpdateBindingRequest {
            environment_id: "other".into(),
            ownership_generation: 1,
            binding_revision: 3,
            binding: Some(Binding::Retired(Retired {})),
            ..Default::default()
        };
        task.old_pending.insert("other".into(), retired.clone());
        routes
            .local
            .lock()
            .await
            .unconfirmed
            .insert("other".into(), retired.clone());
        routes.flush_handoff(&mut task, Some("env")).await.unwrap();
        assert_eq!(task.old_pending["other"], retired);
        assert_eq!(routes.local.lock().await.unconfirmed["other"], retired);
    }

    #[tokio::test]
    async fn task_slot_keeps_idle_fenced_and_rejects_other_identity() {
        let routes = UdsRoutes::new("/run/old.sock".into(), Duration::from_secs(1)).unwrap();
        let request = request();
        *routes.handoff.lock().await = Some(Task::new(request.clone()));
        assert!(routes.require_stable_topology().await.is_err());
        let mut other = request.clone();
        other.candidate_proxy_session_id = "other".into();
        assert!(routes.switch_handoff(switch_request(other)).await.is_err());
    }

    #[test]
    fn pending_retirement_survives_same_version_retry_and_stale_ack() {
        let mut task = Task::new(request());
        task.phase = Phase::ActiveFenced;
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
            assert_eq!(routes.local.lock().await.unconfirmed["env"], retired);
        }
    }
}
