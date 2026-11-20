//! Versioned bindings and complete startup synchronization over a protected UDS.
use crate::Routes;
mod activity;
mod handoff;
use adx_core::{EnvironmentRecord, Error, Result};
use adx_protocol::relay::{
    self as pb, relay_service_client::RelayServiceClient, update_binding_request::Binding, Retired,
    RuntimeTarget, UpdateBindingRequest,
};
use async_trait::async_trait;
use hyper_util::rt::TokioIo;
use std::{collections::BTreeMap, path::PathBuf, sync::Arc, time::Duration};
use tokio::{net::UnixStream, sync::Mutex};
use tonic::transport::{Channel, Endpoint};
use tower::service_fn;
#[derive(Default)]
struct Local {
    session: Option<pb::BindingState>,
    buffering: bool,
    initialized: bool,
    bindings: BTreeMap<String, UpdateBindingRequest>,
    unconfirmed: BTreeMap<String, UpdateBindingRequest>,
    confirmed_full: Option<(String, u64)>,
    admission_revision: u64,
    admissions: BTreeMap<(String, String, u64), u64>,
    process_takeover: Option<pb::BindingTakeoverRequest>,
}
impl Local {
    fn forget_completed_takeover(&mut self, remote: &pb::BindingState) {
        if self.initialized
            && self
                .process_takeover
                .as_ref()
                .and_then(|request| request.expected.as_ref())
                .is_some_and(|old| old.proxy_session_id != remote.proxy_session_id)
        {
            self.process_takeover = None;
        }
    }
    fn preflight_catalog_ready(&self) -> bool {
        self.initialized && !self.buffering && self.unconfirmed.is_empty()
    }
    fn confirmed_relay_ready(&self, state: &pb::BindingState, expected: &str, owner: &str) -> bool {
        self.preflight_catalog_ready()
            && state.ready
            && state.proxy_session_id == expected
            && state.controller_session_id == owner
            && self.session.as_ref() == Some(state)
            && self.confirmed_full.as_ref()
                == Some(&(state.proxy_session_id.clone(), state.sync_epoch))
    }
    fn binding_ack(&mut self, sent: &UpdateBindingRequest) {
        if self.unconfirmed.get(&sent.environment_id) == Some(sent) {
            self.unconfirmed.remove(&sent.environment_id);
        }
    }
    fn full_ack(&mut self, accepted: &pb::BindingState) {
        let epoch = (accepted.proxy_session_id.clone(), accepted.sync_epoch);
        // Only confirmed full synchronization in a new epoch resets admission.
        if self
            .confirmed_full
            .as_ref()
            .is_some_and(|old| old != &epoch)
        {
            self.admissions.clear();
        }
        self.confirmed_full = Some(epoch);
        self.unconfirmed.clear();
    }
    fn admission_ack(&mut self, key: &(String, String, u64), revision: u64, closed: bool) {
        if !closed && self.admissions.get(key) == Some(&revision) {
            self.admissions.remove(key);
        }
    }
}
#[derive(Clone)]
pub struct UdsRoutes {
    path: tokio::sync::watch::Sender<PathBuf>,
    topology: Arc<tokio::sync::RwLock<()>>,
    handoff: Arc<Mutex<Option<handoff::Task>>>,
    timeout: Duration,
    local: Arc<Mutex<Local>>,
    sync: Arc<Mutex<()>>,
    owner: String,
    activity: Arc<activity::ActivityHub>,
}
impl UdsRoutes {
    /// Observe the actual protected UDS identity without Begin or catalog mutation.
    pub async fn inspect_process_relay(&self) -> Result<pb::BindingState> {
        let mut client = self.connect().await?;
        self.remote(&mut client).await
    }
    /// Freeze the actual Relay predecessor once and replay only that payload.
    pub async fn takeover_bindings(
        &self,
        node_id: &str,
        records: &[EnvironmentRecord],
    ) -> Result<()> {
        let _topology = self.topology.write().await;
        self.require_stable_topology().await?;
        let _sync = self.sync.lock().await;
        let mut local = self.local.lock().await;
        let predecessor = match &local.process_takeover {
            Some(request) => request.expected.clone().ok_or(Error::Conflict)?,
            None => self.inspect_process_relay().await?,
        };
        let epoch = predecessor
            .sync_epoch
            .checked_add(1)
            .ok_or(Error::Conflict)?;
        let mut bindings = BTreeMap::new();
        for record in records {
            if record.assignment.node_id != node_id
                || !matches!(
                    record.state,
                    adx_core::EnvironmentState::Running | adx_core::EnvironmentState::Paused
                )
                || record.restart_pending
            {
                return Err(Error::Conflict);
            }
            let active = record.state == adx_core::EnvironmentState::Running;
            let revision = record
                .revision
                .checked_mul(2)
                .and_then(|v| v.checked_add(u64::from(!active)))
                .filter(|v| *v != 0)
                .ok_or(Error::Conflict)?;
            let binding = UpdateBindingRequest {
                proxy_session_id: predecessor.proxy_session_id.clone(),
                sync_epoch: epoch,
                environment_id: record.spec.id.clone(),
                ownership_generation: record.assignment.generation,
                binding_revision: revision,
                binding: Some(if active {
                    Binding::Active(RuntimeTarget {
                        runtime_id: record.runtime.id.clone(),
                        ip: record.runtime.ip.ok_or(Error::Conflict)?.to_string(),
                    })
                } else {
                    Binding::Retired(Retired {})
                }),
            };
            if bindings.insert(record.spec.id.clone(), binding).is_some() {
                return Err(Error::Conflict);
            }
        }
        let request = pb::BindingTakeoverRequest {
            expected: Some(predecessor.clone()),
            new_controller_session_id: self.owner.clone(),
            snapshot: Some(pb::BindingSnapshot {
                proxy_session_id: predecessor.proxy_session_id.clone(),
                sync_epoch: epoch,
                bindings: bindings.values().cloned().collect(),
            }),
        };
        if let Some(previous) = &local.process_takeover {
            if previous != &request {
                return Err(Error::Conflict);
            }
        } else {
            if local.initialized || local.buffering || !local.admissions.is_empty() {
                return Err(Error::Conflict);
            }
            // Fix before sending: ambiguous replies may only retry this payload.
            local.process_takeover = Some(request.clone());
        }
        let path = self.path.borrow().clone();
        let channel = Self::channel_at(path, self.timeout).await?;
        let mut client = pb::relay_upgrade_service_client::RelayUpgradeServiceClient::new(channel)
            .max_encoding_message_size(64 * 1024 * 1024)
            .max_decoding_message_size(64 * 1024 * 1024);
        let accepted = client
            .takeover_bindings(self.request(request))
            .await
            .map_err(rpc_error)?
            .into_inner();
        if accepted.proxy_session_id != predecessor.proxy_session_id
            || accepted.sync_epoch != epoch
            || accepted.controller_session_id != self.owner
            || !accepted.ready
        {
            return Err(Error::Conflict);
        }
        // Receipt replay must not reinstall an old local catalog after updates.
        if !local.initialized {
            local.bindings = bindings;
            local.full_ack(&accepted);
            local.session = Some(accepted);
            local.initialized = true;
        }
        Ok(())
    }

    pub(crate) async fn process_preflight(&self, expected: &str) -> Result<()> {
        let _topology = self.topology.read().await;
        self.require_stable_topology().await?;
        if expected.is_empty() {
            return Err(Error::Conflict);
        }
        let state = self
            .connect()
            .await?
            .get_binding_state(self.request(pb::GetBindingStateRequest {}))
            .await
            .map_err(unavailable)?
            .into_inner();
        let local = self.local.lock().await;
        if !local.confirmed_relay_ready(&state, expected, &self.owner) {
            return Err(Error::Conflict);
        }
        Ok(())
    }
    pub fn new(path: PathBuf, timeout: Duration) -> Result<Self> {
        if path.as_os_str().is_empty() || timeout.is_zero() {
            return Err(Error::Invalid(
                "route socket and positive timeout required".into(),
            ));
        }
        Ok(Self {
            path: tokio::sync::watch::channel(path).0,
            topology: Arc::default(),
            handoff: Arc::default(),
            timeout,
            local: Arc::default(),
            sync: Arc::default(),
            owner: uuid::Uuid::new_v4().to_string(),
            activity: Arc::default(),
        })
    }
    async fn connect(&self) -> Result<RelayServiceClient<Channel>> {
        let path = self.path.borrow().clone();
        Self::connect_at(path, self.timeout).await
    }
    async fn connect_at(path: PathBuf, timeout: Duration) -> Result<RelayServiceClient<Channel>> {
        Ok(
            RelayServiceClient::new(Self::channel_at(path, timeout).await?)
                .max_encoding_message_size(64 * 1024 * 1024)
                .max_decoding_message_size(64 * 1024 * 1024),
        )
    }
    async fn channel_at(path: PathBuf, timeout: Duration) -> Result<Channel> {
        let channel = tokio::time::timeout(
            timeout,
            Endpoint::from_static("http://relay")
                .http2_keep_alive_interval(Duration::from_secs(30))
                .keep_alive_timeout(Duration::from_secs(10))
                .keep_alive_while_idle(true)
                .connect_with_connector(service_fn(move |_| {
                    let path = path.clone();
                    async move { UnixStream::connect(path).await.map(TokioIo::new) }
                })),
        )
        .await
        .map_err(|_| unavailable("connection deadline"))?
        .map_err(unavailable)?;
        Ok(channel)
    }
    fn request<T>(&self, message: T) -> tonic::Request<T> {
        let mut r = tonic::Request::new(message);
        r.set_timeout(self.timeout);
        adx_observability::trace::inject_metadata(r.metadata_mut());
        r
    }
    async fn set_admission(
        &self,
        record: &EnvironmentRecord,
        close: bool,
        drain: bool,
        idle: Option<(&str, u64)>,
    ) -> Result<(pb::BindingState, bool)> {
        let _topology = self.topology.read().await;
        self.require_stable_topology().await?;
        let mut client = self.connect().await?;
        let state = self.remote(&mut client).await?;
        if idle.is_some_and(|(session, _)| session != state.proxy_session_id) {
            return Err(Error::Conflict);
        }
        let revision = {
            let mut local = self.local.lock().await;
            if !state.ready
                || state.controller_session_id != self.owner
                || !local.initialized
                || local.buffering
                || !local.bindings.get(&record.spec.id).is_some_and(|binding| {
                    binding.ownership_generation == record.assignment.generation
                        && matches!(&binding.binding, Some(Binding::Active(target))
                        if target.runtime_id == record.runtime.id)
                })
                || local.session.as_ref().is_none_or(|known| {
                    known.proxy_session_id != state.proxy_session_id
                        || known.sync_epoch != state.sync_epoch
                })
            {
                return Err(Error::Conflict);
            }
            local.admission_revision = local
                .admission_revision
                .checked_add(1)
                .ok_or(Error::Conflict)?;
            let revision = local.admission_revision;
            local.admissions.insert(
                (
                    record.spec.id.clone(),
                    record.runtime.id.clone(),
                    record.assignment.generation,
                ),
                revision,
            );
            revision
        };
        let response = client
            .set_environment_admission(self.request(pb::EnvironmentAdmissionRequest {
                environment_id: record.spec.id.clone(),
                runtime_id: record.runtime.id.clone(),
                proxy_session_id: state.proxy_session_id.clone(),
                sync_epoch: state.sync_epoch,
                admission_revision: revision,
                activity_revision: idle.map_or(0, |(_, revision)| revision),
                close,
                drain,
            }))
            .await
            .map_err(rpc_error)?
            .into_inner();
        self.local.lock().await.admission_ack(
            &(
                record.spec.id.clone(),
                record.runtime.id.clone(),
                record.assignment.generation,
            ),
            revision,
            response.closed,
        );
        Ok((state, response.closed))
    }
    async fn environment_activity(
        &self,
        record: &EnvironmentRecord,
    ) -> Result<pb::EnvironmentActivityState> {
        let _topology = self.topology.read().await;
        self.require_stable_topology().await?;
        Ok(self
            .connect()
            .await?
            .get_environment_activity(self.request(pb::GetEnvironmentActivityRequest {
                environment_id: record.spec.id.clone(),
                runtime_id: record.runtime.id.clone(),
            }))
            .await
            .map_err(rpc_error)?
            .into_inner())
    }
    async fn remote(&self, client: &mut RelayServiceClient<Channel>) -> Result<pb::BindingState> {
        Ok(client
            .get_binding_state(self.request(pb::GetBindingStateRequest {}))
            .await
            .map_err(rpc_error)?
            .into_inner())
    }
    async fn full(&self, s: &mut Local, client: &mut RelayServiceClient<Channel>) -> Result<()> {
        let session = s.session.clone().ok_or(Error::Conflict)?;
        let bindings = s
            .bindings
            .values()
            .cloned()
            .map(|mut r| {
                r.proxy_session_id = session.proxy_session_id.clone();
                r.sync_epoch = session.sync_epoch;
                r
            })
            .collect();
        let accepted = client
            .replace_bindings(self.request(pb::BindingSnapshot {
                proxy_session_id: session.proxy_session_id.clone(),
                sync_epoch: session.sync_epoch,
                bindings,
            }))
            .await
            .map_err(rpc_error)?
            .into_inner();
        if accepted.proxy_session_id != session.proxy_session_id
            || accepted.sync_epoch != session.sync_epoch
            || accepted.controller_session_id != self.owner
            || !accepted.ready
        {
            return Err(Error::Conflict);
        }
        s.full_ack(&accepted);
        s.session = Some(accepted);
        Ok(())
    }
    async fn ensure(&self, s: &mut Local, client: &mut RelayServiceClient<Channel>) -> Result<()> {
        if !s.initialized || s.buffering {
            return Err(unavailable("complete local catalog required"));
        }
        let mut remote = self.remote(client).await?;
        s.forget_completed_takeover(&remote);
        if s.process_takeover.is_some()
            && s.session
                .as_ref()
                .is_some_and(|old| old.proxy_session_id == remote.proxy_session_id)
        {
            if s.session.as_ref() != Some(&remote)
                || remote.controller_session_id != self.owner
                || !remote.ready
            {
                return Err(Error::Conflict);
            }
            return Ok(());
        }
        if let Some(old) = &s.session {
            if remote.proxy_session_id == old.proxy_session_id {
                if remote.sync_epoch != old.sync_epoch && remote.controller_session_id != self.owner
                {
                    return Err(Error::Conflict);
                }
                s.session = Some(remote.clone());
                if remote.ready {
                    return Ok(());
                }
                return self.full(s, client).await;
            }
        }
        remote.controller_session_id = self.owner.clone();
        s.session = Some(
            client
                .begin_bindings(self.request(remote))
                .await
                .map_err(rpc_error)?
                .into_inner(),
        );
        self.full(s, client).await
    }
    async fn update(&self, mut r: UpdateBindingRequest, session: &pb::BindingState) -> Result<()> {
        r.proxy_session_id = session.proxy_session_id.clone();
        r.sync_epoch = session.sync_epoch;
        let expected = (r.ownership_generation, r.binding_revision);
        let accepted = self
            .connect()
            .await?
            .update_binding(self.request(r))
            .await
            .map_err(rpc_error)?
            .into_inner();
        if (accepted.ownership_generation, accepted.binding_revision) != expected {
            return Err(Error::Conflict);
        }
        Ok(())
    }
    async fn send(&self, mut r: UpdateBindingRequest) -> Result<()> {
        let _topology = self.topology.read().await;
        // Only handoff traffic serializes on the task slot; steady-state updates
        // retain the existing independent-environment concurrency.
        let mut slot = self.handoff.lock().await;
        if let Some(task) = slot.as_mut() {
            if !task.is_completed() {
                return self.send_handoff(task, r).await;
            }
        }
        drop(slot);
        let session = {
            let mut s = self.local.lock().await;
            if let Some(old) = s.bindings.get(&r.environment_id) {
                if matches!(r.binding, Some(Binding::Retired(_)))
                    && (old.ownership_generation > r.ownership_generation
                        || (old.ownership_generation == r.ownership_generation
                            && old.binding_revision > r.binding_revision
                            && matches!(old.binding, Some(Binding::Retired(_)))))
                {
                    // Confirm the superseding fence with Relay before allowing
                    // old execution cleanup; never replace its newer binding.
                    r = old.clone();
                }
                let a = (old.ownership_generation, old.binding_revision);
                let b = (r.ownership_generation, r.binding_revision);
                let availability = matches!((&old.binding, &r.binding),
                    (Some(Binding::Active(a)), Some(Binding::Suspended(b)))
                    | (Some(Binding::Suspended(a)), Some(Binding::Active(b))) if a == b);
                if b < a || (b == a && old.binding != r.binding && !availability) {
                    return Err(Error::Conflict);
                }
            }
            if !s.initialized && !s.buffering {
                return Err(unavailable("binding catalog not initialized"));
            }
            s.bindings.insert(r.environment_id.clone(), r.clone());
            if s.buffering {
                return Ok(());
            }
            s.unconfirmed.insert(r.environment_id.clone(), r.clone());
            s.session.clone().ok_or(Error::Conflict)?
        };
        match self.update(r.clone(), &session).await {
            Ok(()) => {
                self.local.lock().await.binding_ack(&r);
                self.retired_admission(&r).await;
                return Ok(());
            }
            Err(Error::Conflict) => (),
            Err(error) => return Err(error),
        }

        // A proxy restart invalidates the cached session. Only this recovery path
        // serializes: established-session updates for independent environments stay
        // concurrent. Recheck the catalog so a delayed activation cannot overwrite a
        // newer retirement while it was in flight.
        let _sync = self.sync.lock().await;
        let session = {
            let mut s = self.local.lock().await;
            if s.bindings.get(&r.environment_id) != Some(&r) {
                return Err(Error::Conflict);
            }
            let mut client = self.connect().await?;
            self.ensure(&mut s, &mut client).await?;
            s.session.clone().ok_or(Error::Conflict)?
        };
        self.update(r.clone(), &session).await?;
        self.local.lock().await.binding_ack(&r);
        self.retired_admission(&r).await;
        Ok(())
    }
    async fn retired_admission(&self, r: &UpdateBindingRequest) {
        if matches!(r.binding, Some(Binding::Retired(_))) {
            let mut local = self.local.lock().await;
            if local.bindings.get(&r.environment_id) == Some(r) {
                local
                    .admissions
                    .retain(|(id, _, _), _| id != &r.environment_id);
            }
        }
    }
    async fn apply(&self, r: &EnvironmentRecord, active: bool) -> Result<()> {
        let revision = r
            .revision
            .checked_mul(2)
            .and_then(|v| v.checked_add(u64::from(!active)))
            .ok_or(Error::Conflict)?;
        if revision == 0 || r.assignment.generation == 0 || r.spec.id.is_empty() {
            return Err(Error::Invalid("versioned binding required".into()));
        }
        let binding = if active {
            Binding::Active(RuntimeTarget {
                runtime_id: r.runtime.id.clone(),
                ip: r
                    .runtime
                    .ip
                    .ok_or_else(|| Error::Invalid("runtime IP required".into()))?
                    .to_string(),
            })
        } else {
            Binding::Retired(Retired {})
        };
        self.send(UpdateBindingRequest {
            environment_id: r.spec.id.clone(),
            ownership_generation: r.assignment.generation,
            binding_revision: revision,
            binding: Some(binding),
            ..Default::default()
        })
        .await
    }
}
fn unavailable(e: impl std::fmt::Display) -> Error {
    Error::Unavailable(format!("Relay: {e}"))
}
fn rpc_error(s: tonic::Status) -> Error {
    match s.code() {
        tonic::Code::Unavailable
        | tonic::Code::DeadlineExceeded
        | tonic::Code::Cancelled
        | tonic::Code::Unknown => unavailable(s),
        _ => Error::Conflict,
    }
}
#[async_trait]
impl Routes for UdsRoutes {
    async fn drain(&self, record: &EnvironmentRecord) -> Result<()> {
        let (state, closed) = self.set_admission(record, true, true, None).await?;
        if !closed {
            return Err(unavailable("proxy did not close stream admission"));
        }
        loop {
            // Read the authoritative counter after the close acknowledgement;
            // a cached pre-close idle snapshot is not a drain barrier.
            let activity = self.environment_activity(record).await?;
            if activity.proxy_session_id != state.proxy_session_id
                || activity.sync_epoch != state.sync_epoch
            {
                return Err(Error::Conflict);
            }
            if activity.active_streams == 0 {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    async fn quiesce(
        &self,
        record: &EnvironmentRecord,
        session: &str,
        revision: u64,
    ) -> Result<bool> {
        self.set_admission(record, true, false, Some((session, revision)))
            .await
            .map(|(_, closed)| closed)
    }
    async fn reopen(&self, record: &EnvironmentRecord) -> Result<()> {
        self.set_admission(record, false, false, None)
            .await
            .map(|_| ())
    }
    async fn activity_changed(
        &self,
        record: &EnvironmentRecord,
        previous: Option<(String, u64, u64)>,
    ) -> Result<(String, u64, u64)> {
        let mut changes = self.activity_receiver(record);
        loop {
            let topology = self.topology.read().await;
            if let Err(error) = self.require_stable_topology().await {
                // Unknown multi-generation activity must not become an idle stamp
                // or a tight error/retry loop in a waiting controller.
                drop(topology);
                tokio::time::sleep(Duration::from_secs(1)).await;
                return Err(error);
            }
            let current = changes.borrow_and_update().clone();
            if current != previous {
                return current.ok_or_else(|| {
                    unavailable("proxy activity disconnected or execution binding changed")
                });
            }
            drop(topology);
            changes
                .changed()
                .await
                .map_err(|_| unavailable("proxy activity subscription closed"))?;
        }
    }
    async fn activity(&self, record: &EnvironmentRecord) -> Result<(String, u64, u64)> {
        let result = self.environment_activity(record).await?;
        Ok((
            result.proxy_session_id,
            result.activity_revision,
            result.active_streams,
        ))
    }

    async fn begin_reconcile(&self) -> Result<()> {
        let _topology = self.topology.read().await;
        self.require_stable_topology().await?;
        let mut s = self.local.lock().await;
        if s.process_takeover.is_some() && !s.initialized {
            return Err(Error::Conflict);
        }
        let mut client = self.connect().await?;
        let mut remote = self.remote(&mut client).await?;
        s.forget_completed_takeover(&remote);
        if let Some(old) = &s.session {
            if old.proxy_session_id == remote.proxy_session_id
                && old.sync_epoch != remote.sync_epoch
                && remote.controller_session_id != self.owner
            {
                return Err(Error::Conflict);
            }
        }
        if s.initialized
            && !s.buffering
            && remote.ready
            && remote.controller_session_id == self.owner
            && s.session.as_ref().is_some_and(|old| {
                old.proxy_session_id == remote.proxy_session_id
                    && old.sync_epoch == remote.sync_epoch
            })
        {
            // A validated live session refreshes bindings by version. Starting
            // another full synchronization would interrupt valid active streams.
            return Ok(());
        }
        remote.controller_session_id = self.owner.clone();
        if s.process_takeover.is_some()
            && s.session
                .as_ref()
                .is_some_and(|old| old.proxy_session_id == remote.proxy_session_id)
        {
            return Err(Error::Conflict);
        }
        let session = client
            .begin_bindings(self.request(remote))
            .await
            .map_err(rpc_error)?
            .into_inner();
        s.session = Some(session);
        s.buffering = true;
        s.bindings.clear();
        Ok(())
    }
    async fn finish_reconcile(&self) -> Result<()> {
        let _topology = self.topology.read().await;
        self.require_stable_topology().await?;
        let mut s = self.local.lock().await;
        let mut client = self.connect().await?;
        if !s.buffering {
            return self.ensure(&mut s, &mut client).await;
        }
        self.full(&mut s, &mut client).await?;
        s.buffering = false;
        s.initialized = true;
        Ok(())
    }
    async fn ensure_synced(&self) -> Result<()> {
        let _topology = self.topology.read().await;
        self.require_stable_topology().await?;
        let mut s = self.local.lock().await;
        let mut client = self.connect().await?;
        self.ensure(&mut s, &mut client).await
    }
    async fn suspend(&self, r: &EnvironmentRecord) -> Result<()> {
        self.send(UpdateBindingRequest {
            environment_id: r.spec.id.clone(),
            ownership_generation: r.assignment.generation,
            binding_revision: r.revision.checked_mul(2).ok_or(Error::Conflict)?,
            binding: Some(Binding::Suspended(RuntimeTarget {
                runtime_id: r.runtime.id.clone(),
                ip: r.runtime.ip.map(|ip| ip.to_string()).unwrap_or_default(),
            })),
            ..Default::default()
        })
        .await
    }
    async fn seed_recovery(&self, record: &EnvironmentRecord) -> Result<()> {
        if self.local.lock().await.buffering {
            if record.state == adx_core::EnvironmentState::Paused
                || matches!(
                    record.state,
                    adx_core::EnvironmentState::Failed | adx_core::EnvironmentState::Deleted
                ) && !record.resources_held
                    && !record.restart_pending
            {
                self.retire(record).await?;
            } else {
                self.suspend(record).await?;
            }
        }
        Ok(())
    }
    async fn retire_orphan(&self, r: &crate::RuntimeObservation) -> Result<()> {
        self.send(UpdateBindingRequest {
            environment_id: r.environment_id.clone(),
            ownership_generation: r.generation,
            binding_revision: u64::MAX,
            binding: Some(Binding::Retired(Retired {})),
            ..Default::default()
        })
        .await
    }
    async fn activate(&self, r: &EnvironmentRecord) -> Result<()> {
        self.apply(r, true).await
    }
    async fn retire(&self, r: &EnvironmentRecord) -> Result<()> {
        self.apply(r, false).await
    }
}

#[cfg(test)]
mod pending_tests {
    #[test]
    fn process_preflight_requires_initialized_confirmed_catalog() {
        let mut local = super::Local::default();
        assert!(!local.preflight_catalog_ready());
        local.initialized = true;
        assert!(local.preflight_catalog_ready());
        local.buffering = true;
        assert!(!local.preflight_catalog_ready());
        local.buffering = false;
        local.unconfirmed.insert("env".into(), Default::default());
        assert!(!local.preflight_catalog_ready());
    }
    #[test]
    fn remote_ready_without_exact_local_full_catalog_is_not_readiness() {
        let state = pb::BindingState {
            ready: true,
            proxy_session_id: "relay".into(),
            controller_session_id: "owner".into(),
            sync_epoch: 7,
        };
        let mut local = super::Local {
            initialized: true,
            session: Some(state.clone()),
            ..Default::default()
        };
        assert!(!local.confirmed_relay_ready(&state, "relay", "owner"));
        local.full_ack(&state);
        assert!(local.confirmed_relay_ready(&state, "relay", "owner"));
        assert!(!local.confirmed_relay_ready(&state, "other", "owner"));
        assert!(!local.confirmed_relay_ready(&state, "relay", "other"));
        let mut changed = state.clone();
        changed.sync_epoch += 1;
        assert!(!local.confirmed_relay_ready(&changed, "relay", "owner"));
        local.buffering = true;
        assert!(!local.confirmed_relay_ready(&state, "relay", "owner"));
    }
    use super::*;

    #[tokio::test]
    async fn failed_ordinary_write_retains_only_latest_unconfirmed_version() {
        let routes = UdsRoutes::new(
            "/nonexistent-adx-ledger/route.sock".into(),
            Duration::from_millis(10),
        )
        .unwrap();
        {
            let mut local = routes.local.lock().await;
            local.initialized = true;
            local.session = Some(pb::BindingState {
                proxy_session_id: "relay".into(),
                sync_epoch: 1,
                ready: true,
                controller_session_id: routes.owner.clone(),
            });
        }
        let mut retired = UpdateBindingRequest {
            environment_id: "env".into(),
            ownership_generation: 1,
            binding_revision: 1,
            binding: Some(Binding::Retired(Retired {})),
            ..Default::default()
        };
        assert!(routes.send(retired.clone()).await.is_err());
        retired.binding_revision = 2;
        assert!(routes.send(retired.clone()).await.is_err());
        let local = routes.local.lock().await;
        assert_eq!(local.unconfirmed.len(), 1);
        assert_eq!(local.unconfirmed["env"], retired);
        assert_eq!(local.bindings["env"], retired);
    }

    #[test]
    fn only_exact_pending_version_ack_clears_the_ledger() {
        let mut local = Local::default();
        let old = UpdateBindingRequest {
            environment_id: "env".into(),
            ownership_generation: 1,
            binding_revision: 1,
            binding: Some(Binding::Retired(Retired {})),
            ..Default::default()
        };
        let mut latest = old.clone();
        latest.binding_revision = 2;
        local.unconfirmed.insert("env".into(), latest.clone());
        local.binding_ack(&old);
        assert_eq!(local.unconfirmed["env"], latest);
        local.binding_ack(&latest);
        assert!(local.unconfirmed.is_empty());
    }

    #[test]
    fn full_ack_clears_admissions_only_after_confirmed_new_epoch() {
        let mut local = Local::default();
        let state = pb::BindingState {
            proxy_session_id: "relay".into(),
            sync_epoch: 1,
            controller_session_id: "controller".into(),
            ready: true,
        };
        local.confirmed_full = Some(("relay".into(), 1));
        local
            .admissions
            .insert(("env".into(), "runtime".into(), 1), 2);
        local.full_ack(&state);
        assert_eq!(local.admissions.len(), 1);
        let mut next = state;
        next.sync_epoch = 2;
        local.full_ack(&next);
        assert!(local.admissions.is_empty());
    }
}
