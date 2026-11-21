//! Node-local mirror of Coordinator upgrade authority. It never owns migration.
use adx_core::{Error, Result};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    os::unix::fs::OpenOptionsExt,
    path::PathBuf,
    sync::{atomic::Ordering, Arc, Mutex},
};
use tokio::sync::Notify;

pub use adx_protocol::control::{
    NodeUpgradeControl as NodeControl, NodeUpgradeGrant as ExecutionGrant,
};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredControl {
    #[serde(default)]
    process: Option<adx_protocol::relay::AdxletProcessRequest>,
    #[serde(default)]
    process_deadline_ms: Option<u64>,
    #[serde(default)]
    process_sealed: bool,
    #[serde(default)]
    completed_successor: Option<(adx_protocol::relay::AdxletProcessRequest, String)>,
    control: Option<NodeControl>,
    bootstrap_isolated: bool,
    may_have_executed: bool,
}

pub struct UpgradeGate {
    session: String,
    observed_pod_uid: String,
    path: Option<PathBuf>,
    state: Mutex<StoredControl>,
    activity: Arc<Activity>,
    transitions: tokio::sync::Mutex<()>,
    process_monotonic_deadline: Mutex<Option<std::time::Instant>>,
}

#[derive(Default)]
struct Activity {
    by_id: Mutex<std::collections::BTreeMap<String, usize>>,
    idle: Notify,
}

pub struct ExecutionLease(Arc<Activity>, String);

impl Drop for ExecutionLease {
    fn drop(&mut self) {
        if let Ok(mut ids) = self.0.by_id.lock() {
            if let Some(count) = ids.get_mut(&self.1) {
                *count -= 1;
                if *count == 0 {
                    ids.remove(&self.1);
                }
            }
        }
        self.0.idle.notify_waiters();
    }
}

fn same_process_owner(
    saved: &adx_protocol::relay::AdxletProcessRequest,
    requested: &adx_protocol::relay::AdxletProcessRequest,
) -> bool {
    saved.rollout_id == requested.rollout_id
        && saved.local_task_id == requested.local_task_id
        && saved.epoch == requested.epoch
}

impl UpgradeGate {
    fn active_pod_control(&self, state: &StoredControl) -> bool {
        state.control.as_ref().is_some_and(|control| {
            control.phase != "released" || control.node_session_id != self.session
        })
    }

    pub(crate) fn release_process_maintenance(
        &self,
        identity: &adx_protocol::relay::AdxletProcessRequest,
    ) -> Result<()> {
        let state = self.lock()?;
        if state.process.is_some()
            || self.active_pod_control(&state)
            || state.bootstrap_isolated
            || !state
                .completed_successor
                .as_ref()
                .is_some_and(|(saved, session)| {
                    same_process_owner(saved, identity) && session == &self.session
                })
        {
            return Err(Error::Conflict);
        }
        Ok(())
    }
    pub(crate) fn finish_successor(
        &self,
        identity: &adx_protocol::relay::AdxletProcessRequest,
    ) -> Result<()> {
        let mut state = self.lock()?;
        if identity.rollout_id.trim().is_empty()
            || identity.local_task_id.trim().is_empty()
            || identity.epoch == 0
            || self.active_pod_control(&state)
        {
            return Err(Error::Conflict);
        }
        if state.process.is_none()
            && !state.bootstrap_isolated
            && state
                .completed_successor
                .as_ref()
                .is_some_and(|(saved, session)| {
                    same_process_owner(saved, identity) && session == &self.session
                })
        {
            return Ok(());
        }
        // Begin retains the full immutable request. Successor startup and the
        // parent's ready query carry its owner, not the predecessor's deadline.
        let predecessor = state.process.as_ref().is_some_and(|saved| {
            state.process_sealed
                && saved.old_node_session != self.session
                && same_process_owner(saved, identity)
        });
        // A reaped ready candidate may be followed by another process of the
        // same task. Its saved completion is not readiness for this session.
        let previous_attempt =
            state
                .completed_successor
                .as_ref()
                .is_some_and(|(saved, session)| {
                    same_process_owner(saved, identity) && session != &self.session
                });
        if identity.old_node_session == self.session
            || !(predecessor
                || (state.process.is_none() && (state.bootstrap_isolated || previous_attempt)))
        {
            return Err(Error::Conflict);
        }
        let mut next = state.clone();
        next.process = None;
        next.process_deadline_ms = None;
        next.process_sealed = false;
        next.bootstrap_isolated = false;
        next.completed_successor = Some((identity.clone(), self.session.clone()));
        self.persist(&next)?;
        *state = next;
        Ok(())
    }
    pub(crate) async fn wait_process_leases(&self) -> Result<()> {
        if self.process_identity()?.is_none() {
            return Err(Error::Conflict);
        }
        self.wait().await
    }
    pub(crate) fn process_identity(
        &self,
    ) -> Result<Option<adx_protocol::relay::AdxletProcessRequest>> {
        Ok(self.lock()?.process.clone())
    }
    pub(crate) fn process_state(&self) -> Result<adx_protocol::relay::AdxletProcessState> {
        let state = self.lock()?;
        Ok(adx_protocol::relay::AdxletProcessState {
            identity: Some(state.process.clone().ok_or(Error::NotFound)?),
            phase: if state.process_sealed {
                "sealed"
            } else {
                "sealing_unknown"
            }
            .into(),
            node_session: self.session.clone(),
        })
    }
    pub(crate) fn completed_process(
        &self,
        identity: &adx_protocol::relay::AdxletProcessRequest,
    ) -> Result<bool> {
        let state = self.lock()?;
        Ok(state.process.is_none()
            && !state.bootstrap_isolated
            && !self.active_pod_control(&state)
            && state
                .completed_successor
                .as_ref()
                .is_some_and(|(saved, session)| {
                    same_process_owner(saved, identity) && session == &self.session
                }))
    }
    pub(crate) fn fence_process(
        &self,
        identity: adx_protocol::relay::AdxletProcessRequest,
        deadline: std::time::Instant,
    ) -> Result<()> {
        let remaining = deadline
            .checked_duration_since(std::time::Instant::now())
            .filter(|value| !value.is_zero())
            .ok_or(Error::Conflict)?;
        let mut state = self.lock()?;
        if state.process.is_some() {
            return Err(Error::Conflict);
        }
        let mut next = state.clone();
        next.process = Some(identity);
        next.process_deadline_ms = Some(
            adx_core::observation::now_millis()?
                .checked_add(u64::try_from(remaining.as_millis()).map_err(|_| Error::Conflict)?)
                .ok_or(Error::Conflict)?,
        );
        self.persist(&next)?;
        *state = next;
        *self
            .process_monotonic_deadline
            .lock()
            .map_err(|_| Error::Conflict)? = Some(deadline);
        Ok(())
    }
    pub(crate) fn process_remaining(&self) -> Result<std::time::Duration> {
        if let Some(deadline) = *self
            .process_monotonic_deadline
            .lock()
            .map_err(|_| Error::Conflict)?
        {
            return deadline
                .checked_duration_since(std::time::Instant::now())
                .filter(|remaining| !remaining.is_zero())
                .ok_or(Error::Conflict);
        }
        let deadline = self.lock()?.process_deadline_ms.ok_or(Error::Conflict)?;
        let remaining = deadline
            .checked_sub(adx_core::observation::now_millis()?)
            .filter(|ms| *ms > 0)
            .ok_or(Error::Conflict)?;
        Ok(std::time::Duration::from_millis(remaining))
    }
    pub(crate) fn mark_process_sealed(&self) -> Result<()> {
        let mut state = self.lock()?;
        let mut next = state.clone();
        next.process_sealed = true;
        self.persist(&next)?;
        *state = next;
        Ok(())
    }
    pub(crate) fn unmanaged() -> Self {
        Self {
            session: String::new(),
            observed_pod_uid: String::new(),
            path: None,
            state: Mutex::new(StoredControl::default()),
            activity: Arc::default(),
            transitions: tokio::sync::Mutex::new(()),
            process_monotonic_deadline: Mutex::new(None),
        }
    }

    /// Load durable authority. A process replacement always revokes active grants.
    pub fn open(path: PathBuf, session: String) -> Result<Self> {
        if session.trim().is_empty() {
            return Err(Error::Invalid("upgrade node session required".into()));
        }
        let observed_pod_uid = match std::env::var("ADX_POD_UID") {
            Ok(value)
                if !value.is_empty()
                    && value.len() <= 128
                    && !value.chars().any(char::is_whitespace) =>
            {
                value
            }
            Err(std::env::VarError::NotPresent) => String::new(),
            _ => {
                return Err(Error::Invalid(
                    "ADX_POD_UID must be nonblank UTF-8 of at most 128 bytes".into(),
                ));
            }
        };
        let mut state: StoredControl = match fs::read(&path) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map_err(|e| Error::Invalid(format!("invalid upgrade authority: {e}")))?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => StoredControl::default(),
            Err(e) => return Err(io_error(e)),
        };
        if let Some(control) = &mut state.control {
            if control.node_session_id != session {
                if control.phase != "released" {
                    control.phase = "isolated".into();
                    state.bootstrap_isolated = true;
                }
                control.node_session_id = session.clone();
                control.grants.clear();
            }
        }
        let gate = Self {
            session,
            observed_pod_uid,
            path: Some(path),
            state: Mutex::new(state),
            activity: Arc::default(),
            transitions: tokio::sync::Mutex::new(()),
            process_monotonic_deadline: Mutex::new(None),
        };
        gate.persist(&*gate.lock()?)?;
        Ok(gate)
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, StoredControl>> {
        self.state
            .lock()
            .map_err(|_| Error::Unavailable("upgrade authority lock unavailable".into()))
    }

    fn persist(&self, state: &StoredControl) -> Result<()> {
        let Some(path) = &self.path else {
            return Ok(());
        };
        let parent = path
            .parent()
            .ok_or_else(|| Error::Invalid("upgrade state parent required".into()))?;
        fs::create_dir_all(parent).map_err(io_error)?;
        let staged = parent.join(format!(".upgrade-{}.json", uuid::Uuid::new_v4()));
        let result = (|| {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&staged)
                .map_err(io_error)?;
            let bytes = serde_json::to_vec(state)
                .map_err(|e| Error::Invalid(format!("upgrade authority encoding failed: {e}")))?;
            file.write_all(&bytes).map_err(io_error)?;
            file.sync_all().map_err(io_error)?;
            fs::rename(&staged, path).map_err(io_error)?;
            File::open(parent)
                .map_err(io_error)?
                .sync_all()
                .map_err(io_error)
        })();
        if result.is_err() {
            let _ = fs::remove_file(staged);
        }
        result
    }

    pub fn isolate_bootstrap(&self) -> Result<()> {
        let mut state = self.lock()?;
        // OnDelete leaves the startup env on existing Pods after task completion.
        // Keep durable release across process restarts. Startup lifecycle stays
        // paused until registration can isolate a subsequent task before adoption.
        state.bootstrap_isolated = !state.control.as_ref().is_some_and(|control| {
            control.node_session_id == self.session && control.phase == "released"
        });
        self.persist(&state)
    }

    pub fn control(&self) -> Result<Option<NodeControl>> {
        Ok(self.lock()?.control.clone())
    }

    pub fn may_have_executed(&self) -> Result<bool> {
        Ok(self.lock()?.may_have_executed)
    }

    pub fn accepting_allocations(&self) -> bool {
        self.lock().is_ok_and(|state| {
            state.process.is_none()
                && !state.bootstrap_isolated
                && state.control.as_ref().is_none_or(|c| {
                    c.node_session_id == self.session
                        && matches!(c.phase.as_str(), "verified" | "released")
                })
        })
    }

    fn allowed(
        &self,
        state: &StoredControl,
        id: &str,
        generation: u64,
        action: &str,
        task: Option<&str>,
    ) -> bool {
        if state.process.is_some() {
            return false;
        }
        let Some(control) = &state.control else {
            return task.is_none() && !state.bootstrap_isolated;
        };
        if control.node_session_id != self.session || state.bootstrap_isolated {
            return false;
        }
        let phase = control.phase.as_str();
        if task.is_none() && matches!(phase, "verified" | "released") {
            return true;
        }
        if !matches!(phase, "verified" | "evacuating") {
            return false;
        }
        if task
            .is_some_and(|operation| !operation.starts_with(&format!("task:{}:", control.task_id)))
        {
            return false;
        }
        control.grants.iter().any(|grant| {
            grant.environment_id == id
                && grant.assignment_generation == generation
                && (grant.actions.iter().any(|a| a == action)
                    || task.is_none()
                        && phase != "verified"
                        && matches!(action, "resume" | "adopt")
                        && grant.actions.iter().any(|a| a == "recover"))
        })
    }

    pub fn check(&self, id: &str, generation: u64, action: &str) -> Result<()> {
        if self.allowed(&*self.lock()?, id, generation, action, None) {
            Ok(())
        } else {
            Err(Error::Unavailable(
                "node upgrade execution admission is closed".into(),
            ))
        }
    }

    /// Management actions require an explicit task-scoped grant even on a verified peer.
    pub(crate) fn management_grant(
        &self,
        id: &str,
        generation: u64,
        action: &str,
        operation_id: &str,
    ) -> Result<()> {
        if self.allowed(&*self.lock()?, id, generation, action, Some(operation_id)) {
            Ok(())
        } else {
            Err(Error::Conflict)
        }
    }

    /// Persist ambiguity before creating a Controller, hold, or backend action.
    pub(crate) fn record_action(&self, id: &str, generation: u64, action: &str) -> Result<()> {
        let mut state = self.lock()?;
        self.record_action_locked(&mut state, id, generation, action)
    }

    /// Terminal records may be adopted for query/deletion, without opening execution.
    pub(crate) fn record_cleanup(&self) -> Result<()> {
        let mut state = self.lock()?;
        if state.process.is_some() {
            return Err(Error::Conflict);
        }
        if state
            .control
            .as_ref()
            .is_some_and(|c| c.phase == "quiescing")
        {
            return Err(Error::Unavailable(
                "node upgrade execution admission is closed".into(),
            ));
        }
        self.record_effect(&mut state)
    }

    fn record_action_locked(
        &self,
        state: &mut StoredControl,
        id: &str,
        generation: u64,
        action: &str,
    ) -> Result<()> {
        if !self.allowed(state, id, generation, action, None) {
            return Err(Error::Unavailable(
                "node upgrade execution admission is closed".into(),
            ));
        }
        self.record_effect(state)
    }

    fn record_effect(&self, state: &mut StoredControl) -> Result<()> {
        if (state.bootstrap_isolated
            || state
                .control
                .as_ref()
                .is_some_and(|c| c.phase != "released"))
            && !state.may_have_executed
        {
            let mut next = state.clone();
            next.may_have_executed = true;
            self.persist(&next)?;
            *state = next;
        }
        Ok(())
    }

    /// The lease spans the actual side effect, so Quiescing can await accepted work.
    pub async fn enter(&self, id: &str, generation: u64, action: &str) -> Result<ExecutionLease> {
        let mut state = self.lock()?;
        self.record_action_locked(&mut state, id, generation, action)?;
        *self
            .activity
            .by_id
            .lock()
            .map_err(|_| Error::Unavailable("execution lease lock unavailable".into()))?
            .entry(id.into())
            .or_default() += 1;
        Ok(ExecutionLease(self.activity.clone(), id.into()))
    }

    pub async fn apply(&self, control: NodeControl) -> Result<()> {
        let _transition = self.transitions.lock().await;
        validate(&control, &self.session)?;
        let open = matches!(control.phase.as_str(), "verified" | "released");
        {
            let mut state = self.lock()?;
            if state.process.is_some() && matches!(control.phase.as_str(), "verified" | "released")
            {
                return Err(Error::Conflict);
            }
            let unchanged = state.control.as_ref() == Some(&control) && !state.bootstrap_isolated;
            if let Some(previous) = &state.control {
                let same_task = previous.task_id == control.task_id;
                if !unchanged
                    && ((!same_task && previous.phase != "released")
                        || (same_task
                            && (control.epoch < previous.epoch
                                || (control.epoch == previous.epoch
                                    && control.revision <= previous.revision))))
                {
                    return Err(Error::Conflict);
                }
            }
            if !unchanged {
                let mut next = state.clone();
                if !next.bootstrap_isolated
                    && next
                        .control
                        .as_ref()
                        .is_some_and(|old| old.task_id != control.task_id)
                {
                    next.may_have_executed = false;
                }
                next.control = Some(control);
                next.bootstrap_isolated = false;
                self.persist(&next)?;
                *state = next;
            }
        }
        // Persist closed authority before waiting for already accepted effects.
        if !open {
            self.wait().await?;
        }
        Ok(())
    }

    async fn wait(&self) -> Result<()> {
        loop {
            let notified = self.activity.idle.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let idle = self
                .activity
                .by_id
                .lock()
                .map_err(|_| Error::Unavailable("execution lease lock unavailable".into()))?
                .is_empty();
            if idle {
                return Ok(());
            }
            notified.await;
        }
    }

    pub(crate) fn session(&self) -> &str {
        &self.session
    }
}

fn validate(control: &NodeControl, session: &str) -> Result<()> {
    if control.task_id.trim().is_empty()
        || control.task_id.len() > 256
        || control.epoch == 0
        || control.revision == 0
        || control.node_session_id != session
        || session.is_empty()
        || !matches!(
            control.phase.as_str(),
            "isolated" | "evacuating" | "quiescing" | "verified" | "released"
        )
        || (!matches!(control.phase.as_str(), "evacuating" | "verified")
            && !control.grants.is_empty())
    {
        return Err(Error::Invalid(
            "invalid node upgrade control identity or phase".into(),
        ));
    }
    for grant in &control.grants {
        if grant.environment_id.trim().is_empty()
            || grant.assignment_generation == 0
            || grant.actions.is_empty()
            || grant.actions.iter().any(|action| {
                !matches!(
                    action.as_str(),
                    "create" | "resume" | "recover" | "restart" | "pause" | "route" | "adopt"
                )
            })
        {
            return Err(Error::Invalid(
                "invalid node upgrade execution grant".into(),
            ));
        }
    }
    Ok(())
}

fn io_error(error: std::io::Error) -> Error {
    Error::Unavailable(format!("upgrade authority persistence failed: {error}"))
}

impl crate::Adxlet {
    /// Fails with `Error::Conflict` when the identity is incomplete or the node
    /// inventory is not provably stable, and with `Error::Unavailable` when an
    /// inventory source cannot be read.
    pub(crate) async fn process_preflight(
        &self,
        identity: adx_protocol::control::ProcessPreflightRequest,
        inspection: &adx_protocol::control::NodeUpgradeInspection,
    ) -> Result<()> {
        if identity.rollout_id.trim().is_empty()
            || identity.local_task_id.trim().is_empty()
            || identity.epoch == 0
            || identity.old_relay_session.is_empty()
        {
            return Err(Error::Conflict);
        }
        let lifecycle = self.lifecycle_ready.read().await;
        process_inventory_ready(
            inspection.unknown,
            *lifecycle,
            self.authority_reconciling.load(Ordering::Acquire),
            inspection.holds,
            inspection.authority_tasks,
        )?;
        if !self
            .services
            .upgrade
            .activity
            .by_id
            .lock()
            .map_err(|_| Error::Unavailable("execution activity unavailable".into()))?
            .is_empty()
        {
            return Err(Error::Conflict);
        }
        let handles: Vec<_> = self
            .environments
            .lock()
            .map_err(|_| Error::Unavailable("environment registry unavailable".into()))?
            .values()
            .map(|(_, _, handle)| handle.clone())
            .collect();
        let mut records = Vec::new();
        for handle in handles {
            let (record, durability) = handle.view();
            if durability != Some(crate::Durability::Published) {
                return Err(Error::Conflict);
            }
            records.push(record);
        }
        let inventory = tokio::time::timeout(
            self.services.operation_timeout,
            self.services.runtime.inventory(),
        )
        .await
        .map_err(|_| Error::Unavailable("process inventory deadline".into()))??;
        if inventory.iter().any(|runtime| {
            !records.iter().any(|record| {
                record.spec.id == runtime.environment_id
                    && record.runtime.id == runtime.runtime_id
                    && record.resources_held
            })
        }) || records
            .iter()
            .filter(|record| record.resources_held)
            .any(|record| {
                !inventory.iter().any(|runtime| {
                    runtime.environment_id == record.spec.id
                        && runtime.runtime_id == record.runtime.id
                })
            })
        {
            return Err(Error::Conflict);
        }
        process_inventory_ready(
            false,
            *lifecycle,
            self.authority_reconciling.load(Ordering::Acquire),
            scoped_count(&self.local_holds, None, String::as_str)?,
            scoped_count(&self.authority_tasks, None, |key| key.0.as_str())?,
        )?;
        if !self
            .services
            .upgrade
            .activity
            .by_id
            .lock()
            .map_err(|_| Error::Unavailable("execution activity unavailable".into()))?
            .is_empty()
        {
            return Err(Error::Conflict);
        }
        Ok(())
    }

    /// Configure a durable mirror before starting reconciliation or RPC services.
    pub fn with_upgrade_control(mut self, path: PathBuf, session: String) -> Result<Self> {
        Arc::get_mut(&mut self.services)
            .ok_or(Error::Conflict)?
            .upgrade = Arc::new(UpgradeGate::open(path, session)?);
        Ok(self)
    }

    pub fn isolate_upgrade_bootstrap(&self) -> Result<()> {
        self.services.upgrade.isolate_bootstrap()
    }

    pub(crate) async fn apply_registration_upgrade_control(
        &self,
        control: adx_protocol::control::NodeUpgradeControl,
    ) -> Result<()> {
        if control.phase != "isolated" || !control.grants.is_empty() {
            return Err(Error::Invalid(
                "registration may only close upgrade admission".into(),
            ));
        }
        if control.node_session_id != self.services.upgrade.session() {
            return Err(Error::Conflict);
        }
        if self.services.upgrade.control()?.is_some_and(|current| {
            current.task_id == control.task_id
                && current.node_session_id == control.node_session_id
                && (current.epoch > control.epoch
                    || (current.epoch == control.epoch && current.revision >= control.revision))
        }) {
            return Ok(());
        }
        self.services.upgrade.apply(control).await
    }

    pub async fn apply_upgrade_control(
        &self,
        control: NodeControl,
    ) -> Result<adx_protocol::control::NodeUpgradeInspection> {
        self.services.upgrade.apply(control).await?;
        self.inspect_upgrade().await
    }

    /// Inventory errors are unknown, never evidence that the node is empty.
    pub async fn inspect_upgrade(&self) -> Result<adx_protocol::control::NodeUpgradeInspection> {
        self.inspect_upgrade_environment(None).await
    }

    pub(crate) async fn inspect_upgrade_environment(
        &self,
        environment: Option<&str>,
    ) -> Result<adx_protocol::control::NodeUpgradeInspection> {
        let _transition = self.services.upgrade.transitions.lock().await;
        let control = self.services.upgrade.control()?;
        let admission_closed = control
            .as_ref()
            .is_some_and(|c| matches!(c.phase.as_str(), "isolated" | "quiescing"));
        let _lifecycle = if admission_closed {
            self.services.upgrade.wait().await?;
            Some(self.lifecycle_ready.write().await)
        } else {
            None
        };
        let handles: Vec<_> = self
            .environments
            .lock()
            .map_err(|_| Error::Unavailable("environment registry unavailable".into()))?
            .iter()
            .filter(|(id, _)| environment.is_none_or(|expected| id.as_str() == expected))
            .map(|(_, (_, _, h))| h.clone())
            .collect();
        if admission_closed {
            for handle in &handles {
                tokio::time::timeout(self.services.operation_timeout, handle.sync())
                    .await
                    .map_err(|_| {
                        Error::Unavailable("upgrade controller barrier timed out".into())
                    })??;
            }
        }
        let controllers = scoped_count(&self.environments, environment, String::as_str)?;
        let holds = scoped_count(&self.local_holds, environment, String::as_str)?;
        let authority_tasks =
            scoped_count(&self.authority_tasks, environment, |key| key.0.as_str())?;
        let actual = tokio::time::timeout(
            self.services.operation_timeout,
            self.services.runtime.inventory(),
        )
        .await;
        let (backends, mut unknown) = match actual {
            Ok(Ok(actual)) => (
                count(
                    actual
                        .iter()
                        .filter(|runtime| environment.is_none_or(|id| runtime.environment_id == id))
                        .count(),
                )?,
                false,
            ),
            _ => (0, true),
        };
        unknown |= self.services.upgrade.control()? != control;
        let used = self.used();
        let no_ledger = used == adx_core::Resources::default();
        unknown |= !self.authority_initialized.load(Ordering::Acquire);
        let strict_empty = (admission_closed || environment.is_some())
            && !unknown
            && controllers == 0
            && holds == 0
            && authority_tasks == 0
            && backends == 0
            && (environment.is_some() || no_ledger);
        let control = control.unwrap_or(NodeControl {
            node_session_id: self.services.upgrade.session().into(),
            phase: if self.services.upgrade.accepting_allocations() {
                "released"
            } else {
                "isolated"
            }
            .into(),
            ..Default::default()
        });
        let rootfs = self
            .services
            .runtime
            .runtime_rootfs_identity()
            .unwrap_or_default();
        Ok(adx_protocol::control::NodeUpgradeInspection {
            observed_pod_uid: self.services.upgrade.observed_pod_uid.clone(),
            protocol_version: 3,
            runtime_rootfs_sha256: rootfs.sha256,
            runtime_rootfs_path: rootfs.path,
            task_id: control.task_id,
            epoch: control.epoch,
            node_session_id: control.node_session_id,
            revision: control.revision,
            phase: control.phase,
            controllers,
            holds,
            backends,
            authority_tasks,
            unknown,
            strict_empty,
            may_have_executed: self.services.upgrade.may_have_executed()?,
            grants: control.grants,
            environment: None,
        })
    }
}

fn count(value: usize) -> Result<u64> {
    u64::try_from(value).map_err(|_| Error::Unavailable("upgrade inventory count overflow".into()))
}

fn process_inventory_ready(
    unknown: bool,
    ready: bool,
    reconciling: bool,
    holds: u64,
    tasks: u64,
) -> Result<()> {
    if unknown || !ready || reconciling || holds != 0 || tasks != 0 {
        return Err(Error::Conflict);
    }
    Ok(())
}

fn scoped_count<K: Ord, V>(
    registry: &Mutex<std::collections::BTreeMap<K, V>>,
    environment: Option<&str>,
    id: impl Fn(&K) -> &str,
) -> Result<u64> {
    let entries = registry
        .lock()
        .map_err(|_| Error::Unavailable("upgrade inventory lock unavailable".into()))?;
    count(
        entries
            .keys()
            .filter(|key| environment.is_none_or(|expected| id(key) == expected))
            .count(),
    )
}

#[cfg(test)]
mod process_preflight_tests {
    #[test]
    fn observed_pod_identity_is_not_durable_authority() {
        let mut gate = super::UpgradeGate::unmanaged();
        assert!(gate.observed_pod_uid.is_empty());
        gate.observed_pod_uid = "actual-startup-pod".into();
        let control = super::NodeControl {
            task_id: "task-bound-to-another-pod".into(),
            ..Default::default()
        };
        gate.lock().unwrap().control = Some(control);
        let stored = serde_json::to_value(&*gate.lock().unwrap()).unwrap();
        assert!(!stored.to_string().contains("actual-startup-pod"));
        assert_eq!(gate.observed_pod_uid, "actual-startup-pod");
    }

    #[test]
    fn successor_gate_remains_isolated_until_finish_and_only_exact_generation_replays() {
        let gate = super::UpgradeGate::unmanaged();
        gate.isolate_bootstrap().unwrap();
        let identity = adx_protocol::relay::AdxletProcessRequest {
            rollout_id: "task".into(),
            local_task_id: "local".into(),
            epoch: 1,
            old_node_session: "old".into(),
            ..Default::default()
        };
        assert!(gate.lock().unwrap().bootstrap_isolated);
        gate.finish_successor(&identity).unwrap();
        assert!(!gate.lock().unwrap().bootstrap_isolated);
        gate.finish_successor(&identity).unwrap();
        let mut wrong = identity;
        wrong.epoch += 1;
        assert!(gate.finish_successor(&wrong).is_err());
    }

    #[test]
    fn successor_owner_does_not_bypass_seal_session_or_pod_authority() {
        use adx_protocol::relay::AdxletProcessRequest;
        for fault in [
            "rollout",
            "task",
            "epoch",
            "unsealed",
            "origin",
            "active-pod",
            "foreign-release",
        ] {
            let mut gate = super::UpgradeGate::unmanaged();
            gate.session = "candidate".into();
            gate.isolate_bootstrap().unwrap();
            let mut identity = AdxletProcessRequest {
                rollout_id: "rollout".into(),
                local_task_id: "local".into(),
                epoch: 1,
                ..Default::default()
            };
            {
                let mut state = gate.lock().unwrap();
                state.process = Some(AdxletProcessRequest {
                    old_node_session: "origin".into(),
                    old_relay_session: "relay".into(),
                    deadline_unix_millis: 100,
                    ..identity.clone()
                });
                state.process_sealed = true;
                match fault {
                    "rollout" => identity.rollout_id.push_str("-other"),
                    "task" => identity.local_task_id.push_str("-other"),
                    "epoch" => identity.epoch += 1,
                    "unsealed" => state.process_sealed = false,
                    "origin" => {
                        state.process.as_mut().unwrap().old_node_session = gate.session.clone()
                    }
                    _ => {
                        state.control = Some(super::NodeControl {
                            node_session_id: if fault == "foreign-release" {
                                "other"
                            } else {
                                "candidate"
                            }
                            .into(),
                            phase: if fault == "active-pod" {
                                "evacuating"
                            } else {
                                "released"
                            }
                            .into(),
                            ..Default::default()
                        })
                    }
                }
            }
            assert!(gate.finish_successor(&identity).is_err(), "{fault}");
            assert!(!gate.completed_process(&identity).unwrap(), "{fault}");
            assert!(gate.process_identity().unwrap().is_some(), "{fault}");
        }
    }

    #[test]
    fn completed_owner_is_not_ready_after_another_fence_or_bootstrap_isolation() {
        let mut gate = super::UpgradeGate::unmanaged();
        gate.session = "candidate".into();
        let identity = adx_protocol::relay::AdxletProcessRequest {
            rollout_id: "rollout".into(),
            local_task_id: "local".into(),
            epoch: 1,
            ..Default::default()
        };
        gate.isolate_bootstrap().unwrap();
        gate.finish_successor(&identity).unwrap();
        assert!(gate.completed_process(&identity).unwrap());
        gate.isolate_bootstrap().unwrap();
        assert!(!gate.completed_process(&identity).unwrap());
        gate.finish_successor(&identity).unwrap();
        let mut next = identity.clone();
        next.local_task_id = "another-task".into();
        next.old_node_session = gate.session.clone();
        gate.fence_process(
            next,
            std::time::Instant::now() + std::time::Duration::from_secs(30),
        )
        .unwrap();
        assert!(!gate.completed_process(&identity).unwrap());
        assert!(gate.finish_successor(&identity).is_err());
    }
    #[test]
    fn accepted_process_uses_grant_limited_deadline_and_replay_cannot_refresh() {
        let gate = super::UpgradeGate::unmanaged();
        let identity = adx_protocol::relay::AdxletProcessRequest::default();
        assert!(matches!(
            gate.fence_process(identity.clone(), std::time::Instant::now()),
            Err(Error::Conflict)
        ));
        assert!(gate.process_identity().unwrap().is_none());
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        gate.fence_process(identity.clone(), deadline).unwrap();
        assert!(gate.process_remaining().unwrap() <= std::time::Duration::from_secs(10));
        assert!(gate
            .fence_process(
                identity,
                std::time::Instant::now() + std::time::Duration::from_secs(500)
            )
            .is_err());
        assert_eq!(
            *gate.process_monotonic_deadline.lock().unwrap(),
            Some(deadline)
        );
    }
    use super::*;

    #[test]
    fn process_preflight_rejects_unknown_claim_or_authority_without_requiring_empty_node() {
        assert!(process_inventory_ready(false, true, false, 0, 0).is_ok());
        for (unknown, ready, reconciling, holds, tasks) in [
            (true, true, false, 0, 0),
            (false, false, false, 0, 0),
            (false, true, true, 0, 0),
            (false, true, false, 1, 0),
            (false, true, false, 0, 1),
        ] {
            assert!(process_inventory_ready(unknown, ready, reconciling, holds, tasks).is_err());
        }
    }
}
