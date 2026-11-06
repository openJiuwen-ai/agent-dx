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
    control: Option<NodeControl>,
    bootstrap_isolated: bool,
    may_have_executed: bool,
}

pub struct UpgradeGate {
    session: String,
    path: Option<PathBuf>,
    state: Mutex<StoredControl>,
    activity: Arc<Activity>,
    transitions: tokio::sync::Mutex<()>,
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

impl UpgradeGate {
    pub(crate) fn unmanaged() -> Self {
        Self {
            session: String::new(),
            path: None,
            state: Mutex::new(StoredControl::default()),
            activity: Arc::default(),
            transitions: tokio::sync::Mutex::new(()),
        }
    }

    /// Load durable authority. A process replacement always revokes active grants.
    pub fn open(path: PathBuf, session: String) -> Result<Self> {
        if session.trim().is_empty() {
            return Err(Error::Invalid("upgrade node session required".into()));
        }
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
            path: Some(path),
            state: Mutex::new(state),
            activity: Arc::default(),
            transitions: tokio::sync::Mutex::new(()),
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
            !state.bootstrap_isolated
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
        let canary = id.starts_with("default-upgrade-canary-");
        let Some(control) = &state.control else {
            return task.is_none() && !state.bootstrap_isolated && !canary;
        };
        if control.node_session_id != self.session || state.bootstrap_isolated {
            return false;
        }
        let phase = control.phase.as_str();
        if task.is_none() && matches!(phase, "verified" | "released") {
            if !canary {
                return true;
            }
            if phase == "released" {
                return false;
            }
        } else if !matches!(phase, "verified" | "canary" | "evacuating") {
            return false;
        }
        if task
            .is_some_and(|operation| !operation.starts_with(&format!("task:{}:", control.task_id)))
        {
            return false;
        }
        control.grants.iter().any(|grant| {
            let wildcard = phase == "verified"
                && (task.is_none() || action == "pause" && (grant.actions == ["pause"] || canary));
            grant.environment_id == id
                && (grant.assignment_generation == generation
                    || wildcard && grant.assignment_generation == 0)
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
        let authorized: std::collections::BTreeSet<_> = control
            .grants
            .iter()
            .map(|grant| grant.environment_id.clone())
            .collect();
        {
            let mut state = self.lock()?;
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
        // Persist revocation before waiting. Replays also wait for canary effects
        // whose grants have already disappeared from durable authority.
        self.wait(open.then_some(&authorized)).await
    }

    async fn wait(&self, allowed: Option<&std::collections::BTreeSet<String>>) -> Result<()> {
        loop {
            let notified = self.activity.idle.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let busy = self
                .activity
                .by_id
                .lock()
                .map_err(|_| Error::Unavailable("execution lease lock unavailable".into()))?
                .keys()
                .any(|id| {
                    allowed.is_none_or(|allowed| {
                        id.starts_with("default-upgrade-canary-") && !allowed.contains(id)
                    })
                });
            if !busy {
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
            "isolated" | "evacuating" | "canary" | "quiescing" | "verified" | "released"
        )
        || (!matches!(control.phase.as_str(), "canary" | "evacuating" | "verified")
            && !control.grants.is_empty())
    {
        return Err(Error::Invalid(
            "invalid node upgrade control identity or phase".into(),
        ));
    }
    for grant in &control.grants {
        if grant.environment_id.trim().is_empty()
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
        // A pre-revocation local claim can have lost its response. Once the
        // reserved ID is closed and has no Controller, release only its exact
        // tentative token; unrelated peer reservations remain untouched.
        let reservations: Vec<_> = {
            let environments = self
                .environments
                .lock()
                .map_err(|_| Error::Unavailable("environment registry unavailable".into()))?;
            let holds = self
                .local_holds
                .lock()
                .map_err(|_| Error::Unavailable("local reservations unavailable".into()))?;
            holds
                .iter()
                .filter(|(id, _)| {
                    id.starts_with("default-upgrade-canary-")
                        && !environments.contains_key(*id)
                        && self.services.upgrade.check(id, 0, "create").is_err()
                })
                .map(|(id, hold)| {
                    (
                        id.clone(),
                        crate::local::LocalReservation {
                            token: Some(hold.token.clone()),
                            devices: hold.devices.clone(),
                        },
                    )
                })
                .collect()
        };
        for (id, reservation) in reservations {
            self.release_local(&id, &reservation)?;
        }
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
        let quiescing = control.as_ref().is_some_and(|c| c.phase == "quiescing");
        let _lifecycle = if quiescing {
            self.services.upgrade.wait(None).await?;
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
        if quiescing {
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
        let strict_empty = (quiescing || environment.is_some())
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
            protocol_version: 2,
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
