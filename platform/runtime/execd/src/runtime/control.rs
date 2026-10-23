//! HTTP control state independent of transport connections and request lifetime.
use adx_core::{runtime::*, Error, Result};
use std::{
    collections::BTreeMap,
    future::Future,
    io,
    pin::Pin,
    sync::{Arc, Mutex, OnceLock},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::watch;

pub type Handoff = Pin<Box<dyn Future<Output = io::Result<HandoffOutcome>> + Send>>;
pub type Restore<'a> = Pin<Box<dyn Future<Output = io::Result<RuntimeRestore>> + Send + 'a>>;
pub type Cooperation<'a> = Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'a>>;
#[derive(Debug, Clone, Copy)]
pub enum HandoffOutcome {
    Resume,
    Restore,
    Error,
}
pub trait CheckpointHooks: Send + Sync + 'static {
    /// Prepare guest services before acknowledging a checkpoint.
    fn prepare_checkpoint(&self) -> Cooperation<'_> {
        Box::pin(async { Ok(()) })
    }
    /// Release guest preparation only when checkpointing never started.
    fn rollback_checkpoint_prepare(&self) -> Cooperation<'_> {
        Box::pin(async { Ok(()) })
    }
    /// Release guest preparation after the backend confirms source continuation.
    fn resume_after_checkpoint(&self) -> Cooperation<'_> {
        Box::pin(async { Ok(()) })
    }
    /// Open the backend's handoff before publishing Prepared.
    fn open(&self) -> io::Result<Handoff>;
    /// Validate restored identity, refresh environment and rearm listeners.
    fn restore(&self, _previous: &RuntimeIdentity) -> io::Result<RuntimeIdentity> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "checkpoint hook requires asynchronous restore",
        ))
    }
    fn restore_context(&self, previous: &RuntimeIdentity) -> io::Result<RuntimeRestore> {
        Ok(RuntimeRestore {
            target: self.restore(previous)?,
            origin: None,
        })
    }
    /// Await required guest services before publishing the restored runtime as Running.
    /// The default preserves synchronous hooks without blocking the Tokio runtime.
    fn restore_context_async<'a>(&'a self, previous: &'a RuntimeIdentity) -> Restore<'a> {
        Box::pin(async move { self.restore_context(previous) })
    }
}
struct State {
    status: RuntimeStatus,
    probe_epoch: u64,
    probe_phase: RuntimePhase,
    prepare: Option<PrepareCheckpoint>,
    reader_running: bool,
    workload_reply: Option<watch::Sender<Option<std::result::Result<(), String>>>>,
    workload_finished: Option<FinishWorkloadCheckpoint>,
}
pub struct Controller {
    state: Mutex<State>,
    hooks: Arc<dyn CheckpointHooks>,
    changed: watch::Sender<u64>,
    probe_readiness: std::sync::atomic::AtomicBool,
    cooperation: tokio::sync::Mutex<()>,
}
static CURRENT: OnceLock<Arc<Controller>> = OnceLock::new();
pub(crate) fn install(controller: Arc<Controller>) -> Result<()> {
    CURRENT.set(controller).map_err(|_| Error::Conflict)
}
pub(crate) fn current() -> Option<&'static Arc<Controller>> {
    CURRENT.get()
}

impl Controller {
    pub fn new(identity: RuntimeIdentity, hooks: Arc<dyn CheckpointHooks>) -> Result<Arc<Self>> {
        identity.validate()?;
        let (changed, _) = watch::channel(1);
        Ok(Arc::new(Self {
            state: Mutex::new(State {
                status: RuntimeStatus {
                    failure: None,
                    identity,
                    revision: 1,
                    phase: RuntimePhase::Running,
                    checkpoint: None,
                    requested_checkpoint: None,
                    requested_checkpoint_deadline_unix_millis: None,
                    active_requests: 0,
                    active_commands: 0,
                    activity_revision: 1,
                    required_services: BTreeMap::new(),
                },
                probe_epoch: 1,
                probe_phase: RuntimePhase::Running,
                prepare: None,
                reader_running: false,
                workload_reply: None,
                workload_finished: None,
            }),
            hooks,
            changed,
            probe_readiness: std::sync::atomic::AtomicBool::new(false),
            cooperation: tokio::sync::Mutex::new(()),
        }))
    }
    pub fn begin_probe_initialization(&self) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::Unavailable("runtime state lock poisoned".into()))?;
        self.probe_readiness
            .store(true, std::sync::atomic::Ordering::Release);
        state.status.phase = RuntimePhase::Initializing;
        state.status.failure = None;
        self.publish(&mut state);
        Ok(())
    }
    pub fn probe_ready(&self, identity: &RuntimeIdentity, epoch: u64) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::Unavailable("runtime state lock poisoned".into()))?;
        if state.status.identity != *identity
            || state.probe_epoch != epoch
            || state.status.phase != RuntimePhase::Initializing
        {
            return Err(Error::Conflict);
        }
        state.status.phase = RuntimePhase::Running;
        self.publish(&mut state);
        Ok(())
    }
    pub fn probe_failed(
        &self,
        identity: &RuntimeIdentity,
        epoch: u64,
        failure: adx_core::probes::ProbeFailure,
    ) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::Unavailable("runtime state lock poisoned".into()))?;
        if state.status.identity != *identity
            || state.probe_epoch != epoch
            || !matches!(
                state.status.phase,
                RuntimePhase::Initializing | RuntimePhase::Running
            )
        {
            return Err(Error::Conflict);
        }
        state.status.phase = RuntimePhase::Failed;
        state.status.failure = Some(failure);
        self.publish(&mut state);
        Ok(())
    }
    fn handoff_probe_phase(&self) -> RuntimePhase {
        if self
            .probe_readiness
            .load(std::sync::atomic::Ordering::Acquire)
        {
            RuntimePhase::Initializing
        } else {
            RuntimePhase::Running
        }
    }
    /// Only the local Unix listener calls this. A disconnected caller does not cancel it.
    /// Concurrent workload requests or an active checkpoint return Conflict.
    pub async fn request_checkpoint(
        &self,
        operation_id: String,
        timeout: Option<Duration>,
    ) -> Result<()> {
        if operation_id.is_empty() || operation_id.len() > 128 {
            return Err(Error::Invalid("invalid checkpoint operation id".into()));
        }
        let deadline = timeout
            .map(|timeout| {
                if timeout.is_zero() || timeout > Duration::from_secs(3600) {
                    return Err(Error::Invalid(
                        "checkpoint timeout must be positive and at most 3600 seconds".into(),
                    ));
                }
                SystemTime::now()
                    .checked_add(timeout)
                    .ok_or_else(|| Error::Invalid("checkpoint deadline overflow".into()))?
                    .duration_since(UNIX_EPOCH)
                    .map_err(|_| Error::Unavailable("system clock before epoch".into()))?
                    .as_millis()
                    .try_into()
                    .map_err(|_| Error::Invalid("checkpoint deadline overflow".into()))
            })
            .transpose()?;
        let mut reply = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| Error::Unavailable("runtime state poisoned".into()))?;
            if state.status.phase != RuntimePhase::Running || state.workload_reply.is_some() {
                return Err(Error::Conflict);
            }
            let (sender, reply) = watch::channel(None);
            state.status.requested_checkpoint = Some(operation_id);
            state.status.requested_checkpoint_deadline_unix_millis = deadline;
            state.workload_reply = Some(sender);
            state.workload_finished = None;
            self.publish(&mut state);
            reply
        };
        let wait = async move {
            loop {
                if let Some(result) = reply.borrow_and_update().clone() {
                    return result.map_err(Error::Unavailable);
                }
                reply
                    .changed()
                    .await
                    .map_err(|_| Error::Unavailable("checkpoint request retired".into()))?;
            }
        };
        match timeout {
            Some(timeout) => tokio::time::timeout(timeout, wait)
                .await
                .map_err(|_| Error::Unavailable("checkpoint request timed out".into()))?,
            None => wait.await,
        }
    }
    /// A successful ACK requires the matching backend handoff. Duplicate ACKs are idempotent.
    pub fn finish_checkpoint(&self, request: FinishWorkloadCheckpoint) -> Result<RuntimeStatus> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::Unavailable("runtime state poisoned".into()))?;
        if state.status.identity != request.identity {
            return Err(Error::Conflict);
        }
        if state.workload_finished.as_ref() == Some(&request) {
            return Ok(state.status.clone());
        }
        if state.status.requested_checkpoint.as_deref() != Some(&request.operation_id) {
            return Err(Error::Conflict);
        }
        if request.error.is_none()
            && (state.status.phase != RuntimePhase::Running
                || state.status.checkpoint.as_ref().is_none_or(|cp| {
                    cp.operation_id != request.operation_id || cp.phase != CheckpointPhase::Resumed
                }))
        {
            return Err(Error::Conflict);
        }
        let result = request.error.clone().map_or(Ok(()), Err);
        if let Some(reply) = state.workload_reply.take() {
            reply.send_replace(Some(result));
        }
        state.status.requested_checkpoint = None;
        state.status.requested_checkpoint_deadline_unix_millis = None;
        state.workload_finished = Some(request);
        self.publish(&mut state);
        Ok(state.status.clone())
    }
    pub fn status(&self) -> RuntimeStatus {
        let mut status = self.state.lock().unwrap().status.clone();
        status.active_requests = super::activity::active_count().max(0) as u64;
        status.active_commands = super::activity::active_command_count().max(0) as u64;
        status.activity_revision = super::activity::revision();
        status
    }
    pub fn probe_status(&self) -> Result<(RuntimeStatus, u64)> {
        let state = self
            .state
            .lock()
            .map_err(|_| Error::Unavailable("runtime state lock poisoned".into()))?;
        Ok((state.status.clone(), state.probe_epoch))
    }
    /// Update a deployment-owned guest service without changing the platform
    /// runtime phase. A failed auxiliary service must not erase Execd's
    /// control path, which is still needed for diagnosis and deletion.
    pub fn set_required_service(
        &self,
        name: impl Into<String>,
        service: RequiredServiceStatus,
    ) -> Result<()> {
        let name = name.into();
        if name.trim().is_empty() {
            return Err(Error::Invalid("required service name is empty".into()));
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::Unavailable("runtime state poisoned".into()))?;
        state.status.required_services.insert(name, service);
        self.publish(&mut state);
        Ok(())
    }
    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.changed.subscribe()
    }
    fn publish(&self, state: &mut State) {
        if state.probe_phase != state.status.phase {
            state.probe_epoch += 1;
            state.probe_phase = state.status.phase;
        }
        state.status.revision += 1;
        self.changed.send_replace(state.status.revision);
    }
    pub async fn prepare(self: &Arc<Self>, request: PrepareCheckpoint) -> Result<RuntimeStatus> {
        if request.operation_id.trim().is_empty() {
            return Err(Error::Invalid("operation id required".into()));
        }
        let operation_id = request.operation_id.clone();
        let mut changes = self.subscribe();
        {
            let mut state = self.state.lock().unwrap();
            if state
                .status
                .requested_checkpoint
                .as_ref()
                .is_some_and(|id| id != &request.operation_id)
            {
                return Err(Error::Conflict);
            }
            if request.identity != state.status.identity {
                return Err(Error::Conflict);
            }
            if state
                .prepare
                .as_ref()
                .is_some_and(|old| old.operation_id == request.operation_id)
            {
                if state.prepare.as_ref() != Some(&request) {
                    return Err(Error::Conflict);
                }
            } else {
                if state.status.phase != RuntimePhase::Running
                    || request.expected_revision != state.status.revision
                {
                    return Err(Error::Conflict);
                }
                if let Some(revision) = request.idle_revision {
                    super::activity::quiesce(revision)?;
                }
                state.status.phase = RuntimePhase::Preparing;
                state.status.checkpoint = Some(CheckpointStatus {
                    operation_id: request.operation_id.clone(),
                    phase: CheckpointPhase::Preparing,
                    error: None,
                });
                state.prepare = Some(request);
                self.publish(&mut state);
                let controller = self.clone();
                let preparation_operation = operation_id.clone();
                // Accepted preparation survives cancellation of either transport.
                tokio::spawn(async move {
                    controller.prepare_guest(preparation_operation).await;
                });
            }
        }
        loop {
            let status = self.status();
            if status
                .checkpoint
                .as_ref()
                .is_none_or(|checkpoint| checkpoint.operation_id != operation_id)
            {
                return Err(Error::Conflict);
            }
            if status.phase != RuntimePhase::Preparing {
                return Ok(status);
            }
            changes
                .changed()
                .await
                .map_err(|_| Error::Unavailable("control stopped".into()))?;
        }
    }
    async fn prepare_guest(self: Arc<Self>, operation_id: String) {
        // Serialize guest preparation, rollback, and backend completion. A retained
        // reader can finish while a later operation is preparing guest services.
        let _cooperation = self.cooperation.lock().await;
        if self.status().phase != RuntimePhase::Preparing {
            return;
        }
        let open_reader = !self.status_reader_running();
        let reader = if open_reader {
            execd_info!(
                "[checkpoint-control] handoff_task_started operation={}",
                operation_id
            );
            match super::checkpoint_handoff::open(self.hooks.clone(), operation_id.clone()).await {
                Ok(reader) => {
                    execd_info!(
                        "[checkpoint-control] handoff_open_joined operation={}",
                        operation_id
                    );
                    Some(reader)
                }
                Err(error) => {
                    self.fail_preparation(error.to_string(), RuntimePhase::Running);
                    super::activity::reopen();
                    return;
                }
            }
        } else {
            None
        };
        if let Err(error) = self.hooks.prepare_checkpoint().await {
            let rollback = self.hooks.rollback_checkpoint_prepare().await;
            let (phase, message) = match rollback {
                Ok(()) => (RuntimePhase::Running, error.to_string()),
                Err(rollback) => (
                    RuntimePhase::Failed,
                    format!("{error}; rollback failed: {rollback}"),
                ),
            };
            // Preserve the handoff reader even when guest preparation fails.
            if let Some(reader) = reader {
                self.spawn_reader(reader, operation_id);
            }
            self.fail_preparation(message, phase);
            return;
        }
        {
            let Ok(mut state) = self.state.lock() else {
                return;
            };
            state.status.phase = RuntimePhase::Prepared;
            if let Some(checkpoint) = &mut state.status.checkpoint {
                checkpoint.phase = CheckpointPhase::Prepared;
            }
            self.publish(&mut state);
            execd_info!(
                "[checkpoint-control] prepared_published operation={} revision={}",
                operation_id,
                state.status.revision
            );
        }
        if let Some(reader) = reader {
            self.spawn_reader(reader, operation_id);
        }
    }
    fn status_reader_running(&self) -> bool {
        self.state
            .lock()
            .map(|state| state.reader_running)
            .unwrap_or(true)
    }
    fn spawn_reader(self: &Arc<Self>, reader: Handoff, operation_id: String) {
        if let Ok(mut state) = self.state.lock() {
            state.reader_running = true;
        }
        let controller = self.clone();
        tokio::spawn(async move {
            controller.read_handoff(reader, operation_id).await;
        });
    }
    fn fail_preparation(&self, error: String, phase: RuntimePhase) {
        if let Ok(mut state) = self.state.lock() {
            state.status.phase = phase;
            if let Some(checkpoint) = &mut state.status.checkpoint {
                checkpoint.phase = CheckpointPhase::Failed;
                checkpoint.error = Some(error);
            }
            self.publish(&mut state);
        }
    }
    async fn read_handoff(self: Arc<Self>, reader: Handoff, operation_id: String) {
        execd_info!(
            "[checkpoint-control] handoff_wait_started operation={}",
            operation_id
        );
        let outcome = reader.await;
        execd_info!(
            "[checkpoint-control] handoff_wait_completed operation={} outcome={:?}",
            operation_id,
            outcome
        );
        let _cooperation = self.cooperation.lock().await;
        let previous = {
            let mut state = self.state.lock().unwrap();
            state.reader_running = false;
            if state.status.phase != RuntimePhase::Prepared {
                state.status.phase = RuntimePhase::Failed;
                if let Some(checkpoint) = &mut state.status.checkpoint {
                    checkpoint.phase = CheckpointPhase::Failed;
                    checkpoint.error =
                        Some("handoff received without an active prepared checkpoint".into());
                }
                self.publish(&mut state);
                return;
            }
            if matches!(outcome, Ok(HandoffOutcome::Restore)) {
                if let Some(reply) = state.workload_reply.take() {
                    reply.send_replace(Some(Err(
                        "source checkpoint request retired on restore".into()
                    )));
                }
                state.status.requested_checkpoint = None;
                state.status.requested_checkpoint_deadline_unix_millis = None;
                state.workload_finished = None;
                state.status.phase = RuntimePhase::Restoring;
                self.publish(&mut state);
            }
            state.status.identity.clone()
        };
        let restored = if matches!(outcome, Ok(HandoffOutcome::Restore)) {
            Some(
                self.hooks
                    .restore_context_async(&previous)
                    .await
                    .and_then(|restored| {
                        restored.validate(&previous).map_err(io::Error::other)?;
                        Ok(restored.target)
                    }),
            )
        } else {
            None
        };
        let released = match &outcome {
            Ok(HandoffOutcome::Resume | HandoffOutcome::Error) => {
                self.hooks.resume_after_checkpoint().await
            }
            _ => Ok(()),
        };
        let mut state = self.state.lock().unwrap();
        state.status.phase = self.handoff_probe_phase();
        state.status.failure = None;
        super::activity::reopen();
        let (phase, error) = if let Err(error) = released {
            state.status.phase = RuntimePhase::Failed;
            (CheckpointPhase::Failed, Some(error.to_string()))
        } else {
            match (outcome, restored) {
                (Ok(HandoffOutcome::Restore), Some(Ok(identity))) => {
                    state.status.identity = identity;
                    (CheckpointPhase::Restored, None)
                }
                (Ok(HandoffOutcome::Restore), Some(Err(error))) => {
                    state.status.phase = RuntimePhase::Failed;
                    (CheckpointPhase::Failed, Some(error.to_string()))
                }
                (Ok(HandoffOutcome::Resume), _) => (CheckpointPhase::Resumed, None),
                (Ok(HandoffOutcome::Error), _) => (
                    CheckpointPhase::Failed,
                    Some("backend handoff reported error".into()),
                ),
                (Err(error), _) => {
                    state.status.phase = RuntimePhase::Failed;
                    (CheckpointPhase::Failed, Some(error.to_string()))
                }
                _ => unreachable!(),
            }
        };
        if let Some(checkpoint) = &mut state.status.checkpoint {
            checkpoint.phase = phase;
            checkpoint.error = error;
        }
        self.publish(&mut state);
    }
    /// Caller may use this only when the backend confirms checkpoint never started.
    /// Keep the existing reader: the next attempt must not leak another reader.
    pub async fn abort_unstarted(
        self: &Arc<Self>,
        operation_id: &str,
        identity: &RuntimeIdentity,
        revision: u64,
    ) -> Result<RuntimeStatus> {
        let controller = self.clone();
        let operation_id = operation_id.to_owned();
        let identity = identity.clone();
        // Own the rollback independently of the HTTP/stream request lifetime.
        tokio::spawn(async move {
            controller
                .abort_guest(&operation_id, &identity, revision)
                .await
        })
        .await
        .map_err(|error| Error::Unavailable(error.to_string()))?
    }
    async fn abort_guest(
        &self,
        operation_id: &str,
        identity: &RuntimeIdentity,
        revision: u64,
    ) -> Result<RuntimeStatus> {
        let _cooperation = self.cooperation.lock().await;
        {
            let state = self
                .state
                .lock()
                .map_err(|_| Error::Unavailable("runtime state poisoned".into()))?;
            if &state.status.identity != identity
                || state
                    .status
                    .checkpoint
                    .as_ref()
                    .is_none_or(|checkpoint| checkpoint.operation_id != operation_id)
            {
                return Err(Error::Conflict);
            }
            if state
                .status
                .checkpoint
                .as_ref()
                .is_some_and(|checkpoint| checkpoint.phase == CheckpointPhase::Aborted)
            {
                return Ok(state.status.clone());
            }
            if state.status.revision != revision || state.status.phase != RuntimePhase::Prepared {
                return Err(Error::Conflict);
            }
        }
        let reopen_admission = self
            .state
            .lock()
            .map_err(|_| Error::Unavailable("runtime state poisoned".into()))?
            .prepare
            .as_ref()
            .is_some_and(|prepare| prepare.idle_revision.is_some());
        let released = self.hooks.rollback_checkpoint_prepare().await;
        if released.is_ok() && reopen_admission {
            super::activity::reopen();
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::Unavailable("runtime state poisoned".into()))?;
        state.status.phase = if released.is_ok() {
            self.handoff_probe_phase()
        } else {
            RuntimePhase::Failed
        };
        if let Some(checkpoint) = &mut state.status.checkpoint {
            checkpoint.phase = if released.is_ok() {
                CheckpointPhase::Aborted
            } else {
                CheckpointPhase::Failed
            };
            checkpoint.error = released.err().map(|error| error.to_string());
        }
        self.publish(&mut state);
        Ok(state.status.clone())
    }
}
