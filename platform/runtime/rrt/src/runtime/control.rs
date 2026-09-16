//! HTTP control state independent of transport connections and request lifetime.
use adx_core::{runtime::*, Error, Result};
use std::{
    future::Future,
    io,
    pin::Pin,
    sync::{Arc, Mutex, OnceLock},
};
use tokio::sync::watch;

pub type Handoff = Pin<Box<dyn Future<Output = io::Result<HandoffOutcome>> + Send>>;
#[derive(Debug, Clone, Copy)]
pub enum HandoffOutcome {
    Resume,
    Restore,
    Error,
}
pub trait CheckpointHooks: Send + Sync + 'static {
    /// Open the backend's handoff before publishing Prepared.
    fn open(&self) -> io::Result<Handoff>;
    /// Validate restored identity, refresh environment and rearm listeners.
    fn restore(&self, previous: &RuntimeIdentity) -> io::Result<RuntimeIdentity>;
    fn restore_context(&self, previous: &RuntimeIdentity) -> io::Result<RuntimeRestore> {
        Ok(RuntimeRestore {
            target: self.restore(previous)?,
            origin: None,
        })
    }
}
struct State {
    status: RuntimeStatus,
    prepare: Option<PrepareCheckpoint>,
    reader_running: bool,
}
pub struct Controller {
    state: Mutex<State>,
    hooks: Arc<dyn CheckpointHooks>,
    changed: watch::Sender<u64>,
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
                    identity,
                    revision: 1,
                    phase: RuntimePhase::Running,
                    checkpoint: None,
                    active_requests: 0,
                    active_commands: 0,
                    activity_revision: 1,
                },
                prepare: None,
                reader_running: false,
            }),
            hooks,
            changed,
        }))
    }
    pub fn status(&self) -> RuntimeStatus {
        let mut status = self.state.lock().unwrap().status.clone();
        status.active_requests = super::activity::active_count().max(0) as u64;
        status.active_commands = super::activity::active_command_count().max(0) as u64;
        status.activity_revision = super::activity::revision();
        status
    }
    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.changed.subscribe()
    }
    fn publish(&self, state: &mut State) {
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
                let prepared = state.reader_running;
                state.status.phase = if prepared {
                    RuntimePhase::Prepared
                } else {
                    RuntimePhase::Preparing
                };
                state.status.checkpoint = Some(CheckpointStatus {
                    operation_id: request.operation_id.clone(),
                    phase: if prepared {
                        CheckpointPhase::Prepared
                    } else {
                        CheckpointPhase::Preparing
                    },
                    error: None,
                });
                state.prepare = Some(request);
                self.publish(&mut state);
                if !prepared {
                    state.reader_running = true;
                    let controller = self.clone();
                    // The accepted operation survives cancellation of the HTTP request.
                    tokio::spawn(async move {
                        controller.read_handoff().await;
                    });
                }
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
    async fn read_handoff(self: Arc<Self>) {
        let hooks = self.hooks.clone();
        let opened = tokio::task::spawn_blocking(move || hooks.open()).await;
        let reader = match opened {
            Ok(Ok(reader)) => reader,
            error => {
                let mut state = self.state.lock().unwrap();
                state.reader_running = false;
                state.status.phase = RuntimePhase::Running;
                if let Some(checkpoint) = &mut state.status.checkpoint {
                    checkpoint.phase = CheckpointPhase::Failed;
                    checkpoint.error = Some(match error {
                        Ok(Err(error)) => error.to_string(),
                        Err(error) => error.to_string(),
                        _ => unreachable!(),
                    });
                }
                self.publish(&mut state);
                return;
            }
        };
        {
            let mut state = self.state.lock().unwrap();
            if state.status.phase == RuntimePhase::Preparing {
                state.status.phase = RuntimePhase::Prepared;
                state.status.checkpoint.as_mut().unwrap().phase = CheckpointPhase::Prepared;
                self.publish(&mut state);
            }
        }
        let outcome = reader.await;
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
                state.status.phase = RuntimePhase::Restoring;
                self.publish(&mut state);
            }
            state.status.identity.clone()
        };
        let restored = if matches!(outcome, Ok(HandoffOutcome::Restore)) {
            Some(self.hooks.restore_context(&previous).and_then(|restored| {
                restored.validate(&previous).map_err(io::Error::other)?;
                Ok(restored.target)
            }))
        } else {
            None
        };
        let mut state = self.state.lock().unwrap();
        state.status.phase = RuntimePhase::Running;
        let (phase, error) = match (outcome, restored) {
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
        };
        if let Some(checkpoint) = &mut state.status.checkpoint {
            checkpoint.phase = phase;
            checkpoint.error = error;
        }
        self.publish(&mut state);
    }
    /// Caller may use this only when the backend confirms checkpoint never started.
    /// Keep the existing reader: the next attempt must not leak another reader.
    pub fn abort_unstarted(
        &self,
        operation_id: &str,
        identity: &RuntimeIdentity,
        revision: u64,
    ) -> Result<RuntimeStatus> {
        let mut state = self.state.lock().unwrap();
        if &state.status.identity != identity
            || state
                .status
                .checkpoint
                .as_ref()
                .is_none_or(|checkpoint| checkpoint.operation_id != operation_id)
        {
            return Err(Error::Conflict);
        }
        if state.status.checkpoint.as_ref().unwrap().phase == CheckpointPhase::Aborted {
            return Ok(state.status.clone());
        }
        if state.status.revision != revision
            || !matches!(state.status.phase, RuntimePhase::Prepared)
        {
            return Err(Error::Conflict);
        }
        state.status.phase = RuntimePhase::Running;
        state.status.checkpoint.as_mut().unwrap().phase = CheckpointPhase::Aborted;
        self.publish(&mut state);
        Ok(state.status.clone())
    }
}
