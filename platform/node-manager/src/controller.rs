use super::{Durability, OperationResult, Services};
use adx_core::{
    Assignment, CapsuleRecord, CapsuleSpec, CapsuleState, Error, Event, LifecycleKind, Result,
};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;

mod lifecycle;
mod monitor;
mod snapshots;
use super::checkpoint::{PauseRequest, ResumeRequest};

enum Command {
    Snapshot(
        super::checkpoint::SnapshotRequest,
        oneshot::Sender<Result<super::checkpoint::SnapshotResult>>,
    ),
    Pause(PauseRequest),
    Resume(ResumeRequest),
    Network {
        policy: Option<adx_core::sandbox::NetworkPolicy>,
        operation_id: String,
        expected_revision: u64,
    },
    Reload {
        operation_id: String,
        expected_revision: u64,
    },
    Recover(ResumeRequest),
    Create,
    Clone(Box<adx_core::snapshots::Snapshot>),
    Delete,
    Sync,
    Reconcile,
    Discard(oneshot::Sender<Result<()>>),
    Expire(u64, oneshot::Sender<Result<()>>),
    Tick(oneshot::Sender<Result<()>>),
}
type Reply = oneshot::Sender<Result<OperationResult>>;
struct Envelope {
    command: Command,
    reply: Reply,
    trace: adx_observability::trace::Trace,
}
impl Envelope {
    fn new(command: Command, reply: Reply) -> Self {
        Self {
            command,
            reply,
            trace: adx_observability::trace::Trace::child("capsule.queue"),
        }
    }
}

#[derive(Clone)]
pub struct CapsuleHandle {
    tx: mpsc::Sender<Envelope>,
}

impl CapsuleHandle {
    pub async fn snapshot(
        &self,
        request: super::checkpoint::SnapshotRequest,
    ) -> Result<super::checkpoint::SnapshotResult> {
        let (tx, rx) = oneshot::channel();
        let (unused, _) = oneshot::channel();
        self.tx
            .send(Envelope::new(Command::Snapshot(request, tx), unused))
            .await
            .map_err(|_| Error::Unavailable("capsule controller stopped".into()))?;
        rx.await
            .map_err(|_| Error::Unavailable("capsule controller stopped".into()))?
    }

    async fn send(&self, command: Command) -> Result<OperationResult> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Envelope::new(command, tx))
            .await
            .map_err(|_| Error::Unavailable("capsule controller stopped".into()))?;
        rx.await
            .map_err(|_| Error::Unavailable("capsule controller stopped".into()))?
    }
    pub async fn pause(&self, request: PauseRequest) -> Result<OperationResult> {
        self.send(Command::Pause(request)).await
    }
    pub async fn resume(&self, request: ResumeRequest) -> Result<OperationResult> {
        self.send(Command::Resume(request)).await
    }
    pub async fn update_network_policy(
        &self,
        policy: Option<adx_core::sandbox::NetworkPolicy>,
        operation_id: String,
        expected_revision: u64,
    ) -> Result<OperationResult> {
        self.send(Command::Network {
            policy,
            operation_id,
            expected_revision,
        })
        .await
    }
    pub async fn reload(
        &self,
        operation_id: String,
        expected_revision: u64,
    ) -> Result<OperationResult> {
        self.send(Command::Reload {
            operation_id,
            expected_revision,
        })
        .await
    }
    pub(crate) async fn recover(&self, request: ResumeRequest) -> Result<OperationResult> {
        self.send(Command::Recover(request)).await
    }
    pub async fn create_from_snapshot(
        &self,
        snapshot: adx_core::snapshots::Snapshot,
    ) -> Result<OperationResult> {
        self.send(Command::Clone(Box::new(snapshot))).await
    }
    pub async fn create(&self) -> Result<OperationResult> {
        self.send(Command::Create).await
    }
    pub async fn delete(&self) -> Result<OperationResult> {
        self.send(Command::Delete).await
    }
    pub(crate) async fn reconcile(&self) -> Result<OperationResult> {
        self.send(Command::Reconcile).await
    }
    pub(crate) async fn discard(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        let (unused, _) = oneshot::channel();
        self.tx
            .send(Envelope::new(Command::Discard(tx), unused))
            .await
            .map_err(|_| Error::Unavailable("capsule controller stopped".into()))?;
        rx.await
            .map_err(|_| Error::Unavailable("capsule controller stopped".into()))?
    }
    pub async fn expire_checkpoint(&self, now: u64) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        let (unused, _) = oneshot::channel();
        self.tx
            .send(Envelope::new(Command::Expire(now, tx), unused))
            .await
            .map_err(|_| Error::Unavailable("capsule controller stopped".into()))?;
        rx.await
            .map_err(|_| Error::Unavailable("capsule controller stopped".into()))?
    }
    pub async fn tick(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        let (unused, _) = oneshot::channel();
        self.tx
            .send(Envelope::new(Command::Tick(tx), unused))
            .await
            .map_err(|_| Error::Unavailable("capsule controller stopped".into()))?;
        rx.await
            .map_err(|_| Error::Unavailable("capsule controller stopped".into()))?
    }
    pub async fn sync(&self) -> Result<OperationResult> {
        self.send(Command::Sync).await
    }
}

pub(crate) fn spawn(
    spec: CapsuleSpec,
    assignment: Assignment,
    services: Arc<Services>,
    held: bool,
) -> CapsuleHandle {
    let runtime_id = format!("{}-{}", spec.id, assignment.generation);
    spawn_restored(
        CapsuleRecord {
            restart_attempts: 0,
            restart_pending: false,
            spec,
            assignment,
            runtime: adx_core::Runtime {
                id: runtime_id,
                ip: None,
            },
            state: CapsuleState::Pending,
            revision: 0,
            resources_held: held,
            checkpoint: None,
            last_operation: None,
        },
        services,
    )
}

pub(crate) fn spawn_restored(record: CapsuleRecord, services: Arc<Services>) -> CapsuleHandle {
    let (tx, mut rx) = mpsc::channel::<Envelope>(32);
    let mut controller = Controller {
        recovery_files: None,
        held: record.resources_held,
        record,
        services,
        durability: None,
        retired: false,
        obsolete_checkpoints: Vec::new(),
        idle: None,
        restart_after: None,
        health_failures: 0,
    };
    tokio::spawn(async move {
        while let Some(Envelope {
            command,
            reply,
            trace,
        }) = rx.recv().await
        {
            trace.attribute("capsule.id", controller.record.spec.id.clone());
            trace
                .run(async {
                    adx_observability::trace::Trace::child("capsule.execute")
                        .run(controller.dispatch(command, reply))
                        .await;
                })
                .await;
        }
    });
    CapsuleHandle { tx }
}

impl Controller {
    async fn dispatch(&mut self, command: Command, reply: Reply) {
        let command = match command {
            Command::Snapshot(request, ack) => {
                let result = if self.retired {
                    Err(Error::Conflict)
                } else {
                    self.snapshot(request).await
                };
                let _ = ack.send(result);
                return;
            }
            Command::Tick(ack) => {
                let result = if self.retired {
                    Err(Error::Conflict)
                } else {
                    self.tick().await
                };
                let _ = ack.send(result);
                return;
            }
            Command::Expire(now, ack) => {
                let result = if self.retired {
                    Err(Error::Conflict)
                } else {
                    self.expire_checkpoint(now).await
                };
                let _ = ack.send(result);
                return;
            }
            Command::Discard(ack) => {
                let result = if self.retired {
                    Ok(())
                } else {
                    self.cleanup().await
                };
                if result.is_ok() {
                    self.retired = true;
                }
                let _ = ack.send(result);
                return;
            }
            command => command,
        };
        if self.retired {
            let _ = reply.send(Err(Error::Conflict));
            return;
        }
        let operation = match &command {
            Command::Pause(_) => "pause",
            Command::Resume(_) => "resume",
            Command::Network { .. } => "network",
            Command::Reload { .. } => "reload",
            Command::Recover(_) => "recover",
            Command::Create | Command::Clone(_) => "create",
            Command::Delete => "delete",
            Command::Sync => "sync",
            Command::Reconcile => "reconcile",
            _ => unreachable!(),
        };
        let result = match command {
            Command::Snapshot(_, _)
            | Command::Discard(_)
            | Command::Expire(_, _)
            | Command::Tick(_) => unreachable!(),
            Command::Pause(request) => self.pause(request).await,
            Command::Resume(request) => self.resume(request).await,
            Command::Network {
                policy,
                operation_id,
                expected_revision,
            } => {
                self.update_network_policy(policy, operation_id, expected_revision)
                    .await
            }
            Command::Reload {
                operation_id,
                expected_revision,
            } => self.reload(operation_id, expected_revision).await,
            Command::Recover(request) => self.recover(request).await,
            Command::Create => self.create().await,
            Command::Clone(snapshot) => self.clone_snapshot(*snapshot).await,
            Command::Delete => self.delete().await,
            Command::Sync => self.sync().await,
            Command::Reconcile => self.reconcile().await,
        };
        if let Err(error) = &result {
            adx_observability::trace::error();
            eprintln!(
                "{}",
                serde_json::json!({
                    "event": "capsule operation failed",
                    "unix_seconds": crate::checkpoint::now().ok(),
                    "capsule_id": self.record.spec.id,
                    "runtime_id": self.record.runtime.id,
                    "revision": self.record.revision,
                    "operation": operation,
                    "error": error.to_string(),
                })
            );
        }
        // Dropping the caller's reply never cancels an accepted operation.
        let _ = reply.send(result);
    }
}

struct Controller {
    recovery_files: Option<crate::checkpoint::MaterializedCheckpoint>,
    record: CapsuleRecord,
    services: Arc<Services>,
    held: bool,
    durability: Option<Durability>,
    retired: bool,
    obsolete_checkpoints: Vec<adx_core::CheckpointArtifact>,
    idle: Option<((String, u64, u64), tokio::time::Instant)>,
    restart_after: Option<tokio::time::Instant>,
    health_failures: u32,
}

impl Controller {
    async fn reconcile(&mut self) -> Result<OperationResult> {
        if self.record.state == CapsuleState::Running
            && self
                .services
                .runtime
                .is_running(&self.record.runtime.id)
                .await?
        {
            if self.recovery_files.is_none() {
                if let (Some(cp), Some(services)) =
                    (&self.record.checkpoint, &self.services.checkpoint)
                {
                    self.recovery_files = Some(services.store.materialize(&cp.artifact).await?);
                }
            }
            tokio::time::timeout(self.services.operation_timeout, async {
                self.services.readiness.wait_ready(&self.record).await?;
                self.services.routes.activate(&self.record).await
            })
            .await
            .map_err(|_| Error::Unavailable("recovery readiness timed out".into()))??;
        } else if self.record.state == CapsuleState::Paused {
            let cp = self.record.checkpoint.clone().ok_or(Error::Conflict)?;
            let services = self.services.checkpoint.clone().ok_or(Error::Conflict)?;
            self.cleanup().await?;
            if cp.expires_at_unix_seconds <= super::checkpoint::now()? {
                return self.delete().await;
            }
            match services.store.validate_artifact(&cp.artifact).await {
                Ok(()) => (),
                Err(Error::Invalid(_) | Error::NotFound) => self.transition(Event::Fail)?,
                Err(error) => return Err(error),
            }
        } else {
            self.cleanup().await?;
            if !matches!(
                self.record.state,
                CapsuleState::Deleted | CapsuleState::Failed
            ) {
                self.record.state = CapsuleState::Failed;
                self.record.revision =
                    self.record.revision.checked_add(1).ok_or(Error::Conflict)?;
            } else if self.record.state == CapsuleState::Failed {
                self.record.revision =
                    self.record.revision.checked_add(1).ok_or(Error::Conflict)?;
            }
        }
        self.sync().await
    }

    fn transition(&mut self, event: Event) -> Result<()> {
        let next = self.record.state.apply(event)?;
        self.record.revision = self.record.revision.checked_add(1).ok_or(Error::Conflict)?;
        self.record.state = next;
        self.record.last_operation = None;
        self.idle = None;
        self.health_failures = 0;
        self.durability = None;
        Ok(())
    }

    async fn sync(&mut self) -> Result<OperationResult> {
        if matches!(
            self.record.state,
            CapsuleState::Pending
                | CapsuleState::Starting
                | CapsuleState::Deleting
                | CapsuleState::Pausing
                | CapsuleState::Resuming
        ) {
            return Err(Error::Conflict);
        }
        let durability = timeout(
            self.services.operation_timeout,
            self.services.sink.commit(&self.record),
        )
        .await
        .map_err(|_| Error::Unavailable("state commit timed out".into()))??;
        self.durability = Some(durability);
        self.clean_obsolete_checkpoints().await?;
        Ok(OperationResult {
            record: self.record.clone(),
            durability,
        })
    }

    async fn replay_or_sync(&mut self) -> Result<OperationResult> {
        if self.durability == Some(Durability::Published) {
            self.clean_obsolete_checkpoints().await?;
            Ok(OperationResult {
                record: self.record.clone(),
                durability: Durability::Published,
            })
        } else {
            self.sync().await
        }
    }

    async fn clean_obsolete_checkpoints(&mut self) -> Result<()> {
        if self.durability != Some(Durability::Published) {
            return Ok(());
        }
        if let (Some(cp), Some(services)) = (&self.record.checkpoint, &self.services.checkpoint) {
            services.store.committed(&cp.artifact).await?;
        }
        while let Some(artifact) = self.obsolete_checkpoints.last() {
            self.services
                .checkpoint
                .as_ref()
                .ok_or(Error::Conflict)?
                .store
                .remove(artifact)
                .await?;
            self.obsolete_checkpoints.pop();
        }
        Ok(())
    }

    async fn create(&mut self) -> Result<OperationResult> {
        if self.record.state == CapsuleState::Running {
            return self.replay_or_sync().await;
        }
        if self.record.state != CapsuleState::Pending {
            return Err(Error::Conflict);
        }
        if self.record.spec.snapshot_id.is_some() && self.record.checkpoint.is_none() {
            return Err(Error::Unavailable(
                "snapshot source required before starting".into(),
            ));
        }
        self.start_attempt(false, false, None).await
    }

    async fn start_attempt(
        &mut self,
        restart: bool,
        restore_checkpoint: bool,
        completion: Option<(String, u64, LifecycleKind)>,
    ) -> Result<OperationResult> {
        let runtime_id = if restart {
            format!(
                "{}-{}-r{}",
                self.record.spec.id,
                self.record.assignment.generation,
                self.record.revision.checked_add(1).ok_or(Error::Conflict)?
            )
        } else {
            self.record.runtime.id.clone()
        };
        if !self.held || restart {
            self.services
                .admission
                .lock()
                .expect("shared state lock poisoned")
                .reserve(&runtime_id, &self.record.spec, &self.record.assignment)?;
        }
        if restart {
            self.record.runtime.id = runtime_id;
            self.record.runtime.ip = None;
            // restart_attempts belongs to the configured cold-restart policy.
            // Checkpoint replacement (explicit reload or failover) has its own
            // recovery contract and must not be rejected by Master as a cold
            // restart from a non-Failed record.
            if !restore_checkpoint {
                self.record.restart_attempts = self
                    .record
                    .restart_attempts
                    .checked_add(1)
                    .ok_or(Error::Conflict)?;
            }
        }
        self.held = true;
        self.record.resources_held = true;
        self.transition(Event::Start)?;
        let attempt = timeout(self.services.operation_timeout, async {
            self.record.runtime.ip = Some(
                if restore_checkpoint || self.record.spec.snapshot_id.is_some() {
                    let cp = self.record.checkpoint.as_ref().ok_or(Error::Conflict)?;
                    let store = &self
                        .services
                        .checkpoint
                        .as_ref()
                        .ok_or(Error::Conflict)?
                        .store;
                    let path = store.materialize(&cp.artifact).await?;
                    // Firecracker reads restore files after the RPC has returned.
                    self.recovery_files = Some(path);
                    self.services
                        .runtime
                        .restore_from(
                            &self.record.spec,
                            &self.record.runtime.id,
                            self.record.assignment.generation,
                            &self.record.assignment.devices,
                            self.recovery_files
                                .as_ref()
                                .expect("recovery file is stored immediately before restore"),
                            cp.origin.as_ref(),
                        )
                        .await?
                } else {
                    self.services
                        .runtime
                        .start(
                            &self.record.spec,
                            &self.record.runtime.id,
                            self.record.assignment.generation,
                            &self.record.assignment.devices,
                        )
                        .await?
                },
            );
            self.services.readiness.wait_ready(&self.record).await?;
            self.services.routes.activate(&self.record).await
        })
        .await
        .unwrap_or_else(|_| Err(Error::Unavailable("capsule start timed out".into())));
        if let Err(start_error) = attempt {
            let cleanup = self.cleanup().await;
            self.transition(Event::Fail)?;
            self.record.restart_pending = restart
                && !restore_checkpoint
                && self
                    .record
                    .spec
                    .lifecycle
                    .restart
                    .as_ref()
                    .is_some_and(|p| self.record.restart_attempts < p.max_attempts);
            self.restart_after = None;
            let commit = self.sync().await;
            return match (cleanup, commit) {
                (Ok(()), Ok(_)) => Err(start_error),
                (cleanup, commit) => Err(Error::Unavailable(format!(
                    "start failed: {start_error}; cleanup: {cleanup:?}; commit: {}",
                    if commit.is_ok() {
                        "accepted".into()
                    } else {
                        format!("{:?}", commit.err())
                    }
                ))),
            };
        }
        self.transition(Event::Ready)?;
        if let Some((id, revision, kind)) = completion {
            self.completed(id, revision, kind);
        }
        self.record.restart_pending = false;
        self.restart_after = None;
        self.sync().await
    }

    async fn cleanup(&mut self) -> Result<()> {
        timeout(self.services.operation_timeout, async {
            self.services.routes.retire(&self.record).await?;
            self.services.runtime.remove(&self.record.runtime.id).await
        })
        .await
        .map_err(|_| Error::Unavailable("capsule cleanup timed out".into()))??;
        self.recovery_files = None;
        self.services.metrics.remove(&self.record.spec.id);
        if self.held {
            self.services
                .admission
                .lock()
                .expect("shared state lock poisoned")
                .release(&self.record.runtime.id)?;
            self.held = false;
            self.record.resources_held = false;
        }
        Ok(())
    }

    async fn delete(&mut self) -> Result<OperationResult> {
        self.record.restart_pending = false;
        self.restart_after = None;
        if self.record.state == CapsuleState::Deleted {
            return self.replay_or_sync().await;
        }
        self.record.last_operation = None;
        if self.record.state != CapsuleState::Deleting {
            self.transition(Event::Delete)?;
        }
        if let Err(error) = self.cleanup().await {
            self.transition(Event::Fail)?;
            return match self.sync().await {
                Ok(_) => Err(error),
                Err(commit) => Err(Error::Unavailable(format!(
                    "cleanup failed: {error}; commit failed: {commit}"
                ))),
            };
        }
        if let Some(cp) = self.record.checkpoint.take() {
            self.obsolete_checkpoints.push(cp.artifact);
        }
        self.record.runtime.ip = None;
        self.transition(Event::Removed)?;
        self.sync().await
    }
}
