use super::{Durability, OperationResult, Services};
use adx_core::{Assignment, Error, Event, InstanceRecord, InstanceSpec, InstanceState, Result};
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

#[derive(Clone)]
pub struct InstanceHandle {
    tx: mpsc::Sender<(Command, Reply)>,
}

impl InstanceHandle {
    pub async fn snapshot(
        &self,
        request: super::checkpoint::SnapshotRequest,
    ) -> Result<super::checkpoint::SnapshotResult> {
        let (tx, rx) = oneshot::channel();
        let (unused, _) = oneshot::channel();
        self.tx
            .send((Command::Snapshot(request, tx), unused))
            .await
            .map_err(|_| Error::Unavailable("instance controller stopped".into()))?;
        rx.await
            .map_err(|_| Error::Unavailable("instance controller stopped".into()))?
    }

    async fn send(&self, command: Command) -> Result<OperationResult> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send((command, tx))
            .await
            .map_err(|_| Error::Unavailable("instance controller stopped".into()))?;
        rx.await
            .map_err(|_| Error::Unavailable("instance controller stopped".into()))?
    }
    pub async fn pause(&self, request: PauseRequest) -> Result<OperationResult> {
        self.send(Command::Pause(request)).await
    }
    pub async fn resume(&self, request: ResumeRequest) -> Result<OperationResult> {
        self.send(Command::Resume(request)).await
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
            .send((Command::Discard(tx), unused))
            .await
            .map_err(|_| Error::Unavailable("instance controller stopped".into()))?;
        rx.await
            .map_err(|_| Error::Unavailable("instance controller stopped".into()))?
    }
    pub async fn expire_checkpoint(&self, now: u64) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        let (unused, _) = oneshot::channel();
        self.tx
            .send((Command::Expire(now, tx), unused))
            .await
            .map_err(|_| Error::Unavailable("instance controller stopped".into()))?;
        rx.await
            .map_err(|_| Error::Unavailable("instance controller stopped".into()))?
    }
    pub async fn tick(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        let (unused, _) = oneshot::channel();
        self.tx
            .send((Command::Tick(tx), unused))
            .await
            .map_err(|_| Error::Unavailable("instance controller stopped".into()))?;
        rx.await
            .map_err(|_| Error::Unavailable("instance controller stopped".into()))?
    }
    pub async fn sync(&self) -> Result<OperationResult> {
        self.send(Command::Sync).await
    }
}

pub(crate) fn spawn(
    spec: InstanceSpec,
    assignment: Assignment,
    services: Arc<Services>,
) -> InstanceHandle {
    let runtime_id = format!("{}-{}", spec.id, assignment.generation);
    spawn_restored(
        InstanceRecord {
            restart_attempts: 0,
            restart_pending: false,
            spec,
            assignment,
            runtime_id,
            state: InstanceState::Pending,
            revision: 0,
            resources_held: false,
            runtime_ip: None,
            checkpoint: None,
            last_operation: None,
        },
        services,
    )
}

pub(crate) fn spawn_restored(record: InstanceRecord, services: Arc<Services>) -> InstanceHandle {
    let (tx, mut rx) = mpsc::channel::<(Command, Reply)>(32);
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
        while let Some((command, reply)) = rx.recv().await {
            let command = match command {
                Command::Snapshot(request, ack) => {
                    let result = if controller.retired {
                        Err(Error::Conflict)
                    } else {
                        controller.snapshot(request).await
                    };
                    let _ = ack.send(result);
                    continue;
                }
                Command::Tick(ack) => {
                    let result = if controller.retired {
                        Err(Error::Conflict)
                    } else {
                        controller.tick().await
                    };
                    let _ = ack.send(result);
                    continue;
                }
                Command::Expire(now, ack) => {
                    let result = if controller.retired {
                        Err(Error::Conflict)
                    } else {
                        controller.expire_checkpoint(now).await
                    };
                    let _ = ack.send(result);
                    continue;
                }
                Command::Discard(ack) => {
                    let result = if controller.retired {
                        Ok(())
                    } else {
                        controller.cleanup().await
                    };
                    if result.is_ok() {
                        controller.retired = true;
                    }
                    let _ = ack.send(result);
                    continue;
                }
                command => command,
            };
            if controller.retired {
                let _ = reply.send(Err(Error::Conflict));
                continue;
            }
            let operation = match &command {
                Command::Pause(_) => "pause",
                Command::Resume(_) => "resume",
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
                Command::Pause(request) => controller.pause(request).await,
                Command::Resume(request) => controller.resume(request).await,
                Command::Recover(request) => controller.recover(request).await,
                Command::Create => controller.create().await,
                Command::Clone(snapshot) => controller.clone_snapshot(*snapshot).await,
                Command::Delete => controller.delete().await,
                Command::Sync => controller.sync().await,
                Command::Reconcile => controller.reconcile().await,
            };
            if let Err(error) = &result {
                eprintln!(
                    "{}",
                    serde_json::json!({
                        "event": "instance operation failed",
                        "unix_seconds": crate::checkpoint::now().ok(),
                        "instance_id": controller.record.spec.id,
                        "runtime_id": controller.record.runtime_id,
                        "revision": controller.record.revision,
                        "operation": operation,
                        "error": error.to_string(),
                    })
                );
            }
            // Dropping the caller's reply never cancels an accepted operation.
            let _ = reply.send(result);
        }
    });
    InstanceHandle { tx }
}

struct Controller {
    recovery_files: Option<crate::checkpoint::MaterializedCheckpoint>,
    record: InstanceRecord,
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
        if self.record.state == InstanceState::Running
            && self
                .services
                .runtime
                .is_running(&self.record.runtime_id)
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
        } else if self.record.state == InstanceState::Paused {
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
                InstanceState::Deleted | InstanceState::Failed
            ) {
                self.record.state = InstanceState::Failed;
                self.record.revision =
                    self.record.revision.checked_add(1).ok_or(Error::Conflict)?;
            } else if self.record.state == InstanceState::Failed {
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
            InstanceState::Pending
                | InstanceState::Starting
                | InstanceState::Deleting
                | InstanceState::Pausing
                | InstanceState::Resuming
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
        if self.record.state == InstanceState::Running {
            return self.replay_or_sync().await;
        }
        if self.record.state != InstanceState::Pending {
            return Err(Error::Conflict);
        }
        if self.record.spec.snapshot_id.is_some() && self.record.checkpoint.is_none() {
            return Err(Error::Unavailable(
                "snapshot source required before starting".into(),
            ));
        }
        self.start_attempt(false).await
    }

    async fn start_attempt(&mut self, restart: bool) -> Result<OperationResult> {
        let runtime_id = if restart {
            format!(
                "{}-{}-r{}",
                self.record.spec.id,
                self.record.assignment.generation,
                self.record.revision.checked_add(1).ok_or(Error::Conflict)?
            )
        } else {
            self.record.runtime_id.clone()
        };
        self.services.admission.lock().unwrap().reserve(
            &runtime_id,
            &self.record.spec,
            &self.record.assignment,
        )?;
        if restart {
            self.record.runtime_id = runtime_id;
            self.record.runtime_ip = None;
            self.record.restart_attempts = self
                .record
                .restart_attempts
                .checked_add(1)
                .ok_or(Error::Conflict)?;
        }
        self.held = true;
        self.record.resources_held = true;
        self.transition(Event::Start)?;
        let attempt = timeout(self.services.operation_timeout, async {
            self.record.runtime_ip = Some(if self.record.spec.snapshot_id.is_some() {
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
                        &self.record.runtime_id,
                        self.record.assignment.generation,
                        &self.record.assignment.devices,
                        self.recovery_files.as_ref().unwrap(),
                        cp.origin.as_ref(),
                    )
                    .await?
            } else {
                self.services
                    .runtime
                    .start(
                        &self.record.spec,
                        &self.record.runtime_id,
                        self.record.assignment.generation,
                        &self.record.assignment.devices,
                    )
                    .await?
            });
            self.services.readiness.wait_ready(&self.record).await?;
            self.services.routes.activate(&self.record).await
        })
        .await
        .unwrap_or_else(|_| Err(Error::Unavailable("instance start timed out".into())));
        if let Err(start_error) = attempt {
            let cleanup = self.cleanup().await;
            self.transition(Event::Fail)?;
            self.record.restart_pending = restart
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
        self.record.restart_pending = false;
        self.restart_after = None;
        self.sync().await
    }

    async fn cleanup(&mut self) -> Result<()> {
        timeout(self.services.operation_timeout, async {
            self.services.routes.retire(&self.record).await?;
            self.services.runtime.remove(&self.record.runtime_id).await
        })
        .await
        .map_err(|_| Error::Unavailable("instance cleanup timed out".into()))??;
        self.recovery_files = None;
        self.services.metrics.remove(&self.record.spec.id);
        if self.held {
            self.services
                .admission
                .lock()
                .unwrap()
                .release(&self.record.runtime_id)?;
            self.held = false;
            self.record.resources_held = false;
        }
        Ok(())
    }

    async fn delete(&mut self) -> Result<OperationResult> {
        self.record.restart_pending = false;
        self.restart_after = None;
        if self.record.state == InstanceState::Deleted {
            return self.replay_or_sync().await;
        }
        self.record.last_operation = None;
        if self.record.state != InstanceState::Deleting {
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
        self.record.runtime_ip = None;
        self.transition(Event::Removed)?;
        self.sync().await
    }
}
