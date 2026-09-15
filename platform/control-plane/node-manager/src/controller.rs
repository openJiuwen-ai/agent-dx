use super::{Durability, OperationResult, Services};
use adx_core::{Assignment, Error, Event, InstanceRecord, InstanceSpec, InstanceState, Result};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;

enum Command {
    Create,
    Delete,
    Sync,
    Reconcile,
    Discard(oneshot::Sender<Result<()>>),
}
type Reply = oneshot::Sender<Result<OperationResult>>;

#[derive(Clone)]
pub struct InstanceHandle {
    tx: mpsc::Sender<(Command, Reply)>,
}

impl InstanceHandle {
    async fn send(&self, command: Command) -> Result<OperationResult> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send((command, tx))
            .await
            .map_err(|_| Error::Unavailable("instance controller stopped".into()))?;
        rx.await
            .map_err(|_| Error::Unavailable("instance controller stopped".into()))?
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
            spec,
            assignment,
            runtime_id,
            state: InstanceState::Pending,
            revision: 0,
            resources_held: false,
            runtime_ip: None,
        },
        services,
    )
}

pub(crate) fn spawn_restored(record: InstanceRecord, services: Arc<Services>) -> InstanceHandle {
    let (tx, mut rx) = mpsc::channel::<(Command, Reply)>(32);
    let mut controller = Controller {
        held: record.resources_held,
        record,
        services,
        durability: None,
        retired: false,
    };
    tokio::spawn(async move {
        while let Some((command, reply)) = rx.recv().await {
            let command = match command {
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
            let result = match command {
                Command::Discard(_) => unreachable!(),
                Command::Create => controller.create().await,
                Command::Delete => controller.delete().await,
                Command::Sync => controller.sync().await,
                Command::Reconcile => controller.reconcile().await,
            };
            // Dropping the caller's reply never cancels an accepted operation.
            let _ = reply.send(result);
        }
    });
    InstanceHandle { tx }
}

struct Controller {
    record: InstanceRecord,
    services: Arc<Services>,
    held: bool,
    durability: Option<Durability>,
    retired: bool,
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
            tokio::time::timeout(self.services.operation_timeout, async {
                self.services.readiness.wait_ready(&self.record).await?;
                self.services.routes.activate(&self.record).await
            })
            .await
            .map_err(|_| Error::Unavailable("recovery readiness timed out".into()))??;
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
        self.durability = None;
        Ok(())
    }

    async fn sync(&mut self) -> Result<OperationResult> {
        if matches!(
            self.record.state,
            InstanceState::Pending | InstanceState::Starting | InstanceState::Deleting
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
        Ok(OperationResult {
            record: self.record.clone(),
            durability,
        })
    }

    async fn replay_or_sync(&mut self) -> Result<OperationResult> {
        if self.durability == Some(Durability::Published) {
            Ok(OperationResult {
                record: self.record.clone(),
                durability: Durability::Published,
            })
        } else {
            self.sync().await
        }
    }

    async fn create(&mut self) -> Result<OperationResult> {
        if self.record.state == InstanceState::Running {
            return self.replay_or_sync().await;
        }
        if self.record.state != InstanceState::Pending {
            return Err(Error::Conflict);
        }
        self.services.admission.lock().unwrap().reserve(
            &self.record.runtime_id,
            &self.record.spec,
            &self.record.assignment,
        )?;
        self.held = true;
        self.record.resources_held = true;
        self.transition(Event::Start)?;
        let attempt = timeout(self.services.operation_timeout, async {
            self.record.runtime_ip = Some(
                self.services
                    .runtime
                    .start(
                        &self.record.spec,
                        &self.record.runtime_id,
                        self.record.assignment.generation,
                        &self.record.assignment.devices,
                    )
                    .await?,
            );
            self.services.readiness.wait_ready(&self.record).await?;
            self.services.routes.activate(&self.record).await
        })
        .await
        .unwrap_or_else(|_| Err(Error::Unavailable("instance start timed out".into())));
        if let Err(start_error) = attempt {
            let cleanup = self.cleanup().await;
            self.transition(Event::Fail)?;
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
        self.sync().await
    }

    async fn cleanup(&mut self) -> Result<()> {
        timeout(self.services.operation_timeout, async {
            self.services.routes.retire(&self.record).await?;
            self.services.runtime.remove(&self.record.runtime_id).await
        })
        .await
        .map_err(|_| Error::Unavailable("instance cleanup timed out".into()))??;
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
        if self.record.state == InstanceState::Deleted {
            return self.replay_or_sync().await;
        }
        self.transition(Event::Delete)?;
        if let Err(error) = self.cleanup().await {
            self.transition(Event::Fail)?;
            return match self.sync().await {
                Ok(_) => Err(error),
                Err(commit) => Err(Error::Unavailable(format!(
                    "cleanup failed: {error}; commit failed: {commit}"
                ))),
            };
        }
        self.transition(Event::Removed)?;
        self.sync().await
    }
}
