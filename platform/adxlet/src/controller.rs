use super::{Durability, OperationResult, Services};
use adx_core::{
    Assignment, EnvironmentRecord, EnvironmentSpec, EnvironmentState, Error, Event, LifecycleKind,
    Result,
};
use std::{collections::BTreeSet, sync::Arc, time::Instant};
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;

mod lifecycle;
mod monitor;
mod snapshots;
mod workload_checkpoint;
use super::checkpoint::{PauseRequest, ResumeRequest};

pub(super) type CheckpointReferences = BTreeSet<(String, String)>;

enum Command {
    Snapshot(
        super::checkpoint::SnapshotRequest,
        oneshot::Sender<Result<super::checkpoint::SnapshotResult>>,
    ),
    Pause(PauseRequest),
    Resume(ResumeRequest),
    EnsureRunning(String),
    Network {
        policy: Option<adx_core::execution::NetworkPolicy>,
        operation_id: String,
        expected_revision: u64,
    },
    Reload {
        operation_id: String,
        expected_revision: u64,
    },
    Recover(ResumeRequest),
    Create(Option<tokio::time::Instant>),
    Clone(
        Box<adx_core::snapshots::Snapshot>,
        Option<tokio::time::Instant>,
    ),
    Delete,
    DeleteAt(u64),
    CollectFailed(u64),
    Sync,
    Reconcile,
    AlignAuthority(Box<EnvironmentRecord>, oneshot::Sender<Result<()>>),
    Discard(
        Option<Arc<CheckpointReferences>>,
        oneshot::Sender<Result<()>>,
    ),
    Expire(u64, oneshot::Sender<Result<Option<OperationResult>>>),
    Tick(oneshot::Sender<Result<Option<OperationResult>>>),
    RuntimeEvent(
        adx_core::runtime::RuntimeIdentity,
        oneshot::Sender<Result<()>>,
    ),
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
            trace: adx_observability::trace::Trace::child("environment.queue"),
        }
    }
}

#[derive(Clone)]
pub struct EnvironmentHandle {
    tx: mpsc::Sender<Envelope>,
    demand: Arc<std::sync::atomic::AtomicU64>,
}

impl EnvironmentHandle {
    pub async fn snapshot(
        &self,
        request: super::checkpoint::SnapshotRequest,
    ) -> Result<super::checkpoint::SnapshotResult> {
        let (tx, rx) = oneshot::channel();
        let (unused, _) = oneshot::channel();
        self.tx
            .send(Envelope::new(Command::Snapshot(request, tx), unused))
            .await
            .map_err(|_| Error::Unavailable("environment controller stopped".into()))?;
        rx.await
            .map_err(|_| Error::Unavailable("environment controller stopped".into()))?
    }

    async fn send(&self, command: Command) -> Result<OperationResult> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Envelope::new(command, tx))
            .await
            .map_err(|_| Error::Unavailable("environment controller stopped".into()))?;
        rx.await
            .map_err(|_| Error::Unavailable("environment controller stopped".into()))?
    }
    pub async fn pause(&self, request: PauseRequest) -> Result<OperationResult> {
        self.send(Command::Pause(request)).await
    }
    pub async fn resume(&self, request: ResumeRequest) -> Result<OperationResult> {
        self.send(Command::Resume(request)).await
    }
    pub async fn ensure_running(&self, operation_id: String) -> Result<OperationResult> {
        self.demand
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        self.send(Command::EnsureRunning(operation_id)).await
    }
    pub async fn update_network_policy(
        &self,
        policy: Option<adx_core::execution::NetworkPolicy>,
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
        self.send(Command::Clone(Box::new(snapshot), None)).await
    }
    pub async fn create(&self) -> Result<OperationResult> {
        self.send(Command::Create(None)).await
    }
    pub async fn create_with_deadline(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<OperationResult> {
        self.send(Command::Create(Some(deadline))).await
    }
    pub(crate) async fn create_from_snapshot_with_deadline(
        &self,
        snapshot: adx_core::snapshots::Snapshot,
        deadline: tokio::time::Instant,
    ) -> Result<OperationResult> {
        self.send(Command::Clone(Box::new(snapshot), Some(deadline)))
            .await
    }
    pub async fn delete(&self) -> Result<OperationResult> {
        self.send(Command::Delete).await
    }
    /// Expiry cleanup is fenced to the exact record revision that Coordinator
    /// claimed. It must not delete a later resume or replacement execution.
    pub async fn delete_at_revision(&self, expected_revision: u64) -> Result<OperationResult> {
        self.send(Command::DeleteAt(expected_revision)).await
    }
    /// GC must not delete an execution that moved on after the candidate was read.
    pub async fn collect_failed(&self, expected_revision: u64) -> Result<OperationResult> {
        self.send(Command::CollectFailed(expected_revision)).await
    }
    pub(crate) async fn reconcile(&self) -> Result<OperationResult> {
        self.send(Command::Reconcile).await
    }
    pub(crate) async fn align_authority(&self, record: EnvironmentRecord) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        let (unused, _) = oneshot::channel();
        self.tx
            .send(Envelope::new(
                Command::AlignAuthority(Box::new(record), tx),
                unused,
            ))
            .await
            .map_err(|_| Error::Unavailable("environment controller stopped".into()))?;
        rx.await
            .map_err(|_| Error::Unavailable("environment controller stopped".into()))?
    }
    pub(crate) async fn discard(
        &self,
        references: Option<Arc<CheckpointReferences>>,
    ) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        let (unused, _) = oneshot::channel();
        self.tx
            .send(Envelope::new(Command::Discard(references, tx), unused))
            .await
            .map_err(|_| Error::Unavailable("environment controller stopped".into()))?;
        rx.await
            .map_err(|_| Error::Unavailable("environment controller stopped".into()))?
    }
    pub async fn expire_checkpoint(&self, now: u64) -> Result<Option<OperationResult>> {
        let (tx, rx) = oneshot::channel();
        let (unused, _) = oneshot::channel();
        self.tx
            .send(Envelope::new(Command::Expire(now, tx), unused))
            .await
            .map_err(|_| Error::Unavailable("environment controller stopped".into()))?;
        rx.await
            .map_err(|_| Error::Unavailable("environment controller stopped".into()))?
    }
    pub(crate) async fn observe_runtime(
        &self,
        identity: adx_core::runtime::RuntimeIdentity,
    ) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        let (unused, _) = oneshot::channel();
        self.tx
            .send(Envelope::new(Command::RuntimeEvent(identity, tx), unused))
            .await
            .map_err(|_| Error::Unavailable("environment controller stopped".into()))?;
        rx.await
            .map_err(|_| Error::Unavailable("environment controller stopped".into()))?
    }

    pub async fn tick(&self) -> Result<Option<OperationResult>> {
        let (tx, rx) = oneshot::channel();
        let (unused, _) = oneshot::channel();
        self.tx
            .send(Envelope::new(Command::Tick(tx), unused))
            .await
            .map_err(|_| Error::Unavailable("environment controller stopped".into()))?;
        rx.await
            .map_err(|_| Error::Unavailable("environment controller stopped".into()))?
    }
    pub async fn sync(&self) -> Result<OperationResult> {
        self.send(Command::Sync).await
    }
}

pub(crate) fn spawn(
    spec: EnvironmentSpec,
    assignment: Assignment,
    services: Arc<Services>,
    held: bool,
) -> EnvironmentHandle {
    let runtime_id = format!("{}-{}", spec.id, assignment.generation);
    spawn_restored(
        EnvironmentRecord {
            failure: None,
            restart_attempts: 0,
            restart_pending: false,
            spec,
            assignment,
            runtime: adx_core::Runtime {
                id: runtime_id,
                ip: None,
            },
            state: EnvironmentState::Pending,
            revision: 0,
            resources_held: held,
            checkpoint: None,
            last_operation: None,
        },
        services,
    )
}

pub(crate) fn spawn_restored(
    record: EnvironmentRecord,
    services: Arc<Services>,
) -> EnvironmentHandle {
    let (tx, mut rx) = mpsc::channel::<Envelope>(32);
    let demand = Arc::new(std::sync::atomic::AtomicU64::new(0));
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
        demand: demand.clone(),
        observed_demand: 0,
        auto_pause_guard: None,
        create_deadline: None,
    };
    tokio::spawn(async move {
        while let Some(Envelope {
            command,
            reply,
            trace,
        }) = rx.recv().await
        {
            trace.attribute("environment.id", controller.record.spec.id.clone());
            trace
                .run(async {
                    adx_observability::trace::Trace::child("environment.execute")
                        .run(controller.dispatch(command, reply))
                        .await;
                })
                .await;
        }
    });
    EnvironmentHandle { tx, demand }
}

impl Controller {
    async fn dispatch(&mut self, command: Command, reply: Reply) {
        let command = match command {
            Command::AlignAuthority(record, ack) => {
                let result = self.align_authority(*record).await;
                let _ = ack.send(result);
                return;
            }
            Command::Snapshot(request, ack) => {
                let result = if self.retired {
                    Err(Error::Conflict)
                } else {
                    self.snapshot(request).await
                };
                let _ = ack.send(result);
                return;
            }
            Command::RuntimeEvent(identity, ack) => {
                let result = if self.retired
                    || identity
                        != super::runtime_control::RuntimeControlClient::identity(&self.record)
                    || self.record.state != EnvironmentState::Running
                {
                    Ok(())
                } else {
                    async {
                        if self.runtime_probe_failure().await? {
                            return Ok(());
                        }
                        match self.workload_checkpoint().await {
                            // A guest checkpoint deliberately changes RuntimeStatus before
                            // resuming. Its own checkpoint protocol owns that transition.
                            Ok(true) => Ok(()),
                            Err(error) => Err(error),
                            Ok(false) if self.record.spec.compatibility.is_some() => {
                                let health = tokio::time::timeout(
                                    self.services.operation_timeout,
                                    self.services.readiness.activity(&self.record),
                                )
                                .await
                                .unwrap_or_else(|_| {
                                    Err(Error::Unavailable("runtime health timed out".into()))
                                });
                                if health.is_err() {
                                    self.unexpected_exit().await.map(|_| ())
                                } else {
                                    Ok(())
                                }
                            }
                            Ok(false) => Ok(()),
                        }
                    }
                    .await
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
            Command::Discard(references, ack) => {
                let result = self.discard_execution(references.as_deref()).await;
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
            Command::EnsureRunning(_) => "ensure_running",
            Command::Network { .. } => "network",
            Command::Reload { .. } => "reload",
            Command::Recover(_) => "recover",
            Command::Create(_) | Command::Clone(_, _) => "create",
            Command::Delete => "delete",
            Command::DeleteAt(_) => "delete_expired",
            Command::CollectFailed(_) => "collect_failed",
            Command::Sync => "sync",
            Command::Reconcile => "reconcile",
            _ => unreachable!(),
        };
        let result = match command {
            Command::Snapshot(_, _)
            | Command::AlignAuthority(_, _)
            | Command::Discard(_, _)
            | Command::Expire(_, _)
            | Command::Tick(_)
            | Command::RuntimeEvent(_, _) => unreachable!(),
            Command::Pause(request) => self.pause(request).await,
            Command::Resume(request) => self.resume(request).await,
            Command::EnsureRunning(operation_id) => {
                self.idle = None;
                if self.record.state == EnvironmentState::Running {
                    self.sync().await
                } else {
                    self.resume(ResumeRequest {
                        operation_id,
                        expected_revision: self.record.revision,
                    })
                    .await
                }
            }
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
            Command::Create(deadline) => {
                self.create_deadline = deadline;
                let result = self.create().await;
                self.create_deadline = None;
                result
            }
            Command::Clone(snapshot, deadline) => {
                self.create_deadline = deadline;
                let result = self.clone_snapshot(*snapshot).await;
                self.create_deadline = None;
                result
            }
            Command::Delete => self.delete().await,
            Command::DeleteAt(revision) => {
                if self.record.revision != revision {
                    Err(Error::Conflict)
                } else {
                    self.delete().await
                }
            }
            Command::CollectFailed(revision) => {
                if self.record.state != EnvironmentState::Failed
                    || self.record.restart_pending
                    || self.record.revision != revision
                {
                    Err(Error::Conflict)
                } else {
                    self.delete().await
                }
            }
            Command::Sync => self.sync().await,
            Command::Reconcile => self.reconcile().await,
        };
        if let Err(error) = &result {
            adx_observability::trace::error();
            eprintln!(
                "{}",
                serde_json::json!({
                    "event": "environment operation failed",
                    "unix_seconds": crate::checkpoint::now().ok(),
                    "environment_id": self.record.spec.id,
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
    create_deadline: Option<tokio::time::Instant>,
    auto_pause_guard: Option<((String, u64, u64), u64)>,
    demand: Arc<std::sync::atomic::AtomicU64>,
    observed_demand: u64,
    recovery_files: Option<crate::checkpoint::MaterializedCheckpoint>,
    record: EnvironmentRecord,
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
    async fn discard_execution(&mut self, references: Option<&CheckpointReferences>) -> Result<()> {
        if !self.retired {
            self.cleanup().await?;
            // Stop accepting serial operations before deleting retired artifacts.
            // A failed object deletion is retried without republishing old state.
            self.retired = true;
        }
        let Some(references) = references else {
            return Ok(());
        };
        let Some(checkpoint) = &self.services.checkpoint else {
            return Ok(());
        };
        if let Some(cp) = self.record.checkpoint.as_ref() {
            let key = (cp.artifact.storage.clone(), cp.artifact.location.clone());
            if !references.contains(&key) {
                checkpoint.store.remove(&cp.artifact).await?;
                self.record.checkpoint = None;
            }
        }
        while let Some(artifact) = self.obsolete_checkpoints.last() {
            let key = (artifact.storage.clone(), artifact.location.clone());
            if !references.contains(&key) {
                checkpoint.store.remove(artifact).await?;
            }
            self.obsolete_checkpoints.pop();
        }
        Ok(())
    }

    async fn align_authority(&mut self, mut record: EnvironmentRecord) -> Result<()> {
        if self.retired
            || record.spec != self.record.spec
            || record.assignment != self.record.assignment
        {
            return Err(Error::Conflict);
        }
        if record.state == EnvironmentState::Paused && record.runtime.id != self.record.runtime.id {
            // A current controller may have finished restore while its commit
            // was fenced by a Coordinator restart. Clean that execution and
            // re-adopt the durable paused point instead of publishing Failed.
            // A known published result requires a fresh catalog; do not delete it.
            if self.durability == Some(Durability::Published) {
                return Err(Error::Conflict);
            }
            self.cleanup().await?;
            // Cleanup retires the uncommitted execution at its local revision.
            // A live Relay retains that version across full catalog replacement;
            // publishing the older paused revision would prevent reconciliation
            // and the next restore from advancing its binding.
            record.revision = record
                .revision
                .max(self.record.revision.checked_add(1).ok_or(Error::Conflict)?);
            self.record = record;
            self.durability = None;
            self.idle = None;
        }
        Ok(())
    }

    async fn reconcile(&mut self) -> Result<OperationResult> {
        let observation = if self.record.state == EnvironmentState::Running {
            Some(
                self.services
                    .runtime
                    .observe(&self.record.runtime.id)
                    .await?,
            )
        } else {
            None
        };
        if observation.as_ref().is_some_and(|status| status.running) {
            self.services.readiness.expect_runtime(&self.record)?;
            if let Some(control) = &self.services.runtime_control {
                control.expect_runtime(&self.record)?;
                // Reconnect must deliver a full snapshot before pending-operation reconciliation.
                if control.uses_stream() {
                    control.status(&self.record).await?;
                }
            }
            if self.runtime_probe_failure().await? {
                return self.sync().await;
            }
            self.reconcile_workload_checkpoint().await?;
            if self.record.state == EnvironmentState::Failed {
                return self.sync().await;
            }
            if self.recovery_files.is_none()
                && self
                    .services
                    .runtime
                    .retains_checkpoint_files(&self.record.spec.runtime_class)
            {
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
        } else if self.record.state == EnvironmentState::Paused {
            let cp = self.record.checkpoint.clone().ok_or(Error::Conflict)?;
            let services = self.services.checkpoint.clone().ok_or(Error::Conflict)?;
            self.cleanup().await?;
            if cp.expired(super::checkpoint::now()?) {
                return self.delete().await;
            }
            match services.store.validate_artifact(&cp.artifact).await {
                Ok(()) => (),
                Err(Error::Invalid(_) | Error::NotFound) => self.transition(Event::Fail)?,
                Err(error) => return Err(error),
            }
        } else {
            if let Some(status) = observation {
                self.record.failure = status.failure;
            }
            self.cleanup().await?;
            if !matches!(
                self.record.state,
                EnvironmentState::Deleted | EnvironmentState::Failed
            ) {
                self.record.state = EnvironmentState::Failed;
                self.record.revision =
                    self.record.revision.checked_add(1).ok_or(Error::Conflict)?;
            } else if self.record.state == EnvironmentState::Failed {
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
        if matches!(event, Event::Start | Event::Resume | Event::Ready) {
            self.record.failure = None;
        }
        self.record.last_operation = None;
        self.idle = None;
        self.health_failures = 0;
        self.durability = None;
        Ok(())
    }

    async fn sync(&mut self) -> Result<OperationResult> {
        if matches!(
            self.record.state,
            EnvironmentState::Pending
                | EnvironmentState::Starting
                | EnvironmentState::Deleting
                | EnvironmentState::Pausing
                | EnvironmentState::Resuming
        ) {
            return Err(Error::Conflict);
        }
        let cleanup_artifacts = if self.record.state == EnvironmentState::Deleted {
            self.obsolete_checkpoints.clone()
        } else {
            Vec::new()
        };
        let durability = timeout(
            self.services.operation_timeout,
            adx_observability::trace::Trace::child("environment.state.commit").run_result(async {
                if self.record.state == EnvironmentState::Deleted {
                    self.services
                        .sink
                        .commit_deleted(&self.record, &cleanup_artifacts)
                        .await
                } else {
                    self.services.sink.commit(&self.record).await
                }
            }),
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
        if self.record.state == EnvironmentState::Deleted {
            // Coordinator has atomically retained the cleanup task with the
            // Deleted record. Its collector applies the live Environment and
            // Snapshot alias fences before removing physical bytes.
            self.obsolete_checkpoints.clear();
            return Ok(());
        }
        if let (Some(cp), Some(services)) = (&self.record.checkpoint, &self.services.checkpoint) {
            services.store.committed(&cp.artifact).await?;
        }
        // A restored backend may still map an older recovery point. Keep it until stop.
        if self.recovery_files.is_some() {
            return Ok(());
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
        if self.record.state == EnvironmentState::Running {
            return self.replay_or_sync().await;
        }
        if self.record.state != EnvironmentState::Pending {
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
            // recovery contract and must not be rejected by Coordinator as a cold
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
        self.record.failure = None;
        let initialization_timeout = if let Some(deadline) = self.create_deadline {
            deadline.saturating_duration_since(tokio::time::Instant::now())
        } else if self.record.spec.execution.init_timeout_seconds > 0 {
            std::time::Duration::from_secs(self.record.spec.execution.init_timeout_seconds)
        } else {
            self.services.operation_timeout
        };
        let mut awaiting_readiness = false;
        let mut stage = "backend preparation/runtime start";
        let operation_started = Instant::now();
        let attempt = timeout(initialization_timeout, async {
            if self
                .create_deadline
                .is_some_and(|deadline| deadline <= tokio::time::Instant::now())
            {
                return Err(Error::DeadlineExceeded(format!(
                    "create deadline exceeded during {stage}"
                )));
            }
            if self.record.spec.lifecycle.idle_action == adx_core::lifecycle::IdleAction::Pause {
                self.services
                    .runtime
                    .checkpoint_supported(&self.record.spec.runtime_class)
                    .await?;
            }
            self.services.readiness.expect_runtime(&self.record)?;
            let runtime_start_started = Instant::now();
            self.record.runtime.ip = Some(
                adx_observability::trace::Trace::child("environment.runtime.start")
                    .run_result(async {
                        if restore_checkpoint || self.record.spec.snapshot_id.is_some() {
                            let cp = self.record.checkpoint.as_ref().ok_or(Error::Conflict)?;
                            let store = &self
                                .services
                                .checkpoint
                                .as_ref()
                                .ok_or(Error::Conflict)?
                                .store;
                            let path = store.materialize(&cp.artifact).await?;
                            // Retain through the RPC, including ambiguous failure.
                            self.recovery_files = Some(path);
                            let result = self
                                .services
                                .runtime
                                .restore_from_with_deadline(
                                    &self.record.spec,
                                    &self.record.runtime.id,
                                    self.record.assignment.generation,
                                    &self.record.assignment.devices,
                                    crate::RestoreSource {
                                        path: self.recovery_files.as_ref().expect(
                                            "recovery file is stored immediately before restore",
                                        ),
                                        origin: cp.origin.as_ref(),
                                    },
                                    self.create_deadline.unwrap_or_else(|| {
                                        tokio::time::Instant::now()
                                            + self.services.operation_timeout
                                    }),
                                )
                                .await;
                            if result.is_ok()
                                && !self
                                    .services
                                    .runtime
                                    .retains_checkpoint_files(&self.record.spec.runtime_class)
                            {
                                self.recovery_files = None;
                            }
                            result
                        } else {
                            self.services
                                .runtime
                                .start_with_deadline(
                                    &self.record.spec,
                                    &self.record.runtime.id,
                                    self.record.assignment.generation,
                                    &self.record.assignment.devices,
                                    self.create_deadline.unwrap_or_else(|| {
                                        tokio::time::Instant::now() + initialization_timeout
                                    }),
                                )
                                .await
                        }
                    })
                    .await?,
            );
            let runtime_start_ms = runtime_start_started.elapsed().as_secs_f64() * 1_000.0;
            awaiting_readiness = true;
            stage = "Execd readiness";
            let runtime_ready_started = Instant::now();
            adx_observability::trace::Trace::child("environment.runtime.ready")
                .run_result(self.services.readiness.wait_ready(&self.record))
                .await?;
            let runtime_ready_ms = runtime_ready_started.elapsed().as_secs_f64() * 1_000.0;
            awaiting_readiness = false;
            stage = "route activation";
            let route_started = Instant::now();
            adx_observability::trace::Trace::child("environment.route.activate")
                .run_result(self.services.routes.activate(&self.record))
                .await?;
            Ok((
                runtime_start_ms,
                runtime_ready_ms,
                route_started.elapsed().as_secs_f64() * 1_000.0,
            ))
        })
        .await
        .unwrap_or_else(|_| {
            Err(Error::DeadlineExceeded(format!(
                "create deadline exceeded during {stage}"
            )))
        });
        let (runtime_start_ms, runtime_ready_ms, route_activate_ms) = match attempt {
            Ok(stages) => stages,
            Err(start_error) => {
                let start_error = if matches!(start_error, Error::Unavailable(_))
                    && self
                        .create_deadline
                        .is_some_and(|deadline| deadline <= tokio::time::Instant::now())
                {
                    Error::DeadlineExceeded(format!(
                        "environment initialization deadline exceeded during {stage}"
                    ))
                } else {
                    start_error
                };
                let start_error =
                    if awaiting_readiness && self.record.spec.execution.readiness_probe.is_some() {
                        match start_error {
                            Error::ProbeFailed(_) => start_error,
                            other => Error::ProbeFailed(adx_core::probes::ProbeFailure::timeout(
                                other.to_string(),
                            )),
                        }
                    } else {
                        start_error
                    };
                if let Error::ProbeFailed(failure) = &start_error {
                    self.record.failure = Some(failure.clone().into());
                }
                if let Error::DeadlineExceeded(message) = &start_error {
                    self.record.failure = Some(adx_core::RuntimeFailure {
                        reason: adx_core::RuntimeFailureReason::DeadlineExceeded,
                        message: message.clone(),
                        exit_code: None,
                        probe_failure: None,
                    });
                }
                self.transition(Event::Fail)?;
                self.release_capacity()?;
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
                if self.create_deadline.is_some()
                    && matches!(start_error, Error::DeadlineExceeded(_))
                {
                    // Report the known failed attempt without waiting for physical
                    // cleanup. Failed controllers retain ownership and reconcile it.
                    let publication = tokio::time::timeout(
                        adx_transport::deadline::CREATE_RESPONSE_GRACE / 3,
                        self.sync(),
                    )
                    .await;
                    if !matches!(publication, Ok(Ok(_))) {
                        adx_observability::warn!(environment_id = %self.record.spec.id,
                            "timed-out create publication requires reconciliation");
                        return Err(Error::ResponseTimeout(format!(
                            "{start_error}; failure publication not confirmed; query the same environment"
                        )));
                    }
                    return Err(start_error);
                }
                let commit = self.sync().await;
                let cleanup = self.cleanup().await;
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
        };
        self.transition(Event::Ready)?;
        if let Some((id, revision, kind)) = completion {
            self.completed(id, revision, kind);
        }
        self.record.restart_pending = false;
        self.restart_after = None;
        let commit_started = Instant::now();
        let result = if let Some(deadline) = self.create_deadline {
            tokio::time::timeout_at(deadline, self.sync())
                .await
                .map_err(|_| {
                    Error::Unavailable("create state publication remains unknown".into())
                })?
        } else {
            self.sync().await
        };
        adx_observability::info!(
            event = "environment_start_stages",
            environment_id = %self.record.spec.id,
            runtime_id = %self.record.runtime.id,
            runtime_start_ms,
            runtime_ready_ms,
            route_activate_ms,
            state_commit_ms = commit_started.elapsed().as_secs_f64() * 1_000.0,
            total_ms = operation_started.elapsed().as_secs_f64() * 1_000.0,
            published = result.is_ok(),
            "environment start stages completed"
        );
        result
    }

    fn release_capacity(&mut self) -> Result<()> {
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

    async fn cleanup(&mut self) -> Result<()> {
        timeout(self.services.operation_timeout, async {
            self.services.routes.retire(&self.record).await?;
            if let Some(control) = &self.services.runtime_control {
                control.retire(&self.record);
            }
            self.services.runtime.remove(&self.record.runtime.id).await
        })
        .await
        .map_err(|_| Error::Unavailable("environment cleanup timed out".into()))??;
        self.recovery_files = None;
        self.services.metrics.remove(&self.record.spec.id);
        self.release_capacity()
    }

    async fn delete(&mut self) -> Result<OperationResult> {
        self.record.restart_pending = false;
        self.restart_after = None;
        if self.record.state == EnvironmentState::Deleted {
            return self.replay_or_sync().await;
        }
        self.record.last_operation = None;
        if self.record.state != EnvironmentState::Deleting {
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
