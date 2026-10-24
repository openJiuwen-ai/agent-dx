use super::{Durability, OperationResult, Services};
use adx_core::{
    Assignment, EnvironmentRecord, EnvironmentSpec, EnvironmentState, Error, Event, LifecycleKind,
    Result,
};
use std::{collections::BTreeSet, sync::Arc, time::Instant};
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;

mod authority;
mod events;
mod lifecycle;
pub(super) use authority::{AuthorityCheck, AuthorityResult};
mod monitor;
mod snapshots;
mod workload_checkpoint;
use super::checkpoint::{PauseRequest, ResumeRequest};

pub(super) type CheckpointReferences = BTreeSet<(String, String)>;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum AuthorityComparison {
    Published,
    Unpublished,
    StaleCatalog,
    RetiredByAuthority,
}

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
    CollectFailed(u64),
    Sync,
    Reconcile,
    AlignAuthority(Box<EnvironmentRecord>, oneshot::Sender<Result<()>>),
    CheckAuthority(
        Box<AuthorityCheck>,
        oneshot::Sender<Result<AuthorityResult>>,
    ),
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
    trace: Option<adx_observability::trace::Trace>,
}
impl Envelope {
    fn new(command: Command, reply: Reply) -> Self {
        let trace = (!matches!(
            command,
            Command::Tick(_) | Command::Expire(_, _) | Command::CheckAuthority(_, _)
        ))
        .then(|| adx_observability::trace::Trace::child("environment.queue"));
        Self {
            command,
            reply,
            trace,
        }
    }
}

#[derive(Clone)]
pub struct EnvironmentHandle {
    tx: mpsc::Sender<Envelope>,
    demand: Arc<std::sync::atomic::AtomicU64>,
    current: tokio::sync::watch::Receiver<(EnvironmentRecord, Option<Durability>)>,
    cleanup_pending: Arc<std::sync::atomic::AtomicBool>,
    metrics_registration: crate::metrics::Registration,
}

impl EnvironmentHandle {
    pub(crate) fn record_usage(
        &self,
        metrics: &crate::metrics::Metrics,
        record: &EnvironmentRecord,
        usage: crate::metrics::RuntimeUsage,
    ) {
        metrics.record(
            &self.metrics_registration,
            &record.runtime.id,
            record.assignment.generation,
            usage,
        );
    }
    pub(crate) fn view(&self) -> (EnvironmentRecord, Option<Durability>) {
        self.current.borrow().clone()
    }
    pub(crate) fn retained_runtime_id(&self) -> Option<String> {
        let view = self.current.borrow();
        let record = &view.0;
        (record.resources_held
            || self
                .cleanup_pending
                .load(std::sync::atomic::Ordering::Acquire)
            || matches!(
                record.state,
                EnvironmentState::Starting | EnvironmentState::Resuming | EnvironmentState::Pausing
            ))
        .then(|| record.runtime.id.clone())
    }

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
    /// Compare and clean as one serial operation, with no lifecycle command in between.
    pub(crate) async fn check_authority(&self, check: AuthorityCheck) -> Result<AuthorityResult> {
        let (tx, rx) = oneshot::channel();
        let (unused, _) = oneshot::channel();
        self.tx
            .send(Envelope::new(
                Command::CheckAuthority(Box::new(check), tx),
                unused,
            ))
            .await
            .map_err(|_| Error::Unavailable("environment controller stopped".into()))?;
        rx.await
            .map_err(|_| Error::Unavailable("environment controller stopped".into()))?
    }
    pub(crate) fn same_controller(&self, other: &Self) -> bool {
        self.tx.same_channel(&other.tx)
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
            execution_artifacts: vec![],
            snapshot_source: None,
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
    spawn_controller(record, services, None, false)
}

pub(crate) fn spawn_comparison(
    record: EnvironmentRecord,
    services: Arc<Services>,
) -> EnvironmentHandle {
    // The registry exposes the handle before its authority command is queued.
    // Only that command may open lifecycle admission for an adopted execution.
    spawn_controller(record, services, None, true)
}

/// Retain explicit deletion/replay for an already published, released terminal record.
pub(crate) fn spawn_terminal(
    record: EnvironmentRecord,
    services: Arc<Services>,
) -> Result<EnvironmentHandle> {
    if !matches!(
        record.state,
        EnvironmentState::Failed | EnvironmentState::Deleted
    ) || record.resources_held
        || record.restart_pending
    {
        return Err(Error::Conflict);
    }
    Ok(spawn_controller(
        record,
        services,
        Some(Durability::Published),
        true,
    ))
}

fn spawn_controller(
    record: EnvironmentRecord,
    services: Arc<Services>,
    durability: Option<Durability>,
    authority_fenced: bool,
) -> EnvironmentHandle {
    let (tx, mut rx) = mpsc::channel::<Envelope>(32);
    let (observed, current) = tokio::sync::watch::channel((record.clone(), durability));
    let demand = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let cleanup_pending = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let metrics_registration = services.metrics.register(&record.spec.id);
    let mut controller = Controller {
        capture_preparation: None,
        recovery_files: None,
        execution_files: vec![],
        held: record.resources_held,
        record,
        services,
        durability,
        retired: false,
        authority_fenced,
        obsolete_checkpoints: Vec::new(),
        idle: None,
        restart_after: None,
        health_failures: 0,
        events: events::LifecycleEvents::default(),
        observed,
        metrics_registration: metrics_registration.clone(),
        cleanup_pending: cleanup_pending.clone(),
        demand: demand.clone(),
        observed_demand: 0,
        auto_pause_guard: None,
        create_deadline: None,
    };
    controller.publish_view();
    tokio::spawn(async move {
        let mut observers = events::Observers::default();
        loop {
            controller.publish_view();
            observers.refresh(&mut controller);
            let deadline = controller.lifecycle_deadline();
            let record = controller.record.clone();
            let routes = controller.services.routes.clone();
            let proxy_stamp = controller.proxy_stamp();
            let running = record.state == EnvironmentState::Running
                && !controller.authority_fenced
                && !controller.retired;
            enum Wake {
                Command(Option<Envelope>),
                Control(Option<adx_core::runtime::RuntimeStatus>),
                Exit(Result<adx_core::RuntimeFailure>),
                Proxy(Result<(String, u64, u64)>),
                Deadline,
            }
            let wake = tokio::select! {
                command = rx.recv() => Wake::Command(command),
                status = async {
                    let Some(changes) = observers.control.as_mut() else { return std::future::pending().await; };
                    if !observers.initial && changes.changed().await.is_err() { return None; }
                    changes.borrow_and_update().clone()
                } => Wake::Control(status),
                result = async {
                    match observers.exit.as_mut() { Some(wait) => wait.await, None => std::future::pending().await }
                } => Wake::Exit(result),
                proxy = async {
                    if !running || record.spec.lifecycle.idle_timeout_seconds == 0 { return std::future::pending().await; }
                    routes.activity_changed(&record, proxy_stamp).await
                } => Wake::Proxy(proxy),
                _ = async { match deadline { Some(deadline) => tokio::time::sleep_until(deadline).await, None => std::future::pending().await } } => Wake::Deadline,
            };
            let envelope = match wake {
                Wake::Command(command) => {
                    let Some(command) = command else {
                        break;
                    };
                    command
                }
                Wake::Control(status) => {
                    observers.initial = false;
                    if observers
                        .control
                        .as_ref()
                        .is_some_and(|changes| changes.has_changed().is_err())
                    {
                        observers.control = None;
                    }
                    let result = controller.control_observed(status).await;
                    controller.trace_observation_error("runtime.control", &result);
                    if let Err(error) = result {
                        controller.retry_lifecycle();
                        adx_observability::warn!(%error, "runtime control event incomplete");
                    }
                    continue;
                }
                Wake::Exit(result) => {
                    observers.exit = None;
                    let result = controller.backend_exited(result).await;
                    controller.trace_observation_error("runtime.exit", &result);
                    if let Err(error) = result {
                        controller.retry_lifecycle();
                        adx_observability::warn!(%error, "runtime exit observation incomplete");
                    }
                    continue;
                }
                Wake::Proxy(status) => {
                    controller.proxy_observed(status);
                    continue;
                }
                Wake::Deadline => {
                    let result = controller.lifecycle_due().await;
                    controller.trace_observation_error("lifecycle.deadline", &result);
                    if let Err(error) = result {
                        controller.retry_lifecycle();
                        adx_observability::warn!(%error, "lifecycle deadline incomplete");
                    }
                    observers.arm_exit(&controller);
                    continue;
                }
            };
            let Envelope {
                command,
                reply,
                trace,
            } = envelope;
            let trace = match &command {
                Command::CheckAuthority(check, _) => controller.authority_trace(check),
                _ => trace,
            };
            if let Some(trace) = trace {
                trace.attribute("environment.id", controller.record.spec.id.clone());
                let reconciliation = matches!(&command, Command::CheckAuthority(_, _));
                let work = trace.scope(|| {
                    adx_observability::trace::with_current_context(async {
                        adx_observability::trace::Trace::child("environment.execute")
                            .run(controller.dispatch(command, reply))
                            .await;
                    })
                });
                if reconciliation {
                    trace.run(work).await;
                } else {
                    // Queue time ends at dispatch. The captured context keeps
                    // execution linked even after the queue span has ended.
                    drop(trace);
                    work.await;
                }
            } else {
                // Observation is passive. State-changing work below creates its
                // own lifecycle spans only when that work actually occurs.
                controller.dispatch(command, reply).await;
            }
            if controller.record.state == EnvironmentState::Failed {
                controller.retry_lifecycle();
            }
        }
    });
    EnvironmentHandle {
        tx,
        demand,
        current,
        cleanup_pending,
        metrics_registration,
    }
}

impl Controller {
    async fn dispatch(&mut self, command: Command, reply: Reply) {
        let command = match command {
            Command::CheckAuthority(check, ack) => {
                self.authority_fenced = true;
                let result = self.check_authority(*check).await;
                if result.is_err() {
                    adx_observability::trace::error();
                }
                self.authority_fenced = result.is_err();
                self.publish_view();
                let _ = ack.send(result);
                return;
            }
            Command::AlignAuthority(record, ack) => {
                let result = self.align_authority(*record).await;
                if result.is_ok() {
                    self.authority_fenced = false;
                }
                self.publish_view();
                let _ = ack.send(result);
                return;
            }
            Command::Snapshot(request, ack) => {
                let result = if self.retired || self.authority_fenced {
                    Err(Error::Conflict)
                } else {
                    self.snapshot(request).await
                };
                let _ = ack.send(result);
                return;
            }
            Command::RuntimeEvent(identity, ack) => {
                let result = if self.retired
                    || self.authority_fenced
                    || identity
                        != super::runtime_control::RuntimeControlClient::identity(&self.record)
                    || self.record.state != EnvironmentState::Running
                {
                    Ok(())
                } else {
                    async {
                        if !self.runtime_probe_failure().await? {
                            self.workload_checkpoint().await?;
                        }
                        Ok(())
                    }
                    .await
                };
                let _ = ack.send(result);
                return;
            }
            Command::Tick(ack) => {
                let result = if self.retired || self.authority_fenced {
                    Err(Error::Conflict)
                } else {
                    self.tick().await
                };
                self.trace_observation_error("tick", &result);
                let _ = ack.send(result);
                return;
            }
            Command::Expire(now, ack) => {
                let result = if self.retired || self.authority_fenced {
                    Err(Error::Conflict)
                } else {
                    self.expire_checkpoint(now).await
                };
                self.trace_observation_error("checkpoint_expiry", &result);
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
        if self.authority_fenced {
            let _ = reply.send(Err(Error::Unavailable(
                "environment authority reconciliation pending".into(),
            )));
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
            Command::CollectFailed(_) => "collect_failed",
            Command::Sync => "sync",
            Command::Reconcile => "reconcile",
            _ => unreachable!(),
        };
        let result = match command {
            Command::Snapshot(_, _)
            | Command::AlignAuthority(_, _)
            | Command::CheckAuthority(_, _)
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
                        expected_revision: self
                            .capture_preparation
                            .as_ref()
                            .filter(|(op, _)| op.kind == adx_core::LifecycleKind::Resume)
                            .map_or(self.record.revision, |(op, _)| op.expected_revision),
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
    capture_preparation: Option<(adx_core::CompletedOperation, std::path::PathBuf)>,
    create_deadline: Option<tokio::time::Instant>,
    auto_pause_guard: Option<((String, u64, u64), u64)>,
    demand: Arc<std::sync::atomic::AtomicU64>,
    observed_demand: u64,
    recovery_files: Option<crate::checkpoint::RestoreAccess>,
    execution_files: Vec<crate::checkpoint::RestoreAccess>,
    record: EnvironmentRecord,
    services: Arc<Services>,
    held: bool,
    durability: Option<Durability>,
    retired: bool,
    authority_fenced: bool,
    obsolete_checkpoints: Vec<adx_core::CheckpointArtifact>,
    idle: Option<((String, u64, u64), tokio::time::Instant)>,
    restart_after: Option<tokio::time::Instant>,
    health_failures: u32,
    events: events::LifecycleEvents,
    observed: tokio::sync::watch::Sender<(EnvironmentRecord, Option<Durability>)>,
    metrics_registration: crate::metrics::Registration,
    cleanup_pending: Arc<std::sync::atomic::AtomicBool>,
}

impl Drop for Controller {
    fn drop(&mut self) {
        self.services.metrics.unregister(&self.metrics_registration);
    }
}

impl Controller {
    fn publish_view(&self) {
        let eligible = self.record.state == EnvironmentState::Running
            && !self.retired
            && !self.authority_fenced
            && !self
                .cleanup_pending
                .load(std::sync::atomic::Ordering::Acquire);
        self.services
            .metrics
            .observe(&self.metrics_registration, eligible.then_some(&self.record));
        self.observed
            .send_replace((self.record.clone(), self.durability));
    }

    pub(super) fn hold_artifact(
        &mut self,
        artifact: adx_core::CheckpointArtifact,
        access: crate::checkpoint::RestoreAccess,
    ) {
        self.record.execution_artifacts.retain(|a| {
            a.size_bytes != 0 || a.storage != artifact.storage || a.location != artifact.location
        });
        if !self.record.execution_artifacts.contains(&artifact) {
            self.record.execution_artifacts.push(artifact);
        }
        if let Some(previous) = self.recovery_files.replace(access) {
            self.execution_files.push(previous);
        }
    }

    fn trace_observation_error<T>(&self, observation: &'static str, result: &Result<T>) {
        if result.is_err() {
            // Emit only a failure event after a passive check fails. Successful
            // checks never allocate a span; lifecycle work has its own spans.
            let trace = adx_observability::trace::Trace::child("environment.observation_failed");
            trace.attribute("environment.id", self.record.spec.id.clone());
            trace.attribute("environment.observation", observation.into());
            trace.scope(adx_observability::trace::error);
        }
    }
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
                checkpoint.store.release(&cp.artifact).await?;
                self.record.checkpoint = None;
            }
        }
        while let Some(artifact) = self.obsolete_checkpoints.last() {
            let key = (artifact.storage.clone(), artifact.location.clone());
            if !references.contains(&key) {
                checkpoint.store.release(artifact).await?;
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
        if self
            .record
            .execution_artifacts
            .iter()
            .any(|a| a.size_bytes == 0)
        {
            // Capture RPC outcome was not durably sealed. Stop the writer before dropping its reservation.
            self.cleanup().await?;
            self.transition(Event::Fail)?;
            return self.sync().await;
        }
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
            if self.execution_files.is_empty() {
                if let Some(services) = &self.services.checkpoint {
                    for artifact in &self.record.execution_artifacts {
                        self.execution_files
                            .push(services.store.prepare_restore(artifact).await?);
                    }
                }
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
                    self.recovery_files = Some(services.store.prepare_restore(&cp.artifact).await?);
                }
            }
            tokio::time::timeout(self.services.operation_timeout, async {
                self.services.readiness.wait_ready(&self.record).await?;
                if let Some(control) = &self.services.runtime_control {
                    if control.uses_stream() {
                        // A previous node process may have lost the idle barrier reply.
                        control.reopen_idle(&self.record).await?;
                    }
                }
                self.services.routes.activate(&self.record).await
            })
            .await
            .map_err(|_| Error::Unavailable("recovery readiness timed out".into()))??;
        } else if self.record.state == EnvironmentState::Paused {
            let cp = self
                .record
                .checkpoint
                .clone()
                .or_else(|| self.record.snapshot_source.clone())
                .ok_or(Error::Conflict)?;
            let services = self.services.checkpoint.clone().ok_or(Error::Conflict)?;
            let had_inputs = !self.record.execution_artifacts.is_empty();
            self.cleanup().await?;
            if had_inputs {
                self.record.revision =
                    self.record.revision.checked_add(1).ok_or(Error::Conflict)?;
            }
            if cp.expired(super::checkpoint::now()?) {
                return self.delete().await;
            }
            match services.store.validate_artifact(&cp.artifact).await {
                Ok(()) => (),
                Err(Error::Invalid(_) | Error::NotFound) => self.transition(Event::Fail)?,
                Err(error) => return Err(error),
            }
        } else {
            let lost_running = self.record.state == EnvironmentState::Running;
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
            // A boot/reconnect can confirm the same loss that a live monitor
            // would detect. Keep the existing bounded restart policy after
            // physical cleanup, without restoring a snapshot or resetting attempts.
            if lost_running && !self.record.spec.execution.failover {
                self.schedule_restart();
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
        self.publish_view();
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
        let durability = timeout(
            self.services.operation_timeout,
            adx_observability::trace::Trace::child("environment.state.commit")
                .run_result(self.services.sink.commit(&self.record)),
        )
        .await
        .map_err(|_| Error::Unavailable("state commit timed out".into()))??;
        self.durability = Some(durability);
        self.publish_view();
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
                .release(artifact)
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
        if self.record.spec.snapshot_id.is_some()
            && self.record.checkpoint.is_none()
            && self.record.snapshot_source.is_none()
        {
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
                            let cp = self
                                .record
                                .checkpoint
                                .clone()
                                .or_else(|| self.record.snapshot_source.clone())
                                .ok_or(Error::Conflict)?;
                            let store = &self
                                .services
                                .checkpoint
                                .as_ref()
                                .ok_or(Error::Conflict)?
                                .store;
                            let path = store.prepare_restore(&cp.artifact).await?;
                            // Retain through the RPC, including ambiguous failure.
                            self.hold_artifact(cp.artifact.clone(), path);
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
                self.cleanup_pending
                    .store(true, std::sync::atomic::Ordering::Release);
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
        self.services.metrics.remove(&self.metrics_registration);
        self.cleanup_pending
            .store(true, std::sync::atomic::Ordering::Release);
        timeout(self.services.operation_timeout, async {
            self.services.routes.retire(&self.record).await?;
            if let Some(control) = &self.services.runtime_control {
                control.retire(&self.record);
            }
            self.services.runtime.remove(&self.record.runtime.id).await
        })
        .await
        .map_err(|_| Error::Unavailable("environment cleanup timed out".into()))??;
        self.cleanup_pending
            .store(false, std::sync::atomic::Ordering::Release);
        self.capture_preparation = None;
        self.recovery_files = None;
        self.execution_files.clear();
        self.record.execution_artifacts.clear();
        self.services.metrics.remove(&self.metrics_registration);
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
        self.record.snapshot_source = None;
        self.record.runtime.ip = None;
        self.transition(Event::Removed)?;
        self.sync().await
    }
}
