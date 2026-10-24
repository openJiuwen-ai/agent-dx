//! Lifecycle work is driven by control observations, backend exits and owned deadlines.
use super::*;
use adx_core::runtime::{RuntimeIdentity, RuntimePhase, RuntimeStatus};
use std::{future::Future, pin::Pin};
use tokio::{sync::watch, time::Instant as Deadline};

type ExitWait = Pin<Box<dyn Future<Output = Result<adx_core::RuntimeFailure>> + Send>>;
#[derive(Default)]
pub(super) struct LifecycleEvents {
    runtime: Option<RuntimeStatus>,
    proxy: Option<(String, u64, u64)>,
    disconnected: Option<Deadline>,
    retry: Option<Deadline>,
    retry_attempts: u32,
    reopen_idle: bool,
    workload: Option<PendingWorkload>,
}
struct PendingWorkload {
    identity: RuntimeIdentity,
    operation_id: String,
    deadline: Deadline,
}
#[derive(Default)]
pub(super) struct Observers {
    identity: Option<RuntimeIdentity>,
    pub control: Option<watch::Receiver<Option<RuntimeStatus>>>,
    pub initial: bool,
    pub exit: Option<ExitWait>,
}
impl Observers {
    pub fn refresh(&mut self, controller: &mut Controller) {
        let identity = (controller.record.state == EnvironmentState::Running
            && !controller.retired
            && !controller.authority_fenced)
            .then(|| crate::runtime_control::RuntimeControlClient::identity(&controller.record));
        if identity == self.identity {
            return;
        }
        *self = Self::default();
        let retry = controller.events.retry;
        let retry_attempts = controller.events.retry_attempts;
        let workload = controller.events.workload.take().filter(|pending| {
            controller.record.state == EnvironmentState::Running
                && !controller.retired
                && pending.identity
                    == crate::runtime_control::RuntimeControlClient::identity(&controller.record)
        });
        controller.events = LifecycleEvents {
            workload,
            retry,
            retry_attempts,
            ..LifecycleEvents::default()
        };
        controller.idle = None;
        let Some(identity) = identity else {
            return;
        };
        self.control = controller
            .services
            .runtime_control
            .as_ref()
            .and_then(|client| client.subscribe(&controller.record).ok().flatten());
        self.initial = self.control.is_some();
        self.identity = Some(identity);
        controller.events.retry = None;
        controller.events.retry_attempts = 0;
        self.arm_exit(controller);
    }
    pub fn arm_exit(&mut self, controller: &Controller) {
        if self.exit.is_none() && self.identity.is_some() {
            let runtime = controller.services.runtime.clone();
            let id = controller.record.runtime.id.clone();
            self.exit = Some(Box::pin(async move { runtime.wait_exit(&id).await }));
        }
    }
}

impl Controller {
    pub(super) fn lifecycle_deadline(&self) -> Option<Deadline> {
        if self.services.runtime_control.is_none() || self.retired || self.authority_fenced {
            return None;
        }
        if let Some(retry) = self.events.retry {
            return Some(retry);
        }
        let mut deadline = (self.record.state == EnvironmentState::Failed
            && self.record.restart_pending)
            .then(|| self.restart_after.unwrap_or_else(Deadline::now));
        let mut include = |candidate: Deadline| {
            deadline = Some(deadline.map_or(candidate, |old| old.min(candidate)));
        };
        if let Some((_, since)) = &self.idle {
            include(
                *since
                    + std::time::Duration::from_secs(
                        self.record.spec.lifecycle.idle_timeout_seconds,
                    ),
            );
        }
        if let Some(since) = self.events.disconnected {
            include(since + self.reconnect_grace());
        }
        if let Some(expires) = self
            .record
            .checkpoint
            .as_ref()
            .and_then(|cp| cp.expires_at_unix_seconds)
        {
            if let Ok(now) = crate::checkpoint::now() {
                include(
                    Deadline::now() + std::time::Duration::from_secs(expires.saturating_sub(now)),
                );
            }
        }
        if self.events.workload.is_some() && self.events.runtime.is_some() {
            include(Deadline::now());
        }
        if self.events.reopen_idle {
            include(Deadline::now());
        }
        if self.durability == Some(Durability::Journaled)
            || (self.record.state == EnvironmentState::Failed && self.record.resources_held)
        {
            include(Deadline::now() + std::time::Duration::from_secs(1));
        }
        deadline
    }
    fn reconnect_grace(&self) -> std::time::Duration {
        std::time::Duration::from_secs(5)
            .saturating_mul(self.services.health_failure_threshold.unwrap_or(1))
    }
    pub(super) async fn control_observed(&mut self, status: Option<RuntimeStatus>) -> Result<()> {
        let Some(status) = status else {
            self.events.runtime = None;
            self.idle = None;
            self.events.disconnected.get_or_insert_with(Deadline::now);
            return Ok(());
        };
        if status.identity != crate::runtime_control::RuntimeControlClient::identity(&self.record) {
            return Err(Error::Conflict);
        }
        self.events.disconnected = None;
        if status.phase == RuntimePhase::Failed || status.failure.is_some() {
            self.record.failure = status.failure.clone().map(Into::into);
            return self.unexpected_exit().await;
        }
        self.remember_workload(&status);
        self.events.runtime = Some(status);
        self.continue_workload().await?;
        self.update_idle();
        Ok(())
    }
    fn remember_workload(&mut self, status: &RuntimeStatus) {
        let Some(operation_id) = status
            .requested_checkpoint
            .as_ref()
            .filter(|_| self.services.checkpoint.is_some())
        else {
            self.events.workload = None;
            return;
        };
        if self.events.workload.as_ref().is_some_and(|pending| {
            pending.identity == status.identity && pending.operation_id == *operation_id
        }) {
            return;
        }
        // Capture the budget once. Replayed status notifications cannot extend it.
        let deadline = super::workload_checkpoint::checkpoint_deadline(
            status.requested_checkpoint_deadline_unix_millis,
        )
        .unwrap_or(Some(Deadline::now()))
        .unwrap_or_else(|| {
            Deadline::now() + super::workload_checkpoint::DEFAULT_BACKEND_CHECKPOINT_TIMEOUT
        });
        self.events.workload = Some(PendingWorkload {
            identity: status.identity.clone(),
            operation_id: operation_id.clone(),
            deadline,
        });
    }
    pub(super) fn workload_deadline(
        &self,
        identity: &RuntimeIdentity,
        operation_id: &str,
    ) -> Option<Deadline> {
        self.events
            .workload
            .as_ref()
            .filter(|pending| pending.identity == *identity && pending.operation_id == operation_id)
            .map(|pending| pending.deadline)
    }
    async fn continue_workload(&mut self) -> Result<()> {
        let Some(pending) = self.events.workload.as_ref() else {
            return Ok(());
        };
        if pending.identity != crate::runtime_control::RuntimeControlClient::identity(&self.record)
        {
            self.events.workload = None;
            return Ok(());
        }
        // Read the latest pushed snapshot. A missing connection is not evidence
        // that Execd has completed the operation, so retain the pending request.
        let Some(status) = self
            .services
            .runtime_control
            .as_ref()
            .ok_or_else(|| Error::Unavailable("runtime control unavailable".into()))?
            .pending_checkpoint(&self.record)?
        else {
            self.events.runtime = None;
            self.idle = None;
            self.events.disconnected.get_or_insert_with(Deadline::now);
            return Ok(());
        };
        self.remember_workload(&status);
        if self.events.workload.is_none() {
            return Ok(());
        }
        if self.workload_checkpoint().await? {
            self.events.workload = None;
            Ok(())
        } else {
            Err(Error::Unavailable(
                "pending checkpoint observation unavailable".into(),
            ))
        }
    }
    pub(super) fn proxy_observed(&mut self, status: Result<(String, u64, u64)>) {
        self.events.proxy = status.ok();
        self.update_idle();
    }
    fn update_idle(&mut self) {
        let demand = self.demand.load(std::sync::atomic::Ordering::Acquire);
        if demand != self.observed_demand {
            self.idle = None;
            self.observed_demand = demand;
        }
        if self.record.state != EnvironmentState::Running
            || self.events.reopen_idle
            || self.record.spec.lifecycle.idle_timeout_seconds == 0
        {
            self.idle = None;
            return;
        }
        let Some(runtime) = self.events.runtime.as_ref().filter(|status| {
            status.phase == RuntimePhase::Running
                && status.active_requests == 0
                && status.activity_revision != 0
        }) else {
            self.idle = None;
            return;
        };
        let Some((session, revision, 0)) = &self.events.proxy else {
            self.idle = None;
            return;
        };
        let stamp = (session.clone(), *revision, runtime.activity_revision);
        if self.idle.as_ref().is_none_or(|(old, _)| *old != stamp) {
            self.idle = Some((stamp, Deadline::now()));
        }
    }
    pub(super) fn proxy_stamp(&self) -> Option<(String, u64, u64)> {
        self.events.proxy.clone()
    }
    pub(super) fn retry_lifecycle(&mut self) {
        if self.events.retry.is_some() {
            return;
        }
        self.events.retry_attempts = self.events.retry_attempts.saturating_add(1);
        let seconds = 1_u64 << self.events.retry_attempts.saturating_sub(1).min(5);
        self.events.retry = Some(Deadline::now() + std::time::Duration::from_secs(seconds));
    }
    pub(super) async fn lifecycle_due(&mut self) -> Result<()> {
        self.events.retry = None;
        if self.record.state == EnvironmentState::Running && self.events.reopen_idle {
            self.recover_idle_admission().await?;
        }
        if let Ok(now) = crate::checkpoint::now() {
            self.expire_checkpoint(now).await?;
        }
        if self.record.state == EnvironmentState::Running {
            self.continue_workload().await?;
            if self.record.state != EnvironmentState::Running {
                return Ok(());
            }
            if self
                .events
                .disconnected
                .is_some_and(|since| since.elapsed() >= self.reconnect_grace())
            {
                // Only an exceptional lost stream triggers a backend confirmation.
                let status = timeout(
                    self.services.operation_timeout,
                    self.services.runtime.observe(&self.record.runtime.id),
                )
                .await
                .map_err(|_| Error::Unavailable("exit confirmation timed out".into()))??;
                if !status.running {
                    self.record.failure = status.failure;
                    return self.unexpected_exit().await;
                }
                if self.services.health_failure_threshold.is_some() {
                    return self.unexpected_exit().await;
                }
                self.events.disconnected = Some(Deadline::now());
            }
            self.update_idle();
            if let Some((stamp, since)) = self.idle.clone() {
                if since.elapsed()
                    >= std::time::Duration::from_secs(
                        self.record.spec.lifecycle.idle_timeout_seconds,
                    )
                {
                    let demand = self.demand.load(std::sync::atomic::Ordering::Acquire);
                    match self.record.spec.lifecycle.idle_action {
                        adx_core::lifecycle::IdleAction::Delete => {
                            if self
                                .services
                                .routes
                                .quiesce(&self.record, &stamp.0, stamp.1)
                                .await?
                            {
                                let control =
                                    self.services.runtime_control.clone().ok_or_else(|| {
                                        Error::Unavailable(
                                            "idle barrier requires runtime control".into(),
                                        )
                                    })?;
                                let barrier = control.quiesce_idle(&self.record, stamp.2).await;
                                match barrier {
                                    Ok(()) => {
                                        self.delete().await?;
                                    }
                                    Err(error) => {
                                        self.events.reopen_idle = true;
                                        self.recover_idle_admission().await?;
                                        if error != Error::Conflict {
                                            return Err(error);
                                        }
                                    }
                                }
                            }
                        }
                        adx_core::lifecycle::IdleAction::Pause => {
                            let policy =
                                self.record.spec.lifecycle.pause.clone().unwrap_or_default();
                            self.auto_pause(
                                crate::checkpoint::PauseRequest {
                                    operation_id: format!(
                                        "idle-{}-{}",
                                        self.record.assignment.generation,
                                        uuid::Uuid::new_v4()
                                    ),
                                    expected_revision: self.record.revision,
                                    retention: policy.retention,
                                    timeout_seconds: policy.timeout_seconds,
                                },
                                stamp,
                                demand,
                            )
                            .await?;
                        }
                    }
                    self.idle = None;
                    self.update_idle();
                }
            }
        } else if self.record.state == EnvironmentState::Failed {
            self.tick().await?;
        }
        if self.record.state != EnvironmentState::Failed
            && self.durability == Some(Durability::Journaled)
        {
            self.sync().await?;
        }
        Ok(())
    }
    async fn recover_idle_admission(&mut self) -> Result<()> {
        self.idle = None;
        let control =
            self.services.runtime_control.clone().ok_or_else(|| {
                Error::Unavailable("idle barrier requires runtime control".into())
            })?;
        control.reopen_idle(&self.record).await?;
        self.services.routes.reopen(&self.record).await?;
        self.events.reopen_idle = false;
        Ok(())
    }
    pub(super) async fn backend_exited(
        &mut self,
        result: Result<adx_core::RuntimeFailure>,
    ) -> Result<()> {
        match result {
            Ok(failure) => {
                self.record.failure = Some(failure);
                self.unexpected_exit().await
            }
            Err(error) => Err(error),
        }
    }
}
