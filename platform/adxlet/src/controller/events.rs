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
    pub(super) reopen_idle: bool,
    workload: Option<PendingWorkload>,
}
struct PendingWorkload {
    identity: RuntimeIdentity,
    operation_id: String,
    deadline: Deadline,
}
/// Releases a pressure-pause in-flight registration when it goes out of
/// scope, covering completion, failure and task cancellation alike.
struct PressurePauseFlight {
    ledger: std::sync::Arc<std::sync::Mutex<crate::pressure_pause::PressurePauseLedger>>,
    env_id: String,
}
impl Drop for PressurePauseFlight {
    fn drop(&mut self) {
        self.ledger
            .lock()
            .expect("pressure pause ledger lock poisoned")
            .release(&self.env_id);
    }
}
#[derive(Default)]
pub(super) struct Observers {
    identity: Option<RuntimeIdentity>,
    pub control: Option<watch::Receiver<Option<RuntimeStatus>>>,
    pub initial: bool,
    pub exit: Option<ExitWait>,
    /// Node-scoped pressure wake subscription. Independent of the runtime
    /// identity lifecycle so a long-lived Running environment keeps it.
    pub pressure_signal: Option<watch::Receiver<()>>,
}
impl Observers {
    pub fn refresh(&mut self, controller: &mut Controller) {
        // The pressure signal subscription is node-scoped and independent of
        // the runtime identity: establish it before the identity fast-path
        // below, which returns early for a stable Running environment, and
        // carry it across identity resets (reset_preserving_pressure).
        if self.pressure_signal.is_none() {
            self.pressure_signal = Some(controller.services.pressure_signal.subscribe());
        }
        let identity = (controller.record.state == EnvironmentState::Running
            && !controller.retired
            && !controller.authority_fenced)
            .then(|| crate::runtime_control::RuntimeControlClient::identity(&controller.record));
        if identity == self.identity {
            return;
        }
        self.reset_preserving_pressure();
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
    /// Identity transitions reset the runtime-scoped observers. The
    /// node-scoped pressure subscription is not one of them: carry it
    /// across the reset, because a round firing while the controller
    /// sits in the post-reset select stays visible only to the carried
    /// receiver — a fresh re-subscription marks the current watch
    /// version as seen and silently eats it.
    fn reset_preserving_pressure(&mut self) {
        let pressure_signal = self.pressure_signal.take();
        *self = Self::default();
        self.pressure_signal = pressure_signal;
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
        // Required guest services are part of the runtime's readiness contract,
        // even while Execd remains available for control and diagnostics.
        if status.phase == RuntimePhase::Running
            && status
                .validate_required_services(self.record.spec.required_services())
                .is_err()
        {
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
        self.sync_demand();
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
    /// Demand moves before its command is observed: a queued
    /// ensure-running has already bumped the counter. Refresh the idle
    /// window against it so a stale window cannot ride through the
    /// eligibility checks, and return the demand the surviving window
    /// is bound to.
    pub(super) fn sync_demand(&mut self) -> u64 {
        let demand = self.demand.load(std::sync::atomic::Ordering::Acquire);
        if demand != self.observed_demand {
            self.idle = None;
            self.observed_demand = demand;
        }
        demand
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
                    return self.runtime_stopped().await;
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
                    // The window-bound demand: update_idle just refreshed
                    // it, and a bump racing this read stays visible to
                    // the recheck inside auto_pause.
                    let demand = self.observed_demand;
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
                                        self.operation_intent(
                                            adx_core::observation::OperationIntent::Idle,
                                        );
                                        self.delete(false).await?;
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
                                    storage: None,
                                },
                                stamp,
                                demand,
                                super::PauseOrigin::Idle,
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
        // Correctness fuse: if the pressure signal subscription is ever
        // broken by a future refactor, an idle environment is still
        // evaluated when its own lifecycle deadline fires.
        self.consider_pressure_pause().await?;
        if self.record.state != EnvironmentState::Failed
            && self.durability == Some(Durability::Journaled)
        {
            self.sync().await?;
        }
        Ok(())
    }

    /// Evaluates a node-pressure pause for this environment. The wake-up
    /// only rings the bell; the verdict reads the pressure board here.
    /// Conjunction order: node facts first, eligibility next, the claim
    /// last so only a real candidate consumes a budget slot.
    pub(super) async fn consider_pressure_pause(&mut self) -> Result<()> {
        let options = self.services.pressure_pause_options.clone();
        if !options.enabled {
            return Ok(());
        }
        // Refresh the window against demand before any eligibility
        // claim: an ensure-running queued behind this evaluation has
        // already bumped the counter, and its stale window must not
        // pass the checks on a baseline that hides the bump.
        let demand = self.sync_demand();
        let board = self
            .services
            .pressure
            .read()
            .expect("pressure board lock poisoned")
            .clone();
        // C1: some resource sits at High. C2: failed rounds keep the board
        // untrusted for relief. C3: never observed, or too stale to act on.
        let high = board.memory.level == crate::resources::PressureLevel::High
            || board.disk.level == crate::resources::PressureLevel::High;
        if !high || board.consecutive_failures != 0 || board.observed_at_unix_seconds == 0 {
            return Ok(());
        }
        let now = crate::checkpoint::now()?;
        if now.saturating_sub(board.observed_at_unix_seconds) > options.freshness_seconds {
            return Ok(());
        }
        // C5: a Running environment holding an idle window. C6: its idle
        // action must be Pause under a configured idle policy.
        if self.record.state != EnvironmentState::Running
            || self.record.spec.lifecycle.idle_timeout_seconds == 0
            || self.record.spec.lifecycle.idle_action != adx_core::lifecycle::IdleAction::Pause
        {
            return Ok(());
        }
        let Some((stamp, since)) = self.idle.clone() else {
            return Ok(());
        };
        // C7: the idle window must be old enough that a recently resumed
        // environment is not immediately re-paused.
        if since.elapsed() < std::time::Duration::from_secs(options.min_idle_seconds) {
            self.services
                .metrics
                .pressure_skip(crate::metrics::pressure_skip_reason::WINDOW_TOO_YOUNG);
            return Ok(());
        }
        // C4 last: claiming consumes a rate slot and registers in-flight.
        let env_id = self.record.spec.id.clone();
        let ledger = self.services.pressure_pause_ledger.clone();
        let claimed = ledger
            .lock()
            .expect("pressure pause ledger lock poisoned")
            .claim(
                board.observed_at_unix_seconds,
                &env_id,
                options.budget,
                options.in_flight_cap,
                now,
            );
        match claimed {
            crate::pressure_pause::ClaimOutcome::Granted => (),
            crate::pressure_pause::ClaimOutcome::BudgetExhausted => {
                self.services
                    .metrics
                    .pressure_skip(crate::metrics::pressure_skip_reason::BUDGET_EXHAUSTED);
                return Ok(());
            }
            crate::pressure_pause::ClaimOutcome::InFlightExhausted => {
                self.services
                    .metrics
                    .pressure_skip(crate::metrics::pressure_skip_reason::IN_FLIGHT_EXHAUSTED);
                return Ok(());
            }
        }
        let flight = PressurePauseFlight {
            ledger: ledger.clone(),
            env_id,
        };
        // Escalation is governance best-effort: express the shared override
        // only when the mode allows it and the node accepts shared storage.
        let policy = self.record.spec.lifecycle.pause.clone().unwrap_or_default();
        let eligible = |mode: crate::EscalationMode,
                        spec: adx_core::lifecycle::CheckpointStorage| {
            match mode {
                crate::EscalationMode::Off => false,
                crate::EscalationMode::ConfiguredOnly => {
                    spec != adx_core::lifecycle::CheckpointStorage::Local
                }
                crate::EscalationMode::All => true,
            }
        };
        let escalation = if eligible(options.escalate_checkpoint_storage, policy.storage)
            && self.services.checkpoint.as_ref().is_some_and(|services| {
                services
                    .store
                    .validate_policy(adx_core::lifecycle::CheckpointStorage::Shared)
                    .is_ok()
            }) {
            Some(adx_core::lifecycle::CheckpointStorage::Shared)
        } else {
            None
        };
        let result = self
            .auto_pause(
                crate::checkpoint::PauseRequest {
                    operation_id: format!(
                        "pressure-{}-{}-{}",
                        self.record.assignment.generation,
                        self.record.revision,
                        uuid::Uuid::new_v4()
                    ),
                    expected_revision: self.record.revision,
                    retention: policy.retention,
                    timeout_seconds: policy.timeout_seconds,
                    storage: escalation,
                },
                stamp,
                // The window-bound demand: a bump racing this call stays
                // visible to the recheck inside auto_pause.
                demand,
                super::PauseOrigin::Pressure,
            )
            .await;
        drop(flight);
        result?;
        // The window restarts after a successful pressure pause, matching
        // the idle path.
        self.idle = None;
        Ok(())
    }

    /// Re-arms the idle admission after a quiesced route could not be
    /// reopened in line: the idle Delete path (quiesce barrier rejected)
    /// and the pressure pause path (reopen failed) share this recovery.
    pub(super) async fn recover_idle_admission(&mut self) -> Result<()> {
        self.idle = None;
        // Without runtime control there is no idle barrier to reopen;
        // the routes admission alone restores.
        if let Some(control) = self.services.runtime_control.clone() {
            control.reopen_idle(&self.record).await?;
        }
        self.services.reopen_route(&self.record).await?;
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
                self.runtime_stopped().await
            }
            Err(error) => Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A pressure round firing around an identity reset must stay visible:
    /// the subscription is carried across the reset instead of being
    /// re-established, because a fresh subscribe() marks the current watch
    /// version as seen and would silently eat the round that fired while
    /// no receiver was armed.
    #[tokio::test(start_paused = true)]
    async fn a_round_firing_across_an_identity_reset_stays_visible() {
        let mut observers = Observers::default();
        let (tx, _keep) = tokio::sync::watch::channel(());
        observers.pressure_signal = Some(tx.subscribe());
        // The round fires before the reset is observed; the carried
        // receiver has not seen it yet.
        tx.send(()).unwrap();
        observers.reset_preserving_pressure();
        assert!(
            observers.pressure_signal.is_some(),
            "the node-scoped subscription must survive the identity reset"
        );
        let observed = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            observers.pressure_signal.as_mut().unwrap().changed(),
        )
        .await;
        assert!(
            observed.is_ok(),
            "the carried subscription must still observe the fired round"
        );
        // The runtime-scoped observers do reset.
        assert!(observers.identity.is_none());
        assert!(observers.control.is_none());
        assert!(!observers.initial);
        assert!(observers.exit.is_none());
    }
}
