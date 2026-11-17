use super::Controller;
use adx_core::{EnvironmentState, Error, Event, Result};
use std::time::Duration;
use tokio::time::Instant;

impl Controller {
    pub(super) async fn tick(&mut self) -> Result<Option<crate::OperationResult>> {
        // Keep lifecycle-sized state out of the caller's dispatch future.
        Box::pin(self.tick_inner()).await?;
        Ok(self.published_delete())
    }

    async fn tick_inner(&mut self) -> Result<()> {
        let demand = self.sync_demand();
        let failed_cleanup =
            self.record.state == EnvironmentState::Failed && !self.record.restart_pending;
        if self.durability == Some(crate::Durability::Journaled) && !failed_cleanup {
            self.sync().await?;
        }
        if self.record.state == EnvironmentState::Running {
            // A quiesced route left behind by a failed pause recovers
            // here: the monitoring tick retries the reopen even after
            // the pressure has fully subsided and no further high
            // signal rings.
            if self.events.reopen_idle {
                self.recover_idle_admission().await?;
            }
            let running = tokio::time::timeout(
                self.services.operation_timeout,
                self.services.runtime.observe(&self.record.runtime.id),
            )
            .await
            .unwrap_or_else(|_| Err(Error::Unavailable("runtime observation timed out".into())));
            match running {
                Err(error) => {
                    self.idle = None;
                    return Err(error);
                }
                Ok(status) if !status.running => {
                    self.record.failure = status.failure;
                    return self.runtime_stopped().await;
                }
                Ok(_) => (),
            }
            if self.runtime_probe_failure().await? {
                return Ok(());
            }
            if let Ok(Ok(usage)) = tokio::time::timeout(
                self.services.operation_timeout,
                self.services.runtime.stats(&self.record.runtime.id),
            )
            .await
            {
                self.services.metrics.record(
                    &self.metrics_registration,
                    &self.record.runtime.id,
                    self.record.assignment.generation,
                    usage,
                );
            }
            if self.workload_checkpoint().await? {
                return Ok(());
            }
            // Explicit mandatory services remain health dependencies even when
            // optional idle and periodic health policies are disabled.
            let services_require_health = self.record.spec.required_services().next().is_some();
            let runtime_activity = if self.record.spec.lifecycle.idle_timeout_seconds > 0
                || self.services.health_failure_threshold.is_some()
                || services_require_health
            {
                tokio::time::timeout(
                    self.services.operation_timeout,
                    self.services.readiness.activity(&self.record),
                )
                .await
                .unwrap_or_else(|_| Err(Error::Unavailable("runtime health timed out".into())))
            } else {
                Ok((0, 0))
            };
            if services_require_health && runtime_activity.is_err() {
                return self.unexpected_exit().await;
            }
            if let Some(threshold) = self.services.health_failure_threshold {
                if runtime_activity.is_ok() {
                    self.health_failures = 0;
                } else {
                    self.health_failures = self.health_failures.saturating_add(1);
                    if self.health_failures >= threshold {
                        return self.unexpected_exit().await;
                    }
                }
            }
            if self.record.spec.lifecycle.idle_timeout_seconds == 0 {
                return Ok(());
            }
            let sample = tokio::time::timeout(self.services.operation_timeout, async {
                let runtime = runtime_activity?;
                let proxy = self.services.routes.activity(&self.record).await?;
                Ok::<_, Error>((runtime, proxy))
            })
            .await;
            match sample {
                Ok(Ok(((runtime_revision, 0), (session, proxy_revision, 0)))) => {
                    let stamp = (session, proxy_revision, runtime_revision);
                    match &self.idle {
                        Some((old, since)) if *old == stamp => {
                            if since.elapsed()
                                >= Duration::from_secs(
                                    self.record.spec.lifecycle.idle_timeout_seconds,
                                )
                            {
                                match self.record.spec.lifecycle.idle_action {
                                    adx_core::lifecycle::IdleAction::Delete => {
                                        self.operation_intent(
                                            adx_core::observation::OperationIntent::Idle,
                                        );
                                        self.delete(false).await?;
                                    }
                                    adx_core::lifecycle::IdleAction::Pause => {
                                        let policy = self
                                            .record
                                            .spec
                                            .lifecycle
                                            .pause
                                            .clone()
                                            .unwrap_or_default();
                                        self.auto_pause(
                                            crate::checkpoint::PauseRequest {
                                                // An aborted preparation does not advance the
                                                // Environment revision. A later idle attempt
                                                // must not replay that terminal operation.
                                                operation_id: format!(
                                                    "idle-{}-{}-{}",
                                                    self.record.assignment.generation,
                                                    self.record.revision,
                                                    uuid::Uuid::new_v4()
                                                ),
                                                expected_revision: self.record.revision,
                                                retention: policy.retention,
                                                timeout_seconds: policy.timeout_seconds,
                                                storage: None,
                                            },
                                            stamp.clone(),
                                            demand,
                                            super::PauseOrigin::Idle,
                                        )
                                        .await?;
                                        self.idle = None;
                                    }
                                }
                            }
                        }
                        _ => self.idle = Some((stamp, Instant::now())),
                    }
                }
                // Unknown, stale or active observations restart the idle window.
                _ => self.idle = None,
            }
            // Tick-driven path shares the pressure trigger so tests observe
            // the same behavior as the event loop.
            self.consider_pressure_pause().await?;
        } else if self.record.state == EnvironmentState::Failed && self.record.restart_pending {
            let Some(policy) = &self.record.spec.lifecycle.restart else {
                return Ok(());
            };
            if self.record.restart_attempts >= policy.max_attempts {
                return Ok(());
            }
            if self.restart_after.is_none() {
                self.schedule_restart();
            }
            if self.restart_after.is_some_and(|due| Instant::now() >= due) {
                // A failed cleanup is never permission to start a second runtime.
                self.cleanup().await?;
                self.start_attempt(true, false, None).await?;
            }
        } else if self.record.state == EnvironmentState::Failed {
            let held = self.record.resources_held;
            // Failed creates release capacity independently of backend cleanup.
            // Retrying cleanup must not delay publication of that released ledger.
            let cleanup = self.cleanup().await;
            let released = held && !self.record.resources_held;
            if released {
                self.record.revision =
                    self.record.revision.checked_add(1).ok_or(Error::Conflict)?;
            }
            if released || self.durability != Some(crate::Durability::Published) {
                self.sync().await?;
            }
            cleanup?;
        }
        Ok(())
    }

    pub(super) fn published_delete(&self) -> Option<crate::OperationResult> {
        (self.record.state == EnvironmentState::Deleted
            && !self.record.resources_held
            && self.durability == Some(crate::Durability::Published))
        .then(|| crate::OperationResult {
            newly_created: false,
            record: self.record.clone(),
            durability: crate::Durability::Published,
        })
    }
    pub(super) async fn runtime_probe_failure(&mut self) -> Result<bool> {
        if self.record.state != EnvironmentState::Running
            || (self.record.spec.execution.healthcheck_probe.is_none()
                && self.record.spec.execution.readiness_probe.is_none())
        {
            return Ok(false);
        }
        let observation = tokio::time::timeout(
            self.services.operation_timeout,
            self.services.readiness.probe_failure(&self.record),
        )
        .await;
        let Ok(Ok(Some(failure))) = observation else {
            return Ok(false);
        };
        self.record.failure = Some(failure.into());
        self.unexpected_exit().await?;
        Ok(true)
    }
    // Absence or a generic exit after an intended stop is not independent fault
    // evidence. Preserve intent through reconciliation; explicit OOM/probe
    // evidence still takes the unexpected-exit path.
    pub(super) async fn runtime_stopped(&mut self) -> Result<()> {
        if self.intended_execution_absence() {
            self.unexpected_exit_inner().await
        } else {
            self.unexpected_exit().await
        }
    }

    pub(super) async fn unexpected_exit(&mut self) -> Result<()> {
        let source = if self
            .record
            .failure
            .as_ref()
            .is_some_and(|f| f.reason == adx_core::RuntimeFailureReason::ProbeFailed)
        {
            adx_core::observation::EvidenceSource::Probe
        } else if self.record.failure.is_some() {
            adx_core::observation::EvidenceSource::BackendExit
        } else {
            adx_core::observation::EvidenceSource::Control
        };
        self.mark_interruption(source)?;
        let trace = adx_observability::trace::Trace::child("environment.unexpected_exit");
        trace.attribute("environment.id", self.record.spec.id.clone());
        trace
            .run_result(Box::pin(self.unexpected_exit_inner()))
            .await
    }

    async fn unexpected_exit_inner(&mut self) -> Result<()> {
        self.idle = None;
        let cleanup = self.cleanup().await;
        self.transition(Event::Fail)?;
        if self.record.spec.execution.failover {
            self.record.restart_pending = false;
            self.restart_after = None;
            if let Err(error) = cleanup {
                self.sync().await?;
                return Err(error);
            }
            let now = crate::checkpoint::now()?;
            if self
                .record
                .checkpoint
                .as_ref()
                .is_none_or(|checkpoint| checkpoint.expired(now))
            {
                self.sync().await?;
                return Err(Error::Unavailable(
                    "failover recovery point is unavailable".into(),
                ));
            }
            return self.start_attempt(true, true, None).await.map(|_| ());
        }
        self.schedule_restart();
        self.sync().await?;
        cleanup
    }
    pub(super) fn schedule_restart(&mut self) {
        self.record.restart_pending = self
            .record
            .spec
            .lifecycle
            .restart
            .as_ref()
            .is_some_and(|p| self.record.restart_attempts < p.max_attempts);
        self.restart_after = self.record.spec.lifecycle.restart.as_ref().and_then(|p| {
            Instant::now().checked_add(Duration::from_secs(
                p.backoff_seconds(self.record.restart_attempts),
            ))
        });
    }
}
