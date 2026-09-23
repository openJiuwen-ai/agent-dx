use super::Controller;
use adx_core::{EnvironmentState, Error, Event, Result};
use std::time::Duration;
use tokio::time::Instant;

impl Controller {
    pub(super) async fn tick(&mut self) -> Result<()> {
        if self.durability == Some(crate::Durability::Journaled) {
            self.sync().await?;
        }
        if self.record.state == EnvironmentState::Running {
            let running = tokio::time::timeout(
                self.services.operation_timeout,
                self.services.runtime.is_running(&self.record.runtime.id),
            )
            .await
            .unwrap_or_else(|_| Err(Error::Unavailable("runtime observation timed out".into())));
            match running {
                Err(error) => {
                    self.idle = None;
                    return Err(error);
                }
                Ok(false) => {
                    return self.unexpected_exit().await;
                }
                Ok(true) => (),
            }
            if let Ok(Ok(usage)) = tokio::time::timeout(
                self.services.operation_timeout,
                self.services.runtime.stats(&self.record.runtime.id),
            )
            .await
            {
                self.services
                    .metrics
                    .record(&self.record.spec.id, &self.record.runtime.id, usage);
            }
            if self.workload_checkpoint().await? {
                return Ok(());
            }
            let runtime_activity = if self.record.spec.lifecycle.idle_timeout_seconds > 0
                || self.services.health_failure_threshold.is_some()
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
                                self.delete().await?;
                            }
                        }
                        _ => self.idle = Some((stamp, Instant::now())),
                    }
                }
                // Unknown, stale or active observations restart the idle window.
                _ => self.idle = None,
            }
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
        } else if self.record.state == EnvironmentState::Failed && self.record.resources_held {
            self.cleanup().await?;
            self.record.revision = self.record.revision.checked_add(1).ok_or(Error::Conflict)?;
            self.sync().await?;
        }
        Ok(())
    }
    async fn unexpected_exit(&mut self) -> Result<()> {
        self.idle = None;
        let cleanup = self.cleanup().await;
        self.transition(Event::Fail)?;
        if self.record.spec.sandbox.failover {
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
                .is_none_or(|checkpoint| checkpoint.expires_at_unix_seconds <= now)
            {
                self.sync().await?;
                return Err(Error::Unavailable(
                    "failover recovery point is unavailable".into(),
                ));
            }
            return self.start_attempt(true, true, None).await.map(|_| ());
        }
        self.record.restart_pending = self
            .record
            .spec
            .lifecycle
            .restart
            .as_ref()
            .is_some_and(|p| self.record.restart_attempts < p.max_attempts);
        self.schedule_restart();
        self.sync().await?;
        cleanup
    }
    fn schedule_restart(&mut self) {
        self.restart_after = self.record.spec.lifecycle.restart.as_ref().and_then(|p| {
            Instant::now().checked_add(Duration::from_secs(
                p.backoff_seconds(self.record.restart_attempts),
            ))
        });
    }
}
