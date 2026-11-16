//! The current node process owns each execution's monotonic run timer.
use super::*;
use adx_core::environment_control::{EnvironmentControlRecord, RunExpiryAction};
use std::time::Duration;
use tokio::time::Instant as Deadline;

#[derive(Clone)]
pub(super) struct RunTimer {
    pub(super) runtime_id: String,
    pub(super) generation: u64,
    sequence: u64,
    pub(super) deadline: Deadline,
    retry_at: Option<Deadline>,
    started_at_unix_ms: u64,
}

impl RunTimer {
    fn matches(&self, runtime_id: &str, generation: u64, sequence: u64) -> bool {
        self.runtime_id == runtime_id && self.generation == generation && self.sequence == sequence
    }

    fn renew(&mut self, now: Deadline, seconds: u64, extend_only: bool) -> Result<bool> {
        let next = now
            .checked_add(Duration::from_secs(seconds))
            .ok_or_else(|| Error::Invalid("run timeout out of range".into()))?;
        if extend_only && next <= self.deadline {
            return Ok(false);
        }
        self.deadline = next;
        self.retry_at = None;
        Ok(true)
    }
}

impl Controller {
    pub(super) fn reconcile_run_timer(&mut self) {
        if self.retired {
            self.run_timer = None;
            return;
        }
        // A temporary authority outage fences lifecycle work without extending
        // the same execution's timer when authority becomes available again.
        if self.authority_fenced {
            return;
        }
        if self.record.state != EnvironmentState::Running {
            self.run_timer = None;
            return;
        }
        if self.run_timer.as_ref().is_some_and(|timer| {
            timer.matches(
                &self.record.runtime.id,
                self.record.assignment.generation,
                self.timer_sequence,
            )
        }) {
            return;
        }
        let Some(policy) = self.record.spec.run_policy.as_ref() else {
            self.run_timer = None;
            return;
        };
        let seconds = self
            .next_run_timeout
            .take()
            .unwrap_or(policy.timeout_seconds);
        let Some(deadline) = Deadline::now().checked_add(Duration::from_secs(seconds)) else {
            return;
        };
        self.timer_sequence = self.timer_sequence.saturating_add(1);
        self.run_timer = Some(RunTimer {
            runtime_id: self.record.runtime.id.clone(),
            generation: self.record.assignment.generation,
            sequence: self.timer_sequence,
            deadline,
            retry_at: None,
            started_at_unix_ms: adx_core::observation::now_millis().unwrap_or_default(),
        });
    }

    pub(super) fn run_deadline(&self) -> Option<Deadline> {
        if self.retired || self.authority_fenced {
            return None;
        }
        self.run_timer
            .as_ref()
            .map(|timer| timer.retry_at.unwrap_or(timer.deadline))
    }

    pub(super) fn run_control(&self) -> Result<EnvironmentControlRecord> {
        self.record
            .spec
            .run_policy
            .as_ref()
            .ok_or(Error::NotFound)?;
        let now_ms = adx_core::observation::now_millis()?;
        let expires_at_unix_ms = self.run_timer.as_ref().map_or(Ok(0), |timer| {
            let remaining = u64::try_from(
                timer
                    .deadline
                    .saturating_duration_since(Deadline::now())
                    .as_millis(),
            )
            .map_err(|_| Error::Invalid("run timeout out of range".into()))?;
            now_ms
                .checked_add(remaining)
                .ok_or_else(|| Error::Invalid("run timeout out of range".into()))
        })?;
        Ok(EnvironmentControlRecord {
            environment_id: self.record.spec.id.clone(),
            generation: self.record.assignment.generation,
            revision: self.timer_sequence,
            expires_at_unix_ms,
            started_at_unix_ms: self
                .run_timer
                .as_ref()
                .map_or(0, |timer| timer.started_at_unix_ms),
        })
    }

    pub(super) fn update_run_timeout(
        &mut self,
        seconds: u64,
        extend_only: bool,
    ) -> Result<EnvironmentControlRecord> {
        self.record
            .spec
            .run_policy
            .as_ref()
            .ok_or(Error::NotFound)?;
        seconds
            .checked_mul(1000)
            .ok_or_else(|| Error::Invalid("run timeout out of range".into()))?;
        Deadline::now()
            .checked_add(Duration::from_secs(seconds))
            .ok_or_else(|| Error::Invalid("run timeout out of range".into()))?;
        if self.record.state == EnvironmentState::Paused {
            self.next_run_timeout = Some(seconds);
            return self.run_control();
        }
        if self.record.state != EnvironmentState::Running {
            return Err(Error::Conflict);
        }
        let timer = self.run_timer.as_mut().ok_or(Error::Conflict)?;
        if !timer.matches(
            &self.record.runtime.id,
            self.record.assignment.generation,
            self.timer_sequence,
        ) {
            return Err(Error::Conflict);
        }
        let now = Deadline::now();
        if timer.deadline <= now {
            return Err(Error::Conflict);
        }
        if timer.renew(now, seconds, extend_only)? {
            self.timer_sequence = self.timer_sequence.checked_add(1).ok_or(Error::Conflict)?;
            timer.sequence = self.timer_sequence;
        }
        self.run_control()
    }

    pub(super) async fn run_timeout_due(&mut self) -> Result<()> {
        let Some(timer) = self.run_timer.as_ref() else {
            return Ok(());
        };
        if self.retired
            || self.authority_fenced
            || self.record.state != EnvironmentState::Running
            || !timer.matches(
                &self.record.runtime.id,
                self.record.assignment.generation,
                self.timer_sequence,
            )
            || timer.deadline > Deadline::now()
        {
            return Ok(());
        }
        let sequence = timer.sequence;
        let action = self
            .record
            .spec
            .run_policy
            .as_ref()
            .ok_or(Error::Conflict)?
            .action;
        self.operation_intent(adx_core::observation::OperationIntent::Policy);
        let result = match action {
            RunExpiryAction::Delete => self.delete(false).await,
            RunExpiryAction::Pause => {
                let policy = self.record.spec.lifecycle.pause.clone().unwrap_or_default();
                let prefix = format!(
                    "run-timeout-{}-{sequence}-",
                    self.record.assignment.generation
                );
                let (operation_id, expected_revision) = self
                    .capture_preparation
                    .as_ref()
                    .filter(|(operation, _)| {
                        operation.kind == LifecycleKind::Pause && operation.id.starts_with(&prefix)
                    })
                    .map(|(operation, _)| (operation.id.clone(), operation.expected_revision))
                    .unwrap_or_else(|| {
                        (
                            format!("{prefix}{}", uuid::Uuid::new_v4()),
                            self.record.revision,
                        )
                    });
                self.pause(PauseRequest {
                    operation_id,
                    expected_revision,
                    retention: policy.retention,
                    timeout_seconds: policy.timeout_seconds,
                })
                .await
            }
        };
        if result.is_err() {
            // Automatic retries have no caller retaining the original request.
            // Release only our own definitely-aborted pre-capture attempt; normal
            // Pause requests retain their existing replay contract.
            if self.record.state == EnvironmentState::Running {
                let prefix = format!(
                    "run-timeout-{}-{sequence}-",
                    self.record.assignment.generation
                );
                let prepared = self
                    .capture_preparation
                    .as_ref()
                    .filter(|(operation, _)| {
                        operation.kind == LifecycleKind::Pause && operation.id.starts_with(&prefix)
                    })
                    .cloned();
                if let (Some((operation, path)), Some(checkpoint)) =
                    (prepared, self.services.checkpoint.clone())
                {
                    let cleanup = async {
                        checkpoint
                            .cooperation
                            .abort_unstarted(&self.record, &operation.id)
                            .await?;
                        let references = checkpoint.store.capture_references(&path)?;
                        checkpoint.store.discard_staged(&path).await?;
                        self.record.execution_artifacts.retain(|artifact| {
                            !references
                                .iter()
                                .any(|reference| reference.aliases(artifact))
                        });
                        self.capture_preparation = None;
                        self.record.revision =
                            self.record.revision.checked_add(1).ok_or(Error::Conflict)?;
                        self.durability = None;
                        self.sync().await.map(|_| ())
                    }
                    .await;
                    self.trace_observation_error("run.timeout.abort", &cleanup);
                }
            }
            if let Some(timer) = self.run_timer.as_mut() {
                timer.retry_at = Deadline::now().checked_add(Duration::from_secs(1));
            }
        }
        self.reconcile_run_timer();
        result.map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn timer(now: Deadline) -> RunTimer {
        RunTimer {
            runtime_id: "runtime-a".into(),
            generation: 7,
            sequence: 3,
            deadline: now + Duration::from_secs(60),
            retry_at: None,
            started_at_unix_ms: 1000,
        }
    }

    #[test]
    fn connect_only_extends_but_set_timeout_replaces_and_zero_is_due() {
        let now = Deadline::now();
        let mut timer = timer(now);
        assert!(!timer.renew(now, 10, true).unwrap());
        assert_eq!(timer.deadline, now + Duration::from_secs(60));
        assert!(timer.renew(now, 120, true).unwrap());
        assert_eq!(timer.deadline, now + Duration::from_secs(120));
        assert!(timer.renew(now, 5, false).unwrap());
        assert_eq!(timer.deadline, now + Duration::from_secs(5));
        assert!(timer.renew(now, 0, false).unwrap());
        assert_eq!(timer.deadline, now);
    }

    #[test]
    fn stale_execution_generation_or_timer_sequence_cannot_match() {
        let timer = timer(Deadline::now());
        assert!(timer.matches("runtime-a", 7, 3));
        assert!(!timer.matches("runtime-b", 7, 3));
        assert!(!timer.matches("runtime-a", 8, 3));
        assert!(!timer.matches("runtime-a", 7, 2));
    }
}
