use super::*;
use adx_core::observation::{
    bounded_detail, now_millis, CreateStage, EvidenceSource, InterruptionEvidence,
    InterruptionReason, OperationIntent, Outcome,
};
use std::time::{Duration, Instant};

/// Process-local monotonic timing for the current create/restore stage.
///
/// It never crosses an `await`: the actor records a stage when the state machine
/// moves into it and closes it at the next transition or at the terminal result.
/// `take()` semantics guarantee exactly one sample per stage.
#[derive(Default)]
pub(super) struct StageTiming {
    current: Option<(CreateStage, Instant)>,
}

impl StageTiming {
    /// Drop any stage left open by an earlier operation path.
    pub(super) fn reset(&mut self) {
        self.current = None;
    }
    /// Start `next` and close the previous stage with its elapsed duration.
    /// Returns `None` for the first stage of an operation.
    pub(super) fn transition(&mut self, next: CreateStage) -> Option<(CreateStage, Duration)> {
        let now = Instant::now();
        let previous = self.current.replace((next, now))?;
        Some((previous.0, now.saturating_duration_since(previous.1)))
    }
    /// Close the current stage, if any. A second call returns `None`.
    pub(super) fn finish(&mut self) -> Option<(CreateStage, Duration)> {
        let (stage, started) = self.current.take()?;
        Some((stage, started.elapsed()))
    }
}

impl Controller {
    pub(super) fn intended_execution_absence(&self) -> bool {
        self.record
            .observation
            .as_ref()
            .is_some_and(|o| o.intent.is_some())
            && self.record.failure.as_ref().is_none_or(|failure| {
                // A generic exit can be the delayed result of our own stop.
                // Only independent fault evidence overrides the persisted intent.
                matches!(
                    failure.reason,
                    adx_core::RuntimeFailureReason::Missing
                        | adx_core::RuntimeFailureReason::Exited
                )
            })
    }

    pub(super) fn mark_interruption(&mut self, source: EvidenceSource) -> Result<()> {
        if self.record.state != EnvironmentState::Running {
            return Ok(());
        }
        let observation = self.record.observation.get_or_insert_with(Default::default);
        if observation.interruption.as_ref().is_some_and(|old| {
            old.runtime_id == self.record.runtime.id
                && old.assignment_generation == self.record.assignment.generation
        }) {
            return Ok(());
        }
        let reason = self.record.failure.as_ref().map_or_else(
            || {
                if source == EvidenceSource::Control {
                    InterruptionReason::ControlUnhealthy
                } else {
                    InterruptionReason::Unknown
                }
            },
            |failure| match failure.reason {
                adx_core::RuntimeFailureReason::OomKilled => InterruptionReason::Oom,
                adx_core::RuntimeFailureReason::Exited => InterruptionReason::RuntimeExit,
                adx_core::RuntimeFailureReason::Missing => InterruptionReason::ExecutionMissing,
                adx_core::RuntimeFailureReason::ProbeFailed => InterruptionReason::ProbeFailed,
                _ => InterruptionReason::Unknown,
            },
        );
        observation.intent = None;
        observation.interruption = Some(InterruptionEvidence {
            event_id: format!(
                "execution:{}:{}",
                self.record.assignment.generation, self.record.runtime.id
            ),
            detected_at_unix_millis: now_millis()?,
            assignment_generation: self.record.assignment.generation,
            runtime_id: self.record.runtime.id.clone(),
            worker_id: self.record.assignment.node_id.clone(),
            reason,
            evidence_source: source,
            exit_code: self.record.failure.as_ref().and_then(|f| f.exit_code),
            detail: bounded_detail(
                self.record
                    .failure
                    .as_ref()
                    .map_or("execution unavailable", |f| f.message.as_str()),
            ),
        });
        Ok(())
    }
    pub(super) fn create_stage(&mut self, stage: CreateStage) {
        self.record
            .observation
            .get_or_insert_with(Default::default)
            .create_stage = Some(stage);
        if let Some((previous, elapsed)) = self.stage_timing.transition(stage) {
            // Reaching the next transition is proof the previous stage succeeded.
            self.observe_phase(previous, Outcome::Success, elapsed);
        }
    }
    /// Close the final stage with the operation's actual result. Called before
    /// fallible cleanup so the failing stage cannot be lost.
    pub(super) fn finish_stage(&mut self, outcome: Outcome) {
        if let Some((stage, elapsed)) = self.stage_timing.finish() {
            self.observe_phase(stage, outcome, elapsed);
        }
    }
    fn observe_phase(&self, stage: CreateStage, outcome: Outcome, elapsed: Duration) {
        self.services.metrics.observe_phase(
            stage,
            outcome,
            &self.record.spec.runtime_class,
            elapsed,
        );
    }
    pub(super) fn operation_intent(&mut self, intent: OperationIntent) {
        self.record
            .observation
            .get_or_insert_with(Default::default)
            .intent = Some(intent);
    }
    pub(super) async fn clear_operation_intent(&mut self) -> Result<()> {
        if self.record.state == EnvironmentState::Running
            && !self
                .cleanup_pending
                .load(std::sync::atomic::Ordering::Acquire)
            && self
                .record
                .observation
                .as_ref()
                .is_some_and(|o| o.intent.is_some())
        {
            self.record
                .observation
                .as_mut()
                .ok_or(Error::Conflict)?
                .intent = None;
            self.record.revision = self.record.revision.checked_add(1).ok_or(Error::Conflict)?;
            self.durability = None;
            if self.sync().await?.durability != Durability::Published {
                return Err(Error::Unavailable(
                    "operation intent publication unconfirmed".into(),
                ));
            }
        }
        Ok(())
    }
    pub(super) async fn publish_operation_intent(&mut self) -> Result<()> {
        if self.record.state == EnvironmentState::Running {
            self.record.revision = self.record.revision.checked_add(1).ok_or(Error::Conflict)?;
            self.durability = None;
            if self.sync().await?.durability != Durability::Published {
                return Err(Error::Unavailable(
                    "operation intent publication unconfirmed".into(),
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stage_timing_closes_each_stage_exactly_once() {
        let mut timing = StageTiming::default();
        assert!(timing.transition(CreateStage::RuntimePrepare).is_none());
        std::thread::sleep(Duration::from_millis(2));
        let (stage, elapsed) = timing
            .transition(CreateStage::RuntimeStart)
            .expect("the previous stage must close on transition");
        assert_eq!(stage, CreateStage::RuntimePrepare);
        assert!(elapsed >= Duration::from_millis(2));
        assert_eq!(
            timing.finish().map(|(stage, _)| stage),
            Some(CreateStage::RuntimeStart)
        );
        assert!(timing.finish().is_none(), "a stage must not emit twice");
    }

    #[test]
    fn reset_drops_a_stage_left_open_by_an_earlier_path() {
        let mut timing = StageTiming::default();
        assert!(timing.transition(CreateStage::Readiness).is_none());
        timing.reset();
        assert!(timing.finish().is_none());
        // The next operation starts clean rather than closing the stale stage.
        assert!(timing.transition(CreateStage::RuntimePrepare).is_none());
    }
}
