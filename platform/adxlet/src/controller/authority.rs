//! A periodic comparison owns only this controller's serial queue.
use super::*;

pub(crate) struct AuthorityCheck {
    pub record: Option<EnvironmentRecord>,
    pub references: Option<Arc<CheckpointReferences>>,
    pub recover: bool,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum AuthorityResult {
    Current,
    Stale,
    Retired,
}

impl Controller {
    pub(super) async fn reap_orphan(&mut self, runtime: crate::RuntimeObservation) -> Result<()> {
        // Execute after accepted lifecycle work. The published view may still
        // describe the previous runtime while Resume is starting its successor.
        if runtime.environment_id != self.record.spec.id
            || runtime.generation > self.record.assignment.generation
            || protects_execution(&self.record, &runtime.runtime_id)
        {
            return Ok(());
        }
        tokio::time::timeout(self.services.operation_timeout, async {
            if runtime.generation != self.record.assignment.generation {
                self.services.routes.retire_orphan(&runtime).await?;
            }
            self.services.runtime.remove(&runtime.runtime_id).await
        })
        .await
        .map_err(|_| Error::Unavailable("orphan cleanup timed out".into()))??;
        if runtime.runtime_id == self.record.runtime.id {
            self.release_capacity()?;
        } else {
            let mut admission = self
                .services
                .admission
                .lock()
                .expect("shared state lock poisoned");
            if admission.ledger.contains(&runtime.runtime_id) {
                admission.release(&runtime.runtime_id)?;
            }
        }
        Ok(())
    }

    fn matches_unstarted(&self, record: &EnvironmentRecord) -> bool {
        // A local reservation may already hold resources; a centrally assigned
        // controller acquires them when Create runs. Neither has started yet.
        // CheckAuthority temporarily fences both before making this comparison.
        !self.retired
            && unstarted(&self.record)
            && unstarted(record)
            && self.record.spec == record.spec
            && self.record.assignment == record.assignment
            && self.record.runtime == record.runtime
    }

    pub(super) fn authority_trace(
        &self,
        check: &AuthorityCheck,
    ) -> Option<adx_observability::trace::Trace> {
        // Classify only telemetry here. Every check still executes the same
        // ownership, publication and cleanup logic below in the serial queue.
        if !self.authority_fenced
            && !self.retired
            && check.record.as_ref().is_some_and(|record| {
                self.matches_unstarted(record)
                    || (self.durability == Some(Durability::Published) && self.record == *record)
            })
        {
            return None;
        }
        let trace = adx_observability::trace::Trace::child("environment.authority_reconcile");
        trace.attribute("environment.id", self.record.spec.id.clone());
        Some(trace)
    }

    fn compare_authority(&self, record: &EnvironmentRecord) -> Result<AuthorityComparison> {
        if self.matches_unstarted(record) {
            return Ok(AuthorityComparison::Unstarted);
        }
        if self.durability != Some(Durability::Published) {
            return Ok(AuthorityComparison::Unpublished);
        }
        if self.record == *record {
            Ok(AuthorityComparison::Published)
        } else if self.record.revision > record.revision {
            Ok(AuthorityComparison::StaleCatalog)
        } else if record.revision > self.record.revision && settled(record) {
            Ok(AuthorityComparison::RetiredByAuthority)
        } else {
            Err(Error::Conflict)
        }
    }

    pub(super) async fn check_authority(
        &mut self,
        check: AuthorityCheck,
    ) -> Result<AuthorityResult> {
        let Some(record) = check.record else {
            self.discard_execution(check.references.as_deref()).await?;
            return Ok(AuthorityResult::Retired);
        };
        if record.assignment.generation < self.record.assignment.generation {
            return Ok(AuthorityResult::Stale);
        }
        if record.assignment != self.record.assignment {
            if record.assignment.generation == self.record.assignment.generation {
                return Err(Error::Conflict);
            }
            self.discard_execution(check.references.as_deref()).await?;
            return Ok(AuthorityResult::Retired);
        }
        if !record.spec.matches_immutable_fields(&self.record.spec) {
            return Err(Error::Conflict);
        }
        // A completed local stop may still have a late physical execution.
        // Confirm its cleanup before replaying the recorded release evidence.
        if self.held
            && !self.record.resources_held
            && self
                .record
                .execution_artifacts
                .iter()
                .all(|a| a.size_bytes != 0)
        {
            self.cleanup().await?;
        }
        if check.recover
            && self.record.resources_held
            && self
                .record
                .execution_artifacts
                .iter()
                .all(|artifact| artifact.size_bytes != 0)
            && !terminal(&record)
            && !(settled(&record) && record.revision > self.record.revision)
        {
            if let Err(error) = self.retain_inputs().await {
                // Missing inputs are irrelevant once backend absence is proven.
                // An unavailable probe cannot justify releasing a live input.
                if self
                    .services
                    .runtime
                    .observe(&self.record.runtime.id)
                    .await?
                    .running
                {
                    return Err(error);
                }
            }
        }
        // A live controller owns its pending journal results. Replay them in
        // this queue before comparing revisions, without a node-wide journal
        // writer waiting behind another environment's in-flight commit.
        if self.durability == Some(Durability::Journaled)
            && !(settled(&record)
                && (record.state != EnvironmentState::Paused
                    || record.revision > self.record.revision))
            && self.sync().await?.durability != Durability::Published
        {
            return Err(Error::Unavailable("environment publication pending".into()));
        }
        let comparison = self.compare_authority(&record)?;
        if comparison == AuthorityComparison::Unstarted && !check.recover {
            return Ok(AuthorityResult::Current);
        }
        if comparison == AuthorityComparison::StaleCatalog {
            if check.recover && !terminal(&self.record) {
                self.reconcile().await?;
            }
            return Ok(AuthorityResult::Stale);
        }
        // A published update may have changed mutable spec fields after
        // InspectNode read this catalog. Compare revisions before rejecting
        // that old spec, while still fencing unpublished or current conflicts.
        if record.spec != self.record.spec {
            return Err(Error::Conflict);
        }
        if terminal(&record) {
            if comparison != AuthorityComparison::Published || self.retired {
                self.discard_execution(check.references.as_deref()).await?;
                self.record = record;
                self.held = false;
                self.retired = false;
                self.durability = Some(Durability::Published);
            }
            if check.recover && !self.record.execution_artifacts.is_empty() {
                self.reconcile().await?;
            }
            // Authority supersedes the old outcome, not the stop proof just
            // obtained by cleaning a late execution with an exclusive volume.
            if !self.stopped_runtime_ids.is_empty()
                && self.sync().await?.durability != Durability::Published
            {
                return Err(Error::Unavailable("environment publication pending".into()));
            }
            return Ok(AuthorityResult::Current);
        }
        self.align_authority(record.clone()).await?;
        if comparison != AuthorityComparison::Published || check.recover {
            let result = self.reconcile().await?;
            if result.durability != Durability::Published {
                return Err(Error::Unavailable(
                    "reconciliation state is not published".into(),
                ));
            }
        }
        Ok(AuthorityResult::Current)
    }
}

fn settled(record: &EnvironmentRecord) -> bool {
    matches!(
        record.state,
        EnvironmentState::Paused | EnvironmentState::Failed | EnvironmentState::Deleted
    ) && !record.resources_held
        && !record.restart_pending
}

fn terminal(record: &EnvironmentRecord) -> bool {
    record.state != EnvironmentState::Paused && settled(record)
}
