//! A periodic comparison owns only this controller's serial queue.
use super::*;

pub(crate) struct AuthorityCheck {
    pub record: Option<EnvironmentRecord>,
    pub references: Option<Arc<CheckpointReferences>>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum AuthorityResult {
    Current,
    Stale,
    Retired,
}

impl Controller {
    pub(super) fn authority_trace(
        &self,
        check: &AuthorityCheck,
    ) -> Option<adx_observability::trace::Trace> {
        // Classify only telemetry here. Every check still executes the same
        // ownership, publication and cleanup logic below in the serial queue.
        if !self.authority_fenced
            && !self.retired
            && self.durability == Some(Durability::Published)
            && check.record.as_ref() == Some(&self.record)
        {
            return None;
        }
        let trace = adx_observability::trace::Trace::child("environment.authority_reconcile");
        trace.attribute("environment.id", self.record.spec.id.clone());
        Some(trace)
    }

    fn compare_authority(&self, record: &EnvironmentRecord) -> Result<AuthorityComparison> {
        if self.durability != Some(Durability::Published) {
            return Ok(AuthorityComparison::Unpublished);
        }
        if self.record == *record {
            Ok(AuthorityComparison::Published)
        } else if self.record.revision > record.revision {
            Ok(AuthorityComparison::StaleCatalog)
        } else if record.revision > self.record.revision && terminal(record) {
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
        // A live controller owns its pending journal results. Replay them in
        // this queue before comparing revisions, without a node-wide journal
        // writer waiting behind another environment's in-flight commit.
        if self.durability == Some(Durability::Journaled)
            && !terminal(&record)
            && self.sync().await?.durability != Durability::Published
        {
            return Err(Error::Unavailable("environment publication pending".into()));
        }
        let comparison = self.compare_authority(&record)?;
        if comparison == AuthorityComparison::StaleCatalog {
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
            return Ok(AuthorityResult::Current);
        }
        self.align_authority(record.clone()).await?;
        if comparison != AuthorityComparison::Published {
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

fn terminal(record: &EnvironmentRecord) -> bool {
    matches!(
        record.state,
        EnvironmentState::Failed | EnvironmentState::Deleted
    ) && !record.resources_held
        && !record.restart_pending
}
