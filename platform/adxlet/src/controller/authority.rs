//! A periodic comparison owns only this controller's serial queue.
use super::*;
use crate::RuntimeObservation;

pub(crate) struct AuthorityCheck {
    pub record: Option<EnvironmentRecord>,
    pub actual: Vec<RuntimeObservation>,
    pub references: Option<Arc<CheckpointReferences>>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum AuthorityResult {
    Current,
    Stale,
    Retired,
}

impl Controller {
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
            self.remove_unowned(&check.actual, None).await?;
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
            self.remove_unowned(&check.actual, None).await?;
            return Ok(AuthorityResult::Current);
        }
        self.align_authority(record.clone()).await?;
        let current_runtime = self.record.runtime.id.clone();
        self.remove_unowned(&check.actual, Some(&current_runtime))
            .await?;
        let execution_matches = match record.state {
            EnvironmentState::Running => check
                .actual
                .iter()
                .any(|r| r.runtime_id == current_runtime && r.running),
            EnvironmentState::Paused => {
                !check.actual.iter().any(|r| r.runtime_id == current_runtime)
            }
            _ => false,
        };
        if comparison != AuthorityComparison::Published || !execution_matches {
            let result = self.reconcile().await?;
            if result.durability != Durability::Published {
                return Err(Error::Unavailable(
                    "reconciliation state is not published".into(),
                ));
            }
        }
        Ok(AuthorityResult::Current)
    }

    async fn remove_unowned(
        &self,
        actual: &[RuntimeObservation],
        keep: Option<&str>,
    ) -> Result<()> {
        for runtime in actual
            .iter()
            .filter(|r| Some(r.runtime_id.as_str()) != keep)
        {
            timeout(self.services.operation_timeout, async {
                if runtime.generation != self.record.assignment.generation {
                    self.services.routes.retire_orphan(runtime).await?;
                }
                self.services.runtime.remove(&runtime.runtime_id).await
            })
            .await
            .map_err(|_| Error::Unavailable("orphan cleanup timed out".into()))??;
        }
        Ok(())
    }
}

fn terminal(record: &EnvironmentRecord) -> bool {
    matches!(
        record.state,
        EnvironmentState::Failed | EnvironmentState::Deleted
    ) && !record.resources_held
        && !record.restart_pending
}
