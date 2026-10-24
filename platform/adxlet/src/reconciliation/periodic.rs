//! Live comparisons use independent controller queues, never the node write barrier.
use super::*;
use crate::{
    controller::{AuthorityCheck, AuthorityResult},
    EnvironmentHandle, LocalReservation,
};
use std::sync::atomic::Ordering;

#[cfg(test)]
#[path = "periodic_tests.rs"]
mod tests;

pub(crate) struct ComparisonBasis {
    controllers: crate::EnvironmentDirectory,
    holds: Vec<(String, LocalReservation)>,
}

pub(crate) enum ReconciliationOutcome {
    Complete,
    Pending,
    StaleCatalog,
    Incomplete(Error),
}

impl Adxlet {
    fn start_comparison(
        &self,
        id: String,
        work: impl std::future::Future<Output = Result<AuthorityResult>> + Send + 'static,
    ) {
        let mut tasks = self
            .authority_tasks
            .lock()
            .expect("shared state lock poisoned");
        tasks.entry(id).or_insert_with(|| {
            let progress = self.authority_progress.clone();
            let (send, receive) = tokio::sync::oneshot::channel();
            tokio::spawn(async move {
                let result = work.await;
                let _ = send.send(result);
                progress.notify_one();
            });
            receive
        });
    }

    fn completed_comparisons(&self) -> (Option<Error>, bool) {
        let mut failure = None;
        let mut stale = false;
        self.authority_tasks.lock().expect("shared state lock poisoned").retain(|id, task| {
            let result = match task.try_recv() {
                Err(tokio::sync::oneshot::error::TryRecvError::Empty) => return true,
                Err(tokio::sync::oneshot::error::TryRecvError::Closed) => Err(Error::Unavailable("environment reconciliation task stopped".into())),
                Ok(result) => result,
            };
            match result {
                Ok(AuthorityResult::Stale) => stale = true,
                Ok(_) => (),
                Err(error) => {
                    adx_observability::warn!(environment_id=%id, %error, "environment authority reconciliation incomplete");
                    failure = Some(error);
                },
            }
            false
        });
        (failure, stale)
    }

    /// Capture before InspectNode. A controller admitted after this point cannot
    /// be discarded merely because it is missing from that earlier catalog.
    pub(crate) fn comparison_basis(&self) -> ComparisonBasis {
        let controllers = self
            .environments
            .lock()
            .expect("shared state lock poisoned")
            .clone();
        let holds = self
            .local_holds
            .lock()
            .expect("shared state lock poisoned")
            .iter()
            .map(|(id, hold)| {
                (
                    id.clone(),
                    LocalReservation {
                        token: Some(hold.token.clone()),
                        devices: hold.devices.clone(),
                    },
                )
            })
            .collect();
        ComparisonBasis { controllers, holds }
    }

    pub(crate) async fn compare_retained(
        &self,
        basis: ComparisonBasis,
        records: Vec<EnvironmentRecord>,
        snapshots: Vec<Snapshot>,
        retained: Vec<adx_core::CheckpointArtifact>,
        wait: std::time::Duration,
    ) -> Result<ReconciliationOutcome> {
        let catalog = Catalog::new(&self.node_id, records, snapshots, Some(retained))?;

        if self.is_draining() {
            return Err(Error::Unavailable("node is draining".into()));
        }
        // Reopen after a previous session/transport fence only with fresh authority.
        // A normal live comparison never requests an exclusive node lock.
        if !*self.lifecycle_ready.read().await {
            *self.lifecycle_ready.write().await = true;
        }
        let _gate = Arc::new(self.lifecycle_ready.clone().read_owned().await);
        let mut work = BTreeMap::new();
        let mut stale = false;
        let (mut failure, previously_stale) = self.completed_comparisons();
        stale |= previously_stale;
        for (id, (_, assignment, handle)) in basis.controllers {
            let current = self
                .environments
                .lock()
                .expect("shared state lock poisoned")
                .get(&id)
                .is_some_and(|(_, _, current)| current.same_controller(&handle));
            if current {
                work.insert(id, (assignment, handle));
            }
        }
        for record in catalog.records.values() {
            if work.contains_key(&record.spec.id) {
                continue;
            }
            if self
                .retired_environment(&record.spec.id)
                .is_some_and(|retired| {
                    retired.generation > record.assignment.generation
                        || (retired.generation == record.assignment.generation
                            && retired.result.as_ref().is_none_or(|result| {
                                result.durability == crate::Durability::Published
                                    && result.record.revision > record.revision
                            }))
                })
            {
                stale = true;
                continue;
            }
            match self.adopt_comparison_record(record) {
                Ok(Some(handle)) => {
                    work.insert(record.spec.id.clone(), (record.assignment.clone(), handle));
                }
                Ok(None) => (),
                Err(error) => failure = Some(error),
            }
        }
        for (id, reservation) in basis.holds {
            if !catalog.records.contains_key(&id) {
                self.release_local(&id, &reservation)?;
            }
        }
        // Inspection still uses the Coordinator's closed-admission snapshot
        // contract. Slow per-environment work starts only after admission reopens.
        self.authority_reconciling.store(false, Ordering::Release);
        for (id, (assignment, handle)) in work {
            let record = catalog.records.get(&id).cloned();
            let references = catalog.references.clone();
            let manager = self.clone();
            let gate = _gate.clone();
            self.start_comparison(id.clone(), async move {
                let _gate = gate;
                let result = handle
                    .check_authority(AuthorityCheck {
                        record: record.clone(),
                        references: references.clone(),
                    })
                    .await?;
                if result != AuthorityResult::Retired {
                    return Ok(result);
                }
                manager.remember_retired(&assignment, None);
                {
                    let mut controllers = manager
                        .environments
                        .lock()
                        .expect("shared state lock poisoned");
                    if controllers
                        .get(&id)
                        .is_some_and(|(_, _, current)| current.same_controller(&handle))
                    {
                        controllers.remove(&id);
                    }
                }
                if let Some(record) = record {
                    if let Some(handle) = manager.adopt_comparison_record(&record)? {
                        // Re-observe in the adopted controller; the old owner's
                        // inventory cannot describe its replacement execution.
                        return handle
                            .check_authority(AuthorityCheck {
                                record: Some(record),
                                references,
                            })
                            .await;
                    }
                }
                Ok(AuthorityResult::Current)
            });
        }
        // A long restore must not stall the next catalog/resource sample. Keep
        // one tracked task per environment; accepted work is never cancelled.
        let deadline = tokio::time::sleep(wait);
        tokio::pin!(deadline);
        loop {
            let (error, was_stale) = self.completed_comparisons();
            if error.is_some() {
                failure = error;
            }
            stale |= was_stale;
            if self
                .authority_tasks
                .lock()
                .expect("shared state lock poisoned")
                .is_empty()
            {
                break;
            }
            tokio::select! {
                _ = &mut deadline => break,
                _ = self.authority_progress.notified() => (),
            }
        }
        // Broad directory GC requires quiescence and remains a startup operation.
        // Live controllers release their own artifacts; pinned restore inputs are
        // never collected using a catalog captured before a concurrent checkpoint.
        self.services.routes.ensure_synced().await?;
        Ok(if let Some(error) = failure {
            ReconciliationOutcome::Incomplete(error)
        } else if stale {
            ReconciliationOutcome::StaleCatalog
        } else if !self
            .authority_tasks
            .lock()
            .expect("shared state lock poisoned")
            .is_empty()
        {
            ReconciliationOutcome::Pending
        } else {
            ReconciliationOutcome::Complete
        })
    }

    fn adopt_comparison_record(
        &self,
        record: &EnvironmentRecord,
    ) -> Result<Option<EnvironmentHandle>> {
        let retired = self
            .retired_environments
            .lock()
            .expect("shared state lock poisoned");
        let mut controllers = self
            .environments
            .lock()
            .expect("shared state lock poisoned");
        if controllers.contains_key(&record.spec.id) {
            // This controller arrived after the basis was captured. Its own next
            // comparison will decide ownership, including accepted creates.
            return Ok(None);
        }
        if retired.get(&record.spec.id).is_some_and(|retired| {
            retired.expires_at > tokio::time::Instant::now()
                && (retired.generation > record.assignment.generation
                    || (retired.generation == record.assignment.generation
                        && retired
                            .result
                            .as_ref()
                            .is_none_or(|result| result.record.revision > record.revision)))
        }) {
            return Err(Error::Conflict);
        }
        self.adopt_local(&record.spec, &record.assignment)?;
        if record.resources_held {
            let mut admission = self
                .services
                .admission
                .lock()
                .expect("shared state lock poisoned");
            let mut ledger = admission.ledger.clone();
            let mut devices = admission.devices.clone();
            ledger.restore(&record.runtime.id, record.spec.resources)?;
            devices.restore(&record.runtime.id, &record.assignment.devices)?;
            admission.ledger = ledger;
            admission.devices = devices;
        }
        let handle = if terminal_without_execution(record) {
            controller::spawn_terminal(record.clone(), self.services.clone())?
        } else {
            controller::spawn_comparison(record.clone(), self.services.clone())
        };
        controllers.insert(
            record.spec.id.clone(),
            (
                record.spec.clone(),
                record.assignment.clone(),
                handle.clone(),
            ),
        );
        Ok(Some(handle))
    }
}
