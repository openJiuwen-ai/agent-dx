//! Live comparisons use independent controller queues, never the node write barrier.
use super::*;
use crate::{
    controller::{AuthorityCheck, AuthorityResult},
    EnvironmentHandle,
};
use std::sync::atomic::Ordering;

#[cfg(test)]
#[path = "periodic_tests.rs"]
mod tests;

pub(crate) struct ComparisonBasis {
    controllers: crate::EnvironmentDirectory,
    inventory: Result<Vec<crate::RuntimeObservation>>,
}

enum OrphanTarget {
    Controller(EnvironmentHandle),
    AwaitingController,
    Unowned,
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
        runtime_id: Option<String>,
        work: impl std::future::Future<Output = Result<AuthorityResult>> + Send + 'static,
    ) {
        let mut tasks = self
            .authority_tasks
            .lock()
            .expect("shared state lock poisoned");
        tasks.entry((id, runtime_id)).or_insert_with(|| {
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
        self.authority_tasks.lock().expect("shared state lock poisoned").retain(|(id, _), task| {
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
    pub(crate) async fn comparison_basis(&self, budget: std::time::Duration) -> ComparisonBasis {
        let controllers = self
            .environments
            .lock()
            .expect("shared state lock poisoned")
            .clone();
        // Sample before authority: anything created after this inventory is not
        // eligible for orphan collection from the following catalog snapshot.
        let inventory = tokio::time::timeout(
            budget.min(self.services.operation_timeout),
            self.services.runtime.inventory(),
        )
        .await
        .map_err(|_| Error::Unavailable("inventory timed out".into()))
        .and_then(|result| result);
        ComparisonBasis {
            controllers,
            inventory,
        }
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
        // Missing catalog entries cannot resolve an in-flight Claim RPC. Its
        // explicit outcome/retry owns the local reservation, not this snapshot.
        // Clear a prior transport fence only after validating fresh authority.
        self.authority_reconciling.store(false, Ordering::Release);
        for (id, (assignment, handle)) in work {
            let record = catalog.records.get(&id).cloned();
            let references = catalog.references.clone();
            let manager = self.clone();
            let gate = _gate.clone();
            self.start_comparison(id.clone(), None, async move {
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
        match basis.inventory {
            Ok(actual) => {
                validate_inventory(&actual, &catalog.records)?;
                for runtime in actual {
                    // Controllers own same-generation lifecycle decisions. An
                    // older observation must not delete a resumed execution or
                    // a create admitted while InspectNode was in flight.
                    if catalog
                        .records
                        .get(&runtime.environment_id)
                        .is_some_and(|record| {
                            record.assignment.generation < runtime.generation
                                || controller::protects_execution(record, &runtime.runtime_id)
                        })
                    {
                        continue;
                    }
                    let retire_route = catalog
                        .records
                        .get(&runtime.environment_id)
                        .is_none_or(|record| record.assignment.generation != runtime.generation);
                    let manager = self.clone();
                    let gate = _gate.clone();
                    self.start_comparison(
                        runtime.environment_id.clone(),
                        Some(runtime.runtime_id.clone()),
                        async move {
                            let _gate = gate;
                            match manager.claim_orphan(&runtime, !retire_route) {
                                OrphanTarget::Controller(handle) => {
                                    handle.reap_orphan(runtime).await?;
                                    return Ok(AuthorityResult::Current);
                                }
                                OrphanTarget::AwaitingController => {
                                    return Ok(AuthorityResult::Current);
                                }
                                OrphanTarget::Unowned => (),
                            }
                            tokio::time::timeout(manager.services.operation_timeout, async {
                                if retire_route {
                                    manager.services.routes.retire_orphan(&runtime).await?;
                                }
                                manager.services.runtime.remove(&runtime.runtime_id).await
                            })
                            .await
                            .map_err(|_| Error::Unavailable("orphan cleanup timed out".into()))??;
                            Ok(AuthorityResult::Current)
                        },
                    );
                }
            }
            Err(error) => failure = Some(error),
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

    fn claim_orphan(
        &self,
        runtime: &crate::RuntimeObservation,
        same_generation_owner: bool,
    ) -> OrphanTarget {
        // Same lock order as environment(): protect newly admitted work and
        // retire the observed generation atomically before asynchronous cleanup.
        let mut retired = self
            .retired_environments
            .lock()
            .expect("shared state lock poisoned");
        let controllers = self
            .environments
            .lock()
            .expect("shared state lock poisoned");
        if let Some((_, _, handle)) = controllers.get(&runtime.environment_id) {
            return OrphanTarget::Controller(handle.clone());
        }
        // Authority still owns this generation. Its Create may be in flight,
        // or its controller may be between retirement and adoption. Let the
        // next inventory retry through that controller rather than installing
        // a generation tombstone that would prevent the owner from arriving.
        if same_generation_owner {
            return OrphanTarget::AwaitingController;
        }
        let now = tokio::time::Instant::now();
        let expires_at = now
            .checked_add(self.services.operation_timeout.saturating_mul(2))
            .unwrap_or(now);
        retired
            .entry(runtime.environment_id.clone())
            .and_modify(|entry| {
                if runtime.generation > entry.generation {
                    entry.generation = runtime.generation;
                    entry.result = None;
                    entry.coordinator_delete_fence = None;
                }
                entry.expires_at = entry.expires_at.max(expires_at);
            })
            .or_insert(crate::RetiredEnvironment {
                generation: runtime.generation,
                expires_at,
                result: None,
                coordinator_delete_fence: None,
            });
        OrphanTarget::Unowned
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
        // Coordinator has assigned ownership, but its create RPC may still be
        // in flight. A live catalog must not recover/retire that fresh claim.
        if controller::unstarted(record) {
            return Ok(None);
        }
        if terminal_without_execution(record) {
            self.services.upgrade.record_cleanup()?;
        } else {
            self.services.upgrade.record_action(
                &record.spec.id,
                record.assignment.generation,
                "adopt",
            )?;
        }
        if record.resources_held {
            // An unheld record leaves the tentative reservation with its claim.
            self.adopt_local(&record.spec, &record.assignment)?;
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
