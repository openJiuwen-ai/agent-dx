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
    pub(super) async fn reconcile_controller(
        &self,
        assignment: adx_core::Assignment,
        handle: EnvironmentHandle,
        check: AuthorityCheck,
    ) -> Result<AuthorityResult> {
        let result = handle
            .check_authority(AuthorityCheck {
                record: check.record.clone(),
                references: check.references.clone(),
                recover: check.recover,
            })
            .await?;
        if result != AuthorityResult::Retired {
            return Ok(result);
        }
        self.remember_retired(&assignment, None);
        {
            let mut controllers = self
                .environments
                .lock()
                .expect("shared state lock poisoned");
            if controllers
                .get(&assignment.environment_id)
                .is_some_and(|(_, _, current)| current.same_controller(&handle))
            {
                controllers.remove(&assignment.environment_id);
            }
        }
        if let Some(record) = check.record {
            if let Some(handle) = self.adopt_record(&record, check.recover)? {
                return handle
                    .check_authority(AuthorityCheck {
                        record: Some(record),
                        references: check.references,
                        recover: check.recover,
                    })
                    .await;
            }
        }
        Ok(AuthorityResult::Current)
    }

    pub(super) async fn cleanup_orphan(
        &self,
        runtime: crate::RuntimeObservation,
        retire_route: bool,
    ) -> Result<()> {
        match self.claim_orphan(&runtime, !retire_route) {
            OrphanTarget::Controller(handle) => return handle.reap_orphan(runtime).await,
            OrphanTarget::AwaitingController => return Ok(()),
            OrphanTarget::Unowned => (),
        }
        tokio::time::timeout(self.services.operation_timeout, async {
            if retire_route {
                self.services.routes.retire_orphan(&runtime).await?;
            }
            self.services.runtime.remove(&runtime.runtime_id).await
        })
        .await
        .map_err(|_| Error::Unavailable("orphan cleanup timed out".into()))??;
        let mut admission = self
            .services
            .admission
            .lock()
            .expect("shared state lock poisoned");
        if admission.ledger.contains(&runtime.runtime_id) {
            admission.release(&runtime.runtime_id)?;
        }
        Ok(())
    }

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

    fn completed_comparisons(&self) -> (Option<Error>, bool, bool) {
        // Seal owns completion consumption once its durable fence is visible.
        // Ordinary comparison retries must not erase failed/unknown receipts.
        if self.process_sealing() {
            return (
                Some(Error::Unavailable(
                    "process seal owns authority receipts".into(),
                )),
                false,
                false,
            );
        }
        let mut failure = None;
        let mut stale = false;
        let mut orphan_failed = false;
        self.authority_tasks.lock().expect("shared state lock poisoned").retain(|(id, runtime), task| {
            let result = match task.try_recv() {
                Err(tokio::sync::oneshot::error::TryRecvError::Empty) => return true,
                Err(tokio::sync::oneshot::error::TryRecvError::Closed) => Err(Error::Unavailable("environment reconciliation task stopped".into())),
                Ok(result) => result,
            };
            match result {
                Ok(AuthorityResult::Stale) => stale = true,
                Ok(_) => (),
                Err(error) => {
                    orphan_failed |= runtime.as_ref().is_some_and(|id| !self.execution_reserved(id));
                    adx_observability::warn!(environment_id=%id, %error, "environment authority reconciliation incomplete");
                    failure = Some(error);
                },
            }
            false
        });
        (failure, stale, orphan_failed)
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
        let _recovery = self.recovery.lock().await;
        let catalog = match Catalog::new(&self.node_id, records, snapshots, Some(retained)) {
            Ok(catalog) => catalog,
            Err(error) => {
                self.authority_reconciling.store(true, Ordering::Release);
                return Ok(ReconciliationOutcome::Incomplete(error));
            }
        };
        // Malformed global facts cannot authorize adoption or orphan cleanup.
        // Preserve the last trusted controllers while restricting new allocation.
        if let Ok(actual) = &basis.inventory {
            if let Err(error) = validate_inventory(actual, &catalog.records) {
                self.authority_reconciling.store(true, Ordering::Release);
                return Ok(ReconciliationOutcome::Incomplete(error));
            }
        }

        if self.is_draining() || self.process_sealing() {
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
        let (mut failure, previously_stale, mut allocation_fenced) = self.completed_comparisons();
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
            match self.adopt_record(record, false) {
                Ok(Some(handle)) => {
                    work.insert(record.spec.id.clone(), (record.assignment.clone(), handle));
                }
                Ok(None) => (),
                Err(error) => {
                    allocation_fenced = true;
                    failure = Some(error);
                }
            }
        }
        // Missing catalog entries cannot resolve an in-flight Claim RPC. Its
        // explicit outcome/retry owns the local reservation, not this snapshot.
        // Clear a prior transport fence only after validating fresh authority.
        for (id, (assignment, handle)) in work {
            let record = catalog.records.get(&id).cloned();
            let references = catalog.references.clone();
            let manager = self.clone();
            let gate = _gate.clone();
            self.start_comparison(id.clone(), None, async move {
                let _gate = gate;
                manager
                    .reconcile_controller(
                        assignment,
                        handle,
                        AuthorityCheck {
                            recover: false,
                            record,
                            references,
                        },
                    )
                    .await
            });
        }
        match basis.inventory {
            Ok(actual) => {
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
                    if !self.execution_reserved(&runtime.runtime_id) {
                        self.authority_reconciling.store(true, Ordering::Release);
                    }
                    self.start_comparison(
                        runtime.environment_id.clone(),
                        Some(runtime.runtime_id.clone()),
                        async move {
                            let _gate = gate;
                            manager.cleanup_orphan(runtime, retire_route).await?;
                            Ok(AuthorityResult::Current)
                        },
                    );
                }
            }
            Err(error) => {
                self.authority_reconciling.store(true, Ordering::Release);
                allocation_fenced = true;
                failure = Some(error);
            }
        }
        // A long restore must not stall the next catalog/resource sample. Keep
        // one tracked task per environment; accepted work is never cancelled.
        let deadline = tokio::time::sleep(wait);
        tokio::pin!(deadline);
        loop {
            let (error, was_stale, orphan_failed) = self.completed_comparisons();
            allocation_fenced |= orphan_failed;
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
        if failure.is_none()
            && self
                .authority_tasks
                .lock()
                .expect("shared state lock poisoned")
                .is_empty()
        {
            if let Some(checkpoint) = &self.services.checkpoint {
                if let Err(error) = checkpoint.store.maintain_cache().await {
                    adx_observability::warn!(%error, "idle checkpoint cache maintenance deferred");
                }
            }
        }
        allocation_fenced |= self
            .authority_tasks
            .lock()
            .expect("shared state lock poisoned")
            .keys()
            .any(|(_, runtime)| {
                runtime
                    .as_ref()
                    .is_some_and(|id| !self.execution_reserved(id))
            });
        self.authority_reconciling
            .store(allocation_fenced, Ordering::Release);
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

    fn adopt_record(
        &self,
        record: &EnvironmentRecord,
        recover: bool,
    ) -> Result<Option<EnvironmentHandle>> {
        self.adopt_comparison_record_inner(record, false, recover)
    }

    pub(crate) fn adopt_successor_record(
        &self,
        record: &EnvironmentRecord,
    ) -> Result<Option<EnvironmentHandle>> {
        self.adopt_comparison_record_inner(record, true, true)
    }

    fn adopt_comparison_record_inner(
        &self,
        record: &EnvironmentRecord,
        successor: bool,
        recover: bool,
    ) -> Result<Option<EnvironmentHandle>> {
        let retired = self
            .retired_environments
            .lock()
            .expect("shared state lock poisoned");
        let mut controllers = self
            .environments
            .lock()
            .expect("shared state lock poisoned");
        if let Some((_, _, handle)) = controllers.get(&record.spec.id) {
            if successor {
                return if handle.view().0 == *record {
                    Ok(Some(handle.clone()))
                } else {
                    Err(Error::Conflict)
                };
            }
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
        if !recover && controller::unstarted(record) {
            return Ok(None);
        }
        if successor
            && !matches!(
                record.state,
                EnvironmentState::Running | EnvironmentState::Paused
            )
        {
            return Err(Error::Conflict);
        }
        if (successor || !recover) && record.resources_held {
            // An unheld record leaves the tentative reservation with its claim.
            self.adopt_local(&record.spec, &record.assignment)?;
            self.restore_execution(record, &record.runtime.id)?;
        }
        let handle = if successor {
            // This inspected successor adopts an existing execution while normal
            // lifecycle admission is still fenced by the process upgrade.
            controller::spawn_comparison(record.clone(), self.services.clone())
        } else {
            self.restore_controller(record, None)?
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
