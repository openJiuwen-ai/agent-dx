//! Recover or compare a complete authoritative node catalog.
use crate::{controller, Adxlet};
use adx_core::{
    scheduling::{validate_device_assignment, DeviceLedger},
    snapshots::{Snapshot, SnapshotState},
    EnvironmentRecord, EnvironmentState, Error, ResourceLedger, Resources, Result,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
mod catalog;
mod periodic;
use catalog::{validate_inventory, Catalog};
use futures_util::{stream, StreamExt};
pub(crate) use periodic::ReconciliationOutcome;

fn terminal_without_execution(record: &EnvironmentRecord) -> bool {
    matches!(
        record.state,
        EnvironmentState::Failed | EnvironmentState::Deleted
    ) && !record.resources_held
        && !record.restart_pending
}

impl Adxlet {
    fn execution_reserved(&self, runtime_id: &str) -> bool {
        self.services
            .admission
            .lock()
            .expect("shared state lock poisoned")
            .ledger
            .contains(runtime_id)
    }

    fn restore_controller(
        &self,
        record: &EnvironmentRecord,
        pending: Option<crate::journal::JournaledResult>,
    ) -> Result<crate::EnvironmentHandle> {
        if terminal_without_execution(record) {
            self.services.upgrade.record_cleanup()?;
        } else {
            self.services.upgrade.record_action(
                &record.spec.id,
                record.assignment.generation,
                "adopt",
            )?;
        }
        match pending {
            Some(pending) => Ok(controller::spawn_journaled(pending, self.services.clone())),
            None if terminal_without_execution(record) => Ok(controller::spawn_terminal(
                record.clone(),
                self.services.clone(),
            )),
            None => Ok(controller::spawn_comparison(
                record.clone(),
                self.services.clone(),
            )),
        }
    }

    fn restore_execution(&self, record: &EnvironmentRecord, runtime_id: &str) -> Result<()> {
        let mut admission = self
            .services
            .admission
            .lock()
            .expect("shared state lock poisoned");
        let mut ledger = admission.ledger.clone();
        let mut devices = admission.devices.clone();
        ledger.restore(runtime_id, record.spec.resources)?;
        devices.restore(runtime_id, &record.assignment.devices)?;
        admission.ledger = ledger;
        admission.devices = devices;
        Ok(())
    }

    /// Called only with a Coordinator-authorized, unreferenced deletion record.
    /// A lost acknowledgement can safely repeat the same artifact removal.
    pub async fn collect_snapshot(&self, snapshot: &Snapshot, delegated: bool) -> Result<()> {
        snapshot.validate()?;
        if delegated
            || snapshot.source_node_id != self.node_id
            || snapshot.artifact.storage != "local"
            || !snapshot.collectable()
        {
            return Err(Error::Conflict);
        }
        let checkpoint = self
            .services
            .checkpoint
            .as_ref()
            .ok_or_else(|| Error::Unavailable("checkpoint storage is not configured".into()))?;
        checkpoint.store.release(&snapshot.artifact).await
    }

    /// Remove artifacts authorized by a durable Coordinator cleanup task.
    /// Repeating a task after a lost acknowledgement is safe because storage
    /// removal is idempotent; a later artifact remains pending on failure.
    pub async fn collect_checkpoint_cleanup(
        &self,
        cleanup: &adx_core::CheckpointCleanup,
        delegated: bool,
    ) -> Result<()> {
        cleanup.validate()?;
        let checkpoint = self
            .services
            .checkpoint
            .as_ref()
            .ok_or_else(|| Error::Unavailable("checkpoint storage is not configured".into()))?;
        if delegated
            || cleanup.assignment.node_id != self.node_id
            || cleanup
                .artifacts
                .iter()
                .any(|artifact| artifact.storage != "local")
        {
            return Err(Error::Conflict);
        }
        for artifact in &cleanup.artifacts {
            checkpoint.store.release(artifact).await?;
        }
        Ok(())
    }

    /// Caller must obtain the entire catalog from the current Coordinator while node
    /// admission is closed. An unavailable Coordinator must never supply an empty catalog.
    /// Invalid authority/inventory keeps the gate closed. Per-execution failures
    /// retain their controller's fence without closing healthy lifecycle admission.
    pub async fn reconcile(&self, records: Vec<EnvironmentRecord>) -> Result<()> {
        self.reconcile_catalog(records, vec![]).await
    }
    /// The complete source-node snapshot catalog is required even when there are
    /// no remaining Environments. Deleting snapshots retain ownership until GC commits.
    pub async fn reconcile_catalog(
        &self,
        records: Vec<EnvironmentRecord>,
        snapshots: Vec<Snapshot>,
    ) -> Result<()> {
        self.reconcile_authority(records, snapshots, None, None)
            .await
    }
    /// `retained_artifacts` must include all globally referenced artifacts,
    /// including other nodes and pending restores. Only this complete catalog
    /// permits reclaiming checkpoints owned by retired local controllers.
    pub async fn reconcile_retained(
        &self,
        records: Vec<EnvironmentRecord>,
        snapshots: Vec<Snapshot>,
        retained_artifacts: Vec<adx_core::CheckpointArtifact>,
    ) -> Result<()> {
        self.reconcile_authority(records, snapshots, Some(retained_artifacts), None)
            .await
    }
    pub(crate) async fn reconcile_recovery(
        &self,
        records: Vec<EnvironmentRecord>,
        snapshots: Vec<Snapshot>,
        retained_artifacts: Vec<adx_core::CheckpointArtifact>,
        journal: Option<&crate::journal::JournalSink>,
    ) -> Result<()> {
        self.reconcile_authority(records, snapshots, Some(retained_artifacts), journal)
            .await
    }

    async fn reconcile_authority(
        &self,
        records: Vec<EnvironmentRecord>,
        snapshots: Vec<Snapshot>,
        retained_artifacts: Option<Vec<adx_core::CheckpointArtifact>>,
        journal: Option<&crate::journal::JournalSink>,
    ) -> Result<()> {
        let _recovery = self.recovery.lock().await;
        let mut ready = self.lifecycle_ready.write().await;
        if self.is_draining() {
            return Err(Error::Unavailable("node is draining".into()));
        }
        *ready = false;
        drop(ready);
        self.authority_reconciling
            .store(true, std::sync::atomic::Ordering::Release);
        // Drain accepted serial work before taking local result/ledger snapshots.
        // Fencing preserves the same execution's observations and deadlines.
        let existing = self
            .environments
            .lock()
            .expect("shared state lock poisoned")
            .clone();
        let fenced = stream::iter(existing)
            .map(|(id, (_, _, handle))| async move { (id, handle.fence().await) })
            .buffer_unordered(4)
            .collect::<Vec<_>>()
            .await;
        let mut failure = None;
        for (id, result) in fenced {
            if let Err(error) = result {
                deferred(&mut failure, &id, error);
            }
        }
        let Catalog {
            records: catalog,
            mut retained,
            references,
        } = Catalog::new(&self.node_id, records, snapshots, retained_artifacts)?;
        let pending = match journal {
            Some(journal) => {
                journal
                    .recover(&catalog.values().cloned().collect::<Vec<_>>())
                    .await?
            }
            None => vec![],
        };
        let mut pending: BTreeMap<_, _> = pending
            .into_iter()
            .map(|p| (p.spec.id.clone(), p))
            .collect();
        let journal_facts: Vec<_> = pending.values().map(|p| p.record.clone()).collect();

        // Phase one contains no result publication or Environment transitions.
        // Select local results only under current authority; revoked journal
        // records remain facts for cleanup, never a grant to run an execution.
        let mut recovery = catalog.clone();
        for result in pending.values() {
            retained.extend(result.cleanup_artifacts.iter().cloned());
            if catalog
                .get(&result.spec.id)
                .map_or(result.resources_held, |current| {
                    result.authorized_by(current) && result.revision >= current.revision
                })
            {
                recovery.insert(result.spec.id.clone(), result.record.clone());
            }
        }
        for record in recovery.values().filter(|r| r.resources_held) {
            self.adopt_local(&record.spec, &record.assignment)?;
            self.restore_execution(record, &record.runtime.id)?;
        }
        {
            let mut controllers = self
                .environments
                .lock()
                .expect("shared state lock poisoned");
            for record in recovery.values() {
                if controllers.contains_key(&record.spec.id) {
                    continue;
                }
                let local = pending.remove(&record.spec.id).filter(|p| {
                    catalog
                        .get(&record.spec.id)
                        .is_some_and(|current| p.authorized_by(current))
                        && p.record == *record
                });
                match self.restore_controller(record, local) {
                    Ok(handle) => {
                        controllers.insert(
                            record.spec.id.clone(),
                            (record.spec.clone(), record.assignment.clone(), handle),
                        );
                    }
                    Err(error) => deferred(&mut failure, &record.spec.id, error),
                }
            }
        }
        let controllers = self
            .environments
            .lock()
            .expect("shared state lock poisoned")
            .clone();
        let facts: Vec<_> = catalog
            .values()
            .cloned()
            .chain(recovery.values().cloned())
            .chain(journal_facts)
            .chain(controllers.values().map(|(_, _, handle)| handle.view().0))
            .collect();
        let runtime_catalog: Vec<_> = catalog
            .values()
            .map(|current| {
                controllers
                    .get(&current.spec.id)
                    .map(|(_, _, handle)| handle.view())
                    .filter(|(local, durability)| {
                        *durability == Some(crate::Durability::Journaled)
                            && local.assignment == current.assignment
                            && local.spec.matches_immutable_fields(&current.spec)
                            && local.revision >= current.revision
                    })
                    .map_or_else(|| current.clone(), |(local, _)| local)
            })
            .collect();
        let actual = tokio::time::timeout(
            self.services.operation_timeout,
            self.services.runtime.reconcile_inventory(&runtime_catalog),
        )
        .await
        .map_err(|_| Error::Unavailable("inventory timed out".into()))??;
        validate_inventory(&actual, &catalog)?;
        for runtime in &actual {
            if let Some(record) = facts.iter().find(|r| {
                r.spec.id == runtime.environment_id
                    && r.assignment.generation == runtime.generation
                    && r.spec.tenant_id == runtime.tenant_id
            }) {
                self.restore_execution(record, &runtime.runtime_id)?;
            }
        }
        let mut allocation_fenced = actual.iter().any(|r| {
            !self.execution_reserved(&r.runtime_id)
                || catalog
                    .get(&r.environment_id)
                    .is_some_and(|record| record.assignment.generation < r.generation)
        });
        let protected = stream::iter(controllers.clone())
            .map(|(id, (_, _, handle))| {
                let record = handle.view().0;
                let mut inputs = record.execution_artifacts.clone();
                if actual.iter().any(|r| r.runtime_id == record.runtime.id) {
                    inputs.extend(
                        facts
                            .iter()
                            .filter(|r| r.runtime.id == record.runtime.id)
                            .flat_map(|r| {
                                r.execution_artifacts
                                    .iter()
                                    .chain(r.checkpoint.iter().map(|cp| &cp.artifact))
                            })
                            .cloned(),
                    );
                }
                async move { (id, handle.prepare_recovery(inputs).await) }
            })
            .buffer_unordered(4)
            .collect::<Vec<_>>()
            .await;
        let mut inputs_ready = true;
        for (id, result) in protected {
            match result {
                Ok(ready) => inputs_ready &= ready,
                Err(error) => {
                    inputs_ready = false;
                    deferred(&mut failure, &id, error);
                }
            }
        }
        // Directory GC is authorized only while controllers and lifecycle RPCs
        // are quiescent. Never sweep roots after admitting a new capture.
        retained.extend(
            facts
                .iter()
                .flat_map(|r| {
                    r.execution_artifacts.iter().chain(
                        r.checkpoint
                            .iter()
                            .chain(r.snapshot_source.iter())
                            .map(|cp| &cp.artifact),
                    )
                })
                .cloned(),
        );
        let known_inputs = actual.iter().all(|runtime| {
            controllers
                .get(&runtime.environment_id)
                .is_some_and(|(_, _, handle)| {
                    controller::protects_execution(&handle.view().0, &runtime.runtime_id)
                })
        });
        if failure.is_none() && inputs_ready && known_inputs {
            if let Some(checkpoint) = &self.services.checkpoint {
                if let Err(error) = checkpoint.store.reconcile_local(&retained).await {
                    adx_observability::warn!(%error, "idle checkpoint cache maintenance deferred");
                }
            }
        }
        self.services.routes.begin_reconcile().await?;
        for current in catalog.values() {
            let local = controllers
                .get(&current.spec.id)
                .map(|(_, _, handle)| handle.view().0);
            let record = local
                .as_ref()
                .filter(|local| {
                    local.assignment == current.assignment && local.revision >= current.revision
                })
                .unwrap_or(current);
            self.services.routes.seed_recovery(record).await?;
        }
        self.services.routes.finish_reconcile().await?;
        self.authority_initialized
            .store(true, std::sync::atomic::Ordering::Release);
        self.authority_reconciling
            .store(allocation_fenced, std::sync::atomic::Ordering::Release);
        // Phase-one trust is complete independently of result publication.
        // The complete route directory retains unavailable owned identities;
        // each Controller opens its own route and lifecycle after phase two.
        *self.lifecycle_ready.write().await = true;

        // Phase two uses the same serial authority operation as live comparison.
        // An individual publication/readiness failure cannot cancel its peers.
        let results = stream::iter(controllers)
            .map(|(id, (_, assignment, handle))| {
                let current = catalog.get(&id).cloned();
                let references = references.clone();
                async move {
                    let result = self
                        .reconcile_controller(
                            assignment,
                            handle,
                            controller::AuthorityCheck {
                                record: current,
                                references,
                                recover: true,
                            },
                        )
                        .await
                        .map(|_| ());
                    (id, result)
                }
            })
            .buffer_unordered(4)
            .collect::<Vec<_>>()
            .await;
        for (id, result) in results {
            if let Err(error) = result {
                deferred(&mut failure, &id, error);
            }
        }
        let orphans = stream::iter(actual)
            .map(|runtime| {
                let catalog = &catalog;
                async move {
                    let protected = self
                        .environments
                        .lock()
                        .expect("shared state lock poisoned")
                        .get(&runtime.environment_id)
                        .is_some_and(|(_, owner, handle)| {
                            catalog
                                .get(&runtime.environment_id)
                                .is_some_and(|r| r.assignment == *owner)
                                && controller::protects_execution(
                                    &handle.view().0,
                                    &runtime.runtime_id,
                                )
                        });
                    let result = if protected {
                        Ok(())
                    } else {
                        self.cleanup_orphan(
                            runtime.clone(),
                            catalog
                                .get(&runtime.environment_id)
                                .is_none_or(|r| r.assignment.generation != runtime.generation),
                        )
                        .await
                    };
                    (runtime, result)
                }
            })
            .buffer_unordered(4)
            .collect::<Vec<_>>()
            .await;
        allocation_fenced = false;
        for (runtime, result) in orphans {
            allocation_fenced |= catalog
                .get(&runtime.environment_id)
                .is_some_and(|r| r.assignment.generation < runtime.generation);
            if let Err(error) = result {
                allocation_fenced |= !self.execution_reserved(&runtime.runtime_id);
                deferred(&mut failure, &runtime.environment_id, error);
            }
        }
        self.authority_reconciling
            .store(allocation_fenced, std::sync::atomic::Ordering::Release);
        failure.map_or(Ok(()), Err)
    }
}

fn deferred(failure: &mut Option<Error>, environment: &str, error: Error) {
    adx_observability::warn!(environment_id=%environment, %error, "environment recovery deferred");
    *failure = Some(error);
}

impl Adxlet {
    /// Adopt only a Coordinator-issued, durably transferred recovery point. The normal
    /// serialized resume path provides idempotence and owns runtime execution.
    pub async fn recover_environment(
        &self,
        record: EnvironmentRecord,
    ) -> Result<crate::OperationResult> {
        self.recover_environment_with_timeout(record, None).await
    }

    /// Resume a transferred execution with the duration bound to its wake intent.
    pub async fn recover_environment_with_timeout(
        &self,
        record: EnvironmentRecord,
        run_timeout_seconds: Option<u64>,
    ) -> Result<crate::OperationResult> {
        if self
            .authority_reconciling
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(Error::Unavailable(
                "node authority reconciliation pending".into(),
            ));
        }
        let gate = self.lifecycle_ready.read().await;
        if !*gate || self.is_draining() {
            return Err(Error::Unavailable("node is reconciling or draining".into()));
        }
        record.spec.validate()?;
        validate_device_assignment(&record.spec.scheduling.devices, &record.assignment.devices)?;
        let cp = record
            .checkpoint
            .as_ref()
            .or(record.snapshot_source.as_ref())
            .ok_or(Error::Conflict)?;
        let origin = cp.origin.as_ref().ok_or(Error::Conflict)?;
        if record.assignment.node_id != self.node_id
            || record.assignment.environment_id != record.spec.id
            || record.state != EnvironmentState::Paused
            || record.resources_held
            || record.runtime.ip.is_some()
            || record.revision == 0
            || record.assignment.generation == 0
            || cp.artifact.storage == "local"
            || !adx_core::valid_runtime_id(
                &record.spec.id,
                record.assignment.generation,
                &record.runtime.id,
            )
            || (origin.environment_id == record.spec.id
                && origin.ownership_generation >= record.assignment.generation)
            || origin.runtime_id != cp.source_runtime_id
        {
            return Err(Error::Conflict);
        }
        if origin.environment_id != record.spec.id && record.spec.snapshot_id.is_none() {
            return Err(Error::Conflict);
        }
        adx_core::runtime::RuntimeRestore {
            target: adx_core::runtime::RuntimeIdentity {
                environment_id: record.spec.id.clone(),
                runtime_id: record.runtime.id.clone(),
                ownership_generation: record.assignment.generation,
            },
            origin: Some(origin.clone()),
        }
        .validate(origin)?;
        let request = crate::checkpoint::ResumeRequest {
            run_timeout_seconds,
            operation_id: format!("recover-{}", record.assignment.generation),
            expected_revision: record.revision,
        };
        // The Coordinator may reassign an invalidated shared pause to the same
        // healthy node at a fresh generation. Retire the old controller in
        // its serial queue and leave cached checkpoint data to the catalog GC.
        let previous = self
            .environments
            .lock()
            .expect("shared state lock poisoned")
            .get(&record.spec.id)
            .filter(|(_, owner, _)| owner.generation < record.assignment.generation)
            .map(|(_, owner, handle)| (owner.clone(), handle.clone()));
        if let Some((owner, handle)) = previous {
            self.reconcile_controller(
                owner,
                handle,
                controller::AuthorityCheck {
                    record: Some(record.clone()),
                    references: None,
                    recover: false,
                },
            )
            .await?;
        }
        let handle = {
            let now = tokio::time::Instant::now();
            let mut retired = self
                .retired_environments
                .lock()
                .expect("shared state lock poisoned");
            retired.retain(|_, value| value.expires_at > now);
            if retired
                .get(&record.spec.id)
                .is_some_and(|value| value.generation >= record.assignment.generation)
            {
                return Err(Error::Conflict);
            }
            let mut environments = self
                .environments
                .lock()
                .expect("shared state lock poisoned");
            if let Some((spec, owner, handle)) = environments.get(&record.spec.id) {
                if *spec != record.spec || *owner != record.assignment {
                    return Err(Error::Conflict);
                }
                handle.clone()
            } else {
                self.services.upgrade.record_action(
                    &record.spec.id,
                    record.assignment.generation,
                    "recover",
                )?;
                let environment_id = record.spec.id.clone();
                let handle = controller::spawn_restored(record.clone(), self.services.clone());
                environments.insert(
                    environment_id.clone(),
                    (record.spec, record.assignment, handle.clone()),
                );
                retired.remove(&environment_id);
                handle
            }
        };
        handle.recover(request).await
    }
}
