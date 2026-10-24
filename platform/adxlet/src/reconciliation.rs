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
pub(crate) use periodic::ReconciliationOutcome;

fn terminal_without_execution(record: &EnvironmentRecord) -> bool {
    matches!(
        record.state,
        EnvironmentState::Failed | EnvironmentState::Deleted
    ) && !record.resources_held
        && !record.restart_pending
}

impl Adxlet {
    /// Called only with a Coordinator-authorized, unreferenced deletion record.
    /// A lost acknowledgement can safely repeat the same artifact removal.
    pub async fn collect_snapshot(&self, snapshot: &Snapshot) -> Result<()> {
        snapshot.validate()?;
        if snapshot.source_node_id != self.node_id || !snapshot.collectable() {
            return Err(Error::Conflict);
        }
        let checkpoint = self
            .services
            .checkpoint
            .as_ref()
            .ok_or_else(|| Error::Unavailable("checkpoint storage is not configured".into()))?;
        checkpoint.store.release(&snapshot.artifact).await
    }

    /// Caller must obtain the entire catalog from the current Coordinator while node
    /// admission is closed. An unavailable Coordinator must never supply an empty catalog.
    /// Gate remains closed on any failure; rerunning the same catalog is safe.
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
        self.reconcile_authority(records, snapshots, None).await
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
        self.reconcile_authority(records, snapshots, Some(retained_artifacts))
            .await
    }
    async fn reconcile_authority(
        &self,
        records: Vec<EnvironmentRecord>,
        snapshots: Vec<Snapshot>,
        retained_artifacts: Option<Vec<adx_core::CheckpointArtifact>>,
    ) -> Result<()> {
        let mut ready = self.lifecycle_ready.write().await;
        if self.is_draining() {
            return Err(Error::Unavailable("node is draining".into()));
        }
        *ready = false;
        let Catalog {
            records: catalog,
            mut retained,
            references,
        } = Catalog::new(&self.node_id, records, snapshots, retained_artifacts)?;
        let controllers = self
            .environments
            .lock()
            .expect("shared state lock poisoned")
            .clone();
        for (id, (spec, assignment, _)) in &controllers {
            if let Some(r) = catalog.get(id) {
                if r.assignment.generation == assignment.generation
                    && (r.spec != *spec || r.assignment != *assignment)
                {
                    return Err(Error::Conflict);
                }
            }
        }
        // Startup owns the node barrier; drain already accepted controller work.
        for (id, (_, assignment, handle)) in &controllers {
            if let Some(record) = catalog.get(id).filter(|r| r.assignment == *assignment) {
                handle.align_authority(record.clone()).await?;
            }
        }
        let runtime_catalog: Vec<_> = catalog.values().cloned().collect();
        let actual = tokio::time::timeout(
            self.services.operation_timeout,
            self.services.runtime.reconcile_inventory(&runtime_catalog),
        )
        .await
        .map_err(|_| Error::Unavailable("inventory timed out".into()))??;
        validate_inventory(&actual, &catalog)?;
        self.services.routes.begin_reconcile().await?;
        for (id, (_, assignment, handle)) in controllers {
            let ownership_retired = catalog.get(&id).is_none_or(|r| r.assignment != assignment);
            if ownership_retired || catalog.get(&id).is_some_and(terminal_without_execution) {
                // Drain accepted operations, then clean locally without inventing a
                // cluster record for an identity the authority does not own.
                handle.discard(references.clone()).await?;
                if ownership_retired {
                    self.remember_retired(&assignment, None);
                }
                self.environments
                    .lock()
                    .expect("shared state lock poisoned")
                    .remove(&id);
            }
        }
        for runtime in &actual {
            if catalog
                .get(&runtime.environment_id)
                .is_none_or(|r| r.runtime.id != runtime.runtime_id || terminal_without_execution(r))
            {
                tokio::time::timeout(self.services.operation_timeout, async {
                    // A stale execution under current ownership must not publish a
                    // MAX revision tombstone that also fences the restored execution.
                    if catalog
                        .get(&runtime.environment_id)
                        .is_none_or(|r| r.assignment.generation != runtime.generation)
                    {
                        self.services.routes.retire_orphan(runtime).await?;
                    }
                    self.services.runtime.remove(&runtime.runtime_id).await
                })
                .await
                .map_err(|_| Error::Unavailable("orphan cleanup timed out".into()))??;
            }
        }
        for r in catalog.into_values() {
            let handle = {
                let mut environments = self
                    .environments
                    .lock()
                    .expect("shared state lock poisoned");
                if let Some((_, _, handle)) = environments.get(&r.spec.id) {
                    handle.clone()
                } else {
                    retained.extend(r.execution_artifacts.iter().cloned());
                    if let Some(source) = &r.snapshot_source {
                        retained.push(source.artifact.clone());
                    }
                    if r.resources_held {
                        let mut admission = self
                            .services
                            .admission
                            .lock()
                            .expect("shared state lock poisoned");
                        // Catalog has already been validated for scalar/card conflicts.
                        admission.ledger.restore(&r.runtime.id, r.spec.resources)?;
                        admission
                            .devices
                            .restore(&r.runtime.id, &r.assignment.devices)?;
                    }
                    let handle = controller::spawn_restored(r.clone(), self.services.clone());
                    environments.insert(
                        r.spec.id.clone(),
                        (r.spec.clone(), r.assignment.clone(), handle.clone()),
                    );
                    handle
                }
            };
            let result = handle.reconcile().await?;
            if result.durability != crate::Durability::Published {
                return Err(Error::Unavailable(
                    "reconciliation state is not published".into(),
                ));
            }
            if let Some(checkpoint) = result.record.checkpoint {
                retained.push(checkpoint.artifact);
            }
        }
        if let Some(checkpoint) = &self.services.checkpoint {
            checkpoint.store.reconcile_local(&retained).await?;
        }
        self.services.routes.finish_reconcile().await?;
        self.local_holds
            .lock()
            .expect("shared state lock poisoned")
            .clear();
        *ready = true;
        self.authority_initialized
            .store(true, std::sync::atomic::Ordering::Release);
        Ok(())
    }
}

impl Adxlet {
    /// Adopt only a Coordinator-issued, durably transferred recovery point. The normal
    /// serialized resume path provides idempotence and owns runtime execution.
    pub async fn recover_environment(
        &self,
        record: EnvironmentRecord,
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
        let cp = record.checkpoint.as_ref().ok_or(Error::Conflict)?;
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
            operation_id: format!("recover-{}", record.assignment.generation),
            expected_revision: record.revision,
        };
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
