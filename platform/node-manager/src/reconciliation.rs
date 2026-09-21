//! Restore only an authoritative complete node catalog. No local directory is required.
use crate::{controller, NodeManager};
use adx_core::{
    scheduling::{validate_device_assignment, DeviceLedger},
    snapshots::{Snapshot, SnapshotState},
    Error, InstanceRecord, InstanceState, ResourceLedger, Resources, Result,
};
use std::collections::{BTreeMap, BTreeSet};
impl NodeManager {
    /// Collection is permitted only after complete authoritative recovery.
    pub async fn collect_remote_orphans(&self) -> Result<usize> {
        let ready = self.lifecycle_ready.read().await;
        if !*ready || self.is_draining() {
            return Ok(0);
        }
        match &self.services.checkpoint {
            Some(checkpoint) => checkpoint.store.collect_remote_orphans().await,
            None => Ok(0),
        }
    }

    /// Called only with a Master-authorized, unreferenced deletion record.
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
        checkpoint.store.remove(&snapshot.artifact).await
    }

    /// Caller must obtain the entire catalog from the current Master while node
    /// admission is closed. An unavailable Master must never supply an empty catalog.
    /// Gate remains closed on any failure; rerunning the same catalog is safe.
    pub async fn reconcile(&self, records: Vec<InstanceRecord>) -> Result<()> {
        self.reconcile_catalog(records, vec![]).await
    }
    /// The complete source-node snapshot catalog is required even when there are
    /// no remaining Instances. Deleting snapshots retain ownership until GC commits.
    pub async fn reconcile_catalog(
        &self,
        records: Vec<InstanceRecord>,
        snapshots: Vec<Snapshot>,
    ) -> Result<()> {
        self.reconcile_retained(records, snapshots, vec![]).await
    }
    pub async fn reconcile_retained(
        &self,
        records: Vec<InstanceRecord>,
        snapshots: Vec<Snapshot>,
        retained_checkpoints: Vec<adx_core::CheckpointArtifact>,
    ) -> Result<()> {
        let mut ready = self.lifecycle_ready.write().await;
        if self.is_draining() {
            return Err(Error::Unavailable("node is draining".into()));
        }
        *ready = false;
        let mut snapshot_ids = BTreeSet::new();
        let mut retained = retained_checkpoints;
        for snapshot in snapshots {
            snapshot.validate()?;
            if snapshot.source_node_id != self.node_id
                || snapshot.state == SnapshotState::Deleted
                || !snapshot_ids.insert(snapshot.id)
            {
                return Err(Error::Conflict);
            }
            retained.push(snapshot.artifact);
        }
        let mut catalog = BTreeMap::new();
        let mut scalar = ResourceLedger::new(Resources::default());
        let mut cards = DeviceLedger::default();
        for r in records {
            r.spec.validate()?;
            validate_device_assignment(&r.spec.scheduling.devices, &r.assignment.devices)?;
            if r.assignment.node_id != self.node_id
                || r.assignment.instance_id != r.spec.id
                || r.assignment.generation == 0
                || !adx_core::valid_runtime_id(&r.spec.id, r.assignment.generation, &r.runtime_id)
                || (r.state == InstanceState::Running
                    && (!r.resources_held || r.runtime_ip.is_none() || r.revision == 0))
                || (r.state == InstanceState::Deleted && r.resources_held)
                || catalog.contains_key(&r.spec.id)
            {
                return Err(Error::Conflict);
            }
            if r.resources_held {
                scalar.restore(&r.runtime_id, r.spec.resources)?;
                cards.restore(&r.runtime_id, &r.assignment.devices)?;
            }
            catalog.insert(r.spec.id.clone(), r);
        }
        let controllers = self
            .instances
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
        let actual = tokio::time::timeout(
            self.services.operation_timeout,
            self.services.runtime.inventory(),
        )
        .await
        .map_err(|_| Error::Unavailable("inventory timed out".into()))??;
        let mut seen = BTreeSet::new();
        for runtime in &actual {
            if runtime.instance_id.is_empty()
                || runtime.tenant_id.is_empty()
                || runtime.generation == 0
                || !adx_core::valid_runtime_id(
                    &runtime.instance_id,
                    runtime.generation,
                    &runtime.runtime_id,
                )
                || !seen.insert(runtime.runtime_id.clone())
            {
                return Err(Error::Conflict);
            }
            if let Some(r) = catalog.get(&runtime.instance_id) {
                if r.runtime_id == runtime.runtime_id && r.spec.tenant_id != runtime.tenant_id {
                    return Err(Error::Conflict);
                }
            }
        }
        // Validate everything before any destructive action or partial ledger update.
        self.services.routes.begin_reconcile().await?;
        for (id, (_, assignment, handle)) in controllers {
            if catalog.get(&id).is_none_or(|r| {
                r.assignment != assignment
                    || (matches!(r.state, InstanceState::Failed | InstanceState::Deleted)
                        && !r.resources_held
                        && !r.restart_pending)
            }) {
                // Drain accepted operations, then clean locally without inventing a
                // cluster record for an identity the authority does not own.
                handle.discard().await?;
                self.retired_generations
                    .lock()
                    .expect("shared state lock poisoned")
                    .entry(id.clone())
                    .and_modify(|g| *g = (*g).max(assignment.generation))
                    .or_insert(assignment.generation);
                self.instances
                    .lock()
                    .expect("shared state lock poisoned")
                    .remove(&id);
            }
        }
        for runtime in &actual {
            if catalog
                .get(&runtime.instance_id)
                .is_none_or(|r| r.runtime_id != runtime.runtime_id)
            {
                tokio::time::timeout(self.services.operation_timeout, async {
                    // A stale execution under current ownership must not publish a
                    // MAX revision tombstone that also fences the restored execution.
                    if catalog
                        .get(&runtime.instance_id)
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
                let mut instances = self.instances.lock().expect("shared state lock poisoned");
                if let Some((_, _, handle)) = instances.get(&r.spec.id) {
                    handle.clone()
                } else {
                    if r.resources_held {
                        let mut admission = self
                            .services
                            .admission
                            .lock()
                            .expect("shared state lock poisoned");
                        // Catalog has already been validated for scalar/card conflicts.
                        admission.ledger.restore(&r.runtime_id, r.spec.resources)?;
                        admission
                            .devices
                            .restore(&r.runtime_id, &r.assignment.devices)?;
                    }
                    let handle = controller::spawn_restored(r.clone(), self.services.clone());
                    instances.insert(r.spec.id.clone(), (r.spec, r.assignment, handle.clone()));
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
        if let Some(checkpoint) = &self.services.checkpoint {
            checkpoint.store.authorize_remote_gc(&retained).await?;
        }
        self.local_holds
            .lock()
            .expect("shared state lock poisoned")
            .clear();
        *ready = true;
        Ok(())
    }
}

impl NodeManager {
    /// Adopt only a Master-issued, durably transferred recovery point. The normal
    /// serialized resume path provides idempotence and owns runtime execution.
    pub async fn recover_instance(&self, record: InstanceRecord) -> Result<crate::OperationResult> {
        let gate = self.lifecycle_ready.read().await;
        if !*gate || self.is_draining() {
            return Err(Error::Unavailable("node is reconciling or draining".into()));
        }
        record.spec.validate()?;
        validate_device_assignment(&record.spec.scheduling.devices, &record.assignment.devices)?;
        let cp = record.checkpoint.as_ref().ok_or(Error::Conflict)?;
        let origin = cp.origin.as_ref().ok_or(Error::Conflict)?;
        if record.assignment.node_id != self.node_id
            || record.assignment.instance_id != record.spec.id
            || record.state != InstanceState::Paused
            || record.resources_held
            || record.runtime_ip.is_some()
            || record.revision == 0
            || record.assignment.generation == 0
            || cp.artifact.storage == "local"
            || !adx_core::valid_runtime_id(
                &record.spec.id,
                record.assignment.generation,
                &record.runtime_id,
            )
            || (origin.instance_id == record.spec.id
                && origin.ownership_generation >= record.assignment.generation)
            || origin.runtime_id != cp.source_runtime_id
        {
            return Err(Error::Conflict);
        }
        if origin.instance_id != record.spec.id && record.spec.snapshot_id.is_none() {
            return Err(Error::Conflict);
        }
        adx_core::runtime::RuntimeRestore {
            target: adx_core::runtime::RuntimeIdentity {
                instance_id: record.spec.id.clone(),
                runtime_id: record.runtime_id.clone(),
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
            let mut instances = self.instances.lock().expect("shared state lock poisoned");
            if self
                .retired_generations
                .lock()
                .expect("shared state lock poisoned")
                .get(&record.spec.id)
                .is_some_and(|g| *g >= record.assignment.generation)
            {
                return Err(Error::Conflict);
            }
            if let Some((spec, owner, handle)) = instances.get(&record.spec.id) {
                if *spec != record.spec || *owner != record.assignment {
                    return Err(Error::Conflict);
                }
                handle.clone()
            } else {
                let handle = controller::spawn_restored(record.clone(), self.services.clone());
                instances.insert(
                    record.spec.id.clone(),
                    (record.spec, record.assignment, handle.clone()),
                );
                handle
            }
        };
        handle.recover(request).await
    }
}
