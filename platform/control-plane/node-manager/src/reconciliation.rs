//! Restore only an authoritative complete node catalog. No local directory is required.
use crate::{controller, NodeManager};
use adx_core::{
    scheduling::{validate_device_assignment, DeviceLedger},
    Error, InstanceRecord, InstanceState, ResourceLedger, Resources, Result,
};
use std::collections::{BTreeMap, BTreeSet};
impl NodeManager {
    /// Caller must obtain the entire catalog from the current Master while node
    /// admission is closed. An unavailable Master must never supply an empty catalog.
    /// Gate remains closed on any failure; rerunning the same catalog is safe.
    pub async fn reconcile(&self, records: Vec<InstanceRecord>) -> Result<()> {
        let mut ready = self.lifecycle_ready.write().await;
        if self.is_draining() {
            return Err(Error::Unavailable("node is draining".into()));
        }
        *ready = false;
        let mut catalog = BTreeMap::new();
        let mut scalar = ResourceLedger::new(Resources::default());
        let mut cards = DeviceLedger::default();
        for r in records {
            r.spec.validate()?;
            validate_device_assignment(&r.spec.scheduling.devices, &r.assignment.devices)?;
            if r.assignment.node_id != self.node_id
                || r.assignment.instance_id != r.spec.id
                || r.assignment.generation == 0
                || r.runtime_id != format!("{}-{}", r.spec.id, r.assignment.generation)
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
        let controllers = self.instances.lock().unwrap().clone();
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
                || runtime.runtime_id != format!("{}-{}", runtime.instance_id, runtime.generation)
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
            if catalog.get(&id).is_none_or(|r| r.assignment != assignment) {
                // Drain accepted operations, then clean locally without inventing a
                // cluster record for an identity the authority does not own.
                handle.discard().await?;
                self.retired_generations
                    .lock()
                    .unwrap()
                    .entry(id.clone())
                    .and_modify(|g| *g = (*g).max(assignment.generation))
                    .or_insert(assignment.generation);
                self.instances.lock().unwrap().remove(&id);
            }
        }
        for runtime in &actual {
            if catalog
                .get(&runtime.instance_id)
                .is_none_or(|r| r.runtime_id != runtime.runtime_id)
            {
                tokio::time::timeout(self.services.operation_timeout, async {
                    self.services.routes.retire_orphan(runtime).await?;
                    self.services.runtime.remove(&runtime.runtime_id).await
                })
                .await
                .map_err(|_| Error::Unavailable("orphan cleanup timed out".into()))??;
            }
        }
        for r in catalog.into_values() {
            let handle = {
                let mut instances = self.instances.lock().unwrap();
                if let Some((_, _, handle)) = instances.get(&r.spec.id) {
                    handle.clone()
                } else {
                    if r.resources_held {
                        let mut admission = self.services.admission.lock().unwrap();
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
            handle.reconcile().await?;
        }
        self.services.routes.finish_reconcile().await?;
        *ready = true;
        Ok(())
    }
}
