//! Validate the complete catalog before any cleanup, including its global references.
use super::*;

pub(super) struct Catalog {
    pub records: BTreeMap<String, EnvironmentRecord>,
    pub retained: Vec<adx_core::CheckpointArtifact>,
    pub references: Option<Arc<controller::CheckpointReferences>>,
}
impl Catalog {
    pub fn new(
        node_id: &str,
        records: Vec<EnvironmentRecord>,
        snapshots: Vec<Snapshot>,
        retained_artifacts: Option<Vec<adx_core::CheckpointArtifact>>,
    ) -> Result<Self> {
        let mut snapshot_ids = BTreeSet::new();
        let complete_references = retained_artifacts.is_some();
        let mut retained = retained_artifacts.unwrap_or_default();
        for snapshot in snapshots {
            snapshot.validate()?;
            if snapshot.source_node_id != node_id
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
            if r.assignment.node_id != node_id
                || r.assignment.environment_id != r.spec.id
                || r.assignment.generation == 0
                || !adx_core::valid_runtime_id(&r.spec.id, r.assignment.generation, &r.runtime.id)
                || (r.state == EnvironmentState::Running
                    && (!r.resources_held || r.runtime.ip.is_none() || r.revision == 0))
                || (r.state == EnvironmentState::Deleted && r.resources_held)
                || catalog.contains_key(&r.spec.id)
            {
                return Err(Error::Conflict);
            }
            retained.extend(r.execution_artifacts.iter().cloned());
            if let Some(source) = &r.snapshot_source {
                retained.push(source.artifact.clone());
            }
            if r.resources_held {
                scalar.restore(&r.runtime.id, r.spec.resources)?;
                cards.restore(&r.runtime.id, &r.assignment.devices)?;
            }
            catalog.insert(r.spec.id.clone(), r);
        }
        // Only a complete global reference catalog proves that a shared point
        // is no longer needed by a different node or a recovery in progress.
        let references = complete_references.then(|| {
            Arc::new(
                retained
                    .iter()
                    .chain(catalog.values().flat_map(|record| {
                        record
                            .checkpoint
                            .iter()
                            .chain(record.snapshot_source.iter())
                            .map(|cp| &cp.artifact)
                            .chain(record.execution_artifacts.iter())
                    }))
                    .map(|artifact| (artifact.storage.clone(), artifact.location.clone()))
                    .collect(),
            )
        });
        Ok(Self {
            records: catalog,
            retained,
            references,
        })
    }
}

pub(super) fn validate_inventory(
    actual: &[crate::RuntimeObservation],
    catalog: &BTreeMap<String, EnvironmentRecord>,
) -> Result<()> {
    let mut seen = BTreeSet::new();
    for runtime in actual {
        if runtime.environment_id.is_empty()
            || runtime.tenant_id.is_empty()
            || runtime.generation == 0
            || !adx_core::valid_runtime_id(
                &runtime.environment_id,
                runtime.generation,
                &runtime.runtime_id,
            )
            || !seen.insert(runtime.runtime_id.clone())
        {
            return Err(Error::Conflict);
        }
        if let Some(r) = catalog.get(&runtime.environment_id) {
            if r.runtime.id == runtime.runtime_id && r.spec.tenant_id != runtime.tenant_id {
                return Err(Error::Conflict);
            }
        }
    }
    Ok(())
}
