//! Snapshot creation uses ordinary scheduling; the reference protects its source
//! until the node commits its independently owned recovery artifact.
use super::*;
use adx_core::snapshots::{Reference, SnapshotState};

pub(super) async fn normalize(
    session: &Session,
    mut request: pb::CapsuleSpec,
) -> Result<CapsuleSpec> {
    let Some(id) = &request.snapshot_id else {
        return request.try_into();
    };
    let snapshot = session.get_snapshot(id).await?;
    let reference = Reference::Restore {
        capsule_id: request.id.clone(),
    };
    if request.tenant_id != snapshot.template.tenant_id {
        return Err(Error::NotFound);
    }
    // A completed request may be retried after its source snapshot was deleted.
    let existing = match session.get(&request.id).await {
        Ok(stored) if stored.spec.snapshot_id == request.snapshot_id => Some(stored.spec),
        Ok(_) => return Err(Error::Conflict),
        Err(Error::NotFound) => None,
        Err(error) => return Err(error),
    };
    if existing.is_none()
        && snapshot.state != SnapshotState::Ready
        && !snapshot.references.contains(&reference)
    {
        return Err(Error::Conflict);
    }
    if request.id == snapshot.template.id {
        return Err(Error::Conflict);
    }
    snapshot.origin()?;
    let template = &snapshot.template;
    request.environment = template.environment.clone().map(Into::into);
    if request.image.is_empty() {
        request.image = template.image.clone();
    }
    if request.runtime_class.is_empty() {
        request.runtime_class = template.runtime_class.clone();
    }
    let resources = request.resources.get_or_insert_with(Default::default);
    if resources.cpu_millis == 0 {
        resources.cpu_millis = template.resources.cpu_millis;
    }
    if resources.memory_bytes == 0 {
        resources.memory_bytes = template.resources.memory_bytes;
    }
    if resources.disk_bytes == 0 {
        resources.disk_bytes = template.resources.disk_bytes;
    }
    if request.image != template.image
        || request.runtime_class != template.runtime_class
        || *resources != pb::Resources::from(template.resources)
    {
        return Err(Error::Invalid(
            "snapshot image, runtime and resource geometry must match its source".into(),
        ));
    }
    let overrides = std::mem::take(&mut request.env);
    request.env = template.env.clone().into_iter().collect();
    request.env.extend(overrides);
    let policy = request.scheduling.get_or_insert_with(Default::default);
    if policy.devices.is_empty() {
        policy.devices = template
            .scheduling
            .devices
            .clone()
            .into_iter()
            .map(Into::into)
            .collect();
    }
    if snapshot.artifact.storage == "local" {
        if policy.required_node.is_empty() {
            policy.required_node.push(Default::default());
        }
        for alternative in &mut policy.required_node {
            let required = pb::LabelRequirement {
                key: "NODE_ID".into(),
                op: pb::SelectorOp::In as i32,
                values: vec![snapshot.source_node_id.clone()],
            };
            if !alternative.expressions.contains(&required) {
                alternative.expressions.push(required);
            }
        }
    }
    request.try_into()
}
