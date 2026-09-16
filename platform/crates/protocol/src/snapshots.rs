use crate::control as pb;
use adx_core::{
    snapshots::{Reference, Snapshot, SnapshotState},
    CheckpointArtifact, Error, Result,
};
impl TryFrom<Snapshot> for pb::ReusableSnapshot {
    type Error = Error;
    fn try_from(s: Snapshot) -> Result<Self> {
        s.validate()?;
        Ok(Self {
            id: s.id,
            names: s.names,
            template: Some(s.template.into()),
            source_node_id: s.source_node_id,
            source_runtime_id: s.source_runtime_id,
            artifact: Some(pb::CheckpointArtifact {
                storage: s.artifact.storage,
                location: s.artifact.location,
                size_bytes: s.artifact.size_bytes,
            }),
            revision: s.revision,
            state: match s.state {
                SnapshotState::Ready => pb::SnapshotState::Ready,
                SnapshotState::Deleting => pb::SnapshotState::Deleting,
                SnapshotState::Deleted => pb::SnapshotState::Deleted,
            } as i32,
            references: s
                .references
                .into_iter()
                .map(|r| pb::SnapshotReference {
                    owner: Some(match r {
                        Reference::Restore { instance_id } => {
                            pb::snapshot_reference::Owner::RestoreInstanceId(instance_id)
                        }
                        Reference::Template {
                            node_id,
                            template_id,
                        } => pb::snapshot_reference::Owner::Template(pb::TemplateReference {
                            node_id,
                            template_id,
                        }),
                    }),
                })
                .collect(),
        })
    }
}
impl TryFrom<pb::ReusableSnapshot> for Snapshot {
    type Error = Error;
    fn try_from(s: pb::ReusableSnapshot) -> Result<Self> {
        let artifact = s
            .artifact
            .ok_or_else(|| Error::Invalid("snapshot artifact required".into()))?;
        let state = match pb::SnapshotState::try_from(s.state).ok() {
            Some(pb::SnapshotState::Ready) => SnapshotState::Ready,
            Some(pb::SnapshotState::Deleting) => SnapshotState::Deleting,
            Some(pb::SnapshotState::Deleted) => SnapshotState::Deleted,
            _ => return Err(Error::Invalid("snapshot state required".into())),
        };
        let mut references = std::collections::BTreeSet::new();
        for r in s.references {
            let owner = match r.owner {
                Some(pb::snapshot_reference::Owner::RestoreInstanceId(instance_id)) => {
                    Reference::Restore { instance_id }
                }
                Some(pb::snapshot_reference::Owner::Template(t)) => Reference::Template {
                    node_id: t.node_id,
                    template_id: t.template_id,
                },
                None => return Err(Error::Invalid("snapshot reference owner required".into())),
            };
            if !references.insert(owner) {
                return Err(Error::Invalid("duplicate snapshot reference".into()));
            }
        }
        let result = Snapshot {
            id: s.id,
            names: s.names,
            template: s
                .template
                .ok_or_else(|| Error::Invalid("snapshot template required".into()))?
                .try_into()?,
            source_node_id: s.source_node_id,
            source_runtime_id: s.source_runtime_id,
            artifact: CheckpointArtifact {
                storage: artifact.storage,
                location: artifact.location,
                size_bytes: artifact.size_bytes,
            },
            revision: s.revision,
            state,
            references,
        };
        result.validate()?;
        Ok(result)
    }
}
