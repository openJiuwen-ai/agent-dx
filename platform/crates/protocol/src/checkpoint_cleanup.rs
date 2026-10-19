use crate::control as pb;
use adx_core::{CheckpointArtifact, CheckpointCleanup, Error, Result};

impl From<CheckpointArtifact> for pb::CheckpointArtifact {
    fn from(a: CheckpointArtifact) -> Self {
        Self {
            storage: a.storage,
            scope: a.scope,
            location: a.location,
            size_bytes: a.size_bytes,
        }
    }
}

impl TryFrom<pb::CheckpointArtifact> for CheckpointArtifact {
    type Error = Error;
    fn try_from(a: pb::CheckpointArtifact) -> Result<Self> {
        if a.storage.is_empty() || a.location.is_empty() || a.size_bytes == 0 {
            return Err(Error::Invalid("invalid checkpoint artifact".into()));
        }
        Ok(Self {
            storage: a.storage,
            scope: a.scope,
            location: a.location,
            size_bytes: a.size_bytes,
        })
    }
}

impl TryFrom<CheckpointCleanup> for pb::CheckpointCleanup {
    type Error = Error;
    fn try_from(task: CheckpointCleanup) -> Result<Self> {
        task.validate()?;
        Ok(Self {
            assignment: Some(task.assignment.try_into()?),
            revision: task.revision,
            artifacts: task.artifacts.into_iter().map(Into::into).collect(),
        })
    }
}

impl TryFrom<pb::CheckpointCleanup> for CheckpointCleanup {
    type Error = Error;
    fn try_from(task: pb::CheckpointCleanup) -> Result<Self> {
        let task = Self {
            assignment: task
                .assignment
                .ok_or_else(|| Error::Invalid("cleanup assignment required".into()))?
                .try_into()?,
            revision: task.revision,
            artifacts: task
                .artifacts
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_>>()?,
        };
        task.validate()?;
        Ok(task)
    }
}
