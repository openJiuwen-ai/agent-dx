use crate::control as pb;
use adx_core::{CheckpointCleanup, Error, Result};

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
