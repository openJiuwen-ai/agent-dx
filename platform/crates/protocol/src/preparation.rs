use crate::{control as pb, Error, Result};
use adx_core::snapshot_preparation::{
    PreparationFile, PreparationLog, PreparationPlan, PreparationStep, SourcePreparationResult,
    SourcePreparationStatus,
};

impl From<PreparationFile> for pb::PreparationFile {
    fn from(value: PreparationFile) -> Self {
        Self {
            scope: value.scope,
            location: value.location,
            size_bytes: value.size_bytes,
            sha256: value.sha256,
        }
    }
}

impl From<SourcePreparationResult> for pb::SourcePreparationResult {
    fn from(value: SourcePreparationResult) -> Self {
        let status = match value.status {
            SourcePreparationStatus::Running => pb::SourcePreparationStatus::Running,
            SourcePreparationStatus::Succeeded => pb::SourcePreparationStatus::Succeeded,
            SourcePreparationStatus::Failed => pb::SourcePreparationStatus::Failed,
            SourcePreparationStatus::Unknown => pb::SourcePreparationStatus::Unknown,
        };
        Self {
            operation_id: value.operation_id,
            plan_sha256: value.plan_sha256,
            status: status as i32,
            step_index: value.step_index,
            error: value.error,
            logs: value
                .logs
                .into_iter()
                .map(|log| pb::PreparationLogEntry {
                    offset: log.offset,
                    timestamp_unix_ms: log.timestamp_unix_ms,
                    level: log.level,
                    message: log.message,
                })
                .collect(),
            next_offset: value.next_offset,
            logs_truncated: value.logs_truncated,
        }
    }
}
impl TryFrom<pb::SourcePreparationResult> for SourcePreparationResult {
    type Error = Error;
    fn try_from(value: pb::SourcePreparationResult) -> Result<Self> {
        let status = match pb::SourcePreparationStatus::try_from(value.status) {
            Ok(pb::SourcePreparationStatus::Running) => SourcePreparationStatus::Running,
            Ok(pb::SourcePreparationStatus::Succeeded) => SourcePreparationStatus::Succeeded,
            Ok(pb::SourcePreparationStatus::Failed) => SourcePreparationStatus::Failed,
            Ok(pb::SourcePreparationStatus::Unknown) => SourcePreparationStatus::Unknown,
            _ => return Err(Error::Invalid("invalid source preparation status".into())),
        };
        let result = Self {
            operation_id: value.operation_id,
            plan_sha256: value.plan_sha256,
            status,
            step_index: value.step_index,
            error: value.error,
            logs: value
                .logs
                .into_iter()
                .map(|log| PreparationLog {
                    offset: log.offset,
                    timestamp_unix_ms: log.timestamp_unix_ms,
                    level: log.level,
                    message: log.message,
                })
                .collect(),
            next_offset: value.next_offset,
            logs_truncated: value.logs_truncated,
        };
        result.validate()?;
        Ok(result)
    }
}
impl TryFrom<pb::PreparationFile> for PreparationFile {
    type Error = Error;
    fn try_from(value: pb::PreparationFile) -> Result<Self> {
        let file = Self {
            scope: value.scope,
            location: value.location,
            size_bytes: value.size_bytes,
            sha256: value.sha256,
        };
        file.validate()?;
        Ok(file)
    }
}
impl From<PreparationPlan> for pb::PreparationPlan {
    fn from(value: PreparationPlan) -> Self {
        use pb::preparation_step::Operation as Op;
        let steps = value
            .steps
            .into_iter()
            .map(|step| {
                let operation = match step {
                    PreparationStep::Copy {
                        file,
                        source,
                        destination,
                        user,
                        mode,
                    } => Op::Copy(pb::PreparationCopy {
                        file: Some(file.into()),
                        source,
                        destination,
                        user,
                        mode,
                    }),
                    PreparationStep::Run { command, user } => {
                        Op::Run(pb::PreparationRun { command, user })
                    }
                    PreparationStep::Environment { values } => {
                        Op::Environment(pb::PreparationEnvironment {
                            values: values.into_iter().collect(),
                        })
                    }
                    PreparationStep::User { user } => Op::User(user),
                    PreparationStep::Workdir { path } => Op::Workdir(path),
                };
                pb::PreparationStep {
                    operation: Some(operation),
                }
            })
            .collect();
        Self {
            steps,
            start_command: value.start_command,
            ready_command: value.ready_command,
            timeout_seconds: value.timeout_seconds,
        }
    }
}
impl TryFrom<pb::PreparationPlan> for PreparationPlan {
    type Error = Error;
    fn try_from(value: pb::PreparationPlan) -> Result<Self> {
        use pb::preparation_step::Operation as Op;
        let steps = value
            .steps
            .into_iter()
            .map(|step| {
                Ok(
                    match step.operation.ok_or_else(|| {
                        Error::Invalid("preparation step operation required".into())
                    })? {
                        Op::Copy(copy) => PreparationStep::Copy {
                            file: copy
                                .file
                                .ok_or_else(|| {
                                    Error::Invalid("sealed preparation file required".into())
                                })?
                                .try_into()?,
                            source: copy.source,
                            destination: copy.destination,
                            user: copy.user,
                            mode: copy.mode,
                        },
                        Op::Run(run) => PreparationStep::Run {
                            command: run.command,
                            user: run.user,
                        },
                        Op::Environment(env) => PreparationStep::Environment {
                            values: env.values.into_iter().collect(),
                        },
                        Op::User(user) => PreparationStep::User { user },
                        Op::Workdir(path) => PreparationStep::Workdir { path },
                    },
                )
            })
            .collect::<Result<Vec<_>>>()?;
        let plan = Self {
            steps,
            start_command: value.start_command,
            ready_command: value.ready_command,
            timeout_seconds: value.timeout_seconds,
        };
        plan.validate()?;
        Ok(plan)
    }
}
