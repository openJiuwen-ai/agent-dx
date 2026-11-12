//! Concrete source-instance preparation inputs, independent of client protocols.
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

pub const MAX_PREPARATION_STEPS: usize = 128;
pub const MAX_PREPARATION_PLAN_BYTES: usize = 256 * 1024;
pub const MAX_PREPARATION_FILE_BYTES: u64 = 512 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparationFile {
    pub scope: String,
    pub location: String,
    pub size_bytes: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum PreparationStep {
    Copy {
        file: PreparationFile,
        source: String,
        destination: String,
        user: Option<String>,
        mode: Option<u32>,
    },
    Run {
        command: String,
        user: Option<String>,
    },
    Environment {
        values: BTreeMap<String, String>,
    },
    User {
        user: String,
    },
    Workdir {
        path: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparationPlan {
    pub steps: Vec<PreparationStep>,
    pub start_command: Option<String>,
    pub ready_command: Option<String>,
    pub timeout_seconds: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourcePreparationStatus {
    Running,
    Succeeded,
    Failed,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreparationLog {
    pub offset: u64,
    pub timestamp_unix_ms: u64,
    pub level: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourcePreparationResult {
    pub operation_id: String,
    pub plan_sha256: String,
    pub status: SourcePreparationStatus,
    pub step_index: u32,
    pub error: String,
    pub logs: Vec<PreparationLog>,
    pub next_offset: u64,
    pub logs_truncated: bool,
}

impl SourcePreparationResult {
    pub fn validate(&self) -> Result<()> {
        if self.operation_id.is_empty()
            || self.operation_id.len() > 128
            || self.operation_id.chars().any(char::is_control)
            || self.plan_sha256.len() != 64
            || !self.plan_sha256.bytes().all(|b| b.is_ascii_hexdigit())
            || self.error.len() > 2048
            || self.error.contains('\0')
            || self.logs.len() > 256
            || self.next_offset > 256
            || self.logs.iter().any(|v| {
                v.offset >= self.next_offset || v.message.len() > 4096 || v.level.len() > 16
            })
            || self
                .logs
                .windows(2)
                .any(|v| v[0].offset.checked_add(1) != Some(v[1].offset))
            || serde_json::to_vec(&self.logs)
                .map_err(|_| invalid("cannot encode logs"))?
                .len()
                > 128 * 1024 + 512
        {
            return Err(invalid("invalid or oversized source result"));
        }
        Ok(())
    }
}

fn invalid(message: &str) -> Error {
    Error::Invalid(format!("source preparation: {message}"))
}

fn text(value: &str) -> Result<()> {
    if value.is_empty() || value.len() > 64 * 1024 || value.contains('\0') {
        return Err(invalid("empty, oversized or NUL-containing input"));
    }
    Ok(())
}

fn user(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 256
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_-.:".contains(&b))
        || value.starts_with('-')
    {
        return Err(invalid("invalid user"));
    }
    Ok(())
}

impl PreparationFile {
    pub fn validate(&self) -> Result<()> {
        if self.scope.is_empty()
            || self.scope.len() > 256
            || self.location.is_empty()
            || self.location.len() > 1024
            || self
                .location
                .split('/')
                .any(|part| part.is_empty() || part == "." || part == "..")
            || self.location.contains(['\\', '\0'])
            || self.size_bytes == 0
            || self.size_bytes > MAX_PREPARATION_FILE_BYTES
            || self.sha256.len() != 64
            || !self.sha256.bytes().all(|b| b.is_ascii_hexdigit())
        {
            return Err(invalid("invalid sealed file reference"));
        }
        Ok(())
    }
}

impl PreparationPlan {
    /// Validate the complete recipe before any source-instance side effect.
    pub fn validate(&self) -> Result<()> {
        if self.steps.len() > MAX_PREPARATION_STEPS || !(1..=3600).contains(&self.timeout_seconds) {
            return Err(invalid("step count or execution budget exceeds limit"));
        }
        for command in [&self.start_command, &self.ready_command]
            .into_iter()
            .flatten()
        {
            text(command)?;
        }
        for step in &self.steps {
            match step {
                PreparationStep::Copy {
                    file,
                    source,
                    destination,
                    user: owner,
                    mode,
                } => {
                    file.validate()?;
                    text(source)?;
                    text(destination)?;
                    if source.starts_with('/') || source.split('/').any(|part| part == "..") {
                        return Err(invalid("copy source must stay within its uploaded context"));
                    }
                    if let Some(owner) = owner {
                        user(owner)?;
                    }
                    if mode.is_some_and(|mode| mode > 0o7777) {
                        return Err(invalid("invalid copy mode"));
                    }
                }
                PreparationStep::Run {
                    command,
                    user: identity,
                } => {
                    text(command)?;
                    if let Some(identity) = identity {
                        user(identity)?;
                    }
                }
                PreparationStep::Environment { values } => {
                    for (key, value) in values {
                        if key.is_empty()
                            || !key.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
                            || key.as_bytes()[0].is_ascii_digit()
                            || key.starts_with("ADX_")
                            || key.starts_with("EXECD_")
                            || value.contains('\0')
                        {
                            return Err(invalid("invalid or reserved environment variable"));
                        }
                    }
                }
                PreparationStep::User { user: identity } => user(identity)?,
                PreparationStep::Workdir { path } => text(path)?,
            }
        }
        if serde_json::to_vec(self)
            .map_err(|_| invalid("cannot encode plan"))?
            .len()
            > MAX_PREPARATION_PLAN_BYTES
        {
            return Err(invalid("plan exceeds limit"));
        }
        Ok(())
    }

    pub fn fingerprint(&self) -> Result<String> {
        self.validate()?;
        let bytes = serde_json::to_vec(self).map_err(|_| invalid("cannot encode plan"))?;
        Ok(format!("{:x}", Sha256::digest(bytes)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plan() -> PreparationPlan {
        PreparationPlan {
            steps: vec![PreparationStep::Run {
                command: "echo hello".into(),
                user: None,
            }],
            start_command: None,
            ready_command: None,
            timeout_seconds: 600,
        }
    }

    #[test]
    fn source_plan_preserves_order_and_fences_changed_command_identity() {
        let first = plan();
        let mut changed = first.clone();
        changed.steps.insert(
            0,
            PreparationStep::User {
                user: "user".into(),
            },
        );
        assert_ne!(first.fingerprint().unwrap(), changed.fingerprint().unwrap());
        assert_eq!(
            first,
            serde_json::from_slice(&serde_json::to_vec(&first).unwrap()).unwrap()
        );
    }

    #[test]
    fn source_plan_rejects_control_environment_and_unbounded_execution() {
        let mut input = plan();
        input.steps.push(PreparationStep::Environment {
            values: BTreeMap::from([("EXECD_HTTP_TOKEN".into(), "override".into())]),
        });
        assert!(input.validate().is_err());
        input = plan();
        input.timeout_seconds = 3601;
        assert!(input.validate().is_err());
    }
}
