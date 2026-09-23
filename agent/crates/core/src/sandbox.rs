//! General Sandbox capability boundary shared by Gateway adapters and Activator clients.
//! Contains no Coordinator/Adxlet identities, Platform storage keys or business invoke protocol.
use crate::{Resources, Service, TemplateVersion, ValidationResult};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionSpec {
    pub image: String,
    pub isolation_runtime: String,
    /// Empty only for an inline base sandbox which runs EXECD without a user process.
    pub entrypoint: Vec<String>,
    pub working_dir: String,
    pub user: Option<String>,
    pub env: BTreeMap<String, String>,
    pub resources: Resources,
    pub service: Vec<Service>,
}
impl From<&TemplateVersion> for ExecutionSpec {
    fn from(value: &TemplateVersion) -> Self {
        Self {
            image: value.image.clone(),
            isolation_runtime: value.isolation_runtime.clone(),
            entrypoint: value.entrypoint.clone(),
            working_dir: value.working_dir.clone().unwrap_or_else(|| "/".into()),
            user: None,
            env: value.env.clone(),
            resources: value.resources.clone(),
            service: value.service.clone(),
        }
    }
}
impl ExecutionSpec {
    pub fn validate(&self) -> ValidationResult {
        let template = TemplateVersion {
            name: "execution".into(),
            version: "1".into(),
            image: self.image.clone(),
            isolation_runtime: self.isolation_runtime.clone(),
            entrypoint: if self.entrypoint.is_empty() {
                vec!["execd-idle".into()]
            } else {
                self.entrypoint.clone()
            },
            working_dir: Some(self.working_dir.clone()),
            env: self.env.clone(),
            resources: self.resources.clone(),
            service: self.service.clone(),
        };
        template.validate()?;
        if self
            .user
            .as_ref()
            .is_some_and(|s| s.trim().is_empty() || s.contains('\0'))
        {
            return Err("invalid process user".into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateSandbox {
    pub id: String,
    pub tenant: String,
    pub execution: ExecutionSpec,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SandboxPhase {
    Creating,
    Running,
    Failed,
    Deleted,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SandboxObservation {
    pub id: String,
    pub tenant: String,
    pub phase: SandboxPhase,
    pub ready: bool,
    pub runtime_id: Option<String>,
    pub message: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "message", rename_all = "snake_case")]
pub enum SandboxError {
    /// Absent from the caller's visible scope; never confirms physical deletion.
    NotFound,
    Invalid(String),
    Unsupported(String),
    Conflict(String),
    Unavailable(String),
    OutcomeUnknown(String),
}
impl std::fmt::Display for SandboxError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for SandboxError {}

#[async_trait]
pub trait Sandbox: Send + Sync {
    /// Local admission validation only; implementations may reject deployment
    /// capabilities before any state or execution is created.
    fn validate_execution(&self, execution: &ExecutionSpec) -> Result<(), SandboxError> {
        execution.validate().map_err(SandboxError::Invalid)
    }
    /// Must be idempotent for a stable tenant/id/spec. Never regenerate id after an unknown response.
    async fn create(&self, request: &CreateSandbox) -> Result<SandboxObservation, SandboxError>;
    async fn get(&self, tenant: &str, id: &str)
        -> Result<Option<SandboxObservation>, SandboxError>;
    /// Deleted is returned only with backend confirmation/fencing; absence alone may race a late create.
    async fn delete(&self, tenant: &str, id: &str) -> Result<SandboxObservation, SandboxError>;
}
