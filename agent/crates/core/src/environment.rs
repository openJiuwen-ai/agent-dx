//! General Environment capability boundary shared by Gateway adapters and Activator clients.
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
    /// Managed Agent execution inherits the image process. Internal runtime profiles may use explicit startup.
    #[serde(default)]
    pub inherit_entrypoint: bool,
    /// Empty for image inheritance or an idle environment.
    pub entrypoint: Vec<String>,
    pub working_dir: String,
    pub user: Option<String>,
    pub env: BTreeMap<String, String>,
    pub resources: Resources,
    #[serde(
        default,
        skip_serializing_if = "crate::lifecycle::LifecyclePolicy::is_default"
    )]
    pub lifecycle: crate::lifecycle::LifecyclePolicy,
    pub service: Vec<Service>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub mounts: Vec<crate::mounts::Mount>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub init_timeout_seconds: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub readiness_probe: Option<crate::probes::ReadinessProbe>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub healthcheck: Option<crate::probes::Healthcheck>,
}
impl From<&TemplateVersion> for ExecutionSpec {
    fn from(value: &TemplateVersion) -> Self {
        Self {
            image: value.image.clone(),
            isolation_runtime: value.isolation_runtime.clone(),
            inherit_entrypoint: true,
            entrypoint: vec![],
            working_dir: String::new(),
            user: None,
            env: value.env.clone(),
            resources: value.resources.clone(),
            lifecycle: value.lifecycle.clone(),
            service: value.service.clone(),
            mounts: value.mounts.clone(),
            init_timeout_seconds: value.init_timeout_seconds,
            readiness_probe: value.readiness_probe.clone(),
            healthcheck: value.healthcheck.clone(),
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
            env: self.env.clone(),
            resources: self.resources.clone(),
            lifecycle: self.lifecycle.clone(),
            service: self.service.clone(),
            mounts: self.mounts.clone(),
            volumes: vec![],
            init_timeout_seconds: self.init_timeout_seconds,
            readiness_probe: self.readiness_probe.clone(),
            healthcheck: self.healthcheck.clone(),
        };
        template.validate()?;
        for mount in &self.mounts {
            mount.validate()?;
        }
        if self.inherit_entrypoint {
            if !self.entrypoint.is_empty() || !self.working_dir.is_empty() || self.user.is_some() {
                return Err("image process inheritance cannot include startup overrides".into());
            }
        } else {
            if self
                .entrypoint
                .first()
                .is_some_and(|arg| arg.trim().is_empty())
                || self.entrypoint.iter().any(|arg| arg.contains('\0'))
            {
                return Err("entrypoint requires a nonempty executable and NUL-free argv".into());
            }
            if !self.working_dir.starts_with('/')
                || self.working_dir.contains('\0')
                || self.working_dir.split('/').any(|part| part == "..")
            {
                return Err(
                    "working_dir must be an absolute environment path without parent traversal"
                        .into(),
                );
            }
        }

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
pub struct CreateEnvironment {
    pub id: String,
    pub tenant: String,
    pub execution: ExecutionSpec,
    /// Request metadata, not part of environment identity or execution specification.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline_unix_ms: Option<u64>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EnvironmentPhase {
    Creating,
    Running,
    Pausing,
    Paused,
    Resuming,
    Failed,
    Deleted,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvironmentObservation {
    pub id: String,
    pub tenant: String,
    pub phase: EnvironmentPhase,
    pub ready: bool,
    pub runtime_id: Option<String>,
    pub message: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "message", rename_all = "snake_case")]
pub enum EnvironmentError {
    /// Absent from the caller's scope. On delete, completes binding cleanup, not a physical-deletion guarantee.
    NotFound,
    Invalid(String),
    Unsupported(String),
    Conflict(String),
    Unavailable(String),
    OutcomeUnknown(String),
}
impl std::fmt::Display for EnvironmentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for EnvironmentError {}

/// Platform-owned description; optional fields are unavailable, never synthesized state.
#[derive(Debug, Clone)]
pub struct EnvironmentInfo {
    pub observation: EnvironmentObservation,
    pub node_ip: Option<String>,
    pub environment_ip: Option<String>,
    pub execution: Option<ExecutionSpec>,
}
#[async_trait]
pub trait Environment: Send + Sync {
    async fn ensure_volume(
        &self,
        _tenant: &str,
        _owner_id: &str,
        _spec: &crate::volumes::VolumeSpec,
    ) -> Result<crate::volumes::VolumeRef, EnvironmentError> {
        Err(EnvironmentError::Unsupported(
            "Volume management unavailable".into(),
        ))
    }
    /// Called after confirmed deletion/absence; implementations release only this lifecycle's associations.
    async fn release_volumes(
        &self,
        _tenant: &str,
        _owner_id: &str,
    ) -> Result<(), EnvironmentError> {
        Ok(())
    }
    async fn describe(
        &self,
        tenant: &str,
        id: &str,
    ) -> Result<Option<EnvironmentInfo>, EnvironmentError> {
        Ok(self
            .get(tenant, id)
            .await?
            .map(|observation| EnvironmentInfo {
                observation,
                node_ip: None,
                environment_ip: None,
                execution: None,
            }))
    }
    /// A fresh, tenant-filtered Platform directory; no ADX instance index is maintained.
    async fn list(&self, _tenant: &str) -> Result<Vec<EnvironmentInfo>, EnvironmentError> {
        Err(EnvironmentError::Unsupported(
            "Environment directory unavailable".into(),
        ))
    }
    /// Local admission validation only; implementations may reject deployment
    /// capabilities before any state or execution is created.
    fn validate_execution(&self, execution: &ExecutionSpec) -> Result<(), EnvironmentError> {
        execution.validate().map_err(EnvironmentError::Invalid)
    }
    /// Must be idempotent for a stable tenant/id/spec. Never regenerate id after an unknown response.
    async fn create(
        &self,
        request: &CreateEnvironment,
    ) -> Result<EnvironmentObservation, EnvironmentError>;
    async fn get(
        &self,
        tenant: &str,
        id: &str,
    ) -> Result<Option<EnvironmentObservation>, EnvironmentError>;
    /// Deleted is returned only with backend confirmation/fencing; absence alone may race a late create.
    /// Explicit NotFound permits binding cleanup; authentication and transport failures must
    /// use other errors so they cannot be mistaken for an absent environment.
    async fn delete(
        &self,
        tenant: &str,
        id: &str,
    ) -> Result<EnvironmentObservation, EnvironmentError>;
}
