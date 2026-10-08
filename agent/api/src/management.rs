//! Inline protocol adaptation only. Sandbox owns all lifecycle state.
use crate::{Error, Result};
use adx_agent_core::{
    inline::{CreateRequest, Mount, SandboxType},
    sandbox::*,
    *,
};
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Duration};
use tokio::sync::Semaphore;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InlineProfile {
    pub sandbox_type: SandboxType,
    /// Exact legacy image spelling. None matches only omitted rootfs, never an arbitrary image.
    pub request_image: Option<String>,
    pub image: String,
    pub isolation_runtime: String,
    pub request_user: Option<String>,
    pub working_dir: String,
    #[serde(default)]
    pub default_entrypoint: Vec<String>,
    #[serde(default)]
    pub service: Vec<Service>,
    #[serde(default)]
    pub preinstalled_workspace: Option<String>,
    #[serde(default)]
    pub preinstalled_mounts: Vec<Mount>,
}
impl InlineProfile {
    fn matches(&self, r: &CreateRequest) -> bool {
        self.sandbox_type == r.runtime_spec.sandbox_type
            && self.request_image.as_deref()
                == r.runtime_spec.rootfs.as_ref().map(|v| v.imageurl.as_str())
            && self.request_user.as_ref()
                == r.runtime_spec.rootfs.as_ref().and_then(|v| v.user.as_ref())
            && r.runtime_spec
                .rootfs
                .as_ref()
                .and_then(|rootfs| rootfs.workdir.as_deref())
                .filter(|v| !v.is_empty())
                .is_none_or(|cwd| cwd == self.working_dir)
            && self.preinstalled_workspace == r.workspace
            && self.preinstalled_mounts == r.mounts
    }
    fn execution(&self, r: &CreateRequest) -> ExecutionSpec {
        ExecutionSpec {
            image: self.image.clone(),
            isolation_runtime: self.isolation_runtime.clone(),
            entrypoint: r
                .runtime_spec
                .cmds
                .first()
                .cloned()
                .unwrap_or_else(|| self.default_entrypoint.clone()),
            working_dir: self.working_dir.clone(),
            user: self.request_user.clone(),
            env: r.env_vars.clone(),
            resources: r.resources(),
            service: self.service.clone(),
        }
    }
}
#[derive(Debug, Clone)]
pub struct Options {
    pub profiles: Vec<InlineProfile>,
    pub backend_timeout: Duration,
    pub max_inflight: usize,
}
pub struct InlineService {
    sandbox: Arc<dyn Sandbox>,
    options: Options,
    budget: Semaphore,
}
#[derive(Debug, Serialize)]
pub struct Created {
    pub code: u16,
    pub instance_id: String,
}
#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum InlinePhase {
    Creating,
    Ready,
    Failed,
}
#[derive(Debug, Serialize)]
pub struct Detail {
    pub instance_id: String,
    pub status_code: i32,
    pub status: String,
    pub phase: InlinePhase,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status_msg: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sandbox_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_ip: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sandbox_ip: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sandbox_type: Option<SandboxType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rootfs: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub env_vars: Option<std::collections::BTreeMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resources: Option<serde_json::Value>,
}
impl InlineService {
    pub fn new(sandbox: Arc<dyn Sandbox>, options: Options) -> Result<Self> {
        if options.profiles.is_empty()
            || options.backend_timeout.is_zero()
            || options.backend_timeout >= limits::AGENT_REQUEST_TIMEOUT
            || !(1..=256).contains(&options.max_inflight)
        {
            return Err(Error::Invalid(
                "inline profiles, backend deadline below the Agent deadline and 1..256 backend slots required".into(),
            ));
        }
        for p in &options.profiles {
            if p.sandbox_type == SandboxType::Docker && p.request_image.as_ref() != Some(&p.image) {
                return Err(Error::Invalid(
                    "Docker inline profiles must preserve the requested image".into(),
                ));
            }
            let spec = ExecutionSpec {
                image: p.image.clone(),
                isolation_runtime: p.isolation_runtime.clone(),
                entrypoint: p.default_entrypoint.clone(),
                working_dir: p.working_dir.clone(),
                user: p.request_user.clone(),
                env: Default::default(),
                resources: Resources {
                    cpu_millis: 1,
                    memory_mib: 1,
                },
                service: p.service.clone(),
            };
            sandbox.validate_execution(&spec)?;
        }
        Ok(Self {
            sandbox,
            budget: Semaphore::new(options.max_inflight),
            options,
        })
    }
    fn validate_identity(tenant: &str, id: &str) -> Result<()> {
        identifier(tenant, "tenant").map_err(Error::Invalid)?;
        uuid::Uuid::parse_str(id)
            .map_err(|_| Error::Invalid("invalid inline Sandbox ID".into()))?;
        Ok(())
    }
    fn validate_observation(tenant: &str, id: &str, observed: &SandboxObservation) -> Result<()> {
        if observed.id != id || observed.tenant != tenant {
            return Err(Error::Unavailable(
                "Sandbox response identity mismatch".into(),
            ));
        }
        Ok(())
    }
    fn slot(&self) -> Result<tokio::sync::SemaphorePermit<'_>> {
        self.budget
            .try_acquire()
            .map_err(|_| Error::Unavailable("inline Sandbox capacity busy".into()))
    }
    pub async fn create(
        &self,
        ctx: &crate::request::RequestContext,
        tenant: &str,
        request: CreateRequest,
    ) -> Result<Created> {
        identifier(tenant, "tenant").map_err(Error::Invalid)?;
        request.validate().map_err(Error::Invalid)?;
        if request
            .runtime_spec
            .probes
            .as_ref()
            .is_some_and(|p| p.startup.is_some() || p.liveness.is_some())
        {
            return Err(Error::Unsupported("Platform user startup/liveness probes are unavailable; no ADX recovery substitute is installed".into()));
        }
        let matches: Vec<_> = self
            .options
            .profiles
            .iter()
            .filter(|p| p.matches(&request))
            .collect();
        if matches.len() != 1 {
            return Err(Error::Unsupported("exactly one deployment profile must match inline sandbox type, image, process user and preinstalled filesystem intent".into()));
        }
        let profile = matches[0];
        let execution = profile.execution(&request);
        self.sandbox.validate_execution(&execution)?;
        let _slot = self.slot()?;
        // The public UUID is the actual Sandbox ID. No ADX alias or durable admission exists.
        let canonical = format!(
            "yuanrong-agent-instance:v1|{}:{}{}:{}",
            request.namespace.len(),
            request.namespace,
            request.name.len(),
            request.name
        );
        let id = uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_URL, canonical.as_bytes()).to_string();
        let deadline =
            transport::capped_deadline(Some(ctx.deadline_unix_ms()), self.options.backend_timeout);
        let remaining = transport::remaining_time(deadline);
        if remaining.is_zero() {
            return Err(Error::Unavailable(
                "Sandbox create deadline expired before submission".into(),
            ));
        }
        let request = CreateSandbox {
            deadline_unix_ms: Some(deadline),
            id: id.clone(),
            tenant: tenant.into(),
            execution,
        };
        let observed = tokio::time::timeout(remaining, self.sandbox.create(&request))
            .await
            .map_err(|_| {
                Error::OutcomeUnknown(format!(
                    "Sandbox create timed out for instance {id}; inspect this ID"
                ))
            })?
            .map_err(|error| match error {
                SandboxError::OutcomeUnknown(message) => {
                    Error::OutcomeUnknown(format!("instance {id}: {message}"))
                }
                other => other.into(),
            })?;
        Self::validate_observation(tenant, &id, &observed).map_err(|_| {
            Error::OutcomeUnknown(format!(
                "Sandbox create returned mismatched identity for instance {id}"
            ))
        })?;
        if matches!(observed.phase, SandboxPhase::Failed | SandboxPhase::Deleted) {
            return Err(Error::Unavailable(format!(
                "Sandbox create failed for instance {id}: {}",
                observed.message.unwrap_or_default()
            )));
        }
        Ok(Created {
            code: 200,
            instance_id: id,
        })
    }
    pub async fn get(&self, tenant: &str, id: &str) -> Result<Detail> {
        Self::validate_identity(tenant, id)?;
        let _slot = self.slot()?;
        let observed = tokio::time::timeout(
            self.options.backend_timeout,
            self.sandbox.describe(tenant, id),
        )
        .await
        .map_err(|_| Error::Unavailable("Sandbox query timed out".into()))??
        .ok_or(Error::NotFound)?;
        let info = observed;
        let observed = info.observation;
        Self::validate_observation(tenant, id, &observed)?;
        let profile = info.execution.as_ref().and_then(|execution| {
            self.options.profiles.iter().find(|profile| {
                profile.image == execution.image
                    && profile.isolation_runtime == execution.isolation_runtime
                    && profile.working_dir == execution.working_dir
                    && profile.request_user == execution.user
            })
        });
        let (status_code, status, phase) = match observed.phase {
            SandboxPhase::Creating => (2, "CREATING", InlinePhase::Creating),
            SandboxPhase::Running if observed.ready => (3, "RUNNING", InlinePhase::Ready),
            SandboxPhase::Running => (2, "CREATING", InlinePhase::Creating),
            SandboxPhase::Failed => (4, "FAILED", InlinePhase::Failed),
            SandboxPhase::Deleted => return Err(Error::NotFound),
        };
        Ok(Detail {
            instance_id: id.into(),
            status_code,
            status: status.into(),
            phase,
            status_msg: observed.message,
            sandbox_id: observed.runtime_id,
            node_ip: info.node_ip, sandbox_ip: info.sandbox_ip,
            sandbox_type: profile.map(|p| p.sandbox_type.clone()),
            rootfs: info.execution.as_ref().map(|execution| serde_json::json!({"imageurl":execution.image,"workdir":execution.working_dir,"user":execution.user})),
            env_vars: info.execution.as_ref().map(|execution| execution.env.clone()),
            resources: info.execution.as_ref().map(|execution| serde_json::json!({"CPU":execution.resources.cpu_millis,"Memory":execution.resources.memory_mib})),
        })
    }
    pub async fn list(&self, tenant: &str) -> Result<Vec<serde_json::Value>> {
        identifier(tenant, "tenant").map_err(Error::Invalid)?;
        let _slot = self.slot()?;
        let values = self.sandbox.list(tenant).await?;
        values.into_iter().map(|info| {
            Self::validate_observation(tenant, &info.observation.id, &info.observation)?;
            let profile = info.execution.as_ref().and_then(|execution| self.options.profiles.iter().find(|profile| profile.image == execution.image && profile.isolation_runtime == execution.isolation_runtime));
            Ok(serde_json::json!({"instance_id":info.observation.id,"node_ip":info.node_ip,"sandbox_ip":info.sandbox_ip,"sandbox_type":profile.map(|p| &p.sandbox_type)}))
        }).collect()
    }
    pub async fn runtime(&self, tenant: &str, id: &str) -> Result<SandboxRuntime> {
        Self::validate_identity(tenant, id)?;
        self.sandbox.runtime(tenant, id).await.map_err(Into::into)
    }
    pub async fn kill(&self, tenant: &str, id: &str) -> Result<()> {
        Self::validate_identity(tenant, id)?;
        let _slot = self.slot()?;
        let observed = tokio::time::timeout(
            self.options.backend_timeout,
            self.sandbox.delete(tenant, id),
        )
        .await
        .map_err(|_| {
            Error::OutcomeUnknown(format!("Sandbox delete timed out for instance {id}"))
        })??;
        Self::validate_observation(tenant, id, &observed).map_err(|_| {
            Error::OutcomeUnknown(format!(
                "Sandbox delete returned mismatched identity for instance {id}"
            ))
        })?;
        if observed.phase != SandboxPhase::Deleted {
            return Err(Error::OutcomeUnknown(format!(
                "Sandbox deletion not confirmed for instance {id}"
            )));
        }
        Ok(())
    }
}
