//! In-process Activator adapter over the API Server Sandbox application service.
use crate::{operations::Kind, sandbox_service::SandboxService as ApplicationService};
use adx_agent_core::sandbox::{
    CreateSandbox, ExecutionSpec, Sandbox as SandboxServiceContract, SandboxError,
    SandboxObservation,
};
use adx_protocol::control as pb;
use async_trait::async_trait;
use data_plane_gateway::ingress::sandbox_api::EnvironmentRequestMapper;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tonic::{Code, Status};

/// Shares API Server lifecycle state with an embedded Activator without HTTP or RPC loopback.
pub struct ActivatorSandboxAdapter {
    service: Arc<ApplicationService>,
    mapper: EnvironmentRequestMapper,
}

impl ActivatorSandboxAdapter {
    pub fn new(service: Arc<ApplicationService>, mapper: EnvironmentRequestMapper) -> Arc<Self> {
        Arc::new(Self { service, mapper })
    }

    async fn observe(
        &self,
        tenant: &str,
        id: &str,
    ) -> Result<Option<SandboxObservation>, SandboxError> {
        let Some(owner) = self
            .service
            .inspect(tenant, id)
            .await
            .map_err(|error| map_status(error, false))?
        else {
            return Ok(None);
        };
        let observation = EnvironmentRequestMapper::observation(
            owner
                .record
                .ok_or_else(|| SandboxError::Unavailable("Environment record missing".into()))?,
        )?;
        if observation.id != id || observation.tenant != tenant {
            return Err(SandboxError::Unavailable(
                "Environment response identity mismatch".into(),
            ));
        }
        Ok(Some(observation))
    }
}

#[async_trait]
impl SandboxServiceContract for ActivatorSandboxAdapter {
    fn validate_execution(&self, execution: &ExecutionSpec) -> Result<(), SandboxError> {
        self.mapper.validate_execution(execution)
    }

    async fn create(&self, request: &CreateSandbox) -> Result<SandboxObservation, SandboxError> {
        let spec = self.mapper.environment_spec(request)?;
        let caller = caller(&request.tenant);
        // The caller's deadline changes on retry; it is not part of the create identity.
        let input = create_input(request);
        self.service
            .create(
                spec,
                input,
                &operation_id("create", &request.tenant, &request.id),
                &caller,
            )
            .await
            .map_err(|error| map_status(error, true))?;
        self.observe(&request.tenant, &request.id)
            .await?
            .ok_or_else(|| {
                SandboxError::OutcomeUnknown(
                    "Sandbox create completed without a visible Environment record".into(),
                )
            })
    }

    async fn get(
        &self,
        tenant: &str,
        id: &str,
    ) -> Result<Option<SandboxObservation>, SandboxError> {
        self.observe(tenant, id).await
    }

    async fn delete(&self, tenant: &str, id: &str) -> Result<SandboxObservation, SandboxError> {
        self.service
            .execute(
                Kind::Delete,
                id,
                &operation_id("delete", tenant, id),
                json!({}),
                &caller(tenant),
            )
            .await
            .map_err(|error| map_status(error, true))?;
        self.observe(tenant, id).await?.ok_or_else(|| {
            SandboxError::OutcomeUnknown(
                "Sandbox deletion completed without a visible Environment tombstone".into(),
            )
        })
    }
}

fn caller(tenant: &str) -> pb::CallerContext {
    pb::CallerContext {
        tenant_id: tenant.to_owned(),
        administrator: false,
    }
}

fn create_input(request: &CreateSandbox) -> serde_json::Value {
    json!({
        "id": request.id,
        "tenant": request.tenant,
        "execution": request.execution,
    })
}

fn operation_id(kind: &str, tenant: &str, id: &str) -> String {
    let digest = Sha256::digest(format!("{tenant}\0{id}"));
    format!("activator-{kind}-{digest:x}")
}

fn map_status(status: Status, write: bool) -> SandboxError {
    match status.code() {
        Code::NotFound | Code::PermissionDenied => SandboxError::NotFound,
        Code::InvalidArgument => SandboxError::Invalid(status.message().into()),
        Code::AlreadyExists | Code::FailedPrecondition | Code::Aborted => {
            SandboxError::Conflict(status.message().into())
        }
        Code::Unimplemented => SandboxError::Unsupported(status.message().into()),
        _ if write => SandboxError::OutcomeUnknown(
            "Environment write result is unknown; inspect the original Sandbox ID".into(),
        ),
        _ => SandboxError::Unavailable("Environment read is unavailable".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operation_identity_is_stable_and_separates_actions() {
        let create = operation_id("create", "tenant", "sandbox");
        assert_eq!(create, operation_id("create", "tenant", "sandbox"));
        assert_ne!(create, operation_id("delete", "tenant", "sandbox"));
        assert_ne!(create, operation_id("create", "tenant-2", "sandbox"));
    }

    #[test]
    fn uncertain_writes_and_private_reads_keep_distinct_errors() {
        assert_eq!(
            map_status(Status::permission_denied("private"), false),
            SandboxError::NotFound
        );
        assert!(matches!(
            map_status(Status::unavailable("lost response"), true),
            SandboxError::OutcomeUnknown(_)
        ));
        assert!(matches!(
            map_status(Status::unavailable("read failed"), false),
            SandboxError::Unavailable(_)
        ));
    }

    #[test]
    fn changed_retry_deadline_does_not_change_create_identity() {
        let mut request = CreateSandbox {
            id: "sandbox".into(),
            tenant: "tenant".into(),
            execution: serde_json::from_value(json!({
                "image": "test:1",
                "isolation_runtime": "runc",
                "entrypoint": ["/start"],
                "working_dir": "/",
                "user": null,
                "env": {},
                "resources": {"cpu_millis": 1000, "memory_mib": 512},
                "service": []
            }))
            .unwrap(),
            deadline_unix_ms: Some(100),
        };
        let first = create_input(&request);
        request.deadline_unix_ms = Some(200);
        assert_eq!(create_input(&request), first);
    }
}
