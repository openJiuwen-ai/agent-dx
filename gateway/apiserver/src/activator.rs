//! Composition bridge from Activator's local transport to the Sandbox application service.
//! Agent execution specifications are converted in the Activator crate.
use crate::{http::instance_view, operations::Kind, sandbox_service::SandboxService};
use adx_activator::local_sandbox::SandboxApplication;
use adx_agent_core::sandbox::SandboxError;
use adx_protocol::control as pb;
use async_trait::async_trait;
use serde_json::Value;
use tonic::{Code, Status};

#[async_trait]
impl SandboxApplication for SandboxService {
    async fn create_request(
        &self,
        tenant: &str,
        input: Value,
        operation_id: &str,
    ) -> Result<Value, SandboxError> {
        Box::pin(SandboxService::create_request(
            self,
            input,
            operation_id,
            &caller(tenant),
        ))
        .await
        .map_err(|error| map_status(error, true))
    }
    async fn instance(&self, tenant: &str, id: &str) -> Result<Option<Value>, SandboxError> {
        let Some(owner) = self
            .inspect(tenant, id)
            .await
            .map_err(|error| map_status(error, false))?
        else {
            return Ok(None);
        };
        match instance_view(owner) {
            Ok(value) => Ok(Some(value)),
            Err(error) if error.code() == Code::NotFound => Ok(None),
            Err(error) => Err(map_status(error, false)),
        }
    }
    async fn delete_request(
        &self,
        tenant: &str,
        id: &str,
        input: Value,
        operation_id: &str,
    ) -> Result<(), SandboxError> {
        self.execute(Kind::Delete, id, operation_id, input, &caller(tenant))
            .await
            .map_err(|error| map_status(error, true))?;
        Ok(())
    }
}
fn caller(tenant: &str) -> pb::CallerContext {
    pb::CallerContext {
        tenant_id: tenant.to_owned(),
        administrator: false,
    }
}
fn map_status(status: Status, write: bool) -> SandboxError {
    match status.code() {
        Code::PermissionDenied => SandboxError::NotFound,
        Code::NotFound if !write => SandboxError::NotFound,
        Code::InvalidArgument => SandboxError::Invalid("API Server rejected request".into()),
        Code::AlreadyExists | Code::FailedPrecondition | Code::Aborted => {
            SandboxError::Conflict("Sandbox identity/specification conflict".into())
        }
        Code::Unimplemented => SandboxError::Unsupported("Sandbox capability unavailable".into()),
        _ if write => SandboxError::OutcomeUnknown(
            "Sandbox write result is unknown; inspect the original ID".into(),
        ),
        _ => SandboxError::Unavailable("Sandbox read is unavailable".into()),
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn missing_writes_and_private_reads_keep_distinct_errors() {
        assert_eq!(
            map_status(Status::permission_denied("private"), false),
            SandboxError::NotFound
        );
        assert!(matches!(
            map_status(Status::not_found("absent"), true),
            SandboxError::OutcomeUnknown(_)
        ));
        assert!(matches!(
            map_status(Status::unavailable("lost response"), true),
            SandboxError::OutcomeUnknown(_)
        ));
        assert!(matches!(
            map_status(Status::unavailable("read failed"), false),
            SandboxError::Unavailable(_)
        ));
    }
}
