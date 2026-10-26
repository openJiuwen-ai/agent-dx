//! Composition bridge from Activator's local transport to the Environment application service.
//! Agent execution specifications are converted in the Activator crate.
use crate::{environment_service::EnvironmentService, http::instance_view, operations::Kind};
use adx_activator::local_environment::EnvironmentApplication;
use adx_agent_core::environment::EnvironmentError;
use adx_protocol::control as pb;
use async_trait::async_trait;
use serde_json::Value;
use tonic::{Code, Status};

#[async_trait]
impl EnvironmentApplication for EnvironmentService {
    async fn ensure_volume_request(
        &self,
        tenant: &str,
        input: Value,
    ) -> Result<Value, EnvironmentError> {
        Box::pin(self.volume_request(
            "POST",
            None,
            input,
            &std::collections::HashMap::new(),
            &caller(tenant),
        ))
        .await
        .map_err(map_volume_status)
    }
    async fn release_volume_owner(&self, tenant: &str, id: &str) -> Result<(), EnvironmentError> {
        EnvironmentService::release_volume_owner(self, id, &caller(tenant))
            .await
            .map_err(map_volume_status)
    }
    async fn create_request(
        &self,
        tenant: &str,
        input: Value,
        operation_id: &str,
    ) -> Result<Value, EnvironmentError> {
        Box::pin(EnvironmentService::create_request(
            self,
            input,
            operation_id,
            &caller(tenant),
        ))
        .await
        .map_err(|error| map_status(error, true))
    }
    async fn instance(&self, tenant: &str, id: &str) -> Result<Option<Value>, EnvironmentError> {
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
    ) -> Result<(), EnvironmentError> {
        self.execute(Kind::Delete, id, operation_id, input, &caller(tenant))
            .await
            .map_err(map_delete_status)?;
        Ok(())
    }
}
fn caller(tenant: &str) -> pb::CallerContext {
    pb::CallerContext {
        tenant_id: tenant.to_owned(),
        administrator: false,
    }
}
fn map_volume_status(status: Status) -> EnvironmentError {
    match status.code() {
        Code::PermissionDenied | Code::Unauthenticated => {
            EnvironmentError::Invalid("Volume request credentials rejected".into())
        }
        Code::NotFound => EnvironmentError::NotFound,
        _ => map_status(status, true),
    }
}

fn map_delete_status(status: Status) -> EnvironmentError {
    if status.code() == Code::NotFound {
        EnvironmentError::NotFound
    } else {
        map_status(status, true)
    }
}
fn map_status(status: Status, write: bool) -> EnvironmentError {
    match status.code() {
        Code::PermissionDenied if !write => EnvironmentError::NotFound,
        Code::PermissionDenied => {
            EnvironmentError::Unavailable("API Server permission denied".into())
        }
        Code::NotFound if !write => EnvironmentError::NotFound,
        Code::InvalidArgument => EnvironmentError::Invalid("API Server rejected request".into()),
        Code::AlreadyExists | Code::FailedPrecondition | Code::Aborted => {
            EnvironmentError::Conflict("Environment identity/specification conflict".into())
        }
        Code::Unimplemented => {
            EnvironmentError::Unsupported("Environment capability unavailable".into())
        }
        _ if write => EnvironmentError::OutcomeUnknown(
            "Environment write result is unknown; inspect the original ID".into(),
        ),
        _ => EnvironmentError::Unavailable("Environment read is unavailable".into()),
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn volume_refusals_are_terminal_and_transport_failures_remain_unknown() {
        for status in [
            Status::permission_denied("private"),
            Status::unauthenticated("private"),
        ] {
            assert!(matches!(
                map_volume_status(status),
                EnvironmentError::Invalid(_)
            ));
        }
        assert_eq!(
            map_volume_status(Status::not_found("absent")),
            EnvironmentError::NotFound
        );
        assert!(matches!(
            map_volume_status(Status::unavailable("lost reply")),
            EnvironmentError::OutcomeUnknown(_)
        ));
    }

    #[test]
    fn delete_absence_is_distinct_from_permission_and_unknown_outcomes() {
        assert_eq!(
            map_delete_status(Status::not_found("absent")),
            EnvironmentError::NotFound
        );
        assert!(matches!(
            map_delete_status(Status::permission_denied("private")),
            EnvironmentError::Unavailable(_)
        ));
        assert!(matches!(
            map_delete_status(Status::unavailable("lost response")),
            EnvironmentError::OutcomeUnknown(_)
        ));
    }
    #[test]
    fn missing_writes_and_private_reads_keep_distinct_errors() {
        assert_eq!(
            map_status(Status::permission_denied("private"), false),
            EnvironmentError::NotFound
        );
        assert!(matches!(
            map_status(Status::not_found("absent"), true),
            EnvironmentError::OutcomeUnknown(_)
        ));
        assert!(matches!(
            map_status(Status::unavailable("lost response"), true),
            EnvironmentError::OutcomeUnknown(_)
        ));
        assert!(matches!(
            map_status(Status::unavailable("read failed"), false),
            EnvironmentError::Unavailable(_)
        ));
    }
}
