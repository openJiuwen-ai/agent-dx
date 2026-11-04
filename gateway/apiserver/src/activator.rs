//! Composition bridge from Activator's local transport to the Environment application service.
//! Agent execution specifications are converted in the Activator crate.
use crate::{environment_service::EnvironmentService, http::instance_view, operations::Kind};
use adx_activator::local_environment::EnvironmentApplication;
use adx_agent_core::environment::EnvironmentError;
use adx_protocol::control as pb;
use async_trait::async_trait;
use serde_json::Value;
use std::sync::{atomic::Ordering, Arc};
use tonic::{Code, Status};

/// Counts one in-process embedded Activator operation for the shutdown
/// completion criterion: these run inside Ingress connection tasks, invisible
/// to the management HTTP connection count, so the host must wait for the
/// gauge separately ("management HTTP at zero ≠ activations finished").
struct EmbeddedOperationGuard(Arc<std::sync::atomic::AtomicUsize>);

impl EmbeddedOperationGuard {
    fn new(counter: Arc<std::sync::atomic::AtomicUsize>) -> Self {
        counter.fetch_add(1, Ordering::Relaxed);
        Self(counter)
    }
}

impl Drop for EmbeddedOperationGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

#[async_trait]
impl EnvironmentApplication for EnvironmentService {
    async fn ensure_volume_request(
        &self,
        tenant: &str,
        input: Value,
    ) -> Result<Value, EnvironmentError> {
        let _operation = EmbeddedOperationGuard::new(self.embedded_operations.clone());
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
        let _operation = EmbeddedOperationGuard::new(self.embedded_operations.clone());
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
        let _operation = EmbeddedOperationGuard::new(self.embedded_operations.clone());
        Box::pin(EnvironmentService::create_request(
            self,
            input,
            operation_id,
            &caller(tenant),
        ))
        .await
        .map_err(map_create_status)
    }
    async fn instance(&self, tenant: &str, id: &str) -> Result<Option<Value>, EnvironmentError> {
        let _operation = EmbeddedOperationGuard::new(self.embedded_operations.clone());
        let Some(owner) = self
            .inspect(tenant, id)
            .await
            .map_err(|error| map_status(error, false))?
        else {
            return Ok(None);
        };
        match instance_view(owner, false) {
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
        let _operation = EmbeddedOperationGuard::new(self.embedded_operations.clone());
        self.execute(Kind::Delete, id, operation_id, input, &caller(tenant))
            .await
            .map_err(map_delete_status)?;
        Ok(())
    }
    async fn pause_request(
        &self,
        tenant: &str,
        id: &str,
        input: Value,
        operation_id: &str,
    ) -> Result<Value, EnvironmentError> {
        self.execute(Kind::Pause, id, operation_id, input, &caller(tenant))
            .await
            .map_err(map_pause_status)
    }
}
fn caller(tenant: &str) -> pb::CallerContext {
    pb::CallerContext {
        tenant_id: tenant.to_owned(),
        administrator: false,
    }
}
fn map_create_status(status: Status) -> EnvironmentError {
    if status.code() == Code::FailedPrecondition
        && status.message() == adx_error::CREATE_TERMINAL_MESSAGE
    {
        return EnvironmentError::Conflict(status.message().into());
    }
    match adx_protocol::scheduling_failure(&status) {
        Some(failure) => EnvironmentError::SchedulingCancelled(Box::new(failure)),
        None => map_status(status, true),
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
/// A pause is a write: a missing target is `OutcomeUnknown` (re-query the original ID)
/// rather than confirmed absence, so the orchestrator converges on the next pass.
/// Conflict surfaces idle/replay races; `Unsupported` blocks rather than degrading.
fn map_pause_status(status: Status) -> EnvironmentError {
    map_status(status, true)
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
    fn only_the_explicit_terminal_create_reason_survives_the_embedded_adapter() {
        let message = adx_error::CREATE_TERMINAL_MESSAGE;
        assert_eq!(
            map_create_status(Status::failed_precondition(message)),
            EnvironmentError::Conflict(message.into())
        );
        for status in [
            Status::already_exists(message),
            Status::aborted(message),
            Status::failed_precondition("environment specification conflict"),
        ] {
            assert_eq!(
                map_create_status(status),
                EnvironmentError::Conflict("Environment identity/specification conflict".into())
            );
        }
        assert_eq!(
            map_status(Status::failed_precondition(message), false),
            EnvironmentError::Conflict("Environment identity/specification conflict".into()),
            "only CREATE carries the terminal creation signal"
        );
    }
    #[test]
    fn only_confirmed_queue_cancellation_authorizes_another_creation_attempt() {
        let failure = adx_core::scheduling::SchedulingFailure {
            rejections: [("waiting_for_volume".into(), 1)].into(),
            ..Default::default()
        };
        assert_eq!(
            map_create_status(adx_protocol::scheduling_deadline(failure.clone())),
            EnvironmentError::SchedulingCancelled(Box::new(failure))
        );
        for status in [
            Status::deadline_exceeded("timeout"),
            Status::unavailable("lost response"),
        ] {
            assert!(matches!(
                map_create_status(status),
                EnvironmentError::OutcomeUnknown(_)
            ));
        }
        assert!(matches!(
            map_create_status(adx_protocol::status(adx_core::Error::DeadlineExceeded(
                "runtime start failed".into()
            ))),
            EnvironmentError::OutcomeUnknown(_)
        ));
    }

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
    fn pause_failures_keep_write_semantics_without_confirmed_absence() {
        // A missing pause target is unknown, not confirmed absence, so the orchestrator
        // re-queries the original ID and converges on the next pass.
        assert!(matches!(
            map_pause_status(Status::not_found("missing")),
            EnvironmentError::OutcomeUnknown(_)
        ));
        // Idle/replay races surface as Conflict for convergence, and an incapable
        // deployment blocks as Unsupported rather than degrading to Running.
        assert!(matches!(
            map_pause_status(Status::failed_precondition("idle race")),
            EnvironmentError::Conflict(_)
        ));
        assert!(matches!(
            map_pause_status(Status::unimplemented("no pause")),
            EnvironmentError::Unsupported(_)
        ));
        assert!(matches!(
            map_pause_status(Status::unavailable("lost response")),
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
