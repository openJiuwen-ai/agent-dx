//! Agent-side adapter for a co-located Sandbox management application.
use crate::sandbox_request;
use adx_agent_core::{
    sandbox::*,
    transport::{capped_deadline, remaining_time},
};
use async_trait::async_trait;
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};

/// The public Sandbox protocol without HTTP framing. The embedding process
/// authenticates the caller before passing its tenant to this private interface.
#[async_trait]
pub trait SandboxApplication: Send + Sync {
    async fn create_request(
        &self,
        tenant: &str,
        input: Value,
        operation_id: &str,
    ) -> Result<Value, SandboxError>;
    async fn instance(&self, tenant: &str, id: &str) -> Result<Option<Value>, SandboxError>;
    async fn delete_request(
        &self,
        tenant: &str,
        id: &str,
        input: Value,
        operation_id: &str,
    ) -> Result<(), SandboxError>;
}

pub struct LocalSandbox {
    application: Arc<dyn SandboxApplication>,
    timeout: Duration,
}
impl LocalSandbox {
    pub fn new(
        application: Arc<dyn SandboxApplication>,
        timeout: Duration,
    ) -> Result<Self, SandboxError> {
        sandbox_request::validate_timeout(timeout)?;
        Ok(Self {
            application,
            timeout,
        })
    }
}
#[async_trait]
impl Sandbox for LocalSandbox {
    fn validate_execution(&self, execution: &ExecutionSpec) -> Result<(), SandboxError> {
        sandbox_request::validate_execution(execution)
    }
    async fn create(&self, request: &CreateSandbox) -> Result<SandboxObservation, SandboxError> {
        let input = sandbox_request::create_input(request, self.timeout)?;
        let operation_id = sandbox_request::operation_id("create", &request.tenant, &request.id)?;
        let remaining = remaining_time(capped_deadline(request.deadline_unix_ms, self.timeout));
        if remaining.is_zero() {
            return Err(SandboxError::Unavailable(
                "Sandbox deadline expired before submission".into(),
            ));
        }
        let value = tokio::time::timeout(
            remaining,
            Box::pin(
                self.application
                    .create_request(&request.tenant, input, &operation_id),
            ),
        )
        .await
        .map_err(|_| {
            SandboxError::OutcomeUnknown("Sandbox create timed out; query the original ID".into())
        })??;
        sandbox_request::created(&request.tenant, &request.id, &value)
    }
    async fn get(
        &self,
        tenant: &str,
        id: &str,
    ) -> Result<Option<SandboxObservation>, SandboxError> {
        sandbox_request::validate_identity(tenant)?;
        sandbox_request::validate_identity(id)?;
        let value = tokio::time::timeout(self.timeout, self.application.instance(tenant, id))
            .await
            .map_err(|_| SandboxError::Unavailable("Sandbox read timed out".into()))??;
        value
            .map(|value| sandbox_request::observed(tenant, id, &value))
            .transpose()
    }
    async fn delete(&self, tenant: &str, id: &str) -> Result<SandboxObservation, SandboxError> {
        let operation_id = sandbox_request::operation_id("delete", tenant, id)?;
        tokio::time::timeout(
            self.timeout,
            self.application.delete_request(
                tenant,
                id,
                json!({"timeoutSeconds":self.timeout.as_secs().max(1)}),
                &operation_id,
            ),
        )
        .await
        .map_err(|_| {
            SandboxError::OutcomeUnknown("Sandbox deletion timed out; query the original ID".into())
        })??;
        Ok(sandbox_request::observation(
            tenant,
            id,
            SandboxPhase::Deleted,
        ))
    }
}
