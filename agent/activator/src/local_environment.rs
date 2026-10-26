//! Agent-side adapter for a co-located Environment management application.
use crate::environment_request;
use adx_agent_core::network::SandboxNetworkPolicy;
use adx_agent_core::{
    environment::*,
    transport::{capped_deadline, remaining_time},
};
use async_trait::async_trait;
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};

/// The public Environment protocol without HTTP framing. The embedding process
/// authenticates the caller before passing its tenant to this private interface.
#[async_trait]
pub trait EnvironmentApplication: Send + Sync {
    async fn ensure_volume_request(
        &self,
        _tenant: &str,
        _input: Value,
    ) -> Result<Value, EnvironmentError> {
        Err(EnvironmentError::Unsupported(
            "Volume management unavailable".into(),
        ))
    }
    async fn release_volume_owner(&self, _tenant: &str, _id: &str) -> Result<(), EnvironmentError> {
        Ok(())
    }
    async fn create_request(
        &self,
        tenant: &str,
        input: Value,
        operation_id: &str,
    ) -> Result<Value, EnvironmentError>;
    async fn instance(&self, tenant: &str, id: &str) -> Result<Option<Value>, EnvironmentError>;
    async fn delete_request(
        &self,
        tenant: &str,
        id: &str,
        input: Value,
        operation_id: &str,
    ) -> Result<(), EnvironmentError>;
}

pub struct LocalEnvironment {
    application: Arc<dyn EnvironmentApplication>,
    timeout: Duration,
    network: SandboxNetworkPolicy,
}
impl LocalEnvironment {
    pub fn new(
        application: Arc<dyn EnvironmentApplication>,
        timeout: Duration,
        network: SandboxNetworkPolicy,
    ) -> Result<Self, EnvironmentError> {
        environment_request::validate_timeout(timeout)?;
        environment_request::network_input(&network)?;
        Ok(Self {
            application,
            timeout,
            network,
        })
    }
}
#[async_trait]
impl Environment for LocalEnvironment {
    async fn ensure_volume(
        &self,
        tenant: &str,
        owner: &str,
        spec: &adx_agent_core::volumes::VolumeSpec,
    ) -> Result<adx_agent_core::volumes::VolumeRef, EnvironmentError> {
        spec.validate().map_err(EnvironmentError::Invalid)?;
        let mut input = serde_json::to_value(spec)
            .map_err(|_| EnvironmentError::Invalid("invalid Volume definition".into()))?;
        input["owner_id"] = json!(owner);
        let value = tokio::time::timeout(
            self.timeout,
            self.application.ensure_volume_request(tenant, input),
        )
        .await
        .map_err(|_| {
            EnvironmentError::OutcomeUnknown("Volume ensure response timed out".into())
        })??;
        environment_request::volume_reference(&value, tenant, &spec.id)
    }
    async fn release_volumes(&self, tenant: &str, owner: &str) -> Result<(), EnvironmentError> {
        tokio::time::timeout(
            self.timeout,
            self.application.release_volume_owner(tenant, owner),
        )
        .await
        .map_err(|_| EnvironmentError::OutcomeUnknown("Volume owner cleanup timed out".into()))?
    }
    fn validate_execution(&self, execution: &ExecutionSpec) -> Result<(), EnvironmentError> {
        environment_request::validate_execution(execution)
    }
    async fn create(
        &self,
        request: &CreateEnvironment,
    ) -> Result<EnvironmentObservation, EnvironmentError> {
        let input = environment_request::create_input(request, self.timeout, &self.network)?;
        let operation_id =
            environment_request::operation_id("create", &request.tenant, &request.id)?;
        let remaining = remaining_time(capped_deadline(request.deadline_unix_ms, self.timeout));
        if remaining.is_zero() {
            return Err(EnvironmentError::Unavailable(
                "Environment deadline expired before submission".into(),
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
            EnvironmentError::OutcomeUnknown(
                "Environment create timed out; query the original ID".into(),
            )
        })??;
        environment_request::created(&request.tenant, &request.id, &value)
    }
    async fn get(
        &self,
        tenant: &str,
        id: &str,
    ) -> Result<Option<EnvironmentObservation>, EnvironmentError> {
        environment_request::validate_identity(tenant)?;
        environment_request::validate_identity(id)?;
        let value = tokio::time::timeout(self.timeout, self.application.instance(tenant, id))
            .await
            .map_err(|_| EnvironmentError::Unavailable("Environment read timed out".into()))??;
        value
            .map(|value| environment_request::observed(tenant, id, &value))
            .transpose()
    }
    async fn delete(
        &self,
        tenant: &str,
        id: &str,
    ) -> Result<EnvironmentObservation, EnvironmentError> {
        let operation_id = environment_request::operation_id("delete", tenant, id)?;
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
            EnvironmentError::OutcomeUnknown(
                "Environment deletion timed out; query the original ID".into(),
            )
        })??;
        Ok(environment_request::observation(
            tenant,
            id,
            EnvironmentPhase::Deleted,
        ))
    }
}
