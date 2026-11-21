//! Agent-side adapter for a co-located Environment management application.
use crate::environment_request;
use adx_agent_core::network::PlatformEgress;
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
    async fn update_snapshot_reference_request(
        &self,
        _tenant: &str,
        _id: &str,
        _change: &adx_agent_core::environment::SnapshotChange,
    ) -> Result<(), EnvironmentError> {
        Err(EnvironmentError::Unsupported(
            "snapshot reference management unavailable".into(),
        ))
    }
    async fn prepare_snapshot_request(
        &self,
        _tenant: &str,
        _id: &str,
        _input: Value,
    ) -> Result<adx_agent_core::prewarm::SeedResult, EnvironmentError> {
        Err(EnvironmentError::Unsupported(
            "snapshot preparation unavailable".into(),
        ))
    }
    async fn snapshot_metadata(
        &self,
        _tenant: &str,
        _id: &str,
    ) -> Result<Option<Value>, EnvironmentError> {
        Ok(None)
    }
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
    /// Pause a Running Environment. The embedding application owns durable publication;
    /// a returned value must already reflect a committed Paused state. `operation_id`
    /// carries the deterministic `pause-activator-<sha256>` identity for replay.
    ///
    /// This is a required method, not an opt-in capability: `LocalEnvironment::pause` always
    /// routes here for an embedded deployment, and the upgrade fork pauses the candidate
    /// before every switch when the previous Environment is dormant. A missing implementation
    /// therefore fails at compile time rather than degrading to a permanently blocked
    /// reconciliation. Contrast `Environment::pause`, whose default `Unsupported` body remains
    /// the opt-out for the many `impl Environment` test fakes that never reach the fork.
    async fn pause_request(
        &self,
        tenant: &str,
        id: &str,
        input: Value,
        operation_id: &str,
    ) -> Result<Value, EnvironmentError>;
}

pub struct LocalEnvironment {
    application: Arc<dyn EnvironmentApplication>,
    timeout: Duration,
    platform_egress: Option<PlatformEgress>,
}
impl LocalEnvironment {
    pub fn new(
        application: Arc<dyn EnvironmentApplication>,
        timeout: Duration,
        platform_egress: Option<PlatformEgress>,
    ) -> Result<Self, EnvironmentError> {
        environment_request::validate_timeout(timeout)?;
        environment_request::validate_platform_egress(platform_egress.as_ref())?;
        Ok(Self {
            application,
            timeout,
            platform_egress,
        })
    }
}
#[async_trait]
impl Environment for LocalEnvironment {
    async fn update_snapshot_reference(
        &self,
        tenant: &str,
        id: &str,
        change: &adx_agent_core::environment::SnapshotChange,
    ) -> Result<(), EnvironmentError> {
        self.application
            .update_snapshot_reference_request(tenant, id, change)
            .await
    }

    fn freeze_preparation(
        &self,
        tenant: &str,
        id: &str,
        seed: &adx_agent_core::prewarm::SeedExecution,
    ) -> Result<Option<Value>, EnvironmentError> {
        environment_request::seed_input(
            tenant,
            id,
            seed,
            self.timeout,
            self.platform_egress.as_ref(),
        )
        .map(Some)
    }

    async fn prepare_snapshot(
        &self,
        tenant: &str,
        id: &str,
        seed: &adx_agent_core::prewarm::SeedExecution,
        deadline_unix_ms: u64,
    ) -> Result<adx_agent_core::prewarm::SeedResult, EnvironmentError> {
        let mut input = environment_request::seed_input(
            tenant,
            id,
            seed,
            self.timeout,
            self.platform_egress.as_ref(),
        )?;
        input["registration_deadline_unix_ms"] = serde_json::json!(deadline_unix_ms);
        tokio::time::timeout(
            self.timeout,
            self.application.prepare_snapshot_request(tenant, id, input),
        )
        .await
        .map_err(|_| {
            EnvironmentError::OutcomeUnknown("snapshot preparation result unknown".into())
        })?
    }
    async fn snapshot_revision(&self, tenant: &str, id: &str) -> Result<u64, EnvironmentError> {
        self.application
            .snapshot_metadata(tenant, id)
            .await?
            .ok_or(EnvironmentError::NotFound)?
            .get("revision")
            .and_then(Value::as_u64)
            .ok_or_else(|| EnvironmentError::Unavailable("snapshot revision unavailable".into()))
    }
    async fn golden_available(
        &self,
        tenant: &str,
        id: &str,
        image: &str,
        runtime: &str,
    ) -> Result<bool, EnvironmentError> {
        let metadata =
            tokio::time::timeout(self.timeout, self.application.snapshot_metadata(tenant, id))
                .await
                .map_err(|_| {
                    EnvironmentError::Unavailable("snapshot metadata timed out".into())
                })??;
        let value = metadata
            .ok_or_else(|| EnvironmentError::Unavailable("snapshot metadata unavailable".into()))?;
        environment_request::check_golden(&value, id, image, runtime)
    }
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
        let input = environment_request::create_input(
            request,
            self.timeout,
            self.platform_egress.as_ref(),
        )?;
        let operation_id = environment_request::creation_request_id(request)?;
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
    async fn pause(
        &self,
        tenant: &str,
        id: &str,
        timeout_seconds: u64,
    ) -> Result<EnvironmentObservation, EnvironmentError> {
        environment_request::validate_identity(tenant)?;
        environment_request::validate_identity(id)?;
        if !(1..=3600).contains(&timeout_seconds) {
            return Err(EnvironmentError::Invalid(
                "pause timeout_seconds out of API bounds 1..=3600".into(),
            ));
        }
        let operation_id = environment_request::pause_operation_id(tenant, id)?;
        let input = environment_request::pause_input(timeout_seconds);
        let value = tokio::time::timeout(
            Duration::from_secs(timeout_seconds),
            self.application
                .pause_request(tenant, id, input, &operation_id),
        )
        .await
        .map_err(|_| {
            EnvironmentError::OutcomeUnknown(
                "Environment pause timed out; query the original ID".into(),
            )
        })??;
        environment_request::paused(tenant, id, &value)
    }
}
