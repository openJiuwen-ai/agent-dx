//! Process membership leases; independent of AgentBinding cache TTL and Environment lifecycle.
use crate::{Error, Result};
use adx_agent_core::discovery::{ActivatorEndpoint, MemberCondition};
use adx_agent_store::discovery::RedisRegistry;
use serde::Deserialize;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegistrationConfig {
    pub instance_id: String,
    pub advertise_url: String,
    #[serde(default = "lease_seconds")]
    pub lease_seconds: u64,
    #[serde(default = "heartbeat_seconds")]
    pub heartbeat_seconds: u64,
}
fn lease_seconds() -> u64 {
    15
}
fn heartbeat_seconds() -> u64 {
    5
}

/// Source of the member condition written into every lease renewal. Lets the
/// owning process report its current ready/draining state without the lease
/// owning that state.
pub type ConditionSource = Arc<dyn Fn() -> MemberCondition + Send + Sync>;

pub struct RegistrationLease {
    registry: RedisRegistry,
    endpoint: ActivatorEndpoint,
    incarnation: String,
    condition: ConditionSource,
    task: Option<tokio::task::JoinHandle<()>>,
}
impl Drop for RegistrationLease {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}
impl RegistrationLease {
    /// Register after binding the listener. Reject invalid settings, duplicate live ids and
    /// unavailable Redis at startup; later renewal failures leave expiry to Redis server time.
    /// Every renewal (including the initial one) records the member condition
    /// reported by `condition`, so Gateway-side selection can filter
    /// not-ready/draining members.
    pub async fn start(
        registry: RedisRegistry,
        config: RegistrationConfig,
        allow_plaintext: bool,
        condition: ConditionSource,
    ) -> Result<Self> {
        if !(6..=300).contains(&config.lease_seconds)
            || config.heartbeat_seconds == 0
            || config.heartbeat_seconds > config.lease_seconds / 3
        {
            return Err(Error::Invalid(
                "registration requires lease_seconds 6..300 and heartbeat_seconds <= lease/3"
                    .into(),
            ));
        }
        let endpoint = ActivatorEndpoint {
            id: config.instance_id,
            url: config.advertise_url,
        };
        endpoint.validate(allow_plaintext).map_err(Error::Invalid)?;
        let incarnation = uuid::Uuid::new_v4().to_string();
        let lease = Duration::from_secs(config.lease_seconds);
        registry
            .renew(&endpoint, &incarnation, lease, condition())
            .await?;
        // Announce the new member so Ingress replicas re-read promptly instead
        // of waiting for the periodic refresh. Best-effort: periodic refresh is
        // the fallback if this is lost or the Ingress is an older version.
        if let Err(error) = registry.publish_changed().await {
            tracing::warn!(
                %error,
                instance_id = %endpoint.id,
                "Activator registration notify failed; periodic refresh will catch up"
            );
        }
        let heartbeat_registry = registry.clone();
        let heartbeat_endpoint = endpoint.clone();
        let heartbeat_incarnation = incarnation.clone();
        let heartbeat_condition = condition.clone();
        let task = tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(config.heartbeat_seconds)).await;
                if let Err(error) = heartbeat_registry
                    .renew(
                        &heartbeat_endpoint,
                        &heartbeat_incarnation,
                        lease,
                        heartbeat_condition(),
                    )
                    .await
                {
                    tracing::warn!(%error, instance_id = %heartbeat_endpoint.id, "Activator registration renewal failed");
                }
            }
        });
        Ok(Self {
            registry,
            endpoint,
            incarnation,
            condition,
            task: Some(task),
        })
    }
    /// Stop renewal before removing this incarnation so an in-flight renewal
    /// cannot resurrect it. Retry unregister within `budget` (the process
    /// shutdown timeout) so a transient Redis failure does not strand the
    /// member record for a full lease; on final failure the caller warns and
    /// continues, relying on lease expiry to remove the member.
    pub async fn shutdown(mut self, budget: Duration) -> Result<()> {
        if let Some(task) = self.task.take() {
            task.abort();
            let _ = task.await;
        }
        let deadline = tokio::time::Instant::now() + budget;
        let mut backoff = Duration::from_millis(200);
        loop {
            match self
                .registry
                .unregister(&self.endpoint.id, &self.incarnation)
                .await
            {
                Ok(()) => {
                    // Announce the removal so Ingress replicas stop selecting
                    // this instance promptly. Best-effort.
                    if let Err(error) = self.registry.publish_changed().await {
                        tracing::warn!(
                            %error,
                            instance_id = %self.endpoint.id,
                            "Activator unregister notify failed; periodic refresh will catch up"
                        );
                    }
                    return Ok(());
                }
                Err(error) => {
                    tracing::warn!(
                        %error,
                        instance_id = %self.endpoint.id,
                        "Activator unregister failed; retrying within shutdown budget"
                    );
                    let now = tokio::time::Instant::now();
                    let Some(remaining) = deadline.checked_duration_since(now) else {
                        return Err(error.into());
                    };
                    tokio::time::sleep(backoff.min(remaining)).await;
                    backoff = backoff.saturating_mul(2);
                }
            }
        }
    }
    /// The condition this lease currently reports. Test/observation hook.
    pub fn condition(&self) -> MemberCondition {
        (self.condition)()
    }
}
