//! Process membership leases; independent of Env cache TTL and Sandbox lifecycle.
use crate::{Error, Result};
use adx_agent_core::discovery::ActivatorEndpoint;
use adx_agent_store::discovery::RedisRegistry;
use serde::Deserialize;
use std::time::Duration;

#[derive(Deserialize)]
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

pub struct RegistrationLease {
    registry: RedisRegistry,
    endpoint: ActivatorEndpoint,
    incarnation: String,
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
    pub async fn start(
        registry: RedisRegistry,
        config: RegistrationConfig,
        allow_plaintext: bool,
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
        registry.renew(&endpoint, &incarnation, lease).await?;
        let heartbeat_registry = registry.clone();
        let heartbeat_endpoint = endpoint.clone();
        let heartbeat_incarnation = incarnation.clone();
        let task = tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(config.heartbeat_seconds)).await;
                if let Err(error) = heartbeat_registry
                    .renew(&heartbeat_endpoint, &heartbeat_incarnation, lease)
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
            task: Some(task),
        })
    }
    /// Stop renewal before removing this incarnation so an in-flight renewal cannot resurrect it.
    pub async fn shutdown(mut self) -> Result<()> {
        if let Some(task) = self.task.take() {
            task.abort();
            let _ = task.await;
        }
        self.registry
            .unregister(&self.endpoint.id, &self.incarnation)
            .await?;
        Ok(())
    }
}
