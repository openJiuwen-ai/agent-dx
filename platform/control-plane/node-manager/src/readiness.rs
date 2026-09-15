//! RRT HTTP readiness after sandboxd has returned the runtime address.
use crate::runtime_control::RuntimeControlClient;
use crate::{Readiness, RuntimeBackend};
use adx_core::runtime::RuntimePhase;
use adx_core::{Error, InstanceRecord, Result};
use async_trait::async_trait;
use std::{sync::Arc, time::Duration};

pub struct RrtReadiness {
    runtime: Arc<dyn RuntimeBackend>,
    client: RuntimeControlClient,
    poll_interval: Duration,
    probe_timeout: Duration,
    ready_timeout: Duration,
}

impl RrtReadiness {
    pub fn new(
        runtime: Arc<dyn RuntimeBackend>,
        port: u16,
        poll_interval: Duration,
        probe_timeout: Duration,
        ready_timeout: Duration,
    ) -> Result<Self> {
        if port == 0
            || poll_interval.is_zero()
            || probe_timeout.is_zero()
            || ready_timeout.is_zero()
        {
            return Err(Error::Invalid(
                "RRT port and readiness durations must be positive".into(),
            ));
        }
        let client = RuntimeControlClient::new(port, probe_timeout)?;
        Ok(Self {
            runtime,
            client,
            poll_interval,
            probe_timeout,
            ready_timeout,
        })
    }

    pub fn with_token(mut self, token: &str) -> Result<Self> {
        self.client = self.client.with_token(token)?;
        Ok(self)
    }
    async fn probe(&self, record: &InstanceRecord) -> Result<()> {
        if self.client.status(record).await?.phase != RuntimePhase::Running {
            return Err(unavailable("runtime control is not running"));
        }
        Ok(())
    }
}
fn unavailable(error: impl std::fmt::Display) -> Error {
    Error::Unavailable(format!("RRT readiness: {error}"))
}

#[async_trait]
impl Readiness for RrtReadiness {
    async fn wait_ready(&self, record: &InstanceRecord) -> Result<()> {
        tokio::time::timeout(self.ready_timeout, async {
            loop {
                let probe = tokio::time::timeout(self.probe_timeout, async {
                    if !self.runtime.is_running(&record.runtime_id).await? {
                        return Err(unavailable("runtime is not running"));
                    }
                    self.probe(record).await?;
                    if !self.runtime.is_running(&record.runtime_id).await? {
                        return Err(unavailable("runtime exited during readiness"));
                    }
                    Ok(())
                })
                .await;
                if matches!(probe, Ok(Ok(()))) {
                    return Ok(());
                }
                tokio::time::sleep(self.poll_interval).await;
            }
        })
        .await
        .map_err(|_| unavailable("deadline exceeded"))?
    }
}
