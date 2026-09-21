//! RRT HTTP readiness after sandboxd has returned the runtime address.
use crate::runtime_control::RuntimeControlClient;
use crate::{Readiness, RuntimeDriver};
use adx_core::runtime::RuntimePhase;
use adx_core::{CapsuleRecord, Error, Result};
use async_trait::async_trait;
use std::{sync::Arc, time::Duration};

pub struct RrtReadiness {
    runtime: Arc<dyn RuntimeDriver>,
    client: RuntimeControlClient,
    poll_interval: Duration,
    probe_timeout: Duration,
    ready_timeout: Duration,
}

impl RrtReadiness {
    pub fn new(
        runtime: Arc<dyn RuntimeDriver>,
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
    async fn probe(&self, record: &CapsuleRecord) -> Result<()> {
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
    async fn activity(&self, record: &CapsuleRecord) -> Result<(u64, u64)> {
        let status = self.client.status(record).await?;
        if status.phase != RuntimePhase::Running || status.activity_revision == 0 {
            return Err(unavailable("runtime activity observation is not ready"));
        }
        Ok((
            status.activity_revision,
            status
                .active_requests
                .saturating_add(status.active_commands),
        ))
    }
    async fn wait_ready(&self, record: &CapsuleRecord) -> Result<()> {
        tokio::time::timeout(self.ready_timeout, async {
            loop {
                let probe = tokio::time::timeout(self.probe_timeout, async {
                    if !self.runtime.is_running(&record.runtime.id).await? {
                        return Err(unavailable("runtime is not running"));
                    }
                    self.probe(record).await?;
                    if !self.runtime.is_running(&record.runtime.id).await? {
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
