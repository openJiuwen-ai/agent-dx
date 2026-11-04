//! Execd Ready notification, or HTTP readiness when no stream is configured.
use crate::runtime_control::RuntimeControlClient;
use crate::{Readiness, RuntimeDriver};
use adx_core::runtime::RuntimePhase;
use adx_core::{EnvironmentRecord, Error, Result};
use async_trait::async_trait;
use std::{sync::Arc, time::Duration};

pub struct ExecdReadiness {
    runtime: Arc<dyn RuntimeDriver>,
    client: RuntimeControlClient,
    poll_interval: Duration,
    probe_timeout: Duration,
    ready_timeout: Duration,
}

impl ExecdReadiness {
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
                "EXECD port and readiness durations must be positive".into(),
            ));
        }
        let client = RuntimeControlClient::new(port, probe_timeout)?;
        Self::with_client(runtime, client, poll_interval, probe_timeout, ready_timeout)
    }

    pub fn with_client(
        runtime: Arc<dyn RuntimeDriver>,
        client: RuntimeControlClient,
        poll_interval: Duration,
        probe_timeout: Duration,
        ready_timeout: Duration,
    ) -> Result<Self> {
        if poll_interval.is_zero() || probe_timeout.is_zero() || ready_timeout.is_zero() {
            return Err(Error::Invalid(
                "EXECD readiness durations must be positive".into(),
            ));
        }
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
    async fn probe(&self, record: &EnvironmentRecord) -> Result<()> {
        let status = self.client.probe_status(record).await?;
        self.validate_status(record, &status)
    }
    fn validate_status(
        &self,
        record: &EnvironmentRecord,
        status: &adx_core::runtime::RuntimeStatus,
    ) -> Result<()> {
        if let Some(failure) = &status.failure {
            return Err(Error::ProbeFailed(failure.clone()));
        }
        if status.phase != RuntimePhase::Running {
            return Err(unavailable("runtime control is not running"));
        }
        status.validate_required_services(record.spec.required_services())?;
        Ok(())
    }
}
fn unavailable(error: impl std::fmt::Display) -> Error {
    Error::Unavailable(format!("EXECD readiness: {error}"))
}

#[async_trait]
impl Readiness for ExecdReadiness {
    async fn probe_failure(
        &self,
        record: &EnvironmentRecord,
    ) -> Result<Option<adx_core::probes::ProbeFailure>> {
        Ok(self.client.probe_status(record).await?.failure)
    }
    fn expect_runtime(&self, record: &EnvironmentRecord) -> Result<()> {
        self.client.expect_runtime(record)
    }
    async fn activity(&self, record: &EnvironmentRecord) -> Result<(u64, u64)> {
        let status = self.client.status(record).await?;
        self.validate_status(record, &status)?;
        if status.activity_revision == 0 {
            return Err(unavailable("runtime activity observation is not ready"));
        }
        // A detached background command belongs to the runtime, not to a live
        // client. Its start still advances activity_revision, but it must not
        // indefinitely prevent an otherwise idle capsule from being reaped.
        Ok((status.activity_revision, status.active_requests))
    }
    async fn required_service_health(&self, record: &EnvironmentRecord) -> Result<()> {
        match self.client.status(record).await {
            Ok(status)
                if record.spec.required_services().next().is_some()
                    || !status.required_services.is_empty() =>
            {
                self.validate_status(record, &status)
            }
            Ok(_) => Ok(()),
            // No configured service means an unavailable observation alone cannot
            // establish a mandatory-service failure for a legacy runtime.
            Err(_) if record.spec.required_services().next().is_none() => Ok(()),
            Err(error) => Err(error),
        }
    }
    async fn wait_ready(&self, record: &EnvironmentRecord) -> Result<()> {
        // Execd enforces the probe deadline from `init_timeout_seconds`. Wait a
        // little longer here so the terminal probe failure notification wins the
        // race; otherwise this side reports a retryable DeadlineExceeded first.
        let base_timeout = if record.spec.execution.init_timeout_seconds > 0 {
            Duration::from_secs(record.spec.execution.init_timeout_seconds)
        } else {
            self.ready_timeout
        };
        let ready_timeout = base_timeout + Duration::from_secs(5);
        if self.client.uses_stream() {
            let ready = tokio::time::timeout(ready_timeout, async {
                let status = self
                    .client
                    .wait_ready_notification(record)
                    .await?
                    .ok_or_else(|| unavailable("Ready notification omitted runtime status"))?;
                self.validate_status(record, &status)?;
                if !self.runtime.is_running(&record.runtime.id).await? {
                    return Err(unavailable("runtime exited before Ready"));
                }
                Ok(())
            })
            .await;
            return match ready {
                Ok(result) => result,
                Err(_) => readiness_timeout(self, record).await,
            };
        }
        let polling = async {
            loop {
                let probe = tokio::time::timeout(self.probe_timeout, async {
                    self.probe(record).await?;
                    if !self.runtime.is_running(&record.runtime.id).await? {
                        return Err(unavailable("runtime exited during readiness"));
                    }
                    Ok(())
                })
                .await;
                if let Ok(Err(error @ Error::ProbeFailed(_))) = &probe {
                    return Err(error.clone());
                }
                if matches!(probe, Ok(Ok(()))) {
                    return Ok(());
                }
                tokio::time::sleep(self.poll_interval).await;
            }
        };
        match tokio::time::timeout(ready_timeout, polling).await {
            Ok(result) => result,
            Err(_) => readiness_timeout(self, record).await,
        }
    }
}

/// After the ready deadline, prefer the terminal probe failure Execd reported
/// over a generic retryable deadline. This keeps probe failures terminal even
/// if the notification arrived just after the local timeout fired.
async fn readiness_timeout(
    readiness: &ExecdReadiness,
    record: &EnvironmentRecord,
) -> Result<()> {
    match readiness.probe_failure(record).await {
        Ok(Some(failure)) => Err(Error::ProbeFailed(failure)),
        _ => Err(Error::DeadlineExceeded(
            "Execd readiness deadline exceeded".into(),
        )),
    }
}
