//! Runtime-originated recovery points; serialized with all lifecycle work.
use super::Controller;
use crate::Durability;
use adx_core::{runtime::RuntimePhase, Error, Event, RestorePoint, Result};
use std::{
    future::Future,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

const DEFAULT_BACKEND_CHECKPOINT_TIMEOUT: Duration = Duration::from_secs(600);

impl Controller {
    /// Reconciliation must not replay a capture whose metadata never committed.
    pub(super) async fn reconcile_workload_checkpoint(&mut self) -> Result<()> {
        let Some(services) = self.services.checkpoint.clone() else {
            return Ok(());
        };
        let Some(status) = services.cooperation.workload_status(&self.record).await? else {
            return Ok(());
        };
        if status.identity != crate::runtime_control::RuntimeControlClient::identity(&self.record) {
            return Err(Error::Conflict);
        }
        let Some(id) = status.requested_checkpoint else {
            return Ok(());
        };
        let committed = self
            .record
            .checkpoint
            .as_ref()
            .is_some_and(|cp| cp.id == id && cp.source_runtime_id == self.record.runtime.id);
        let capture_started = status.phase != RuntimePhase::Running
            || status
                .checkpoint
                .as_ref()
                .is_some_and(|cp| cp.operation_id == id);
        if capture_started && !committed {
            // Best effort reply; backend cleanup must also work when EXECD is unavailable.
            let _ = services
                .cooperation
                .finish_workload(
                    &self.record,
                    &id,
                    Some("uncommitted checkpoint cannot be recovered".into()),
                )
                .await;
            self.cleanup().await?;
            self.transition(Event::Fail)?;
        }
        Ok(())
    }

    pub(super) async fn workload_checkpoint(&mut self) -> Result<bool> {
        let Some(services) = self.services.checkpoint.clone() else {
            return Ok(false);
        };
        let status = services.cooperation.workload_status(&self.record).await?;
        let Some(status) = status else {
            return Ok(false);
        };
        let Some(id) = status.requested_checkpoint else {
            return Ok(false);
        };
        self.idle = None;
        if status.identity != crate::runtime_control::RuntimeControlClient::identity(&self.record) {
            return Err(Error::Conflict);
        }
        let deadline = match checkpoint_deadline(status.requested_checkpoint_deadline_unix_millis) {
            Ok(deadline) => deadline,
            Err(error) => {
                services
                    .cooperation
                    .finish_workload(&self.record, &id, Some(error.to_string()))
                    .await?;
                return Err(error);
            }
        };
        // A lost commit/ACK response must not execute the backend a second time.
        if self
            .record
            .checkpoint
            .as_ref()
            .is_some_and(|cp| cp.id == id && cp.source_runtime_id == self.record.runtime.id)
        {
            let result = before_deadline(deadline, "state publication", self.sync()).await?;
            if result.durability != Durability::Published {
                return Err(Error::Unavailable("checkpoint publication pending".into()));
            }
            before_deadline(
                deadline,
                "runtime acknowledgement",
                services
                    .cooperation
                    .finish_workload(&self.record, &id, None),
            )
            .await?;
            return Ok(true);
        }
        if status.phase != RuntimePhase::Running
            || status
                .checkpoint
                .as_ref()
                .is_some_and(|cp| cp.operation_id == id)
        {
            // Recovered controller cannot infer success from an uncommitted artifact,
            // nor repeat a possibly in-flight backend operation.
            services
                .cooperation
                .finish_workload(
                    &self.record,
                    &id,
                    Some("uncommitted checkpoint cannot be recovered".into()),
                )
                .await?;
            self.cleanup().await?;
            self.transition(Event::Fail)?;
            self.sync().await?;
            return Err(Error::Unavailable(
                "uncommitted checkpoint execution retired".into(),
            ));
        }
        if let Err(error) = before_deadline(
            deadline,
            "runtime capability check",
            self.services
                .runtime
                .checkpoint_supported(&self.record.spec.runtime_class),
        )
        .await
        {
            services
                .cooperation
                .finish_workload(&self.record, &id, Some(error.to_string()))
                .await?;
            return Err(error);
        }
        let staged =
            match before_deadline(deadline, "checkpoint allocation", services.store.allocate())
                .await
            {
                Ok(path) => path,
                Err(error) => {
                    services
                        .cooperation
                        .finish_workload(&self.record, &id, Some(error.to_string()))
                        .await?;
                    return Err(error);
                }
            };
        if let Err(error) = before_deadline(
            deadline,
            "runtime preparation",
            services.cooperation.prepare(&self.record, &id),
        )
        .await
        {
            let abort = services
                .cooperation
                .abort_unstarted(&self.record, &id)
                .await;
            let ack = services
                .cooperation
                .finish_workload(&self.record, &id, Some(error.to_string()))
                .await;
            services.store.discard_staged(&staged).await?;
            if abort.is_err() {
                self.cleanup().await?;
                self.transition(Event::Fail)?;
                self.sync().await?;
            }
            ack?;
            return Err(error);
        }
        // Only this operation uses leave_running=true. It never retires routes,
        // releases admission, changes execution identity, or creates a reusable snapshot.
        let backend_timeout = remaining_backend_timeout(deadline)?;
        let capture = before_deadline(
            deadline,
            "sandboxd checkpoint",
            self.services.runtime.checkpoint_running(
                &self.record.runtime.id,
                &staged,
                backend_timeout,
            ),
        )
        .await;
        let capture = match capture {
            Ok(()) => {
                before_deadline(
                    deadline,
                    "checkpoint handoff",
                    services.cooperation.resumed(&self.record, &id),
                )
                .await
            }
            Err(error) => Err(error),
        };
        if let Err(error) = capture {
            // Unknown backend outcomes do not authorize re-execution or a success ACK.
            let _ = services
                .cooperation
                .finish_workload(&self.record, &id, Some(error.to_string()))
                .await;
            self.cleanup().await?;
            self.transition(Event::Fail)?;
            self.sync().await?;
            services.store.discard_staged(&staged).await?;
            return Err(error);
        }
        let artifact = match before_deadline(
            deadline,
            "checkpoint publication",
            services.store.publish(&staged),
        )
        .await
        {
            Ok(artifact) => artifact,
            Err(error) => {
                services
                    .cooperation
                    .finish_workload(&self.record, &id, Some(error.to_string()))
                    .await?;
                services.store.discard_staged(&staged).await?;
                return Err(error);
            }
        };
        if let Some(previous) = self.record.checkpoint.take() {
            self.obsolete_checkpoints.push(previous.artifact);
        }
        self.record.checkpoint = Some(RestorePoint {
            id: id.clone(),
            artifact,
            origin: None,
            source_runtime_id: self.record.runtime.id.clone(),
            // Anonymous recovery points follow their Environment's lifecycle.
            expires_at_unix_seconds: u64::MAX,
        });
        self.record.revision = self.record.revision.checked_add(1).ok_or(Error::Conflict)?;
        self.record.last_operation = None;
        self.durability = None;
        let result = before_deadline(deadline, "state publication", self.sync()).await?;
        if result.durability != Durability::Published {
            return Err(Error::Unavailable("checkpoint publication pending".into()));
        }
        before_deadline(
            deadline,
            "runtime acknowledgement",
            services
                .cooperation
                .finish_workload(&self.record, &id, None),
        )
        .await?;
        Ok(true)
    }
}

fn checkpoint_deadline(deadline_unix_millis: Option<u64>) -> Result<Option<tokio::time::Instant>> {
    let Some(deadline_unix_millis) = deadline_unix_millis else {
        return Ok(None);
    };
    let now_millis: u64 = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Error::Unavailable("system clock before epoch".into()))?
        .as_millis()
        .try_into()
        .map_err(|_| Error::Unavailable("system clock overflow".into()))?;
    let remaining = deadline_unix_millis
        .checked_sub(now_millis)
        .filter(|remaining| *remaining > 0)
        .ok_or_else(|| Error::Unavailable("workload checkpoint timed out".into()))?;
    Ok(Some(
        tokio::time::Instant::now() + Duration::from_millis(remaining),
    ))
}

fn remaining_backend_timeout(deadline: Option<tokio::time::Instant>) -> Result<Duration> {
    let Some(deadline) = deadline else {
        return Ok(DEFAULT_BACKEND_CHECKPOINT_TIMEOUT);
    };
    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
    if remaining.is_zero() {
        return Err(Error::Unavailable("workload checkpoint timed out".into()));
    }
    let seconds = remaining
        .as_secs()
        .checked_add(u64::from(remaining.subsec_nanos() != 0))
        .ok_or_else(|| Error::Invalid("checkpoint timeout overflow".into()))?;
    Ok(Duration::from_secs(seconds.max(1)))
}

async fn before_deadline<T>(
    deadline: Option<tokio::time::Instant>,
    stage: &str,
    operation: impl Future<Output = Result<T>>,
) -> Result<T> {
    match deadline {
        Some(deadline) => tokio::time::timeout_at(deadline, operation)
            .await
            .map_err(|_| {
                Error::Unavailable(format!("workload checkpoint timed out during {stage}"))
            })?,
        None => operation.await,
    }
}
