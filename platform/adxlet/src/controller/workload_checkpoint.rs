//! Runtime-originated recovery points; serialized with all lifecycle work.
use super::Controller;
use crate::Durability;
use adx_core::{runtime::RuntimePhase, Error, Event, RestorePoint, Result};
use std::{
    future::Future,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

pub(super) const DEFAULT_BACKEND_CHECKPOINT_TIMEOUT: Duration = Duration::from_secs(600);

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
        let storage = self
            .record
            .spec
            .lifecycle
            .pause
            .clone()
            .unwrap_or_default()
            .storage;
        let Some(id) = status.requested_checkpoint else {
            return Ok(false);
        };
        if let Err(error) = services.store.validate_policy(storage) {
            services
                .cooperation
                .finish_workload(&self.record, &id, Some(error.to_string()))
                .await?;
            return Err(error);
        }
        self.idle = None;
        if status.identity != crate::runtime_control::RuntimeControlClient::identity(&self.record) {
            return Err(Error::Conflict);
        }
        let deadline = match checkpoint_deadline(status.requested_checkpoint_deadline_unix_millis)
            .and_then(|deadline| {
                let fixed = self.workload_deadline(&status.identity, &id);
                let deadline = match (deadline, fixed) {
                    (Some(a), Some(b)) => Some(a.min(b)),
                    (a, b) => a.or(b),
                };
                if deadline.is_some_and(|deadline| deadline <= tokio::time::Instant::now()) {
                    return Err(Error::Unavailable("workload checkpoint timed out".into()));
                }
                Ok(deadline)
            }) {
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
        let staged = match before_deadline(
            deadline,
            "checkpoint allocation",
            self.capture_directory(
                services.store.as_ref(),
                &id,
                self.capture_preparation
                    .as_ref()
                    .map_or(self.record.revision, |(op, _)| op.expected_revision),
                adx_core::LifecycleKind::Snapshot,
            ),
        )
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
        for reference in services
            .store
            .capture_references(&staged)?
            .into_iter()
            .filter(|a| {
                storage != adx_core::lifecycle::CheckpointStorage::Local || a.storage == "local"
            })
        {
            if !self.record.execution_artifacts.contains(&reference) {
                self.record.execution_artifacts.push(reference);
            }
        }
        self.record.revision = self.record.revision.checked_add(1).ok_or(Error::Conflict)?;
        let reserved = before_deadline(deadline, "capture input reservation", self.sync()).await?;
        if reserved.durability != Durability::Published {
            return Err(Error::Unavailable(
                "capture input reservation publication pending".into(),
            ));
        }
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
            self.capture_preparation = None;
            if abort.is_err() {
                self.cleanup().await?;
                self.transition(Event::Fail)?;
                self.sync().await?;
            }
            self.record
                .execution_artifacts
                .retain(|a| a.size_bytes != 0);
            self.record.revision = self.record.revision.checked_add(1).ok_or(Error::Conflict)?;
            self.sync().await?;
            ack?;
            return Err(error);
        }
        self.capture_preparation = None;
        // Runtime-originated capture uses leave_running=true. It never retires routes,
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
            if let Error::ProbeFailed(failure) = &error {
                self.record.failure = Some(failure.clone().into());
            }
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
        let retained = self
            .services
            .runtime
            .retains_capture_files(&self.record.spec.runtime_class);
        if retained {
            let original = services.store.retain_staged(&staged).await?;
            self.hold_artifact(
                original.clone(),
                services.store.prepare_restore(&original).await?,
            );
            self.record.revision = self.record.revision.checked_add(1).ok_or(Error::Conflict)?;
            self.sync().await?;
        }
        let artifact = match before_deadline(
            deadline,
            "checkpoint publication",
            services.store.publish_with_policy(&staged, storage),
        )
        .await
        {
            Ok(artifact) => artifact,
            Err(error) => {
                services
                    .cooperation
                    .finish_workload(&self.record, &id, Some(error.to_string()))
                    .await?;
                if !retained {
                    self.record
                        .execution_artifacts
                        .retain(|a| a.size_bytes != 0);
                    services.store.discard_staged(&staged).await?;
                }
                self.record.revision =
                    self.record.revision.checked_add(1).ok_or(Error::Conflict)?;
                self.sync().await?;
                return Err(error);
            }
        };
        // Firecracker keeps a running capture's memory as its next incremental
        // base. Pin the original local path before authoritative commit admits
        // it to the cache. runsc does not retain its capture inputs.
        if self
            .services
            .runtime
            .retains_capture_files(&self.record.spec.runtime_class)
        {
            self.hold_artifact(
                artifact.clone(),
                services.store.prepare_restore(&artifact).await?,
            );
        }
        self.record
            .execution_artifacts
            .retain(|a| a.size_bytes != 0);
        if let Some(previous) = self.record.checkpoint.take() {
            self.obsolete_checkpoints.push(previous.artifact);
        }
        self.record.snapshot_source = None;
        self.record.checkpoint = Some(RestorePoint {
            id: id.clone(),
            artifact,
            origin: None,
            source_runtime_id: self.record.runtime.id.clone(),
            // Anonymous recovery points follow their Environment's lifecycle.
            expires_at_unix_seconds: None,
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

pub(super) fn checkpoint_deadline(
    deadline_unix_millis: Option<u64>,
) -> Result<Option<tokio::time::Instant>> {
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
