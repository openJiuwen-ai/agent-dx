//! Runtime-originated recovery points; serialized with all lifecycle work.
use super::Controller;
use crate::Durability;
use adx_core::{runtime::RuntimePhase, Error, Event, RestorePoint, Result};
use std::time::Duration;

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
        // A lost commit/ACK response must not execute the backend a second time.
        if self
            .record
            .checkpoint
            .as_ref()
            .is_some_and(|cp| cp.id == id && cp.source_runtime_id == self.record.runtime.id)
        {
            let result = self.sync().await?;
            if result.durability != Durability::Published {
                return Err(Error::Unavailable("checkpoint publication pending".into()));
            }
            services
                .cooperation
                .finish_workload(&self.record, &id, None)
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
        if let Err(error) = self
            .services
            .runtime
            .checkpoint_supported(&self.record.spec.runtime_class)
            .await
        {
            services
                .cooperation
                .finish_workload(&self.record, &id, Some(error.to_string()))
                .await?;
            return Err(error);
        }
        let staged = match services.store.allocate().await {
            Ok(path) => path,
            Err(error) => {
                services
                    .cooperation
                    .finish_workload(&self.record, &id, Some(error.to_string()))
                    .await?;
                return Err(error);
            }
        };
        if let Err(error) = services.cooperation.prepare(&self.record, &id).await {
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
        let capture = self
            .services
            .runtime
            .checkpoint_running(&self.record.runtime.id, &staged, Duration::from_secs(300))
            .await;
        let capture = match capture {
            Ok(()) => tokio::time::timeout(
                self.services.operation_timeout,
                services.cooperation.resumed(&self.record, &id),
            )
            .await
            .map_err(|_| Error::Unavailable("checkpoint handoff timed out".into()))
            .and_then(|r| r),
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
        let artifact = match services.store.publish(&staged).await {
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
        let result = self.sync().await?;
        if result.durability != Durability::Published {
            return Err(Error::Unavailable("checkpoint publication pending".into()));
        }
        services
            .cooperation
            .finish_workload(&self.record, &id, None)
            .await?;
        Ok(true)
    }
}
