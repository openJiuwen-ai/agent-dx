use super::*;
use crate::checkpoint::{now, validate_operation};
use adx_core::{CompletedOperation, LifecycleKind, RestorePoint};
use std::{path::Path, time::Duration};

impl Controller {
    pub(super) async fn expire_checkpoint(&mut self, now: u64) -> Result<()> {
        let Some(cp) = self
            .record
            .checkpoint
            .clone()
            .filter(|cp| cp.expires_at_unix_seconds <= now)
        else {
            return Ok(());
        };
        if self.record.state == InstanceState::Paused {
            self.delete().await?;
            return Ok(());
        }
        if self.record.state != InstanceState::Running && self.record.state != InstanceState::Failed
        {
            return Ok(());
        }
        // An expired restore point is no longer usable, but the live execution
        // can still read its state/memory files. Persist that reference until stop.
        if self.held && self.recovery_files.is_some() {
            return Ok(());
        }
        self.obsolete_checkpoints.push(cp.artifact);
        self.record.checkpoint = None;
        self.record.last_operation = None;
        self.record.revision = self.record.revision.checked_add(1).ok_or(Error::Conflict)?;
        self.durability = None;
        self.sync().await?;
        Ok(())
    }

    pub(super) fn replay_operation(
        &self,
        id: &str,
        revision: u64,
        kind: LifecycleKind,
    ) -> Result<bool> {
        validate_operation(id, revision)?;
        if self
            .record
            .last_operation
            .as_ref()
            .is_some_and(|op| op.id == id && op.kind == kind && op.expected_revision == revision)
        {
            return Ok(true);
        }
        if self.record.revision != revision {
            return Err(Error::Conflict);
        }
        Ok(false)
    }
    pub(super) fn completed(&mut self, id: String, expected_revision: u64, kind: LifecycleKind) {
        self.record.last_operation = Some(CompletedOperation {
            id,
            expected_revision,
            kind,
        });
    }
    fn release_capacity(&mut self) -> Result<()> {
        if self.held {
            self.services
                .admission
                .lock()
                .expect("shared state lock poisoned")
                .release(&self.record.runtime_id)?;
            self.held = false;
            self.record.resources_held = false;
        }
        Ok(())
    }
    pub(super) async fn pause(&mut self, request: PauseRequest) -> Result<OperationResult> {
        if self.replay_operation(
            &request.operation_id,
            request.expected_revision,
            LifecycleKind::Pause,
        )? {
            return self.replay_or_sync().await;
        }
        if self.record.state != InstanceState::Running {
            return Err(Error::Conflict);
        }
        if request.ttl_seconds == 0
            || request.timeout_seconds == 0
            || request.timeout_seconds > 3600
        {
            return Err(Error::Invalid(
                "positive checkpoint TTL and timeout (at most 3600 seconds) required".into(),
            ));
        }
        let expires_at = now()?
            .checked_add(request.ttl_seconds)
            .ok_or_else(|| Error::Invalid("checkpoint TTL overflow".into()))?;
        let services = self
            .services
            .checkpoint
            .clone()
            .ok_or_else(|| Error::Invalid("checkpoint storage is not configured".into()))?;
        self.services
            .runtime
            .checkpoint_supported(&self.record.spec.runtime)
            .await?;
        let staged = services.store.allocate().await?;
        // Preparation errors can be ambiguous: abort is valid only before the backend call.
        if let Err(error) = services
            .cooperation
            .prepare(&self.record, &request.operation_id)
            .await
        {
            let abort = services
                .cooperation
                .abort_unstarted(&self.record, &request.operation_id)
                .await;
            services.store.discard_staged(&staged).await?;
            if let Err(abort) = abort {
                self.transition(Event::Fail)?;
                let retire = self.services.routes.retire(&self.record).await;
                let commit = self.sync().await;
                return Err(Error::Unavailable(format!("prepare failed: {error}; abort failed: {abort}; route retirement: {retire:?}; state commit: {}", if commit.is_ok() {"accepted"} else {"unavailable"})));
            }
            return Err(error);
        }
        self.transition(Event::Pause)?;
        if let Err(error) = self.services.routes.retire(&self.record).await {
            let abort = services
                .cooperation
                .abort_unstarted(&self.record, &request.operation_id)
                .await;
            services.store.discard_staged(&staged).await?;
            if let Err(abort) = abort {
                self.transition(Event::Fail)?;
                self.sync().await?;
                return Err(Error::Unavailable(format!(
                    "route retirement failed: {error}; abort failed: {abort}"
                )));
            }
            self.transition(Event::Rollback)?;
            if let Err(activate) = self.services.routes.activate(&self.record).await {
                self.transition(Event::Fail)?;
                self.sync().await?;
                return Err(activate);
            }
            self.sync().await?;
            return Err(error);
        }
        // This accepted call runs to settlement even if the HTTP caller disconnects.
        if let Err(error) = self
            .services
            .runtime
            .checkpoint(
                &self.record.runtime_id,
                &staged,
                Duration::from_secs(request.timeout_seconds),
            )
            .await
        {
            // The backend may have started checkpointing. Never tell RRT to cancel an
            // "unstarted" checkpoint here, and never advertise the execution as ready.
            let cleanup = self.cleanup().await;
            self.transition(Event::Fail)?;
            self.sync().await?;
            cleanup?;
            services.store.discard_staged(&staged).await?;
            return Err(error);
        }
        // Checkpoint success confirms the artifact, not completion of the backend's
        // asynchronous exit notification. Idempotent remove is the stop barrier.
        // Source deletion must be confirmed before releasing local resources.
        if let Err(error) = self.services.runtime.remove(&self.record.runtime_id).await {
            self.transition(Event::Fail)?;
            self.sync().await?;
            return Err(error);
        }
        self.recovery_files = None;
        self.release_capacity()?;
        self.record.runtime_ip = None;
        let artifact = match services.store.publish(&staged).await {
            Ok(artifact) => artifact,
            Err(error) => {
                // Staging contains the complete local recovery point, even if upload
                // failed. Restore it with a fresh execution identity before reporting failure.
                let artifact = match services.store.retain_staged(&staged).await {
                    Ok(artifact) => artifact,
                    Err(local_error) => {
                        self.transition(Event::Fail)?;
                        self.sync().await?;
                        return Err(local_error);
                    }
                };
                if let Some(old) = self.record.checkpoint.take() {
                    self.obsolete_checkpoints.push(old.artifact);
                }
                self.record.checkpoint = Some(RestorePoint {
                    origin: None,
                    id: request.operation_id.clone(),
                    artifact,
                    expires_at_unix_seconds: expires_at,
                    source_runtime_id: self.record.runtime_id.clone(),
                });
                self.transition(Event::Checkpointed)?;
                let artifact = &self
                    .record
                    .checkpoint
                    .as_ref()
                    .ok_or(Error::Conflict)?
                    .artifact;
                let path = services.store.materialize(artifact).await?;
                let rollback = self.restore_execution(&path).await;
                if self.held {
                    self.recovery_files = Some(path);
                }
                if rollback.is_err() && self.record.state != InstanceState::Failed {
                    self.transition(Event::Fail)?;
                }
                self.sync().await?;
                rollback?;
                return Err(error);
            }
        };
        if let Some(previous) = self.record.checkpoint.take() {
            self.obsolete_checkpoints.push(previous.artifact);
        }
        self.record.checkpoint = Some(RestorePoint {
            origin: None,
            id: request.operation_id.clone(),
            artifact,
            expires_at_unix_seconds: expires_at,
            source_runtime_id: self.record.runtime_id.clone(),
        });
        self.transition(Event::Checkpointed)?;
        self.completed(
            request.operation_id,
            request.expected_revision,
            LifecycleKind::Pause,
        );
        self.sync().await
    }
    async fn restore_execution(&mut self, path: &Path) -> Result<()> {
        let next_revision = self.record.revision.checked_add(1).ok_or(Error::Conflict)?;
        let runtime_id = format!(
            "{}-{}-r{}",
            self.record.spec.id, self.record.assignment.generation, next_revision
        );
        self.services
            .admission
            .lock()
            .expect("shared state lock poisoned")
            .reserve(&runtime_id, &self.record.spec, &self.record.assignment)?;
        self.record.runtime_id = runtime_id;
        self.held = true;
        self.record.resources_held = true;
        self.transition(Event::Resume)?;
        let attempt = async {
            self.record.runtime_ip = Some(
                self.services
                    .runtime
                    .restore_from(
                        &self.record.spec,
                        &self.record.runtime_id,
                        self.record.assignment.generation,
                        &self.record.assignment.devices,
                        path,
                        self.record
                            .checkpoint
                            .as_ref()
                            .and_then(|cp| cp.origin.as_ref()),
                    )
                    .await?,
            );
            self.services.readiness.wait_ready(&self.record).await?;
            self.services.routes.activate(&self.record).await
        }
        .await;
        if let Err(error) = attempt {
            match self.cleanup().await {
                Ok(()) => {
                    self.record.runtime_ip = None;
                    self.transition(Event::Rollback)?;
                }
                Err(cleanup) => {
                    self.transition(Event::Fail)?;
                    return Err(Error::Unavailable(format!(
                        "restore failed: {error}; cleanup failed: {cleanup}"
                    )));
                }
            }
            return Err(error);
        }
        self.transition(Event::Ready)?;
        Ok(())
    }
    pub(super) async fn recover(&mut self, request: ResumeRequest) -> Result<OperationResult> {
        if matches!(
            self.record.state,
            InstanceState::Failed | InstanceState::Deleted
        ) {
            return self.replay_or_sync().await;
        }
        let result = self.resume(request).await;
        if let Err(error) = &result {
            // Paused plus no local reservation proves this attempt did not leave
            // an execution. A Running result whose publication failed is retried
            // through the operation record, never by starting another runtime.
            if self.record.state == InstanceState::Paused
                && !self.held
                && !self.record.resources_held
            {
                eprintln!("checkpoint recovery failed: {error}");
                self.transition(Event::Fail)?;
                return self.sync().await;
            }
        }
        result
    }
    pub(super) async fn resume(&mut self, request: ResumeRequest) -> Result<OperationResult> {
        if self.replay_operation(
            &request.operation_id,
            request.expected_revision,
            LifecycleKind::Resume,
        )? {
            return self.replay_or_sync().await;
        }
        if self.record.state != InstanceState::Paused {
            return Err(Error::Conflict);
        }
        let cp = self.record.checkpoint.clone().ok_or(Error::Conflict)?;
        if cp.expires_at_unix_seconds <= now()? {
            return Err(Error::Invalid("checkpoint expired".into()));
        }
        let services = self.services.checkpoint.clone().ok_or(Error::Conflict)?;
        let path = services.store.materialize(&cp.artifact).await?;
        let restored = self.restore_execution(&path).await;
        if self.held {
            self.recovery_files = Some(path);
        }
        if let Err(error) = restored {
            if let Err(commit) = self.sync().await {
                return Err(Error::Unavailable(format!(
                    "{error}; result commit failed: {commit}"
                )));
            }
            return Err(error);
        }
        // Keep the artifact through commit: a lost result must never leave Redis
        // pointing at a deleted recovery point. Expiry/delete performs later cleanup.
        self.completed(
            request.operation_id,
            request.expected_revision,
            LifecycleKind::Resume,
        );
        self.sync().await
    }
}
