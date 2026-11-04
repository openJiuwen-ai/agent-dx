use super::*;
use crate::checkpoint::{SnapshotRequest, SnapshotResult};
use adx_core::{
    snapshots::{snapshot_id, Snapshot, SnapshotState},
    LifecycleKind,
};
use std::time::Duration;
use tokio::time::Instant;

impl Controller {
    async fn publish_snapshot_state(&mut self, deadline: Instant) -> Result<OperationResult> {
        let mut delay_ms = 10_u64;
        let jitter_ms = self.record.assignment.generation % 17;
        loop {
            if Instant::now() >= deadline {
                return Err(Error::Unavailable(
                    "snapshot state publication timed out".into(),
                ));
            }
            let result = tokio::time::timeout_at(deadline, self.sync())
                .await
                .map_err(|_| Error::Unavailable("snapshot state publication timed out".into()))??;
            if result.durability == Durability::Published {
                return Ok(result);
            }
            // Journaled is durable locally, but cannot authorize a reusable
            // snapshot. Republish the same record; never repeat capture or
            // advance its revision while waiting for Coordinator acceptance.
            let delay = Duration::from_millis(delay_ms + jitter_ms);
            tokio::time::sleep_until(deadline.min(Instant::now() + delay)).await;
            delay_ms = (delay_ms * 2).min(250);
        }
    }

    pub(super) async fn snapshot(&mut self, request: SnapshotRequest) -> Result<SnapshotResult> {
        let id = snapshot_id(
            &self.record.spec.tenant_id,
            &self.record.spec.id,
            &request.operation_id,
        )?;
        if self.record.state != EnvironmentState::Running
            || request.timeout_seconds == 0
            || request.timeout_seconds > 3600
        {
            return Err(Error::Conflict);
        }
        let deadline = Instant::now() + Duration::from_secs(request.timeout_seconds);
        let services = self
            .services
            .checkpoint
            .clone()
            .ok_or_else(|| Error::Unavailable("checkpoint storage not configured".into()))?;
        let catalog = self
            .services
            .snapshots
            .clone()
            .ok_or_else(|| Error::Unavailable("snapshot catalog not configured".into()))?;
        // Probe before stopping the source; creating a cluster snapshot needs Coordinator.
        match catalog.get(&id).await {
            Ok(snapshot) => {
                if snapshot.kind != request.kind
                    || snapshot.names != request.names
                    || !snapshot.matches_template(&self.record.spec)
                    || snapshot.state != SnapshotState::Ready
                    || snapshot.source_node_id != self.record.assignment.node_id
                {
                    return Err(Error::Conflict);
                }
                let environment = self.publish_snapshot_state(deadline).await?;
                return Ok(SnapshotResult {
                    snapshot,
                    environment,
                });
            }
            Err(Error::NotFound) => (),
            Err(error) => return Err(error),
        }
        self.replay_operation(
            &request.operation_id,
            request.expected_revision,
            LifecycleKind::Snapshot,
        )?;

        // Validate names before any lifecycle mutation.
        let mut snapshot = Snapshot::new_with_kind(
            request.kind,
            id.clone(),
            request.names,
            self.record.spec.clone(),
            self.record.assignment.node_id.clone(),
            self.record.runtime.id.clone(),
            adx_core::CheckpointArtifact {
                storage: "pending".into(),
                scope: String::new(),
                location: "pending".into(),
                size_bytes: 1,
            },
        )?;
        self.services
            .runtime
            .checkpoint_supported(&self.record.spec.runtime_class)
            .await?;
        // Capture directly for the reusable Snapshot. It never owns an anonymous restore point.
        let staged = self
            .capture_directory(
                services.store.as_ref(),
                &request.operation_id,
                request.expected_revision,
                LifecycleKind::Snapshot,
            )
            .await?;
        for reference in services.store.capture_references(&staged)? {
            if !self.record.execution_artifacts.contains(&reference) {
                self.record.execution_artifacts.push(reference);
            }
        }
        self.record.revision = self.record.revision.checked_add(1).ok_or(Error::Conflict)?;
        self.publish_snapshot_state(deadline).await?;
        if let Err(error) = services
            .cooperation
            .prepare(&self.record, &request.operation_id)
            .await
        {
            let aborted = services
                .cooperation
                .abort_unstarted(&self.record, &request.operation_id)
                .await;
            if aborted.is_err() {
                self.cleanup().await?;
                self.transition(Event::Fail)?;
            }
            self.record
                .execution_artifacts
                .retain(|a| a.size_bytes != 0);
            self.record.revision = self.record.revision.checked_add(1).ok_or(Error::Conflict)?;
            self.sync().await?;
            services.store.discard_staged(&staged).await?;
            self.capture_preparation = None;
            return Err(error);
        }
        self.capture_preparation = None;
        let remaining = deadline.saturating_duration_since(Instant::now());
        let captured = if remaining.is_zero() {
            Err(Error::Unavailable("snapshot capture budget expired".into()))
        } else {
            self.services
                .runtime
                .checkpoint_running(&self.record.runtime.id, &staged, remaining)
                .await
        };
        let captured = match captured {
            Ok(()) => {
                services
                    .cooperation
                    .resumed(&self.record, &request.operation_id)
                    .await
            }
            Err(error) => Err(error),
        };
        if let Err(error) = captured {
            // Outcome may be unknown: stop the writer before touching its files.
            self.cleanup().await?;
            self.transition(Event::Fail)?;
            self.sync().await?;
            services.store.discard_staged(&staged).await?;
            self.capture_preparation = None;
            return Err(error);
        }
        let retains_capture = self
            .services
            .runtime
            .retains_capture_files(&self.record.spec.runtime_class);
        // Firecracker may still use a running capture as its incremental base.
        // runsc captures do not replace the input held by background restore.
        if retains_capture {
            let captured = services.store.retain_staged(&staged).await?;
            let original = services.store.prepare_restore(&captured).await?;
            self.hold_artifact(captured, original);
        }
        self.record.revision = self.record.revision.checked_add(1).ok_or(Error::Conflict)?;
        self.publish_snapshot_state(deadline).await?;
        snapshot.artifact = services.store.seal(&staged).await?;
        // Publication requires a durable temporary hold even when the runtime
        // does not retain capture output. Existing restore inputs move into
        // execution_files and remain protected when this temporary hold drops.
        let access = services.store.prepare_restore(&snapshot.artifact).await?;
        self.hold_artifact(snapshot.artifact.clone(), access);
        self.record.revision = self.record.revision.checked_add(1).ok_or(Error::Conflict)?;
        self.publish_snapshot_state(deadline).await?;
        let snapshot = catalog.publish(snapshot.clone()).await?;
        services.store.committed(&snapshot.artifact).await?;
        if !retains_capture {
            self.record
                .execution_artifacts
                .retain(|a| a != &snapshot.artifact && a.location != staged.to_string_lossy());
            self.recovery_files = None;
        }
        self.record.revision = self.record.revision.checked_add(1).ok_or(Error::Conflict)?;
        self.completed(
            request.operation_id,
            request.expected_revision,
            LifecycleKind::Snapshot,
        );
        self.durability = None;
        let environment = self.publish_snapshot_state(deadline).await?;
        Ok(SnapshotResult {
            snapshot,
            environment,
        })
    }
}

impl super::Controller {
    pub(super) async fn clone_snapshot(
        &mut self,
        snapshot: adx_core::snapshots::Snapshot,
    ) -> Result<OperationResult> {
        use adx_core::snapshots::{Reference, SnapshotState};
        snapshot.validate()?;
        let spec = &self.record.spec;
        if spec.snapshot_id.as_deref() != Some(snapshot.id.as_str())
            || spec.id == snapshot.template.id
            || spec.tenant_id != snapshot.template.tenant_id
            || spec.image != snapshot.template.image
            || spec.runtime_class != snapshot.template.runtime_class
            || spec.resources != snapshot.template.resources
            || snapshot.state == SnapshotState::Deleted
            || !snapshot.references.contains(&Reference::Restore {
                environment_id: spec.id.clone(),
            })
            || (snapshot.artifact.storage == "local"
                && snapshot.source_node_id != self.record.assignment.node_id)
        {
            return Err(Error::Conflict);
        }
        let origin = snapshot.origin()?;
        if self.record.state != EnvironmentState::Pending || self.record.checkpoint.is_some() {
            return self.create().await;
        }
        let store = self
            .services
            .checkpoint
            .as_ref()
            .ok_or_else(|| Error::Unavailable("checkpoint storage unavailable".into()))?
            .store
            .clone();
        if let Err(error) = store.validate_artifact(&snapshot.artifact).await {
            self.record.state = EnvironmentState::Failed;
            self.record.revision = self.record.revision.checked_add(1).ok_or(Error::Conflict)?;
            self.sync().await?;
            return Err(error);
        }
        self.record.snapshot_source = Some(adx_core::RestorePoint {
            id: snapshot.id.clone(),
            artifact: snapshot.artifact.clone(),
            expires_at_unix_seconds: None,
            source_runtime_id: origin.runtime_id.clone(),
            origin: Some(origin),
        });
        self.create().await
    }
}
