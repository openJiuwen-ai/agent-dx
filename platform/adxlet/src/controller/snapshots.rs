use super::*;
use crate::checkpoint::{SnapshotRequest, SnapshotResult};
use adx_core::{
    snapshots::{Snapshot, SnapshotState},
    LifecycleKind,
};
use sha2::{Digest, Sha256};
use std::time::Duration;

impl Controller {
    pub(super) async fn snapshot(&mut self, request: SnapshotRequest) -> Result<SnapshotResult> {
        // The operation ID is scoped to tenant and Environment. Names are deliberately
        // excluded so retries with changed arguments conflict instead of creating twice.
        let key = serde_json::to_vec(&(
            &self.record.spec.tenant_id,
            &self.record.spec.id,
            &request.operation_id,
        ))
        .map_err(|e| Error::Invalid(e.to_string()))?;
        let id = format!("snapshot-{:x}", Sha256::digest(key));
        if self.record.state != EnvironmentState::Running
            || request.timeout_seconds == 0
            || request.timeout_seconds > 3600
        {
            return Err(Error::Conflict);
        }
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
                if snapshot.names != request.names
                    || !snapshot.matches_template(&self.record.spec)
                    || snapshot.state != SnapshotState::Ready
                    || snapshot.source_node_id != self.record.assignment.node_id
                {
                    return Err(Error::Conflict);
                }
                let environment = self.sync().await?;
                if environment.durability != Durability::Published {
                    return Err(Error::Unavailable(
                        "snapshot source publication pending".into(),
                    ));
                }
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
        let mut snapshot = Snapshot::new(
            id.clone(),
            request.names,
            self.record.spec.clone(),
            self.record.assignment.node_id.clone(),
            self.record.runtime.id.clone(),
            adx_core::CheckpointArtifact {
                storage: "pending".into(),
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
        if self.sync().await?.durability != Durability::Published {
            return Err(Error::Unavailable(
                "capture input reservation publication pending".into(),
            ));
        }
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
        let captured = self
            .services
            .runtime
            .checkpoint_running(
                &self.record.runtime.id,
                &staged,
                Duration::from_secs(request.timeout_seconds),
            )
            .await;
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
        self.sync().await?;
        snapshot.artifact = services.store.seal(&staged).await?;
        // Publication requires a durable temporary hold even when the runtime
        // does not retain capture output. Existing restore inputs move into
        // execution_files and remain protected when this temporary hold drops.
        let access = services.store.prepare_restore(&snapshot.artifact).await?;
        self.hold_artifact(snapshot.artifact.clone(), access);
        self.record.revision = self.record.revision.checked_add(1).ok_or(Error::Conflict)?;
        if self.sync().await?.durability != Durability::Published {
            return Err(Error::Unavailable(
                "snapshot input reference publication pending".into(),
            ));
        }
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
        let environment = self.sync().await?;
        if environment.durability != Durability::Published {
            return Err(Error::Unavailable(
                "snapshot source publication pending".into(),
            ));
        }
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
