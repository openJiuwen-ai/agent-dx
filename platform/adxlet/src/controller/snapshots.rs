use super::*;
use crate::checkpoint::{SnapshotRequest, SnapshotResult};
use adx_core::{
    snapshots::{snapshot_id, Snapshot, SnapshotState},
    LifecycleKind,
};
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    path::PathBuf,
    time::Duration,
};
use tokio::time::Instant;

#[derive(Clone, Copy)]
enum PublicationStage {
    Captured,
    CaptureRecorded,
    Sealed,
    HoldRecorded,
    CatalogPublished,
    Completed,
}

// Retained by the live owner across RPC failures. Persisted execution-artifact
// holds protect the files; restart recovery still retires an unsealed capture.
#[derive(Clone)]
pub(super) struct SnapshotPublication {
    pub(super) request: SnapshotRequest,
    snapshot: Snapshot,
    staged: PathBuf,
    retains_capture: bool,
    stage: PublicationStage,
}

fn publication_jitter_ms(environment_id: &str, generation: u64) -> u64 {
    let mut hasher = DefaultHasher::new();
    environment_id.hash(&mut hasher);
    generation.hash(&mut hasher);
    hasher.finish() % 17
}

impl Controller {
    async fn publish_snapshot_state(&mut self, deadline: Instant) -> Result<OperationResult> {
        let mut delay_ms = 10_u64;
        let jitter_ms =
            publication_jitter_ms(&self.record.spec.id, self.record.assignment.generation);
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
        // A snapshot may outlive runtime capture while publishing its artifact.
        // Keep accepted work visible to quiescing through the final commit.
        let _execution = self.services.execution(&self.record, "pause").await?;
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
        if let Some(pending) = &self.snapshot_publication {
            let original = &pending.request;
            if original.operation_id != request.operation_id
                || original.expected_revision != request.expected_revision
                || original.kind != request.kind
                || original.names != request.names
            {
                return Err(Error::Conflict);
            }
            return self.finish_snapshot_publication(deadline).await;
        }
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
        let snapshot = Snapshot::new_with_kind(
            request.kind,
            id.clone(),
            request.names.clone(),
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
        let prepared = services
            .cooperation
            .prepare(&self.record, &request.operation_id)
            .await
            .and_then(|()| super::workload_checkpoint::remaining_backend_timeout(Some(deadline)));
        let timeout = match prepared {
            Ok(timeout) => timeout,
            Err(error) => {
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
                self.record.revision =
                    self.record.revision.checked_add(1).ok_or(Error::Conflict)?;
                self.sync().await?;
                services.store.discard_staged(&staged).await?;
                self.capture_preparation = None;
                return Err(error);
            }
        };
        self.capture_preparation = None;
        let source_environment_id = self.record.spec.id.clone();
        let operation_id = request.operation_id.clone();
        let step_error = |phase: &'static str, error: Error| {
            adx_observability::warn!(
                environment_id = %source_environment_id,
                operation_id = %operation_id,
                phase,
                error = %error,
                "Reusable snapshot step failed"
            );
            error
        };
        let captured = self
            .services
            .runtime
            .checkpoint_running(&self.record.runtime.id, &staged, timeout)
            .await
            .map_err(|error| step_error("runtime-capture", error));
        let captured = match captured {
            Ok(()) => services
                .cooperation
                .resumed(&self.record, &request.operation_id)
                .await
                .map_err(|error| step_error("runtime-resume-handoff", error)),
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
        self.snapshot_publication = Some(SnapshotPublication {
            request,
            snapshot,
            staged,
            retains_capture,
            stage: PublicationStage::Captured,
        });
        self.finish_snapshot_publication(deadline).await
    }

    async fn finish_snapshot_publication(&mut self, deadline: Instant) -> Result<SnapshotResult> {
        let mut pending = self.snapshot_publication.clone().ok_or(Error::Conflict)?;
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
        let source_environment_id = self.record.spec.id.clone();
        let operation_id = pending.request.operation_id.clone();
        let step_error = |phase: &'static str, error: Error| {
            adx_observability::warn!(
                environment_id = %source_environment_id,
                operation_id = %operation_id,
                phase,
                error = %error,
                "Reusable snapshot step failed"
            );
            error
        };
        loop {
            match pending.stage {
                PublicationStage::Captured => {
                    // Firecracker may still use this capture as its incremental
                    // base. Preserve the existing background-restore input too.
                    if pending.retains_capture {
                        let captured = services.store.retain_staged(&pending.staged).await?;
                        let access = services.store.prepare_restore(&captured).await?;
                        self.hold_artifact(captured, access);
                    }
                    self.record.revision =
                        self.record.revision.checked_add(1).ok_or(Error::Conflict)?;
                    pending.stage = PublicationStage::CaptureRecorded;
                }
                PublicationStage::CaptureRecorded => {
                    self.publish_snapshot_state(deadline)
                        .await
                        .map_err(|error| step_error("capture-state-commit", error))?;
                    pending.snapshot.artifact = services
                        .store
                        .seal(&pending.staged)
                        .await
                        .map_err(|error| step_error("seal-artifact", error))?;
                    pending.stage = PublicationStage::Sealed;
                }
                PublicationStage::Sealed => {
                    // Even a runtime that does not retain output needs a
                    // published temporary hold before catalog publication.
                    let access = services
                        .store
                        .prepare_restore(&pending.snapshot.artifact)
                        .await
                        .map_err(|error| step_error("prepare-artifact-hold", error))?;
                    self.hold_artifact(pending.snapshot.artifact.clone(), access);
                    self.record.revision =
                        self.record.revision.checked_add(1).ok_or(Error::Conflict)?;
                    pending.stage = PublicationStage::HoldRecorded;
                }
                PublicationStage::HoldRecorded => {
                    self.publish_snapshot_state(deadline)
                        .await
                        .map_err(|error| step_error("artifact-hold-commit", error))?;
                    pending.snapshot = catalog.publish(pending.snapshot.clone()).await?;
                    pending.stage = PublicationStage::CatalogPublished;
                }
                PublicationStage::CatalogPublished => {
                    services.store.committed(&pending.snapshot.artifact).await?;
                    if !pending.retains_capture {
                        self.record.execution_artifacts.retain(|artifact| {
                            artifact != &pending.snapshot.artifact
                                && artifact.location != pending.staged.to_string_lossy()
                        });
                        self.recovery_files = None;
                    }
                    self.record.revision =
                        self.record.revision.checked_add(1).ok_or(Error::Conflict)?;
                    self.completed(
                        pending.request.operation_id.clone(),
                        pending.request.expected_revision,
                        LifecycleKind::Snapshot,
                    );
                    self.durability = None;
                    pending.stage = PublicationStage::Completed;
                }
                PublicationStage::Completed => {
                    let environment = self.publish_snapshot_state(deadline).await?;
                    self.snapshot_publication = None;
                    return Ok(SnapshotResult {
                        snapshot: pending.snapshot,
                        environment,
                    });
                }
            }
            // Advance exactly once before the next fallible await. A retry of
            // the same operation resumes publication and never repeats capture.
            self.snapshot_publication = Some(pending.clone());
        }
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

#[cfg(test)]
mod publication_tests {
    use super::*;

    #[test]
    fn publication_jitter_distinguishes_environments_with_the_same_generation() {
        let values: std::collections::BTreeSet<_> = (0..64)
            .map(|index| publication_jitter_ms(&format!("environment-{index}"), 1))
            .collect();
        assert!(values.len() > 1);
        assert!(values.iter().all(|value| *value < 17));
        assert_eq!(
            publication_jitter_ms("environment-a", 1),
            publication_jitter_ms("environment-a", 1)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn capture_budget_rounds_up_without_reviving_an_expired_deadline() {
        use super::super::workload_checkpoint::remaining_backend_timeout;
        let now = Instant::now();
        assert_eq!(
            remaining_backend_timeout(Some(now + Duration::from_millis(1))).unwrap(),
            Duration::from_secs(1)
        );
        assert_eq!(
            remaining_backend_timeout(Some(now + Duration::from_millis(1500))).unwrap(),
            Duration::from_secs(2)
        );
        assert!(matches!(
            remaining_backend_timeout(Some(now)),
            Err(Error::Unavailable(_))
        ));
    }
}
