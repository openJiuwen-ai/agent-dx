use super::*;
use crate::checkpoint::{SnapshotRequest, SnapshotResult};
use adx_core::{
    snapshots::{Snapshot, SnapshotState},
    LifecycleKind,
};
use sha2::{Digest, Sha256};

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
        let replay = self.replay_operation(
            &request.operation_id,
            request.expected_revision,
            LifecycleKind::Snapshot,
        )?;
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
                if !replay
                    || snapshot.names != request.names
                    || snapshot.template != self.record.spec
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
            Err(Error::NotFound) if !replay => (),
            Err(error) => return Err(error),
        }
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
        let paused = self
            .pause(PauseRequest {
                operation_id: format!("{id}-pause"),
                expected_revision: request.expected_revision,
                ttl_seconds: 86400,
                timeout_seconds: request.timeout_seconds,
            })
            .await;
        let paused = match paused {
            Ok(result) => result,
            Err(error) => {
                // The backend may already be stopped even though its completed
                // result could not be committed. Preserve the source when possible.
                if self.record.state == EnvironmentState::Paused {
                    if let Err(resume) = self
                        .resume(ResumeRequest {
                            operation_id: format!("{id}-resume"),
                            expected_revision: self.record.revision,
                        })
                        .await
                    {
                        return Err(Error::Unavailable(format!(
                            "snapshot pause failed: {error}; source resume: {resume}"
                        )));
                    }
                }
                return Err(error);
            }
        };
        let publication = async {
            if paused.durability != Durability::Published {
                return Err(Error::Unavailable(
                    "source checkpoint publication pending".into(),
                ));
            }
            let cp = paused.record.checkpoint.as_ref().ok_or(Error::Conflict)?;
            snapshot.source_runtime_id = cp.source_runtime_id.clone();
            snapshot.artifact = services.store.duplicate(&cp.artifact).await?;
            let saved = catalog.publish(snapshot.clone()).await?;
            if !saved.same_content(&snapshot) || saved.state != SnapshotState::Ready {
                return Err(Error::Conflict);
            }
            services.store.committed(&saved.artifact).await?;
            Ok(saved)
        }
        .await;
        // Even when copying or publication fails, attempt to return the source to
        // Running. Publication ambiguity never authorizes deleting the copied bytes.
        let resumed = self
            .resume(ResumeRequest {
                operation_id: format!("{id}-resume"),
                expected_revision: self.record.revision,
            })
            .await?;
        let snapshot = publication?;
        if resumed.durability != Durability::Published {
            return Err(Error::Unavailable(
                "snapshot source resume publication pending".into(),
            ));
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
        let copied = tokio::time::timeout(
            self.services.operation_timeout,
            store.duplicate(&snapshot.artifact),
        )
        .await
        .unwrap_or_else(|_| Err(Error::Unavailable("snapshot copy timed out".into())));
        let artifact = match copied {
            Ok(artifact) => artifact,
            Err(error) => {
                self.record.state = EnvironmentState::Failed;
                self.record.revision =
                    self.record.revision.checked_add(1).ok_or(Error::Conflict)?;
                self.sync().await?;
                return Err(error);
            }
        };
        self.record.checkpoint = Some(adx_core::RestorePoint {
            id: format!("clone-{}", self.record.spec.id),
            artifact,
            expires_at_unix_seconds: u64::MAX,
            source_runtime_id: origin.runtime_id.clone(),
            origin: Some(origin),
        });
        self.create().await
    }
}
