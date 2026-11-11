//! Runtime cooperation and lifecycle requests. Storage is shared with Coordinator.
pub use adx_checkpoint_storage::*;
use adx_core::{EnvironmentRecord, Error, Result};
use async_trait::async_trait;
use std::time::Duration;
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PauseRequest {
    pub operation_id: String,
    pub expected_revision: u64,
    pub retention: adx_core::lifecycle::RetentionPolicy,
    pub timeout_seconds: u64,
    /// Single-shot storage override. `None` keeps the Spec's pause policy
    /// storage; only `Shared` may override, because an automatic pause must
    /// not weaken a Spec's stronger storage requirement.
    pub storage: Option<adx_core::lifecycle::CheckpointStorage>,
}
impl PauseRequest {
    /// Resolve the effective checkpoint storage against the Spec's pause
    /// policy storage. Explicit `Shared` overrides; anything other than
    /// `None` or `Shared` is rejected as an invalid override direction.
    pub fn resolve_storage(
        &self,
        spec_storage: adx_core::lifecycle::CheckpointStorage,
    ) -> Result<adx_core::lifecycle::CheckpointStorage> {
        match self.storage {
            None => Ok(spec_storage),
            Some(adx_core::lifecycle::CheckpointStorage::Shared) => {
                Ok(adx_core::lifecycle::CheckpointStorage::Shared)
            }
            Some(_) => Err(Error::Invalid(
                "pause storage override only allows shared".into(),
            )),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResumeRequest {
    /// Override only the runtime started by this resume; does not change Spec defaults.
    pub run_timeout_seconds: Option<u64>,
    pub operation_id: String,
    pub expected_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotRequest {
    pub kind: adx_core::snapshots::SnapshotKind,
    pub operation_id: String,
    pub expected_revision: u64,
    pub names: Vec<String>,
    pub timeout_seconds: u64,
}
#[derive(Debug, Clone)]
pub struct SnapshotResult {
    pub snapshot: adx_core::snapshots::Snapshot,
    pub environment: crate::OperationResult,
}
#[async_trait]
pub trait SnapshotCatalog: Send + Sync {
    async fn get(&self, id: &str) -> Result<adx_core::snapshots::Snapshot>;
    /// Publish the same immutable capture within its remaining operation budget.
    /// A timeout does not prove that the catalog rejected the publication.
    async fn publish(
        &self,
        snapshot: adx_core::snapshots::Snapshot,
        timeout: Duration,
    ) -> Result<adx_core::snapshots::Snapshot>;
}

pub(crate) fn validate_operation(id: &str, revision: u64) -> Result<()> {
    if id.trim().is_empty() || id.len() > 128 || revision == 0 {
        return Err(Error::Invalid(
            "operation ID and positive expected revision required".into(),
        ));
    }
    Ok(())
}
#[async_trait]
pub trait CheckpointCooperation: Send + Sync {
    async fn workload_status(
        &self,
        _record: &EnvironmentRecord,
    ) -> Result<Option<adx_core::runtime::RuntimeStatus>> {
        Ok(None)
    }
    async fn finish_workload(
        &self,
        _record: &EnvironmentRecord,
        _id: &str,
        _error: Option<String>,
    ) -> Result<()> {
        Err(Error::Unavailable(
            "workload checkpoint acknowledgement unsupported".into(),
        ))
    }
    async fn resumed(&self, _record: &EnvironmentRecord, _id: &str) -> Result<()> {
        Err(Error::Unavailable(
            "checkpoint handoff observation unsupported".into(),
        ))
    }
    async fn prepare(&self, record: &EnvironmentRecord, operation_id: &str) -> Result<()>;
    async fn prepare_idle(
        &self,
        record: &EnvironmentRecord,
        operation_id: &str,
        _revision: u64,
    ) -> Result<()> {
        self.prepare(record, operation_id).await
    }
    /// Caller guarantees backend checkpoint has not been invoked.
    async fn abort_unstarted(&self, record: &EnvironmentRecord, operation_id: &str) -> Result<()>;
}
#[async_trait]
impl CheckpointCooperation for crate::runtime_control::RuntimeControlClient {
    async fn prepare_idle(
        &self,
        record: &EnvironmentRecord,
        id: &str,
        revision: u64,
    ) -> Result<()> {
        let status = self.status(record).await?;
        self.prepare_with_idle(record, id, status.revision, Some(revision))
            .await
            .map(|_| ())
    }
    async fn workload_status(
        &self,
        record: &EnvironmentRecord,
    ) -> Result<Option<adx_core::runtime::RuntimeStatus>> {
        let status = if self.uses_stream() {
            match self.pending_checkpoint(record)? {
                Some(status) => status,
                None => return Ok(None),
            }
        } else {
            self.status(record).await?
        };
        Ok(status.requested_checkpoint.is_some().then_some(status))
    }
    async fn finish_workload(
        &self,
        record: &EnvironmentRecord,
        id: &str,
        error: Option<String>,
    ) -> Result<()> {
        self.finish_checkpoint(record, id, error).await.map(|_| ())
    }
    async fn resumed(&self, record: &EnvironmentRecord, id: &str) -> Result<()> {
        use adx_core::runtime::{CheckpointPhase, RuntimePhase};
        if self.wait_resume_notification(record, id).await? {
            return Ok(());
        }
        loop {
            let status = self.status(record).await?;
            if let Some(failure) = status.failure {
                return Err(Error::ProbeFailed(failure));
            }
            let cp = status.checkpoint.as_ref().ok_or(Error::Conflict)?;
            if cp.operation_id != id {
                return Err(Error::Conflict);
            }
            if status.phase == RuntimePhase::Running && cp.phase == CheckpointPhase::Resumed {
                return Ok(());
            }
            if status.phase == RuntimePhase::Failed || cp.phase == CheckpointPhase::Failed {
                return Err(Error::Unavailable("checkpoint handoff failed".into()));
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }
    async fn prepare(&self, record: &EnvironmentRecord, id: &str) -> Result<()> {
        let status = self.status(record).await?;
        self.prepare(record, id, status.revision).await.map(|_| ())
    }
    async fn abort_unstarted(&self, record: &EnvironmentRecord, id: &str) -> Result<()> {
        let status = self.status(record).await?;
        self.abort_unstarted(record, id, status.revision)
            .await
            .map(|_| ())
    }
}

pub(crate) struct CheckpointServices {
    pub store: std::sync::Arc<dyn CheckpointAccess>,
    pub cooperation: std::sync::Arc<dyn CheckpointCooperation>,
}

#[cfg(test)]
mod pause_request_storage_tests {
    use super::*;
    use adx_core::lifecycle::{CheckpointStorage, RetentionPolicy};

    fn request(storage: Option<CheckpointStorage>) -> PauseRequest {
        PauseRequest {
            operation_id: "pause-1".into(),
            expected_revision: 1,
            retention: RetentionPolicy::Ttl { ttl_seconds: 60 },
            timeout_seconds: 30,
            storage,
        }
    }

    #[test]
    fn no_override_keeps_the_spec_storage() {
        let request = request(None);
        for spec in [CheckpointStorage::Configured, CheckpointStorage::Local] {
            assert_eq!(request.resolve_storage(spec), Ok(spec));
        }
    }

    #[test]
    fn shared_override_wins_over_any_spec_storage() {
        let request = request(Some(CheckpointStorage::Shared));
        for spec in [CheckpointStorage::Configured, CheckpointStorage::Local] {
            assert_eq!(request.resolve_storage(spec), Ok(CheckpointStorage::Shared));
        }
    }

    #[test]
    fn weaker_override_directions_are_rejected() {
        for storage in [CheckpointStorage::Local, CheckpointStorage::Configured] {
            let request = request(Some(storage));
            for spec in [CheckpointStorage::Configured, CheckpointStorage::Local] {
                assert!(request.resolve_storage(spec).is_err());
            }
        }
    }
}
