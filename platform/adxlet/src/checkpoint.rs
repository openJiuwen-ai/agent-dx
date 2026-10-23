//! Runtime cooperation and lifecycle requests. Storage is shared with Coordinator.
pub use adx_checkpoint_storage::*;
use adx_core::{EnvironmentRecord, Error, Result};
use async_trait::async_trait;
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PauseRequest {
    pub operation_id: String,
    pub expected_revision: u64,
    pub retention: adx_core::lifecycle::RetentionPolicy,
    pub timeout_seconds: u64,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResumeRequest {
    pub operation_id: String,
    pub expected_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotRequest {
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
    async fn publish(
        &self,
        snapshot: adx_core::snapshots::Snapshot,
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
