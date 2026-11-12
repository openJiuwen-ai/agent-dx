//! Peer read admission. Every peer read holds one pending slot and one RPC slot
//! for its scope, bounded by the configured read pipeline limits.
use super::*;
use crate::node::read::admission::ReadPipelineLimits;

pub(super) struct PeerReadPipeline {
    rpc: Arc<tokio::sync::Semaphore>,
    pending: Arc<tokio::sync::Semaphore>,
}

fn expired(message: &'static str) -> afs_error::Error {
    afs_error::Error::coded(afs_error::CLIENT_DEADLINE_EXCEEDED, message)
}

impl GrpcChunkTransfer {
    /// Borrowed/cache/CRC paths share the same RPC and pending limits. A scope
    /// owner can be cloned by a streaming producer.
    pub(super) async fn admit_borrowed_read(
        &self,
    ) -> afs_error::Result<crate::node::read::admission::ReadLifetime> {
        let inherited = crate::node::read::admission::current_read_scope();
        let deadline = inherited
            .as_ref()
            .map_or_else(|| std::time::Instant::now() + self.timeout, |s| s.deadline);
        let Some(pipeline) = self.read_pipeline.get() else {
            return Ok(Arc::new(inherited));
        };
        let pending = tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            pipeline.pending.clone().acquire_owned(),
        )
        .await
        .map_err(|_| expired("read pending admission expired"))?
        .map_err(|_| expired("read pending admission closed"))?;
        let rpc = tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            pipeline.rpc.clone().acquire_owned(),
        )
        .await
        .map_err(|_| expired("read RPC admission expired"))?
        .map_err(|_| expired("read RPC admission closed"))?;
        Ok(Arc::new((pending, rpc, inherited.map(|s| s.lifetime))))
    }

    pub(super) fn configure_native_read_pipeline(
        &self,
        limits: ReadPipelineLimits,
    ) -> afs_error::Result<()> {
        let limits = limits.validate()?;
        self.read_pipeline
            .set(Arc::new(PeerReadPipeline {
                rpc: Arc::new(tokio::sync::Semaphore::new(limits.max_rpcs)),
                pending: Arc::new(tokio::sync::Semaphore::new(limits.max_pending)),
            }))
            .map_err(|_| {
                afs_error::Error::coded(
                    afs_error::CONFIG_INVALID,
                    "read pipeline already configured",
                )
            })
    }
}
