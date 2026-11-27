//! Shared artifact deletion is performed by Coordinator, without a source-node RPC.
use super::*;
use crate::storage::{artifact_key, record_artifacts};
use adx_core::snapshots::SnapshotState;

const SHARED_ARTIFACTS_PER_SWEEP: usize = 16;

impl CoordinatorRpc {
    pub async fn collect_shared_artifacts(&self) -> Result<usize> {
        let session = {
            let mut state = self.0.state.lock().await;
            state.recover_authoritative_state().await?;
            state.healthy()?;
            state.session.clone()
        };
        let (publication, _) = session.publication_snapshot()?;
        let (snapshots, holds) = session.inspection_inventory().await?;
        let referenced: BTreeSet<_> = publication
            .environments
            .values()
            .filter_map(|environment| environment.result.as_ref())
            .flat_map(record_artifacts)
            .chain(
                snapshots
                    .into_iter()
                    .filter(|snapshot| snapshot.state != SnapshotState::Deleted)
                    .map(|snapshot| snapshot.artifact),
            )
            .chain(holds.into_iter().flat_map(|hold| hold.artifacts))
            .map(|artifact| artifact_key(&artifact))
            .collect();
        let candidates = session
            .shared_artifacts()
            .await?
            .into_iter()
            .filter(|artifact| !referenced.contains(&artifact_key(artifact)))
            .take(SHARED_ARTIFACTS_PER_SWEEP);
        let stores = self
            .0
            .reclaimers
            .read()
            .expect("reclaimer registry lock poisoned")
            .clone();
        let mut removed = 0;
        for artifact in candidates {
            // Verify the Coordinator epoch before destructive I/O. Artifact identities are never reused.
            let _deletion = session.claim_artifact_deletion(&artifact, None).await?;
            stores.remove(&artifact).await?;
            session.forget_artifact(&artifact).await?;
            removed += 1;
        }
        Ok(removed)
    }
}
