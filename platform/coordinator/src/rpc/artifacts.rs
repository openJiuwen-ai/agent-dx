//! Shared artifact deletion is performed by Coordinator, without a source-node RPC.
use super::*;
impl CoordinatorRpc {
    pub async fn collect_shared_artifacts(&self) -> Result<usize> {
        let (session, candidates) = {
            let state = self.0.state.lock().await;
            state.healthy()?;
            let snapshots = state.session.retained_snapshots().await?;
            let holds = state.session.execution_holds().await?;
            let candidates = state
                .session
                .shared_artifacts()
                .await?
                .into_iter()
                .filter(|artifact| {
                    !holds.iter().any(|h| {
                        h.artifacts.iter().any(|a| {
                            a.storage == artifact.storage && a.location == artifact.location
                        })
                    }) && !snapshots.iter().any(|s| &s.artifact == artifact)
                        && !state.environments.values().any(|e| {
                            e.result.as_ref().is_some_and(|r| {
                                r.checkpoint
                                    .iter()
                                    .chain(r.snapshot_source.iter())
                                    .any(|p| &p.artifact == artifact)
                                    || r.execution_artifacts.contains(artifact)
                            })
                        })
                })
                .collect::<Vec<_>>();
            (state.session.clone(), candidates)
        };
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
