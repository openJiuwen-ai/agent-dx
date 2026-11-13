//! `ReleaseWriteLeases`: a Node hands back write leases it no longer needs,
//! so another Node can write the file without proxying and without waiting
//! for the 30 s expiry.
//!
//! Provided by Meta from cluster protocol 8; Nodes do not call it yet (R106).
//! Enabling the Node side later needs no protocol change. A release ends a
//! lease by moving its expiry to now: the entity stays, its epoch unchanged,
//! so the next acquire finds it expired and takes the next epoch (epochs are
//! never reused; a proxy session on the old epoch is fenced). Each lease is
//! released only if it still equals, field for field, the copy the Node
//! presents: a renewal or re-acquire by the same owner moves the expiry and
//! makes a late release a no-op. A lease that does not match is skipped, not
//! an error: it is no longer the lease the Node meant to hand back.
//!
//! The batch commits as one transaction recorded under the caller's
//! operation id, so a retry after a lost reply replays instead of releasing a
//! lease taken since. It records `StoreOperation::DfsRenewWriteLease` (a
//! release is a renewal to now) with an empty result: no new operation
//! variant, so the WAL and checkpoint encodings are unchanged.
use super::*;

/// Most leases one release call hands back (one transaction).
pub const MAX_LEASE_RELEASE_BATCH: usize = 1024;

impl DfsService {
    /// Releases every listed lease of `caller_id`'s `owner_session_id` that is
    /// still exactly as presented; returns how many were released by this
    /// call (0 on a replay).
    pub async fn release_write_leases(
        &self,
        caller_id: String,
        owner_session_id: String,
        operation_id: OperationId,
        leases: Vec<WriteLease>,
    ) -> Result<usize> {
        require_id(&caller_id, "caller_id")?;
        require_id(&owner_session_id, "owner_session_id")?;
        require_id(&operation_id.0, "operation_id")?;
        if leases.len() > MAX_LEASE_RELEASE_BATCH {
            return Err(invalid("DFS lease release batch too large"));
        }
        for lease in &leases {
            require_id(&lease.inode_id.0, "lease.inode_id")?;
            if lease.owner_node_id != caller_id || lease.owner_session_id != owner_session_id {
                return Err(conflict(
                    "DFS write lease owner does not match release caller",
                ));
            }
        }
        let request = RequestKey::new(caller_id.clone(), operation_id.0.clone());
        for _ in 0..16 {
            if self.replayed_release(&request).await? {
                return Ok(0);
            }
            let view = self.store.read_view().await?;
            let mut released = Vec::new();
            for lease in &leases {
                if let Some(MetaEntity::DfsWriteLease(stored)) = view
                    .lookup(&MetaKey::DfsWriteLease(lease.inode_id.clone()))?
                    .entity
                    && stored == *lease
                    && !released.contains(&stored)
                {
                    released.push(stored);
                }
            }
            if released.is_empty() {
                return Ok(0);
            }
            let now = now_unix_ms();
            let mut txn = MetaTxn::new(request.clone(), StoreOperation::DfsRenewWriteLease);
            txn.conditions
                .push(TxnCondition::RequestAbsent(request.clone()));
            for stored in &released {
                txn.conditions
                    .push(TxnCondition::EntityEquals(MetaEntity::DfsWriteLease(
                        stored.clone(),
                    )));
                let mut ended = stored.clone();
                ended.expires_at_unix_ms = ended.expires_at_unix_ms.min(now);
                txn.mutations
                    .push(TxnMutation::Put(MetaEntity::DfsWriteLease(ended)));
            }
            txn.mutations
                .push(TxnMutation::RecordRequestOutcome(RequestOutcome {
                    request: request.clone(),
                    operation: StoreOperation::DfsRenewWriteLease,
                    result: OperationResult::Empty,
                }));
            match self.store.compare_and_commit(txn).await? {
                TxnOutcome::Committed { .. } => return Ok(released.len()),
                TxnOutcome::ConditionFailed {
                    existing_outcome: Some(outcome),
                    ..
                } => {
                    release_replay(&outcome)?;
                    return Ok(0);
                }
                // A lease changed between the read and the commit: plan again.
                TxnOutcome::ConditionFailed { .. } => continue,
            }
        }
        Err(Error::coded(
            afs_error::META_DFS_LEASE_RETRY,
            "DFS lease release remained contended after retries",
        ))
    }

    async fn replayed_release(&self, request: &RequestKey) -> Result<bool> {
        let snapshot = self
            .store
            .read(MetaRead::RequestOutcome(request.clone()))
            .await?;
        match snapshot.request_outcome {
            Some(outcome) => release_replay(&outcome).map(|()| true),
            None => Ok(false),
        }
    }
}

/// A recorded outcome under a release's id must be a release.
fn release_replay(outcome: &RequestOutcome) -> Result<()> {
    if outcome.operation == StoreOperation::DfsRenewWriteLease
        && matches!(outcome.result, OperationResult::Empty)
    {
        Ok(())
    } else {
        Err(invalid(
            "DFS operation_id was already used for another operation",
        ))
    }
}

#[cfg(test)]
#[path = "lease_release_tests.rs"]
mod tests;
