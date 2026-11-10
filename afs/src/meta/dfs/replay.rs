//! Idempotent replay: request digests and recorded outcomes.

use super::*;

impl DfsService {
    pub(super) async fn replayed_namespace_result(
        &self,
        caller_id: &str,
        operation_id: &OperationId,
        operation: StoreOperation,
        request_digest: [u8; 32],
    ) -> Result<Option<OperationResult>> {
        let key = RequestKey::new(caller_id, operation_id.0.clone());
        let snapshot = self.store.read(MetaRead::RequestOutcome(key)).await?;
        let Some(outcome) = snapshot.request_outcome else {
            return Ok(None);
        };
        if outcome.operation != operation {
            return Err(invalid(
                "DFS operation_id was already used for another operation",
            ));
        }
        namespace_result_inner(outcome.result, request_digest).map(Some)
    }

    pub(super) async fn replayed_namespace_inode(
        &self,
        caller_id: &str,
        operation_id: &OperationId,
        operation: StoreOperation,
        request_digest: [u8; 32],
    ) -> Result<Option<InodeRecord>> {
        match self
            .replayed_namespace_result(caller_id, operation_id, operation, request_digest)
            .await?
        {
            Some(OperationResult::DfsInode(inode)) => Ok(Some(inode)),
            None => Ok(None),
            _ => Err(invalid("DFS namespace replay returned the wrong result")),
        }
    }

    pub(super) async fn replayed_namespace_inode_and_lease(
        &self,
        caller_id: &str,
        operation_id: &OperationId,
        operation: StoreOperation,
        request_digest: [u8; 32],
    ) -> Result<Option<(InodeRecord, WriteLease)>> {
        match self
            .replayed_namespace_result(caller_id, operation_id, operation, request_digest)
            .await?
        {
            Some(OperationResult::DfsInodeWithLease { inode, lease }) => Ok(Some((inode, lease))),
            None => Ok(None),
            _ => Err(invalid("DFS namespace replay returned the wrong result")),
        }
    }

    pub(super) async fn replayed_namespace_rename(
        &self,
        caller_id: &str,
        operation_id: &OperationId,
        operation: StoreOperation,
        request_digest: [u8; 32],
    ) -> Result<Option<RenameOutcome>> {
        match self
            .replayed_namespace_result(caller_id, operation_id, operation, request_digest)
            .await?
        {
            Some(OperationResult::DfsRename(value)) => Ok(Some(value)),
            None => Ok(None),
            _ => Err(invalid("DFS namespace replay returned the wrong result")),
        }
    }

    pub(super) async fn replayed_inode(
        &self,
        caller_id: &str,
        operation_id: &OperationId,
        operation: StoreOperation,
    ) -> Result<Option<InodeRecord>> {
        let key = RequestKey::new(caller_id, operation_id.0.clone());
        let snapshot = self.store.read(MetaRead::RequestOutcome(key)).await?;
        let Some(outcome) = snapshot.request_outcome else {
            return Ok(None);
        };
        if outcome.operation != operation {
            return Err(invalid(
                "DFS operation_id was already used for another operation",
            ));
        }
        match outcome.result {
            OperationResult::DfsInode(inode) => Ok(Some(inode)),
            _ => Err(invalid("DFS operation replay returned the wrong result")),
        }
    }

    pub(super) async fn replayed_lease(
        &self,
        caller_id: &str,
        operation_id: &OperationId,
        operation: StoreOperation,
    ) -> Result<Option<WriteLease>> {
        let key = RequestKey::new(caller_id, operation_id.0.clone());
        let snapshot = self.store.read(MetaRead::RequestOutcome(key)).await?;
        let Some(outcome) = snapshot.request_outcome else {
            return Ok(None);
        };
        if outcome.operation != operation {
            return Err(invalid(
                "DFS operation_id was already used for another operation",
            ));
        }
        match outcome.result {
            OperationResult::DfsWriteLease(lease) => Ok(Some(lease)),
            _ => Err(invalid("DFS operation replay returned the wrong result")),
        }
    }

    pub(super) async fn replayed_replication_claim(
        &self,
        caller_id: &str,
        operation_id: &OperationId,
        operation: StoreOperation,
        request_digest: [u8; 32],
    ) -> Result<Option<Option<ReplicationClaim>>> {
        let key = RequestKey::new(caller_id, operation_id.0.clone());
        let snapshot = self.store.read(MetaRead::RequestOutcome(key)).await?;
        let Some(outcome) = snapshot.request_outcome else {
            return Ok(None);
        };
        if outcome.operation != operation {
            return Err(invalid(
                "DFS operation_id was already used for another operation",
            ));
        }
        match outcome.result {
            OperationResult::DfsReplicationClaim {
                request_digest: found,
                claim,
            } if found == request_digest => Ok(Some(claim)),
            OperationResult::DfsReplicationClaim { .. } => Err(invalid(
                "DFS replication claim replay request does not match original",
            )),
            _ => Err(invalid(
                "DFS replication claim replay returned the wrong result",
            )),
        }
    }

    pub(super) async fn replayed_replication_task(
        &self,
        caller_id: &str,
        operation_id: &OperationId,
        operation: StoreOperation,
        request_digest: [u8; 32],
    ) -> Result<Option<ReplicationTask>> {
        let key = RequestKey::new(caller_id, operation_id.0.clone());
        let snapshot = self.store.read(MetaRead::RequestOutcome(key)).await?;
        let Some(outcome) = snapshot.request_outcome else {
            return Ok(None);
        };
        if outcome.operation != operation {
            return Err(invalid(
                "DFS operation_id was already used for another operation",
            ));
        }
        match outcome.result {
            OperationResult::DfsNamespace {
                request_digest: found,
                result,
            } if found == request_digest => match *result {
                OperationResult::DfsReplicationTask(task) => Ok(Some(task)),
                _ => Err(invalid(
                    "DFS replication task replay returned the wrong result",
                )),
            },
            OperationResult::DfsNamespace { .. } => Err(invalid(
                "DFS replication report replay request does not match original",
            )),
            OperationResult::DfsReplicationTask(task) => Ok(Some(task)),
            _ => Err(invalid(
                "DFS replication task replay returned the wrong result",
            )),
        }
    }

    pub(super) async fn record_empty_namespace_outcome(
        &self,
        caller_id: &str,
        operation_id: &OperationId,
        operation: StoreOperation,
        request_digest: [u8; 32],
        mut conditions: Vec<TxnCondition>,
    ) -> Result<()> {
        let request_key = RequestKey::new(caller_id, operation_id.0.clone());
        let mut txn = MetaTxn::new(request_key.clone(), operation);
        txn.conditions
            .push(TxnCondition::RequestAbsent(request_key.clone()));
        txn.conditions.append(&mut conditions);
        txn.mutations
            .push(TxnMutation::RecordRequestOutcome(RequestOutcome {
                request: request_key,
                operation,
                result: namespace_result(request_digest, OperationResult::Empty),
            }));
        empty_namespace_outcome(
            self.store.compare_and_commit(txn).await?,
            operation,
            request_digest,
        )
    }
}

/// `FormatKind::MetaRequestDigest`: the digest is stored in WAL records and
/// checkpoints, so the JSON of every digested request is a storage format
/// without a version field, pinned by `request_digest_golden_tests`.
pub(super) fn namespace_request_digest(request: &impl serde::Serialize) -> Result<[u8; 32]> {
    let bytes = serde_json::to_vec(request)
        .map_err(|_| invalid("DFS namespace request cannot be encoded"))?;
    Ok(*blake3::hash(&bytes).as_bytes())
}

pub(super) fn namespace_inode_id(caller_id: &str, operation_id: &OperationId) -> InodeId {
    let mut bytes = (caller_id.len() as u64).to_be_bytes().to_vec();
    bytes.extend_from_slice(caller_id.as_bytes());
    bytes.extend_from_slice(operation_id.0.as_bytes());
    InodeId::new(format!("inode:{}", blake3::hash(&bytes).to_hex()))
}

pub(super) fn namespace_result(
    request_digest: [u8; 32],
    result: OperationResult,
) -> OperationResult {
    OperationResult::DfsNamespace {
        request_digest,
        result: Box::new(result),
    }
}

fn namespace_result_inner(result: OperationResult, expected: [u8; 32]) -> Result<OperationResult> {
    match result {
        OperationResult::DfsNamespace {
            request_digest,
            result,
        } if request_digest == expected => Ok(*result),
        _ => Err(invalid(
            "DFS namespace retry differs from the recorded request or lacks its identity proof",
        )),
    }
}

pub(super) fn validate_namespace_outcome(
    mut outcome: TxnOutcome,
    expected: [u8; 32],
) -> Result<TxnOutcome> {
    let stored = match &mut outcome {
        TxnOutcome::Committed { outcome, .. } => Some(outcome),
        TxnOutcome::ConditionFailed {
            existing_outcome, ..
        } => existing_outcome.as_mut(),
    };
    if let Some(stored) = stored {
        stored.result = namespace_result_inner(stored.result.clone(), expected)?;
    }
    Ok(outcome)
}

pub(super) fn file_commit_attempt(
    outcome: TxnOutcome,
    operation: StoreOperation,
) -> Result<FileCommitAttempt> {
    if matches!(
        &outcome,
        TxnOutcome::ConditionFailed {
            existing_outcome: None,
            ..
        }
    ) {
        // Re-read head, lease identity and every other precondition. A renewed
        // TTL is benign; changed authority/head fails on the next validation.
        Ok(FileCommitAttempt::Retry)
    } else {
        inode_outcome(outcome, operation).map(FileCommitAttempt::Committed)
    }
}

pub(super) fn inode_outcome(outcome: TxnOutcome, operation: StoreOperation) -> Result<InodeRecord> {
    let stored = match outcome {
        TxnOutcome::Committed { outcome, .. }
        | TxnOutcome::ConditionFailed {
            existing_outcome: Some(outcome),
            ..
        } if outcome.operation == operation => outcome,
        TxnOutcome::ConditionFailed { .. } => {
            return Err(conflict("DFS metadata condition changed during commit"));
        }
        _ => {
            return Err(invalid(
                "DFS metadata operation returned an invalid outcome",
            ));
        }
    };
    match stored.result {
        OperationResult::DfsInode(inode) => Ok(inode),
        _ => Err(invalid("DFS metadata operation replayed the wrong result")),
    }
}

pub(super) fn lease_outcome(outcome: TxnOutcome, operation: StoreOperation) -> Result<WriteLease> {
    let stored = match outcome {
        TxnOutcome::Committed { outcome, .. }
        | TxnOutcome::ConditionFailed {
            existing_outcome: Some(outcome),
            ..
        } if outcome.operation == operation => outcome,
        TxnOutcome::ConditionFailed { .. } => {
            return Err(conflict(
                "DFS metadata condition changed during lease update",
            ));
        }
        _ => {
            return Err(invalid(
                "DFS metadata operation returned an invalid outcome",
            ));
        }
    };
    match stored.result {
        OperationResult::DfsWriteLease(lease) => Ok(lease),
        _ => Err(invalid("DFS metadata operation replayed the wrong result")),
    }
}

pub(super) fn replication_claim_outcome(
    outcome: TxnOutcome,
    request_digest: [u8; 32],
) -> Result<Option<ReplicationClaim>> {
    match outcome {
        TxnOutcome::Committed { outcome, .. }
        | TxnOutcome::ConditionFailed {
            existing_outcome: Some(outcome),
            ..
        } => {
            if outcome.operation != StoreOperation::DfsClaimReplicationTask {
                return Err(invalid(
                    "DFS replication claim replay used another operation",
                ));
            }
            match outcome.result {
                OperationResult::DfsReplicationClaim {
                    request_digest: found,
                    claim,
                } if found == request_digest => Ok(claim),
                OperationResult::DfsReplicationClaim { .. } => Err(invalid(
                    "DFS replication claim replay request does not match original",
                )),
                _ => Err(invalid("DFS replication claim outcome has wrong result")),
            }
        }
        TxnOutcome::ConditionFailed { .. } => {
            Err(conflict("DFS replication claim changed during commit"))
        }
    }
}

pub(super) fn replication_task_outcome(
    outcome: TxnOutcome,
    operation: StoreOperation,
    request_digest: [u8; 32],
) -> Result<ReplicationTask> {
    match outcome {
        TxnOutcome::Committed { outcome, .. }
        | TxnOutcome::ConditionFailed {
            existing_outcome: Some(outcome),
            ..
        } => {
            if outcome.operation != operation {
                return Err(invalid(
                    "DFS replication task replay used another operation",
                ));
            }
            match namespace_result_inner(outcome.result, request_digest)? {
                OperationResult::DfsReplicationTask(task) => Ok(task),
                _ => Err(invalid("DFS replication task outcome has wrong result")),
            }
        }
        TxnOutcome::ConditionFailed { .. } => {
            Err(conflict("DFS replication task changed during commit"))
        }
    }
}

pub(super) fn empty_namespace_outcome(
    outcome: TxnOutcome,
    operation: StoreOperation,
    request_digest: [u8; 32],
) -> Result<()> {
    match outcome {
        TxnOutcome::Committed { outcome, .. }
        | TxnOutcome::ConditionFailed {
            existing_outcome: Some(outcome),
            ..
        } => {
            if outcome.operation != operation {
                return Err(invalid("DFS empty replay used another operation"));
            }
            match namespace_result_inner(outcome.result, request_digest)? {
                OperationResult::Empty => Ok(()),
                _ => Err(invalid("DFS empty outcome has wrong result")),
            }
        }
        TxnOutcome::ConditionFailed { .. } => {
            Err(conflict("DFS empty operation changed during commit"))
        }
    }
}

pub(super) fn inode_and_lease_outcome(
    outcome: TxnOutcome,
    operation: StoreOperation,
) -> Result<(InodeRecord, WriteLease)> {
    let stored = match outcome {
        TxnOutcome::Committed { outcome, .. }
        | TxnOutcome::ConditionFailed {
            existing_outcome: Some(outcome),
            ..
        } if outcome.operation == operation => outcome,
        TxnOutcome::ConditionFailed { .. } => {
            return Err(conflict("DFS metadata condition changed during create"));
        }
        _ => {
            return Err(invalid(
                "DFS metadata operation returned an invalid outcome",
            ));
        }
    };
    match stored.result {
        OperationResult::DfsInodeWithLease { inode, lease } => Ok((inode, lease)),
        _ => Err(invalid("DFS metadata operation replayed the wrong result")),
    }
}

pub(super) fn rename_outcome(
    outcome: TxnOutcome,
    operation: StoreOperation,
) -> Result<RenameOutcome> {
    let stored = match outcome {
        TxnOutcome::Committed { outcome, .. }
        | TxnOutcome::ConditionFailed {
            existing_outcome: Some(outcome),
            ..
        } if outcome.operation == operation => outcome,
        TxnOutcome::ConditionFailed { .. } => {
            return Err(conflict("DFS metadata condition changed during rename"));
        }
        _ => {
            return Err(invalid(
                "DFS metadata operation returned an invalid outcome",
            ));
        }
    };
    match stored.result {
        OperationResult::DfsRename(outcome) => Ok(outcome),
        _ => Err(invalid("DFS metadata operation replayed the wrong result")),
    }
}
