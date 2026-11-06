//! Repair: the Node's background replication-task loop.
//!
//! [`ReplicationWorker`] reports quarantined local copies to Meta, claims
//! repair tasks, copies the source along its chain and reports the result.
//! Meta decides who repairs, backoff and Corrupt marking; the Node only drops
//! a request Meta definitively rejected. [`RepairService`] runs the worker on
//! the blocking pool until shutdown.

use std::{collections::HashSet, sync::Arc, time::Duration};

use afs_error::{Error, Result};

use crate::{
    dfs::{ChunkId, EffectiveLocalCopy, ReplicaAck},
    node::{
        chunk::{LocalChunkStore, QuarantineTrigger, StagedChunk},
        fault::classify::{
            corruption_report_definitively_rejected, repair_report_definitively_rejected,
            repair_request_definitively_rejected, repair_source_is_invalid,
            verified_copy_is_invalid,
        },
        replication::{AckRequirement, ReplicaChain, ReplicaDataPlane, ReplicationPlan},
    },
    runtime::{ServiceResult, Services, cancelled},
};

/// Background repair uses Meta's durable task identity rather than an inode
/// write lease. Only a node with an existing live source copy claims the task;
/// no file bytes pass through Meta and no FileVersion is changed by repair.
pub trait ReplicationTaskAuthority: Send + Sync {
    fn report_corrupt_chunk(&self, request: crate::dfs::ReportChunkCorruption) -> Result<()>;
    fn claim(
        &self,
        request: crate::dfs::ClaimReplicationTask,
    ) -> Result<Option<crate::dfs::ReplicationClaim>>;
    fn report(
        &self,
        request: crate::dfs::ReportReplicationTask,
    ) -> Result<crate::dfs::ReplicationTask>;
}

#[derive(Clone)]
enum PendingRepair {
    Corruption(Box<crate::dfs::ReportChunkCorruption>),
    Claiming(crate::dfs::ClaimReplicationTask),
    Reporting(Box<crate::dfs::ReportReplicationTask>),
}

/// One task and one bounded chunk buffer per worker. An unknown Meta response
/// retains the exact request; restarting a node leaves the persisted claim to
/// expire/reassign. A receiver ACK alone never promotes a Meta copy record.
pub struct ReplicationWorker {
    node_id: String,
    node_epoch: u64,
    session_id: String,
    local: Arc<LocalChunkStore>,
    authority: Arc<dyn ReplicationTaskAuthority>,
    chain: ReplicaChain,
    pending: std::sync::Mutex<Option<PendingRepair>>,
    reported_quarantines: std::sync::Mutex<HashSet<(ChunkId, u64)>>,
    sequence: std::sync::atomic::AtomicU64,
}

impl ReplicationWorker {
    pub fn new(
        node_id: String,
        node_epoch: u64,
        session_id: String,
        local: Arc<LocalChunkStore>,
        authority: Arc<dyn ReplicationTaskAuthority>,
        data_plane: Arc<dyn ReplicaDataPlane>,
    ) -> Self {
        Self {
            chain: ReplicaChain::new(node_id.clone(), node_epoch, local.clone(), data_plane),
            node_id,
            node_epoch,
            session_id,
            local,
            authority,
            pending: std::sync::Mutex::new(None),
            reported_quarantines: std::sync::Mutex::new(HashSet::new()),
            sequence: std::sync::atomic::AtomicU64::new(1),
        }
    }

    fn operation_id(&self, kind: &str) -> crate::dfs::OperationId {
        use std::sync::atomic::Ordering;
        crate::dfs::OperationId::new(format!(
            "repair-{kind}:{}:{}",
            self.session_id,
            self.sequence.fetch_add(1, Ordering::Relaxed)
        ))
    }

    /// Called from a blocking worker, never from the Tokio reactor. Serialize
    /// this worker's claim/report requests so a timeout cannot launch another
    /// transfer while the original outcome is unknown.
    pub fn run_once(&self) -> Result<Option<crate::dfs::ReplicationTask>> {
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| invalid("replication worker state lock is poisoned"))?;
        if pending.is_none() {
            let records = self.local.quarantined_chunks()?;
            let active: HashSet<_> = records
                .iter()
                .map(|record| (record.chunk.id.clone(), record.catalog_revision))
                .collect();
            let mut reported = self
                .reported_quarantines
                .lock()
                .map_err(|_| invalid("corruption report state lock is poisoned"))?;
            reported.retain(|key| active.contains(key));
            if let Some(record) = records.iter().find(|record| {
                !reported.contains(&(record.chunk.id.clone(), record.catalog_revision))
            }) {
                *pending = Some(PendingRepair::Corruption(Box::new(
                    crate::dfs::ReportChunkCorruption {
                        caller_id: self.node_id.clone(),
                        caller_session_id: self.session_id.clone(),
                        caller_node_epoch: self.node_epoch,
                        operation_id: self.operation_id("corrupt"),
                        chunk_id: record.chunk.id.clone(),
                        device_id: record.device_id.clone(),
                        device_epoch: record.device_epoch,
                        catalog_revision: record.catalog_revision,
                    },
                )));
            }
        }
        if let Some(PendingRepair::Corruption(request)) = pending.as_ref() {
            // Unknown ACK, timeout or CAS conflict retains this exact request.
            // It cannot accuse another node and contains no file bytes.
            if let Err(error) = self
                .authority
                .report_corrupt_chunk(request.as_ref().clone())
            {
                if corruption_report_definitively_rejected(&error) {
                    // A definitive refusal is not an unknown commit. Keep the
                    // local quarantine, suppress this identity for this worker
                    // incarnation and allow other repair work to progress.
                    self.reported_quarantines
                        .lock()
                        .map_err(|_| invalid("corruption report state lock is poisoned"))?
                        .insert((request.chunk_id.clone(), request.catalog_revision));
                    afs_logging::warn!("dfs.local_corruption_report_rejected";
                        "chunk" => request.chunk_id.0.clone(),
                        "catalog_revision" => request.catalog_revision,
                        "error" => error.to_string());
                    *pending = None;
                }
                return Err(error);
            }
            self.reported_quarantines
                .lock()
                .map_err(|_| invalid("corruption report state lock is poisoned"))?
                .insert((request.chunk_id.clone(), request.catalog_revision));
            afs_logging::warn!("dfs.local_corruption_reported";
                "chunk" => request.chunk_id.0.clone(),
                "catalog_revision" => request.catalog_revision);
            *pending = None;
            return Ok(None);
        }
        let current = pending.get_or_insert_with(|| {
            PendingRepair::Claiming(crate::dfs::ClaimReplicationTask {
                caller_id: self.node_id.clone(),
                caller_session_id: self.session_id.clone(),
                caller_node_epoch: self.node_epoch,
                operation_id: self.operation_id("claim"),
                lease_seconds: 120,
            })
        });
        if let PendingRepair::Claiming(request) = current {
            let claim = match self.authority.claim(request.clone()) {
                Ok(Some(claim)) => claim,
                Ok(None) => {
                    *pending = None;
                    return Ok(None);
                }
                Err(error) => {
                    if repair_request_definitively_rejected(&error) {
                        *pending = None;
                    }
                    return Err(error);
                }
            };
            if claim.operation_id != request.operation_id
                || claim.worker_node_id != self.node_id
                || claim.worker_node_epoch != self.node_epoch
                || claim.worker_session_id != self.session_id
            {
                // A mismatched authority result must never become a transfer.
                return Err(invalid("Meta repair claim does not bind this request"));
            }
            let now = unix_ms()?;
            if claim.expires_at_unix_ms <= now {
                // Exact replay can confirm a previously accepted but now stale
                // claim. Start a fresh poll; do not transfer under that claim.
                *pending = None;
                return Ok(None);
            }
            let (durable_acks, error, source_invalid) = match self.replicate(&claim) {
                Ok(acks) => (acks, None, false),
                Err((error, source_invalid)) => {
                    (Vec::new(), Some(error.to_string()), source_invalid)
                }
            };
            *pending = Some(PendingRepair::Reporting(Box::new(
                crate::dfs::ReportReplicationTask {
                    caller_id: self.node_id.clone(),
                    caller_session_id: self.session_id.clone(),
                    caller_node_epoch: self.node_epoch,
                    operation_id: self.operation_id("report"),
                    claim,
                    durable_acks,
                    error,
                    source_invalid,
                },
            )));
        }
        let Some(PendingRepair::Reporting(request)) = pending.as_ref() else {
            return Err(invalid("repair worker lost the original claim"));
        };
        match self.authority.report(request.as_ref().clone()) {
            Ok(task) => {
                if task.id != request.claim.task_id
                    || task.chunk_id != request.claim.chunk.id
                    || (task.state == crate::dfs::ReplicationTaskState::Completed
                        && task.placement_epoch != request.claim.replica_group.placement_epoch)
                    || usize::from(task.desired_copies) != request.claim.replica_group.targets.len()
                    || !matches!(
                        task.state,
                        crate::dfs::ReplicationTaskState::Completed
                            | crate::dfs::ReplicationTaskState::RetryWaiting
                            | crate::dfs::ReplicationTaskState::BlockedNoSource
                    )
                {
                    // Keep the exact report pending: an unrelated response does
                    // not confirm this transfer or authorize the next task.
                    return Err(invalid("Meta repair report does not bind this task"));
                }
                *pending = None;
                Ok(Some(task))
            }
            Err(error) => {
                if repair_report_definitively_rejected(&error) {
                    *pending = None;
                }
                Err(error)
            }
        }
    }

    fn replicate(
        &self,
        claim: &crate::dfs::ReplicationClaim,
    ) -> std::result::Result<Vec<ReplicaAck>, (Error, bool)> {
        let length = usize::try_from(claim.chunk.length)
            .map_err(|_| (invalid("repair chunk length overflows this node"), false))?;
        if length == 0 || length > crate::node::chunk::MAX_STAGED_CHUNK_BYTES {
            return Err((invalid("repair chunk exceeds the staging budget"), false));
        }
        // Local source failures cannot be conflated with remote transport or
        // capacity errors. Mark corrupt only for missing/invalid durable data,
        // not permission, transient I/O, ENOSPC, or an unavailable peer.
        let reader = self.local.open_verified(&claim.chunk.id).map_err(|error| {
            let source_invalid = repair_source_is_invalid(&error);
            (error, source_invalid)
        })?;
        let mut bytes = vec![0; length];
        let mut offset = 0;
        while offset < length {
            let count = reader
                .read_at(offset as u64, &mut bytes[offset..])
                .map_err(|error| {
                    let source_invalid = verified_copy_is_invalid(&error);
                    if source_invalid {
                        self.local
                            .lifecycle()
                            .quarantine(&claim.chunk.id, QuarantineTrigger::RepairSource);
                    }
                    (error, source_invalid)
                })?;
            if count == 0 {
                return Err((
                    invalid("repair source ended before its immutable length"),
                    true,
                ));
            }
            offset += count;
        }
        let staged = StagedChunk::from_replica(claim.operation_id.clone(), &claim.chunk, bytes)
            .map_err(|error| (error, true))?;
        if staged.chunk != claim.chunk {
            return Err((invalid("repair source identity or digest differs"), true));
        }
        // Meta forced Required on this claim because a repair source must head
        // its chain: a chain constraint, not user policy, so it never follows an
        // effective value. The explicit check below states it again.
        let local_copy = EffectiveLocalCopy::declared(&claim.replication);
        let plan = ReplicationPlan {
            chunk_id: claim.chunk.id.clone(),
            placement_revision: claim.placement_revision,
            placement_epoch: claim.replica_group.placement_epoch,
            replica_group_id: claim.replica_group.id.clone(),
            config: claim.replication.clone(),
            local_copy,
            ack_requirement: AckRequirement::strict(&claim.replication, local_copy),
            ordered_targets: claim.replica_group.targets.clone(),
        };
        plan.validate_initiator(&self.node_id)
            .map_err(|error| (error, false))?;
        if !plan.ordered_targets.first().is_some_and(|target| {
            target.node_id == self.node_id && target.node_epoch == self.node_epoch
        }) || plan.sync_target_count().map_err(|error| (error, false))?
            != plan.ordered_targets.len()
        {
            return Err((
                invalid("repair claim does not execute its full source-first chain"),
                false,
            ));
        }
        self.chain
            .put_repair(&plan, &staged, claim)
            .map_err(|error| (error, false))
    }
}

pub(crate) fn unix_ms() -> Result<u64> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|time| time.as_millis() as u64)
        .map_err(|_| invalid("system clock is before epoch"))
}

/// Idle poll and post-error delay of the repair loop.
const REPAIR_IDLE_DELAY: Duration = Duration::from_secs(1);
/// Delay after a reported task, so a backlog drains without a busy loop.
const REPAIR_BUSY_DELAY: Duration = Duration::from_millis(1);

/// Runs one [`ReplicationWorker`] until shutdown.
///
/// Conventions: a pass error is logged (`dfs.replication_task_retry`) and
/// retried after the idle delay; a worker panic fails the service, because it
/// can invalidate the worker's exact pending request and a duplicate must not
/// be launched; shutdown is observed between passes, never inside one.
pub(crate) struct RepairService {
    worker: Arc<ReplicationWorker>,
}

impl RepairService {
    pub(crate) fn new(worker: ReplicationWorker) -> Self {
        Self {
            worker: Arc::new(worker),
        }
    }

    /// Start the loop as one service; it stops with `services.stop`.
    pub(crate) fn start(self, services: &mut Services) {
        #[cfg(feature = "diagnostics")]
        if super::experiment::repair_loop_off() {
            return;
        }
        let stop = services.stop.subscribe();
        let worker = self.worker;
        services.spawn(run_repair_loop(stop, move || worker.run_once()));
    }
}

async fn run_repair_loop(
    stop: tokio::sync::watch::Receiver<bool>,
    pass: impl Fn() -> Result<Option<crate::dfs::ReplicationTask>> + Send + Sync + 'static,
) -> ServiceResult {
    let pass = Arc::new(pass);
    let shutdown = cancelled(stop);
    tokio::pin!(shutdown);
    let mut delay = REPAIR_IDLE_DELAY;
    loop {
        tokio::select! {
            _ = &mut shutdown => return Ok(()),
            _ = tokio::time::sleep(delay) => {}
        }
        let pass = pass.clone();
        // The worker owns an exact pending request until its Meta
        // outcome is confirmed. A started blocking transfer is not
        // detached on a tick or replaced by another task.
        match crate::node::bridge::spawn_blocking(move || pass()).await {
            Ok(Ok(Some(task))) => {
                afs_logging::info!("dfs.replication_task_reported";
                    "task" => task.id.0,
                    "state" => format!("{:?}", task.state),
                    "attempt" => task.attempt);
                delay = REPAIR_BUSY_DELAY;
            }
            Ok(Ok(None)) => delay = REPAIR_IDLE_DELAY,
            Ok(Err(error)) => {
                afs_logging::warn!("dfs.replication_task_retry"; "error" => error.to_string());
                delay = REPAIR_IDLE_DELAY;
            }
            Err(error) => {
                // A panic can invalidate the worker's exact state.
                // Fail the service instead of launching a duplicate.
                return Err(error.into());
            }
        }
    }
}

fn invalid(message: impl Into<String>) -> Error {
    Error::coded(afs_error::NODE_STORAGE_INVALID, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn repair_service_fails_on_worker_panic() {
        let (_stop, rx) = tokio::sync::watch::channel(false);
        let result = run_repair_loop(rx, || panic!("worker state invalidated")).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn repair_service_retries_errors_and_stops_on_shutdown() {
        let (stop, rx) = tokio::sync::watch::channel(false);
        let passes = Arc::new(AtomicUsize::new(0));
        let counted = passes.clone();
        let service = tokio::spawn(run_repair_loop(rx, move || {
            counted.fetch_add(1, Ordering::SeqCst);
            Err(invalid("meta unavailable"))
        }));
        // The first pass runs after the idle delay; an error does not end it.
        while passes.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(!service.is_finished());
        stop.send(true).unwrap();
        assert!(service.await.unwrap().is_ok());
    }
}
