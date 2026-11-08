//! Copy lifecycle: the shared phase vocabulary and the Node's single entry for
//! local copy transitions.
//!
//! Node and Meta each keep their own authority over a copy; this module does
//! not merge them into one state machine. `crate::dfs::CopyPhase` names the phases both
//! sides already have, and [`CopyLifecycle`] is the only Node-side writer of
//! the Durable → Quarantined and → Retired transitions. Normal paths call it
//! to record and report; the decisions those records feed (report to Meta,
//! repair, deletion) belong to the fault layer and Meta.
//!
//! Quarantine stays synchronous on the detecting thread: a read that found a
//! bad copy returns only after the quarantine took effect (pinned by the
//! `characterize_*quarantine*` tests). Only the entry is shared, not a thread.

use super::*;
use crate::dfs::CopyPhase;

impl CopyPhase {
    /// The phase of a cataloged local copy.
    pub fn of_local(state: LocalChunkState) -> Self {
        match state {
            LocalChunkState::Durable => Self::Durable,
            LocalChunkState::Quarantined => Self::Quarantined,
        }
    }
}

/// Which detection asked for a quarantine. It names the call site in the
/// failure log; it never changes whether the copy is quarantined.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QuarantineTrigger {
    /// `LocalChunkStore::read_at`.
    LocalRead,
    /// `LocalChunkStore::read_range_at_uncommitted` (RDMA serving).
    UncommittedRangeRead,
    /// `LocalChunkStore::open_verified` full scan (reads and repair source).
    VerifiedOpen,
    /// `LocalChunkStore::open_cataloged` could not open the file.
    CatalogOpen,
    /// `PeerGroupReader` streaming a verified group to a peer.
    PeerStreamGroup,
    /// `read_peer_range_private` serving a peer range.
    PeerRangeServe,
    /// DFS read engine's local source failed.
    DfsLocalRead,
    /// Reading back a native upload receipt.
    UploadedReadBack,
    /// Background repair reading its local source.
    RepairSource,
    /// Background scrub verifying a durable copy (S4-16).
    Scrub,
}

impl QuarantineTrigger {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::LocalRead => "local_read",
            Self::UncommittedRangeRead => "uncommitted_range_read",
            Self::VerifiedOpen => "verified_open",
            Self::CatalogOpen => "catalog_open",
            Self::PeerStreamGroup => "peer_stream_group",
            Self::PeerRangeServe => "peer_range_serve",
            Self::DfsLocalRead => "dfs_local_read",
            Self::UploadedReadBack => "uploaded_read_back",
            Self::RepairSource => "repair_source",
            Self::Scrub => "scrub",
        }
    }
}

/// The Node's single entry for local copy transitions. A borrowed view of the
/// store; it adds no thread, queue or lock of its own.
pub(crate) struct CopyLifecycle<'a> {
    store: &'a LocalChunkStore,
}

impl LocalChunkStore {
    pub(crate) fn lifecycle(&self) -> CopyLifecycle<'_> {
        CopyLifecycle { store: self }
    }
}

impl CopyLifecycle<'_> {
    /// Durable → Quarantined, best effort: re-verify the actual file and
    /// quarantine only if it really is missing or corrupt. Runs synchronously
    /// on the caller's thread. A failure is logged and swallowed so the read
    /// error (or a fallback read) still decides the caller's result.
    pub(crate) fn quarantine(&self, chunk_id: &ChunkId, trigger: QuarantineTrigger) {
        if let Err(error) = self.quarantine_checked(chunk_id) {
            // Read fallback can still succeed. No durable quarantine or report
            // is claimed if its catalog barrier fails.
            afs_logging::warn!("dfs.local_quarantine_failed";
                "chunk" => chunk_id.0.clone(),
                "trigger" => trigger.as_str(),
                "error" => error.to_string());
        }
    }

    /// Durable → Quarantined, propagating errors (startup recovery fails
    /// instead of serving an unchecked catalog).
    pub(crate) fn quarantine_checked(&self, chunk_id: &ChunkId) -> Result<()> {
        self.store.quarantine_if_invalid(chunk_id)
    }

    /// Durable/Quarantined → Retired for one Meta GC generation: delete the
    /// files and records and persist the generation. See `collect_generation`.
    #[cfg(any(feature = "dfs", test))]
    pub(crate) fn retire_generation(&self, generation: u64, chunks: &[ChunkId]) -> Result<usize> {
        self.store.collect_generation(generation, chunks)
    }
}

/// Read-thread transition of a verified copy's proof: after a successful body
/// scan, re-install a missing or stale sidecar. The body and catalog are never
/// rewritten and every error is swallowed, so the read result is unchanged.
pub(super) fn repair_sidecar_on_read(
    path: &Path,
    proof: &SharedOutboard,
    gates: &OutboardRepairGates,
) {
    let mut hash = std::collections::hash_map::DefaultHasher::new();
    path.hash(&mut hash);
    let index = (hash.finish() % OUTBOARD_REPAIR_SHARDS as u64) as usize;
    let Ok(_repair) = gates[index].lock() else {
        return;
    };
    // Compare again under the shared gate: another reader may have repaired
    // this exact sidecar after our body scan. No catalog or body is rewritten.
    if !outboard_file_matches(path, proof).unwrap_or(false) {
        let _ = persist_outboard_file(path, proof);
    }
}

impl LocalChunkStore {
    /// Serialize with installation, then verify outside the publication lock.
    /// An old failed reader must not quarantine a healthy replacement inode.
    pub(super) fn quarantine_if_invalid(&self, chunk_id: &ChunkId) -> Result<()> {
        let admission = self.catalog_committer.admit(
            std::iter::once(chunk_id),
            catalog_commit::charge_for(std::iter::once(chunk_id), &self.device_id),
        )?;
        let record = self
            .catalog
            .lock()
            .map_err(|_| invalid("local chunk catalog lock is poisoned"))?
            .records
            .get(chunk_id)
            .cloned();
        let Some(mut record) = record else {
            return Ok(());
        };
        if CopyPhase::of_local(record.state) == CopyPhase::Quarantined {
            return Ok(());
        }
        match if record.segment_crc.is_some() {
            self.verify_crc_file(
                &self.path_for_record(&record)?,
                &record.chunk,
                record
                    .segment_crc
                    .as_ref()
                    .ok_or_else(|| corrupt(&record.chunk.id, "CRC descriptor missing"))?,
            )
        } else {
            verify_file(&self.path_for_record(&record)?, &record.chunk)
        } {
            Ok(()) => Ok(()),
            Err(error) if verified_copy_is_invalid(&error) => {
                record.state = LocalChunkState::Quarantined;
                self.commit_catalog(&admission, vec![record])?;
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    #[cfg(any(feature = "dfs", test))]
    pub(crate) fn quarantined_chunks(&self) -> Result<Vec<LocalChunkRecord>> {
        let catalog = self
            .catalog
            .lock()
            .map_err(|_| invalid("local chunk catalog lock is poisoned"))?;
        let mut records: Vec<_> = catalog
            .records
            .values()
            .filter(|record| CopyPhase::of_local(record.state) == CopyPhase::Quarantined)
            .cloned()
            .collect();
        records.sort_by(|left, right| {
            (left.catalog_revision, &left.chunk.id).cmp(&(right.catalog_revision, &right.chunk.id))
        });
        Ok(records)
    }
}

pub(super) use crate::node::fault::classify::verified_copy_is_invalid;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dfs::{CopyRole, CopyState};

    #[test]
    fn copy_phase_maps_node_local_states() {
        assert_eq!(
            CopyPhase::of_local(LocalChunkState::Durable),
            CopyPhase::Durable
        );
        assert_eq!(
            CopyPhase::of_local(LocalChunkState::Quarantined),
            CopyPhase::Quarantined
        );
    }

    #[test]
    fn copy_phase_maps_every_meta_copy_record() {
        for role in [
            CopyRole::DurableReplica,
            CopyRole::VerifiedCache,
            CopyRole::ExternalCommitted,
        ] {
            assert_eq!(
                CopyPhase::of_meta(role, CopyState::Reserved0),
                CopyPhase::Staged
            );
            assert_eq!(
                CopyPhase::of_meta(role, CopyState::Corrupt),
                CopyPhase::Quarantined
            );
            assert_eq!(
                CopyPhase::of_meta(role, CopyState::Deleting),
                CopyPhase::Retired
            );
        }
        assert_eq!(
            CopyPhase::of_meta(CopyRole::DurableReplica, CopyState::Ready),
            CopyPhase::Durable
        );
        assert_eq!(
            CopyPhase::of_meta(CopyRole::ExternalCommitted, CopyState::Ready),
            CopyPhase::Durable
        );
        assert_eq!(
            CopyPhase::of_meta(CopyRole::VerifiedCache, CopyState::Ready),
            CopyPhase::VerifiedCache
        );
    }

    #[test]
    fn quarantine_entry_rechecks_the_file_and_is_synchronous() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let staged = StagedChunk::new(OperationId::new("lifecycle"), b"abcdef".to_vec());
        store.put(staged.clone()).unwrap();
        let id = staged.chunk.id.clone();

        // A healthy copy is re-verified and left Durable.
        store
            .lifecycle()
            .quarantine(&id, QuarantineTrigger::LocalRead);
        assert!(store.quarantined_chunks().unwrap().is_empty());

        // A corrupt copy is quarantined before the entry returns.
        fs::write(temp.path().join("chunks").join(&id.0), b"abXdef").unwrap();
        store
            .lifecycle()
            .quarantine(&id, QuarantineTrigger::LocalRead);
        let quarantined = store.quarantined_chunks().unwrap();
        assert_eq!(quarantined.len(), 1);
        assert_eq!(quarantined[0].chunk.id, id);

        // Retirement removes the copy through the same entry.
        assert!(store.lifecycle().retire_generation(1, &[id]).unwrap() >= 1);
        assert!(store.quarantined_chunks().unwrap().is_empty());
    }
}
