//! Local copy transitions from Durable to Quarantined.

use super::*;

impl LocalChunkStore {
    /// Serialize with installation, then verify outside the publication lock.
    /// An old failed reader must not quarantine a healthy replacement inode.
    pub(crate) fn quarantine_if_invalid(&self, chunk_id: &ChunkId) -> Result<()> {
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
        if record.state == LocalChunkState::Quarantined {
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

    pub(crate) fn try_quarantine(&self, chunk_id: &ChunkId) {
        if let Err(error) = self.quarantine_if_invalid(chunk_id) {
            // Read fallback can still succeed. No durable quarantine or report
            // is claimed if its catalog barrier fails.
            afs_logging::warn!("dfs.local_quarantine_failed";
                "chunk" => chunk_id.0.clone(), "error" => error.to_string());
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
            .filter(|record| record.state == LocalChunkState::Quarantined)
            .cloned()
            .collect();
        records.sort_by(|left, right| {
            (left.catalog_revision, &left.chunk.id).cmp(&(right.catalog_revision, &right.chunk.id))
        });
        Ok(records)
    }
}

pub(super) fn verified_copy_is_invalid(error: &Error) -> bool {
    matches!(
        error.kind(),
        afs_error::ErrorKind::DataLoss | afs_error::ErrorKind::NotFound
    )
}
