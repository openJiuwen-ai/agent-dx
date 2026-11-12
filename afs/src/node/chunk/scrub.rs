//! Background scrub, store side (S4-16): pick the next durable copies in id
//! order and verify one copy's complete body.
//!
//! A copy found missing or corrupt goes through the same `CopyLifecycle`
//! quarantine as a failed read, so the existing report → repair chain
//! (`node::fault::repair`) picks it up. The scrubber reads like any verified
//! reader: no file, catalog record or sidecar is written except by that
//! quarantine, so it needs no exclusion against GC or repair. A copy retired
//! or replaced between batch selection and verification is skipped.

use std::collections::BinaryHeap;

use super::*;

/// One durable local copy the scrubber may verify.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ScrubItem {
    pub(crate) chunk_id: ChunkId,
    /// Logical body length, the bytes one verification reads.
    pub(crate) length: u64,
}

/// What verifying one copy found.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ScrubOutcome {
    /// The complete body verified; carries the bytes read.
    Verified(u64),
    /// The copy was missing or corrupt and is now quarantined.
    Quarantined,
    /// The copy was retired, replaced or already quarantined meanwhile, or a
    /// re-check found it healthy: nothing to count.
    Skipped,
}

impl LocalChunkStore {
    /// Up to `limit` durable copies whose id sorts after `after`, in id order.
    /// Holds the catalog lock for one pass over the records.
    pub(crate) fn scrub_batch(
        &self,
        after: Option<&ChunkId>,
        limit: usize,
    ) -> Result<Vec<ScrubItem>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let catalog = self
            .catalog
            .lock()
            .map_err(|_| invalid("local chunk catalog lock is poisoned"))?;
        // Max-heap of the smallest `limit` ids seen so far.
        let mut smallest: BinaryHeap<(&ChunkId, u64)> = BinaryHeap::new();
        for (id, record) in &catalog.records {
            if record.state != LocalChunkState::Durable || after.is_some_and(|after| id <= after) {
                continue;
            }
            if smallest.len() < limit {
                smallest.push((id, record.chunk.length));
            } else if smallest.peek().is_some_and(|(largest, _)| id < *largest) {
                smallest.pop();
                smallest.push((id, record.chunk.length));
            }
        }
        Ok(smallest
            .into_sorted_vec()
            .into_iter()
            .map(|(id, length)| ScrubItem {
                chunk_id: id.clone(),
                length,
            })
            .collect())
    }

    /// Verify one durable copy's complete body. Missing or corrupt copies are
    /// quarantined (trigger `scrub`); other errors (permission, transient I/O)
    /// are returned and change nothing.
    pub(crate) fn scrub_verify(&self, chunk_id: &ChunkId) -> Result<ScrubOutcome> {
        if !self.scrub_is_durable(chunk_id)? {
            return Ok(ScrubOutcome::Skipped);
        }
        let verified = self
            .open_cataloged(chunk_id)
            .and_then(|reader| reader.verify_full_and_build().map(|()| reader.chunk.length));
        match verified {
            Ok(length) => Ok(ScrubOutcome::Verified(length)),
            Err(error) if verified_copy_is_invalid(&error) => {
                // Re-verifies the current file under the catalog barrier, so
                // a replacement installed meanwhile is never quarantined.
                self.lifecycle()
                    .quarantine(chunk_id, QuarantineTrigger::Scrub);
                if self.scrub_is_quarantined(chunk_id)? {
                    Ok(ScrubOutcome::Quarantined)
                } else {
                    Ok(ScrubOutcome::Skipped)
                }
            }
            Err(error) => Err(error),
        }
    }

    fn scrub_is_durable(&self, chunk_id: &ChunkId) -> Result<bool> {
        Ok(self.scrub_state(chunk_id)? == Some(LocalChunkState::Durable))
    }

    fn scrub_is_quarantined(&self, chunk_id: &ChunkId) -> Result<bool> {
        Ok(self.scrub_state(chunk_id)? == Some(LocalChunkState::Quarantined))
    }

    fn scrub_state(&self, chunk_id: &ChunkId) -> Result<Option<LocalChunkState>> {
        Ok(self
            .catalog
            .lock()
            .map_err(|_| invalid("local chunk catalog lock is poisoned"))?
            .records
            .get(chunk_id)
            .map(|record| record.state))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store_with(names: &[&str]) -> (tempfile::TempDir, LocalChunkStore, Vec<StagedChunk>) {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let staged: Vec<_> = names
            .iter()
            .map(|name| StagedChunk::new(OperationId::new(*name), name.repeat(100).into_bytes()))
            .collect();
        for item in &staged {
            store.put(item.clone()).unwrap();
        }
        (temp, store, staged)
    }

    #[test]
    fn batches_walk_every_durable_copy_once_in_id_order() {
        let names: Vec<String> = (0..10).map(|n| format!("scrub-{n}")).collect();
        let names: Vec<&str> = names.iter().map(String::as_str).collect();
        let (_temp, store, staged) = store_with(&names);
        let mut expected: Vec<_> = staged.iter().map(|item| item.chunk.id.clone()).collect();
        expected.sort();
        let mut seen = Vec::new();
        let mut after = None;
        loop {
            let batch = store.scrub_batch(after.as_ref(), 3).unwrap();
            if batch.is_empty() {
                break;
            }
            assert!(batch.len() <= 3);
            after = batch.last().map(|item| item.chunk_id.clone());
            seen.extend(batch.into_iter().map(|item| item.chunk_id));
        }
        assert_eq!(seen, expected);
        assert!(store.scrub_batch(None, 0).unwrap().is_empty());
    }

    #[test]
    fn a_corrupt_copy_is_quarantined_and_reported_by_the_repair_list() {
        let (temp, store, staged) = store_with(&["healthy", "flipped", "missing"]);
        let [healthy, flipped, missing] = [0, 1, 2].map(|n| staged[n].chunk.id.clone());
        let mut body = fs::read(temp.path().join("chunks").join(&flipped.0)).unwrap();
        body[17] ^= 0xff;
        fs::write(temp.path().join("chunks").join(&flipped.0), body).unwrap();
        fs::remove_file(temp.path().join("chunks").join(&missing.0)).unwrap();

        assert_eq!(
            store.scrub_verify(&healthy).unwrap(),
            ScrubOutcome::Verified(staged[0].chunk.length)
        );
        assert_eq!(
            store.scrub_verify(&flipped).unwrap(),
            ScrubOutcome::Quarantined
        );
        assert_eq!(
            store.scrub_verify(&missing).unwrap(),
            ScrubOutcome::Quarantined
        );
        let mut quarantined: Vec<_> = store
            .quarantined_chunks()
            .unwrap()
            .into_iter()
            .map(|record| record.chunk.id)
            .collect();
        quarantined.sort();
        let mut want = vec![flipped.clone(), missing];
        want.sort();
        assert_eq!(quarantined, want, "the repair worker reports these");
        // A quarantined copy is no longer selected or verified again.
        let batch: Vec<_> = store
            .scrub_batch(None, 10)
            .unwrap()
            .into_iter()
            .map(|item| item.chunk_id)
            .collect();
        assert_eq!(batch, vec![healthy]);
        assert_eq!(store.scrub_verify(&flipped).unwrap(), ScrubOutcome::Skipped);
    }

    #[test]
    fn a_retired_copy_is_skipped() {
        let (_temp, store, staged) = store_with(&["retired"]);
        let id = staged[0].chunk.id.clone();
        assert_eq!(store.scrub_batch(None, 4).unwrap().len(), 1);
        assert!(
            store
                .lifecycle()
                .retire_generation(1, std::slice::from_ref(&id))
                .unwrap()
                >= 1
        );
        assert_eq!(store.scrub_verify(&id).unwrap(), ScrubOutcome::Skipped);
        assert!(store.quarantined_chunks().unwrap().is_empty());
    }
}
