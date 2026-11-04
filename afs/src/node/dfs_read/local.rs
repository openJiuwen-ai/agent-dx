//! `LocalSource`: verified local copies; failures leave a remote mask.

use super::*;

impl DfsReadEngine {
    /// `LocalSource`: serve every operation whose chunk has a verified local
    /// copy. Failed and short local reads quarantine a copy that is really bad
    /// and leave their operations in the remote mask.
    pub(super) fn read_local_copies(
        &self,
        batch: &ReadBatch,
        out: &mut [u8],
    ) -> Result<SourceOutcome> {
        self.local
            .record_read_check(super::super::read_work::ReadCheck::BatchGroup);
        let remote = crate::node::bridge::local_io(|| -> Result<RemoteMask> {
            let mut groups: Vec<(ChunkId, Vec<usize>)> = Vec::new();
            let mut positions: HashMap<ChunkId, usize> = HashMap::new();
            for (index, op) in batch.ops.iter().enumerate() {
                if op.length == 0 {
                    continue;
                }
                if let Some(group_index) = positions.get(&op.chunk_id).copied() {
                    groups[group_index].1.push(index);
                } else {
                    positions.insert(op.chunk_id.clone(), groups.len());
                    groups.push((op.chunk_id.clone(), vec![index]));
                }
            }
            let mut remote = RemoteMask::new(batch.ops.len());
            for (chunk_id, indices) in groups {
                self.read_local_group(batch, &chunk_id, &indices, out, &mut remote)?;
            }
            Ok(remote)
        })?;
        Ok(if remote.is_empty() {
            SourceOutcome::Done
        } else {
            SourceOutcome::Partial(remote)
        })
    }

    /// One chunk's operations from its local copy, or into the remote mask.
    fn read_local_group(
        &self,
        batch: &ReadBatch,
        chunk_id: &ChunkId,
        indices: &[usize],
        out: &mut [u8],
        remote: &mut RemoteMask,
    ) -> Result<()> {
        let reader = match self.local_reader(chunk_id) {
            Ok(reader) => reader,
            Err(error) => {
                self.handle_local_read_failure(chunk_id, &error)?;
                for &index in indices {
                    remote.mark(index);
                }
                return Ok(());
            }
        };
        // Published layout ChunkIds are trusted content addresses. The
        // local catalog/pinned descriptor cannot substitute a different
        // standard root or object shape for that Meta layout identity.
        let expected = if batch.file_version_id.is_some() {
            ChunkObject::from_content_addressed_id(chunk_id)
        } else {
            None
        };
        if let Some(expected) = &expected {
            validate_local_object_identity(expected, reader.chunk())?;
        }
        let mut ranges = Vec::with_capacity(indices.len());
        for &index in indices {
            let op = &batch.ops[index];
            let length =
                usize::try_from(op.length).map_err(|_| invalid("DFS read length is too large"))?;
            ranges.push(VerifiedRangeCopy {
                chunk_offset: op.chunk_offset,
                length,
                output_offset: op.output_offset,
            });
        }
        let read = if expected.is_some() {
            reader.read_ranges_at_uncommitted(&ranges, out)
        } else {
            // Legacy IDs and unpublished local reads retain the previous
            // complete-root path; they do not claim a Meta-anchored proof.
            reader.read_ranges_at_uncommitted_fullscan(&ranges, out)
        };
        match read {
            Ok(counts) => {
                let mut short = false;
                for (&index, count) in indices.iter().zip(counts) {
                    let expected = usize::try_from(batch.ops[index].length)
                        .map_err(|_| invalid("DFS read length is too large"))?;
                    if count != expected {
                        remote.mark(index);
                        short = true;
                    }
                }
                if short {
                    let error = corrupt("local Chunk ended before the requested range");
                    self.handle_local_read_failure(chunk_id, &error)?;
                }
            }
            Err(error) => {
                self.handle_local_read_failure(chunk_id, &error)?;
                for &index in indices {
                    remote.mark(index);
                }
            }
        }
        Ok(())
    }

    fn handle_local_read_failure(&self, chunk_id: &ChunkId, error: &Error) -> Result<()> {
        if crate::node::fault::classify::verified_copy_is_invalid(error) {
            self.local.lifecycle().quarantine(
                chunk_id,
                crate::node::chunk::QuarantineTrigger::DfsLocalRead,
            );
        }
        // A cached pin can outlive a corrupt file's replacement. Drop it on
        // failure so the next attempt opens the current physical copy instead
        // of retrying the retired descriptor.
        self.readers
            .lock()
            .map_err(|_| unavailable("DFS local reader cache is poisoned"))?
            .remove(chunk_id);
        Ok(())
    }

    pub(super) fn local_reader(&self, chunk_id: &ChunkId) -> Result<Arc<PinnedChunkReader>> {
        self.local
            .record_read_check(super::super::read_work::ReadCheck::LocalProbe);
        if let Some(reader) = self
            .readers
            .lock()
            .map_err(|_| unavailable("DFS local reader cache is poisoned"))?
            .get(chunk_id)
            .cloned()
        {
            return Ok(reader);
        }
        let reader = Arc::new(self.local.open_verified(chunk_id)?);
        #[cfg(test)]
        {
            // Deterministically hold concurrent cold opens before installation.
            // No cache or hook mutex is held while the test barrier waits.
            let barrier = self.local_reader_install_barrier.lock().unwrap().clone();
            if let Some(barrier) = barrier {
                barrier.wait();
            }
        }
        let mut readers = self
            .readers
            .lock()
            .map_err(|_| unavailable("DFS local reader cache is poisoned"))?;
        // Another cold reader may have installed the same identity while this
        // descriptor was opening/verifying outside the cache lock. Share its
        // canonical pin; the redundant temporary descriptor is released here.
        if let Some(cached) = readers.get(chunk_id) {
            return Ok(cached.clone());
        }
        readers.insert(chunk_id.clone(), reader.clone());
        Ok(reader)
    }
}
