//! Pinned, verified local Chunk readers.

use super::*;

#[derive(Debug)]
pub struct PinnedChunkReader {
    pub(super) segment_crc: Option<super::super::chunk_crc32c::CrcTable>,
    pub(super) file: File,
    pub(super) chunk: ChunkObject,
    pub(super) outboard_path: PathBuf,
    pub(super) outboard_budget: Arc<OutboardBudget>,
    pub(super) outboard_repairs: Arc<OutboardRepairGates>,
    pub(super) outboard: Mutex<Option<SharedOutboard>>,
    pub(super) proof_identity: Mutex<PeerFileIdentity>,
    pub(super) read_work: Option<super::super::metrics::work::ReadWorkMetrics>,
    // Only a private peer-serving reader may opt out; local/recovery readers stay checked.
    pub(super) verify_crc_body: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct VerifiedRangeCopy {
    pub chunk_offset: u64,
    pub length: usize,
    pub output_offset: usize,
}

impl PinnedChunkReader {
    fn crc_ranges(&self, ranges: &[VerifiedRangeCopy], out: &mut [u8]) -> Result<Vec<usize>> {
        self.crc_ranges_with_body_check(ranges, out, self.verify_crc_body)
    }

    fn crc_ranges_with_body_check(
        &self,
        ranges: &[VerifiedRangeCopy],
        out: &mut [u8],
        verify_body: bool,
    ) -> Result<Vec<usize>> {
        let table = self
            .segment_crc
            .as_ref()
            .ok_or_else(|| invalid("CRC descriptor missing"))?;
        if let Some(metrics) = &self.read_work {
            metrics.hash("crc_descriptor_hash", table.descriptor_input_bytes());
        }
        if !self.chunk.is_segment_crc32c()
            || !self.chunk.valid_immutable_identity()
            || !table.valid_shape(self.chunk.length)
            || table.descriptor_digest() != self.chunk.content_digest
        {
            return Err(corrupt(&self.chunk.id, "CRC descriptor identity differs"));
        }
        let before = PeerFileIdentity::read(&self.file)?;
        if before.length != self.chunk.length {
            return Err(corrupt(&self.chunk.id, "CRC body length differs"));
        }
        let mut scratch = Vec::new();
        let mut counts = Vec::with_capacity(ranges.len());
        for r in ranges {
            let allowed =
                (self.chunk.length.saturating_sub(r.chunk_offset)).min(r.length as u64) as usize;
            let output_end = r
                .output_offset
                .checked_add(allowed)
                .ok_or_else(|| invalid("CRC output overflow"))?;
            if output_end > out.len() {
                return Err(invalid("CRC output exceeds buffer"));
            }
            if allowed == 0 {
                counts.push(0);
                continue;
            }
            let end = r.chunk_offset + allowed as u64;
            let segment = super::super::chunk_crc32c::SEGMENT;
            let complete = (r.chunk_offset as usize).is_multiple_of(segment)
                && ((end as usize).is_multiple_of(segment) || end == self.chunk.length);
            if !verify_body || complete {
                let target = &mut out[r.output_offset..output_end];
                let mut read = 0;
                while read < allowed {
                    let got = read_positioned(
                        &self.file,
                        r.chunk_offset + read as u64,
                        &mut target[read..],
                    )?;
                    if got == 0 {
                        return Err(corrupt(&self.chunk.id, "short CRC body"));
                    }
                    read += got;
                }
                if verify_body {
                    for (relative, bytes) in target.chunks(segment).enumerate() {
                        let crc_scope = self
                            .read_work
                            .as_ref()
                            .and_then(|m| m.coarse_diagnostic("crc_storage", bytes.len() as u64));
                        if let Some(m) = &self.read_work {
                            m.crc_read("storage_segments", bytes.len() as u64);
                        }
                        let actual = super::super::chunk_crc32c::checksum(bytes);
                        drop(crc_scope);
                        if actual != table.checksums[r.chunk_offset as usize / segment + relative] {
                            return Err(corrupt(&self.chunk.id, "CRC32C segment mismatch"));
                        }
                    }
                }
                counts.push(allowed);
                continue;
            }
            scratch.resize(segment, 0);
            for index in (r.chunk_offset as usize / super::super::chunk_crc32c::SEGMENT)
                ..=((end as usize - 1) / super::super::chunk_crc32c::SEGMENT)
            {
                let begin = index * super::super::chunk_crc32c::SEGMENT;
                let n = (self.chunk.length as usize - begin).min(scratch.len());
                let mut read = 0;
                while read < n {
                    let got =
                        read_positioned(&self.file, (begin + read) as u64, &mut scratch[read..n])?;
                    if got == 0 {
                        return Err(corrupt(&self.chunk.id, "short CRC body"));
                    }
                    read += got;
                }
                if verify_body {
                    let crc_scope = self
                        .read_work
                        .as_ref()
                        .and_then(|metrics| metrics.coarse_diagnostic("crc_storage", n as u64));
                    if let Some(metrics) = &self.read_work {
                        metrics.crc_read("storage_segments", n as u64);
                    }
                    let actual_crc = super::super::chunk_crc32c::checksum(&scratch[..n]);
                    drop(crc_scope);
                    if actual_crc != table.checksums[index] {
                        return Err(corrupt(&self.chunk.id, "CRC32C segment mismatch"));
                    }
                }
                let a = (begin as u64).max(r.chunk_offset);
                let b = ((begin + n) as u64).min(end);
                let dest = r.output_offset + (a - r.chunk_offset) as usize;
                if let Some(metrics) = &self.read_work {
                    metrics.copy("crc_storage_to_range", b - a);
                }
                out[dest..dest + (b - a) as usize]
                    .copy_from_slice(&scratch[(a as usize - begin)..(b as usize - begin)]);
            }
            counts.push(allowed);
        }
        if PeerFileIdentity::read(&self.file)? != before {
            return Err(corrupt(&self.chunk.id, "CRC body identity changed"));
        }
        Ok(counts)
    }

    #[cfg(feature = "dfs")]
    pub(crate) fn chunk(&self) -> &ChunkObject {
        &self.chunk
    }

    pub(super) fn load_outboard(&self) -> Result<Option<SharedOutboard>> {
        if self.chunk.length > 4 * 1024 * 1024
            || ChunkObject::from_content_addressed_id(&self.chunk.id).as_ref() != Some(&self.chunk)
        {
            return Ok(None);
        }
        let Ok(mut cached) = self.outboard.lock() else {
            return Ok(None);
        };
        if let Some(proof) = cached.as_ref() {
            return Ok(Some(proof.clone()));
        }
        match load_outboard_file(&self.outboard_path, &self.chunk, &self.outboard_budget) {
            Ok(proof) => {
                if proof.try_mark_resident() {
                    *cached = Some(proof.clone());
                }
                // Resident pressure limits retention, not this already charged
                // temporary proof's ability to validate the current range.
                Ok(Some(proof))
            }
            Err(error) if error.code() == afs_error::IO_CAPACITY => Err(error),
            Err(_) => Ok(None),
        }
    }

    fn remember_outboard(&self, proof: Option<SharedOutboard>) {
        if let Some(proof) = proof {
            // Transient proof remains in the original total ledger throughout
            // repair; a full resident subset must not prevent bounded repair.
            repair_sidecar_on_read(&self.outboard_path, &proof, &self.outboard_repairs);
            if proof.try_mark_resident()
                && let Ok(mut cached) = self.outboard.lock()
            {
                *cached = Some(proof);
            }
        }
    }

    fn full_scan(
        &self,
        ranges: &[VerifiedRangeCopy],
        out: &mut [u8],
        force: bool,
        build_outboard: bool,
    ) -> Result<Vec<usize>> {
        if self.segment_crc.is_some() {
            return self.crc_ranges(ranges, out);
        }
        let before = PeerFileIdentity::read(&self.file)?;
        let builder = (build_outboard
            && ChunkObject::from_content_addressed_id(&self.chunk.id).as_ref()
                == Some(&self.chunk))
        .then(|| OutboardBuilder::new(self.chunk.length, self.outboard_budget.clone()));
        let (counts, proof) = verified_ranges_into_building(
            &self.file,
            &self.chunk,
            ranges,
            out,
            force,
            self.read_work.as_ref(),
            builder,
        )?;
        if PeerFileIdentity::read(&self.file)? != before {
            return Err(corrupt(
                &self.chunk.id,
                "identity changed during full verification",
            ));
        }
        *self
            .proof_identity
            .lock()
            .map_err(|_| invalid("pinned proof identity lock poisoned"))? = before;
        self.remember_outboard(proof);
        Ok(counts)
    }

    pub(super) fn verify_full_and_build(&self) -> Result<()> {
        if self.segment_crc.is_some() {
            let mut out = vec![0; super::super::chunk_crc32c::SEGMENT];
            let mut offset = 0;
            while offset < self.chunk.length {
                let length = (self.chunk.length - offset).min(out.len() as u64) as usize;
                self.crc_ranges(
                    &[VerifiedRangeCopy {
                        chunk_offset: offset,
                        length,
                        output_offset: 0,
                    }],
                    &mut out,
                )?;
                offset += length as u64;
            }
            return Ok(());
        }

        self.full_scan(&[], &mut [], true, true).map(|_| ())
    }

    fn outboard_ranges(
        &self,
        ranges: &[VerifiedRangeCopy],
        out: &mut [u8],
    ) -> Option<Result<Vec<usize>>> {
        let before = PeerFileIdentity::read(&self.file).ok()?;
        if self.proof_identity.lock().ok().as_deref() != Some(&before)
            || self.chunk.length > 4 * 1024 * 1024
            || ChunkObject::from_content_addressed_id(&self.chunk.id).as_ref() != Some(&self.chunk)
        {
            return None;
        }
        // Reserve once before loading a resident proof; transfer the same work
        // lease into validation so resident growth cannot steal its scratch.
        let Some(working) = self
            .outboard_budget
            .reserve_range_work(ranges, self.chunk.length)
        else {
            return Some(Err(outboard_capacity()));
        };
        let proof = match self.load_outboard() {
            Ok(Some(proof)) => proof,
            Ok(None) => return None,
            Err(error) => return Some(Err(error)),
        };
        let mut work = OutboardReadWork::default();
        let result = proof.read_ranges_into_with_reserved_work(
            &self.file,
            0,
            &self.chunk,
            ranges,
            out,
            OutboardReadContext {
                work: &mut work,
                working,
            },
        );
        #[cfg(test)]
        VERIFIED_SCAN_BYTES.with(|count| count.set(count.get() + work.hash_bytes));
        if let Some(metrics) = &self.read_work {
            metrics.hash("outboard_body_hash", work.hash_bytes);
            metrics.hash("outboard_parent_hash", work.parent_hash_bytes);
            metrics.copy("outboard_range_delivery", work.copied_bytes);
        }
        match result {
            Ok(counts) if PeerFileIdentity::read(&self.file).ok().as_ref() == Some(&before) => {
                Some(Ok(counts))
            }
            Err(error) if error.code() == afs_error::IO_CAPACITY => Some(Err(error)),
            _ => {
                // Capacity is not corruption. Only an invalid proof/identity
                // retires the cached metadata and permits a bounded rebuild.
                if let Ok(mut cached) = self.outboard.lock() {
                    cached.take();
                }
                None
            }
        }
    }

    fn read_or_full_scan(
        &self,
        ranges: &[VerifiedRangeCopy],
        out: &mut [u8],
        force: bool,
    ) -> Result<Vec<usize>> {
        match self.outboard_ranges(ranges, out) {
            Some(Ok(counts)) => Ok(counts),
            Some(Err(error)) if error.code() == afs_error::IO_CAPACITY => {
                if force
                    || ranges
                        .iter()
                        .any(|range| range.length != 0 && range.chunk_offset < self.chunk.length)
                {
                    self.outboard_budget.record_range_fullscan_fallback();
                }
                self.full_scan(ranges, out, force, false)
            }
            Some(Err(error)) => Err(error),
            None => self.full_scan(ranges, out, force, true),
        }
    }

    pub fn read_at(&self, offset: u64, out: &mut [u8]) -> Result<usize> {
        if offset >= self.chunk.length || out.is_empty() {
            return Ok(0);
        }
        let allowed = usize::try_from((self.chunk.length - offset).min(out.len() as u64))
            .map_err(|_| invalid("Chunk read length is too large"))?;
        // A pinned descriptor protects identity across rename, not integrity
        // after disk damage. Return the exact bytes covered by this digest,
        // rather than verifying and then issuing another unverified read.
        let mut bytes = vec![0; allowed];
        let ranges = [VerifiedRangeCopy {
            chunk_offset: offset,
            length: allowed,
            output_offset: 0,
        }];
        self.read_or_full_scan(&ranges, &mut bytes, true)?;
        out[..allowed].copy_from_slice(&bytes);
        if let Some(metrics) = &self.read_work {
            metrics.copy("pinned_range_to_output", allowed as u64);
        }
        Ok(allowed)
    }

    /// Verify the pinned file once and scatter requested ranges into an
    /// uncommitted staging buffer. The staging buffer may be dirtied before an
    /// error is returned; callers must publish it only after the outer read
    /// batch succeeds.
    #[cfg(any(feature = "dfs", test))]
    pub(crate) fn read_ranges_at_uncommitted(
        &self,
        ranges: &[VerifiedRangeCopy],
        out: &mut [u8],
    ) -> Result<Vec<usize>> {
        self.read_or_full_scan(ranges, out, false)
    }

    #[cfg(feature = "dfs")]
    pub(crate) fn read_ranges_at_uncommitted_fullscan(
        &self,
        ranges: &[VerifiedRangeCopy],
        out: &mut [u8],
    ) -> Result<Vec<usize>> {
        if self.segment_crc.is_some() {
            return self.crc_ranges(ranges, out);
        }
        verified_ranges_into(
            &self.file,
            &self.chunk,
            ranges,
            out,
            false,
            self.read_work.as_ref(),
        )
    }
}

#[cfg(unix)]
pub(super) fn read_positioned(file: &File, offset: u64, out: &mut [u8]) -> Result<usize> {
    loop {
        match file.read_at(out, offset) {
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            result => return result.map_err(Error::from),
        }
    }
}

#[cfg(not(unix))]
pub(super) fn read_positioned(file: &File, offset: u64, out: &mut [u8]) -> Result<usize> {
    let mut file = file.try_clone().map_err(Error::from)?;
    use std::io::{Seek, SeekFrom};
    file.seek(SeekFrom::Start(offset)).map_err(Error::from)?;
    file.read(out).map_err(Error::from)
}
