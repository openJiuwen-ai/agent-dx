//! On-disk reader for sealed, uncompressed OverlayBD LSMT 1.1 layers.

use std::io;

pub(super) const METADATA_SIZE: usize = 4096;
pub(super) const SECTOR_SIZE: u64 = 512;
const RECORD_SIZE: usize = 16;
pub(super) const MAX_INDEX_BYTES: usize = 16 * 1024 * 1024;
const MAGIC: [u8; 24] = [
    0x4c, 0x53, 0x4d, 0x54, 0, 1, 2, 0, 0x65, 0x7e, 0x63, 0xd2, 0x94, 0x44, 0x08, 0x4c, 0xa2, 0xd2,
    0xc8, 0xec, 0x4f, 0xcf, 0xae, 0x8a,
];
const LOGICAL_MASK: u64 = (1 << 50) - 1;
const PHYSICAL_MASK: u64 = (1 << 55) - 1;

pub(super) struct Metadata {
    pub(super) virtual_size: u64,
    pub(super) index_offset: u64,
    pub(super) index_bytes: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Mapping {
    Data(u64),
    Zero,
}

struct Segment {
    start: u64,
    end: u64,
    mapping: Mapping,
}

pub(super) struct Index(Vec<Segment>);

impl Metadata {
    pub(super) fn parse(
        header: &[u8; METADATA_SIZE],
        trailer: &[u8; METADATA_SIZE],
        blob_size: u64,
    ) -> io::Result<Self> {
        for (metadata, expected_flags) in [(header, 7), (trailer, 6)] {
            let flags = u32::from_le_bytes(field(metadata, 28)?);
            if !metadata.starts_with(&MAGIC)
                || u32::from_le_bytes(field(metadata, 24)?) != 390
                || metadata[132..134] != [1, 1]
                || flags & !(1 << 5) != expected_flags
            {
                return Err(invalid("expected a sealed, raw LSMT 1.1 layer"));
            }
        }
        let index_offset = u64::from_le_bytes(field(trailer, 32)?);
        let records = u64::from_le_bytes(field(trailer, 40)?);
        let virtual_size = u64::from_le_bytes(field(trailer, 48)?);
        let index_bytes = usize::try_from(records)
            .ok()
            .and_then(|count| count.checked_mul(RECORD_SIZE))
            .filter(|bytes| *bytes <= MAX_INDEX_BYTES)
            .ok_or_else(|| invalid("LSMT index exceeds the reader limit"))?;
        if virtual_size == 0
            || !virtual_size.is_multiple_of(SECTOR_SIZE)
            || index_offset < METADATA_SIZE as u64
            || !index_offset.is_multiple_of(SECTOR_SIZE)
            || index_offset
                .checked_add(index_bytes as u64)
                .zip(blob_size.checked_sub(METADATA_SIZE as u64))
                .is_none_or(|(end, trailer_start)| end > trailer_start)
        {
            return Err(invalid("invalid LSMT index or virtual disk size"));
        }
        Ok(Self {
            virtual_size,
            index_offset,
            index_bytes,
        })
    }

    pub(super) fn parse_index(&self, bytes: &[u8]) -> io::Result<Index> {
        if bytes.len() != self.index_bytes {
            return Err(invalid("incomplete LSMT index"));
        }
        let mut segments = Vec::new();
        let mut previous_end = 0;
        let mut padding = false;
        for record in bytes.chunks_exact(RECORD_SIZE) {
            let logical = u64::from_le_bytes(field(record, 0)?);
            let physical = u64::from_le_bytes(field(record, 8)?);
            if logical == LOGICAL_MASK && physical == 0 {
                padding = true;
                continue;
            }
            let start = (logical & LOGICAL_MASK) * SECTOR_SIZE;
            let length = (logical >> 50) * SECTOR_SIZE;
            let end = start + length;
            if padding
                || length == 0
                || start < previous_end
                || end > self.virtual_size
                || physical >> 56 != 0
            {
                return Err(invalid("invalid LSMT segment"));
            }
            let mapping = if physical & (1 << 55) != 0 {
                Mapping::Zero
            } else {
                let offset = (physical & PHYSICAL_MASK) * SECTOR_SIZE;
                if offset < METADATA_SIZE as u64
                    || offset
                        .checked_add(length)
                        .is_none_or(|end| end > self.index_offset)
                {
                    return Err(invalid("LSMT segment points outside layer data"));
                }
                Mapping::Data(offset)
            };
            segments.push(Segment {
                start,
                end,
                mapping,
            });
            previous_end = end;
        }
        Ok(Index(segments))
    }
}

impl Index {
    /// Return the mapping and the next offset at which it may change.
    pub(super) fn lookup(&self, offset: u64) -> (Option<Mapping>, u64) {
        match self
            .0
            .get(self.0.partition_point(|segment| segment.end <= offset))
        {
            Some(segment) if segment.start <= offset => {
                let mapping = match segment.mapping {
                    Mapping::Data(source) => Mapping::Data(source + (offset - segment.start)),
                    Mapping::Zero => Mapping::Zero,
                };
                (Some(mapping), segment.end)
            }
            Some(segment) => (None, segment.start),
            None => (None, u64::MAX),
        }
    }
}

fn field<const N: usize>(bytes: &[u8], offset: usize) -> io::Result<[u8; N]> {
    bytes
        .get(offset..offset + N)
        .and_then(|field| field.try_into().ok())
        .ok_or_else(|| invalid("truncated LSMT metadata"))
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[cfg(test)]
#[path = "lsmt/tests.rs"]
mod tests;
