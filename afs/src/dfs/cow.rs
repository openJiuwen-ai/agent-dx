//! Immutable extent remapping for COW; this operation never accesses chunk data.

use super::{CloneFileRange, Extent, LayoutRoot, LayoutRootId};
use afs_error::{Error, Result};

fn invalid() -> Error {
    Error::coded(
        afs_error::META_CATALOG_INVALID_REQUEST,
        "invalid COW clone range",
    )
}

pub fn clone_file_layout(
    base: &LayoutRoot,
    source: &LayoutRoot,
    range: &CloneFileRange,
    id: LayoutRootId,
) -> Result<LayoutRoot> {
    let source_end = range
        .source_offset
        .checked_add(range.length)
        .ok_or_else(invalid)?;
    let destination_end = range
        .destination_offset
        .checked_add(range.length)
        .ok_or_else(invalid)?;
    if range.length == 0 || source_end > source.file_length {
        return Err(invalid());
    }
    let mut extents = Vec::new();
    for extent in &base.inline_extents {
        let end = extent
            .file_offset
            .checked_add(extent.length)
            .ok_or_else(invalid)?;
        if extent.file_offset < range.destination_offset {
            push_slice(
                &mut extents,
                extent,
                extent.file_offset,
                end.min(range.destination_offset),
                extent.file_offset,
            )?;
        }
        if end > destination_end {
            let start = extent.file_offset.max(destination_end);
            push_slice(&mut extents, extent, start, end, start)?;
        }
    }
    for extent in &source.inline_extents {
        let start = extent.file_offset.max(range.source_offset);
        let end = extent
            .file_offset
            .checked_add(extent.length)
            .ok_or_else(invalid)?
            .min(source_end);
        if start < end {
            let target = range
                .destination_offset
                .checked_add(start - range.source_offset)
                .ok_or_else(invalid)?;
            push_slice(&mut extents, extent, start, end, target)?;
        }
    }
    extents.sort_by_key(|extent| extent.file_offset);
    Ok(LayoutRoot {
        id,
        file_length: base.file_length.max(destination_end),
        inline_extents: extents,
    })
}

fn push_slice(
    out: &mut Vec<Extent>,
    extent: &Extent,
    start: u64,
    end: u64,
    destination: u64,
) -> Result<()> {
    if start < end {
        out.push(Extent {
            file_offset: destination,
            length: end - start,
            chunk_id: extent.chunk_id.clone(),
            chunk_offset: extent
                .chunk_offset
                .checked_add(start - extent.file_offset)
                .ok_or_else(invalid)?,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dfs::{ChunkId, FileVersionId};

    fn extent(offset: u64, length: u64, chunk: &str) -> Extent {
        Extent {
            file_offset: offset,
            length,
            chunk_id: ChunkId::new(chunk),
            chunk_offset: 0,
        }
    }

    #[test]
    fn range_clone_preserves_destination_edges_source_holes_and_chunk_offsets() {
        let base = LayoutRoot {
            id: LayoutRootId::new("base"),
            file_length: 20,
            inline_extents: vec![extent(0, 20, "old")],
        };
        let source = LayoutRoot {
            id: LayoutRootId::new("source"),
            file_length: 12,
            inline_extents: vec![extent(0, 4, "a"), extent(8, 4, "b")],
        };
        let range = CloneFileRange {
            version: FileVersionId::new("version"),
            source_offset: 2,
            destination_offset: 5,
            length: 8,
        };
        let result = clone_file_layout(&base, &source, &range, LayoutRootId::new("clone")).unwrap();
        let shape: Vec<_> = result
            .inline_extents
            .iter()
            .map(|e| {
                (
                    e.file_offset,
                    e.length,
                    e.chunk_id.0.as_str(),
                    e.chunk_offset,
                )
            })
            .collect();
        assert_eq!(
            shape,
            vec![
                (0, 5, "old", 0),
                (5, 2, "a", 2),
                (11, 2, "b", 0),
                (13, 7, "old", 13)
            ]
        );
        assert_eq!(result.file_length, 20);
        assert_eq!(
            source.inline_extents,
            vec![extent(0, 4, "a"), extent(8, 4, "b")]
        );
    }

    #[test]
    fn clone_range_rejects_overflow_and_source_eof() {
        let base = LayoutRoot {
            id: LayoutRootId::new("empty"),
            file_length: 0,
            inline_extents: vec![],
        };
        for (source_offset, destination_offset, length) in
            [(0, 0, 1), (0, 0, 0), (u64::MAX, 0, 1), (0, u64::MAX, 1)]
        {
            let range = CloneFileRange {
                version: FileVersionId::new("v"),
                source_offset,
                destination_offset,
                length,
            };
            assert!(clone_file_layout(&base, &base, &range, LayoutRootId::new("clone")).is_err());
        }
    }
}
