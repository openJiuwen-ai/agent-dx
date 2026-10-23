//! Logical read batches stay independent of the native Host-window size.

use afs_error::{Error, Result};
use afs_protocol::node_data::DfsReadRangesRequest;

use crate::node::dfs_read::{
    MAX_DFS_GROUP_BYTES, MAX_DFS_GROUP_OPS, MAX_DFS_READ_BYTES, MAX_DFS_READ_OPS,
};

fn invalid(message: &str) -> Error {
    Error::coded(afs_error::NODE_TRANSFER_INVALID, message)
}

/// Normalize only packed destinations; preserve every chunk/copy/grant identity.
pub(crate) fn pack_read_batch(read: DfsReadRangesRequest) -> Result<DfsReadRangesRequest> {
    pack_with_limits(read, MAX_DFS_READ_OPS, MAX_DFS_READ_BYTES)
}

/// Group batches carry the same identity with the group protocol budget.
pub(crate) fn pack_read_group(read: DfsReadRangesRequest) -> Result<DfsReadRangesRequest> {
    pack_with_limits(read, MAX_DFS_GROUP_OPS, MAX_DFS_GROUP_BYTES)
}

fn pack_with_limits(
    mut read: DfsReadRangesRequest,
    max_ops: usize,
    max_bytes: u64,
) -> Result<DfsReadRangesRequest> {
    if read.operations.is_empty() || read.operations.len() > max_ops {
        return Err(invalid("HIXL logical batch operation count is invalid"));
    }
    let mut length = 0_u64;
    for (index, operation) in read.operations.iter_mut().enumerate() {
        if usize::try_from(operation.operation_index).ok() != Some(index)
            || operation.length == 0
            || operation
                .chunk_offset
                .checked_add(operation.length)
                .is_none()
        {
            return Err(invalid("HIXL logical batch range is invalid"));
        }
        operation.destination_offset = length;
        length = length
            .checked_add(operation.length)
            .filter(|length| *length <= max_bytes)
            .ok_or_else(|| invalid("HIXL logical batch exceeds the DFS byte budget"))?;
    }
    Ok(read)
}

/// Derive a native window from an exact packed parent, without widening grants.
pub(crate) fn slice_read_batch(
    batch: &DfsReadRangesRequest,
    offset: u64,
    length: u64,
    attempt_id: &str,
) -> Result<DfsReadRangesRequest> {
    slice_with_limits(
        batch,
        offset,
        length,
        attempt_id,
        MAX_DFS_READ_OPS,
        MAX_DFS_READ_BYTES,
    )
}

/// Derives a window from a group-sized packed parent.
pub(crate) fn slice_read_group(
    batch: &DfsReadRangesRequest,
    offset: u64,
    length: u64,
    attempt_id: &str,
) -> Result<DfsReadRangesRequest> {
    slice_with_limits(
        batch,
        offset,
        length,
        attempt_id,
        MAX_DFS_GROUP_OPS,
        MAX_DFS_GROUP_BYTES,
    )
}

fn slice_with_limits(
    batch: &DfsReadRangesRequest,
    offset: u64,
    length: u64,
    attempt_id: &str,
    max_ops: usize,
    max_bytes: u64,
) -> Result<DfsReadRangesRequest> {
    let packed = pack_with_limits(batch.clone(), max_ops, max_bytes)?;
    if &packed != batch || length == 0 || attempt_id.is_empty() {
        return Err(invalid("HIXL logical batch or window identity is invalid"));
    }
    let end = offset
        .checked_add(length)
        .ok_or_else(|| invalid("HIXL logical window offset overflow"))?;
    let total = batch.operations.iter().try_fold(0_u64, |sum, operation| {
        sum.checked_add(operation.length)
            .ok_or_else(|| invalid("HIXL logical batch length overflow"))
    })?;
    if end > total {
        return Err(invalid("HIXL window exceeds its logical batch"));
    }
    let mut window = DfsReadRangesRequest {
        attempt_id: attempt_id.to_owned(),
        operations: Vec::new(),
        ..batch.clone()
    };
    let mut copied = 0_u64;
    for operation in &batch.operations {
        let operation_end = operation
            .destination_offset
            .checked_add(operation.length)
            .ok_or_else(|| invalid("HIXL packed range overflow"))?;
        let begin = offset.max(operation.destination_offset);
        let finish = end.min(operation_end);
        if begin >= finish {
            continue;
        }
        let mut part = operation.clone();
        part.operation_index = u32::try_from(window.operations.len())
            .map_err(|_| invalid("HIXL window operation index overflow"))?;
        part.chunk_offset = operation
            .chunk_offset
            .checked_add(begin - operation.destination_offset)
            .ok_or_else(|| invalid("HIXL chunk window offset overflow"))?;
        part.length = finish - begin;
        part.destination_offset = copied;
        copied = copied
            .checked_add(part.length)
            .ok_or_else(|| invalid("HIXL window length overflow"))?;
        window.operations.push(part);
    }
    if copied != length {
        return Err(invalid("HIXL window does not cover its packed interval"));
    }
    Ok(window)
}

#[cfg(test)]
mod tests {
    use super::{pack_read_batch, pack_read_group, slice_read_batch, slice_read_group};
    use afs_protocol::node_data::{DfsChunkReadOp, DfsReadGrant, DfsReadRangesRequest};

    fn request() -> DfsReadRangesRequest {
        DfsReadRangesRequest {
            read_id: "read".into(),
            attempt_id: "batch-attempt".into(),
            file_version_id: "version".into(),
            layout_root_id: "layout".into(),
            include_blake3_outboard: false,
            operations: [0, 1]
                .into_iter()
                .map(|index| DfsChunkReadOp {
                    operation_index: index,
                    chunk_id: format!("chunk-{index}"),
                    chunk_offset: 10,
                    length: 4,
                    destination_offset: u64::from(index) * 10,
                    source_copy_id: format!("copy-{index}"),
                    grant: Some(DfsReadGrant {
                        token: format!("grant-{index}"),
                        ..DfsReadGrant::default()
                    }),
                })
                .collect(),
        }
    }

    fn group_request(bytes: u64) -> DfsReadRangesRequest {
        DfsReadRangesRequest {
            operations: vec![DfsChunkReadOp {
                operation_index: 0,
                chunk_id: "chunk".into(),
                chunk_offset: 0,
                length: bytes,
                destination_offset: 0,
                source_copy_id: "copy".into(),
                grant: Some(DfsReadGrant::default()),
            }],
            ..request()
        }
    }

    #[test]
    fn packing_retains_range_identity_and_normalizes_scatter_offsets() {
        let input = request();
        let packed = pack_read_batch(input.clone()).unwrap();
        assert_eq!(packed.operations[0], input.operations[0]);
        assert_eq!(packed.operations[1].destination_offset, 4);
        assert_eq!(packed.operations[1].grant, input.operations[1].grant);
        assert_eq!(packed.attempt_id, input.attempt_id);
    }

    #[test]
    fn a_window_spanning_chunks_retains_exact_grants_and_offsets() {
        let packed = pack_read_batch(request()).unwrap();
        let window = slice_read_batch(&packed, 2, 4, "window-0").unwrap();
        assert_eq!(window.attempt_id, "window-0");
        assert_eq!(window.operations.len(), 2);
        assert_eq!(window.operations[0].chunk_offset, 12);
        assert_eq!(window.operations[0].length, 2);
        assert_eq!(window.operations[1].operation_index, 1);
        assert_eq!(window.operations[1].destination_offset, 2);
        assert_eq!(window.operations[1].chunk_offset, 10);
        assert_eq!(window.operations[1].length, 2);
        assert_eq!(window.operations[1].grant, packed.operations[1].grant);
    }

    #[test]
    fn batches_reject_empty_zero_length_overflow_and_excess_bytes() {
        assert!(pack_read_batch(DfsReadRangesRequest::default()).is_err());
        for length in [0, u64::MAX, 8 * 1024 * 1024 + 1] {
            let mut input = request();
            input.operations[0].length = length;
            assert!(pack_read_batch(input).is_err());
        }
    }

    #[test]
    fn group_batches_accept_more_than_the_small_batch_budget() {
        let input = group_request(100 * 1024 * 1024);
        assert!(pack_read_batch(input.clone()).is_err());
        let packed = pack_read_group(input).unwrap();
        assert_eq!(packed.operations[0].destination_offset, 0);
        let window =
            slice_read_group(&packed, 96 * 1024 * 1024, 4 * 1024 * 1024, "window").unwrap();
        assert_eq!(window.operations[0].chunk_offset, 96 * 1024 * 1024);
        assert_eq!(window.operations[0].length, 4 * 1024 * 1024);
        assert!(slice_read_batch(&packed, 0, 4, "window").is_err());
    }

    #[test]
    fn windows_reject_gaps_zero_length_overflow_and_out_of_bounds_ranges() {
        let packed = pack_read_batch(request()).unwrap();
        for (offset, length) in [(0, 0), (7, 2), (u64::MAX, 1)] {
            assert!(slice_read_batch(&packed, offset, length, "window").is_err());
        }
        assert!(slice_read_batch(&request(), 0, 4, "window").is_err());
    }
}
