//! Authenticated packed DFS read snapshots backing the HIXL source windows.

use afs_protocol::node_data::{DfsReadRangesCompletion, DfsReadRangesRequest};
use std::sync::Mutex;
use tonic::Status;

pub(super) fn validate_packed_dfs_read(request: &DfsReadRangesRequest) -> Result<usize, Status> {
    let length = packed_dfs_read_length(request, crate::node::chunk::MAX_STAGED_CHUNK_BYTES)?;
    let mut destination_offset = 0_u64;
    for operation in &request.operations {
        if operation.destination_offset != destination_offset {
            return Err(Status::invalid_argument(
                "DFS read windows must be tightly packed",
            ));
        }
        destination_offset = destination_offset
            .checked_add(operation.length)
            .ok_or_else(|| Status::invalid_argument("packed DFS destination overflowed"))?;
    }
    Ok(length)
}

pub(super) fn packed_dfs_read_length(
    request: &DfsReadRangesRequest,
    byte_budget: usize,
) -> Result<usize, Status> {
    // The snapshot is always packed in operation order. The request's
    // destination offsets describe the caller's output layout and may contain
    // gaps; streaming headers retain those original operations unchanged.
    let byte_budget = u64::try_from(byte_budget)
        .map_err(|_| Status::invalid_argument("packed DFS byte budget exceeds protocol range"))?;
    let mut length = 0_u64;
    for op in &request.operations {
        length = length
            .checked_add(op.length)
            .filter(|length| *length <= byte_budget)
            .ok_or_else(|| Status::invalid_argument("packed DFS read exceeds its byte budget"))?;
    }
    usize::try_from(length)
        .map_err(|_| Status::invalid_argument("packed DFS read length exceeds address space"))
}

/// Chunk scans are independent reads plus BLAKE3, so a batch completes in
/// roughly one Chunk's scan time instead of the sum of its Chunks.
fn source_scan_pool() -> Option<&'static rayon_core::ThreadPool> {
    static POOL: std::sync::OnceLock<Option<rayon_core::ThreadPool>> = std::sync::OnceLock::new();
    POOL.get_or_init(|| {
        let workers = std::thread::available_parallelism()
            .map_or(4, |parallel| parallel.get())
            .clamp(1, MAX_SOURCE_SCAN_THREADS);
        rayon_core::ThreadPoolBuilder::new()
            .num_threads(workers)
            .thread_name(|index| format!("afs-source-scan-{index}"))
            .build()
            .ok()
    })
    .as_ref()
}

/// A verified group batch holds at most 128 MiB of 4 MiB Chunks, so this also
/// bounds the transient per-Chunk copy buffers to 64 MiB.
const MAX_SOURCE_SCAN_THREADS: usize = 16;

/// One range's transport digest and its bounded descriptor.
#[derive(Clone)]
pub(super) struct PackedRangeDigest {
    pub(super) checksum: Vec<u8>,
    pub(super) algorithm: i32,
    pub(super) table: Vec<u8>,
}

/// One run of adjacent operations on the same Chunk, in packed-output order.
struct PackedChunkGroup {
    chunk_id: String,
    /// Operation index and its range inside the group's output span.
    ranges: Vec<(usize, crate::node::chunk::VerifiedRangeCopy)>,
    relative_length: usize,
}

/// Runs one independent Chunk scan per slot on the bounded scan pool.
fn run_chunk_scans<F>(count: usize, scan: F)
where
    F: Fn(usize) + Send + Sync,
{
    match source_scan_pool() {
        Some(pool) => {
            let scan = &scan;
            pool.install(|| {
                rayon_core::scope(|scope| {
                    for index in 0..count {
                        scope.spawn(move |_| scan(index));
                    }
                });
            });
        }
        None => (0..count).for_each(scan),
    }
}

/// Verifies one immutable Chunk once, copies every requested range into `out`
/// and returns the per-range digests from that same scan.
fn scan_packed_chunk(
    local: &crate::node::chunk::LocalChunkStore,
    group: &PackedChunkGroup,
    out: &mut [u8],
) -> Result<Vec<PackedRangeDigest>, Status> {
    let id = crate::dfs::ChunkId::new(group.chunk_id.clone());
    let copies = group
        .ranges
        .iter()
        .map(|(_, range)| *range)
        .collect::<Vec<_>>();
    // CRC32C objects are bound to a Meta descriptor, so each range digest is
    // the CRC of the copied bytes plus that bounded descriptor.
    if let Some(table) = local
        .peer_crc32c_table(&id)
        .map_err(super::error_to_status)?
    {
        let descriptor =
            serde_json::to_vec(&table).map_err(|error| Status::internal(error.to_string()))?;
        return scan_crc32c_snapshot(local, &id, &copies, out, &table, &descriptor);
    }
    match local.read_ranges_at_uncommitted_with_checksums(&id, &copies, out) {
        Ok((copied, checksums))
            if copied.len() == copies.len()
                && copied
                    .iter()
                    .zip(&copies)
                    .all(|(count, range)| *count == range.length)
                && checksums.len() == copies.len() =>
        {
            Ok(checksums
                .into_iter()
                .map(|checksum| PackedRangeDigest {
                    checksum: checksum.to_vec(),
                    algorithm: afs_protocol::node_data::DfsDigestAlgorithm::Blake3 as i32,
                    table: Vec::new(),
                })
                .collect())
        }
        Ok(_) => Err(super::coded_status(
            afs_error::NODE_TRANSFER_CORRUPT_DATA,
            "immutable Chunk ended before packed DFS range",
        )),
        Err(error) => Err(super::error_to_status(error)),
    }
}

/// Verifies one CRC32C Chunk's ranges into `out` and digests the copied bytes.
///
/// Only a range covering whole segments carries the Meta-bound descriptor; a
/// HIXL reader rejects a descriptor on a range that cuts a segment.
fn scan_crc32c_snapshot(
    local: &crate::node::chunk::LocalChunkStore,
    id: &crate::dfs::ChunkId,
    copies: &[crate::node::chunk::VerifiedRangeCopy],
    out: &mut [u8],
    table: &crate::node::chunk_crc32c::CrcTable,
    descriptor: &[u8],
) -> Result<Vec<PackedRangeDigest>, Status> {
    let copied = local
        .read_crc32c_ranges_into(id, copies, out)
        .map_err(super::error_to_status)?;
    if copied.len() != copies.len()
        || copied
            .iter()
            .zip(copies)
            .any(|(count, range)| *count != range.length)
    {
        return Err(super::coded_status(
            afs_error::NODE_TRANSFER_CORRUPT_DATA,
            "immutable Chunk ended before packed DFS range",
        ));
    }
    copies
        .iter()
        .map(|range| {
            let data = range
                .output_offset
                .checked_add(range.length)
                .and_then(|end| out.get(range.output_offset..end))
                .ok_or_else(|| Status::internal("packed DFS range exceeds its buffer"))?;
            Ok(PackedRangeDigest {
                checksum: crate::node::chunk_crc32c::checksum(data)
                    .to_le_bytes()
                    .to_vec(),
                algorithm: afs_protocol::node_data::DfsDigestAlgorithm::Crc32c as i32,
                table: if table.covers_whole_segments(range.chunk_offset, data.len() as u64) {
                    descriptor.to_vec()
                } else {
                    Vec::new()
                },
            })
        })
        .collect()
}

/// Packs a verified HIXL snapshot. CRC32C ranges are verified straight into
/// the snapshot buffer, which the HIXL service already bounds with its own
/// snapshot budget, instead of each range taking a private payload from the
/// shared peer read budget first. Each run of adjacent operations on one Chunk
/// is verified and hashed by a single scan.
pub(super) fn read_hixl_snapshot_ranges(
    local: &crate::node::chunk::LocalChunkStore,
    request: &DfsReadRangesRequest,
    budget: usize,
) -> Result<(Vec<u8>, Vec<DfsReadRangesCompletion>), Status> {
    let length = packed_dfs_read_length(request, budget)?;
    let mut groups = Vec::<PackedChunkGroup>::new();
    for (index, op) in request.operations.iter().enumerate() {
        if op.length == 0 || op.chunk_offset.checked_add(op.length).is_none() {
            return Err(Status::invalid_argument(
                "packed DFS read contains an invalid source range",
            ));
        }
        let range_length = usize::try_from(op.length)
            .map_err(|_| Status::invalid_argument("DFS packed output length is too large"))?;
        if groups
            .last()
            .is_none_or(|group| group.chunk_id != op.chunk_id)
        {
            groups.push(PackedChunkGroup {
                chunk_id: op.chunk_id.clone(),
                ranges: Vec::new(),
                relative_length: 0,
            });
        }
        let group = groups
            .last_mut()
            .ok_or_else(|| Status::internal("packed DFS group disappeared"))?;
        group.ranges.push((
            index,
            crate::node::chunk::VerifiedRangeCopy {
                chunk_offset: op.chunk_offset,
                length: range_length,
                output_offset: group.relative_length,
            },
        ));
        group.relative_length = group
            .relative_length
            .checked_add(range_length)
            .ok_or_else(|| Status::invalid_argument("DFS packed chunk length overflow"))?;
    }

    // Groups tile the snapshot in order, so every scan writes its span directly.
    let mut bytes = vec![0; length];
    let mut slices = Vec::with_capacity(groups.len());
    let mut remainder: &mut [u8] = &mut bytes;
    for group in &groups {
        let (head, tail) = remainder.split_at_mut(group.relative_length);
        slices.push(Some(head));
        remainder = tail;
    }
    let outputs = Mutex::new(slices);
    let slots = Mutex::new(
        (0..groups.len())
            .map(|_| None)
            .collect::<Vec<Option<Result<Vec<PackedRangeDigest>, Status>>>>(),
    );
    let scan = |index: usize| {
        let output = match outputs.lock() {
            Ok(mut outputs) => outputs[index].take(),
            Err(poisoned) => poisoned.into_inner()[index].take(),
        };
        let result = output
            .ok_or_else(|| Status::internal("verified DFS range output slice disappeared"))
            .and_then(|output| scan_packed_chunk(local, &groups[index], output));
        match slots.lock() {
            Ok(mut slots) => slots[index] = Some(result),
            Err(poisoned) => poisoned.into_inner()[index] = Some(result),
        }
    };
    run_chunk_scans(groups.len(), scan);
    drop(outputs);
    let mut slots = slots
        .into_inner()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    // Chunk order stays deterministic for error selection.
    let mut range_digests = vec![None; request.operations.len()];
    for (index, group) in groups.iter().enumerate() {
        let scan = slots[index]
            .take()
            .ok_or_else(|| Status::internal("verified DFS range checksum scan did not run"))?;
        let digests = scan?;
        #[cfg(test)]
        crate::node::chunk::note_verified_scan_for_test();
        if digests.len() != group.ranges.len() {
            return Err(Status::internal(
                "verified DFS range checksum count changed",
            ));
        }
        for ((operation_index, _), digest) in group.ranges.iter().zip(digests) {
            range_digests[*operation_index] = Some(digest);
        }
    }

    let mut completions = Vec::with_capacity(request.operations.len());
    for (index, op) in request.operations.iter().enumerate() {
        let digest = range_digests[index]
            .take()
            .ok_or_else(|| Status::internal("verified DFS range checksum disappeared"))?;
        completions.push(DfsReadRangesCompletion {
            crc32c_table: digest.table,
            read_id: request.read_id.clone(),
            attempt_id: request.attempt_id.clone(),
            operation_index: op.operation_index,
            source_copy_id: op.source_copy_id.clone(),
            transferred_bytes: op.length,
            range_checksum: digest.checksum,
            range_checksum_algorithm: digest.algorithm,
        });
    }
    Ok((bytes, completions))
}

#[cfg(test)]
mod tests {
    use crate::node::chunk::ChunkStore;
    use afs_protocol::node_data::{DfsChunkReadOp, DfsReadRangesRequest};

    fn read_request(chunk_id: String, length: u64) -> DfsReadRangesRequest {
        DfsReadRangesRequest {
            read_id: "prepared-read".into(),
            attempt_id: "prepared-attempt".into(),
            file_version_id: "fixture-version".into(),
            layout_root_id: "fixture-layout".into(),
            include_blake3_outboard: false,
            operations: vec![DfsChunkReadOp {
                operation_index: 0,
                chunk_id,
                chunk_offset: 0,
                length,
                destination_offset: 0,
                source_copy_id: "fixture-copy".into(),
                grant: Some(afs_protocol::node_data::DfsReadGrant {
                    namespace_id: "fixture-namespace".into(),
                    file_version_id: "fixture-version".into(),
                    layout_root_id: "fixture-layout".into(),
                    caller_node_id: "reader".into(),
                    caller_node_epoch: 1,
                    expires_at_unix_ms: u64::MAX,
                    fence: 1,
                    token: "fixture-grant".into(),
                }),
            }],
        }
    }

    #[test]
    fn packed_read_hashes_each_range_during_the_single_chunk_scan() {
        use crate::node::chunk::{
            StagedChunk, reset_verified_scan_count_for_test, verified_scan_count_for_test,
        };

        let temp = tempfile::tempdir().unwrap();
        let local = crate::node::chunk::LocalChunkStore::open(temp.path(), "node").unwrap();
        let bytes: Vec<_> = (0..70_000).map(|index| (index % 251) as u8).collect();
        let staged = StagedChunk::new(
            crate::dfs::OperationId::new("packed-single-scan"),
            bytes.clone(),
        );
        local.put(staged.clone()).unwrap();
        let mut request = read_request(staged.chunk.id.0.clone(), 70_000);
        request.operations = vec![
            DfsChunkReadOp {
                operation_index: 0,
                chunk_id: staged.chunk.id.0.clone(),
                chunk_offset: 0,
                length: 70_000,
                destination_offset: 0,
                source_copy_id: "copy-a".into(),
                grant: None,
            },
            DfsChunkReadOp {
                operation_index: 1,
                chunk_id: staged.chunk.id.0.clone(),
                chunk_offset: 40_000,
                length: 30_000,
                destination_offset: 70_000,
                source_copy_id: "copy-b".into(),
                grant: None,
            },
        ];
        reset_verified_scan_count_for_test();
        let (packed, completions) =
            super::read_hixl_snapshot_ranges(&local, &request, 1024 * 1024).unwrap();
        assert_eq!(verified_scan_count_for_test(), 1);
        assert_eq!(&packed[..70_000], &bytes[..]);
        assert_eq!(&packed[70_000..], &bytes[40_000..70_000]);
        assert_eq!(
            completions[0].range_checksum,
            staged.chunk.content_digest.bytes.to_vec()
        );
        assert_eq!(
            completions[1].range_checksum,
            blake3::hash(&bytes[40_000..70_000]).as_bytes().to_vec()
        );
    }

    #[test]
    fn hixl_snapshot_serves_crc32c_ranges_without_the_peer_payload_budget() {
        use crate::node::chunk::{PEER_READ_PAYLOAD_LIMIT, StagedChunk};
        use afs_protocol::node_data::DfsDigestAlgorithm;

        let temp = tempfile::tempdir().unwrap();
        let local = crate::node::chunk::LocalChunkStore::open(temp.path(), "node").unwrap();
        let bytes: Vec<_> = (0..70_000).map(|index| (index % 251) as u8).collect();
        let staged =
            StagedChunk::new_crc32c(crate::dfs::OperationId::new("snapshot-crc"), bytes.clone())
                .unwrap();
        local.put(staged.clone()).unwrap();
        let mut request = read_request(staged.chunk.id.0.clone(), 70_000);
        request.operations = vec![
            DfsChunkReadOp {
                operation_index: 0,
                chunk_id: staged.chunk.id.0.clone(),
                chunk_offset: 0,
                length: 70_000,
                destination_offset: 0,
                source_copy_id: "copy-a".into(),
                grant: None,
            },
            // This range cuts a CRC32C segment, so its covering segments are
            // verified as well.
            DfsChunkReadOp {
                operation_index: 1,
                chunk_id: staged.chunk.id.0.clone(),
                chunk_offset: 40_000,
                length: 30_000,
                destination_offset: 70_000,
                source_copy_id: "copy-b".into(),
                grant: None,
            },
        ];
        // Concurrent peer reads hold every byte of the shared payload budget.
        let held = local
            .reserve_peer_read_payload(PEER_READ_PAYLOAD_LIMIT)
            .unwrap();

        let (packed, completions) =
            super::read_hixl_snapshot_ranges(&local, &request, 1024 * 1024).unwrap();
        assert_eq!(&packed[..70_000], &bytes[..]);
        assert_eq!(&packed[70_000..], &bytes[40_000..70_000]);
        assert_eq!(completions.len(), 2);
        for (completion, range) in completions.iter().zip([&bytes[..], &bytes[40_000..70_000]]) {
            assert_eq!(
                completion.range_checksum_algorithm,
                DfsDigestAlgorithm::Crc32c as i32
            );
            assert_eq!(
                completion.range_checksum,
                crate::node::chunk_crc32c::checksum(range)
                    .to_le_bytes()
                    .to_vec()
            );
        }
        // The first range ends at the object end, so its descriptor binds it;
        // the second cuts a segment and relies on its range checksum alone.
        assert!(!completions[0].crc32c_table.is_empty());
        assert!(completions[1].crc32c_table.is_empty());
        drop(held);
    }

    #[test]
    fn snapshot_rescans_a_chunk_only_when_operations_interleave() {
        use crate::node::chunk::{
            StagedChunk, reset_verified_scan_count_for_test, verified_scan_count_for_test,
        };

        let temp = tempfile::tempdir().unwrap();
        let local = crate::node::chunk::LocalChunkStore::open(temp.path(), "node").unwrap();
        let first = StagedChunk::new(crate::dfs::OperationId::new("first"), (0..64).collect());
        let second = StagedChunk::new(crate::dfs::OperationId::new("second"), (64..128).collect());
        local.put(first.clone()).unwrap();
        local.put(second.clone()).unwrap();
        let op =
            |index, chunk: &StagedChunk, chunk_offset, length, destination_offset| DfsChunkReadOp {
                operation_index: index,
                chunk_id: chunk.chunk.id.0.clone(),
                chunk_offset,
                length,
                destination_offset,
                source_copy_id: "copy".into(),
                grant: None,
            };
        let mut request = read_request(first.chunk.id.0.clone(), 8);
        request.operations = vec![
            op(0, &first, 0, 3, 0),
            op(1, &second, 10, 2, 3),
            op(2, &first, 20, 3, 5),
        ];
        reset_verified_scan_count_for_test();
        let (bytes, completions) =
            super::read_hixl_snapshot_ranges(&local, &request, 1024).unwrap();
        assert_eq!(bytes, [0, 1, 2, 74, 75, 20, 21, 22]);
        assert_eq!(verified_scan_count_for_test(), 3);
        assert_eq!(
            completions[1].range_checksum,
            blake3::hash(&[74, 75]).as_bytes().to_vec()
        );
    }

    #[test]
    fn snapshot_quarantines_corruption_outside_the_selected_ranges() {
        use crate::node::chunk::StagedChunk;

        let temp = tempfile::tempdir().unwrap();
        let local = crate::node::chunk::LocalChunkStore::open(temp.path(), "node").unwrap();
        let staged = StagedChunk::new(crate::dfs::OperationId::new("corrupt"), (0..64).collect());
        local.put(staged.clone()).unwrap();
        let mut damaged = staged.bytes().to_vec();
        damaged[63] ^= 1;
        std::fs::write(temp.path().join("chunks").join(&staged.chunk.id.0), damaged).unwrap();

        let mut request = read_request(staged.chunk.id.0.clone(), 2);
        request.operations[0].chunk_offset = 1;
        let error = super::read_hixl_snapshot_ranges(&local, &request, 1024).unwrap_err();
        assert_eq!(error.code(), tonic::Code::DataLoss);
        assert_eq!(local.quarantined_chunks().unwrap().len(), 1);
    }
}
