//! Authenticated packed DFS read snapshots shared by the prepared reader and
//! the HIXL source windows.

use afs_protocol::node_data::{DfsReadRangesCompletion, DfsReadRangesRequest};
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Instant;
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

// HIXL snapshots use `read_hixl_snapshot_ranges`; RDMA packed reads keep this path.
#[cfg(any(test, feature = "rdma"))]
pub(super) fn read_packed_dfs_ranges(
    local: &crate::node::chunk::LocalChunkStore,
    request: &DfsReadRangesRequest,
    length: usize,
) -> Result<(Vec<u8>, Vec<DfsReadRangesCompletion>), Status> {
    let budget = usize::try_from(crate::node::dfs_read::MAX_DFS_READ_BYTES)
        .map_err(|_| Status::invalid_argument("DFS read byte budget exceeds platform capacity"))?;
    read_packed_dfs_ranges_with_budget(local, request, length, budget)
}

/// Packs a verified HIXL snapshot. CRC32C ranges are verified straight into
/// the snapshot buffer, which the HIXL service already bounds with its own
/// snapshot budget, instead of each range taking a private payload from the
/// shared peer read budget first.
pub(super) fn read_hixl_snapshot_ranges(
    local: &crate::node::chunk::LocalChunkStore,
    request: &DfsReadRangesRequest,
    length: usize,
    budget: usize,
) -> Result<(Vec<u8>, Vec<DfsReadRangesCompletion>), Status> {
    read_packed_ranges(local, request, length, budget, CrcScan::Snapshot)
}

/// Where a packed scan places the bytes of a CRC32C range.
#[derive(Clone, Copy)]
enum CrcScan {
    /// Each range is a private peer payload charged to the shared budget.
    #[cfg_attr(not(any(test, feature = "rdma")), allow(dead_code))]
    PeerPayload,
    /// Each range is verified straight into the bounded snapshot buffer.
    Snapshot,
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

/// One Chunk's requested ranges in packed-output order.
struct PackedChunkGroup {
    chunk_id: String,
    /// Operation index, relative output range, and absolute packed offset.
    ranges: Vec<(usize, crate::node::chunk::VerifiedRangeCopy, usize)>,
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
    deadline: Instant,
    crc_scan: CrcScan,
) -> Result<Vec<PackedRangeDigest>, Status> {
    let id = crate::dfs::ChunkId::new(group.chunk_id.clone());
    // CRC32C objects are bound to a Meta descriptor, so each range digest is
    // the CRC of the copied bytes plus that bounded descriptor.
    if let Some(table) = local
        .peer_crc32c_table(&id)
        .map_err(super::error_to_status)?
    {
        let descriptor =
            serde_json::to_vec(&table).map_err(|error| Status::internal(error.to_string()))?;
        if let CrcScan::Snapshot = crc_scan {
            return scan_crc32c_snapshot(local, &id, group, out, &table, &descriptor);
        }
        let mut digests = Vec::with_capacity(group.ranges.len());
        for (_, range, _) in &group.ranges {
            let end = range
                .output_offset
                .checked_add(range.length)
                .ok_or_else(|| Status::internal("packed DFS range overflow"))?;
            let payload = local
                .read_peer_range_until(&id, range.chunk_offset, range.length, deadline)
                .map_err(super::error_to_status)?;
            let data = payload.as_slice();
            if data.len() != range.length {
                return Err(super::coded_status(
                    afs_error::NODE_TRANSFER_CORRUPT_DATA,
                    "immutable Chunk ended before packed DFS range",
                ));
            }
            out.get_mut(range.output_offset..end)
                .ok_or_else(|| Status::internal("packed DFS range exceeds its buffer"))?
                .copy_from_slice(data);
            digests.push(PackedRangeDigest {
                checksum: crate::node::chunk_crc32c::checksum(data)
                    .to_le_bytes()
                    .to_vec(),
                algorithm: afs_protocol::node_data::DfsDigestAlgorithm::Crc32c as i32,
                table: descriptor.clone(),
            });
        }
        return Ok(digests);
    }
    let copies = group
        .ranges
        .iter()
        .map(|(_, range, _)| *range)
        .collect::<Vec<_>>();
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
    group: &PackedChunkGroup,
    out: &mut [u8],
    table: &crate::node::chunk_crc32c::CrcTable,
    descriptor: &[u8],
) -> Result<Vec<PackedRangeDigest>, Status> {
    let copies = group
        .ranges
        .iter()
        .map(|(_, range, _)| *range)
        .collect::<Vec<_>>();
    let copied = local
        .read_crc32c_ranges_into(id, &copies, out)
        .map_err(super::error_to_status)?;
    if copied.len() != copies.len()
        || copied
            .iter()
            .zip(&copies)
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

/// Returns each Chunk group's packed-output span when every group's ranges are
/// contiguous and the spans tile the whole output. The scan can then write the
/// final snapshot directly instead of copying the payload a second time.
fn contiguous_group_spans(
    groups: &[PackedChunkGroup],
    length: usize,
) -> Option<Vec<(usize, usize)>> {
    let mut spans = Vec::with_capacity(groups.len());
    for group in groups {
        let (_, _, first_output) = group.ranges.first()?;
        let mut cursor = *first_output;
        for (_, range, output_offset) in &group.ranges {
            if *output_offset != cursor {
                return None;
            }
            cursor = cursor.checked_add(range.length)?;
        }
        if cursor != first_output.checked_add(group.relative_length)? {
            return None;
        }
        spans.push((*first_output, cursor));
    }
    let mut ordered = (0..spans.len()).collect::<Vec<_>>();
    ordered.sort_unstable_by_key(|&index| spans[index].0);
    let mut cursor = 0_usize;
    for index in ordered {
        if spans[index].0 != cursor {
            return None;
        }
        cursor = spans[index].1;
    }
    (cursor == length).then_some(spans)
}

/// Group snapshots use the explicit group budget. Each Chunk is verified and
/// hashed once; the per-range BLAKE3 digests come from that same scan.
#[cfg(any(test, feature = "rdma"))]
pub(super) fn read_packed_dfs_ranges_with_budget(
    local: &crate::node::chunk::LocalChunkStore,
    request: &DfsReadRangesRequest,
    length: usize,
    budget: usize,
) -> Result<(Vec<u8>, Vec<DfsReadRangesCompletion>), Status> {
    read_packed_ranges(local, request, length, budget, CrcScan::PeerPayload)
}

fn read_packed_ranges(
    local: &crate::node::chunk::LocalChunkStore,
    request: &DfsReadRangesRequest,
    length: usize,
    budget: usize,
    crc_scan: CrcScan,
) -> Result<(Vec<u8>, Vec<DfsReadRangesCompletion>), Status> {
    if packed_dfs_read_length(request, budget)? != length {
        return Err(Status::invalid_argument(
            "packed DFS read length does not match its operations",
        ));
    }
    let mut bytes = vec![0; length];
    // Chunk order must stay deterministic for error selection; each Chunk is
    // verified and hashed independently, so the scans run concurrently.
    let mut groups = Vec::<PackedChunkGroup>::new();
    let mut group_index = HashMap::<String, usize>::new();
    let mut packed_cursor = 0_usize;
    for (index, op) in request.operations.iter().enumerate() {
        if op.length == 0 || op.chunk_offset.checked_add(op.length).is_none() {
            return Err(Status::invalid_argument(
                "packed DFS read contains an invalid source range",
            ));
        }
        let start = packed_cursor;
        let range_length = usize::try_from(op.length)
            .map_err(|_| Status::invalid_argument("DFS packed output length is too large"))?;
        let end = start
            .checked_add(range_length)
            .filter(|end| *end <= bytes.len())
            .ok_or_else(|| Status::invalid_argument("DFS packed output range exceeds buffer"))?;
        packed_cursor = end;
        let slot = match group_index.get(&op.chunk_id) {
            Some(slot) => *slot,
            None => {
                let slot = groups.len();
                group_index.insert(op.chunk_id.clone(), slot);
                groups.push(PackedChunkGroup {
                    chunk_id: op.chunk_id.clone(),
                    ranges: Vec::new(),
                    relative_length: 0,
                });
                slot
            }
        };
        let relative_offset = groups[slot].relative_length;
        groups[slot].relative_length = groups[slot]
            .relative_length
            .checked_add(range_length)
            .ok_or_else(|| Status::invalid_argument("DFS packed chunk length overflow"))?;
        groups[slot].ranges.push((
            index,
            crate::node::chunk::VerifiedRangeCopy {
                chunk_offset: op.chunk_offset,
                length: range_length,
                output_offset: relative_offset,
            },
            start,
        ));
    }

    let mut range_digests = vec![None; request.operations.len()];
    let deadline = local.peer_serve_deadline();
    if let Some(spans) = contiguous_group_spans(&groups, length) {
        let mut ordered = (0..groups.len()).collect::<Vec<_>>();
        ordered.sort_unstable_by_key(|&index| spans[index].0);
        let mut slices = (0..groups.len())
            .map(|_| None)
            .collect::<Vec<Option<&mut [u8]>>>();
        let mut remainder: &mut [u8] = &mut bytes;
        for index in ordered {
            let (start, end) = spans[index];
            let (head, tail) = remainder.split_at_mut(end - start);
            slices[index] = Some(head);
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
                .and_then(|output| {
                    scan_packed_chunk(local, &groups[index], output, deadline, crc_scan)
                });
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
            for ((operation_index, _, _), digest) in group.ranges.iter().zip(digests) {
                range_digests[*operation_index] = Some(digest);
            }
        }
    } else {
        // Interleaved Chunk operations cannot share one final slice, so each
        // scan keeps a private buffer and the caller copies it afterwards.
        type ChunkScan = Result<(Vec<u8>, Vec<PackedRangeDigest>), Status>;
        let slots = Mutex::new(
            (0..groups.len())
                .map(|_| None)
                .collect::<Vec<Option<ChunkScan>>>(),
        );
        let scan = |index: usize| {
            let group = &groups[index];
            let mut buffer = vec![0; group.relative_length];
            let result = scan_packed_chunk(local, group, &mut buffer, deadline, crc_scan)
                .map(|checksums| (buffer, checksums));
            match slots.lock() {
                Ok(mut slots) => slots[index] = Some(result),
                Err(poisoned) => poisoned.into_inner()[index] = Some(result),
            }
        };
        run_chunk_scans(groups.len(), scan);
        let mut slots = slots
            .into_inner()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for (index, group) in groups.iter().enumerate() {
            let scan = slots[index]
                .take()
                .ok_or_else(|| Status::internal("verified DFS range checksum scan did not run"))?;
            let (buffer, digests) = scan?;
            #[cfg(test)]
            crate::node::chunk::note_verified_scan_for_test();
            let mut offset = 0_usize;
            for ((operation_index, range, output_offset), digest) in
                group.ranges.iter().zip(digests)
            {
                let end = offset
                    .checked_add(range.length)
                    .filter(|end| *end <= buffer.len())
                    .ok_or_else(|| Status::internal("verified DFS range output overflow"))?;
                let output_end = output_offset
                    .checked_add(range.length)
                    .filter(|end| *end <= bytes.len())
                    .ok_or_else(|| Status::internal("verified DFS output range exceeds buffer"))?;
                bytes[*output_offset..output_end].copy_from_slice(&buffer[offset..end]);
                offset = end;
                range_digests[*operation_index] = Some(digest);
            }
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
            super::read_packed_dfs_ranges_with_budget(&local, &request, 100_000, 1024 * 1024)
                .unwrap();
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
            super::read_hixl_snapshot_ranges(&local, &request, 100_000, 1024 * 1024).unwrap();
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

        // Other packed readers keep serving CRC32C ranges as budgeted peer
        // payloads, so they still wait for the shared budget.
        assert!(
            super::read_packed_dfs_ranges_with_budget(&local, &request, 100_000, 1024 * 1024)
                .is_err()
        );
        drop(held);
    }
}
