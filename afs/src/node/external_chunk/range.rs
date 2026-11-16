//! Range reads of one immutable S3 chunk, verified against its BLAKE3
//! outboard sidecar object.
//!
//! A chunk of at least [`SIDECAR_MIN_CHUNK_BYTES`] is stored with a sidecar
//! object beside it (`<chunk key>.bao`, the `NodeChunkSidecar` encoding the
//! local store uses). A read then fetches only the 32 KiB groups it covers and
//! proves them up to the chunk's content root. The root always comes from the
//! chunk identity the caller holds (Meta's content address); the sidecar is
//! untrusted and its header is only compared with that identity.

use std::{collections::HashMap, io, ops::Range};

use bao_tree::{
    BaoTree, BlockSize, ChunkNum, ChunkRanges,
    io::{outboard::PreOrderMemOutboard, sync::ReadAt},
};

use super::*;
use crate::node::{
    chunk::outboard::{ChunkOutboard, OUTBOARD_GROUP_BYTES},
    read::engine::ExternalRangeCopy,
};

/// Smaller chunks carry no sidecar and are always read whole: one GET of a
/// small object costs less than a second object per chunk.
pub(crate) const SIDECAR_MIN_CHUNK_BYTES: u64 = 256 * 1024;
/// Verified sidecars kept in memory (at most 8 KiB each).
const SIDECAR_CACHE_ENTRIES: usize = 256;
/// Chunks known to have no usable sidecar.
const SIDECAR_ABSENT_ENTRIES: usize = 4096;
/// Covered groups are fetched as at most this many range GETs; more
/// scattered reads take the whole object.
const MAX_RANGE_GETS: usize = 4;
const GROUP: u64 = OUTBOARD_GROUP_BYTES as u64;
/// `NodeChunkSidecar` header length; the parent hashes follow it.
const SIDECAR_HEADER_BYTES: usize = 64;
/// log2(group bytes / 1024), as the sidecar header records it.
const GROUP_LOG: u8 = 5;

/// What a caller knows about a chunk's sidecar before fetching.
pub(super) enum SidecarState {
    /// Verified shape, bound to the chunk identity.
    Cached(Arc<Vec<u8>>),
    /// No sidecar object, or one that failed against the chunk identity.
    Absent,
    Unknown,
}

/// Bounded memory of sidecars. A sidecar is written before its chunk's
/// receipt, so "absent" never turns into "present" for a readable chunk.
#[derive(Default)]
pub(super) struct SidecarCache {
    state: Mutex<SidecarCacheState>,
}

#[derive(Default)]
struct SidecarCacheState {
    present: HashMap<ChunkId, Arc<Vec<u8>>>,
    present_order: VecDeque<ChunkId>,
    absent: HashMap<ChunkId, ()>,
    absent_order: VecDeque<ChunkId>,
}

impl SidecarCache {
    pub(super) fn lookup(&self, chunk: &ChunkId) -> SidecarState {
        let Ok(state) = self.state.lock() else {
            return SidecarState::Absent;
        };
        if let Some(encoded) = state.present.get(chunk) {
            return SidecarState::Cached(encoded.clone());
        }
        if state.absent.contains_key(chunk) {
            return SidecarState::Absent;
        }
        SidecarState::Unknown
    }

    pub(super) fn insert(&self, chunk: &ChunkId, encoded: Arc<Vec<u8>>) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        if state.present.insert(chunk.clone(), encoded).is_none() {
            state.present_order.push_back(chunk.clone());
            if state.present_order.len() > SIDECAR_CACHE_ENTRIES
                && let Some(evicted) = state.present_order.pop_front()
            {
                state.present.remove(&evicted);
            }
        }
    }

    /// Forgets a sidecar and remembers that this chunk has none to use.
    pub(super) fn mark_absent(&self, chunk: &ChunkId) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        if state.present.remove(chunk).is_some() {
            state.present_order.retain(|id| id != chunk);
        }
        if state.absent.insert(chunk.clone(), ()).is_none() {
            state.absent_order.push_back(chunk.clone());
            if state.absent_order.len() > SIDECAR_ABSENT_ENTRIES
                && let Some(evicted) = state.absent_order.pop_front()
            {
                state.absent.remove(&evicted);
            }
        }
    }
}

/// The group-aligned byte ranges that cover `ops`, merged when they touch or
/// leave a gap of at most one group. `None` when the read should take the
/// whole object instead: the chunk carries no sidecar, the ranges are invalid
/// for it, they cover half of it or more, or they are too scattered.
pub(super) fn plan_ranges(
    chunk: &ChunkObject,
    ops: &[ExternalRangeCopy],
) -> Option<Vec<Range<u64>>> {
    if chunk.length < SIDECAR_MIN_CHUNK_BYTES {
        return None;
    }
    let mut ranges = Vec::with_capacity(ops.len());
    for op in ops.iter().filter(|op| op.length != 0) {
        let end = op.chunk_offset.checked_add(op.length as u64)?;
        if end > chunk.length {
            return None;
        }
        let start = op.chunk_offset / GROUP * GROUP;
        let end = end.div_ceil(GROUP).saturating_mul(GROUP).min(chunk.length);
        ranges.push(start..end);
    }
    ranges.sort_by_key(|range| range.start);
    let mut merged: Vec<Range<u64>> = Vec::with_capacity(ranges.len());
    for range in ranges {
        match merged.last_mut() {
            Some(last) if range.start <= last.end.saturating_add(GROUP) => {
                last.end = last.end.max(range.end);
            }
            _ => merged.push(range),
        }
    }
    let covered: u64 = merged.iter().map(|range| range.end - range.start).sum();
    if merged.is_empty()
        || merged.len() > MAX_RANGE_GETS
        || covered.saturating_mul(2) >= chunk.length
    {
        return None;
    }
    Some(merged)
}

/// Checks a fetched sidecar against the chunk identity. The header's length
/// and root are compared with the identity, never used in its place.
pub(super) fn validate_sidecar(chunk: &ChunkObject, encoded: &[u8]) -> Result<()> {
    ChunkOutboard::validate_encoded_shape(chunk, encoded)?;
    if encoded.len() < SIDECAR_HEADER_BYTES {
        return Err(corrupt("S3 chunk sidecar is truncated"));
    }
    Ok(())
}

/// Proves every fetched part up to the chunk's content root, then copies the
/// requested bytes into `out`. `out` is written only after the proof holds.
///
/// `sidecar` must have passed [`validate_sidecar`] for `chunk`: its length is
/// then exactly the tree's, so every parent the proof walks is present.
pub(super) fn verify_and_copy(
    chunk: &ChunkObject,
    sidecar: &[u8],
    parts: &[(Range<u64>, Vec<u8>)],
    ops: &[ExternalRangeCopy],
    out: &mut [u8],
) -> Result<()> {
    validate_sidecar(chunk, sidecar)?;
    let mut query = ChunkRanges::empty();
    for (range, bytes) in parts {
        let aligned = range.start.is_multiple_of(GROUP)
            && (range.end.is_multiple_of(GROUP) || range.end == chunk.length);
        if !aligned
            || range.start >= range.end
            || range.end > chunk.length
            || bytes.len() as u64 != range.end - range.start
        {
            return Err(corrupt("S3 range does not match the planned groups"));
        }
        query |= ChunkRanges::from(ChunkNum(range.start / 1024)..ChunkNum::chunks(range.end));
    }
    let outboard = PreOrderMemOutboard {
        // The trust anchor: the content root of the chunk identity.
        root: blake3::Hash::from(chunk.content_digest.bytes),
        tree: BaoTree::new(chunk.length, BlockSize::from_chunk_log(GROUP_LOG)),
        data: &sidecar[SIDECAR_HEADER_BYTES..],
    };
    bao_tree::io::sync::encode_ranges_validated(FetchedParts(parts), outboard, &query, io::sink())
        .map_err(|_| corrupt("S3 range does not prove against the Chunk content root"))?;
    for op in ops.iter().filter(|op| op.length != 0) {
        let source = parts
            .iter()
            .find_map(|(range, bytes)| {
                let start = usize::try_from(op.chunk_offset.checked_sub(range.start)?).ok()?;
                bytes.get(start..start.checked_add(op.length)?)
            })
            .ok_or_else(|| invalid("external range lies outside the fetched groups"))?;
        out.get_mut(op.output_offset..)
            .and_then(|out| out.get_mut(..op.length))
            .ok_or_else(|| invalid("external range exceeds the output buffer"))?
            .copy_from_slice(source);
    }
    Ok(())
}

/// The fetched parts as one sparse object. Offsets no part holds fail, so the
/// proof can never be satisfied by bytes that were not fetched.
struct FetchedParts<'a>(&'a [(Range<u64>, Vec<u8>)]);

impl ReadAt for FetchedParts<'_> {
    fn read_at(&self, offset: u64, out: &mut [u8]) -> io::Result<usize> {
        let Some((range, bytes)) = self
            .0
            .iter()
            .find(|(range, _)| range.start <= offset && offset < range.end)
        else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Bao read outside the fetched groups",
            ));
        };
        let bytes = &bytes[(offset - range.start) as usize..];
        let count = bytes.len().min(out.len());
        out[..count].copy_from_slice(&bytes[..count]);
        Ok(count)
    }
}

#[cfg(test)]
pub(super) mod test_support {
    /// The sidecar a writer stores for `body`, built by the local store's
    /// encoder under an unbounded test budget.
    pub(crate) fn sidecar_for(body: &[u8]) -> Vec<u8> {
        let budget = crate::node::chunk::OutboardBudget::new(1024 * 1024);
        crate::node::chunk::outboard::build_borrowed(body, &budget)
            .outboard
            .expect("test budget holds one outboard")
            .encoded()
            .to_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::{test_support::sidecar_for, *};

    fn body(length: usize, seed: u8) -> Vec<u8> {
        (0..length)
            .map(|index| (index as u8).wrapping_mul(31).wrapping_add(seed) ^ (index >> 11) as u8)
            .collect()
    }

    fn chunk_of(body: &[u8]) -> ChunkObject {
        StagedChunk::new_generational(OperationId::new("range"), body.to_vec()).chunk
    }

    fn op(chunk_offset: u64, length: usize, output_offset: usize) -> ExternalRangeCopy {
        ExternalRangeCopy {
            chunk_offset,
            length,
            output_offset,
        }
    }

    fn parts(body: &[u8], ranges: &[Range<u64>]) -> Vec<(Range<u64>, Vec<u8>)> {
        ranges
            .iter()
            .map(|range| {
                (
                    range.clone(),
                    body[range.start as usize..range.end as usize].to_vec(),
                )
            })
            .collect()
    }

    #[test]
    fn sidecar_layout_constants_match_the_local_encoder() {
        // This module rebuilds the tree and skips the header itself; both must
        // stay the local store's `NodeChunkSidecar` encoding.
        assert_eq!(GROUP, 1024 << GROUP_LOG);
        for length in [
            SIDECAR_MIN_CHUNK_BYTES as usize,
            1024 * 1024 + 7,
            4 * 1024 * 1024,
        ] {
            let body = body(length, 1);
            let budget = crate::node::chunk::OutboardBudget::new(1024 * 1024);
            let built = crate::node::chunk::outboard::build_borrowed(&body, &budget)
                .outboard
                .unwrap();
            assert_eq!(
                built.encoded().len() - built.raw_cv_bytes().len(),
                SIDECAR_HEADER_BYTES
            );
            let tree = BaoTree::new(length as u64, BlockSize::from_chunk_log(GROUP_LOG));
            assert_eq!(built.raw_cv_bytes().len() as u64, tree.outboard_size());
            validate_sidecar(&chunk_of(&body), built.encoded()).unwrap();
        }
    }

    #[test]
    fn plan_covers_only_touched_groups_and_merges_neighbours() {
        let chunk = chunk_of(&body(4 * 1024 * 1024, 2));
        assert_eq!(
            plan_ranges(&chunk, &[op(5000, 4096, 0)]).unwrap(),
            vec![0..GROUP]
        );
        // Crossing a group boundary takes both groups.
        assert_eq!(
            plan_ranges(&chunk, &[op(GROUP - 1, 2, 0)]).unwrap(),
            vec![0..2 * GROUP]
        );
        // A gap of one group merges; a wider gap stays two GETs. Order of ops
        // does not matter.
        assert_eq!(
            plan_ranges(&chunk, &[op(2 * GROUP, 10, 10), op(0, 10, 0)]).unwrap(),
            vec![0..3 * GROUP]
        );
        assert_eq!(
            plan_ranges(&chunk, &[op(0, 10, 0), op(3 * GROUP, 10, 10)]).unwrap(),
            vec![0..GROUP, 3 * GROUP..4 * GROUP]
        );
        // The tail group ends at the chunk length.
        let odd = chunk_of(&body(1024 * 1024 + 7, 3));
        assert_eq!(
            plan_ranges(&odd, &[op(1024 * 1024, 7, 0)]).unwrap(),
            vec![1024 * 1024..1024 * 1024 + 7]
        );
    }

    #[test]
    fn plan_takes_the_whole_object_when_ranges_do_not_pay_off() {
        let chunk = chunk_of(&body(4 * 1024 * 1024, 4));
        // Half of the chunk or more.
        assert!(plan_ranges(&chunk, &[op(0, 2 * 1024 * 1024, 0)]).is_none());
        assert!(plan_ranges(&chunk, &[op(0, 2 * 1024 * 1024 - GROUP as usize, 0)]).is_some());
        // Too scattered.
        let scattered: Vec<_> = (0..5).map(|i| op(i * 8 * GROUP, 1, i as usize)).collect();
        assert!(plan_ranges(&chunk, &scattered).is_none());
        // Nothing to read, or a range outside the chunk.
        assert!(plan_ranges(&chunk, &[]).is_none());
        assert!(plan_ranges(&chunk, &[op(0, 0, 0)]).is_none());
        assert!(plan_ranges(&chunk, &[op(4 * 1024 * 1024 - 1, 2, 0)]).is_none());
        assert!(plan_ranges(&chunk, &[op(u64::MAX, 2, 0)]).is_none());
        // Below the sidecar threshold.
        let small = chunk_of(&body(SIDECAR_MIN_CHUNK_BYTES as usize - 1, 5));
        assert!(plan_ranges(&small, &[op(0, 1, 0)]).is_none());
    }

    #[test]
    fn verified_ranges_return_exactly_the_requested_bytes() {
        for length in [
            SIDECAR_MIN_CHUNK_BYTES as usize,
            1024 * 1024 + 7,
            4 * 1024 * 1024,
        ] {
            let body = body(length, 6);
            let chunk = chunk_of(&body);
            let sidecar = sidecar_for(&body);
            let tail = length as u64 - 5;
            let cases: Vec<Vec<ExternalRangeCopy>> = vec![
                vec![op(0, 1, 0)],
                vec![op(4096, 4096, 0)],
                vec![op(GROUP - 3, 6, 0)],
                vec![op(tail, 5, 0)],
                vec![op(0, 10, 20), op(3 * GROUP + 1, 20, 0)],
                vec![op(GROUP, 2 * GROUP as usize, 0)],
            ];
            for ops in cases {
                let ranges = plan_ranges(&chunk, &ops).unwrap();
                let total = ops
                    .iter()
                    .map(|op| op.output_offset + op.length)
                    .max()
                    .unwrap();
                let mut out = vec![0xAA; total + 3];
                verify_and_copy(&chunk, &sidecar, &parts(&body, &ranges), &ops, &mut out).unwrap();
                for op in &ops {
                    assert_eq!(
                        &out[op.output_offset..op.output_offset + op.length],
                        &body[op.chunk_offset as usize..op.chunk_offset as usize + op.length]
                    );
                }
                assert_eq!(
                    &out[total..],
                    &[0xAA; 3],
                    "bytes past the ops stay untouched"
                );
            }
        }
    }

    #[test]
    fn any_damaged_data_byte_fails_and_leaves_the_output_untouched() {
        let body = body(1024 * 1024, 7);
        let chunk = chunk_of(&body);
        let sidecar = sidecar_for(&body);
        let ops = [op(GROUP + 10, 100, 0)];
        let ranges = plan_ranges(&chunk, &ops).unwrap();
        // Every byte of the fetched group is covered, not only the requested ones.
        for position in [0, 9, 10, 109, 110, GROUP as usize - 1] {
            let mut fetched = parts(&body, &ranges);
            fetched[0].1[position] ^= 1;
            let mut out = vec![0x55; 100];
            let error = verify_and_copy(&chunk, &sidecar, &fetched, &ops, &mut out).unwrap_err();
            assert_eq!(error.code(), afs_error::NODE_TRANSFER_CORRUPT_DATA);
            assert_eq!(out, vec![0x55; 100]);
        }
    }

    #[test]
    fn any_damaged_sidecar_byte_on_the_proof_path_fails() {
        let body = body(1024 * 1024, 8);
        let chunk = chunk_of(&body);
        let sidecar = sidecar_for(&body);
        let ops = [op(0, 100, 0)];
        let fetched = parts(&body, &plan_ranges(&chunk, &ops).unwrap());
        let mut failures = 0;
        for position in 0..sidecar.len() {
            let mut damaged = sidecar.clone();
            damaged[position] ^= 1;
            let mut out = vec![0; 100];
            if verify_and_copy(&chunk, &damaged, &fetched, &ops, &mut out).is_err() {
                failures += 1;
            } else {
                // A parent pair this range's proof never loads may differ; the
                // bytes handed out are still proven against the root.
                assert_eq!(out, &body[..100]);
            }
        }
        // The whole header and the log2(32) parent pairs on the path.
        assert!(failures >= SIDECAR_HEADER_BYTES + 5 * 64, "{failures}");
    }

    #[test]
    fn a_self_consistent_pair_for_other_content_fails_against_the_identity_root() {
        // Review condition 1: data and sidecar replaced together by another,
        // internally consistent chunk of the same length.
        let original = body(1024 * 1024, 9);
        let chunk = chunk_of(&original);
        let other = body(1024 * 1024, 10);
        let ops = [op(0, 100, 0)];
        let ranges = plan_ranges(&chunk, &ops).unwrap();
        let mut out = vec![0x11; 100];

        // The other chunk's own sidecar names another root in its header.
        let other_sidecar = sidecar_for(&other);
        let error = verify_and_copy(
            &chunk,
            &other_sidecar,
            &parts(&other, &ranges),
            &ops,
            &mut out,
        )
        .unwrap_err();
        assert_eq!(error.code(), afs_error::NODE_TRANSFER_CORRUPT_DATA);

        // The same forgery with the header rewritten to claim the expected
        // root: the parent hashes still lead to the other content's root.
        let mut forged = other_sidecar.clone();
        forged[..SIDECAR_HEADER_BYTES]
            .copy_from_slice(&sidecar_for(&original)[..SIDECAR_HEADER_BYTES]);
        validate_sidecar(&chunk, &forged).unwrap();
        let error =
            verify_and_copy(&chunk, &forged, &parts(&other, &ranges), &ops, &mut out).unwrap_err();
        assert_eq!(error.code(), afs_error::NODE_TRANSFER_CORRUPT_DATA);
        assert_eq!(out, vec![0x11; 100]);
    }

    #[test]
    fn parts_that_do_not_match_the_plan_are_rejected() {
        let body = body(1024 * 1024, 11);
        let chunk = chunk_of(&body);
        let sidecar = sidecar_for(&body);
        let ops = [op(GROUP, 10, 0)];
        let mut out = vec![0; 10];
        for (range, bytes) in [
            // Short body, unaligned start, range past the chunk, and a part
            // that does not hold the requested bytes.
            (
                GROUP..2 * GROUP,
                body[GROUP as usize..2 * GROUP as usize - 1].to_vec(),
            ),
            (
                GROUP + 1..2 * GROUP,
                body[GROUP as usize + 1..2 * GROUP as usize].to_vec(),
            ),
            (1024 * 1024..1024 * 1024 + GROUP, vec![0; GROUP as usize]),
            (0..GROUP, body[..GROUP as usize].to_vec()),
        ] {
            assert!(verify_and_copy(&chunk, &sidecar, &[(range, bytes)], &ops, &mut out).is_err());
        }
        // An output buffer that is too short is an error, not a panic.
        let fetched = parts(&body, std::slice::from_ref(&(GROUP..2 * GROUP)));
        assert!(
            verify_and_copy(&chunk, &sidecar, &fetched, &[op(GROUP, 10, 5)], &mut out).is_err()
        );
    }

    #[test]
    fn sidecar_cache_is_bounded_and_absent_wins_over_a_dropped_sidecar() {
        let cache = SidecarCache::default();
        let id = |index: usize| ChunkId::new(format!("chunk-{index}"));
        for index in 0..SIDECAR_CACHE_ENTRIES + 1 {
            cache.insert(&id(index), Arc::new(vec![index as u8]));
        }
        assert!(matches!(cache.lookup(&id(0)), SidecarState::Unknown));
        assert!(matches!(cache.lookup(&id(1)), SidecarState::Cached(_)));
        cache.mark_absent(&id(1));
        assert!(matches!(cache.lookup(&id(1)), SidecarState::Absent));
        for index in 0..SIDECAR_ABSENT_ENTRIES {
            cache.mark_absent(&id(10_000 + index));
        }
        assert!(matches!(cache.lookup(&id(1)), SidecarState::Unknown));
        let state = cache.state.lock().unwrap();
        assert!(state.present.len() <= SIDECAR_CACHE_ENTRIES);
        assert_eq!(state.present.len(), state.present_order.len());
        assert_eq!(state.absent.len(), SIDECAR_ABSENT_ENTRIES);
    }
}
