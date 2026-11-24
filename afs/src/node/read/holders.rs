//! H2 step 2: the versions this Node shares with other readers (plan/15
//! §1.2.1, §2.2.3).
//!
//! The registry is the Node-side twin of Meta's shared reader entries. Its
//! membership is the set of shared soft-reader numbers: a number joins when
//! `GcReferences::finish_shared_reader` accepts it and leaves when its
//! `GcPin` drops, both under the soft-reader lock, so the registry, the
//! Node's live readers and the next `report_readers` always describe the same
//! readers. Meta may keep an entry a little longer (until the release
//! arrives); a holder then answers `NOT_READY`, never more.
//!
//! The registry also holds the byte intervals each shared version exposes,
//! computed with the same [`crate::dfs::read_chunk_intervals`] Meta signs
//! grants with. A holder grant does not name Chunks, so this is where a
//! holder replaces Meta's per-Chunk range check: a version that is not
//! shared here, or whose layout is not (completely) known yet, serves nothing.
use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};

use crate::dfs::{ChunkId, ChunkObject, FileVersionId, LayoutRoot, LayoutRootId};

/// The intervals one shared version exposes, by Chunk.
#[derive(Debug)]
pub(crate) struct HolderLayout {
    layout_root_id: LayoutRootId,
    intervals: HashMap<ChunkId, Vec<(u64, u64)>>,
}

impl HolderLayout {
    /// `None` when the layout is not consistent (an extent exceeding the
    /// length its Chunk identity carries): the version then stays incomplete
    /// and serves nothing.
    ///
    /// The intervals come from [`crate::dfs::read_chunk_intervals`], the
    /// function Meta signs grants with. It uses the Chunk length only as a
    /// guard (`extent end <= length`); the intervals themselves depend on the
    /// extents and the file length alone. A content-addressed Chunk id carries
    /// its length and is checked against it; a CRC-segment id does not, so the
    /// guard is left open there. For every layout Meta accepted, the intervals
    /// are therefore exactly Meta's (pinned by a test), and a served range can
    /// never pass the end of the holder's own Chunk body anyway.
    fn build(layout: &LayoutRoot) -> Option<Self> {
        let mut chunks = BTreeSet::new();
        for extent in &layout.inline_extents {
            chunks.insert(extent.chunk_id.clone());
        }
        let mut intervals = HashMap::with_capacity(chunks.len());
        for id in chunks {
            let chunk = ChunkObject::from_content_addressed_id(&id).unwrap_or(ChunkObject {
                id: id.clone(),
                length: u64::MAX,
                content_digest: crate::dfs::ContentDigest {
                    algorithm: crate::dfs::DigestAlgorithm::Crc32cSegmentsV1,
                    bytes: [0; 32],
                },
                encoding: crate::dfs::ChunkEncoding::Raw,
            });
            let ranges = crate::dfs::read_chunk_intervals(layout, &chunk).ok()?;
            intervals.insert(id, ranges);
        }
        Some(Self {
            layout_root_id: layout.id.clone(),
            intervals,
        })
    }
}

#[derive(Debug, Default)]
struct Shared {
    readers: BTreeSet<u64>,
    layout: Option<Arc<HolderLayout>>,
    /// The layout was seen but could not be indexed completely.
    incomplete: bool,
}

/// Outcome of a holder range check (plan/15 §2.2.3).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum HolderRange {
    /// Inside the intervals the version exposes.
    Covered,
    /// The version is not shared here, or its layout is not known completely:
    /// `NOT_READY` (the caller is already authorized).
    NotReady,
    /// Outside the version's intervals, another layout, or a Chunk the
    /// layout does not name: denied.
    Denied,
}

/// See the module documentation.
#[derive(Debug, Default)]
pub(crate) struct HolderRegistry {
    inner: Mutex<HashMap<FileVersionId, Shared>>,
}

impl HolderRegistry {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<FileVersionId, Shared>> {
        // Every method leaves the map consistent before it can panic.
        self.inner.lock().unwrap_or_else(|error| error.into_inner())
    }

    /// A shared reader of `version` joined (called under the soft lock).
    pub(crate) fn retain(&self, version: &FileVersionId, reader: u64) {
        self.lock()
            .entry(version.clone())
            .or_default()
            .readers
            .insert(reader);
    }

    /// A shared reader of `version` left (called under the soft lock); the
    /// version leaves with its last reader.
    pub(crate) fn release(&self, version: &FileVersionId, reader: u64) {
        let mut inner = self.lock();
        if let Some(shared) = inner.get_mut(version) {
            shared.readers.remove(&reader);
            if shared.readers.is_empty() {
                inner.remove(version);
            }
        }
    }

    /// The checked layout of `version` is known: index it once, only while
    /// the version is shared. `layout` must be the one `ReadOnlyVersion::checked`
    /// bound to `version`.
    pub(crate) fn attach(&self, version: &FileVersionId, layout: &LayoutRoot) {
        let mut inner = self.lock();
        let Some(shared) = inner.get_mut(version) else {
            return;
        };
        if shared.layout.is_some() || shared.incomplete {
            return;
        }
        match HolderLayout::build(layout) {
            Some(index) => shared.layout = Some(Arc::new(index)),
            None => shared.incomplete = true,
        }
    }

    /// Whether `version` is shared here (whatever its layout state).
    pub(crate) fn is_shared(&self, version: &FileVersionId) -> bool {
        self.lock().contains_key(version)
    }

    /// The holder range check of one operation (plan/15 §2.2.3 items 1–6).
    /// `layout_root_id` is the MAC-protected one of the holder grant.
    pub(crate) fn check(
        &self,
        version: &FileVersionId,
        layout_root_id: &LayoutRootId,
        chunk: &ChunkId,
        offset: u64,
        length: u64,
    ) -> HolderRange {
        let layout = {
            let inner = self.lock();
            match inner.get(version) {
                Some(Shared {
                    layout: Some(layout),
                    ..
                }) => layout.clone(),
                _ => return HolderRange::NotReady,
            }
        };
        if &layout.layout_root_id != layout_root_id {
            return HolderRange::Denied;
        }
        match layout.intervals.get(chunk) {
            Some(intervals) if crate::dfs::read_range_covered(intervals, offset, length) => {
                HolderRange::Covered
            }
            _ => HolderRange::Denied,
        }
    }

    #[cfg(test)]
    pub(crate) fn readers(&self, version: &FileVersionId) -> Vec<u64> {
        self.lock()
            .get(version)
            .map(|shared| shared.readers.iter().copied().collect())
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dfs::{Extent, OperationId};
    use crate::node::chunk::StagedChunk;

    fn chunk(bytes: usize, seed: u8) -> ChunkObject {
        StagedChunk::new(OperationId::new("holders"), vec![seed; bytes]).chunk
    }

    fn layout(id: &str, file_length: u64, extents: &[(&ChunkObject, u64, u64, u64)]) -> LayoutRoot {
        LayoutRoot {
            id: LayoutRootId::new(id),
            file_length,
            inline_extents: extents
                .iter()
                .map(|(chunk, file_offset, chunk_offset, length)| Extent {
                    file_offset: *file_offset,
                    length: *length,
                    chunk_id: chunk.id.clone(),
                    chunk_offset: *chunk_offset,
                })
                .collect(),
        }
    }

    fn shared(
        registry: &HolderRegistry,
        version: &str,
        reader: u64,
        layout: &LayoutRoot,
    ) -> FileVersionId {
        let version = FileVersionId::new(version);
        registry.retain(&version, reader);
        registry.attach(&version, layout);
        version
    }

    #[test]
    fn holder_registry_follows_its_shared_readers() {
        let registry = HolderRegistry::default();
        let version = FileVersionId::new("v");
        registry.retain(&version, 3);
        registry.retain(&version, 5);
        registry.release(&version, 3);
        assert!(registry.is_shared(&version));
        assert_eq!(registry.readers(&version), vec![5]);
        registry.release(&version, 5);
        assert!(!registry.is_shared(&version));
        // A layout seen after the last reader left is not indexed.
        let a = chunk(16, 1);
        registry.attach(&version, &layout("l", 16, &[(&a, 0, 0, 16)]));
        assert!(!registry.is_shared(&version));
    }

    #[test]
    fn holder_range_uses_the_grant_layout_and_the_meta_intervals() {
        let registry = HolderRegistry::default();
        let a = chunk(16, 1);
        let b = chunk(16, 2);
        let v1 = shared(&registry, "v1", 1, &layout("l1", 16, &[(&a, 0, 0, 16)]));
        let v2 = shared(&registry, "v2", 2, &layout("l2", 16, &[(&b, 0, 0, 16)]));
        let check = |version: &FileVersionId, layout: &str, chunk: &ChunkObject, offset, length| {
            registry.check(
                version,
                &LayoutRootId::new(layout),
                &chunk.id,
                offset,
                length,
            )
        };
        assert_eq!(check(&v1, "l1", &a, 0, 16), HolderRange::Covered);
        // T6: a V1 grant cannot reach a Chunk only V2 exposes, and a V2
        // layout id never checks against V1.
        assert_eq!(check(&v1, "l1", &b, 0, 16), HolderRange::Denied);
        assert_eq!(check(&v1, "l2", &a, 0, 16), HolderRange::Denied);
        assert_eq!(check(&v2, "l2", &b, 0, 16), HolderRange::Covered);
        // T10/T10b: out of the interval, or a Chunk outside the layout.
        assert_eq!(check(&v1, "l1", &a, 8, 9), HolderRange::Denied);
        assert_eq!(check(&v1, "l1", &chunk(16, 9), 0, 1), HolderRange::Denied);
        assert_eq!(check(&v1, "l1", &a, 0, 0), HolderRange::Denied);
    }

    #[test]
    fn holder_range_cuts_a_truncated_tail_like_meta() {
        // T7: the extent says 16 bytes but the file ends at 10.
        let registry = HolderRegistry::default();
        let a = chunk(16, 3);
        let root = layout("l", 10, &[(&a, 0, 0, 16)]);
        let version = shared(&registry, "v", 1, &root);
        let check = |offset, length| registry.check(&version, &root.id, &a.id, offset, length);
        assert_eq!(check(0, 10), HolderRange::Covered);
        assert_eq!(check(0, 11), HolderRange::Denied);
        assert_eq!(check(10, 6), HolderRange::Denied);
        // The registry's intervals are exactly Meta's.
        assert_eq!(
            crate::dfs::read_chunk_intervals(&root, &a).unwrap(),
            vec![(0, 10)]
        );
    }

    /// A CRC-segment Chunk id carries no length; its intervals are still
    /// exactly what Meta computes with the real Chunk object.
    #[test]
    fn holder_range_matches_meta_for_crc_segment_ids() {
        let registry = HolderRegistry::default();
        let id = ChunkId::new(format!("crc32c-v2-{}-{}", "1".repeat(32), "2".repeat(32)));
        let real = ChunkObject {
            id: id.clone(),
            length: 4096,
            content_digest: crate::dfs::ContentDigest {
                algorithm: crate::dfs::DigestAlgorithm::Crc32cSegmentsV1,
                bytes: [7; 32],
            },
            encoding: crate::dfs::ChunkEncoding::Raw,
        };
        let root = layout(
            "l",
            3000,
            &[(&real, 0, 100, 2000), (&real, 2000, 2500, 1500)],
        );
        let version = shared(&registry, "v", 1, &root);
        let meta = crate::dfs::read_chunk_intervals(&root, &real).unwrap();
        assert_eq!(meta, vec![(100, 2000), (2500, 1000)]);
        for (offset, length) in [(100, 2000), (2500, 1000), (99, 1), (2100, 400), (3499, 2)] {
            let expected = if crate::dfs::read_range_covered(&meta, offset, length) {
                HolderRange::Covered
            } else {
                HolderRange::Denied
            };
            assert_eq!(
                registry.check(&version, &root.id, &id, offset, length),
                expected
            );
        }
    }

    #[test]
    fn holder_range_is_not_ready_without_a_complete_shared_layout() {
        // T8/T9: never shared, shared but no layout yet, the empty sentinel,
        // and a layout whose Chunk identity cannot be derived.
        let registry = HolderRegistry::default();
        let a = chunk(16, 4);
        let id = LayoutRootId::new("l");
        let never = FileVersionId::new("never");
        assert_eq!(
            registry.check(&never, &id, &a.id, 0, 1),
            HolderRange::NotReady
        );
        let pending = FileVersionId::new("pending");
        registry.retain(&pending, 1);
        assert_eq!(
            registry.check(&pending, &id, &a.id, 0, 1),
            HolderRange::NotReady
        );
        let empty = FileVersionId::new("empty");
        assert_eq!(
            registry.check(&empty, &LayoutRootId::new("empty"), &a.id, 0, 1),
            HolderRange::NotReady
        );
        // An extent longer than its content-addressed Chunk: inconsistent,
        // so the version stays incomplete.
        let short = chunk(4, 5);
        let broken = FileVersionId::new("broken");
        registry.retain(&broken, 2);
        registry.attach(&broken, &layout("l", 8, &[(&short, 0, 0, 8)]));
        assert_eq!(
            registry.check(&broken, &id, &short.id, 0, 1),
            HolderRange::NotReady
        );
    }
}

/// Chunks this Node is fetching or installing right now, so a holder can let
/// a reader wait for one instead of answering `NOT_READY` (plan/15 §1.3).
/// Every change wakes the waiters; they re-check their Chunk themselves.
#[derive(Debug, Default)]
pub(crate) struct HolderInFlight {
    chunks: Mutex<HashMap<ChunkId, usize>>,
    changed: tokio::sync::Notify,
}

/// Marks one Chunk in flight until dropped.
#[derive(Debug)]
pub(crate) struct InFlightGuard {
    owner: Arc<HolderInFlight>,
    chunk: ChunkId,
}

impl HolderInFlight {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<ChunkId, usize>> {
        self.chunks
            .lock()
            .unwrap_or_else(|error| error.into_inner())
    }

    pub(crate) fn begin(self: &Arc<Self>, chunk: &ChunkId) -> InFlightGuard {
        *self.lock().entry(chunk.clone()).or_default() += 1;
        InFlightGuard {
            owner: self.clone(),
            chunk: chunk.clone(),
        }
    }

    pub(crate) fn contains(&self, chunk: &ChunkId) -> bool {
        self.lock().contains_key(chunk)
    }

    /// Something was installed or finished: wake every waiter.
    pub(crate) fn notify(&self) {
        self.changed.notify_waiters();
    }

    /// A future that completes at the next change. Create it before checking
    /// the state it waits for, so no change between the two is missed.
    pub(crate) fn changed(&self) -> tokio::sync::futures::Notified<'_> {
        self.changed.notified()
    }
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        let mut chunks = self.owner.lock();
        if let Some(count) = chunks.get_mut(&self.chunk) {
            *count -= 1;
            if *count == 0 {
                chunks.remove(&self.chunk);
            }
        }
        drop(chunks);
        self.owner.notify();
    }
}

#[cfg(test)]
mod in_flight_tests {
    use super::*;

    #[tokio::test]
    async fn in_flight_chunks_count_and_wake_waiters() {
        let flights = HolderInFlight::new();
        let chunk = ChunkId::new("c");
        let first = flights.begin(&chunk);
        let second = flights.begin(&chunk);
        assert!(flights.contains(&chunk));
        let changed = flights.changed();
        drop(first);
        tokio::time::timeout(std::time::Duration::from_secs(1), changed)
            .await
            .unwrap();
        assert!(flights.contains(&chunk));
        drop(second);
        assert!(!flights.contains(&chunk));
    }
}
