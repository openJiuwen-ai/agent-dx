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
//! computed with the same function Meta signs grants with
//! ([`crate::dfs::chunk_extent_intervals`], the core of
//! [`crate::dfs::read_chunk_intervals`]). A holder grant does not name Chunks, so this is where a
//! holder replaces Meta's per-Chunk range check: a version that is not
//! shared here, or whose layout is not (completely) known yet, serves nothing.
use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};

use crate::dfs::{ChunkId, ChunkObject, FileVersionId, LayoutRoot, LayoutRootId};

use super::holder_partial::HolderPartials;

/// The intervals one shared version exposes, by Chunk.
#[derive(Debug)]
pub(crate) struct HolderLayout {
    layout_root_id: LayoutRootId,
    /// Exact-length slices, so the bytes they keep are known (round 3).
    intervals: HashMap<ChunkId, Box<[(u64, u64)]>>,
    /// The budget bytes this index holds, given back when the last
    /// reference to it drops (a range check may still hold one after its
    /// version left).
    charge: Option<Charge>,
}

/// Budget bytes held until dropped.
#[derive(Debug)]
struct Charge {
    used: Arc<std::sync::atomic::AtomicU64>,
    bytes: u64,
}

impl Drop for Charge {
    fn drop(&mut self) {
        self.used
            .fetch_sub(self.bytes, std::sync::atomic::Ordering::SeqCst);
    }
}

/// Buckets a hash map allocates for `capacity` entries (hashbrown: at
/// least 4, then 8, then the next power of two above 8/7 of the entries).
fn buckets_for(capacity: usize) -> usize {
    if capacity < 4 {
        4
    } else if capacity < 8 {
        8
    } else {
        (capacity * 8 / 7).next_power_of_two()
    }
}

/// Buckets behind a map's reported capacity (the inverse of the above).
fn buckets_of(capacity: usize) -> usize {
    if capacity < 7 {
        capacity + 1
    } else {
        capacity / 7 * 8
    }
}

impl HolderLayout {
    /// `None` when the layout is not consistent (an extent exceeding the
    /// length its Chunk identity carries): the version then stays incomplete
    /// and serves nothing.
    ///
    /// The intervals come from [`crate::dfs::chunk_extent_intervals`], the
    /// core of [`crate::dfs::read_chunk_intervals`], the function Meta signs
    /// grants with. It uses the Chunk length only as a guard (`extent end <=
    /// length`); the intervals themselves depend on the extents and the file
    /// length alone. A content-addressed Chunk id carries its length and is
    /// checked against it; a CRC-segment id does not, so the guard is left
    /// open there. For every layout Meta accepted, the intervals are
    /// therefore exactly Meta's (pinned by a test), and a served range can
    /// never pass the end of the holder's own Chunk body anyway.
    fn build(layout: &LayoutRoot) -> Option<Self> {
        #[cfg(test)]
        if let Some(hook) = BUILD_HOOK.with(|hook| hook.borrow_mut().take()) {
            hook();
        }
        // One pass over the extents, grouped by Chunk (!318 P2-1); each
        // group then goes through Meta's own interval function.
        let mut chunks: HashMap<&ChunkId, Vec<&crate::dfs::Extent>> = HashMap::new();
        for extent in &layout.inline_extents {
            #[cfg(test)]
            crate::dfs::count_interval_extent_visit();
            chunks.entry(&extent.chunk_id).or_default().push(extent);
        }
        let mut intervals = HashMap::with_capacity(chunks.len());
        for (id, extents) in chunks {
            let chunk = ChunkObject::from_content_addressed_id(id).unwrap_or(ChunkObject {
                id: id.clone(),
                length: u64::MAX,
                content_digest: crate::dfs::ContentDigest {
                    algorithm: crate::dfs::DigestAlgorithm::Crc32cSegmentsV1,
                    bytes: [0; 32],
                },
                encoding: crate::dfs::ChunkEncoding::Raw,
            });
            let ranges =
                crate::dfs::chunk_extent_intervals(layout.file_length, extents, &chunk).ok()?;
            intervals.insert(id.clone(), ranges.into_boxed_slice());
        }
        Some(Self {
            layout_root_id: layout.id.clone(),
            intervals,
            charge: None,
        })
    }

    /// Bytes of one map bucket: an entry and its control byte.
    const BUCKET_BYTES: usize = std::mem::size_of::<(ChunkId, Box<[(u64, u64)]>)>() + 1;
    /// Bytes of one interval.
    const INTERVAL_BYTES: usize = std::mem::size_of::<(u64, u64)>();
    /// The control bytes a map keeps besides one per bucket.
    const GROUP_BYTES: usize = 16;

    /// An upper bound of [`Self::retained_bytes`] for `layout`, known before
    /// building: the map has at most one entry per extent, so at most the
    /// buckets of that many; each entry keeps its id (cloned: exact length)
    /// and at most one interval per extent of its Chunk (exact slices).
    fn estimate(layout: &LayoutRoot) -> u64 {
        let ids_and_intervals: usize = layout
            .inline_extents
            .iter()
            .map(|extent| extent.chunk_id.0.len() + Self::INTERVAL_BYTES)
            .sum();
        (std::mem::size_of::<Self>()
            + layout.id.0.len()
            + Self::GROUP_BYTES
            + buckets_for(layout.inline_extents.len()) * Self::BUCKET_BYTES
            + ids_and_intervals) as u64
    }

    /// The bytes this index keeps: its map's buckets and control bytes, each
    /// id's and each interval slice's heap, and itself.
    fn retained_bytes(&self) -> u64 {
        let heap: usize = self
            .intervals
            .iter()
            .map(|(id, ranges)| id.0.capacity() + ranges.len() * Self::INTERVAL_BYTES)
            .sum();
        (std::mem::size_of::<Self>()
            + self.layout_root_id.0.capacity()
            + Self::GROUP_BYTES
            + buckets_of(self.intervals.capacity()) * Self::BUCKET_BYTES
            + heap) as u64
    }
}

/// Bytes all holder layout indexes of a Node may keep (!318 round 2 item
/// 3). An index that does not fit is not built: its version answers not
/// ready, so readers use the copies, until bytes return.
pub(crate) const HOLDER_INDEX_BUDGET: u64 = 64 * 1024 * 1024;

#[cfg(test)]
std::thread_local! {
    /// Runs once inside the next [`HolderLayout::build`] on this thread.
    static BUILD_HOOK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
}

#[derive(Debug, Default)]
struct Shared {
    readers: BTreeSet<u64>,
    layout: Option<Arc<HolderLayout>>,
    /// The layout was seen but could not be indexed completely.
    incomplete: bool,
    /// The build that has the right to index this generation, if any.
    building: Option<u64>,
}

#[derive(Debug, Default)]
struct State {
    versions: HashMap<FileVersionId, Shared>,
    /// Identifies builds.
    builds: u64,
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
#[derive(Debug)]
pub(crate) struct HolderRegistry {
    inner: Mutex<State>,
    /// Budget bytes held: published indexes alive (also ones a range check
    /// still holds) and builds still running. Taken only under `inner`.
    used: Arc<std::sync::atomic::AtomicU64>,
    /// See [`HOLDER_INDEX_BUDGET`].
    budget: u64,
    /// Notified whenever a version stops being shared (!318 P3-3).
    unshared: tokio::sync::Notify,
    /// Tests: layout indexes built.
    #[cfg(test)]
    builds: std::sync::atomic::AtomicUsize,
    /// Tests: extents the size estimate walked.
    #[cfg(test)]
    estimate_visits: std::sync::atomic::AtomicUsize,
    /// Tests: attach calls.
    #[cfg(test)]
    attaches: std::sync::atomic::AtomicUsize,
}

impl Default for HolderRegistry {
    fn default() -> Self {
        Self {
            inner: Mutex::default(),
            used: Arc::default(),
            budget: HOLDER_INDEX_BUDGET,
            unshared: tokio::sync::Notify::new(),
            #[cfg(test)]
            builds: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(test)]
            estimate_visits: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(test)]
            attaches: std::sync::atomic::AtomicUsize::new(0),
        }
    }
}

/// The right to build one generation's index; given up when dropped
/// unfinished, also when the build unwinds, so no build stays marked.
struct Claim<'a> {
    registry: &'a HolderRegistry,
    version: &'a FileVersionId,
    build: u64,
}

impl Drop for Claim<'_> {
    fn drop(&mut self) {
        let mut inner = self.registry.lock();
        if let Some(shared) = inner.versions.get_mut(self.version)
            && shared.building == Some(self.build)
        {
            shared.building = None;
        }
    }
}

impl HolderRegistry {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// A registry whose indexes may keep `bytes` (tests).
    #[cfg(test)]
    pub(crate) fn with_index_budget(bytes: u64) -> Arc<Self> {
        Arc::new(Self {
            budget: bytes,
            ..Self::default()
        })
    }

    /// Budget bytes held (tests).
    #[cfg(test)]
    pub(crate) fn index_bytes(&self) -> u64 {
        self.used.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        // Every method leaves the state consistent before it can panic.
        self.inner.lock().unwrap_or_else(|error| error.into_inner())
    }

    /// `bytes` of the budget, if they fit; under `inner`, so admissions are
    /// ordered (a release only makes room).
    fn take(&self, _inner: &State, bytes: u64) -> Option<Charge> {
        use std::sync::atomic::Ordering::SeqCst;
        if self.used.load(SeqCst).saturating_add(bytes) > self.budget {
            return None;
        }
        self.used.fetch_add(bytes, SeqCst);
        Some(Charge {
            used: self.used.clone(),
            bytes,
        })
    }

    /// A shared reader of `version` joined (called under the soft lock).
    pub(crate) fn retain(&self, version: &FileVersionId, reader: u64) {
        self.lock()
            .versions
            .entry(version.clone())
            .or_default()
            .readers
            .insert(reader);
    }

    /// A shared reader of `version` left (called under the soft lock); the
    /// version leaves with its last reader. Its index's bytes return when
    /// the last reference to the index drops; a build still running keeps
    /// its own reservation until it ends and publishes nothing.
    pub(crate) fn release(&self, version: &FileVersionId, reader: u64) {
        let mut inner = self.lock();
        if let Some(shared) = inner.versions.get_mut(version) {
            shared.readers.remove(&reader);
            if shared.readers.is_empty() {
                let gone = inner.versions.remove(version);
                drop(inner);
                drop(gone);
                // A holder request waiting for this version ends now.
                self.unshared.notify_waiters();
            }
        }
    }

    /// A future that completes when a version next stops being shared.
    /// Create it before checking [`Self::is_shared`], so no release between
    /// the two is missed.
    pub(crate) fn unshared(&self) -> tokio::sync::futures::Notified<'_> {
        self.unshared.notified()
    }

    /// The checked layout of `version` is known: index it once, only while
    /// the version is shared. `layout` must be the one `ReadOnlyVersion::checked`
    /// bound to `version`.
    ///
    /// Only the call that claims the build of this generation of the version
    /// does any work (!318 round 3 item 2): every other one (version not
    /// shared here, already indexed, inconsistent, or being built) returns
    /// after one map lookup, without walking the layout. The claim then
    /// sizes the index (an upper bound of what it keeps) and reserves that
    /// from the Node-wide budget ([`HOLDER_INDEX_BUDGET`]); a version that
    /// does not fit stays unindexed (not ready: readers use the copies) and
    /// is tried again at its next attach. Sizing and building run outside the
    /// registry lock. The index, charged its real size, is published only if
    /// this is still the generation's build; otherwise it is dropped and its
    /// bytes return. A build keeps its reservation until it ends, also when
    /// its version left meanwhile, and gives it back exactly once, also when
    /// it unwinds.
    pub(crate) fn attach(&self, version: &FileVersionId, layout: &LayoutRoot) {
        #[cfg(test)]
        self.attaches
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let claim = {
            let mut inner = self.lock();
            let State { versions, builds } = &mut *inner;
            let Some(shared) = versions.get_mut(version) else {
                return;
            };
            if shared.layout.is_some() || shared.incomplete || shared.building.is_some() {
                return;
            }
            *builds += 1;
            shared.building = Some(*builds);
            Claim {
                registry: self,
                version,
                build: *builds,
            }
        };
        #[cfg(test)]
        self.estimate_visits.fetch_add(
            layout.inline_extents.len(),
            std::sync::atomic::Ordering::SeqCst,
        );
        let estimate = HolderLayout::estimate(layout);
        let reserved = {
            let inner = self.lock();
            let current = inner
                .versions
                .get(version)
                .is_some_and(|shared| shared.building == Some(claim.build));
            if !current {
                return;
            }
            let Some(reserved) = self.take(&inner, estimate) else {
                return;
            };
            reserved
        };
        #[cfg(test)]
        self.builds
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let built = HolderLayout::build(layout);
        let mut inner = self.lock();
        let built = built.map(|mut index| {
            let kept = index.retained_bytes();
            debug_assert!(kept <= estimate, "index keeps {kept} > estimate {estimate}");
            // The real size replaces the reservation (never more).
            index.charge = Some(Charge {
                used: self.used.clone(),
                bytes: kept,
            });
            self.used
                .fetch_add(kept, std::sync::atomic::Ordering::SeqCst);
            index
        });
        drop(reserved);
        let Some(shared) = inner.versions.get_mut(version) else {
            return;
        };
        if shared.building != Some(claim.build) {
            return;
        }
        shared.building = None;
        match built {
            Some(index) => shared.layout = Some(Arc::new(index)),
            None => shared.incomplete = true,
        }
        drop(inner);
        drop(claim);
    }

    /// Whether `version` is shared here (whatever its layout state).
    pub(crate) fn is_shared(&self, version: &FileVersionId) -> bool {
        self.lock().versions.contains_key(version)
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
            match inner.versions.get(version) {
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

    /// Tests: the published index of `version`, as a range check holds it.
    #[cfg(test)]
    pub(crate) fn layout_arc(&self, version: &FileVersionId) -> Option<Arc<HolderLayout>> {
        self.lock()
            .versions
            .get(version)
            .and_then(|shared| shared.layout.clone())
    }

    /// Tests: attach calls so far.
    #[cfg(test)]
    pub(crate) fn attach_calls(&self) -> usize {
        self.attaches.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Tests: (extents walked by size estimates, indexes built) so far.
    #[cfg(test)]
    pub(crate) fn work_done(&self) -> (usize, usize) {
        use std::sync::atomic::Ordering::SeqCst;
        (self.estimate_visits.load(SeqCst), self.builds.load(SeqCst))
    }

    #[cfg(test)]
    pub(crate) fn readers(&self, version: &FileVersionId) -> Vec<u64> {
        self.lock()
            .versions
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

    /// !318 P2-1: indexing a version looks at each extent once (one pass
    /// grouping by Chunk), not once per distinct Chunk.
    #[test]
    fn holder_layout_index_visits_each_extent_once() {
        let chunks: Vec<ChunkObject> = (0..256u32)
            .map(|i| StagedChunk::new(OperationId::new("visits"), i.to_le_bytes().repeat(4)).chunk)
            .collect();
        let extents: Vec<(&ChunkObject, u64, u64, u64)> = chunks
            .iter()
            .enumerate()
            .map(|(i, chunk)| (chunk, 16 * i as u64, 0, 16))
            .collect();
        let root = layout("l", 16 * chunks.len() as u64, &extents);
        let registry = HolderRegistry::default();
        let _ = crate::dfs::take_interval_extent_visits();
        let version = shared(&registry, "v", 1, &root);
        let visits = crate::dfs::take_interval_extent_visits();
        assert!(
            visits <= root.inline_extents.len() as u64,
            "{visits} extent visits for {} extents",
            root.inline_extents.len()
        );
        assert_eq!(
            registry.check(&version, &root.id, &chunks[255].id, 0, 16),
            HolderRange::Covered
        );
    }

    /// !318 P2-1: the one-pass index gives exactly Meta's intervals (trimmed
    /// at the file length, overlaps merged, several extents per Chunk), and
    /// an inconsistent extent still leaves the version incomplete.
    #[test]
    fn holder_layout_index_matches_meta_for_every_chunk() {
        let a = chunk(64, 1);
        let b = chunk(64, 2);
        let id = ChunkId::new(format!("crc32c-v2-{}-{}", "3".repeat(32), "4".repeat(32)));
        let crc = ChunkObject {
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
            300,
            &[
                (&a, 0, 0, 32),
                (&b, 32, 8, 16),
                (&a, 48, 16, 40),
                (&crc, 88, 1000, 100),
                (&b, 188, 40, 24),
                (&crc, 212, 1050, 200),
                (&a, 412, 0, 10),
            ],
        );
        let registry = HolderRegistry::default();
        let version = shared(&registry, "v", 1, &root);
        for object in [&a, &b, &crc] {
            let meta = crate::dfs::read_chunk_intervals(&root, object).unwrap();
            let end = meta.iter().map(|(s, l)| s + l).max().unwrap_or(0) + 2;
            for offset in 0..end {
                for length in [1, 7, 16, 50] {
                    let expected = if crate::dfs::read_range_covered(&meta, offset, length) {
                        HolderRange::Covered
                    } else {
                        HolderRange::Denied
                    };
                    assert_eq!(
                        registry.check(&version, &root.id, &object.id, offset, length),
                        expected,
                        "{} {offset}+{length}",
                        object.id.0
                    );
                }
            }
        }
        let short = chunk(4, 5);
        let broken = shared(
            &registry,
            "broken",
            2,
            &layout("l2", 16, &[(&a, 0, 0, 8), (&short, 8, 0, 8)]),
        );
        assert_eq!(
            registry.check(&broken, &LayoutRootId::new("l2"), &a.id, 0, 8),
            HolderRange::NotReady
        );
    }

    /// !318 P2-1: a version's index is built outside the registry lock, so
    /// another version is checked meanwhile; and a version whose last reader
    /// leaves during the build publishes nothing (a later reader of it starts
    /// from an unknown layout again).
    #[test]
    fn holder_layout_is_built_outside_the_lock_and_dropped_when_unshared() {
        let registry = Arc::new(HolderRegistry::default());
        let a = chunk(16, 1);
        let b = chunk(16, 2);
        let other = shared(&registry, "other", 7, &layout("lo", 16, &[(&b, 0, 0, 16)]));
        let version = FileVersionId::new("v");
        registry.retain(&version, 1);
        let seen = Arc::new(Mutex::new(None));
        {
            let registry = registry.clone();
            let seen = seen.clone();
            let other = other.clone();
            let version = version.clone();
            BUILD_HOOK.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move || {
                    // Never block here: a build under the lock would deadlock.
                    let unlocked = registry.inner.try_lock().is_ok();
                    let mut answer = None;
                    if unlocked {
                        answer =
                            Some(registry.check(&other, &LayoutRootId::new("lo"), &b.id, 0, 16));
                        registry.release(&version, 1);
                    }
                    *seen.lock().unwrap() = Some((unlocked, answer));
                }))
            });
        }
        let before = registry.index_bytes();
        registry.attach(&version, &layout("l", 16, &[(&a, 0, 0, 16)]));
        assert_eq!(
            seen.lock().unwrap().take(),
            Some((true, Some(HolderRange::Covered))),
            "built under the registry lock"
        );
        assert!(!registry.is_shared(&version));
        assert_eq!(
            registry.index_bytes(),
            before,
            "a dropped build keeps bytes"
        );
        registry.retain(&version, 2);
        assert_eq!(
            registry.check(&version, &LayoutRootId::new("l"), &a.id, 0, 16),
            HolderRange::NotReady,
            "a layout built for the old readers was published"
        );
    }

    /// A layout of `n` distinct content-addressed Chunks, 16 bytes each.
    fn wide_layout(id: &str, n: u32) -> (LayoutRoot, Vec<ChunkObject>) {
        let chunks: Vec<ChunkObject> = (0..n)
            .map(|i| {
                let mut bytes = id.as_bytes().to_vec();
                bytes.extend_from_slice(&i.to_le_bytes());
                bytes.resize(16, 0);
                StagedChunk::new(OperationId::new("wide"), bytes).chunk
            })
            .collect();
        let extents: Vec<(&ChunkObject, u64, u64, u64)> = chunks
            .iter()
            .enumerate()
            .map(|(i, chunk)| (chunk, 16 * i as u64, 0, 16))
            .collect();
        (layout(id, 16 * u64::from(n), &extents), chunks)
    }

    /// !318 round 2 item 3: two first attaches of one version at once build
    /// its index once; the other finds it being built and leaves it.
    #[test]
    fn holder_layout_concurrent_first_attaches_build_once() {
        let registry = Arc::new(HolderRegistry::default());
        let (root, chunks) = wide_layout("l", 64);
        let version = FileVersionId::new("v");
        registry.retain(&version, 1);
        {
            let registry = registry.clone();
            let version = version.clone();
            let root = root.clone();
            BUILD_HOOK.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move || {
                    // Another reader opens the version meanwhile.
                    std::thread::spawn(move || registry.attach(&version, &root))
                        .join()
                        .unwrap();
                }))
            });
        }
        registry.attach(&version, &root);
        assert_eq!(
            registry.builds.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "built twice"
        );
        assert_eq!(
            registry.check(&version, &root.id, &chunks[63].id, 0, 16),
            HolderRange::Covered
        );
    }

    /// !318 round 2 item 3: the indexes' kept bytes have a Node-wide budget.
    /// A version that does not fit stays unindexed (its holder answers not
    /// ready, readers use the copies); when the last reader of another one
    /// leaves, its bytes return and the version is indexed at its next open.
    #[test]
    fn holder_layout_indexes_are_bounded_and_their_bytes_return() {
        let (big1, chunks1) = wide_layout("l1", 2000);
        let (big2, chunks2) = wide_layout("l2", 2000);
        let probe = HolderRegistry::with_index_budget(u64::MAX);
        let one = shared(&probe, "probe", 1, &big1);
        let size = probe.index_bytes();
        assert!(
            size > 2000 * 16,
            "an index of 2000 Chunks keeps {size} bytes"
        );
        probe.release(&one, 1);
        assert_eq!(
            probe.index_bytes(),
            0,
            "bytes kept after the last reader left"
        );

        let registry = HolderRegistry::with_index_budget(size + size / 2);
        let v1 = shared(&registry, "v1", 1, &big1);
        let v2 = shared(&registry, "v2", 2, &big2);
        assert_eq!(
            registry.check(&v1, &big1.id, &chunks1[0].id, 0, 16),
            HolderRange::Covered
        );
        assert_eq!(
            registry.check(&v2, &big2.id, &chunks2[0].id, 0, 16),
            HolderRange::NotReady,
            "over the budget: not ready, so readers use the copies"
        );
        assert!(registry.index_bytes() <= size + size / 2);
        registry.release(&v1, 1);
        assert_eq!(registry.index_bytes(), 0);
        registry.attach(&v2, &big2);
        assert_eq!(
            registry.check(&v2, &big2.id, &chunks2[0].id, 0, 16),
            HolderRange::Covered,
            "the returned bytes are used again"
        );
        assert_eq!(registry.index_bytes(), size);
    }

    /// A layout of `n` extents, each its own CRC-segment Chunk of 16 bytes.
    fn crc_layout(id: &str, n: u32) -> (LayoutRoot, Vec<ChunkObject>) {
        let chunks: Vec<ChunkObject> = (0..n)
            .map(|i| ChunkObject {
                id: ChunkId::new(format!("crc32c-v2-{id:0>16}{i:016x}-{}", "5".repeat(32))),
                length: 16,
                content_digest: crate::dfs::ContentDigest {
                    algorithm: crate::dfs::DigestAlgorithm::Crc32cSegmentsV1,
                    bytes: [7; 32],
                },
                encoding: crate::dfs::ChunkEncoding::Raw,
            })
            .collect();
        let extents: Vec<(&ChunkObject, u64, u64, u64)> = chunks
            .iter()
            .enumerate()
            .map(|(i, chunk)| (chunk, 16 * i as u64, 0, 16))
            .collect();
        (layout(id, 16 * u64::from(n), &extents), chunks)
    }

    /// Bytes an index really keeps, from its containers' capacities: the
    /// map's buckets (entries and control bytes), each id and each interval
    /// vector. Independent of the registry's own accounting.
    fn kept_bytes(index: &HolderLayout) -> u64 {
        let capacity = index.intervals.capacity();
        let buckets = if capacity < 7 {
            capacity + 1
        } else {
            capacity / 7 * 8
        };
        let entry = std::mem::size_of::<(ChunkId, Box<[(u64, u64)]>)>();
        let heap: usize = index
            .intervals
            .iter()
            .map(|(id, ranges)| id.0.capacity() + std::mem::size_of_val(&**ranges))
            .sum();
        (std::mem::size_of::<HolderLayout>()
            + index.layout_root_id.0.capacity()
            + buckets * (entry + 1)
            + 16
            + heap) as u64
    }

    /// !318 round 3 item 2: an attach that will not build walks nothing:
    /// not for a version not shared here, not for one already indexed, not
    /// for one another attach is building. A concurrent first attach leaves
    /// all the work to the one that builds.
    #[test]
    fn holder_attach_walks_the_layout_only_to_build_it() {
        let (root, chunks) = crc_layout("walk", 4096);
        let registry = Arc::new(HolderRegistry::default());
        let version = FileVersionId::new("v");
        registry.attach(&version, &root);
        assert_eq!(registry.work_done(), (0, 0), "a version not shared here");
        registry.retain(&version, 1);
        {
            let registry = registry.clone();
            let version = version.clone();
            let root = root.clone();
            BUILD_HOOK.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move || {
                    std::thread::spawn(move || registry.attach(&version, &root))
                        .join()
                        .unwrap();
                }))
            });
        }
        registry.attach(&version, &root);
        assert_eq!(
            registry.work_done(),
            (root.inline_extents.len(), 1),
            "one walk and one build for two first attaches"
        );
        assert_eq!(
            registry.check(&version, &root.id, &chunks[7].id, 0, 16),
            HolderRange::Covered
        );
        for _ in 0..100 {
            registry.attach(&version, &root);
        }
        assert_eq!(
            registry.work_done(),
            (root.inline_extents.len(), 1),
            "an indexed version"
        );
    }

    /// !318 round 3 item 3: admission counts what indexes really keep. Five
    /// versions of 65,536 one-extent Chunks each, under the product budget:
    /// the indexes alive never keep more than the budget, and the registry
    /// never counts less than they keep.
    #[test]
    fn holder_indexes_alive_never_exceed_the_budget() {
        let registry = HolderRegistry::default();
        let mut alive = 0;
        for v in 0..5 {
            let (root, _) = crc_layout(&format!("v{v}"), 65_536);
            let version = FileVersionId::new(format!("v{v}"));
            registry.retain(&version, 1);
            registry.attach(&version, &root);
            if let Some(index) = registry.layout_arc(&version) {
                alive += kept_bytes(&index);
            }
            assert!(
                alive <= HOLDER_INDEX_BUDGET,
                "{alive} bytes kept by indexes alive after {} versions",
                v + 1
            );
            assert!(
                registry.index_bytes() >= alive,
                "counted below what is kept"
            );
        }
        // A budget of zero, or one version too large: nothing is published.
        let none = HolderRegistry::with_index_budget(0);
        let (small, chunks) = crc_layout("small", 4);
        let version = FileVersionId::new("small");
        none.retain(&version, 1);
        none.attach(&version, &small);
        assert_eq!(
            none.check(&version, &small.id, &chunks[0].id, 0, 16),
            HolderRange::NotReady
        );
        assert_eq!(none.index_bytes(), 0);
    }

    /// !318 round 3 item 3: the last reader leaves while the version's build
    /// runs, and the version is shared again at once. The old build's bytes
    /// are not free until it ends: the new readers' attach does not fit
    /// meanwhile (range checks answer not ready), the old build publishes
    /// nothing into the new generation and leaves its bytes alone; then the
    /// version is indexed normally.
    #[test]
    fn holder_withdrawn_build_holds_its_bytes_until_it_ends() {
        let (root, chunks) = crc_layout("gen", 2048);
        let estimate = HolderLayout::estimate(&root);
        let registry = HolderRegistry::with_index_budget(estimate + estimate / 2);
        let version = FileVersionId::new("v");
        registry.retain(&version, 1);
        let (paused_tx, paused_rx) = std::sync::mpsc::channel();
        let (resume_tx, resume_rx) = std::sync::mpsc::channel::<()>();
        let old = {
            let registry = registry.clone();
            let version = version.clone();
            let root = root.clone();
            std::thread::spawn(move || {
                BUILD_HOOK.with(|hook| {
                    *hook.borrow_mut() = Some(Box::new(move || {
                        paused_tx.send(()).unwrap();
                        resume_rx
                            .recv_timeout(std::time::Duration::from_secs(10))
                            .unwrap();
                    }))
                });
                registry.attach(&version, &root);
            })
        };
        paused_rx
            .recv_timeout(std::time::Duration::from_secs(10))
            .unwrap();
        registry.release(&version, 1);
        registry.retain(&version, 2);
        registry.attach(&version, &root);
        let check = || registry.check(&version, &root.id, &chunks[0].id, 0, 16);
        assert_eq!(
            check(),
            HolderRange::NotReady,
            "the old build's bytes were reused while it ran"
        );
        assert!(registry.index_bytes() >= estimate);
        resume_tx.send(()).unwrap();
        old.join().unwrap();
        assert_eq!(check(), HolderRange::NotReady, "the old build published");
        assert_eq!(registry.index_bytes(), 0, "the old build kept its bytes");
        registry.attach(&version, &root);
        assert_eq!(check(), HolderRange::Covered);
    }

    /// !318 round 3 item 3: an index a range check still holds keeps its
    /// bytes counted after its version left, until that check lets it go.
    #[test]
    fn holder_index_bytes_return_with_the_last_reference() {
        let (root, _) = crc_layout("arc", 256);
        let registry = HolderRegistry::default();
        let version = FileVersionId::new("v");
        registry.retain(&version, 1);
        registry.attach(&version, &root);
        let held = registry.layout_arc(&version).unwrap();
        let kept = registry.index_bytes();
        assert!(kept >= kept_bytes(&held));
        registry.release(&version, 1);
        assert_eq!(registry.index_bytes(), kept, "returned while still held");
        drop(held);
        assert_eq!(registry.index_bytes(), 0);
    }

    /// !318 round 3 item 3: a build that completes, one that finds the
    /// layout inconsistent and one that unwinds each give back exactly what
    /// they took and leave no build marked; the version can be indexed
    /// again afterwards.
    #[test]
    fn holder_build_ends_give_their_bytes_back_once() {
        let registry = Arc::new(HolderRegistry::default());
        // Inconsistent: the version stays incomplete, no bytes kept.
        let short = chunk(4, 5);
        let broken = FileVersionId::new("broken");
        registry.retain(&broken, 1);
        registry.attach(&broken, &layout("lb", 8, &[(&short, 0, 0, 8)]));
        assert_eq!(registry.index_bytes(), 0);
        // Unwinds.
        let (root, chunks) = crc_layout("unwind", 64);
        let version = FileVersionId::new("v");
        registry.retain(&version, 1);
        BUILD_HOOK
            .with(|hook| *hook.borrow_mut() = Some(Box::new(|| panic!("injected build failure"))));
        let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            registry.attach(&version, &root)
        }));
        assert!(unwound.is_err());
        assert_eq!(registry.index_bytes(), 0, "an unwound build kept its bytes");
        // Not left marked as building: the next attach builds.
        registry.attach(&version, &root);
        assert_eq!(
            registry.check(&version, &root.id, &chunks[0].id, 0, 16),
            HolderRange::Covered,
            "left marked as building"
        );
        let kept = registry.index_bytes();
        assert!(kept > 0);
        registry.release(&version, 1);
        assert_eq!(registry.index_bytes(), 0);
    }
}

/// Chunks this Node is fetching or installing right now, so a holder can let
/// a reader wait for one instead of answering `NOT_READY` (plan/15 §1.3).
/// Every change wakes the waiters; they re-check their Chunk themselves.
#[derive(Debug)]
pub(crate) struct HolderInFlight {
    chunks: Mutex<HashMap<ChunkId, Vec<FlightRange>>>,
    changed: Arc<tokio::sync::Notify>,
    pub(crate) partials: Arc<HolderPartials>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum FlightRange {
    Exact {
        epoch: u64,
        start: u64,
        end: u64,
    },
    #[cfg(test)]
    Whole,
}

/// Marks one range in flight until dropped.
#[derive(Debug)]
pub(crate) struct InFlightGuard {
    owner: Arc<HolderInFlight>,
    chunk: ChunkId,
    range: FlightRange,
}

impl HolderInFlight {
    pub(crate) fn new() -> Arc<Self> {
        let changed = Arc::new(tokio::sync::Notify::new());
        Arc::new(Self {
            chunks: Mutex::new(HashMap::new()),
            partials: Arc::new(HolderPartials::new(changed.clone())),
            changed,
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<ChunkId, Vec<FlightRange>>> {
        self.chunks
            .lock()
            .unwrap_or_else(|error| error.into_inner())
    }

    fn mark(self: &Arc<Self>, chunk: &ChunkId, range: FlightRange) -> InFlightGuard {
        self.lock()
            .entry(chunk.clone())
            .or_default()
            .push(range.clone());
        InFlightGuard {
            owner: self.clone(),
            chunk: chunk.clone(),
            range,
        }
    }

    pub(crate) fn begin_range(
        self: &Arc<Self>,
        chunk: &ChunkId,
        epoch: u64,
        start: u64,
        length: u64,
    ) -> InFlightGuard {
        self.mark(
            chunk,
            FlightRange::Exact {
                epoch,
                start,
                end: start.saturating_add(length),
            },
        )
    }

    #[cfg(test)]
    pub(crate) fn begin(self: &Arc<Self>, chunk: &ChunkId) -> InFlightGuard {
        self.mark(chunk, FlightRange::Whole)
    }

    #[cfg(test)]
    pub(crate) fn contains(&self, chunk: &ChunkId) -> bool {
        self.lock().contains_key(chunk)
    }

    /// An arriving range may complete a covering CRC segment for a fragment
    /// already cached. An unrelated range of the Chunk cannot make progress.
    pub(crate) fn overlaps(&self, chunk: &ChunkId, epoch: u64, offset: u64, length: u64) -> bool {
        let segment = crate::node::chunk_crc32c::SEGMENT as u64;
        let lower = offset / segment * segment;
        let upper = offset
            .saturating_add(length)
            .div_ceil(segment)
            .saturating_mul(segment);
        self.lock().get(chunk).is_some_and(|ranges| {
            ranges.iter().any(|range| match range {
                FlightRange::Exact {
                    epoch: marked_epoch,
                    start,
                    end,
                } => *marked_epoch == epoch && *start < upper && *end > lower,
                #[cfg(test)]
                FlightRange::Whole => true,
            })
        })
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
        if let Some(ranges) = chunks.get_mut(&self.chunk) {
            if let Some(index) = ranges.iter().position(|range| *range == self.range) {
                ranges.swap_remove(index);
            }
            if ranges.is_empty() {
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

    /// !318 P2-4: readers of two versions that reuse one Chunk each hold
    /// their own mark: one ending in success, failure or cancellation never
    /// clears another's.
    #[tokio::test]
    async fn in_flight_marks_of_one_chunk_are_counted_per_reader() {
        let flights = HolderInFlight::new();
        let chunk = ChunkId::new("shared-by-two-versions");
        let other = flights.begin(&chunk);
        // Success: the mark is kept, then dropped.
        drop(flights.begin(&chunk));
        assert!(flights.contains(&chunk));
        // Failure: a fetch that errs drops its mark.
        let failed: Result<(), ()> = {
            let _mark = flights.begin(&chunk);
            Err(())
        };
        assert!(failed.is_err());
        assert!(flights.contains(&chunk));
        // Cancellation: a fetch dropped while it waits drops its mark.
        let fetch = {
            let flights = flights.clone();
            let chunk = chunk.clone();
            tokio::spawn(async move {
                let _mark = flights.begin(&chunk);
                std::future::pending::<()>().await;
            })
        };
        tokio::task::yield_now().await;
        fetch.abort();
        assert!(fetch.await.unwrap_err().is_cancelled());
        assert!(
            flights.contains(&chunk),
            "another reader's mark was cleared"
        );
        drop(other);
        assert!(!flights.contains(&chunk));
    }
}
