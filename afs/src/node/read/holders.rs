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
            intervals.insert(id.clone(), ranges);
        }
        Some(Self {
            layout_root_id: layout.id.clone(),
            intervals,
        })
    }

    /// Bytes one index entry costs besides its Chunk id and intervals: the
    /// entry itself and the map's control byte, doubled for spare buckets.
    const ENTRY_BYTES: usize = 2 * (std::mem::size_of::<(ChunkId, Vec<(u64, u64)>)>() + 1);

    /// An upper bound of [`Self::retained_bytes`] for `layout`, known before
    /// building: at most one entry and one interval per extent.
    fn estimate(layout: &LayoutRoot) -> u64 {
        let interval = std::mem::size_of::<(u64, u64)>();
        let entries: usize = layout
            .inline_extents
            .iter()
            .map(|extent| Self::ENTRY_BYTES + extent.chunk_id.0.len() + interval)
            .sum();
        (std::mem::size_of::<Self>() + layout.id.0.len() + entries) as u64
    }

    /// The heap and inline bytes this index keeps (capacities, not lengths).
    fn retained_bytes(&self) -> u64 {
        let interval = std::mem::size_of::<(u64, u64)>();
        let entries: usize = self
            .intervals
            .iter()
            .map(|(id, ranges)| id.0.capacity() + ranges.capacity() * interval)
            .sum();
        (std::mem::size_of::<Self>()
            + self.layout_root_id.0.capacity()
            + self.intervals.capacity() * Self::ENTRY_BYTES / 2
            + entries) as u64
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
    /// The build in progress, if any (one per version at a time).
    building: Option<u64>,
    /// Budget bytes this version holds: its build's estimate while it runs,
    /// then its index's retained bytes. Returned when the version leaves.
    charged: u64,
}

#[derive(Debug, Default)]
struct State {
    versions: HashMap<FileVersionId, Shared>,
    /// Sum of every version's `charged`.
    used: u64,
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
    /// See [`HOLDER_INDEX_BUDGET`].
    budget: u64,
    /// Notified whenever a version stops being shared (!318 P3-3).
    unshared: tokio::sync::Notify,
    /// Tests: layout indexes built.
    #[cfg(test)]
    builds: std::sync::atomic::AtomicUsize,
}

impl Default for HolderRegistry {
    fn default() -> Self {
        Self {
            inner: Mutex::default(),
            budget: HOLDER_INDEX_BUDGET,
            unshared: tokio::sync::Notify::new(),
            #[cfg(test)]
            builds: std::sync::atomic::AtomicUsize::new(0),
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

    /// Budget bytes in use: indexes kept and builds in progress (tests).
    #[cfg(test)]
    pub(crate) fn index_bytes(&self) -> u64 {
        self.lock().used
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        // Every method leaves the state consistent before it can panic.
        self.inner.lock().unwrap_or_else(|error| error.into_inner())
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
    /// version leaves with its last reader, and its budget bytes with it
    /// (also those of a build still running: that build publishes nothing).
    pub(crate) fn release(&self, version: &FileVersionId, reader: u64) {
        let mut inner = self.lock();
        if let Some(shared) = inner.versions.get_mut(version) {
            shared.readers.remove(&reader);
            if shared.readers.is_empty() {
                if let Some(shared) = inner.versions.remove(version) {
                    inner.used -= shared.charged;
                }
                drop(inner);
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
    /// The index is built outside the registry lock (!318 P2-1), so other
    /// versions are checked, joined and released meanwhile, and at most once
    /// at a time per version (a concurrent first attach leaves it to the
    /// build in progress). The build first reserves an upper bound of its
    /// bytes from the Node-wide budget ([`HOLDER_INDEX_BUDGET`]); a version
    /// that does not fit stays unindexed (not ready: readers use the copies)
    /// and is tried again at its next attach. The index is published only if
    /// the version is still shared and this is still its build; a version
    /// whose last reader left during the build publishes nothing and has
    /// already given its bytes back.
    pub(crate) fn attach(&self, version: &FileVersionId, layout: &LayoutRoot) {
        let estimate = HolderLayout::estimate(layout);
        let build = {
            let mut inner = self.lock();
            let State {
                versions,
                used,
                builds,
            } = &mut *inner;
            let Some(shared) = versions.get_mut(version) else {
                return;
            };
            if shared.layout.is_some() || shared.incomplete || shared.building.is_some() {
                return;
            }
            if used.saturating_add(estimate) > self.budget {
                return;
            }
            *builds += 1;
            *used += estimate;
            shared.charged = estimate;
            shared.building = Some(*builds);
            *builds
        };
        #[cfg(test)]
        self.builds
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let built = HolderLayout::build(layout);
        let mut inner = self.lock();
        let State { versions, used, .. } = &mut *inner;
        let Some(shared) = versions.get_mut(version) else {
            return;
        };
        if shared.building != Some(build) {
            return;
        }
        shared.building = None;
        *used -= shared.charged;
        shared.charged = 0;
        match built {
            Some(index) => {
                // Never more than reserved: the estimate is an upper bound.
                let kept = index.retained_bytes().min(estimate);
                *used += kept;
                shared.charged = kept;
                shared.layout = Some(Arc::new(index));
            }
            None => shared.incomplete = true,
        }
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
