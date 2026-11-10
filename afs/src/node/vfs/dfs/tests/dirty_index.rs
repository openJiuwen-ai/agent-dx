use super::*;

// Frozen 693d144 Vec algorithm: independent of the indexed insert,
// lookup, append index and normalization used by the production candidate.
#[cfg(feature = "dfs")]
#[derive(Default)]
struct W1LegacyDirtyMap {
    extents: Vec<DirtyExtent>,
    builders: Vec<DirtyBuilder>,
    native_admitted_bytes: Option<u64>,
}

#[cfg(feature = "dfs")]
impl W1LegacyDirtyMap {
    fn write_native_at(
        &mut self,
        offset: u64,
        data: &[u8],
        write_seq: u64,
        pool: &Arc<WriteBufferPool>,
        logical_length: u64,
    ) -> Result<usize> {
        offset
            .checked_add(data.len() as u64)
            .ok_or_else(file_too_large)?;
        self.native_admitted_bytes.get_or_insert(0);
        let mut accepted = 0;
        while accepted < data.len() {
            let position = offset + accepted as u64;
            let latest = self.extents.iter().rev().find(|extent| {
                extent.file_offset <= position && position < extent.file_offset + extent.length
            });
            let existing = latest.and_then(|extent| {
                extent.builder.map(|index| {
                    let source = extent.source_offset + (position - extent.file_offset) as usize;
                    (
                        index,
                        source,
                        (extent.file_offset + extent.length - position) as usize,
                    )
                })
            });
            let append = if existing.is_none() && position >= logical_length {
                self.builders
                    .iter()
                    .enumerate()
                    .rev()
                    .find_map(|(index, entry)| {
                        let length = entry.builder.len();
                        (entry.file_offset + length as u64 == position
                            && length < entry.builder.writable_capacity())
                        .then_some((index, length, entry.builder.writable_capacity() - length))
                    })
            } else {
                None
            };
            let (index, source_offset, available) = if let Some(existing) = existing.or(append) {
                existing
            } else {
                let remaining = data.len() - accepted;
                let capacity = if position >= logical_length {
                    COMMIT_CHUNK_BYTES as usize
                } else {
                    remaining
                        .min(COMMIT_CHUNK_BYTES as usize)
                        .max(4096)
                        .next_power_of_two()
                };
                let builder = match ChunkBuilder::with_pool(pool.clone(), capacity) {
                    Ok(builder) => builder,
                    Err(_error) if accepted > 0 => return Ok(accepted),
                    Err(error) => return Err(error),
                };
                let index = self.builders.len();
                self.builders.push(DirtyBuilder {
                    file_offset: position,
                    builder,
                    descriptor: None,
                    creation_order: index as u64,
                });
                (index, 0, capacity)
            };
            let take = available.min(data.len() - accepted);
            self.builders[index]
                .builder
                .write_at(source_offset as u64, &data[accepted..accepted + take])?;
            self.extents.push(DirtyExtent {
                file_offset: position,
                length: take as u64,
                write_seq,
                data: None,
                builder: Some(index),
                source_offset,
                native_reuse_end: 0,
                native_upload: None,
                native_descriptor: None,
            });
            let admitted = self.native_admitted_bytes.get_or_insert(0);
            *admitted = admitted.saturating_add(take as u64);
            accepted += take;
        }
        Ok(accepted)
    }

    fn freeze_builders(&mut self) {
        let bodies = std::mem::take(&mut self.builders)
            .into_iter()
            .map(|entry| entry.builder.freeze())
            .collect::<Vec<_>>();
        for extent in &mut self.extents {
            if let Some(index) = extent.builder.take() {
                extent.data = Some(bodies[index].clone());
            }
        }
    }

    fn snapshot_range(&self, offset: u64, length: usize) -> Result<Self> {
        let end = offset
            .checked_add(length as u64)
            .ok_or_else(|| invalid("dirty snapshot range overflow"))?;
        let mut snapshot = Self::default();
        for extent in &self.extents {
            let start = offset.max(extent.file_offset);
            let stop = end.min(
                extent
                    .file_offset
                    .checked_add(extent.length)
                    .ok_or_else(|| invalid("dirty snapshot extent overflow"))?,
            );
            if start >= stop {
                continue;
            }
            let mut copied = extent.clone();
            copied.file_offset = start;
            copied.length = stop - start;
            let source = extent.source_offset
                + usize::try_from(start - extent.file_offset)
                    .map_err(|_| invalid("dirty snapshot source overflow"))?;
            if let Some(index) = extent.builder {
                let bytes = &self.builders[index].builder.as_bytes();
                // Holes installed by truncate can mask an obsolete tail. Do not
                // even read that tail into the snapshot before applying holes.
                let available = bytes
                    .len()
                    .saturating_sub(source)
                    .min(copied.length as usize);
                let mut body = vec![0; copied.length as usize];
                if available > 0 {
                    body[..available].copy_from_slice(&bytes[source..source + available]);
                }
                tracing::info_span!(
                    "node.dfs.write_buffer.read_snapshot_copy",
                    bytes = available as u64
                )
                .in_scope(|| {});
                copied.data = Some(SharedChunkBytes::from_vec(body));
                copied.source_offset = 0;
                copied.builder = None;
            } else {
                copied.source_offset = source;
            }
            snapshot.extents.push(copied);
        }
        Ok(snapshot)
    }

    fn resize(&mut self, old_length: u64, new_length: u64, write_seq: u64) -> Result<()> {
        if new_length < old_length {
            for entry in &mut self.builders {
                let keep = new_length
                    .saturating_sub(entry.file_offset)
                    .min(entry.builder.len() as u64) as usize;
                entry.builder.truncate(keep);
            }
            // A masked tail must not survive as an extent longer than the
            // now-shorter immutable body when this batch is frozen/retried.
            for extent in &mut self.extents {
                if extent.builder.is_some() {
                    extent.length = extent
                        .length
                        .min(new_length.saturating_sub(extent.file_offset));
                }
            }
            self.extents.retain(|extent| extent.length > 0);
        }
        let (start, stop) = if new_length < old_length {
            (new_length, old_length)
        } else {
            (old_length, new_length)
        };
        if start < stop {
            self.extents.push(DirtyExtent {
                file_offset: start,
                length: stop - start,
                write_seq,
                data: None,
                builder: None,
                source_offset: 0,
                native_reuse_end: 0,
                native_upload: None,
                native_descriptor: None,
            });
        }
        Ok(())
    }

    fn overlay(&self, offset: u64, out: &mut [u8]) -> Result<()> {
        let end = offset
            .checked_add(out.len() as u64)
            .ok_or_else(|| invalid("dirty read range overflow"))?;
        for extent in &self.extents {
            let extent_end = extent
                .file_offset
                .checked_add(extent.length)
                .ok_or_else(|| invalid("dirty extent range overflow"))?;
            let start = offset.max(extent.file_offset);
            let stop = end.min(extent_end);
            if start >= stop {
                continue;
            }
            let output_start = usize::try_from(start - offset)
                .map_err(|_| invalid("dirty output offset is too large"))?;
            let length = usize::try_from(stop - start)
                .map_err(|_| invalid("dirty overlay length is too large"))?;
            match &extent.data {
                Some(data) => {
                    let data_start = extent.source_offset
                        + usize::try_from(start - extent.file_offset)
                            .map_err(|_| invalid("dirty data offset is too large"))?;
                    out[output_start..output_start + length]
                        .copy_from_slice(&data[data_start..data_start + length]);
                }
                None => out[output_start..output_start + length].fill(0),
            }
        }
        Ok(())
    }

    fn restore_before(&mut self, mut older: W1LegacyDirtyMap) {
        older.extents.append(&mut self.extents);
        older.extents.sort_by_key(|extent| extent.write_seq);
        self.extents = older.extents;
        self.native_admitted_bytes = match (older.native_admitted_bytes, self.native_admitted_bytes)
        {
            (None, None) => None,
            (left, right) => Some(left.unwrap_or(0).saturating_add(right.unwrap_or(0))),
        };
    }

    fn dirty_data_bytes(&self) -> u64 {
        if let Some(accepted) = self.native_admitted_bytes {
            return accepted;
        }
        self.extents
            .iter()
            .filter(|extent| extent.data.is_some())
            .fold(0u64, |total, extent| total.saturating_add(extent.length))
    }
}

#[cfg(feature = "dfs")]
fn w1_legacy_normalized_overlay(
    map: &W1LegacyDirtyMap,
    file_length: u64,
) -> Result<Vec<OverlaySegment>> {
    let mut output: Vec<OverlaySegment> = Vec::new();
    for dirty in &map.extents {
        if dirty.file_offset >= file_length || dirty.length == 0 {
            continue;
        }
        let dirty_end = dirty
            .file_offset
            .checked_add(dirty.length)
            .ok_or_else(|| invalid("dirty extent range overflow"))?
            .min(file_length);
        let mut next = Vec::with_capacity(output.len().saturating_add(1));
        for segment in output {
            let segment_end = segment.file_offset + segment.length;
            if dirty_end <= segment.file_offset || dirty.file_offset >= segment_end {
                next.push(segment);
                continue;
            }
            if dirty.file_offset > segment.file_offset {
                next.push(OverlaySegment {
                    file_offset: segment.file_offset,
                    length: dirty.file_offset - segment.file_offset,
                    data: segment.data.clone(),
                });
            }
            if dirty_end < segment_end {
                let data = match segment.data {
                    Some((bytes, source_offset)) => {
                        let delta = usize::try_from(dirty_end - segment.file_offset)
                            .map_err(|_| invalid("overlay source offset is too large"))?;
                        let source_offset = source_offset
                            .checked_add(delta)
                            .ok_or_else(|| invalid("overlay source offset overflow"))?;
                        Some((bytes, source_offset))
                    }
                    None => None,
                };
                next.push(OverlaySegment {
                    file_offset: dirty_end,
                    length: segment_end - dirty_end,
                    data,
                });
            }
        }
        next.push(OverlaySegment {
            file_offset: dirty.file_offset,
            length: dirty_end - dirty.file_offset,
            data: dirty
                .data
                .as_ref()
                .map(|bytes| (bytes.clone(), dirty.source_offset)),
        });
        next.sort_by_key(|segment| segment.file_offset);
        output = next;
    }
    Ok(output)
}

#[cfg(feature = "dfs")]
fn w1_legacy_visible_seq(map: &W1LegacyDirtyMap, offset: u64) -> Option<u64> {
    map.extents
        .iter()
        .rev()
        .find(|extent| extent.file_offset <= offset && offset < extent.file_offset + extent.length)
        .map(|extent| extent.write_seq)
}

#[cfg(feature = "dfs")]
fn w1_indexed_visible_seq(map: &DirtyExtentMap, offset: u64) -> Option<u64> {
    map.extents
        .range(..=offset)
        .next_back()
        .and_then(|(_, extent)| {
            (offset < extent.file_offset + extent.length).then_some(extent.write_seq)
        })
}

#[cfg(feature = "dfs")]
fn w1_legacy_plan_map(map: &W1LegacyDirtyMap, logical_length: u64) -> DirtyExtentMap {
    let mut indexed = DirtyExtentMap {
        native_admitted_bytes: map.native_admitted_bytes,
        ..DirtyExtentMap::default()
    };
    for segment in w1_legacy_normalized_overlay(map, logical_length).unwrap() {
        let (data, source_offset) = match segment.data {
            Some((data, source_offset)) => (Some(data), source_offset),
            None => (None, 0),
        };
        indexed.extents.insert(
            segment.file_offset,
            DirtyExtent {
                file_offset: segment.file_offset,
                length: segment.length,
                write_seq: 0,
                data,
                builder: None,
                source_offset,
                native_reuse_end: 0,
                native_upload: None,
                native_descriptor: None,
            },
        );
    }
    indexed
}

#[cfg(feature = "dfs")]
#[test]
fn p2_w1_dirty_interval_index_matches_legacy_for_ten_thousand_sequences() {
    let (_temp, meta, fs) = test_fs();
    fs.create(
        &context(),
        fs.root_inode(),
        OsStr::new("w1-model.bin"),
        0o640,
        libc::O_RDWR,
    )
    .unwrap();
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    let cell = fs.write_state(&inode_id).unwrap().unwrap();
    let template = DistributedFs::freeze_state_for_commit(&mut cell.lock().unwrap());
    let mut seed = 0xe4d2_b96f_7350_8a61_u64;
    let mut random = || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    };
    for case in 0..10_000_u64 {
        let pool = WriteBufferPool::new(0);
        let legacy_pool = WriteBufferPool::new(0);
        let mut indexed = DirtyExtentMap::default();
        let mut legacy = W1LegacyDirtyMap::default();
        let mut length = 0_u64;
        let mut held = None;
        let mut held_legacy = None;
        let mut pinned: Option<(DirtyExtentMap, W1LegacyDirtyMap, u64, Vec<u8>)> = None;
        let operations = 8 + random() as usize % 17;
        for operation in 0..operations {
            let draw = random();
            let seq = operation as u64 + 1;
            match draw % 10 {
                4 if case % 11 == 0 => {
                    // This offset cannot hit or append any current small
                    // builder. Both implementations must reject the same
                    // allocation without admitting or exposing a tail.
                    pool.fail_next_allocation();
                    legacy_pool.fail_next_allocation();
                    let actual = indexed
                        .write_native_at(1 << 20, &[5; 32], seq, &pool, length)
                        .unwrap_err();
                    let expected = legacy
                        .write_native_at(1 << 20, &[5; 32], seq, &legacy_pool, length)
                        .unwrap_err();
                    assert_eq!(
                        actual.code(),
                        expected.code(),
                        "allocation failure case={case} operation={operation}"
                    );
                }
                5 if case % 13 == 0 => {
                    let actual = indexed
                        .write_native_at(u64::MAX, &[5], seq, &pool, length)
                        .unwrap_err();
                    let expected = legacy
                        .write_native_at(u64::MAX, &[5], seq, &legacy_pool, length)
                        .unwrap_err();
                    assert_eq!(
                        actual.code(),
                        expected.code(),
                        "overflow case={case} operation={operation}"
                    );
                }
                0 | 1 => {
                    let new_length = random() % 16_385;
                    indexed.resize(length, new_length, seq).unwrap();
                    legacy.resize(length, new_length, seq).unwrap();
                    length = new_length;
                }
                2 if held.is_none() => {
                    let mut frozen = std::mem::take(&mut indexed);
                    let mut old_frozen = std::mem::take(&mut legacy);
                    frozen.freeze_builders();
                    old_frozen.freeze_builders();
                    held = Some(frozen);
                    held_legacy = Some(old_frozen);
                }
                3 if held.is_some() => {
                    indexed.restore_before(held.take().unwrap());
                    legacy.restore_before(held_legacy.take().unwrap());
                }
                _ => {
                    let offset = if draw & 16 == 0 {
                        length
                    } else {
                        random() % 16_385
                    };
                    let size = 1 + random() as usize % 2048;
                    let payload = (0..size)
                        .map(|index| (draw.wrapping_add(index as u64 * 37) >> 8) as u8)
                        .collect::<Vec<_>>();
                    let accepted = indexed
                        .write_native_at(offset, &payload, seq, &pool, length)
                        .unwrap();
                    let expected = legacy
                        .write_native_at(offset, &payload, seq, &legacy_pool, length)
                        .unwrap();
                    assert_eq!(accepted, expected, "case={case} operation={operation}");
                    length = length.max(offset + accepted as u64);
                }
            }
            assert_eq!(
                indexed.dirty_data_bytes(),
                legacy.dirty_data_bytes(),
                "admission case={case} operation={operation}"
            );
            let actual_stats = pool.snapshot();
            let expected_stats = legacy_pool.snapshot();
            assert_eq!(
                actual_stats.copied_bytes, expected_stats.copied_bytes,
                "input copy bytes case={case} operation={operation}"
            );
            assert_eq!(
                actual_stats.copy_operations, expected_stats.copy_operations,
                "input copy calls case={case} operation={operation}"
            );
            assert_eq!(
                indexed.builders.len(),
                legacy.builders.len(),
                "owner count case={case} operation={operation}"
            );
            for (actual, expected) in indexed.builders.iter().zip(&legacy.builders) {
                assert_eq!(actual.file_offset, expected.file_offset);
                assert_eq!(actual.builder.capacity(), expected.builder.capacity());
                assert!(
                    actual.builder.as_bytes() == expected.builder.as_bytes(),
                    "owner body case={case} operation={operation}"
                );
            }
            if let Some((snapshot, old_snapshot, offset, bytes)) = &pinned {
                let mut actual = vec![91; bytes.len()];
                let mut expected = vec![91; bytes.len()];
                snapshot.overlay(*offset, &mut actual).unwrap();
                old_snapshot.overlay(*offset, &mut expected).unwrap();
                assert!(
                    actual == *bytes && expected == *bytes,
                    "pinned snapshot case={case} operation={operation}"
                );
            }
            let offset = random() % length.max(1);
            let size = 1024.min(length.saturating_sub(offset) as usize);
            let mut actual = vec![91; size];
            let mut expected = vec![91; size];
            if let Some(frozen) = &held {
                frozen.overlay(offset, &mut actual).unwrap();
                held_legacy
                    .as_ref()
                    .unwrap()
                    .overlay(offset, &mut expected)
                    .unwrap();
            }
            indexed
                .snapshot_range(offset, size)
                .unwrap()
                .overlay(offset, &mut actual)
                .unwrap();
            legacy
                .snapshot_range(offset, size)
                .unwrap()
                .overlay(offset, &mut expected)
                .unwrap();
            assert!(
                actual == expected,
                "visible bytes case={case} operation={operation} offset={offset}"
            );
            if operation % 8 == 0 {
                let actual_snapshot = indexed.snapshot_range(offset, size).unwrap();
                let expected_snapshot = legacy.snapshot_range(offset, size).unwrap();
                let mut bytes = vec![91; size];
                actual_snapshot.overlay(offset, &mut bytes).unwrap();
                pinned = Some((actual_snapshot, expected_snapshot, offset, bytes));
            }
            // Sequence is independent of body identity and cannot be erased
            // just because two neighboring ranges use the same allocation.
            for probe in [0, offset, length.saturating_sub(1)] {
                assert_eq!(
                    w1_indexed_visible_seq(&indexed, probe),
                    w1_legacy_visible_seq(&legacy, probe),
                    "seq case={case} operation={operation} offset={probe}"
                );
            }
            let mut last_end = 0;
            for (start, extent) in &indexed.extents {
                assert!(*start >= last_end);
                assert_eq!(*start, extent.file_offset);
                last_end = *start + extent.length;
            }
        }
        if let Some(frozen) = held.take() {
            indexed.restore_before(frozen);
            legacy.restore_before(held_legacy.take().unwrap());
        }
        let identities = indexed
            .builders
            .iter()
            .map(|entry| {
                (
                    entry.builder.allocation_id(),
                    (entry.builder.data_ptr() as usize, entry.builder.capacity()),
                )
            })
            .collect::<HashMap<_, _>>();
        let before = pool.snapshot();
        indexed.freeze_builders();
        legacy.freeze_builders();
        for extent in indexed.iter_extents() {
            if let Some(data) = &extent.data
                && let Some(identity) = identities.get(&data.allocation_id())
            {
                assert_eq!(
                    *identity,
                    (data.as_ptr() as usize, data.capacity()),
                    "freeze owner case={case}"
                );
            }
        }
        let mut actual_frozen = template.clone();
        actual_frozen.logical_length = length;
        actual_frozen.dirty_extents = indexed;
        let mut expected_frozen = template.clone();
        expected_frozen.logical_length = length;
        expected_frozen.dirty_extents = w1_legacy_plan_map(&legacy, length);
        let operation_id = OperationId::new("w1-differential");
        let layout_id = LayoutRootId::new("w1-differential-layout");
        let actual = CommitPlanner
            .plan(&actual_frozen, operation_id.clone(), layout_id.clone())
            .unwrap();
        let expected = CommitPlanner
            .plan(&expected_frozen, operation_id, layout_id)
            .unwrap();
        assert_eq!(
            actual.layout_root, expected.layout_root,
            "layout case={case}"
        );
        assert_eq!(
            actual.staged_chunks.len(),
            expected.staged_chunks.len(),
            "objects case={case}"
        );
        for (actual, expected) in actual.staged_chunks.iter().zip(&expected.staged_chunks) {
            assert_eq!(actual.chunk, expected.chunk, "standard object case={case}");
            assert!(
                actual.shared_bytes().as_ref() == expected.shared_bytes().as_ref(),
                "submitted body case={case}"
            );
        }
        assert_eq!(
            pool.snapshot().copied_bytes,
            before.copied_bytes,
            "freeze/planner copy bytes case={case}"
        );
        assert_eq!(
            pool.snapshot().copy_operations,
            before.copy_operations,
            "freeze/planner copy calls case={case}"
        );
    }
}

#[test]
fn p2_w1_append_index_keeps_creation_order_after_shrink_regrow() {
    let pool = WriteBufferPool::new(0);
    let mut map = DirtyExtentMap::default();
    map.write_native_at(0, &[1; 4096], 1, &pool, 0).unwrap();
    map.resize(4096, 0, 2).unwrap();
    map.resize(0, 4096, 3).unwrap();
    // The hole masks the old builder, so this interior patch gets a new,
    // smaller owner at the same file start.
    map.write_native_at(0, &[2; 4096], 4, &pool, 4096).unwrap();
    assert_eq!(map.builders.len(), 2);
    assert_eq!(map.builders[1].builder.capacity(), 4096);
    map.resize(4096, 0, 5).unwrap();
    map.write_native_at(0, &[3; 16], 6, &pool, 0).unwrap();
    assert_eq!(map.builders.len(), 2);
    assert_eq!(map.builders[0].builder.len(), 0);
    assert_eq!(map.builders[1].builder.as_bytes(), &[3; 16]);
    assert_eq!(pool.snapshot().copy_operations, 3);
}

#[test]
fn p2_w1_shadowed_frozen_owner_releases_only_after_last_snapshot() {
    let pool = WriteBufferPool::new(0);
    let mut older = DirtyExtentMap::default();
    older
        .write_native_at(0, &[1; 4096], 1, &pool, 4096)
        .unwrap();
    older.freeze_builders();
    let pinned = older.clone();
    let owner = older
        .extents
        .first_key_value()
        .unwrap()
        .1
        .data
        .as_ref()
        .unwrap()
        .clone();
    let mut active = DirtyExtentMap::default();
    active
        .write_native_at(0, &[2; 4096], 2, &pool, 4096)
        .unwrap();
    active.restore_before(older);
    assert_eq!(active.extents.len(), 1);
    assert_eq!(pool.snapshot().live_buffers, 2);
    drop(owner);
    assert_eq!(pool.snapshot().live_buffers, 2);
    let mut out = [0; 4096];
    pinned.overlay(0, &mut out).unwrap();
    assert_eq!(out, [1; 4096]);
    drop(pinned);
    assert_eq!(pool.snapshot().live_buffers, 1);
    drop(active);
    assert_eq!(pool.snapshot().live_buffers, 0);
}

#[test]
fn p2_w1_native_promotion_restore_holes_and_equal_sequence_are_preserved() {
    let pool = WriteBufferPool::new(0);
    let mut map = DirtyExtentMap::default();
    map.resize(0, 16_384, 1).unwrap();
    map.write_native_at(8192, &[3; 4096], 2, &pool, 16_384)
        .unwrap();
    let mut older = std::mem::take(&mut map);
    older.freeze_builders();
    map.write_native_at(8192, &[7; 4096], 2, &pool, 16_384)
        .unwrap();
    map.restore_before(older);
    let snapshot = map.snapshot_range(0, 16_384).unwrap();
    let mut out = vec![91; 16_384];
    snapshot.overlay(0, &mut out).unwrap();
    assert!(out[..8192].iter().all(|byte| *byte == 0));
    assert!(out[8192..12_288].iter().all(|byte| *byte == 7));
    assert!(out[12_288..].iter().all(|byte| *byte == 0));
    assert_eq!(map.native_admitted_bytes, Some(8192));
}

#[cfg(feature = "dfs")]
#[test]
fn p2_w1_aligned_overwrite_metadata_stays_bounded() {
    const OBJECT_BYTES: usize = 4 * 1024 * 1024;
    const FILE_BYTES: usize = 4 * OBJECT_BYTES;
    const BLOCK_BYTES: usize = 4096;
    const BLOCKS: usize = FILE_BYTES / BLOCK_BYTES;
    const WRITES: usize = 100_000;
    let pool = WriteBufferPool::new(0);
    let mut dirty = DirtyExtentMap::default();
    let mut expected = vec![0_u8; BLOCKS];
    for index in 0..4 {
        let marker = index as u8 + 1;
        let payload = vec![marker; OBJECT_BYTES];
        dirty
            .write_native_at(
                (index * OBJECT_BYTES) as u64,
                &payload,
                index as u64 + 1,
                &pool,
                (index * OBJECT_BYTES) as u64,
            )
            .unwrap();
        expected[index * 1024..(index + 1) * 1024].fill(marker);
    }
    let identities = dirty
        .builders
        .iter()
        .map(|entry| {
            (
                entry.builder.allocation_id(),
                (entry.builder.data_ptr() as usize, entry.builder.capacity()),
            )
        })
        .collect::<HashMap<_, _>>();
    let mut seed = 0x39e7_3b5d_2c61_8a47_u64;
    let mut patch = [0_u8; BLOCK_BYTES];
    for iteration in 0..WRITES {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        let block = (seed as usize) % BLOCKS;
        let marker = (seed >> 32) as u8;
        patch.fill(marker);
        assert_eq!(
            dirty
                .write_native_at(
                    (block * BLOCK_BYTES) as u64,
                    &patch,
                    iteration as u64 + 5,
                    &pool,
                    FILE_BYTES as u64,
                )
                .unwrap(),
            BLOCK_BYTES
        );
        expected[block] = marker;
        if (iteration + 1) % 1024 == 0 || iteration + 1 == WRITES {
            // This is live metadata, not normalized temporary output. The
            // old Vec log fails here; the indexed map must finish all writes.
            assert!(
                dirty.extents.len() <= BLOCKS,
                "writes={} live_extents={} maximum={BLOCKS}",
                iteration + 1,
                dirty.extents.len(),
            );
            assert_eq!(dirty.builders.len(), 4);
            let stats = pool.snapshot();
            assert_eq!(stats.allocations, 4);
            assert_eq!(stats.live_buffers, 4);
            assert_eq!(
                stats.copied_bytes,
                (FILE_BYTES + (iteration + 1) * BLOCK_BYTES) as u64,
            );
            assert_eq!(stats.copy_operations, iteration as u64 + 5);
        }
    }
    assert_eq!(
        dirty.dirty_data_bytes(),
        (FILE_BYTES + WRITES * BLOCK_BYTES) as u64,
        "accepted overwrite bytes still count toward the original trigger",
    );
    let before_freeze = pool.snapshot();
    let (_temp, meta, fs) = test_fs();
    fs.create(
        &context(),
        fs.root_inode(),
        OsStr::new("w1-bounded-map.bin"),
        0o640,
        libc::O_RDWR,
    )
    .unwrap();
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    let cell = fs.write_state(&inode_id).unwrap().unwrap();
    let frozen = {
        let mut state = cell.lock().unwrap();
        state.dirty_extents = dirty;
        state.logical_length = FILE_BYTES as u64;
        state.next_write_seq = WRITES as u64 + 4;
        state.visible_write_seq = state.next_write_seq;
        DistributedFs::freeze_state_for_commit(&mut state)
    };
    let plan = CommitPlanner
        .plan(
            &frozen,
            OperationId::new("w1-bounded-map"),
            LayoutRootId::new("w1-bounded-map-layout"),
        )
        .unwrap();
    assert_eq!(plan.staged_chunks.len(), 4, "large objects remain large");
    assert_eq!(plan.layout_root.inline_extents.len(), 4);
    for (index, extent) in plan.layout_root.inline_extents.iter().enumerate() {
        assert_eq!(extent.file_offset, (index * OBJECT_BYTES) as u64);
        assert_eq!(extent.length, OBJECT_BYTES as u64);
        assert_eq!(extent.chunk_offset, 0);
        let staged = plan
            .staged_chunks
            .iter()
            .find(|chunk| chunk.chunk.id == extent.chunk_id)
            .unwrap();
        let body = staged.shared_bytes();
        assert_eq!(body.len(), OBJECT_BYTES);
        assert_eq!(
            identities.get(&body.allocation_id()),
            Some(&(body.as_ptr() as usize, body.capacity())),
            "freeze and planning keep the original body allocation",
        );
        for (block, bytes) in body.chunks_exact(BLOCK_BYTES).enumerate() {
            let marker = expected[index * 1024 + block];
            assert!(bytes.iter().all(|byte| *byte == marker));
        }
    }
    assert_eq!(pool.snapshot().copied_bytes, before_freeze.copied_bytes);
    assert_eq!(
        pool.snapshot().copy_operations,
        before_freeze.copy_operations
    );
}

#[test]
fn p2_w1_overwrite_preserves_original_builder_span_and_copy_calls() {
    let pool = WriteBufferPool::new(0);
    let mut dirty = DirtyExtentMap::default();
    let original = vec![3; 4 * 1024 * 1024];
    dirty.write_native_at(0, &original, 1, &pool, 0).unwrap();
    let pointer = dirty.builders[0].builder.data_ptr();
    dirty
        .write_native_at(8192, &[7; 4096], 2, &pool, original.len() as u64)
        .unwrap();
    let before = pool.snapshot();
    dirty
        .write_native_at(0, &original, 3, &pool, original.len() as u64)
        .unwrap();
    let after = pool.snapshot();
    assert_eq!(
        after.copied_bytes - before.copied_bytes,
        original.len() as u64
    );
    assert_eq!(after.copy_operations - before.copy_operations, 1);
    assert_eq!(dirty.builders.len(), 1);
    assert_eq!(dirty.builders[0].builder.data_ptr(), pointer);
    assert_eq!(dirty.builders[0].builder.as_bytes(), original.as_slice());
}
