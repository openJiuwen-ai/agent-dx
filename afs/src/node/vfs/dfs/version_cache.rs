use super::*;

/// Immutable metadata only. An Arc never grants authority or extends the
/// separately captured inode/session view deadline.
pub(super) struct ReadOnlyVersion {
    pub(super) version: Option<FileVersion>,
    pub(super) layout: LayoutRoot,
    pub(super) index: LayoutReadIndex,
}

pub(super) struct CachedReadOnlyVersion {
    value: Arc<ReadOnlyVersion>,
    retained_bytes: usize,
}

/// Bounds this cache's retained metadata estimate, not total Node RSS. Evicted
/// Arc values stay live until their active readers release them. Cache-owned
/// containers are charged conservatively at their configured maximum, so a
/// HashMap's retained capacity after eviction cannot escape the byte budget.
pub(super) struct ReadOnlyVersionCache {
    entries: BoundedLru<FileVersionId, CachedReadOnlyVersion>,
    pub(super) capacity: usize,
    pub(super) byte_limit: usize,
    pub(super) table_bytes: usize,
    pub(super) value_bytes: usize,
    entry_count: usize,
}

pub(super) fn version_cache_table_bytes(capacity: usize) -> Option<usize> {
    // Preserve the runtime estimate: permit hash-table growth/control storage
    // and sparsely occupied BTree nodes; key buffers are charged per value.
    let table_slot = std::mem::size_of::<(FileVersionId, CachedReadOnlyVersion, u64)>()
        .checked_add(std::mem::size_of::<(u64, FileVersionId)>())?
        .checked_mul(4)?
        .checked_add(64)?;
    capacity.checked_mul(table_slot)?.checked_add(2048)
}

/// Startup floor for the table and one value's fixed fields/Arc header. This
/// makes no assumption about identifier lengths or layout cardinality: actual
/// String/Vec capacities are charged on admission and can still exceed it.
pub(crate) fn version_cache_minimum_bytes(capacity: usize) -> Option<usize> {
    version_cache_table_bytes(capacity)?.checked_add(ReadOnlyVersion::fixed_retained_bytes())
}

impl ReadOnlyVersionCache {
    pub(super) fn new(capacity: usize, byte_limit: usize) -> Self {
        let table_bytes = version_cache_table_bytes(capacity).unwrap_or(usize::MAX);
        Self {
            entries: BoundedLru::new(capacity),
            capacity,
            byte_limit,
            table_bytes,
            value_bytes: 0,
            entry_count: 0,
        }
    }

    pub(super) fn get(&mut self, key: &FileVersionId) -> Option<&Arc<ReadOnlyVersion>> {
        self.entries.get(key).map(|entry| &entry.value)
    }

    pub(super) fn insert(&mut self, key: FileVersionId, value: Arc<ReadOnlyVersion>) {
        if let Some(previous) = self.entries.remove(&key) {
            self.value_bytes = self.value_bytes.saturating_sub(previous.retained_bytes);
            self.entry_count -= 1;
        }
        let retained_bytes = value.cache_retained_bytes(&key);
        if self.capacity == 0 || self.table_bytes.saturating_add(retained_bytes) > self.byte_limit {
            if self.entry_count == 0 {
                // A rejected replacement must also release any old empty table
                // allocation; zero retained entries otherwise have zero charge.
                self.entries = BoundedLru::new(self.capacity);
            }
            return;
        }
        while self.entry_count >= self.capacity
            || self
                .table_bytes
                .saturating_add(self.value_bytes)
                .saturating_add(retained_bytes)
                > self.byte_limit
        {
            let Some((_, evicted)) = self.entries.pop_oldest() else {
                return;
            };
            self.value_bytes = self.value_bytes.saturating_sub(evicted.retained_bytes);
            self.entry_count -= 1;
        }
        self.entries.insert(
            key,
            CachedReadOnlyVersion {
                value,
                retained_bytes,
            },
        );
        self.value_bytes = self.value_bytes.saturating_add(retained_bytes);
        self.entry_count += 1;
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.entry_count
    }
}

impl ReadOnlyVersion {
    pub(super) fn fixed_retained_bytes() -> usize {
        std::mem::size_of::<Self>().saturating_add(2 * std::mem::size_of::<usize>())
    }

    pub(super) fn cache_retained_bytes(&self, key: &FileVersionId) -> usize {
        // The hash table and recency tree each own a key. Counting the source
        // capacity twice conservatively covers the cloned key's allocation.
        // Heap allocations also include a two-word bookkeeping allowance;
        // this remains a portable retained-size estimate, not allocator RSS.
        let heap = |capacity: usize| {
            if capacity == 0 {
                0
            } else {
                capacity.saturating_add(2 * std::mem::size_of::<usize>())
            }
        };
        let mut bytes = Self::fixed_retained_bytes()
            .saturating_add(heap(key.0.capacity()).saturating_mul(2))
            .saturating_add(heap(self.layout.id.0.capacity()))
            .saturating_add(heap(
                self.layout
                    .inline_extents
                    .capacity()
                    .saturating_mul(std::mem::size_of::<Extent>()),
            ))
            .saturating_add(heap(
                self.index
                    .entries
                    .capacity()
                    .saturating_mul(std::mem::size_of::<IndexedExtent>()),
            ));
        if let Some(version) = &self.version {
            for capacity in [
                version.id.0.capacity(),
                version.inode_id.0.capacity(),
                version
                    .parent_version
                    .as_ref()
                    .map_or(0, |id| id.0.capacity()),
                version.layout_root.0.capacity(),
            ] {
                bytes = bytes.saturating_add(heap(capacity));
            }
        }
        for extent in &self.layout.inline_extents {
            bytes = bytes.saturating_add(heap(extent.chunk_id.0.capacity()));
        }
        bytes
    }

    pub(super) fn empty() -> Self {
        Self {
            version: None,
            layout: LayoutRoot {
                id: LayoutRootId::new("empty"),
                file_length: 0,
                inline_extents: Vec::new(),
            },
            index: LayoutReadIndex {
                entries: Vec::new(),
                serialization_ordered: true,
            },
        }
    }

    pub(super) fn checked(
        expected: &FileVersionId,
        version: FileVersion,
        layout: LayoutRoot,
    ) -> Result<Self> {
        if version.id != *expected
            || version.layout_root != layout.id
            || version.length != layout.file_length
        {
            return Err(invalid(
                "DFS readonly version/layout identity or length differs",
            ));
        }
        let index = LayoutReadIndex::new(&layout)?;
        Ok(Self {
            version: Some(version),
            layout,
            index,
        })
    }

    pub(super) fn ops(&self, offset: u64, length: usize) -> Result<Vec<ChunkReadOp>> {
        self.index.ops(&self.layout, offset, length)
    }

    pub(super) fn native_read_batch(
        &self,
        offset: u64,
        length: usize,
        lookahead: usize,
    ) -> Result<ReadBatch> {
        self.read_batch_with_source_window(offset, length, lookahead, true)
    }

    pub(super) fn read_batch(
        &self,
        offset: u64,
        length: usize,
        lookahead: usize,
    ) -> Result<ReadBatch> {
        self.read_batch_with_source_window(offset, length, lookahead, false)
    }

    fn read_batch_with_source_window(
        &self,
        offset: u64,
        length: usize,
        lookahead: usize,
        native: bool,
    ) -> Result<ReadBatch> {
        let ops = self.ops(offset, length)?;
        let end = offset
            .checked_add(length as u64)
            .ok_or_else(|| invalid("read range overflow"))?;
        let source_window = if native && ops.len() == 1 && lookahead > 0 {
            // Use the existing advisory quota in a stable immutable-layout
            // window. A later concurrent offset must not omit earlier peers
            // and force an extra lookup. This never changes body operations.
            let width = lookahead
                .saturating_add(1)
                .min(crate::node::dfs_read::MAX_DFS_READ_OPS);
            let first = self
                .index
                .entries
                .partition_point(|entry| entry.max_end <= offset);
            let start = (first / width) * width;
            start..start.saturating_add(width).min(self.index.entries.len())
        } else {
            let next = self.index.entries.partition_point(|entry| {
                self.layout.inline_extents[entry.original].file_offset < end
            });
            next..self.index.entries.len()
        };
        Ok(ReadBatch {
            lookahead: source_lookahead_ids(
                self.index.entries[source_window]
                    .iter()
                    .map(|entry| &self.layout.inline_extents[entry.original]),
                &ops,
                lookahead,
            ),
            file_version_id: self.version.as_ref().map(|version| version.id.clone()),
            layout_root_id: self.layout.id.clone(),
            ops,
        })
    }
}

/// Metadata only. These IDs never enter body operations or expand ranges.
pub(super) fn source_lookahead_ids<'a>(
    extents: impl Iterator<Item = &'a Extent>,
    ops: &[ChunkReadOp],
    limit: usize,
) -> Vec<crate::dfs::ChunkId> {
    let mut ids = Vec::new();
    let limit = limit.min(crate::node::dfs_read::MAX_DFS_READ_OPS);
    if limit == 0 || ops.is_empty() {
        return ids;
    }
    for extent in extents {
        if extent.length == 0
            || extent.chunk_id.0.is_empty()
            || extent.chunk_id.0.len() > crate::node::dfs_read::MAX_SOURCE_LOOKAHEAD_ID_BYTES
            || ops.iter().any(|op| op.chunk_id == extent.chunk_id)
            || ids.contains(&extent.chunk_id)
        {
            continue;
        }
        ids.push(extent.chunk_id.clone());
        if ids.len() == limit {
            break;
        }
    }
    ids
}

pub(super) struct IndexedExtent {
    original: usize,
    max_end: u64,
}

/// The prefix maximum handles overlapping/unsorted legacy layouts without
/// dropping an extent that starts before the request. Read operations retain
/// original serialization order, including its overlap precedence.
pub(super) struct LayoutReadIndex {
    pub(super) entries: Vec<IndexedExtent>,
    serialization_ordered: bool,
}

impl LayoutReadIndex {
    pub(super) fn new(layout: &LayoutRoot) -> Result<Self> {
        let mut entries = layout
            .inline_extents
            .iter()
            .enumerate()
            .map(|(original, extent)| {
                let max_end = extent
                    .file_offset
                    .checked_add(extent.length)
                    .ok_or_else(|| invalid("extent range overflow"))?;
                Ok(IndexedExtent { original, max_end })
            })
            .collect::<Result<Vec<_>>>()?;
        entries.sort_by_key(|entry| {
            (
                layout.inline_extents[entry.original].file_offset,
                entry.original,
            )
        });
        let mut max_end = 0;
        for entry in &mut entries {
            max_end = max_end.max(entry.max_end);
            entry.max_end = max_end;
        }
        let serialization_ordered = entries
            .windows(2)
            .all(|pair| pair[0].original < pair[1].original);
        Ok(Self {
            entries,
            serialization_ordered,
        })
    }

    pub(super) fn ops(
        &self,
        layout: &LayoutRoot,
        offset: u64,
        length: usize,
    ) -> Result<Vec<ChunkReadOp>> {
        let end = offset
            .checked_add(length as u64)
            .ok_or_else(|| invalid("read range overflow"))?;
        let first = self
            .entries
            .partition_point(|entry| entry.max_end <= offset);
        let last = self
            .entries
            .partition_point(|entry| layout.inline_extents[entry.original].file_offset < end);
        if first >= last {
            return Ok(Vec::new());
        }
        let mut ops = Vec::new();
        let mut append = |original: usize| -> Result<()> {
            if let Some(op) = extent_read_op(&layout.inline_extents[original], offset, end)? {
                ops.push(op);
            }
            Ok(())
        };
        if self.serialization_ordered {
            // Canonical layouts need neither a candidate allocation nor a
            // per-read sort. Legacy unordered layouts retain exact precedence.
            for entry in &self.entries[first..last] {
                append(entry.original)?;
            }
        } else {
            let mut candidates = self.entries[first..last]
                .iter()
                .map(|entry| entry.original)
                .collect::<Vec<_>>();
            candidates.sort_unstable();
            for original in candidates {
                append(original)?;
            }
        }
        Ok(ops)
    }
}

pub(super) fn extent_read_op(
    extent: &Extent,
    offset: u64,
    end: u64,
) -> Result<Option<ChunkReadOp>> {
    let extent_end = extent
        .file_offset
        .checked_add(extent.length)
        .ok_or_else(|| invalid("extent range overflow"))?;
    let start = offset.max(extent.file_offset);
    let stop = end.min(extent_end);
    if start >= stop {
        return Ok(None);
    }
    let output_offset =
        usize::try_from(start - offset).map_err(|_| invalid("read output offset is too large"))?;
    let length = usize::try_from(stop - start).map_err(|_| invalid("read length is too large"))?;
    let chunk_offset = extent
        .chunk_offset
        .checked_add(start - extent.file_offset)
        .ok_or_else(|| invalid("chunk read offset overflow"))?;
    Ok(Some(ChunkReadOp {
        chunk_id: extent.chunk_id.clone(),
        chunk_offset,
        length: length as u64,
        output_offset,
    }))
}

impl DistributedFs {
    pub(super) fn load_version(
        &self,
        version_id: Option<&FileVersionId>,
    ) -> Result<(Option<FileVersion>, LayoutRoot)> {
        let Some(version_id) = version_id else {
            return Ok((
                None,
                LayoutRoot {
                    id: LayoutRootId::new("empty"),
                    file_length: 0,
                    inline_extents: Vec::new(),
                },
            ));
        };
        self.meta
            .get_file_version(version_id)
            .map(|(version, layout)| (Some(version), layout))
    }

    pub(super) fn load_readonly_version(
        &self,
        version_id: Option<&FileVersionId>,
    ) -> Result<Arc<ReadOnlyVersion>> {
        let Some(version_id) = version_id else {
            return Ok(self.readonly_empty_version.clone());
        };
        let span = tracing::info_span!("node.dfs.read.head", version_id = %version_id.0,
            branch = "readonly_version", cached = tracing::field::Empty);
        record_metadata_table_lock();
        if let Some(cached) = self
            .readonly_version_cache
            .lock()
            .map_err(|_| unavailable("DFS readonly version cache is poisoned"))?
            .get(version_id)
            .cloned()
        {
            span.record("cached", true);
            return Ok(cached);
        }
        span.record("cached", false);
        // No cache guard crosses Meta RPC, validation or index construction.
        let (version, layout) = span.in_scope(|| self.meta.get_file_version(version_id))?;
        let cached = Arc::new(ReadOnlyVersion::checked(version_id, version, layout)?);
        #[cfg(test)]
        {
            let barrier = self.readonly_version_before_install.lock().unwrap().clone();
            if let Some(barrier) = barrier {
                barrier.wait();
            }
        }
        record_metadata_table_lock();
        let mut cache = self
            .readonly_version_cache
            .lock()
            .map_err(|_| unavailable("DFS readonly version cache is poisoned"))?;
        if let Some(existing) = cache.get(version_id) {
            if existing.version != cached.version || existing.layout != cached.layout {
                return Err(invalid(
                    "DFS immutable version changed during concurrent cache admission",
                ));
            }
            return Ok(existing.clone());
        }
        cache.insert(version_id.clone(), cached.clone());
        Ok(cached)
    }

    /// Admit this Node's committed write base into the readonly version cache
    /// when it is the version a reader is about to pin, so reading back a file
    /// this Node just wrote costs no extra GetFileVersion.
    fn seed_readonly_version_from_write_state(&self, inode: &InodeRecord) -> Result<()> {
        let Some(version_id) = inode.head_version.as_ref() else {
            return Ok(());
        };
        let Some(state) = self.write_state(&inode.inode_id)? else {
            return Ok(());
        };
        let (version, layout) = {
            let state = state
                .lock()
                .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
            match state.base_version.as_ref() {
                Some(version) if &version.id == version_id => {
                    (version.clone(), state.base_layout.clone())
                }
                _ => return Ok(()),
            }
        };
        record_metadata_table_lock();
        let mut cache = self
            .readonly_version_cache
            .lock()
            .map_err(|_| unavailable("DFS readonly version cache is poisoned"))?;
        if cache.get(version_id).is_none() {
            let cached = Arc::new(ReadOnlyVersion::checked(version_id, version, layout)?);
            cache.insert(version_id.clone(), cached);
        }
        Ok(())
    }

    pub(super) fn readonly_version_for_inode(
        &self,
        inode: &InodeRecord,
    ) -> Result<Arc<ReadOnlyVersion>> {
        self.seed_readonly_version_from_write_state(inode)?;
        let cached = self.load_readonly_version(inode.head_version.as_ref())?;
        if cached
            .version
            .as_ref()
            .is_some_and(|version| version.inode_id != inode.inode_id)
        {
            return Err(invalid("DFS readonly version belongs to a different inode"));
        }
        Ok(cached)
    }
}
