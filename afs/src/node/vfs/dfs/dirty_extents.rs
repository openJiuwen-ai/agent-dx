use super::*;

#[derive(Clone)]
pub(super) struct DirtyExtent {
    pub(super) file_offset: u64,
    pub(super) length: u64,
    pub(super) write_seq: u64,
    pub(super) data: Option<SharedChunkBytes>,
    pub(super) builder: Option<usize>,
    pub(super) source_offset: usize,
    // The last write's original coverage boundary, independent of visible
    // interval splits. Reusing this owner must not turn one input copy into
    // multiple copies merely because later writes split the visible map.
    pub(super) native_reuse_end: u64,
    pub(super) native_upload: Option<Arc<NativeUpload>>,
    pub(super) native_descriptor: Option<Arc<Mutex<NativeMetadataLease>>>,
}

#[derive(Default)]
pub(super) struct DirtyExtentMap {
    // Native writes and resize holes retain only the final nonoverlapping
    // visible intervals.
    pub(super) extents: BTreeMap<u64, DirtyExtent>,
    pub(super) builders: Vec<NativeDirtyBuilder>,
    pub(super) append_builders: BTreeMap<u64, BTreeSet<(u64, usize)>>,
    pub(super) next_builder_order: u64,
    pub(super) native_admitted_bytes: Option<u64>,
    pub(super) native_operation_id: Option<OperationId>,
    pub(super) native_gc_pin: Option<Arc<crate::node::gc::GcPin>>,
}

pub(super) struct NativeDirtyBuilder {
    pub(super) file_offset: u64,
    pub(super) builder: ChunkBuilder,
    pub(super) descriptor: Option<Arc<Mutex<NativeMetadataLease>>>,
    pub(super) creation_order: u64,
}

// Frozen maps and S3 maps only clone immutable owners. The sole mutable-map
// clone fallback is a read snapshot; production reads use snapshot_range instead.
impl Clone for DirtyExtentMap {
    fn clone(&self) -> Self {
        let bodies = self
            .builders
            .iter()
            .map(|entry| SharedChunkBytes::from_vec(entry.builder.as_bytes().to_vec()))
            .collect::<Vec<_>>();
        let freeze = |extent: &DirtyExtent| {
            let mut extent = extent.clone();
            if let Some(index) = extent.builder.take() {
                extent.data = Some(bodies[index].clone());
            }
            extent
        };
        Self {
            extents: self
                .extents
                .iter()
                .map(|(key, extent)| (*key, freeze(extent)))
                .collect(),
            builders: Vec::new(),
            append_builders: BTreeMap::new(),
            next_builder_order: self.next_builder_order,
            native_admitted_bytes: self.native_admitted_bytes,
            native_operation_id: self.native_operation_id.clone(),
            native_gc_pin: self.native_gc_pin.clone(),
        }
    }
}

impl DirtyExtentMap {
    pub(super) fn native_selection(
        &self,
        position: u64,
        logical_length: u64,
    ) -> Option<(usize, usize, usize)> {
        let latest = self
            .extents
            .range(..=position)
            .next_back()
            .map(|(_, extent)| extent)
            .filter(|extent| position < extent.file_offset + extent.length);
        if let Some(extent) = latest
            && let Some(index) = extent.builder
        {
            return Some((
                index,
                extent.source_offset + (position - extent.file_offset) as usize,
                (extent.native_reuse_end - position) as usize,
            ));
        }
        if position >= logical_length {
            return self
                .append_builders
                .get(&position)
                .and_then(|indices| indices.last())
                .map(|(_, index)| {
                    let entry = &self.builders[*index];
                    (
                        *index,
                        entry.builder.len(),
                        entry.builder.writable_capacity() - entry.builder.len(),
                    )
                });
        }
        None
    }

    pub(super) fn native_capacity(position: u64, remaining: usize, logical_length: u64) -> usize {
        if position >= logical_length {
            COMMIT_CHUNK_BYTES as usize
        } else {
            remaining
                .min(COMMIT_CHUNK_BYTES as usize)
                .max(4096)
                .next_power_of_two()
        }
    }

    pub(super) fn write_native_prepared(
        &mut self,
        position: u64,
        data: &[u8],
        write_seq: u64,
        selection: Option<(usize, usize, usize)>,
        prepared: Option<ChunkBuilder>,
        descriptor: Option<Arc<Mutex<NativeMetadataLease>>>,
    ) -> Result<usize> {
        self.promote_native();
        let (index, source_offset, available) = if let Some(selection) = selection {
            selection
        } else {
            let builder =
                prepared.ok_or_else(|| unavailable("native write has no admitted builder"))?;
            let capacity = builder.writable_capacity();
            let index = self
                .builders
                .iter()
                .position(|entry| {
                    entry.builder.writable_capacity() == 0 && entry.descriptor.is_none()
                })
                .unwrap_or(self.builders.len());
            let creation_order = self.next_builder_order;
            self.next_builder_order = self
                .next_builder_order
                .checked_add(1)
                .ok_or_else(file_too_large)?;
            let entry = NativeDirtyBuilder {
                file_offset: position,
                builder,
                descriptor,
                creation_order,
            };
            if index == self.builders.len() {
                self.builders.push(entry);
            } else {
                self.builders[index] = entry;
            }
            (index, 0, capacity)
        };
        let take = available.min(data.len());
        let previous_end =
            self.builders[index].file_offset + self.builders[index].builder.len() as u64;
        self.builders[index]
            .builder
            .write_at(source_offset as u64, &data[..take])?;
        self.update_append_builder(index, Some(previous_end));
        self.insert_native(DirtyExtent {
            file_offset: position,
            length: take as u64,
            write_seq,
            data: None,
            builder: Some(index),
            source_offset,
            native_reuse_end: position + take as u64,
            native_upload: None,
            native_descriptor: self.builders[index].descriptor.clone(),
        });
        let admitted = self.native_admitted_bytes.get_or_insert(0);
        *admitted = admitted.saturating_add(take as u64);
        Ok(take)
    }

    pub(super) fn iter_extents(&self) -> impl DoubleEndedIterator<Item = &DirtyExtent> {
        self.extents.values()
    }

    fn iter_extents_mut(&mut self) -> impl Iterator<Item = &mut DirtyExtent> {
        self.extents.values_mut()
    }

    fn range_start(&self, offset: u64) -> u64 {
        self.extents
            .range(..=offset)
            .next_back()
            .map_or(offset, |(start, extent)| {
                if extent.file_offset + extent.length > offset {
                    *start
                } else {
                    offset
                }
            })
    }

    fn intersecting_extents(&self, offset: u64, end: u64) -> impl Iterator<Item = &DirtyExtent> {
        self.extents
            .range(self.range_start(offset)..end)
            .map(|(_, extent)| extent)
    }

    // Caller has already checked the new extent's endpoint. Only the two
    // boundary fragments survive; masked owners are dropped without touching
    // immutable snapshots, frozen batches or asynchronous holders.
    pub(super) fn insert_native(&mut self, extent: DirtyExtent) {
        let start = extent.file_offset;
        let end = start + extent.length;
        if start == end {
            return;
        }
        let keys = self
            .extents
            .range(self.range_start(start)..end)
            .map(|(key, _)| *key)
            .collect::<Vec<_>>();
        for key in keys {
            let Some(previous) = self.extents.remove(&key) else {
                continue;
            };
            let previous_end = previous.file_offset + previous.length;
            if previous.file_offset < start {
                let mut left = previous.clone();
                left.length = start - previous.file_offset;
                self.extents.insert(left.file_offset, left);
            }
            if previous_end > end {
                let mut right = previous;
                if right.data.is_some() || right.builder.is_some() || right.native_upload.is_some()
                {
                    right.source_offset += (end - right.file_offset) as usize;
                }
                right.file_offset = end;
                right.length = previous_end - end;
                self.extents.insert(end, right);
            }
        }
        self.extents.insert(start, extent);
    }

    fn update_append_builder(&mut self, index: usize, previous_end: Option<u64>) {
        if let Some(end) = previous_end
            && let Some(indices) = self.append_builders.get_mut(&end)
        {
            indices.remove(&(self.builders[index].creation_order, index));
            if indices.is_empty() {
                self.append_builders.remove(&end);
            }
        }
        let entry = &self.builders[index];
        if entry.builder.len() < entry.builder.writable_capacity() {
            self.append_builders
                .entry(entry.file_offset + entry.builder.len() as u64)
                .or_default()
                .insert((entry.creation_order, index));
        }
    }

    fn promote_native(&mut self) {
        self.native_admitted_bytes.get_or_insert(0);
    }

    /// Test driver for map-level index and builder tests. Production native writes
    /// go through `write_native_prepared`; the pipeline-less native write path that
    /// used this was removed in S1a-1 (R53), and R33 decides its fate in S1b.
    #[cfg(test)]
    pub(super) fn write_native_at(
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
        self.promote_native();
        let mut accepted = 0;
        while accepted < data.len() {
            let position = offset + accepted as u64;
            let latest = self
                .extents
                .range(..=position)
                .next_back()
                .map(|(_, extent)| extent)
                .filter(|extent| position < extent.file_offset + extent.length);
            let existing = latest.and_then(|extent| {
                extent.builder.map(|index| {
                    let source = extent.source_offset + (position - extent.file_offset) as usize;
                    (index, source, (extent.native_reuse_end - position) as usize)
                })
            });
            let append = if existing.is_none() && position >= logical_length {
                self.append_builders
                    .get(&position)
                    .and_then(|indices| indices.last())
                    .map(|(_, index)| {
                        let entry = &self.builders[*index];
                        let length = entry.builder.len();
                        (*index, length, entry.builder.writable_capacity() - length)
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
                self.builders.push(NativeDirtyBuilder {
                    file_offset: position,
                    builder,
                    descriptor: None,
                    creation_order: index as u64,
                });
                (index, 0, capacity)
            };
            let take = available.min(data.len() - accepted);
            let previous_end =
                self.builders[index].file_offset + self.builders[index].builder.len() as u64;
            self.builders[index]
                .builder
                .write_at(source_offset as u64, &data[accepted..accepted + take])?;
            self.update_append_builder(index, Some(previous_end));
            self.insert_native(DirtyExtent {
                file_offset: position,
                length: take as u64,
                write_seq,
                data: None,
                builder: Some(index),
                source_offset,
                native_reuse_end: position + take as u64,
                native_upload: None,
                native_descriptor: None,
            });
            let admitted = self.native_admitted_bytes.get_or_insert(0);
            *admitted = admitted.saturating_add(take as u64);
            accepted += take;
        }
        Ok(accepted)
    }

    // Consuming builders is the only path that exposes their allocation to
    // asynchronous holders. No writable alias survives this transition.
    pub(super) fn freeze_builders(&mut self) {
        let bodies = std::mem::take(&mut self.builders)
            .into_iter()
            .map(|entry| entry.builder.freeze())
            .collect::<Vec<_>>();
        self.append_builders.clear();
        for extent in self.iter_extents_mut() {
            if let Some(index) = extent.builder.take() {
                extent.data = Some(bodies[index].clone());
            }
        }
    }

    // Called while the inode mutex is held. Copy only bytes intersecting this
    // read; a later write may mutate its builder while network reads run.
    pub(super) fn snapshot_range(&self, offset: u64, length: usize) -> Result<Self> {
        let end = offset
            .checked_add(length as u64)
            .ok_or_else(|| invalid("dirty snapshot range overflow"))?;
        let mut snapshot = Self {
            native_admitted_bytes: self.native_admitted_bytes.map(|_| 0),
            ..Self::default()
        };
        for extent in self.intersecting_extents(offset, end) {
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
            snapshot.extents.insert(copied.file_offset, copied);
        }
        Ok(snapshot)
    }

    pub(super) fn resize(
        &mut self,
        old_length: u64,
        new_length: u64,
        write_seq: u64,
    ) -> Result<()> {
        if new_length < old_length {
            for entry in &mut self.builders {
                let keep = new_length
                    .saturating_sub(entry.file_offset)
                    .min(entry.builder.len() as u64) as usize;
                entry.builder.truncate(keep);
            }
            self.append_builders.clear();
            for index in 0..self.builders.len() {
                self.update_append_builder(index, None);
            }
            for extent in self.iter_extents_mut() {
                if extent.builder.is_some() {
                    extent.length = extent
                        .length
                        .min(new_length.saturating_sub(extent.file_offset));
                    extent.native_reuse_end = extent.native_reuse_end.min(new_length);
                }
            }
            self.extents.retain(|_, extent| extent.length > 0);
        }
        let (start, stop) = if new_length < old_length {
            (new_length, old_length)
        } else {
            (old_length, new_length)
        };
        if start < stop {
            let hole = DirtyExtent {
                file_offset: start,
                length: stop - start,
                write_seq,
                data: None,
                builder: None,
                source_offset: 0,
                native_reuse_end: 0,
                native_upload: None,
                native_descriptor: None,
            };
            self.insert_native(hole);
        }
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn overlay(&self, offset: u64, out: &mut [u8]) -> Result<()> {
        self.overlay_native(offset, out, None)
    }

    pub(super) fn overlay_native(
        &self,
        offset: u64,
        out: &mut [u8],
        store: Option<&dyn ChunkStore>,
    ) -> Result<()> {
        let end = offset
            .checked_add(out.len() as u64)
            .ok_or_else(|| invalid("dirty read range overflow"))?;
        for extent in self.intersecting_extents(offset, end) {
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
            if let Some(upload) = &extent.native_upload {
                let store = store
                    .ok_or_else(|| unavailable("native uploaded read requires its local store"))?;
                let source = extent.source_offset as u64 + start - extent.file_offset;
                if upload.read_at(store, source, &mut out[output_start..output_start + length])?
                    != length
                {
                    return Err(unavailable("native uploaded local read ended early"));
                }
                continue;
            }
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

    pub(super) fn restore_before(&mut self, mut older: DirtyExtentMap) {
        if self.native_operation_id.is_none() {
            self.native_operation_id = older.native_operation_id.take();
            self.native_gc_pin = older.native_gc_pin.take();
        }
        // The restored batch has already frozen its owners. Mutable builder
        // indices belong only to the active map and must never be renumbered.
        let mut ordered = std::mem::take(&mut older.extents)
            .into_values()
            .chain(std::mem::take(&mut self.extents).into_values())
            .collect::<Vec<_>>();
        ordered.sort_by_key(|extent| extent.write_seq);
        for extent in ordered {
            self.insert_native(extent);
        }
        self.native_admitted_bytes = match (older.native_admitted_bytes, self.native_admitted_bytes)
        {
            (None, None) => None,
            (left, right) => Some(left.unwrap_or(0).saturating_add(right.unwrap_or(0))),
        };
    }

    pub(super) fn dirty_data_bytes(&self) -> u64 {
        if let Some(accepted) = self.native_admitted_bytes {
            return accepted;
        }
        self.iter_extents()
            .filter(|extent| extent.data.is_some())
            .fold(0u64, |total, extent| total.saturating_add(extent.length))
    }

    pub(super) fn is_empty(&self) -> bool {
        self.extents.is_empty()
    }
}
