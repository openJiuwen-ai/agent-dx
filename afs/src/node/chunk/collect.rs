//! Store-local primitives of Node GC execution: the completed generation, the
//! inventory reported to Meta, and retiring one generation's copies. The pass
//! that drives them is `node::gc::collect`; these stay here because they work
//! on `LocalChunkStore` internals. Progress persists through
//! `checkpoint::save_checkpoint`.
use super::{checkpoint::save_checkpoint, *};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::Read,
    os::unix::fs::OpenOptionsExt,
};

const INVENTORY_PAGE: usize = 1024;
// Orphan IDs do not carry a digest. Bound the number of full block reads
// per pass independently of the regular inventory page (32 MiB at most).
const CRC_ORPHANS_PER_PASS: usize = 8;

fn crc_orphan_inventory_entry(path: &Path, id: &ChunkId) -> Result<Option<ChunkObject>> {
    let file = match OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)
    {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let metadata = file.metadata()?;
    let maximum = staged::MAX_STAGED_CHUNK_BYTES;
    if !metadata.is_file() || metadata.len() == 0 || metadata.len() > maximum as u64 {
        return Ok(None);
    }
    let mut bytes = Vec::new();
    file.take(maximum as u64 + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 != metadata.len() {
        return Ok(None);
    }
    let table = super::super::chunk_crc32c::CrcTable::build(&bytes)?;
    // This observation is only for GC. It does not establish a readable copy
    // or replace the descriptor from a committed receipt.
    Ok(Some(ChunkObject {
        id: id.clone(),
        length: table.length,
        content_digest: table.descriptor_digest(),
        encoding: ChunkEncoding::Raw,
    }))
}

impl LocalChunkStore {
    pub(crate) fn gc_generation(&self) -> Result<u64> {
        Ok(self
            .catalog
            .lock()
            .map_err(|_| invalid("chunk catalog poisoned"))?
            .gc_generation)
    }
    pub(crate) fn gc_inventory(&self) -> Result<Vec<ChunkObject>> {
        let mut chunks = self
            .catalog
            .lock()
            .map_err(|_| invalid("chunk catalog poisoned"))?
            .records
            .values()
            .map(|record| (record.chunk.id.clone(), record.chunk.clone()))
            .collect::<BTreeMap<_, _>>();
        let mut crc_orphans = BTreeSet::new();
        for entry in fs::read_dir(&self.chunks)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            let id = ChunkId::new(entry.file_name().to_string_lossy().into_owned());
            if let Some(chunk) = ChunkObject::from_content_addressed_id(&id) {
                chunks.entry(id).or_insert(chunk);
            } else if id.crc32c_write_generation().is_some() && !chunks.contains_key(&id) {
                crc_orphans.insert(id);
            }
        }
        let mut inventory = Vec::new();
        if !crc_orphans.is_empty() {
            let count = crc_orphans.len();
            let limit = count.min(CRC_ORPHANS_PER_PASS);
            let start = self
                .gc_crc_inventory_cursor
                .fetch_add(limit as u64, Ordering::Relaxed) as usize
                % count;
            let observed: Vec<_> = crc_orphans
                .iter()
                .cycle()
                .skip(start)
                .take(limit)
                .filter_map(|id| match crc_orphan_inventory_entry(&self.chunk_path(id), id) {
                    Ok(entry) => entry,
                    Err(error) => {
                        tracing::warn!(chunk_id = %id.0, %error, "cannot inspect CRC orphan for GC");
                        None
                    }
                })
                .collect();
            let catalog = self
                .catalog
                .lock()
                .map_err(|_| invalid("chunk catalog poisoned"))?;
            inventory.extend(observed.into_iter().map(|chunk| {
                catalog
                    .records
                    .get(&chunk.id)
                    .map_or(chunk, |record| record.chunk.clone())
            }));
        }
        let count = chunks.len();
        if count != 0 {
            let limit = count.min(INVENTORY_PAGE - inventory.len());
            let start = self
                .gc_inventory_cursor
                .fetch_add(limit as u64, Ordering::Relaxed) as usize
                % count;
            inventory.extend(chunks.values().cycle().skip(start).take(limit).cloned());
        }
        Ok(inventory)
    }
    pub(crate) fn collect_generation(&self, generation: u64, chunks: &[ChunkId]) -> Result<usize> {
        // Drain installs and exclude new catalog publications through checkpoint + WAL rotation.
        let _admission = self
            .catalog_committer
            .admit(std::iter::empty(), usize::MAX)?;
        #[cfg(any(feature = "dfs", test))]
        {
            // Retire only collected residents. Existing readers keep their Arc
            // and its budget charge until their last use completes.
            let retired = {
                let mut state = self
                    .peer_read_cache
                    .lock()
                    .map_err(|_| invalid("peer read cache poisoned"))?;
                let retired = chunks
                    .iter()
                    .filter_map(|id| state.remove_resident(id))
                    .collect::<Vec<_>>();
                self.observe_peer_serve(&state);
                retired
            };
            drop(retired);
            self.peer_serve_changed.notify_all();
        }
        let mut catalog = self
            .catalog
            .lock()
            .map_err(|_| invalid("chunk catalog poisoned"))?;
        if generation <= catalog.gc_generation {
            return Ok(0);
        }
        for id in chunks {
            if id.0.is_empty()
                || id.0 == "."
                || id.0 == ".."
                || !id
                    .0
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte))
            {
                return Err(invalid("unsafe chunk deletion identity"));
            }
        }
        let mut next = LocalCatalog {
            revision: catalog
                .revision
                .checked_add(1)
                .ok_or_else(|| invalid("chunk catalog revision overflow"))?,
            gc_generation: generation,
            records: catalog.records.clone(),
            cache_intents: catalog.cache_intents.clone(),
            cache_bytes: catalog.cache_bytes,
            cache_order: catalog.cache_order.clone(),
        };
        let mut removed = 0;
        for id in chunks {
            for path in [
                self.chunk_path(id),
                self.outboards.join(format!("{}.bao", safe_id(&id.0))),
            ] {
                match fs::remove_file(path) {
                    Ok(()) => removed += 1,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error.into()),
                }
            }
            next.remove_record(id);
        }
        File::open(&self.chunks)?.sync_all()?;
        File::open(&self.outboards)?.sync_all()?;
        // Persist progress before ACK, and before removing any covered journal.
        // A crash before this point leaves Meta's deletion barrier in place.
        save_checkpoint(&self.root, &next)?;
        *catalog = next;
        OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&self.catalog_path)?
            .sync_all()?;
        self.root_dir.sync_all()?;
        Ok(removed)
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc_inventory_advances_past_rejected_orphans() {
        let root = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(root.path(), "n").unwrap();
        let generation = ChunkObject::generation_id(&OperationId::new("inventory paging"));
        for index in 0..CRC_ORPHANS_PER_PASS {
            fs::write(
                store
                    .chunks
                    .join(format!("crc32c-v2-{generation}-{index:032x}")),
                [],
            )
            .unwrap();
        }
        let id = ChunkId::new(format!("crc32c-v2-{generation}-{}", "f".repeat(32)));
        fs::write(store.chunk_path(&id), b"recoverable").unwrap();
        fs::write(
            store.chunks.join(format!("crc32c-v2-{generation}-invalid")),
            b"bad id",
        )
        .unwrap();
        assert!(store.gc_inventory().unwrap().is_empty());
        let next = store.gc_inventory().unwrap();
        assert_eq!(next.len(), 1);
        assert_eq!(next[0].id, id);
    }
    #[test]
    fn gc_waits_for_inflight_catalog_installation() {
        let root = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalChunkStore::open(root.path(), "n").unwrap());
        let id = ChunkId::new("inflight");
        let admission = store
            .catalog_committer
            .admit(std::iter::once(&id), 1024)
            .unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        let worker_store = store.clone();
        let worker = std::thread::spawn(move || {
            tx.send(worker_store.collect_generation(1, &[])).unwrap();
        });
        assert!(
            rx.recv_timeout(Duration::from_millis(100)).is_err(),
            "GC must not rotate WAL while an install owns admission"
        );
        drop(admission);
        rx.recv_timeout(Duration::from_secs(3)).unwrap().unwrap();
        worker.join().unwrap();
        assert_eq!(store.gc_generation().unwrap(), 1);
    }

    #[test]
    fn covered_wal_prefix_survives_torn_tail_after_checkpoint() {
        let root = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(root.path(), "n").unwrap();
        let staged = StagedChunk::new(OperationId::new("p"), b"state".to_vec());
        ChunkSink::put(&store, staged.clone()).unwrap();
        let prefix = fs::read(&store.catalog_path).unwrap();
        store.collect_generation(1, &[staged.chunk.id]).unwrap();
        let mut torn = prefix.clone();
        torn.extend_from_slice(b"{\"revision\":");
        fs::write(&store.catalog_path, torn).unwrap();
        drop(store);
        let store = LocalChunkStore::open(root.path(), "n").unwrap();
        assert_eq!(store.gc_generation().unwrap(), 1);
        assert_eq!(fs::read(&store.catalog_path).unwrap(), prefix);
        assert!(store.gc_inventory().unwrap().is_empty());
    }

    #[test]
    fn deletion_reply_loss_does_not_delete_recreated_content_after_restart() {
        let root = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(root.path(), "n").unwrap();
        let staged = StagedChunk::new(OperationId::new("upload"), b"state".to_vec());
        ChunkSink::put(&store, staged.clone()).unwrap();
        let reader = store.open_verified(&staged.chunk.id).unwrap();
        store
            .collect_generation(1, std::slice::from_ref(&staged.chunk.id))
            .unwrap();
        assert!(!store.chunk_path(&staged.chunk.id).exists());
        let mut bytes = [0; 5];
        reader.read_at(0, &mut bytes).unwrap();
        assert_eq!(&bytes, b"state", "in-flight descriptor remains valid");
        drop(reader);
        drop(store);
        let store = LocalChunkStore::open(root.path(), "n").unwrap();
        assert_eq!(store.gc_generation().unwrap(), 1);
        ChunkSink::put(&store, staged.clone()).unwrap();
        store
            .collect_generation(1, std::slice::from_ref(&staged.chunk.id))
            .unwrap();
        store.read_at(&staged.chunk.id, 0, &mut bytes).unwrap();
        assert_eq!(&bytes, b"state");
    }
    #[test]
    fn repeated_collection_compacts_catalog_and_reopens_empty() {
        let root = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(root.path(), "n").unwrap();
        for generation in 1..=12 {
            let staged = StagedChunk::new(
                OperationId::new(format!("p-{generation}")),
                vec![generation as u8; 1024],
            );
            ChunkSink::put(&store, staged.clone()).unwrap();
            store
                .collect_generation(generation, &[staged.chunk.id])
                .unwrap();
        }
        drop(store);
        let store = LocalChunkStore::open(root.path(), "n").unwrap();
        assert_eq!(store.gc_generation().unwrap(), 12);
        assert!(store.gc_inventory().unwrap().is_empty());
        assert!(store.catalog_path.metadata().unwrap().len() < 4096);
    }
    #[test]
    fn failed_checkpoint_never_acknowledges_an_unpersisted_gc_generation() {
        let root = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(root.path(), "n").unwrap();
        let staged = StagedChunk::new(OperationId::new("p"), vec![3; 64]);
        ChunkSink::put(&store, staged.clone()).unwrap();
        fs::create_dir(root.path().join("catalog.snapshot.tmp")).unwrap();
        assert!(
            store
                .collect_generation(1, std::slice::from_ref(&staged.chunk.id))
                .is_err()
        );
        assert_eq!(store.gc_generation().unwrap(), 0);
        fs::remove_dir(root.path().join("catalog.snapshot.tmp")).unwrap();
        drop(store);
        let store = LocalChunkStore::open(root.path(), "n").unwrap();
        store.collect_generation(1, &[staged.chunk.id]).unwrap();
        assert_eq!(store.gc_generation().unwrap(), 1);
    }
    #[test]
    fn checkpoint_recovery_skips_covered_wal_and_rejects_corruption() {
        let root = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(root.path(), "n").unwrap();
        let staged = StagedChunk::new(OperationId::new("p"), vec![8; 64]);
        ChunkSink::put(&store, staged.clone()).unwrap();
        let old_wal = fs::read(&store.catalog_path).unwrap();
        store
            .collect_generation(1, std::slice::from_ref(&staged.chunk.id))
            .unwrap();
        // Crash window: checkpoint committed, covered WAL not truncated yet.
        fs::write(&store.catalog_path, old_wal).unwrap();
        drop(store);
        let store = LocalChunkStore::open(root.path(), "n").unwrap();
        assert_eq!(store.gc_generation().unwrap(), 1);
        assert!(store.gc_inventory().unwrap().is_empty());
        drop(store);
        let path = root.path().join("catalog.snapshot");
        let mut bytes = fs::read(&path).unwrap();
        let index = bytes.len() / 2;
        bytes[index] ^= 1;
        fs::write(path, bytes).unwrap();
        assert!(
            LocalChunkStore::open(root.path(), "n").is_err(),
            "corrupt progress cannot reset the deletion fence"
        );
    }
}
