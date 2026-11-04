//! Durable GC progress fences replayed deletion tasks after content is recreated.
use super::*;
#[derive(Deserialize, Serialize)]
struct Checkpoint {
    schema: u32,
    revision: u64,
    gc_generation: u64,
    records: Vec<LocalChunkRecord>,
    checksum: String,
}
fn checksum(revision: u64, generation: u64, records: &[LocalChunkRecord]) -> Result<String> {
    let bytes = serde_json::to_vec(&(1_u32, revision, generation, records))
        .map_err(|error| invalid(error.to_string()))?;
    Ok(blake3::hash(&bytes).to_hex().to_string())
}
pub(super) fn load_checkpoint(path: &Path) -> Result<Option<LocalCatalog>> {
    let data = match fs::read(path) {
        Ok(data) => data,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let value: Checkpoint = serde_json::from_slice(&data)
        .map_err(|error| invalid(format!("chunk checkpoint decode: {error}")))?;
    if value.schema != 1
        || value.checksum != checksum(value.revision, value.gc_generation, &value.records)?
    {
        return Err(invalid("chunk checkpoint checksum mismatch"));
    }
    let mut records = HashMap::new();
    for record in value.records {
        if record.catalog_revision > value.revision
            || records.insert(record.chunk.id.clone(), record).is_some()
        {
            return Err(invalid("invalid chunk checkpoint record"));
        }
    }
    Ok(Some(LocalCatalog {
        revision: value.revision,
        gc_generation: value.gc_generation,
        records,
    }))
}
fn save_checkpoint(root: &Path, catalog: &LocalCatalog) -> Result<()> {
    let mut records = catalog.records.values().cloned().collect::<Vec<_>>();
    records.sort_by(|a, b| a.chunk.id.cmp(&b.chunk.id));
    let checksum = checksum(catalog.revision, catalog.gc_generation, &records)?;
    let value = Checkpoint {
        schema: 1,
        revision: catalog.revision,
        gc_generation: catalog.gc_generation,
        records,
        checksum,
    };
    let bytes = serde_json::to_vec(&value).map_err(|error| invalid(error.to_string()))?;
    let temp = root.join("catalog.snapshot.tmp");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temp)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    drop(file);
    fs::rename(temp, root.join("catalog.snapshot"))?;
    File::open(root)?.sync_all()?;
    Ok(())
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
        let catalog = self
            .catalog
            .lock()
            .map_err(|_| invalid("chunk catalog poisoned"))?;
        let mut chunks = catalog
            .records
            .values()
            .map(|record| (record.chunk.id.clone(), record.chunk.clone()))
            .collect::<std::collections::BTreeMap<_, _>>();
        for entry in fs::read_dir(&self.chunks)? {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                let id = ChunkId::new(entry.file_name().to_string_lossy().into_owned());
                if let Some(chunk) = ChunkObject::from_content_addressed_id(&id) {
                    chunks.entry(id).or_insert(chunk);
                }
            }
        }
        let count = chunks.len();
        if count == 0 {
            return Ok(Vec::new());
        }
        let start = self.gc_inventory_cursor.fetch_add(1024, Ordering::Relaxed) as usize % count;
        let chunks = chunks.into_values().collect::<Vec<_>>();
        Ok(chunks
            .iter()
            .cycle()
            .skip(start)
            .take(count.min(1024))
            .cloned()
            .collect())
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
            next.records.remove(id);
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
        ChunkStore::put(&store, staged.clone()).unwrap();
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
        ChunkStore::put(&store, staged.clone()).unwrap();
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
        ChunkStore::put(&store, staged.clone()).unwrap();
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
            ChunkStore::put(&store, staged.clone()).unwrap();
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
        ChunkStore::put(&store, staged.clone()).unwrap();
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
        ChunkStore::put(&store, staged.clone()).unwrap();
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
