//! Model only the local catalog durability protocol. Capture real file contents
//! and inode identities at the store's barriers, then recover a fresh copy of
//! the possible disk image. The original OS page cache is never used for replay.
use super::*;
use std::{cell::RefCell, collections::BTreeMap, os::unix::fs::MetadataExt};

type Names = BTreeMap<PathBuf, u64>;
type Contents = BTreeMap<u64, Vec<u8>>;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Omit {
    None,
    Wal,
    WalParent,
    Chunks,
}

struct Image {
    label: &'static str,
    names: Names,
    contents: Contents,
    acknowledged: Vec<StagedChunk>,
    generation: u64,
}

struct Trace {
    root: PathBuf,
    names: Names,
    contents: Contents,
    // An unlinked inode must not be recycled into a different file in a trace.
    inodes: BTreeMap<u64, File>,
    cuts: Vec<Image>,
    acknowledged: Vec<StagedChunk>,
    generation: u64,
    omit: Omit,
    fail_wal_parent: bool,
    wal_parent_calls: usize,
}

std::thread_local! {
    static TRACE: RefCell<Option<Trace>> = const { RefCell::new(None) };
}

impl Trace {
    fn files(&mut self, dir: &str) -> Names {
        let mut names = Names::new();
        for entry in fs::read_dir(self.root.join(dir)).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_file() {
                let ino = entry.metadata().unwrap().ino();
                self.inodes
                    .entry(ino)
                    .or_insert_with(|| File::open(entry.path()).unwrap());
                names.insert(Path::new(dir).join(entry.file_name()), ino);
            }
        }
        names
    }

    fn start(root: &Path, omit: Omit) {
        let mut trace = Self {
            root: root.to_owned(),
            names: Names::new(),
            contents: Contents::new(),
            inodes: BTreeMap::new(),
            cuts: Vec::new(),
            acknowledged: Vec::new(),
            generation: 0,
            omit,
            fail_wal_parent: false,
            wal_parent_calls: 0,
        };
        // Begin with a successfully opened, empty store. Its device marker
        // and three subdirectories are already durable before any chunk write.
        trace.names = trace.files("");
        for (name, ino) in &trace.names {
            trace
                .contents
                .insert(*ino, fs::read(root.join(name)).unwrap());
        }
        TRACE.with(|slot| assert!(slot.replace(Some(trace)).is_none()));
    }

    fn push(&mut self, label: &'static str) {
        let mut live = self.files("");
        live.extend(self.files("chunks"));
        // At each cut, let each observed directory keep its last synced view
        // or persist all its current changes early. A write whose file hasn't
        // synced can leave the old bytes, a partial append or the full result.
        for root_survives in [false, true] {
            for chunks_survive in [false, true] {
                let mut names = Names::new();
                for (dir, survives) in [("", root_survives), ("chunks", chunks_survive)] {
                    let source = if survives { &live } else { &self.names };
                    names.extend(source.iter().filter_map(|(name, ino)| {
                        (name.parent() == Some(Path::new(dir))).then_some((name.clone(), *ino))
                    }));
                }
                for persisted in [0, 1, 2] {
                    let contents = names
                        .iter()
                        .map(|(name, ino)| {
                            let old = self.contents.get(ino).map_or(&[][..], Vec::as_slice);
                            let mut bytes = old.to_vec();
                            if persisted != 0 && live.get(name) == Some(ino) {
                                let current = fs::read(self.root.join(name)).unwrap();
                                if persisted == 1 && current.starts_with(old) {
                                    let added = current.len() - old.len();
                                    bytes.extend_from_slice(
                                        &current[old.len()..old.len() + added / 2],
                                    );
                                } else {
                                    bytes = current;
                                }
                            }
                            (*ino, bytes)
                        })
                        .collect();
                    self.cuts.push(Image {
                        label,
                        names: names.clone(),
                        contents,
                        acknowledged: self.acknowledged.clone(),
                        generation: self.generation,
                    });
                }
            }
        }
    }

    fn finish() -> Self {
        TRACE.with(|slot| slot.take().unwrap())
    }
}

pub(super) fn cut(label: &'static str) {
    TRACE.with_borrow_mut(|slot| {
        if let Some(trace) = slot {
            trace.push(label);
        }
    });
}

pub(super) fn file_synced(path: &Path) {
    TRACE.with_borrow_mut(|slot| {
        if let Some(trace) = slot {
            let file = File::open(path).unwrap();
            let ino = file.metadata().unwrap().ino();
            trace.inodes.entry(ino).or_insert(file);
            if path != trace.root.join("catalog.wal") || trace.omit != Omit::Wal {
                trace.contents.insert(ino, fs::read(path).unwrap());
            }
            trace.push("file synced");
        }
    });
}

pub(super) fn directory_synced(path: &Path) {
    TRACE.with_borrow_mut(|slot| {
        if let Some(trace) = slot {
            let dir = path.strip_prefix(&trace.root).unwrap().to_str().unwrap();
            if !(dir == "chunks" && trace.omit == Omit::Chunks
                || dir.is_empty() && trace.omit == Omit::WalParent)
            {
                trace
                    .names
                    .retain(|name, _| name.parent() != Some(Path::new(dir)));
                let files = trace.files(dir);
                trace.names.extend(files);
            }
            trace.push("directory synced");
        }
    });
}

pub(super) fn before_wal_parent() -> Result<()> {
    TRACE.with_borrow_mut(|slot| {
        if let Some(trace) = slot {
            trace.wal_parent_calls += 1;
            if std::mem::take(&mut trace.fail_wal_parent) {
                return Err(invalid("injected WAL parent sync failure"));
            }
        }
        Ok(())
    })
}

impl Image {
    fn restore(&self, root: &Path) {
        for dir in ["chunks", "staging", "outboards"] {
            fs::create_dir(root.join(dir)).unwrap();
        }
        for (path, ino) in &self.names {
            fs::write(root.join(path), &self.contents[ino]).unwrap();
        }
    }

    fn check(&self, root: &Path, written: &[StagedChunk]) -> std::result::Result<(), String> {
        let store = LocalChunkStore::open(root, "node-a").map_err(|e| e.to_string())?;
        let catalog = store.catalog.lock().unwrap();
        if catalog.gc_generation < self.generation {
            return Err("acknowledged GC checkpoint lost".into());
        }
        for item in &self.acknowledged {
            if !catalog.records.contains_key(&item.chunk.id) {
                return Err("acknowledged WAL record lost".into());
            }
        }
        for record in catalog.records.values() {
            if record.state != LocalChunkState::Durable {
                return Err("persisted WAL record has no complete chunk".into());
            }
            let item = written
                .iter()
                .find(|item| item.chunk == record.chunk)
                .unwrap();
            let body = fs::read(store.chunk_path(&record.chunk.id)).map_err(|e| e.to_string())?;
            if body != item.bytes() {
                return Err("persisted WAL record has wrong chunk bytes".into());
            }
        }
        Ok(())
    }
}

fn staged(label: &str) -> StagedChunk {
    StagedChunk::from_shared_crc32c(
        OperationId::new(label),
        SharedChunkBytes::from_buffer(ChunkBuffer::unpooled(label.as_bytes().to_vec())),
    )
    .unwrap()
}

fn ack(item: &StagedChunk) {
    TRACE.with_borrow_mut(|slot| slot.as_mut().unwrap().acknowledged.push(item.clone()));
    cut("write acknowledged");
}

fn verify(trace: Trace, written: &[StagedChunk]) {
    let count = trace.cuts.len();
    for (index, image) in trace.cuts.iter().enumerate() {
        let disk = tempfile::tempdir().unwrap();
        image.restore(disk.path());
        if let Err(err) = image.check(disk.path(), written) {
            panic!("cut {index} ({}) failed: {err}", image.label);
        }
        let store = LocalChunkStore::open(disk.path(), "node-a").unwrap();
        // A torn append or a complete orphan may have survived. Retrying the
        // same chunk must work without creating another immutable identity.
        for item in written {
            store.put(item.clone()).unwrap();
            let mut body = vec![0; item.bytes().len()];
            assert_eq!(
                store.read_at(&item.chunk.id, 0, &mut body).unwrap(),
                body.len()
            );
            assert_eq!(body, item.bytes());
        }
    }
    eprintln!("recovered and retried {count} modeled crash images");
}

#[test]
fn first_append_steady_append_and_gc_truncate_recover() {
    let live = tempfile::tempdir().unwrap();
    let store = LocalChunkStore::open(live.path(), "node-a").unwrap();
    assert!(!store.catalog_path.exists());
    Trace::start(live.path(), Omit::None);
    let first = staged("first");
    let second = staged("second");
    for item in [&first, &second] {
        store.put(item.clone()).unwrap();
        ack(item);
    }
    store.collect_generation(1, &[]).unwrap();
    TRACE.with_borrow_mut(|slot| slot.as_mut().unwrap().generation = 1);
    cut("GC acknowledged");
    let third = staged("after GC");
    store.put(third.clone()).unwrap();
    ack(&third);
    let trace = Trace::finish();
    assert_eq!(trace.wal_parent_calls, 1);
    verify(trace, &[first, second, third]);
}

#[test]
fn failed_first_parent_sync_is_fenced_until_reopen() {
    let live = tempfile::tempdir().unwrap();
    let store = LocalChunkStore::open(live.path(), "node-a").unwrap();
    let item = staged("first barrier failed");
    Trace::start(live.path(), Omit::None);
    TRACE.with_borrow_mut(|slot| slot.as_mut().unwrap().fail_wal_parent = true);
    let err = store.put(item.clone()).unwrap_err();
    assert!(err.to_string().contains("injected WAL parent sync failure"));
    assert!(!store.catalog_name_durable.load(Ordering::Relaxed));
    assert!(store.put(item.clone()).is_err());
    cut("failed parent sync");
    drop(store);
    let store = LocalChunkStore::open(live.path(), "node-a").unwrap();
    let receipt = store.put(item.clone()).unwrap();
    assert_eq!(receipt.durable_acks[0].catalog_revision, 1);
    assert_eq!(store.catalog_commit_stats().groups, 0);
    ack(&item);
    let second = staged("after reopen");
    store.put(second.clone()).unwrap();
    ack(&second);
    let trace = Trace::finish();
    assert_eq!(trace.wal_parent_calls, 1);
    verify(trace, &[item, second]);
}

#[test]
fn gc_can_create_the_first_durable_wal_name() {
    let live = tempfile::tempdir().unwrap();
    let store = LocalChunkStore::open(live.path(), "node-a").unwrap();
    assert!(!store.catalog_path.exists());
    Trace::start(live.path(), Omit::None);
    store.collect_generation(1, &[]).unwrap();
    assert!(store.catalog_path.exists());
    TRACE.with_borrow_mut(|slot| slot.as_mut().unwrap().generation = 1);
    cut("GC acknowledged");
    let item = staged("after GC created WAL");
    store.put(item.clone()).unwrap();
    ack(&item);
    let trace = Trace::finish();
    assert_eq!(trace.wal_parent_calls, 0);
    verify(trace, &[item]);
}

#[test]
fn read_cache_intent_uses_the_same_durable_wal_name() {
    let live = tempfile::tempdir().unwrap();
    let store = LocalChunkStore::open(live.path(), "node-a").unwrap();
    let payload = staged("cache payload");
    Trace::start(live.path(), Omit::None);
    let receipt = store
        .persist_read_cache(&payload, 1, 8192)
        .unwrap()
        .receipt
        .unwrap();
    ack(&payload);
    let intent = ReadCacheIntent {
        namespace_id: crate::dfs::NamespaceId::new("default"),
        file_version_id: crate::dfs::FileVersionId::new("version"),
        layout_root_id: crate::dfs::LayoutRootId::new("layout"),
        copy: crate::dfs::read_cache_copy(&receipt).unwrap(),
    };
    assert!(store.track_read_cache(intent.clone()).unwrap());
    cut("cache intent acknowledged");
    let trace = Trace::finish();
    assert_eq!(trace.wal_parent_calls, 1);
    let disk = tempfile::tempdir().unwrap();
    let committed = trace.cuts.last_chunk::<12>().unwrap().first().unwrap();
    committed.restore(disk.path());
    let recovered = LocalChunkStore::open(disk.path(), "node-a").unwrap();
    assert_eq!(recovered.read_cache_intents().unwrap(), vec![intent]);
    verify(trace, &[payload]);
}

#[test]
fn model_detects_missing_required_barriers() {
    for (omit, failure) in [
        (Omit::Wal, "acknowledged WAL record lost"),
        (Omit::WalParent, "acknowledged WAL record lost"),
        (Omit::Chunks, "persisted WAL record has no complete chunk"),
    ] {
        let live = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(live.path(), "node-a").unwrap();
        let item = staged("must survive ACK");
        Trace::start(live.path(), omit);
        store.put(item.clone()).unwrap();
        ack(&item);
        let trace = Trace::finish();
        let image = trace.cuts.last_chunk::<12>().unwrap().first().unwrap();
        assert_eq!(image.label, "write acknowledged");
        let disk = tempfile::tempdir().unwrap();
        image.restore(disk.path());
        let error = image.check(disk.path(), &[item]).unwrap_err();
        assert_eq!(error, failure);
    }
}
