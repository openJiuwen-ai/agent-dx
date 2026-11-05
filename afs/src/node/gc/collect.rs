//! One local GC collection pass.

use std::sync::{Arc, Mutex};

use afs_error::{Error, Result};

use super::GcReferences;
use crate::node::{chunk::LocalChunkStore, read::engine::DfsReadEngine};

/// Runs collection passes for this Node's local chunk store. The fault
/// layer's reaper (`node::fault::reaper`) calls [`Collector::collect_once`]
/// periodically. S3 mode has no references and therefore no collector.
pub(crate) struct Collector {
    references: Arc<GcReferences>,
    store: Arc<LocalChunkStore>,
    read_engine: Arc<DfsReadEngine>,
    node_id: String,
    /// Orders passes against each other; pins never take it.
    gate: Mutex<()>,
}

impl Collector {
    pub(crate) fn new(
        references: Arc<GcReferences>,
        store: Arc<LocalChunkStore>,
        read_engine: Arc<DfsReadEngine>,
        node_id: String,
    ) -> Self {
        Self {
            references,
            store,
            read_engine,
            node_id,
            gate: Mutex::new(()),
        }
    }

    /// One collection pass; passes never overlap. Releases are flushed first;
    /// local copies are retired only when Meta's batch waits on this node for a
    /// newer generation, and completion is acknowledged with an empty
    /// inventory. Returns the number of chunks retired.
    pub(crate) fn collect_once(&self) -> Result<usize> {
        let _gate = self
            .gate
            .lock()
            .map_err(|_| Error::coded(afs_error::NODE_VFS_UNAVAILABLE, "GC worker poisoned"))?;
        self.references.flush()?;
        let completed = self.store.gc_generation()?;
        let batch = self
            .references
            .control
            .collect(completed, self.store.gc_inventory()?)?;
        if batch.waiting_nodes.iter().any(|node| node == &self.node_id)
            && batch.generation > completed
        {
            self.read_engine.evict_collected(&batch.chunks)?;
            self.store
                .lifecycle()
                .retire_generation(batch.generation, &batch.chunks)?;
            self.references
                .control
                .collect(batch.generation, Vec::new())?;
            return Ok(batch.chunks.len());
        }
        Ok(0)
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::{
        dfs::{ChunkObject, FileVersionId, GcState, InodeId, NamespaceId, OperationId},
        node::{
            chunk::{ChunkSink, StagedChunk},
            gc::GcControl,
            read::engine::{
                DfsReadConfig, UnimplementedChunkTransfer, UnimplementedReadSourceProvider,
            },
        },
    };

    /// Meta stand-in: answers the first `collect` with `batch`, records every call.
    struct Control {
        batch: GcState,
        calls: Mutex<Vec<(u64, usize)>>,
    }

    impl GcControl for Control {
        fn retain(&self, _: &str, _: &InodeId, _: Option<&FileVersionId>, _: bool) -> Result<()> {
            Ok(())
        }
        fn release(&self, _: &str) -> Result<()> {
            Ok(())
        }
        fn reserve_publication(&self, _: &OperationId, _: Vec<ChunkObject>) -> Result<()> {
            Ok(())
        }
        fn collect(&self, completed: u64, inventory: Vec<ChunkObject>) -> Result<GcState> {
            let mut calls = self.calls.lock().unwrap();
            calls.push((completed, inventory.len()));
            Ok(if calls.len() == 1 {
                self.batch.clone()
            } else {
                GcState::default()
            })
        }
    }

    fn collector(
        waiting: &str,
    ) -> (
        tempfile::TempDir,
        Arc<Control>,
        Arc<LocalChunkStore>,
        Collector,
    ) {
        let temp = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalChunkStore::open(temp.path(), "n").unwrap());
        let staged = StagedChunk::new(OperationId::new("p"), b"state".to_vec());
        ChunkSink::put(store.as_ref(), staged.clone()).unwrap();
        let control = Arc::new(Control {
            batch: GcState {
                generation: 1,
                chunks: vec![staged.chunk.id],
                waiting_nodes: vec![waiting.into()],
            },
            calls: Mutex::new(Vec::new()),
        });
        let engine = Arc::new(DfsReadEngine::new(
            NamespaceId::new("default"),
            "n".into(),
            store.clone(),
            Arc::new(UnimplementedReadSourceProvider),
            Arc::new(UnimplementedChunkTransfer),
            DfsReadConfig::default(),
        ));
        let collector = Collector::new(
            GcReferences::new(control.clone()),
            store.clone(),
            engine,
            "n".into(),
        );
        (temp, control, store, collector)
    }

    #[test]
    fn a_batch_waiting_on_this_node_retires_and_acknowledges() {
        let (_temp, control, store, collector) = collector("n");
        assert_eq!(collector.collect_once().unwrap(), 1);
        assert_eq!(*control.calls.lock().unwrap(), vec![(0, 1), (1, 0)]);
        assert_eq!(store.gc_generation().unwrap(), 1);
        assert!(store.gc_inventory().unwrap().is_empty());
    }

    #[test]
    fn a_batch_waiting_on_another_node_keeps_local_copies() {
        let (_temp, control, store, collector) = collector("other");
        assert_eq!(collector.collect_once().unwrap(), 0);
        assert_eq!(*control.calls.lock().unwrap(), vec![(0, 1)]);
        assert_eq!(store.gc_generation().unwrap(), 0);
        assert_eq!(store.gc_inventory().unwrap().len(), 1);
    }
}
