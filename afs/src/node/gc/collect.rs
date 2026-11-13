//! One GC collection pass over this Node's local chunk store or, in S3 mode,
//! the external object store.

use std::sync::{Arc, Mutex};

use afs_error::{Error, Result};

use super::GcReferences;
use crate::{
    dfs::{ChunkId, ChunkObject},
    node::{chunk::LocalChunkStore, read::engine::DfsReadEngine},
};

/// Objects one LIST page covers. Meta accepts at most 1024 inventory entries.
const EXTERNAL_LIST_PAGE: usize = 1000;

/// What collection needs from an external object store. Credentials and the
/// client stay in the Node; Meta only plans.
pub(crate) trait ExternalObjects: Send + Sync {
    /// Missing objects count as deleted, so a retried batch is idempotent.
    fn delete_objects(&self, chunks: &[ChunkId]) -> Result<()>;
    /// One page after `after`: the generational chunks found and the last key.
    fn list_objects(
        &self,
        after: Option<String>,
        limit: usize,
    ) -> Result<(Vec<ChunkObject>, Option<String>)>;
}

enum CollectTarget {
    Local(Arc<LocalChunkStore>),
    External {
        store: Arc<dyn ExternalObjects>,
        list_pages_per_pass: usize,
        progress: Mutex<ExternalProgress>,
    },
}

/// Memory only: after a restart a generation is deleted again (idempotent) and
/// the listing starts over.
#[derive(Default)]
struct ExternalProgress {
    completed: u64,
    listing: bool,
    after: Option<String>,
}

/// Runs collection passes for this Node. The fault layer's reaper
/// (`node::fault::reaper`) calls [`Collector::collect_once`] periodically.
pub(crate) struct Collector {
    references: Arc<GcReferences>,
    target: CollectTarget,
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
            target: CollectTarget::Local(store),
            read_engine,
            node_id,
            gate: Mutex::new(()),
        }
    }

    pub(crate) fn new_external(
        references: Arc<GcReferences>,
        store: Arc<dyn ExternalObjects>,
        read_engine: Arc<DfsReadEngine>,
        node_id: String,
        list_pages_per_pass: usize,
    ) -> Self {
        Self {
            references,
            target: CollectTarget::External {
                store,
                list_pages_per_pass,
                progress: Mutex::default(),
            },
            read_engine,
            node_id,
            gate: Mutex::new(()),
        }
    }

    /// One collection pass; passes never overlap. Releases are flushed first;
    /// copies are retired only when Meta's batch waits on this node for a
    /// newer generation, and completion is acknowledged with an empty
    /// inventory. Returns the number of chunks retired.
    pub(crate) fn collect_once(&self) -> Result<usize> {
        let _gate = self
            .gate
            .lock()
            .map_err(|_| Error::coded(afs_error::NODE_VFS_UNAVAILABLE, "GC worker poisoned"))?;
        self.references.flush_for_collection()?;
        match &self.target {
            CollectTarget::Local(store) => self.collect_local(store),
            CollectTarget::External {
                store,
                list_pages_per_pass,
                progress,
            } => self.collect_external(store.as_ref(), *list_pages_per_pass, progress),
        }
    }

    /// Resolves when dropped pins should be released before the next pass.
    pub(crate) async fn release_wanted(&self) {
        self.references.release_wanted().await;
    }

    /// Releases only: a wake from the dropped-pin backlog, or a pass with
    /// collection switched off (`fault::experiment`).
    pub(crate) fn flush_references(&self) -> Result<usize> {
        let _gate = self
            .gate
            .lock()
            .map_err(|_| Error::coded(afs_error::NODE_VFS_UNAVAILABLE, "GC worker poisoned"))?;
        // One batch per wake (R107); the backlog drains on collection passes.
        self.references.release_batch()?;
        Ok(0)
    }

    fn collect_local(&self, store: &LocalChunkStore) -> Result<usize> {
        let completed = store.gc_generation()?;
        let batch = self
            .references
            .control
            .collect(completed, store.gc_inventory()?)?;
        if batch.waiting_nodes.iter().any(|node| node == &self.node_id)
            && batch.generation > completed
        {
            self.read_engine.evict_collected(&batch.chunks)?;
            store
                .lifecycle()
                .retire_generation(batch.generation, &batch.chunks)?;
            self.references
                .control
                .collect(batch.generation, Vec::new())?;
            return Ok(batch.chunks.len());
        }
        Ok(0)
    }

    /// Meta names one lister and one deleter. A listed page is reported as
    /// inventory, so objects Meta never registered reach the same plan; the
    /// cursor only moves once Meta has taken the page.
    fn collect_external(
        &self,
        store: &dyn ExternalObjects,
        list_pages: usize,
        progress: &Mutex<ExternalProgress>,
    ) -> Result<usize> {
        let mut progress = progress
            .lock()
            .map_err(|_| Error::coded(afs_error::NODE_VFS_UNAVAILABLE, "GC worker poisoned"))?;
        let mut state = None;
        for _ in 0..list_pages.max(1) {
            let (inventory, after) = if progress.listing {
                store.list_objects(progress.after.clone(), EXTERNAL_LIST_PAGE)?
            } else {
                (Vec::new(), progress.after.clone())
            };
            let batch = self
                .references
                .control
                .collect_batch(progress.completed, inventory)?;
            // An exhausted listing (`after` is None) starts over next time.
            progress.after = after;
            progress.listing = batch.list_external_inventory;
            state = Some(batch.state);
            if !progress.listing {
                break;
            }
        }
        let Some(batch) = state else {
            return Ok(0);
        };
        if batch.waiting_nodes.iter().any(|node| node == &self.node_id)
            && batch.generation > progress.completed
        {
            self.read_engine.evict_collected(&batch.chunks)?;
            // On failure nothing is acknowledged; the next pass retries and
            // Meta's generation age shows the stall.
            store.delete_objects(&batch.chunks)?;
            self.references
                .control
                .collect_batch(batch.generation, Vec::new())?;
            progress.completed = batch.generation;
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
            gc::{GcBatch, GcControl},
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
        fn retain(&self, _: &str, _: &InodeId, _: Option<&FileVersionId>) -> Result<()> {
            Ok(())
        }
        fn release(&self, _: &[String]) -> Result<()> {
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

    /// 假对象仓 + 按脚本回复的 Meta：覆盖 S3 回收的一轮。
    #[derive(Default)]
    struct FakeObjects {
        pages: Mutex<std::collections::VecDeque<Vec<ChunkObject>>>,
        list_calls: Mutex<Vec<Option<String>>>,
        deleted: Mutex<Vec<ChunkId>>,
        fail_delete: std::sync::atomic::AtomicBool,
    }

    impl ExternalObjects for FakeObjects {
        fn delete_objects(&self, chunks: &[ChunkId]) -> Result<()> {
            if self
                .fail_delete
                .swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                return Err(Error::coded(afs_error::IO_UNAVAILABLE, "injected"));
            }
            self.deleted.lock().unwrap().extend_from_slice(chunks);
            Ok(())
        }
        fn list_objects(
            &self,
            after: Option<String>,
            _: usize,
        ) -> Result<(Vec<ChunkObject>, Option<String>)> {
            self.list_calls.lock().unwrap().push(after);
            let page = self.pages.lock().unwrap().pop_front().unwrap_or_default();
            let last = page.last().map(|chunk| chunk.id.0.clone());
            Ok((page, last))
        }
    }

    #[derive(Default)]
    struct ScriptedMeta {
        replies: Mutex<std::collections::VecDeque<GcBatch>>,
        calls: Mutex<Vec<(u64, usize)>>,
    }

    impl GcControl for ScriptedMeta {
        fn retain(&self, _: &str, _: &InodeId, _: Option<&FileVersionId>) -> Result<()> {
            Ok(())
        }
        fn release(&self, _: &[String]) -> Result<()> {
            Ok(())
        }
        fn collect(&self, completed: u64, inventory: Vec<ChunkObject>) -> Result<GcState> {
            self.collect_batch(completed, inventory).map(|b| b.state)
        }
        fn collect_batch(&self, completed: u64, inventory: Vec<ChunkObject>) -> Result<GcBatch> {
            self.calls
                .lock()
                .unwrap()
                .push((completed, inventory.len()));
            Ok(self.replies.lock().unwrap().pop_front().unwrap_or(GcBatch {
                state: GcState::default(),
                list_external_inventory: false,
            }))
        }
    }

    fn external_collector(
        objects: Arc<FakeObjects>,
        meta: Arc<ScriptedMeta>,
    ) -> (tempfile::TempDir, Collector) {
        let temp = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalChunkStore::open(temp.path(), "n").unwrap());
        let engine = Arc::new(DfsReadEngine::new(
            NamespaceId::new("ns"),
            "n".into(),
            store,
            Arc::new(UnimplementedReadSourceProvider),
            Arc::new(UnimplementedChunkTransfer),
            DfsReadConfig::default(),
        ));
        let collector =
            Collector::new_external(GcReferences::new(meta), objects, engine, "n".into(), 1);
        (temp, collector)
    }

    fn waiting_batch(generation: u64, chunk: &str) -> GcBatch {
        GcBatch {
            state: GcState {
                generation,
                chunks: vec![ChunkId::new(chunk)],
                waiting_nodes: vec!["n".into()],
            },
            list_external_inventory: false,
        }
    }

    #[test]
    fn external_pass_deletes_then_acknowledges_and_retries_a_failed_delete() {
        let objects = Arc::new(FakeObjects::default());
        let meta = Arc::new(ScriptedMeta::default());
        let (_temp, collector) = external_collector(objects.clone(), meta.clone());
        objects
            .fail_delete
            .store(true, std::sync::atomic::Ordering::SeqCst);
        meta.replies
            .lock()
            .unwrap()
            .push_back(waiting_batch(3, "a"));
        assert!(collector.collect_once().is_err());
        assert_eq!(
            *meta.calls.lock().unwrap(),
            vec![(0, 0)],
            "no acknowledgement"
        );
        assert!(objects.deleted.lock().unwrap().is_empty());

        meta.replies
            .lock()
            .unwrap()
            .push_back(waiting_batch(3, "a"));
        assert_eq!(collector.collect_once().unwrap(), 1);
        assert_eq!(*meta.calls.lock().unwrap(), vec![(0, 0), (0, 0), (3, 0)]);
        assert_eq!(*objects.deleted.lock().unwrap(), vec![ChunkId::new("a")]);
        // The next pass reports the finished generation.
        collector.collect_once().unwrap();
        assert_eq!(meta.calls.lock().unwrap().last(), Some(&(3, 0)));
    }

    #[test]
    fn external_listing_runs_only_when_assigned_and_moves_its_cursor_forward() {
        let chunk = |name: &[u8]| {
            StagedChunk::new_generational(OperationId::new("op"), name.to_vec()).chunk
        };
        let objects = Arc::new(FakeObjects::default());
        let meta = Arc::new(ScriptedMeta::default());
        let (_temp, collector) = external_collector(objects.clone(), meta.clone());
        let (first, second) = (chunk(b"one"), chunk(b"two"));
        let second_id = second.id.0.clone();
        objects.pages.lock().unwrap().push_back(vec![first.clone()]);
        objects.pages.lock().unwrap().push_back(vec![second]);
        let assign = || GcBatch {
            state: GcState::default(),
            list_external_inventory: true,
        };
        meta.replies
            .lock()
            .unwrap()
            .extend([assign(), assign(), assign()]);
        collector.collect_once().unwrap();
        assert!(
            objects.list_calls.lock().unwrap().is_empty(),
            "not assigned yet"
        );
        collector.collect_once().unwrap();
        collector.collect_once().unwrap();
        collector.collect_once().unwrap();
        assert_eq!(
            *objects.list_calls.lock().unwrap(),
            vec![None, Some(first.id.0.clone()), Some(second_id)]
        );
        assert_eq!(
            *meta.calls.lock().unwrap(),
            vec![(0, 0), (0, 1), (0, 1), (0, 0)]
        );
    }
}
