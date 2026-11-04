use super::*;

/// GC reference pins and the gate that serialises local collection passes.
/// The pass itself is the fault layer's reaper (`node::fault::reaper`).
/// Handles, directory handles, writes and commits all pin through
/// `references`; `gate` only orders `collect` against itself.
#[derive(Default)]
pub(super) struct GcCoordinator {
    pub(super) references: Option<Arc<crate::node::gc::GcReferences>>,
    gate: Mutex<()>,
}

impl GcCoordinator {
    pub(super) fn flush(&self) -> Result<()> {
        if let Some(references) = &self.references {
            references.flush()?;
        }
        Ok(())
    }

    fn collect(&self, read_engine: &DfsReadEngine, node_id: &str) -> Result<usize> {
        let Some(references) = &self.references else {
            return Ok(0);
        };
        let _gate = self
            .gate
            .lock()
            .map_err(|_| unavailable("GC worker poisoned"))?;
        crate::node::fault::reaper::collect_once(references, read_engine, node_id)
    }
}

impl DistributedFs {
    pub fn flush_gc_releases(&self) -> Result<()> {
        self.gc.flush()
    }

    pub fn collect_garbage(&self) -> Result<usize> {
        self.gc.collect(&self.read_engine, &self.identity.node_id)
    }
}
