use super::*;

/// GC reference pins and the gate that serialises local collection passes.
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
        references.flush()?;
        let completed = read_engine.gc_generation()?;
        let batch = references
            .control
            .collect(completed, read_engine.gc_inventory()?)?;
        if batch.waiting_nodes.iter().any(|node| node == node_id) && batch.generation > completed {
            read_engine.collect_generation(batch.generation, &batch.chunks)?;
            references.control.collect(batch.generation, Vec::new())?;
            return Ok(batch.chunks.len());
        }
        Ok(0)
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
