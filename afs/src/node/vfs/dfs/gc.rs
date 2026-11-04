use super::*;

impl DistributedFs {
    pub fn flush_gc_releases(&self) -> Result<()> {
        if let Some(references) = &self.gc_references {
            references.flush()?;
        }
        Ok(())
    }
    pub fn collect_garbage(&self) -> Result<usize> {
        let Some(references) = &self.gc_references else {
            return Ok(0);
        };
        let _gate = self
            .gc_gate
            .lock()
            .map_err(|_| unavailable("GC worker poisoned"))?;
        references.flush()?;
        let completed = self.read_engine.gc_generation()?;
        let batch = references
            .control
            .collect(completed, self.read_engine.gc_inventory()?)?;
        if batch.waiting_nodes.contains(&self.node_id) && batch.generation > completed {
            self.read_engine
                .collect_generation(batch.generation, &batch.chunks)?;
            references.control.collect(batch.generation, Vec::new())?;
            return Ok(batch.chunks.len());
        }
        Ok(0)
    }
}
