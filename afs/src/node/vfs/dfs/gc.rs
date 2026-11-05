use super::*;

/// GC reference pins of this filesystem. Handles, directory handles, writes
/// and commits all pin through `references`; collection passes run in
/// `node::gc::Collector`, driven by the fault layer's reaper.
#[derive(Default)]
pub(super) struct GcCoordinator {
    pub(super) references: Option<Arc<crate::node::gc::GcReferences>>,
}

impl GcCoordinator {
    pub(super) fn flush(&self) -> Result<()> {
        if let Some(references) = &self.references {
            references.flush()?;
        }
        Ok(())
    }
}

impl DistributedFs {
    pub fn flush_gc_releases(&self) -> Result<()> {
        self.gc.flush()
    }
}
