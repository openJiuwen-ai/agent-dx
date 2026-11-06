//! GC reference pins. References outlive in-flight handle users; drops
//! enqueue durable release retries.
use crate::dfs::{ChunkObject, FileVersionId, GcState, InodeId};
use afs_error::{Error, Result};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};

pub trait GcControl: Send + Sync {
    /// Unfenced read pin (`open:`); write generations use `retain_fenced`.
    fn retain(&self, id: &str, inode: &InodeId, version: Option<&FileVersionId>) -> Result<()>;
    /// Fails closed if the authority cannot capture a pre-write GC fence.
    fn retain_fenced(
        &self,
        _id: &str,
        _inode: &InodeId,
        _version: Option<&FileVersionId>,
    ) -> Result<crate::dfs::GcFence> {
        Err(Error::coded(
            afs_error::NODE_VFS_UNIMPLEMENTED,
            "fenced GC retain is unsupported",
        ))
    }
    fn release(&self, id: &str) -> Result<()>;
    fn collect(&self, completed: u64, inventory: Vec<ChunkObject>) -> Result<GcState>;
    /// `collect` plus the assignment to list the external object store.
    fn collect_batch(&self, completed: u64, inventory: Vec<ChunkObject>) -> Result<GcBatch> {
        Ok(GcBatch {
            state: self.collect(completed, inventory)?,
            list_external_inventory: false,
        })
    }
}

/// One collection reply.
pub struct GcBatch {
    pub state: GcState,
    pub list_external_inventory: bool,
}
/// Release backlog as operators see it. Registered once per Node.
#[derive(Clone)]
pub(crate) struct GcReleaseMetrics {
    pending: afs_metrics::IntGauge,
    released: afs_metrics::IntCounter,
    failures: afs_metrics::IntCounter,
}

impl GcReleaseMetrics {
    pub(crate) fn register(
        registry: &afs_metrics::Registry,
    ) -> std::result::Result<Self, afs_metrics::MetricsError> {
        let metrics = Self {
            pending: afs_metrics::IntGauge::new(
                "afs_dfs_gc_release_pending",
                "GC references dropped by this Node and not yet released at Meta",
            )?,
            released: afs_metrics::IntCounter::new(
                "afs_dfs_gc_released_total",
                "GC references this Node released at Meta",
            )?,
            failures: afs_metrics::IntCounter::new(
                "afs_dfs_gc_release_failures_total",
                "GC release calls that failed and stay queued",
            )?,
        };
        afs_metrics::register_collector(registry, &metrics.pending)?;
        afs_metrics::register_collector(registry, &metrics.released)?;
        afs_metrics::register_collector(registry, &metrics.failures)?;
        Ok(metrics)
    }
}

pub struct GcReferences {
    pub control: Arc<dyn GcControl>,
    pending: Mutex<Vec<String>>,
    sequence: AtomicU64,
    metrics: Option<GcReleaseMetrics>,
}
pub struct GcPin {
    fence: Option<crate::dfs::GcFence>,
    id: String,
    owner: Arc<GcReferences>,
}
impl GcPin {
    pub(crate) fn fence(&self) -> Option<crate::dfs::GcFence> {
        self.fence
    }
    /// Recover the immutable operation admitted by this generation pin.
    pub(crate) fn write_operation_id(&self) -> Option<crate::dfs::OperationId> {
        self.id
            .strip_prefix("write:")
            .map(crate::dfs::OperationId::new)
    }
}
impl Drop for GcPin {
    fn drop(&mut self) {
        if let Ok(mut pending) = self.owner.pending.lock() {
            pending.push(self.id.clone());
            self.owner.observe_pending(pending.len());
        }
        // Poisoning keeps the durable reference; it never authorizes deletion.
    }
}
impl GcReferences {
    pub fn new(control: Arc<dyn GcControl>) -> Arc<Self> {
        Self::build(control, None)
    }
    pub(crate) fn with_metrics(
        control: Arc<dyn GcControl>,
        metrics: GcReleaseMetrics,
    ) -> Arc<Self> {
        Self::build(control, Some(metrics))
    }
    fn build(control: Arc<dyn GcControl>, metrics: Option<GcReleaseMetrics>) -> Arc<Self> {
        Arc::new(Self {
            control,
            pending: Mutex::new(Vec::new()),
            sequence: AtomicU64::new(1),
            metrics,
        })
    }
    fn observe_pending(&self, pending: usize) {
        if let Some(metrics) = &self.metrics {
            metrics
                .pending
                .set(i64::try_from(pending).unwrap_or(i64::MAX));
        }
    }
    pub fn pin(
        self: &Arc<Self>,
        inode: &InodeId,
        version: Option<&FileVersionId>,
    ) -> Result<Arc<GcPin>> {
        self.pin_mode(inode, version, None)
    }
    pub(crate) fn pin_generation(
        self: &Arc<Self>,
        inode: &InodeId,
        version: Option<&FileVersionId>,
        operation: &crate::dfs::OperationId,
    ) -> Result<Arc<GcPin>> {
        self.pin_mode(inode, version, Some(operation))
    }
    /// `generation` selects a fenced `write:` pin; `None` is an `open:` pin.
    fn pin_mode(
        self: &Arc<Self>,
        inode: &InodeId,
        version: Option<&FileVersionId>,
        generation: Option<&crate::dfs::OperationId>,
    ) -> Result<Arc<GcPin>> {
        let id = match generation {
            Some(operation) => format!("write:{}", operation.0),
            None => format!(
                "open:{}",
                crate::clock::timed_request_id(format!(
                    "{}-{}",
                    std::process::id(),
                    self.sequence.fetch_add(1, Ordering::Relaxed)
                ))
            ),
        };
        // Queue cleanup even when an admission reply is lost.
        let mut pin = GcPin {
            fence: None,
            id,
            owner: self.clone(),
        };
        pin.fence = if generation.is_some() {
            Some(self.control.retain_fenced(&pin.id, inode, version)?)
        } else {
            self.control.retain(&pin.id, inode, version)?;
            None
        };
        if generation.is_some() && pin.fence != Some(crate::dfs::GcFence::WriteGeneration) {
            return Err(Error::coded(
                afs_error::NODE_VFS_UNIMPLEMENTED,
                "Meta did not admit the write generation",
            ));
        }
        Ok(Arc::new(pin))
    }
    pub fn flush(&self) -> Result<()> {
        let pending = {
            let mut queue = self.pending.lock().map_err(|_| {
                Error::coded(afs_error::IO_UNAVAILABLE, "GC release queue poisoned")
            })?;
            let count = queue.len().min(64);
            let drained = queue.drain(..count).collect::<Vec<_>>();
            self.observe_pending(queue.len());
            drained
        };
        for (index, id) in pending.iter().enumerate() {
            if let Err(error) = self.control.release(id) {
                let mut queue = self.pending.lock().map_err(|_| {
                    Error::coded(afs_error::IO_UNAVAILABLE, "GC release queue poisoned")
                })?;
                queue.extend(pending[index..].iter().cloned());
                self.observe_pending(queue.len());
                if let Some(metrics) = &self.metrics {
                    metrics.failures.inc();
                }
                return Err(error);
            }
            if let Some(metrics) = &self.metrics {
                metrics.released.inc();
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[derive(Default)]
    struct Control {
        releases: Mutex<Vec<String>>,
        fail_once: std::sync::atomic::AtomicBool,
    }
    impl GcControl for Control {
        fn retain(&self, _: &str, _: &InodeId, _: Option<&FileVersionId>) -> Result<()> {
            Ok(())
        }
        fn release(&self, id: &str) -> Result<()> {
            if self.fail_once.swap(false, Ordering::Relaxed) {
                return Err(Error::coded(afs_error::IO_UNAVAILABLE, "lost reply"));
            }
            self.releases.lock().unwrap().push(id.into());
            Ok(())
        }
        fn collect(&self, _: u64, _: Vec<ChunkObject>) -> Result<GcState> {
            Ok(GcState::default())
        }
    }
    #[test]
    fn in_flight_access_retains_pin_and_failed_release_is_retried() {
        let control = Arc::new(Control::default());
        let references = GcReferences::new(control.clone());
        let handle = references.pin(&InodeId::new("i"), None).unwrap();
        let in_flight = handle.clone();
        drop(handle);
        references.flush().unwrap();
        assert!(control.releases.lock().unwrap().is_empty());
        drop(in_flight);
        control.fail_once.store(true, Ordering::Relaxed);
        assert!(references.flush().is_err());
        assert!(control.releases.lock().unwrap().is_empty());
        references.flush().unwrap();
        assert_eq!(control.releases.lock().unwrap().len(), 1);
        references.flush().unwrap();
        assert_eq!(control.releases.lock().unwrap().len(), 1);
    }
    #[test]
    fn release_backlog_is_visible_as_metrics() {
        let registry = afs_metrics::registry();
        let metrics = GcReleaseMetrics::register(&registry).unwrap();
        let control = Arc::new(Control::default());
        let references = GcReferences::with_metrics(control.clone(), metrics.clone());
        for _ in 0..3 {
            drop(references.pin(&InodeId::new("i"), None).unwrap());
        }
        assert_eq!(metrics.pending.get(), 3);
        references.flush().unwrap();
        assert_eq!(metrics.pending.get(), 0);
        assert_eq!(metrics.released.get(), 3);
    }
    #[test]
    fn release_work_is_bounded_without_losing_queued_handles() {
        let control = Arc::new(Control::default());
        let references = GcReferences::new(control.clone());
        for _ in 0..130 {
            drop(references.pin(&InodeId::new("i"), None).unwrap());
        }
        references.flush().unwrap();
        assert_eq!(control.releases.lock().unwrap().len(), 64);
        references.flush().unwrap();
        references.flush().unwrap();
        assert_eq!(control.releases.lock().unwrap().len(), 130);
    }
}
