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
    /// Retires every id in one call. Failure means none is known retired: the
    /// caller keeps them all and retries, which is idempotent.
    fn release(&self, ids: &[String]) -> Result<()>;
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

/// Ids per release call. Meta accepts at most `meta::dfs::MAX_RELEASE_BATCH`.
const RELEASE_BATCH: usize = 1024;
/// A dropped pin wakes the release loop once this many wait, so release keeps up
/// with any open rate instead of waiting for the next collection pass.
const RELEASE_WAKE_PENDING: usize = 256;
/// Backpressure: a new pin first drains the queue when this many wait. Dropping a
/// pin never blocks and never loses its id, so the bound is enforced where pins
/// are made; while Meta cannot take releases it cannot admit new pins either.
const RELEASE_MAX_PENDING: usize = 65_536;

pub struct GcReferences {
    pub control: Arc<dyn GcControl>,
    pending: Mutex<Vec<String>>,
    sequence: AtomicU64,
    metrics: Option<GcReleaseMetrics>,
    wake: tokio::sync::Notify,
    /// Live `open:` pins by inode. A reference protects every version of its
    /// inode until its release, and its release waits for the last user of the
    /// pin, so another open of the same inode shares it instead of a new
    /// admission. A finished pin leaves on drop, keeping this bounded by open
    /// inodes; in a racing open the map may briefly miss a live pin, which only
    /// costs one more reference.
    open_pins: Mutex<std::collections::HashMap<InodeId, std::sync::Weak<GcPin>>>,
}
pub struct GcPin {
    fence: Option<crate::dfs::GcFence>,
    id: String,
    owner: Arc<GcReferences>,
    /// The inode it is shared under; only `open:` pins are shared.
    open_inode: Option<InodeId>,
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
            if pending.len() == RELEASE_WAKE_PENDING {
                self.owner.wake.notify_one();
            }
        }
        if let (Some(inode), Ok(mut open_pins)) = (&self.open_inode, self.owner.open_pins.lock())
            && open_pins
                .get(inode)
                .is_some_and(|pin| pin.strong_count() == 0)
        {
            open_pins.remove(inode);
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
            wake: tokio::sync::Notify::new(),
            open_pins: Mutex::default(),
        })
    }
    /// Resolves when enough dropped pins wait that the release loop should not
    /// sleep until its next pass.
    pub(crate) async fn release_wanted(&self) {
        self.wake.notified().await;
    }
    fn observe_pending(&self, pending: usize) {
        if let Some(metrics) = &self.metrics {
            metrics
                .pending
                .set(i64::try_from(pending).unwrap_or(i64::MAX));
        }
    }
    /// An `open:` pin of `inode`, shared with any live pin of the same inode.
    pub fn pin(
        self: &Arc<Self>,
        inode: &InodeId,
        version: Option<&FileVersionId>,
    ) -> Result<Arc<GcPin>> {
        if let Some(shared) = self.shared_open_pin(inode)? {
            return Ok(shared);
        }
        let pin = self.pin_mode(inode, version, None)?;
        self.share_open_pin(inode, &pin)?;
        Ok(pin)
    }
    /// The pin of a handle whose reference Meta admits inside the Create
    /// transaction (`dfs::created_handle_reference_id`): no admission call here.
    pub(crate) fn created_handle_pin(
        self: &Arc<Self>,
        operation: &crate::dfs::OperationId,
    ) -> Arc<GcPin> {
        Arc::new(GcPin {
            fence: None,
            id: crate::dfs::created_handle_reference_id(operation),
            owner: self.clone(),
            open_inode: None,
        })
    }
    fn shared_open_pin(&self, inode: &InodeId) -> Result<Option<Arc<GcPin>>> {
        Ok(self
            .open_pins
            .lock()
            .map_err(|_| Error::coded(afs_error::IO_UNAVAILABLE, "GC pin table poisoned"))?
            .get(inode)
            .and_then(std::sync::Weak::upgrade))
    }
    fn share_open_pin(&self, inode: &InodeId, pin: &Arc<GcPin>) -> Result<()> {
        self.open_pins
            .lock()
            .map_err(|_| Error::coded(afs_error::IO_UNAVAILABLE, "GC pin table poisoned"))?
            .insert(inode.clone(), Arc::downgrade(pin));
        Ok(())
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
        if self.pending_len()? >= RELEASE_MAX_PENDING {
            self.flush()?;
        }
        // Queue cleanup even when an admission reply is lost.
        let mut pin = GcPin {
            fence: None,
            id,
            owner: self.clone(),
            open_inode: generation.is_none().then(|| inode.clone()),
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
    fn pending_len(&self) -> Result<usize> {
        Ok(self
            .pending
            .lock()
            .map_err(|_| Error::coded(afs_error::IO_UNAVAILABLE, "GC release queue poisoned"))?
            .len())
    }
    /// Releases everything dropped so far, a batch at a time. On failure the
    /// unconfirmed ids return to the queue: a lost release only protects longer.
    pub fn flush(&self) -> Result<()> {
        loop {
            let batch = {
                let mut queue = self.pending.lock().map_err(|_| {
                    Error::coded(afs_error::IO_UNAVAILABLE, "GC release queue poisoned")
                })?;
                let count = queue.len().min(RELEASE_BATCH);
                let batch = queue.drain(..count).collect::<Vec<_>>();
                self.observe_pending(queue.len());
                batch
            };
            if batch.is_empty() {
                return Ok(());
            }
            if let Err(error) = self.control.release(&batch) {
                let mut queue = self.pending.lock().map_err(|_| {
                    Error::coded(afs_error::IO_UNAVAILABLE, "GC release queue poisoned")
                })?;
                queue.extend(batch);
                self.observe_pending(queue.len());
                if let Some(metrics) = &self.metrics {
                    metrics.failures.inc();
                }
                return Err(error);
            }
            if let Some(metrics) = &self.metrics {
                metrics.released.inc_by(batch.len() as u64);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[derive(Default)]
    struct Control {
        releases: Mutex<Vec<String>>,
        batches: AtomicU64,
        fail_once: std::sync::atomic::AtomicBool,
    }
    impl GcControl for Control {
        fn retain(&self, _: &str, _: &InodeId, _: Option<&FileVersionId>) -> Result<()> {
            Ok(())
        }
        fn release(&self, ids: &[String]) -> Result<()> {
            if self.fail_once.swap(false, Ordering::Relaxed) {
                return Err(Error::coded(afs_error::IO_UNAVAILABLE, "lost reply"));
            }
            self.batches.fetch_add(1, Ordering::Relaxed);
            self.releases.lock().unwrap().extend_from_slice(ids);
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
    fn opens_of_one_inode_share_a_reference_until_the_last_is_gone() {
        let control = Arc::new(Control::default());
        let references = GcReferences::new(control.clone());
        let first = references.pin(&InodeId::new("i"), None).unwrap();
        let second = references.pin(&InodeId::new("i"), None).unwrap();
        assert!(Arc::ptr_eq(&first, &second), "no second admission");
        let other = references.pin(&InodeId::new("other"), None).unwrap();
        assert!(!Arc::ptr_eq(&first, &other));
        drop(first);
        references.flush().unwrap();
        assert!(
            control.releases.lock().unwrap().is_empty(),
            "a remaining handle keeps the reference unreleased"
        );
        drop(second);
        references.flush().unwrap();
        assert_eq!(control.releases.lock().unwrap().len(), 1);
        // Released means a later open needs a new admission, not the old id.
        let third = references.pin(&InodeId::new("i"), None).unwrap();
        drop(third);
        references.flush().unwrap();
        let released = control.releases.lock().unwrap().clone();
        assert_eq!(released.len(), 2);
        assert_ne!(released[0], released[1]);
        drop(other);
        assert!(references.open_pins.lock().unwrap().is_empty());
    }
    #[test]
    fn a_created_handle_adopts_its_reference_and_releases_it() {
        let control = Arc::new(Control::default());
        let references = GcReferences::new(control.clone());
        let operation = crate::dfs::OperationId::new("op");
        drop(references.created_handle_pin(&operation));
        references.flush().unwrap();
        assert_eq!(
            *control.releases.lock().unwrap(),
            vec![crate::dfs::created_handle_reference_id(&operation)]
        );
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
    fn one_flush_drains_any_backlog_in_batches_without_losing_ids() {
        let control = Arc::new(Control::default());
        let references = GcReferences::new(control.clone());
        let total = RELEASE_BATCH * 2 + 3;
        for _ in 0..total {
            drop(references.pin(&InodeId::new("i"), None).unwrap());
        }
        references.flush().unwrap();
        let released = control.releases.lock().unwrap().clone();
        assert_eq!(released.len(), total);
        assert_eq!(
            released
                .iter()
                .collect::<std::collections::BTreeSet<_>>()
                .len(),
            total
        );
        assert_eq!(control.batches.load(Ordering::Relaxed), 3);
        references.flush().unwrap();
        assert_eq!(control.batches.load(Ordering::Relaxed), 3, "nothing left");
    }
    #[test]
    fn a_failed_batch_keeps_every_id_for_the_retry() {
        let control = Arc::new(Control::default());
        let references = GcReferences::new(control.clone());
        for _ in 0..5 {
            drop(references.pin(&InodeId::new("i"), None).unwrap());
        }
        control.fail_once.store(true, Ordering::Relaxed);
        assert!(references.flush().is_err());
        assert!(control.releases.lock().unwrap().is_empty());
        assert_eq!(references.pending_len().unwrap(), 5);
        references.flush().unwrap();
        assert_eq!(control.releases.lock().unwrap().len(), 5);
    }
    #[tokio::test]
    async fn enough_dropped_pins_wake_the_release_loop() {
        let references = GcReferences::new(Arc::new(Control::default()));
        let waiter = references.release_wanted();
        tokio::pin!(waiter);
        for _ in 0..RELEASE_WAKE_PENDING - 1 {
            drop(references.pin(&InodeId::new("i"), None).unwrap());
        }
        let short = std::time::Duration::from_millis(20);
        assert!(tokio::time::timeout(short, waiter.as_mut()).await.is_err());
        drop(references.pin(&InodeId::new("i"), None).unwrap());
        assert!(tokio::time::timeout(short, waiter.as_mut()).await.is_ok());
    }
    #[test]
    fn a_full_queue_makes_the_next_pin_drain_it_first() {
        let control = Arc::new(Control::default());
        let references = GcReferences::new(control.clone());
        references
            .pending
            .lock()
            .unwrap()
            .extend((0..RELEASE_MAX_PENDING).map(|index| format!("open:{index}")));
        let pin = references.pin(&InodeId::new("i"), None).unwrap();
        assert_eq!(references.pending_len().unwrap(), 0);
        assert_eq!(control.releases.lock().unwrap().len(), RELEASE_MAX_PENDING);
        drop(pin);
        // A Meta that cannot take releases refuses the new pin, not the old ids.
        references
            .pending
            .lock()
            .unwrap()
            .extend((0..RELEASE_MAX_PENDING).map(|index| format!("open:{index}")));
        control.fail_once.store(true, Ordering::Relaxed);
        assert!(references.pin(&InodeId::new("i"), None).is_err());
        assert_eq!(references.pending_len().unwrap(), RELEASE_MAX_PENDING + 1);
    }
}
