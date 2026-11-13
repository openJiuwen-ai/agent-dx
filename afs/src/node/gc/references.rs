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
    /// Round 2 C-1: registers reader `reader` of `version` (the head the
    /// Node saw) in Meta's memory, without a log record, and returns the
    /// replying Meta's writer term. `META_DFS_READER_NOT_HEAD` (and this
    /// default) means: pin it durably with `retain` instead.
    fn retain_reader(
        &self,
        _reader: u64,
        _inode: &InodeId,
        _version: Option<&FileVersionId>,
    ) -> Result<u64> {
        Err(Error::coded(
            afs_error::NODE_VFS_UNIMPLEMENTED,
            "in-memory reader registration is unsupported",
        ))
    }
    /// Drops closed readers at Meta. A lost call only protects longer.
    fn release_readers(&self, _readers: &[u64]) -> Result<()> {
        Ok(())
    }
    /// Replaces this session's readers up to `low_watermark` at Meta;
    /// `term` is the writer term the Node believes current. Returns the
    /// replying Meta's term.
    fn report_readers(
        &self,
        _term: u64,
        _low_watermark: u64,
        _readers: &[SoftReader],
    ) -> Result<u64> {
        Ok(0)
    }
    /// `collect` plus the assignment to list the external object store.
    fn collect_batch(&self, completed: u64, inventory: Vec<ChunkObject>) -> Result<GcBatch> {
        Ok(GcBatch {
            state: self.collect(completed, inventory)?,
            list_external_inventory: false,
        })
    }
}

/// One reader as the Node reports it to Meta (C-1).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SoftReader {
    pub reader: u64,
    pub inode: InodeId,
    pub version: Option<FileVersionId>,
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
    /// `open:` pins made, by how Meta keeps them: `soft` (in memory, C-1)
    /// or `durable` (a logged `UpdateGcReference`).
    reader_references: afs_metrics::IntCounterVec,
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
            reader_references: afs_metrics::IntCounterVec::new(
                afs_metrics::Opts::new(
                    "afs_dfs_reader_references_total",
                    "Open pins made by this Node, by kind: soft (Meta memory) or durable (Meta log)",
                ),
                &["kind"],
            )?,
        };
        afs_metrics::register_collector(registry, &metrics.pending)?;
        afs_metrics::register_collector(registry, &metrics.released)?;
        afs_metrics::register_collector(registry, &metrics.failures)?;
        afs_metrics::register_collector(registry, &metrics.reader_references)?;
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
    /// Live `open:` pins by inode and opened version. A reference keeps its
    /// inode, its head and the one version it names until its release (R81
    /// F2), and its release waits for the last user of the pin, so another open
    /// of the same version shares it instead of a new admission; an open of
    /// another version needs its own. A finished pin leaves on drop, keeping
    /// this bounded by open versions; in a racing open the map may briefly miss
    /// a live pin, which only costs one more reference.
    open_pins: Mutex<std::collections::HashMap<OpenPinKey, std::sync::Weak<GcPin>>>,
    /// In-memory reader registrations of this session (C-1).
    soft: Mutex<SoftReaders>,
}

/// Retain attempts of one soft registration (term drops, a report first)
/// before the pin falls back to a durable reference.
const SOFT_RETAIN_ATTEMPTS: usize = 4;

/// The Node side of C-1 (design §3.3). `reader` numbers increase; a reader
/// is "in flight" from its number until its reply is handled. A report sends
/// every live reader up to the low watermark, the highest number below every
/// in-flight one, so Meta can replace exactly the readers whose fate the
/// Node knows (L3).
#[derive(Debug)]
struct SoftReaders {
    next: u64,
    in_flight: std::collections::BTreeSet<u64>,
    live: std::collections::BTreeMap<u64, (InodeId, Option<FileVersionId>)>,
    /// Highest Meta writer term seen. A reply of a lower term comes from a
    /// replaced Meta and is dropped (L6).
    seen_term: u64,
    /// Closed readers not yet released at Meta.
    released: Vec<u64>,
    /// The control has no soft registration: every pin is durable.
    unsupported: bool,
}

impl Default for SoftReaders {
    fn default() -> Self {
        Self {
            next: 1,
            in_flight: std::collections::BTreeSet::new(),
            live: std::collections::BTreeMap::new(),
            seen_term: 0,
            released: Vec::new(),
            unsupported: false,
        }
    }
}

impl SoftReaders {
    fn low_watermark(&self) -> u64 {
        self.in_flight
            .first()
            .map_or(self.next - 1, |first| first - 1)
    }
}
type OpenPinKey = (InodeId, Option<FileVersionId>);
/// The Meta admission of a deferred `write:` pin (performance batch 1, D).
/// The first attempt runs on a blocking pool thread; `GcPin::admitted`
/// waits for it and, after a failure Meta may still accept, retries once on
/// the caller's thread with the same reference id. At most one attempt is in
/// flight; `None` in `outcome` means one is.
struct DeferredAdmission {
    outcome: Mutex<Option<Result<crate::dfs::GcFence>>>,
    ready: std::sync::Condvar,
    control: Arc<dyn GcControl>,
    inode: InodeId,
    version: Option<FileVersionId>,
}

impl DeferredAdmission {
    fn attempt(&self, id: &str) -> Result<crate::dfs::GcFence> {
        let fence = self
            .control
            .retain_fenced(id, &self.inode, self.version.as_ref())?;
        if fence != crate::dfs::GcFence::WriteGeneration {
            return Err(Error::coded(
                afs_error::NODE_VFS_UNIMPLEMENTED,
                "Meta did not admit the write generation",
            ));
        }
        Ok(fence)
    }

    fn finish(&self, result: Result<crate::dfs::GcFence>) {
        let mut outcome = self.outcome.lock().unwrap_or_else(|e| e.into_inner());
        *outcome = Some(result);
        self.ready.notify_all();
    }
}

/// Runs the first attempt of a deferred admission. If the blocking pool
/// drops the task unrun (runtime shutdown), its drop records a retryable
/// failure so no waiter blocks forever; the next waiter retries itself.
struct AdmissionTask {
    admission: Option<Arc<DeferredAdmission>>,
    id: String,
}

impl AdmissionTask {
    fn run(&mut self) {
        if let Some(admission) = self.admission.take() {
            let result = admission.attempt(&self.id);
            admission.finish(result);
        }
    }
}

impl Drop for AdmissionTask {
    fn drop(&mut self) {
        if let Some(admission) = self.admission.take() {
            admission.finish(Err(Error::coded(
                afs_error::IO_UNAVAILABLE,
                "GC admission task did not run",
            )));
        }
    }
}

/// A failed admission Meta will never accept for this reference id: the
/// write generation can never be published and its writes are rejected.
pub(crate) fn is_permanent_admission_failure(error: &Error) -> bool {
    matches!(
        error.code(),
        afs_error::META_DFS_REFERENCE_ADMISSION_EXPIRED | afs_error::NODE_VFS_UNIMPLEMENTED
    )
}

pub struct GcPin {
    fence: Option<crate::dfs::GcFence>,
    /// Set for a deferred `write:` pin; `fence` is then unused.
    deferred: Option<Arc<DeferredAdmission>>,
    id: String,
    owner: Arc<GcReferences>,
    /// The inode and version it is shared under; only `open:` pins are shared.
    open_inode: Option<OpenPinKey>,
    /// Set for a soft reader (C-1): released with `release_readers`.
    reader: Option<u64>,
}
impl GcPin {
    /// Wait until Meta has durably admitted this pin and return its fence.
    /// Blocks the calling thread: call it only from blocking-pool or
    /// synchronous callers, never from an async worker. Every chunk of a
    /// deferred write generation is persisted only after this succeeds, so
    /// GC can never see such a chunk before its `write:` reference exists.
    /// A failure Meta may still accept is retried once with the same id; a
    /// permanent one ([`is_permanent_admission_failure`]) is returned as is.
    pub(crate) fn admitted(&self) -> Result<crate::dfs::GcFence> {
        let Some(admission) = &self.deferred else {
            return self.fence.ok_or_else(|| {
                Error::coded(
                    afs_error::NODE_VFS_UNIMPLEMENTED,
                    "GC pin holds no write-generation fence",
                )
            });
        };
        let mut outcome = admission
            .outcome
            .lock()
            .map_err(|_| Error::coded(afs_error::IO_UNAVAILABLE, "GC admission poisoned"))?;
        loop {
            match outcome.as_ref() {
                None => {
                    outcome = admission.ready.wait(outcome).map_err(|_| {
                        Error::coded(afs_error::IO_UNAVAILABLE, "GC admission poisoned")
                    })?;
                }
                Some(Ok(fence)) => return Ok(*fence),
                Some(Err(error)) if is_permanent_admission_failure(error) => {
                    return Err(error.clone());
                }
                Some(Err(_)) => {
                    // Claim the retry; other waiters see an attempt in flight.
                    *outcome = None;
                    drop(outcome);
                    let result = admission.attempt(&self.id);
                    admission.finish(result.clone());
                    return result;
                }
            }
        }
    }
    /// The inode and version an `open:` or reader pin protects.
    pub(crate) fn opened(&self) -> Option<(&InodeId, Option<&FileVersionId>)> {
        self.open_inode
            .as_ref()
            .map(|(inode, version)| (inode, version.as_ref()))
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
        if let Some(reader) = self.reader {
            if let Ok(mut soft) = self.owner.soft.lock() {
                soft.live.remove(&reader);
                soft.released.push(reader);
                if soft.released.len() == RELEASE_WAKE_PENDING {
                    self.owner.wake.notify_one();
                }
            }
        } else if let Ok(mut pending) = self.owner.pending.lock() {
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
            soft: Mutex::default(),
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
        let key = (inode.clone(), version.cloned());
        if let Some(shared) = self.shared_open_pin(&key)? {
            return Ok(shared);
        }
        let pin = match self.pin_soft(inode, version)? {
            Some(pin) => pin,
            None => {
                let pin = self.pin_mode(inode, version, None)?;
                self.observe_reader("durable");
                pin
            }
        };
        self.share_open_pin(key, &pin)?;
        Ok(pin)
    }
    fn observe_reader(&self, kind: &str) {
        if let Some(metrics) = &self.metrics {
            metrics.reader_references.with_label_values(&[kind]).inc();
        }
    }
    fn soft(&self) -> Result<std::sync::MutexGuard<'_, SoftReaders>> {
        self.soft
            .lock()
            .map_err(|_| Error::coded(afs_error::IO_UNAVAILABLE, "GC reader table poisoned"))
    }
    /// A soft reader pin of `version`, or `None` when Meta wants it durable
    /// (not the linked head any more, or no soft registration).
    fn pin_soft(
        self: &Arc<Self>,
        inode: &InodeId,
        version: Option<&FileVersionId>,
    ) -> Result<Option<Arc<GcPin>>> {
        let reader = {
            let mut soft = self.soft()?;
            if soft.unsupported {
                return Ok(None);
            }
            let reader = soft.next;
            soft.next += 1;
            soft.in_flight.insert(reader);
            reader
        };
        let outcome = self.retain_soft(reader, inode, version);
        let mut soft = self.soft()?;
        // Live before it leaves flight: a report never misses it (L3).
        if matches!(outcome, Ok(true)) {
            soft.live.insert(reader, (inode.clone(), version.cloned()));
        }
        soft.in_flight.remove(&reader);
        drop(soft);
        if !outcome? {
            return Ok(None);
        }
        self.observe_reader("soft");
        Ok(Some(Arc::new(GcPin {
            fence: None,
            deferred: None,
            id: format!("reader:{reader}"),
            owner: self.clone(),
            open_inode: Some((inode.clone(), version.cloned())),
            reader: Some(reader),
        })))
    }
    /// Round 2 B2: a reader number for a registration another call makes
    /// (`OpenFileRead`), in flight until `finish_reader`. `None`: soft
    /// registration is off.
    pub(crate) fn begin_reader(&self) -> Result<Option<u64>> {
        let mut soft = self.soft()?;
        if soft.unsupported {
            return Ok(None);
        }
        let reader = soft.next;
        soft.next += 1;
        soft.in_flight.insert(reader);
        Ok(Some(reader))
    }
    /// Ends `begin_reader`. `registered`: Meta of `term` registered
    /// `version` of `inode` as this reader. A reply of a term older than one
    /// this Node has seen is dropped (L6): no pin, the caller pins anew.
    pub(crate) fn finish_reader(
        self: &Arc<Self>,
        reader: u64,
        registered: Option<(u64, &InodeId, Option<&FileVersionId>)>,
    ) -> Result<Option<Arc<GcPin>>> {
        let mut soft = self.soft()?;
        let accepted = registered.filter(|(term, _, _)| *term >= soft.seen_term);
        if let Some((term, inode, version)) = accepted {
            soft.seen_term = term;
            soft.live.insert(reader, (inode.clone(), version.cloned()));
        }
        soft.in_flight.remove(&reader);
        drop(soft);
        let Some((_, inode, version)) = accepted else {
            return Ok(None);
        };
        self.observe_reader("soft");
        Ok(Some(Arc::new(GcPin {
            fence: None,
            deferred: None,
            id: format!("reader:{reader}"),
            owner: self.clone(),
            open_inode: Some((inode.clone(), version.cloned())),
            reader: Some(reader),
        })))
    }
    /// `Ok(true)`: Meta of the newest term holds the registration.
    fn retain_soft(
        &self,
        reader: u64,
        inode: &InodeId,
        version: Option<&FileVersionId>,
    ) -> Result<bool> {
        for _ in 0..SOFT_RETAIN_ATTEMPTS {
            match self.control.retain_reader(reader, inode, version) {
                Ok(term) => {
                    let mut soft = self.soft()?;
                    if term < soft.seen_term {
                        // L6: a replaced Meta answered; its memory protects
                        // nothing the newer one knows about. Ask again.
                        continue;
                    }
                    soft.seen_term = term;
                    return Ok(true);
                }
                Err(error) if error.code() == afs_error::META_DFS_SESSION_NOT_REPORTED => {
                    self.report_readers()?;
                }
                Err(error) if error.code() == afs_error::META_DFS_READER_NOT_HEAD => {
                    return Ok(false);
                }
                Err(error) if error.code() == afs_error::NODE_VFS_UNIMPLEMENTED => {
                    self.soft()?.unsupported = true;
                    return Ok(false);
                }
                Err(error) => return Err(error),
            }
        }
        Ok(false)
    }
    /// Sends every live reader up to the low watermark to Meta, made with the
    /// newest Meta term this Node has seen. A reply naming a newer term means
    /// the report reached a Meta this Node did not know yet: the Node now
    /// drops replies of older terms and reports again under the new term,
    /// which is the one Meta counts (design L4).
    pub fn report_readers(&self) -> Result<()> {
        for _ in 0..SOFT_RETAIN_ATTEMPTS {
            let (term, low_watermark, readers) = {
                let soft = self.soft()?;
                if soft.unsupported {
                    return Ok(());
                }
                let low_watermark = soft.low_watermark();
                let readers = soft
                    .live
                    .range(..=low_watermark)
                    .map(|(reader, (inode, version))| SoftReader {
                        reader: *reader,
                        inode: inode.clone(),
                        version: version.clone(),
                    })
                    .collect::<Vec<_>>();
                (soft.seen_term, low_watermark, readers)
            };
            let replied = self.control.report_readers(term, low_watermark, &readers)?;
            let mut soft = self.soft()?;
            if replied <= term {
                // Counted, or an older Meta that counts for nothing.
                return Ok(());
            }
            soft.seen_term = soft.seen_term.max(replied);
        }
        Err(Error::coded(
            afs_error::IO_UNAVAILABLE,
            "Meta writer term kept changing during a reader report",
        ))
    }
    /// Releases closed soft readers, at most `batches` calls of
    /// `RELEASE_BATCH`. A failure keeps every id it did not confirm.
    fn release_soft(&self, batches: usize) -> Result<()> {
        let released = std::mem::take(&mut self.soft()?.released);
        for (index, batch) in released.chunks(RELEASE_BATCH).enumerate() {
            if index == batches {
                let rest = released[index * RELEASE_BATCH..].to_vec();
                self.soft()?.released.extend(rest);
                return Ok(());
            }
            if let Err(error) = self.control.release_readers(batch) {
                let rest = released[index * RELEASE_BATCH..].to_vec();
                self.soft()?.released.extend(rest);
                return Err(error);
            }
        }
        Ok(())
    }
    /// The pin of a handle whose reference Meta admits inside the Create
    /// transaction (`dfs::created_handle_reference_id`): no admission call here.
    pub(crate) fn created_handle_pin(
        self: &Arc<Self>,
        operation: &crate::dfs::OperationId,
    ) -> Arc<GcPin> {
        Arc::new(GcPin {
            fence: None,
            deferred: None,
            id: crate::dfs::created_handle_reference_id(operation),
            owner: self.clone(),
            open_inode: None,
            reader: None,
        })
    }
    fn shared_open_pin(&self, key: &OpenPinKey) -> Result<Option<Arc<GcPin>>> {
        Ok(self
            .open_pins
            .lock()
            .map_err(|_| Error::coded(afs_error::IO_UNAVAILABLE, "GC pin table poisoned"))?
            .get(key)
            .and_then(std::sync::Weak::upgrade))
    }
    fn share_open_pin(&self, key: OpenPinKey, pin: &Arc<GcPin>) -> Result<()> {
        self.open_pins
            .lock()
            .map_err(|_| Error::coded(afs_error::IO_UNAVAILABLE, "GC pin table poisoned"))?
            .insert(key, Arc::downgrade(pin));
        Ok(())
    }
    /// The pin of a write generation Meta admitted inside the Create
    /// transaction (round 2 B1): no admission call here. Built before the
    /// Create is sent, so a lost reply still queues its release.
    pub(crate) fn admitted_generation_pin(
        self: &Arc<Self>,
        operation: &crate::dfs::OperationId,
    ) -> Arc<GcPin> {
        Arc::new(GcPin {
            fence: Some(crate::dfs::GcFence::WriteGeneration),
            deferred: None,
            id: format!("write:{}", operation.0),
            owner: self.clone(),
            open_inode: None,
            reader: None,
        })
    }
    pub(crate) fn pin_generation(
        self: &Arc<Self>,
        inode: &InodeId,
        version: Option<&FileVersionId>,
        operation: &crate::dfs::OperationId,
    ) -> Result<Arc<GcPin>> {
        self.pin_mode(inode, version, Some(operation))
    }
    /// A `write:` pin whose Meta admission runs on `runtime`'s blocking pool
    /// instead of the caller's thread (the FUSE write thread). The caller
    /// returns at once; the generation's uploads and its commit wait for the
    /// admission through [`GcPin::admitted`].
    pub(crate) fn pin_generation_deferred(
        self: &Arc<Self>,
        inode: &InodeId,
        version: Option<&FileVersionId>,
        operation: &crate::dfs::OperationId,
        runtime: &tokio::runtime::Handle,
    ) -> Result<Arc<GcPin>> {
        if self.pending_len()? >= RELEASE_MAX_PENDING {
            self.flush()?;
        }
        let admission = Arc::new(DeferredAdmission {
            outcome: Mutex::new(None),
            ready: std::sync::Condvar::new(),
            control: self.control.clone(),
            inode: inode.clone(),
            version: version.cloned(),
        });
        // Queue cleanup even when an admission reply is lost.
        let pin = Arc::new(GcPin {
            fence: None,
            deferred: Some(admission.clone()),
            id: format!("write:{}", operation.0),
            owner: self.clone(),
            open_inode: None,
            reader: None,
        });
        let mut task = AdmissionTask {
            admission: Some(admission),
            id: pin.id.clone(),
        };
        drop(runtime.spawn_blocking(move || task.run()));
        Ok(pin)
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
            deferred: None,
            id,
            owner: self.clone(),
            open_inode: generation
                .is_none()
                .then(|| (inode.clone(), version.cloned())),
            reader: None,
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
    /// Releases everything dropped so far, a batch at a time, and reports
    /// the soft readers. On failure the unconfirmed ids return to the queue:
    /// a lost release only protects longer. For shutdown and backpressure;
    /// the release loop uses `release_batch` and `flush_for_collection`.
    pub fn flush(&self) -> Result<()> {
        let soft = self
            .release_soft(usize::MAX)
            .and_then(|()| self.report_readers());
        self.flush_durable(usize::MAX)?;
        soft
    }
    /// A wake of the release loop (`release_wanted`): one batch of each
    /// kind, then back to waiting. Ids that drop meanwhile wait for the next
    /// wake or pass instead of each costing a release transaction (R107).
    pub(crate) fn release_batch(&self) -> Result<()> {
        let soft = self.release_soft(1);
        self.flush_durable(1)?;
        // A wake fires when a queue reaches the threshold; one still above it
        // after this batch would otherwise wait for the next pass.
        if self.pending_len()? >= RELEASE_WAKE_PENDING
            || self.soft()?.released.len() >= RELEASE_WAKE_PENDING
        {
            self.wake.notify_one();
        }
        soft
    }
    /// A collection pass: releases what was queued when it started, however
    /// fast new ids arrive, and reconciles the soft readers with Meta (design
    /// §3.3 items 3 and 4) once per pass.
    pub(crate) fn flush_for_collection(&self) -> Result<()> {
        let backlog = |queued: usize| queued.div_ceil(RELEASE_BATCH);
        let soft_batches = backlog(self.soft()?.released.len());
        let soft = self
            .release_soft(soft_batches)
            .and_then(|()| self.report_readers());
        self.flush_durable(backlog(self.pending_len()?))?;
        soft
    }
    /// Sends at most `batches` release calls.
    fn flush_durable(&self, batches: usize) -> Result<()> {
        for _ in 0..batches {
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
        Ok(())
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

    /// A Meta whose soft registration replies are scripted per call.
    #[derive(Default)]
    struct SoftMeta {
        /// Popped per `retain_reader`: Ok(term) or an error code.
        retains: Mutex<std::collections::VecDeque<std::result::Result<u64, afs_error::ErrorCode>>>,
        /// The writer term `report_readers` answers with.
        term: AtomicU64,
        reports: Mutex<Vec<(u64, u64, Vec<SoftReader>)>>,
        durable: Mutex<Vec<String>>,
        released: Mutex<Vec<u64>>,
    }
    impl GcControl for SoftMeta {
        fn retain(&self, id: &str, _: &InodeId, _: Option<&FileVersionId>) -> Result<()> {
            self.durable.lock().unwrap().push(id.to_owned());
            Ok(())
        }
        fn release(&self, _: &[String]) -> Result<()> {
            Ok(())
        }
        fn collect(&self, _: u64, _: Vec<ChunkObject>) -> Result<GcState> {
            Ok(GcState::default())
        }
        fn retain_reader(&self, _: u64, _: &InodeId, _: Option<&FileVersionId>) -> Result<u64> {
            match self.retains.lock().unwrap().pop_front() {
                Some(Ok(term)) => Ok(term),
                Some(Err(code)) => Err(Error::coded(code, "scripted")),
                None => Ok(self.term.load(Ordering::Relaxed)),
            }
        }
        fn release_readers(&self, readers: &[u64]) -> Result<()> {
            self.released.lock().unwrap().extend_from_slice(readers);
            Ok(())
        }
        fn report_readers(&self, term: u64, low: u64, readers: &[SoftReader]) -> Result<u64> {
            self.reports
                .lock()
                .unwrap()
                .push((term, low, readers.to_vec()));
            Ok(self.term.load(Ordering::Relaxed))
        }
    }
    fn soft_meta(term: u64) -> Arc<SoftMeta> {
        let meta = Arc::new(SoftMeta::default());
        meta.term.store(term, Ordering::Relaxed);
        meta
    }

    /// L6/U4: once the Node has seen term 2, a term-1 reply (a replaced Meta
    /// still answering) is dropped and the registration asked again.
    #[test]
    fn a_reply_from_an_older_meta_term_is_dropped_and_retried() {
        let meta = soft_meta(2);
        let references = GcReferences::new(meta.clone());
        references.report_readers().unwrap();
        meta.retains.lock().unwrap().extend([Ok(1), Ok(2)]);
        let pin = references.pin(&InodeId::new("i"), None).unwrap();
        assert_eq!(pin.reader, Some(1));
        assert!(
            meta.retains.lock().unwrap().is_empty(),
            "the term-1 reply was not used"
        );
        assert!(meta.durable.lock().unwrap().is_empty());
        // Only older terms answer: the pin falls back to a durable reference.
        meta.retains
            .lock()
            .unwrap()
            .extend(std::iter::repeat_n(Ok(1), SOFT_RETAIN_ATTEMPTS));
        let durable = references.pin(&InodeId::new("j"), None).unwrap();
        assert_eq!(durable.reader, None);
        assert_eq!(meta.durable.lock().unwrap().len(), 1);
    }

    /// A report learning a newer term is repeated under that term, the only
    /// one Meta counts (L4); a NotReported registration reports first.
    #[test]
    fn a_report_that_learns_a_newer_term_is_made_again_under_it() {
        let meta = soft_meta(3);
        let references = GcReferences::new(meta.clone());
        meta.retains
            .lock()
            .unwrap()
            .push_back(Err(afs_error::META_DFS_SESSION_NOT_REPORTED));
        let pin = references.pin(&InodeId::new("i"), None).unwrap();
        assert!(pin.reader.is_some());
        let terms = meta
            .reports
            .lock()
            .unwrap()
            .iter()
            .map(|(term, _, _)| *term)
            .collect::<Vec<_>>();
        assert_eq!(terms, [0, 3]);
    }

    /// Meta refusing the version as not the linked head (an old version, an
    /// unlinked inode) makes the pin durable, as before C-1.
    #[test]
    fn a_version_that_is_not_the_linked_head_is_pinned_durably() {
        let meta = soft_meta(1);
        let references = GcReferences::new(meta.clone());
        meta.retains
            .lock()
            .unwrap()
            .push_back(Err(afs_error::META_DFS_READER_NOT_HEAD));
        let pin = references
            .pin(&InodeId::new("i"), Some(&FileVersionId::new("old")))
            .unwrap();
        assert_eq!(pin.reader, None);
        assert_eq!(meta.durable.lock().unwrap().len(), 1);
        assert!(pin.id.starts_with("open:"));
    }

    /// U3 (Node side): a report covers live readers up to the low watermark,
    /// below the first registration still in flight; a closed reader is
    /// released and leaves the report.
    #[test]
    fn a_report_stops_below_the_first_registration_in_flight() {
        let meta = soft_meta(1);
        let references = GcReferences::new(meta.clone());
        let first = references.pin(&InodeId::new("a"), None).unwrap();
        let second = references.pin(&InodeId::new("b"), None).unwrap();
        references.soft.lock().unwrap().in_flight.insert(2_000);
        references.soft.lock().unwrap().next = 2_001;
        references.report_readers().unwrap();
        let (_, low, readers) = meta.reports.lock().unwrap().last().cloned().unwrap();
        assert_eq!(low, 1_999);
        assert_eq!(
            readers
                .iter()
                .map(|reader| reader.reader)
                .collect::<Vec<_>>(),
            [1, 2]
        );
        drop(first);
        references.soft.lock().unwrap().in_flight.clear();
        references.flush().unwrap();
        assert_eq!(*meta.released.lock().unwrap(), [1]);
        let (_, low, readers) = meta.reports.lock().unwrap().last().cloned().unwrap();
        assert_eq!(low, 2_000);
        assert_eq!(
            readers
                .iter()
                .map(|reader| reader.reader)
                .collect::<Vec<_>>(),
            [2]
        );
        drop(second);
    }

    /// A Meta whose release call lets more pins drop meanwhile, as creates
    /// keep closing handles while a release is in flight (R107).
    struct BusyMeta {
        owner: Mutex<Option<std::sync::Weak<GcReferences>>>,
        batches: AtomicU64,
        /// Ids that drop during each of the first release calls.
        arrivals: usize,
        reports: AtomicU64,
    }
    impl GcControl for BusyMeta {
        fn retain(&self, _: &str, _: &InodeId, _: Option<&FileVersionId>) -> Result<()> {
            Ok(())
        }
        fn release(&self, _: &[String]) -> Result<()> {
            let call = self.batches.fetch_add(1, Ordering::SeqCst);
            if call < 1_000
                && let Some(owner) = self
                    .owner
                    .lock()
                    .unwrap()
                    .as_ref()
                    .and_then(|o| o.upgrade())
            {
                let mut pending = owner.pending.lock().unwrap();
                pending.extend((0..self.arrivals).map(|i| format!("open:late-{call}-{i}")));
            }
            Ok(())
        }
        fn collect(&self, _: u64, _: Vec<ChunkObject>) -> Result<GcState> {
            Ok(GcState::default())
        }
        fn report_readers(&self, _: u64, _: u64, _: &[SoftReader]) -> Result<u64> {
            self.reports.fetch_add(1, Ordering::SeqCst);
            Ok(0)
        }
    }
    fn busy(arrivals: usize) -> (Arc<BusyMeta>, Arc<GcReferences>) {
        let meta = Arc::new(BusyMeta {
            owner: Mutex::new(None),
            batches: AtomicU64::new(0),
            arrivals,
            reports: AtomicU64::new(0),
        });
        let references = GcReferences::new(meta.clone());
        *meta.owner.lock().unwrap() = Some(Arc::downgrade(&references));
        (meta, references)
    }

    /// R107: a wake sends one batch and goes back to waiting; it does not
    /// chase ids that drop while that batch is in flight.
    #[test]
    fn a_release_wake_sends_one_batch() {
        let (meta, references) = busy(3);
        references
            .pending
            .lock()
            .unwrap()
            .extend((0..RELEASE_WAKE_PENDING).map(|i| format!("open:{i}")));
        references.release_batch().unwrap();
        assert_eq!(meta.batches.load(Ordering::SeqCst), 1);
        assert_eq!(references.pending_len().unwrap(), 3, "the late ids wait");
        assert_eq!(
            meta.reports.load(Ordering::SeqCst),
            0,
            "a wake reports nothing"
        );
    }

    /// R107: a collection pass releases what was queued when it started,
    /// however fast new ids arrive, and reports readers once.
    #[test]
    fn a_collection_pass_releases_only_what_was_queued_when_it_started() {
        let (meta, references) = busy(3);
        references
            .pending
            .lock()
            .unwrap()
            .extend((0..2 * RELEASE_BATCH + 5).map(|i| format!("open:{i}")));
        references.flush_for_collection().unwrap();
        assert_eq!(
            meta.batches.load(Ordering::SeqCst),
            3,
            "the backlog, not the arrivals"
        );
        // The last batch carries the ids that arrived during the first two;
        // those of the last call wait for the next wake or pass.
        assert_eq!(references.pending_len().unwrap(), 3);
        assert_eq!(meta.reports.load(Ordering::SeqCst), 1);
    }

    /// An explicit flush still drains everything (shutdown, backpressure).
    #[test]
    fn an_explicit_flush_still_drains_everything() {
        let (meta, references) = busy(0);
        references
            .pending
            .lock()
            .unwrap()
            .extend((0..RELEASE_BATCH + 1).map(|i| format!("open:{i}")));
        references.flush().unwrap();
        assert_eq!(references.pending_len().unwrap(), 0);
        assert_eq!(meta.batches.load(Ordering::SeqCst), 2);
    }

    /// Soft releases that fail keep every id for the retry, not only the
    /// failed batch.
    #[test]
    fn failed_soft_releases_keep_every_reader() {
        struct Refusing;
        impl GcControl for Refusing {
            fn retain(&self, _: &str, _: &InodeId, _: Option<&FileVersionId>) -> Result<()> {
                Ok(())
            }
            fn release(&self, _: &[String]) -> Result<()> {
                Ok(())
            }
            fn collect(&self, _: u64, _: Vec<ChunkObject>) -> Result<GcState> {
                Ok(GcState::default())
            }
            fn release_readers(&self, _: &[u64]) -> Result<()> {
                Err(Error::coded(afs_error::IO_UNAVAILABLE, "lost"))
            }
        }
        let references = GcReferences::new(Arc::new(Refusing));
        references
            .soft
            .lock()
            .unwrap()
            .released
            .extend(0..(2 * RELEASE_BATCH as u64 + 1));
        assert!(references.flush_for_collection().is_err());
        assert_eq!(
            references.soft.lock().unwrap().released.len(),
            2 * RELEASE_BATCH + 1
        );
    }
}
