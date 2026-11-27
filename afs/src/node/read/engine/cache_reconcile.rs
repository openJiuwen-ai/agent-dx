//! Catalog-backed desired state. Runtime state is only an optimization; after a
//! crash every pending publication/retirement is reconstructed from the same WAL.
use super::*;
use crate::node::{
    chunk::{ReadCacheIntent, SharedChunkBytes},
    read::holder_partial::{PartialBytes, RegisteredPartial},
};
use std::{collections::VecDeque, ops::Range};
use tokio::sync::{oneshot, watch};

type InstallOutcome = Option<Result<()>>;
pub(super) type InstallWaiter = watch::Receiver<InstallOutcome>;
pub(super) type InstallLeader = watch::Sender<InstallOutcome>;

/// Publications attempted at once (D1). Each is one Meta transaction on a
/// different Chunk; the bound keeps one reader from flooding Meta.
pub(super) const PUBLISH_CONCURRENCY: usize = 4;
/// Unconfirmed intents handled in one pass; the next pass starts at once.
pub(super) const MAX_UNCONFIRMED_PER_PASS: usize = 128;
/// Confirmed intents re-checked per pass (eviction or loss -> retirement).
pub(super) const CONFIRMED_CHECKS_PER_PASS: usize = 16;

/// First and longest wait before an intent whose publication failed is
/// tried again (doubling). Other intents keep being published meanwhile.
pub(super) const PUBLISH_RETRY_START: Duration = Duration::from_secs(1);
pub(super) const PUBLISH_RETRY_MAX: Duration = Duration::from_secs(30);

#[derive(Default)]
pub(super) struct CacheSyncState {
    confirmed: BTreeMap<ChunkId, ReadCacheIntent>,
    cursor: usize,
    /// Intents whose last publication or retirement failed: not before
    /// `retry_at`, then with a doubled wait (review HIGH: no head-of-line
    /// blocking by persistently failing intents).
    backoff: BTreeMap<ChunkId, (Instant, Duration)>,
    /// Rotates the start of the unconfirmed set between passes.
    unconfirmed_cursor: usize,
}

/// What one reconciliation pass did, for the loop and for tests.
#[derive(Debug, Default, Eq, PartialEq)]
pub(super) struct ReconcileSummary {
    pub(super) published: usize,
    pub(super) retired: usize,
    pub(super) failed: usize,
    /// Unconfirmed intents not backed off and not handled in this pass.
    pub(super) pending: usize,
    /// Unconfirmed intents skipped because their retry time has not come.
    pub(super) backed_off: usize,
}

/// How much of a local cache copy one reconciliation looks at.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum IntentCheck {
    /// Already published: identity and length only; unchanged is skipped.
    Confirmed,
    /// Installed by this process and unchanged since (same receipt and
    /// catalog revision): identity and length only, then published.
    FreshInstall,
    /// Anything else (first sight after a restart, a changed copy): the file
    /// is read back and fully verified before publication.
    Full,
}

#[derive(Debug)]
pub(super) enum IntentOutcome {
    Unchanged,
    Published(Box<ReadCacheIntent>),
    Retired,
}

impl DfsReadEngine {
    /// D1: unconfirmed intents first, in catalog order, a bounded number at
    /// a time with [`PUBLISH_CONCURRENCY`] in flight; then a bounded
    /// round-robin over confirmed intents so evicted copies are still
    /// retired. Previously one pass took 16 intents in rotation, confirmed
    /// ones included, one Meta call at a time, once per second.
    pub(super) async fn reconcile_read_caches(
        &self,
        state: &mut CacheSyncState,
        deadline: Instant,
    ) -> Result<ReconcileSummary> {
        use futures_util::{StreamExt, stream};
        let mut summary = ReconcileSummary::default();
        let Some(cache) = &self.distributed_cache else {
            return Ok(summary);
        };
        let session = cache.session.as_str();
        let authority = self.capture_native_read_authority()?;
        if authority.epoch == 0 {
            return Ok(summary);
        }
        let intents = self.local.read_cache_intents()?;
        state
            .confirmed
            .retain(|id, _| intents.iter().any(|i| &i.copy.chunk_id == id));
        state
            .backoff
            .retain(|id, _| intents.iter().any(|i| &i.copy.chunk_id == id));
        let (confirmed, unconfirmed): (Vec<&ReadCacheIntent>, Vec<&ReadCacheIntent>) = intents
            .iter()
            .partition(|intent| state.confirmed.get(&intent.copy.chunk_id) == Some(*intent));
        let now = Instant::now();
        let (ready, waiting): (Vec<&ReadCacheIntent>, Vec<&ReadCacheIntent>) =
            unconfirmed.into_iter().partition(|intent| {
                state
                    .backoff
                    .get(&intent.copy.chunk_id)
                    .is_none_or(|(retry_at, _)| *retry_at <= now)
            });
        summary.backed_off = waiting.len();
        summary.pending = ready.len().saturating_sub(MAX_UNCONFIRMED_PER_PASS);
        let mut work: Vec<(ReadCacheIntent, IntentCheck)> = Vec::new();
        if !ready.is_empty() {
            let start = state.unconfirmed_cursor % ready.len();
            let take = ready.len().min(MAX_UNCONFIRMED_PER_PASS);
            state.unconfirmed_cursor = state.unconfirmed_cursor.wrapping_add(take);
            work.extend(ready.iter().cycle().skip(start).take(take).map(|intent| {
                // CPU fix 1: a copy this process just installed and
                // has not changed since is published without reading
                // it back; anything else is fully verified first.
                let check = if cache.take_fresh_install(intent) {
                    IntentCheck::FreshInstall
                } else {
                    IntentCheck::Full
                };
                ((*intent).clone(), check)
            }));
        }
        if !confirmed.is_empty() {
            let start = state.cursor % confirmed.len();
            let take = confirmed.len().min(CONFIRMED_CHECKS_PER_PASS);
            state.cursor = state.cursor.wrapping_add(take);
            work.extend(
                confirmed
                    .iter()
                    .cycle()
                    .skip(start)
                    .take(take)
                    .map(|intent| ((*intent).clone(), IntentCheck::Confirmed)),
            );
        }
        let results: Vec<(ChunkId, Result<IntentOutcome>)> = stream::iter(work)
            .map(|(intent, check)| async move {
                let id = intent.copy.chunk_id.clone();
                let outcome = self
                    .reconcile_intent(session, authority.epoch, intent, check, deadline)
                    .await;
                (id, outcome)
            })
            .buffer_unordered(PUBLISH_CONCURRENCY)
            .collect()
            .await;
        self.check_native_read_authority(&authority)?;
        let mut failure = None;
        for (id, result) in results {
            match result {
                Ok(IntentOutcome::Unchanged) => {
                    state.backoff.remove(&id);
                }
                Ok(IntentOutcome::Published(current)) => {
                    let current = *current;
                    summary.published += 1;
                    state.backoff.remove(&id);
                    state
                        .confirmed
                        .insert(current.copy.chunk_id.clone(), current);
                }
                Ok(IntentOutcome::Retired) => {
                    summary.retired += 1;
                    state.backoff.remove(&id);
                }
                Err(error) => {
                    summary.failed += 1;
                    let wait = state
                        .backoff
                        .get(&id)
                        .map_or(PUBLISH_RETRY_START, |(_, wait)| {
                            (*wait * 2).min(PUBLISH_RETRY_MAX)
                        });
                    state.backoff.insert(id, (Instant::now() + wait, wait));
                    failure = Some(error);
                }
            }
        }
        // A failing pass keeps its summary: pending intents are not lost
        // and failed ones wait for their own retry time.
        if let Some(error) = failure {
            afs_logging::warn!("dfs.read_cache_reconcile_retry"; "error" => error.to_string(), "failed" => summary.failed);
        }
        Ok(summary)
    }

    /// One intent: verify the local copy, then publish it or retire it.
    pub(super) async fn reconcile_intent(
        &self,
        session: &str,
        epoch: u64,
        intent: ReadCacheIntent,
        check: IntentCheck,
        deadline: Instant,
    ) -> Result<IntentOutcome> {
        if Instant::now() >= deadline {
            return Err(unavailable("DFS read cache reconciliation pass expired"));
        }
        let verified = check != IntentCheck::Full;
        match check {
            IntentCheck::FreshInstall => {
                self.local.record_read_cache_event("publish_verify_skipped")
            }
            IntentCheck::Full => self.local.record_read_cache_event("publish_verify_full"),
            IntentCheck::Confirmed => {}
        }
        let store = self.local.clone();
        let expected = intent.clone();
        let current = crate::node::bridge::spawn_blocking(move || {
            store.refresh_read_cache_intent(&expected, epoch, verified)
        })
        .await
        .map_err(|e| unavailable(format!("cache reconciliation worker failed: {e}")))?;
        let current = match current {
            Ok(current) => current,
            // The intent was replaced (evicted and installed again) after this
            // pass listed it; the next pass publishes the current one.
            Err(error) if error.code() == afs_error::NODE_DFS_STALE_HANDLE => {
                return Ok(IntentOutcome::Unchanged);
            }
            Err(error) => return Err(error),
        };
        let Some(current) = current else {
            return self
                .retire_cache_intent(session, epoch, &intent, deadline)
                .await
                .map(|()| IntentOutcome::Retired);
        };
        if check == IntentCheck::Confirmed && current == intent {
            return Ok(IntentOutcome::Unchanged);
        }
        let operation_id = cache_sync_operation("publish", session, &current)?;
        let CopyLocation::Node {
            node_id,
            node_epoch,
            device_id,
            device_epoch,
            catalog_revision,
        } = &current.copy.location
        else {
            return Err(invalid("cache intent must identify a node copy"));
        };
        let receipt = crate::dfs::ReplicaAck {
            operation_id: operation_id.clone(),
            chunk_id: current.copy.chunk_id.clone(),
            placement_revision: 0,
            placement_epoch: 0,
            node_id: node_id.clone(),
            node_epoch: *node_epoch,
            device_id: device_id.clone(),
            device_epoch: *device_epoch,
            catalog_revision: *catalog_revision,
            persisted_bytes: current.copy.persisted_bytes,
            verified_digest: current.copy.verified_digest.clone(),
        };
        match self
            .source_provider
            .publish_read_cache(
                crate::dfs::PublishReadCache {
                    caller_id: self.local_node_id.clone(),
                    caller_session_id: session.to_owned(),
                    caller_node_epoch: epoch,
                    operation_id,
                    namespace_id: current.namespace_id.clone(),
                    file_version_id: current.file_version_id.clone(),
                    layout_root_id: current.layout_root_id.clone(),
                    receipt,
                },
                deadline,
            )
            .await
        {
            Ok(_) => {
                self.local.record_read_cache_event("published");
                Ok(IntentOutcome::Published(Box::new(current)))
            }
            Err(error) if error.code() == afs_error::IO_NOT_FOUND => self
                .retire_cache_intent(session, epoch, &current, deadline)
                .await
                .map(|()| IntentOutcome::Retired),
            Err(error) => {
                self.local.record_read_cache_event("publish_failed");
                Err(error)
            }
        }
    }

    async fn retire_cache_intent(
        &self,
        session: &str,
        epoch: u64,
        intent: &ReadCacheIntent,
        deadline: Instant,
    ) -> Result<()> {
        let operation_id = cache_sync_operation("retire", session, intent)?;
        let retired = self
            .source_provider
            .retire_read_cache(
                crate::dfs::RetireReadCache {
                    caller_id: self.local_node_id.clone(),
                    caller_session_id: session.to_owned(),
                    caller_node_epoch: epoch,
                    operation_id,
                    copy: intent.copy.clone(),
                },
                deadline,
            )
            .await;
        if let Err(error) = retired {
            self.local.record_read_cache_event("retire_failed");
            return Err(error);
        }
        let store = self.local.clone();
        let expected = intent.clone();
        crate::node::bridge::spawn_blocking(move || store.acknowledge_cache_retirement(&expected))
            .await
            .map_err(|error| unavailable(format!("cache acknowledgement failed: {error}")))??;
        self.local.record_read_cache_event("retired");
        Ok(())
    }
}

fn cache_sync_operation(
    kind: &str,
    session: &str,
    intent: &ReadCacheIntent,
) -> Result<crate::dfs::OperationId> {
    let identity = serde_json::to_vec(&(session, intent)).map_err(|e| invalid(e.to_string()))?;
    Ok(crate::dfs::OperationId::new(format!(
        "cache-{kind}:{}",
        blake3::hash(&identity).to_hex()
    )))
}

/// Byte budgets of the read cache bodies held off the FUSE path (D1-6).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ReadCacheBudgets {
    /// Staged complete bodies waiting for (or in) installation.
    pub queue_bytes: u64,
    /// Partial bodies being assembled from separate reads; 0 disables.
    pub partial_bytes: u64,
}

impl Default for ReadCacheBudgets {
    fn default() -> Self {
        Self {
            queue_bytes: DEFAULT_READ_CACHE_QUEUE_BYTES,
            partial_bytes: DEFAULT_READ_CACHE_PARTIAL_BYTES,
        }
    }
}

/// 64 four-MiB Chunks. With 32 MiB a disk-backed reader dropped 37-47 % of
/// its bodies (the installer writes at disk speed); 256 MiB dropped none and
/// peaked at 192 MiB (report 14 D).
pub(crate) const DEFAULT_READ_CACHE_QUEUE_BYTES: u64 = 256 * 1024 * 1024;
/// Eight partial 4 MiB Chunks: four interleaved reader threads, each with a
/// Chunk boundary in flight.
pub(crate) const DEFAULT_READ_CACHE_PARTIAL_BYTES: u64 = 32 * 1024 * 1024;
pub(crate) const MAX_READ_CACHE_BUDGET_BYTES: u64 = 1024 * 1024 * 1024;

impl ReadCacheBudgets {
    pub(crate) fn validate(&self) -> Result<()> {
        let chunk = crate::node::chunk::MAX_STAGED_CHUNK_BYTES as u64;
        if !(chunk..=MAX_READ_CACHE_BUDGET_BYTES).contains(&self.queue_bytes)
            || self.partial_bytes > MAX_READ_CACHE_BUDGET_BYTES
            || (self.partial_bytes != 0 && self.partial_bytes < chunk)
        {
            return Err(invalid(
                "read cache budgets require queue 4MiB..=1GiB and partial 0 or 4MiB..=1GiB",
            ));
        }
        Ok(())
    }
}

/// A lock-free byte budget. Charges are taken without waiting on the FUSE
/// path; only background callers may wait for one ([`Self::charge_until`]).
pub(super) struct StageBudget {
    limit: u64,
    used: std::sync::atomic::AtomicU64,
    peak: std::sync::atomic::AtomicU64,
    freed: tokio::sync::Notify,
    local: Option<Arc<LocalChunkStore>>,
    resource: &'static str,
    peak_resource: &'static str,
}

/// Bytes held against a [`StageBudget`]; released on drop.
pub(super) struct StageCharge {
    budget: Arc<StageBudget>,
    bytes: u64,
}

impl Drop for StageCharge {
    fn drop(&mut self) {
        let used = self
            .budget
            .used
            .fetch_sub(self.bytes, std::sync::atomic::Ordering::AcqRel)
            .saturating_sub(self.bytes);
        self.budget.observe(used);
        self.budget.freed.notify_waiters();
    }
}

impl StageBudget {
    fn new(
        limit: u64,
        local: Option<Arc<LocalChunkStore>>,
        resource: &'static str,
        peak_resource: &'static str,
        limit_resource: &'static str,
    ) -> Arc<Self> {
        if let Some(local) = &local {
            local.record_read_cache_resource(limit_resource, limit);
            local.record_read_cache_resource(resource, 0);
        }
        Arc::new(Self {
            limit,
            used: std::sync::atomic::AtomicU64::new(0),
            peak: std::sync::atomic::AtomicU64::new(0),
            freed: tokio::sync::Notify::new(),
            local,
            resource,
            peak_resource,
        })
    }

    #[cfg(test)]
    pub(super) fn new_for_test(limit: u64) -> Arc<Self> {
        Self::new(
            limit,
            None,
            "queue_bytes",
            "queue_peak_bytes",
            "queue_limit_bytes",
        )
    }

    fn observe(&self, used: u64) {
        if let Some(local) = &self.local {
            local.record_read_cache_resource(self.resource, used);
            let peak = self.peak.load(std::sync::atomic::Ordering::Relaxed);
            local.record_read_cache_resource(self.peak_resource, peak);
        }
    }

    /// Never waits: the FUSE read path calls this.
    pub(super) fn try_charge(self: &Arc<Self>, bytes: u64) -> Option<StageCharge> {
        use std::sync::atomic::Ordering;
        let previous = self
            .used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(bytes).filter(|total| *total <= self.limit)
            })
            .ok()?;
        let used = previous + bytes;
        self.peak.fetch_max(used, Ordering::Relaxed);
        self.observe(used);
        Some(StageCharge {
            budget: self.clone(),
            bytes,
        })
    }

    /// Background only (management prefetch): wait for freed bytes until
    /// `deadline`.
    pub(super) async fn charge_until(
        self: &Arc<Self>,
        bytes: u64,
        deadline: Instant,
    ) -> Result<StageCharge> {
        loop {
            let freed = self.freed.notified();
            tokio::pin!(freed);
            freed.as_mut().enable();
            if let Some(charge) = self.try_charge(bytes) {
                return Ok(charge);
            }
            tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), freed)
                .await
                .map_err(|_| unavailable("DFS prefetch cache admission expired"))?;
        }
    }
}

#[cfg(test)]
type AfterFreeze = Box<dyn FnOnce() + Send>;

/// One byte-budgeted queue and one supervised installer per node. Cache
/// persistence and metadata publication never gate foreground delivery.
pub(super) struct ReadCacheRuntime {
    #[cfg(test)]
    after_freeze: Mutex<Option<AfterFreeze>>,
    session: String,
    capacity: u64,
    send: tokio::sync::mpsc::UnboundedSender<CacheJob>,
    receive: Mutex<Option<tokio::sync::mpsc::UnboundedReceiver<CacheJob>>>,
    /// Every queued job holds a charge here, so the unbounded channel is
    /// bounded by bytes.
    queue: Arc<StageBudget>,
    partial_budget: Arc<StageBudget>,
    /// Segments handed from FUSE completions to the assembler; each holds a
    /// charge here, so the unbounded channel is bounded by bytes.
    segment_budget: Arc<StageBudget>,
    segment_send: tokio::sync::mpsc::UnboundedSender<Segment>,
    segment_receive: Mutex<Option<tokio::sync::mpsc::UnboundedReceiver<Segment>>>,
    /// Owned by the assembler (background task, or tests draining inline);
    /// the FUSE path never locks it.
    partial: Mutex<VecDeque<PartialCopy>>,
    prefetch: Mutex<HashMap<ChunkId, InstallWaiter>>,
    /// Wakes the publication loop when a copy was installed (D1), instead
    /// of waiting for its periodic tick.
    publish_wake: tokio::sync::Notify,
    /// Periodic re-check of confirmed copies (1 s; tests lengthen it).
    recheck_period: Duration,
    /// Intents this process installed (bytes verified on receive, CRC table
    /// bound at install), not yet published. In memory only: after a restart
    /// every intent is fully verified again (CPU fix 1).
    fresh_installs: Mutex<HashMap<ChunkId, ReadCacheIntent>>,
    /// Reusable segment buffers (CPU fix 3), at most [`SEGMENT_POOL_BYTES`].
    /// Each idle buffer keeps the segment-budget charge for its capacity.
    segment_pool: Mutex<Vec<(Vec<u8>, StageCharge)>>,
    /// H2 step 2: Chunks being fetched or installed here, which a holder
    /// may let another reader wait for. Set only with sharing on (!318
    /// P2-5): without it nothing is tracked.
    pub(super) in_flight: std::sync::OnceLock<Arc<crate::node::read::holders::HolderInFlight>>,
    #[cfg(test)]
    pub(super) zero_fill_bytes: std::sync::atomic::AtomicU64,
    #[cfg(test)]
    pub(super) adopted_bodies: std::sync::atomic::AtomicUsize,
    /// Body copies made on the FUSE path (tests: a dropped body is never copied).
    #[cfg(test)]
    pub(super) body_copies: std::sync::atomic::AtomicUsize,
}

pub(super) struct CacheJob {
    chunk: ChunkObject,
    body: Vec<u8>,
    shared: Option<(RegisteredPartial, SharedChunkBytes)>,
    version: FileVersionId,
    layout: LayoutRootId,
    epoch: u64,
    explicit: Option<(Instant, oneshot::Sender<Result<()>>)>,
    /// Queue bytes held until installation ends.
    charge: Option<StageCharge>,
    queued_at: Instant,
    /// Segment table the body was verified against on receive (CPU fix 2).
    table: Option<Arc<crate::node::chunk_crc32c::CrcTable>>,
    /// Marks the Chunk in flight until its installation ends (H2 step 2).
    in_flight: Option<crate::node::read::holders::InFlightGuard>,
}

/// Cache copies can arrive as separate verified FUSE pages. Partial bodies
/// are bounded by the partial byte budget and a fragmented read cannot
/// retain unbounded coverage metadata. Missing bytes are never synthesized.
const MAX_PARTIAL_RANGES: usize = 256;

struct PartialCopy {
    job: CacheJob,
    body: PartialBody,
    _charge: StageCharge,
    /// When the first segment arrived; a body still incomplete after
    /// [`PARTIAL_MAX_AGE`] is released (a segment was lost or never read).
    created: Instant,
}

enum PartialBody {
    Private(PartialBytes),
    Shared(RegisteredPartial),
}

impl PartialBody {
    fn update<T>(&mut self, f: impl FnOnce(&mut PartialBytes) -> T) -> T {
        match self {
            Self::Private(body) => f(body),
            Self::Shared(partial) => f(&mut partial.body.lock()),
        }
    }
}

/// One range of a Chunk, copied off the FUSE path for assembly.
pub(super) struct Segment {
    table: Option<Arc<crate::node::chunk_crc32c::CrcTable>>,
    chunk: ChunkObject,
    version: FileVersionId,
    layout: LayoutRootId,
    epoch: u64,
    offset: usize,
    data: Vec<u8>,
    _charge: StageCharge,
    _in_flight: Option<crate::node::read::holders::InFlightGuard>,
}

/// An incomplete partial body older than this is released. A sequential
/// reader completes a 4 MiB Chunk in milliseconds; one that has waited this
/// long lost a segment (or the reader stopped) and only holds budget.
pub(super) const PARTIAL_MAX_AGE: Duration = Duration::from_secs(2);
/// Idle segment buffers kept for reuse by the FUSE path (CPU fix 3).
pub(super) const SEGMENT_POOL_BYTES: usize = 8 * 1024 * 1024;
const MAX_FRESH_INSTALLS: usize = 1024;

impl PartialBytes {
    /// Takes a first segment (offset 0) as the body without copying it.
    fn adopt(&mut self, mut data: Vec<u8>, length: usize) -> bool {
        let end = data.len();
        // Grow (or trim) to exactly one Chunk, the partial budget's charge.
        data.reserve_exact(length.saturating_sub(data.len()));
        data.shrink_to(length);
        self.data = data;
        self.coverage = vec![Range { start: 0, end }];
        end == length
    }

    /// Writes `data` at `offset`; only a gap before `offset` is zero filled.
    fn copy_range(&mut self, offset: usize, data: &[u8], length: usize) -> bool {
        let mut end = offset + data.len();
        let mut start = offset;
        let body = &mut self.data;
        if body.capacity() < length {
            body.reserve_exact(length - body.len());
        }
        #[cfg(test)]
        let before = body.len();
        if body.len() < offset {
            body.resize(offset, 0);
        }
        #[cfg(test)]
        {
            self.last_zero_fill = body.len() - before;
        }
        let overlap = end.min(body.len());
        if offset < overlap {
            body[offset..overlap].copy_from_slice(&data[..overlap - offset]);
        }
        if end > body.len() {
            let from = body.len() - offset;
            body.extend_from_slice(&data[from..]);
        }
        let left = self.coverage.partition_point(|range| range.end < start);
        let mut right = left;
        while let Some(range) = self.coverage.get(right).filter(|range| range.start <= end) {
            start = start.min(range.start);
            end = end.max(range.end);
            right += 1;
        }
        self.coverage
            .splice(left..right, std::iter::once(start..end));
        matches!(self.coverage.as_slice(), [range] if range.start == 0 && range.end == length)
    }
}

impl ReadCacheRuntime {
    fn new(
        session: String,
        capacity: u64,
        budgets: ReadCacheBudgets,
        local: Option<Arc<LocalChunkStore>>,
    ) -> Self {
        let (send, receive) = tokio::sync::mpsc::unbounded_channel();
        let (segment_send, segment_receive) = tokio::sync::mpsc::unbounded_channel();
        Self {
            #[cfg(test)]
            after_freeze: Mutex::new(None),
            session,
            capacity,
            send,
            receive: Mutex::new(Some(receive)),
            queue: StageBudget::new(
                budgets.queue_bytes,
                local.clone(),
                "queue_bytes",
                "queue_peak_bytes",
                "queue_limit_bytes",
            ),
            partial_budget: StageBudget::new(
                budgets.partial_bytes,
                local.clone(),
                "partial_bytes",
                "partial_peak_bytes",
                "partial_limit_bytes",
            ),
            // Half the partial budget for segments in flight to the assembler.
            segment_budget: StageBudget::new(
                budgets.partial_bytes / 2,
                local,
                "segment_bytes",
                "segment_peak_bytes",
                "segment_limit_bytes",
            ),
            segment_send,
            segment_receive: Mutex::new(Some(segment_receive)),
            partial: Mutex::new(VecDeque::new()),
            prefetch: Mutex::new(HashMap::new()),
            publish_wake: tokio::sync::Notify::new(),
            recheck_period: Duration::from_secs(1),
            fresh_installs: Mutex::new(HashMap::new()),
            segment_pool: Mutex::new(Vec::new()),
            in_flight: std::sync::OnceLock::new(),
            #[cfg(test)]
            zero_fill_bytes: std::sync::atomic::AtomicU64::new(0),
            #[cfg(test)]
            adopted_bodies: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(test)]
            body_copies: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    pub(super) fn capacity(&self) -> u64 {
        self.capacity
    }

    /// Keep the first request's fetch and installation shared, including errors.
    /// A waiter has its own deadline and cannot drop the fetch for other waiters.
    pub(super) fn prefetch_flight(
        &self,
        chunk: &ChunkId,
    ) -> Result<(InstallWaiter, Option<InstallLeader>)> {
        let mut active = self
            .prefetch
            .lock()
            .map_err(|_| unavailable("DFS prefetch coordination poisoned"))?;
        active.retain(|_, waiter| waiter.borrow().is_none() && waiter.has_changed().is_ok());
        if let Some(waiter) = active.get(chunk) {
            return Ok((waiter.clone(), None));
        }
        if active.len() >= 128 {
            return Err(Error::coded(
                afs_error::IO_CAPACITY,
                "too many distinct DFS Chunks being prefetched",
            ));
        }
        let (leader, waiter) = watch::channel(None);
        active.insert(chunk.clone(), waiter.clone());
        Ok((waiter, Some(leader)))
    }

    /// Remembers an intent installed and tracked by this process.
    fn note_fresh_install(&self, intent: &ReadCacheIntent) {
        if let Ok(mut fresh) = self.fresh_installs.lock()
            && (fresh.len() < MAX_FRESH_INSTALLS || fresh.contains_key(&intent.copy.chunk_id))
        {
            fresh.insert(intent.copy.chunk_id.clone(), intent.clone());
        }
    }

    /// Whether `intent` is exactly what this process installed (consumed).
    fn take_fresh_install(&self, intent: &ReadCacheIntent) -> bool {
        let Ok(mut fresh) = self.fresh_installs.lock() else {
            return false;
        };
        fresh.remove(&intent.copy.chunk_id).as_ref() == Some(intent)
    }

    #[cfg(test)]
    pub(super) fn take_fresh_install_for_test(&self, intent: &ReadCacheIntent) -> bool {
        self.take_fresh_install(intent)
    }

    /// FUSE path: an empty buffer for `len` bytes together with the segment
    /// budget charge for its whole capacity (!297 P2). A pooled buffer brings
    /// its own charge; a new one is allocated at exactly `len` after the
    /// charge succeeds. Never waits: `None` is a counted drop.
    fn segment_buffer(&self, len: usize) -> Option<(Vec<u8>, StageCharge)> {
        if let Ok(mut pool) = self.segment_pool.try_lock()
            && let Some(index) = pool
                .iter()
                .enumerate()
                .filter(|(_, (buffer, _))| buffer.capacity() >= len)
                .min_by_key(|(_, (buffer, _))| buffer.capacity())
                .map(|(index, _)| index)
        {
            return Some(pool.swap_remove(index));
        }
        let charge = self.segment_budget.try_charge(len as u64)?;
        Some((Vec::with_capacity(len), charge))
    }

    /// Assembler: return a consumed segment buffer and its charge for reuse;
    /// the pool holds at most a quarter of the segment budget.
    fn recycle_segment(&self, mut buffer: Vec<u8>, charge: StageCharge) {
        buffer.clear();
        if charge.bytes != buffer.capacity() as u64 {
            return;
        }
        if let Ok(mut pool) = self.segment_pool.lock() {
            let held: u64 = pool.iter().map(|(_, charge)| charge.bytes).sum();
            let cap = (SEGMENT_POOL_BYTES as u64).min(self.segment_budget.limit / 4);
            if held + charge.bytes <= cap {
                pool.push((buffer, charge));
            }
        }
    }

    /// Management prefetch runs as a background job, so it may wait for
    /// queue bytes (until its own deadline); FUSE reads never do.
    pub(super) async fn enqueue_prefetch(
        &self,
        length: u64,
        deadline: Instant,
    ) -> Result<StageCharge> {
        self.queue.charge_until(length, deadline).await
    }

    /// FUSE path: queue a complete body if its bytes fit, else count a drop.
    /// `job` is only built (and the body only copied) once the bytes are
    /// charged, so a full queue costs no allocation or copy.
    fn enqueue_staged(&self, local: &LocalChunkStore, length: u64, job: impl FnOnce() -> CacheJob) {
        let staged = self
            .queue
            .try_charge(length)
            .is_some_and(|charge| self.submit(job(), charge).is_ok());
        local.record_read_cache_event(if staged {
            "staged"
        } else {
            "dropped_queue_full"
        });
    }

    /// Hands a charged job to the installer.
    pub(super) fn submit(&self, mut job: CacheJob, charge: StageCharge) -> Result<()> {
        job.charge = Some(charge);
        job.in_flight = self
            .in_flight
            .get()
            .map(|flights| flights.begin_range(&job.chunk.id, job.epoch, 0, job.chunk.length));
        job.queued_at = Instant::now();
        self.send
            .send(job)
            .map_err(|_| unavailable("DFS read cache installer stopped"))
    }

    /// FUSE read path: copies verified bytes and never waits. A body that
    /// finds its byte budget full is dropped and counted (D0/D1-6).
    // The stage counters need the Node store next to the existing six inputs.
    #[allow(clippy::too_many_arguments)]
    fn stage(
        &self,
        local: &LocalChunkStore,
        batch: &ReadBatch,
        version: &FileVersionId,
        chunk: &ChunkObject,
        epoch: u64,
        offset: usize,
        data: &[u8],
        table: Option<Arc<crate::node::chunk_crc32c::CrcTable>>,
    ) {
        let make_job = |body| {
            let mut job = CacheJob::new(
                chunk.clone(),
                body,
                version.clone(),
                batch.layout_root_id.clone(),
                epoch,
            );
            job.table = table.clone();
            job
        };
        if offset == 0 && data.len() as u64 == chunk.length {
            self.enqueue_staged(local, chunk.length, || {
                #[cfg(test)]
                self.body_copies
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                make_job(data.to_vec())
            });
            return;
        }
        // Partial range: copy it (the same one copy the table path made) and
        // hand it to the assembler. The foreground never locks its bodies;
        // the optional flight bridges the segment queue when sharing is on.
        if self.partial_budget.limit == 0 {
            local.record_read_cache_event("dropped_partial");
            return;
        }
        // The queued buffer is charged for its whole capacity, which is only
        // the segment's length (the assembler grows a first segment into the
        // body itself, under the partial budget).
        let Some((mut copy, charge)) = self.segment_buffer(data.len()) else {
            local.record_read_cache_event("dropped_segment_full");
            return;
        };
        copy.extend_from_slice(data);
        let segment = Segment {
            table,
            chunk: chunk.clone(),
            version: version.clone(),
            layout: batch.layout_root_id.clone(),
            epoch,
            offset,
            data: copy,
            _charge: charge,
            _in_flight: self.in_flight.get().map(|flights| {
                flights.begin_range(&chunk.id, epoch, offset as u64, data.len() as u64)
            }),
        };
        if self.segment_send.send(segment).is_err() {
            local.record_read_cache_event("dropped_segment_full");
        }
    }

    /// Assembler: merges one segment into its partial body; a completed body
    /// goes to the install queue. Runs off the FUSE path, so its lock and the
    /// 4 MiB body allocation never delay a read.
    fn assemble(&self, local: &LocalChunkStore, segment: Segment) {
        let Ok(mut partial) = self.partial.lock() else {
            local.record_read_cache_event("dropped_partial");
            return;
        };
        Self::expire_partial(&mut partial, local, Instant::now());
        let flights = self.in_flight.get();
        let position = partial.iter().position(|copy| {
            copy.job.chunk == segment.chunk
                && copy.job.epoch == segment.epoch
                && ((copy.job.chunk.is_segment_crc32c() && flights.is_some())
                    || (copy.job.version == segment.version && copy.job.layout == segment.layout))
        });
        let length = segment.chunk.length;
        let mut copy = match position.and_then(|position| partial.remove(position)) {
            Some(copy) => copy,
            None => {
                // Oldest partial bodies give way until the new one fits.
                let charge = loop {
                    if let Some(charge) = self.partial_budget.try_charge(length) {
                        break Some(charge);
                    }
                    if partial.pop_front().is_none() {
                        break None;
                    }
                    local.record_read_cache_event("dropped_partial");
                };
                let Some(charge) = charge else {
                    local.record_read_cache_event("dropped_partial");
                    return;
                };
                #[cfg(test)]
                self.body_copies
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                // CPU fix 3: no zeroed 4 MiB body. Bytes are written only where
                // covered (gaps before an out-of-order segment are the only
                // zero fill), and a first segment that already has room for
                // the whole Chunk becomes the body itself.
                let mut job = CacheJob::new(
                    segment.chunk.clone(),
                    Vec::new(),
                    segment.version.clone(),
                    segment.layout.clone(),
                    segment.epoch,
                );
                job.table = segment.table.clone();
                PartialCopy {
                    job,
                    body: PartialBody::Private(PartialBytes::default()),
                    _charge: charge,
                    created: Instant::now(),
                }
            }
        };
        if copy.job.table.is_none() {
            copy.job.table = segment.table.clone();
        }
        if let (Some(flights), Some(table), PartialBody::Private(bytes)) =
            (flights, &copy.job.table, &mut copy.body)
            && copy.job.chunk.is_segment_crc32c()
            && table.valid_shape(length)
            && table.descriptor_digest() == copy.job.chunk.content_digest
        {
            copy.body = PartialBody::Shared(flights.partials.register(
                copy.job.chunk.id.clone(),
                copy.job.epoch,
                table.clone(),
                std::mem::take(bytes),
            ));
        }
        let Segment {
            offset,
            data,
            _charge: segment_charge,
            _in_flight: segment_flight,
            ..
        } = segment;
        let shared = matches!(copy.body, PartialBody::Shared(_));
        let (complete, ranges, frozen) = copy.body.update(|bytes| {
            let complete = if bytes.coverage.is_empty() && offset == 0 {
                #[cfg(test)]
                self.adopted_bodies
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                drop(segment_charge);
                bytes.adopt(data, length as usize)
            } else {
                let done = bytes.copy_range(offset, &data, length as usize);
                #[cfg(test)]
                self.zero_fill_bytes.fetch_add(
                    bytes.last_zero_fill as u64,
                    std::sync::atomic::Ordering::Relaxed,
                );
                self.recycle_segment(data, segment_charge);
                done
            };
            let frozen = (complete && shared).then(|| {
                let frozen = SharedChunkBytes::from_vec(std::mem::take(&mut bytes.data));
                bytes.complete = Some(frozen.clone());
                frozen
            });
            (complete, bytes.coverage.len(), frozen)
        });
        if complete {
            #[cfg(test)]
            if let Some(hook) = self.after_freeze.lock().unwrap().take() {
                hook();
            }
            let PartialCopy {
                mut job,
                body,
                _charge,
                ..
            } = copy;
            match body {
                PartialBody::Private(bytes) => job.body = bytes.data,
                PartialBody::Shared(partial) => {
                    job.shared = frozen.map(|body| (partial, body));
                }
            }
            self.enqueue_staged(local, length, || job);
        } else if ranges <= MAX_PARTIAL_RANGES {
            partial.push_back(copy);
        } else {
            local.record_read_cache_event("dropped_partial_ranges");
        }
        // The segment's guard spans both the channel and assembly handoff.
        // Dropping it announces new coverage (or that the segment was lost).
        drop(segment_flight);
    }

    /// Releases partial bodies older than [`PARTIAL_MAX_AGE`].
    fn expire_partial(partial: &mut VecDeque<PartialCopy>, local: &LocalChunkStore, now: Instant) {
        partial.retain(|copy| {
            let keep = now.saturating_duration_since(copy.created) < PARTIAL_MAX_AGE;
            if !keep {
                local.record_read_cache_event("dropped_partial_expired");
            }
            keep
        });
    }

    /// Releases expired partial bodies without a new segment (idle sweep).
    fn sweep_partial(&self, local: &LocalChunkStore, now: Instant) {
        if let Ok(mut partial) = self.partial.lock() {
            Self::expire_partial(&mut partial, local, now);
        }
    }

    /// Tests without a running assembler: merge every segment sent so far.
    #[cfg(test)]
    pub(super) fn assemble_pending(&self, local: &LocalChunkStore) {
        let segments: Vec<Segment> = {
            let mut receive = self.segment_receive.lock().unwrap();
            let Some(receive) = receive.as_mut() else {
                return;
            };
            std::iter::from_fn(|| receive.try_recv().ok()).collect()
        };
        for segment in segments {
            self.assemble(local, segment);
        }
    }

    /// Tests: the jobs queued for installation so far.
    #[cfg(test)]
    pub(super) fn queued_jobs_for_test(&self) -> Vec<CacheJob> {
        let mut receive = self.receive.lock().unwrap();
        let Some(receive) = receive.as_mut() else {
            return Vec::new();
        };
        std::iter::from_fn(|| receive.try_recv().ok()).collect()
    }

    #[cfg(test)]
    pub(super) fn pooled_segments_for_test(&self) -> usize {
        self.segment_pool.lock().unwrap().len()
    }

    #[cfg(test)]
    pub(super) fn pooled_segment_bytes_for_test(&self) -> u64 {
        let pool = self.segment_pool.lock().unwrap();
        pool.iter()
            .map(|(buffer, _)| buffer.capacity() as u64)
            .sum()
    }

    #[cfg(test)]
    pub(super) fn sweep_partial_at(&self, local: &LocalChunkStore, now: Instant) {
        self.sweep_partial(local, now);
    }
}

impl DfsReadEngine {
    /// Default budgets; product wiring uses [`Self::with_distributed_cache_budgets`].
    #[cfg(test)]
    pub(crate) fn with_distributed_cache(self, session: String, capacity: u64) -> Self {
        let mut engine = self;
        let local = engine.local.clone();
        engine.distributed_cache = Some(Arc::new(ReadCacheRuntime::new(
            session,
            capacity,
            ReadCacheBudgets::default(),
            Some(local),
        )));
        engine.track_holder_in_flight();
        engine
    }

    /// Test engines whose periodic publication tick never comes first, so a
    /// publication can only come from the install wake-up.
    #[cfg(test)]
    pub(crate) fn with_distributed_cache_recheck(
        mut self,
        session: String,
        capacity: u64,
        recheck_period: Duration,
    ) -> Self {
        let mut runtime = ReadCacheRuntime::new(
            session,
            capacity,
            ReadCacheBudgets::default(),
            Some(self.local.clone()),
        );
        runtime.recheck_period = recheck_period;
        self.distributed_cache = Some(Arc::new(runtime));
        self.track_holder_in_flight();
        self
    }

    /// [`Self::with_distributed_cache`] with configured byte budgets (D1-6).
    /// H2 step 2: read from other readers that share the version (their
    /// holder lists cached `ttl`).
    pub(crate) fn with_holder_lists(mut self, ttl: Duration) -> Self {
        self.holder_lists = Some(HolderLists::new(ttl));
        self.track_holder_in_flight();
        self
    }

    /// With sharing and the native read cache both on, start tracking the
    /// Chunks in flight here (either may be configured first).
    fn track_holder_in_flight(&self) {
        if self.holder_lists.is_some()
            && let Some(cache) = &self.distributed_cache
        {
            let _ = cache
                .in_flight
                .set(crate::node::read::holders::HolderInFlight::new());
        }
    }

    /// H2 step 2: the Chunks this engine is fetching or installing, for the
    /// holder side of the data plane. `None` without the native read cache
    /// or without sharing (!318 P2-5).
    pub(crate) fn holder_in_flight(
        &self,
    ) -> Option<Arc<crate::node::read::holders::HolderInFlight>> {
        self.distributed_cache
            .as_ref()
            .and_then(|cache| cache.in_flight.get().cloned())
    }

    pub(crate) fn with_distributed_cache_budgets(
        mut self,
        session: String,
        capacity: u64,
        budgets: ReadCacheBudgets,
    ) -> Result<Self> {
        budgets.validate()?;
        let local = self.local.clone();
        self.distributed_cache = Some(Arc::new(ReadCacheRuntime::new(
            session,
            capacity,
            budgets,
            Some(local),
        )));
        self.track_holder_in_flight();
        Ok(self)
    }

    pub(super) fn cache_completed_ranges(
        &self,
        batch: &ReadBatch,
        sources: &[ChunkSources],
        out: &[u8],
    ) {
        let (Some(cache), Some(version)) = (&self.distributed_cache, &batch.file_version_id) else {
            return;
        };
        let Ok(authority) = self.capture_native_read_authority() else {
            return;
        };
        if authority.epoch == 0 {
            return;
        }
        for op in &batch.ops {
            let Some(set) = sources.iter().find(|set| set.chunk_id == op.chunk_id) else {
                continue;
            };
            let Some(chunk) = set.chunk.as_ref() else {
                continue;
            };
            if op.length == 0
                || chunk.length > crate::node::chunk::MAX_STAGED_CHUNK_BYTES as u64
                || chunk.length.saturating_add(4096) > cache.capacity
                || !range_covered(&set.allowed_ranges, 0, chunk.length)
                || op
                    .chunk_offset
                    .checked_add(op.length)
                    .is_none_or(|end| end > chunk.length)
            {
                continue;
            }
            let Some(body) =
                out.get(op.output_offset..op.output_offset.saturating_add(op.length as usize))
            else {
                continue;
            };
            cache.stage(
                &self.local,
                batch,
                version,
                chunk,
                authority.epoch,
                op.chunk_offset as usize,
                body,
                self.trusted_crc_table(&chunk.id),
            );
        }
    }

    /// Two supervised loops. The installer only drains staged bodies, so a
    /// slow Meta round never leaves the bounded queue full (that dropped
    /// bodies before, D1); the publisher runs as soon as an install wakes it,
    /// again at once while unconfirmed intents remain, and once per second
    /// to re-check confirmed copies.
    pub(crate) fn start_distributed_cache(
        self: &Arc<Self>,
        services: &mut crate::runtime::Services,
    ) {
        let Some(cache) = &self.distributed_cache else {
            return;
        };
        let Some(mut receive) = cache
            .receive
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take()
        else {
            return;
        };
        let engine = self.clone();
        let capacity = cache.capacity;
        let stop = crate::runtime::cancelled(services.stop.subscribe());
        services.spawn(async move {
            tokio::pin!(stop);
            loop {
                tokio::select! {
                    biased;
                    _ = &mut stop => return Ok(()),
                    job = receive.recv() => {
                        let Some(job) = job else { return Ok(()) };
                        engine.run_install_job(job, capacity).await;
                    }
                }
            }
        });
        if let Some(mut segments) = cache
            .segment_receive
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take()
        {
            let engine = self.clone();
            let stop = crate::runtime::cancelled(services.stop.subscribe());
            services.spawn(async move {
                tokio::pin!(stop);
                let Some(cache) = engine.distributed_cache.clone() else {
                    return Ok(());
                };
                let mut sweep = tokio::time::interval(PARTIAL_MAX_AGE / 2);
                sweep.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    tokio::select! {
                        biased;
                        _ = &mut stop => return Ok(()),
                        segment = segments.recv() => {
                            let Some(segment) = segment else { return Ok(()) };
                            cache.assemble(&engine.local, segment);
                        }
                        _ = sweep.tick() => cache.sweep_partial(&engine.local, Instant::now()),
                    }
                }
            });
        }
        let engine = self.clone();
        let stop = crate::runtime::cancelled(services.stop.subscribe());
        services.spawn(async move {
            tokio::pin!(stop);
            let Some(cache) = engine.distributed_cache.clone() else {
                return Ok(());
            };
            let mut interval = tokio::time::interval(cache.recheck_period);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let mut state = CacheSyncState::default();
            loop {
                tokio::select! {
                    biased;
                    _ = &mut stop => return Ok(()),
                    _ = cache.publish_wake.notified() => {}
                    _ = interval.tick() => {}
                }
                let deadline = Instant::now() + engine.read_pipeline_timeout();
                let started = Instant::now();
                let pass = engine.reconcile_read_caches(&mut state, deadline).await;
                engine
                    .local
                    .record_read_cache_work("reconcile", started.elapsed());
                match pass {
                    // Re-run at once only while it makes progress; a pass that
                    // only failed waits for the tick and per-intent backoff.
                    Ok(summary)
                        if summary.pending > 0 && summary.published + summary.retired > 0 =>
                    {
                        cache.publish_wake.notify_one()
                    }
                    Ok(_) => {}
                    Err(error) => {
                        afs_logging::warn!("dfs.read_cache_reconcile_retry"; "error" => error.to_string());
                    }
                }
            }
        });
    }

    /// Installs one staged or explicitly requested body and wakes publication.
    pub(super) async fn run_install_job(self: &Arc<Self>, mut job: CacheJob, capacity: u64) {
        self.local
            .record_read_cache_work("queue_wait", job.queued_at.elapsed());
        let started = Instant::now();
        let explicit = job.explicit.take();
        let result = match &explicit {
            Some((deadline, _)) if Instant::now() >= *deadline => {
                Err(unavailable("DFS prefetch cache installation expired"))
            }
            Some(_) => self.capture_native_read_authority().and_then(|authority| {
                if authority.epoch == job.epoch {
                    Ok(())
                } else {
                    Err(external_denied(
                        "DFS prefetch cache installer epoch changed",
                    ))
                }
            }),
            None => Ok(()),
        };
        let result = match result {
            Ok(()) => {
                let installer = self.clone();
                let required = explicit.is_some();
                crate::node::bridge::spawn_blocking(move || {
                    installer.install_read_cache(job, capacity, required)
                })
                .await
                .map_err(|error| unavailable(format!("DFS read cache worker failed: {error}")))
                .and_then(|result| result)
            }
            Err(error) => Err(error),
        };
        self.local
            .record_read_cache_work("install", started.elapsed());
        match &result {
            Ok(true) => {
                self.local.record_read_cache_event("installed");
                if let Some(cache) = &self.distributed_cache {
                    cache.publish_wake.notify_one();
                }
            }
            Ok(false) => self.local.record_read_cache_event("install_skipped"),
            Err(_) => self.local.record_read_cache_event("install_failed"),
        }
        let result = result.map(|_| ());
        if let Some((_, completed)) = explicit {
            let _ = completed.send(result);
        } else if let Err(error) = result {
            afs_logging::warn!("dfs.read_cache_install_failed"; "error" => error.to_string());
        }
    }

    /// Returns whether a new cache copy was recorded for publication.
    fn install_read_cache(&self, job: CacheJob, capacity: u64, required: bool) -> Result<bool> {
        let body = job
            .shared
            .as_ref()
            .map_or_else(|| job.body.into(), |(_, body)| body.clone());
        let (item, reused) = crate::node::chunk::StagedChunk::from_replica_with_table(
            crate::dfs::OperationId::new("read-cache"),
            &job.chunk,
            body,
            job.table.as_ref(),
        )?;
        self.local.record_read_cache_event(if reused {
            "crc_table_reused"
        } else {
            "crc_table_built"
        });
        if let Some(receipt) = self
            .local
            .persist_read_cache(&item, job.epoch, capacity)?
            .receipt
        {
            let intent = ReadCacheIntent {
                namespace_id: self.namespace_id.clone(),
                file_version_id: job.version,
                layout_root_id: job.layout,
                copy: crate::dfs::read_cache_copy(&receipt)?,
            };
            // `false`: the copy changed under us and nothing new is tracked.
            let tracked = self.local.track_read_cache(intent.clone())?;
            if tracked && let Some(cache) = &self.distributed_cache {
                cache.note_fresh_install(&intent);
            }
            return Ok(tracked);
        } else if required && self.local.check_cache_resident(&job.chunk.id).is_err() {
            return Err(Error::coded(
                afs_error::IO_CAPACITY,
                "DFS read cache could not install requested Chunk",
            ));
        }
        Ok(false)
    }
}

impl CacheJob {
    #[cfg(test)]
    pub(super) fn with_table(mut self, table: Arc<crate::node::chunk_crc32c::CrcTable>) -> Self {
        self.table = Some(table);
        self
    }

    #[cfg(test)]
    pub(super) fn body(&self) -> &[u8] {
        self.shared
            .as_ref()
            .map_or(&self.body, |(_, body)| body.as_ref())
    }

    pub(super) fn explicit(mut self, deadline: Instant) -> (Self, oneshot::Receiver<Result<()>>) {
        let (complete, result) = oneshot::channel();
        self.explicit = Some((deadline, complete));
        (self, result)
    }

    pub(super) fn new(
        chunk: ChunkObject,
        body: Vec<u8>,
        version: FileVersionId,
        layout: LayoutRootId,
        epoch: u64,
    ) -> Self {
        Self {
            chunk,
            body,
            shared: None,
            version,
            layout,
            epoch,
            explicit: None,
            charge: None,
            queued_at: Instant::now(),
            table: None,
            in_flight: None,
        }
    }
}

#[cfg(test)]
impl DfsReadEngine {
    pub(crate) fn checkpoint_stage(
        &self,
        chunk: &ChunkObject,
        version: &FileVersionId,
        layout: &LayoutRootId,
        data: &[u8],
        offset: usize,
        length: usize,
    ) {
        self.remember_trusted_crc_table(
            chunk,
            crate::node::chunk_crc32c::CrcTable::build(data).unwrap(),
        );
        let batch = ReadBatch {
            file_version_id: Some(version.clone()),
            layout_root_id: layout.clone(),
            ops: vec![ChunkReadOp {
                chunk_id: chunk.id.clone(),
                chunk_offset: offset as u64,
                length: length as u64,
                output_offset: 0,
            }],
            lookahead: Vec::new(),
        };
        let sources = [ChunkSources {
            chunk_id: chunk.id.clone(),
            chunk: Some(chunk.clone()),
            sources: Vec::new(),
            allowed_ranges: vec![(0, chunk.length)],
        }];
        self.cache_completed_ranges(&batch, &sources, &data[offset..offset + length]);
    }

    pub(crate) fn checkpoint_assemble(&self) {
        self.distributed_cache
            .as_ref()
            .unwrap()
            .assemble_pending(&self.local);
    }

    pub(crate) fn checkpoint_after_freeze(&self, hook: impl FnOnce() + Send + 'static) {
        *self
            .distributed_cache
            .as_ref()
            .unwrap()
            .after_freeze
            .lock()
            .unwrap() = Some(Box::new(hook));
    }

    pub(crate) async fn checkpoint_install(self: &Arc<Self>) {
        let cache = self.distributed_cache.as_ref().unwrap();
        for job in cache.queued_jobs_for_test() {
            self.run_install_job(job, cache.capacity()).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn h2_pr297_segment_budget_counts_allocated_capacity() {
        const MIB: usize = 1024 * 1024;
        let temp = tempfile::tempdir().unwrap();
        let local = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let cache =
            ReadCacheRuntime::new("session".into(), 1 << 30, ReadCacheBudgets::default(), None);
        let version = FileVersionId::new("version");
        let batch = ReadBatch {
            file_version_id: Some(version.clone()),
            layout_root_id: LayoutRootId::new("layout"),
            ops: Vec::new(),
            lookahead: Vec::new(),
        };
        for seed in 0..32 {
            let chunk = crate::node::chunk::StagedChunk::new(
                crate::dfs::OperationId::new("segments"),
                vec![seed; 4 * MIB],
            )
            .chunk;
            cache.stage(
                &local,
                &batch,
                &version,
                &chunk,
                1,
                0,
                &[seed; 128 * 1024],
                None,
            );
        }
        let mut receiver = cache.segment_receive.lock().unwrap().take().unwrap();
        let segments: Vec<_> = std::iter::from_fn(|| receiver.try_recv().ok()).collect();
        let allocated: usize = segments.iter().map(|segment| segment.data.capacity()).sum();
        let charged = cache
            .segment_budget
            .used
            .load(std::sync::atomic::Ordering::Relaxed);
        assert!(
            allocated as u64 <= cache.segment_budget.limit,
            "allocated={allocated} charged={charged} limit={}",
            cache.segment_budget.limit
        );
        // The charge covers every byte of capacity the queued buffers hold.
        assert!(
            allocated as u64 <= charged,
            "allocated={allocated} charged={charged}"
        );
    }

    /// !297 P2: pooled segment buffers keep their charge while idle and hand
    /// it on when reused, so the pool never holds uncharged capacity.
    #[test]
    fn h2_pr297_pooled_segment_buffers_stay_charged() {
        const MIB: usize = 1024 * 1024;
        let temp = tempfile::tempdir().unwrap();
        let local = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let cache =
            ReadCacheRuntime::new("session".into(), 1 << 30, ReadCacheBudgets::default(), None);
        let version = FileVersionId::new("version");
        let batch = ReadBatch {
            file_version_id: Some(version.clone()),
            layout_root_id: LayoutRootId::new("layout"),
            ops: Vec::new(),
            lookahead: Vec::new(),
        };
        let chunk = crate::node::chunk::StagedChunk::new(
            crate::dfs::OperationId::new("pool"),
            vec![3; 4 * MIB],
        )
        .chunk;
        for part in 0..4 {
            cache.stage(
                &local,
                &batch,
                &version,
                &chunk,
                1,
                part * MIB,
                &[3; MIB],
                None,
            );
        }
        cache.assemble_pending(&local);
        let pooled: u64 = cache
            .segment_pool
            .lock()
            .unwrap()
            .iter()
            .map(|(buffer, _)| buffer.capacity() as u64)
            .sum();
        assert!(pooled > 0);
        let charged = cache
            .segment_budget
            .used
            .load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(
            charged, pooled,
            "idle pooled buffers are exactly what is charged"
        );
    }

    #[test]
    fn management_prefetch_shares_install_outcome_and_reopens_after_failure() {
        let cache =
            ReadCacheRuntime::new("session".into(), 1024, ReadCacheBudgets::default(), None);
        let chunk = ChunkId::new("one");
        let (_, leader) = cache.prefetch_flight(&chunk).unwrap();
        let follower = cache.prefetch_flight(&chunk).unwrap();
        assert!(follower.1.is_none());
        let capacity = Error::coded(afs_error::IO_CAPACITY, "cache full");
        leader.unwrap().send_replace(Some(Err(capacity.clone())));
        assert_eq!(*follower.0.borrow(), Some(Err(capacity)));
        assert!(cache.prefetch_flight(&chunk).unwrap().1.is_some());
    }

    /// Acceptance finding (2026-11-20): FUSE completes several reads of one
    /// Chunk concurrently (kernel readahead keeps up to ~11 tasks in flight,
    /// even with one reader thread). A segment whose completion finds the
    /// partial table locked must not leave its Chunk unassemblable: today it
    /// is dropped (`try_lock`), the partial body can never complete, and it
    /// holds 4 MiB of the partial budget until newer Chunks push it out.
    #[test]
    fn concurrent_segment_completion_does_not_lose_the_chunk() {
        let temp = tempfile::tempdir().unwrap();
        let local = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let cache =
            ReadCacheRuntime::new("session".into(), 1 << 30, ReadCacheBudgets::default(), None);
        let body: Vec<u8> = (0..4 * 1024 * 1024).map(|i| (i % 251) as u8).collect();
        let staged = crate::node::chunk::StagedChunk::new(
            crate::dfs::OperationId::new("segments"),
            body.clone(),
        );
        let chunk = staged.chunk.clone();
        let version = FileVersionId::new("version");
        let batch = ReadBatch {
            lookahead: Vec::new(),
            file_version_id: Some(version.clone()),
            layout_root_id: LayoutRootId::new("layout"),
            ops: Vec::new(),
        };
        let segment = 1024 * 1024;
        let stage = |index: usize| {
            let offset = index * segment;
            cache.stage(
                &local,
                &batch,
                &version,
                &chunk,
                1,
                offset,
                &body[offset..offset + segment],
                None,
            );
        };
        stage(0);
        {
            // Another FUSE completion of this Chunk holds the table right now.
            let _busy = cache.partial.lock().unwrap();
            stage(1);
        }
        stage(2);
        stage(3);
        cache.assemble_pending(&local);
        let mut receive = cache.receive.lock().unwrap().take().unwrap();
        let queued = std::iter::from_fn(|| receive.try_recv().ok()).count();
        assert_eq!(
            queued,
            1,
            "the Chunk was lost; partial bytes still held: {}",
            cache
                .partial_budget
                .used
                .load(std::sync::atomic::Ordering::SeqCst)
        );
    }

    #[test]
    fn distribution_cache_accumulates_out_of_order_verified_ranges_without_holes() {
        let mut partial = PartialBytes {
            data: vec![0; 10],
            ..Default::default()
        };
        assert!(!partial.copy_range(7, b"789", 10));
        assert!(!partial.copy_range(0, b"0123", 10));
        assert!(!partial.copy_range(2, b"234", 10));
        assert!(partial.copy_range(5, b"56", 10));
        assert_eq!(partial.data, b"0123456789");
    }
}
