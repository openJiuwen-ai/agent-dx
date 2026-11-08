//! Visible state, checkpoint guards, pending mutations and in-flight admission.

use super::*;

pub(super) struct Visible {
    pub(super) metrics: StoreMetrics,
    pub(super) committed: ArcSwap<State>,
    pub(super) checkpoint_active: AtomicBool,
    pub(super) checkpoint_wakeup: Arc<Notify>,
    pub(super) checkpoint_finished: Arc<Notify>,
    /// Any failed durable write may mean another authority has taken over, or
    /// that the local disk is damaged. Refuse even reads until a fresh process
    /// reloads the backend; stale grants must never be served as current.
    pub(super) poisoned: AtomicBool,
    /// Effective maintenance: `None` when not configured or when the backend
    /// cannot checkpoint. Gates the checkpoint loop and the WAL capacity wait.
    pub(super) maintenance: Option<journal::MaintenancePolicy>,
    pub(super) capture_charge_max_bytes: usize,
}

pub(super) struct CheckpointGuard {
    pub(super) visible: Arc<Visible>,
    pub(super) started: Instant,
    pub(super) completed: bool,
}

impl Drop for CheckpointGuard {
    fn drop(&mut self) {
        let metrics = &self.visible.metrics;
        metrics
            .checkpoint_duration
            .observe(self.started.elapsed().as_secs_f64());
        if !self.completed {
            metrics.checkpoint_failures.inc();
        }
        metrics.checkpoint_inflight.set(0);
        self.visible
            .checkpoint_active
            .store(false, Ordering::Release);
        self.visible.checkpoint_finished.notify_waiters();
    }
}

pub(super) enum Mutation {
    Transaction(MetaTxn),
    RegisterNode(RequestKey, NodeSessionLease),
}

pub(super) const PENDING_INPUT_BYTES: usize = 128 * 1024 * 1024;
/// How long a write waits for queued input to drain before the retryable busy
/// error. Shorter than the Node's default 5 s Meta RPC deadline, so the caller
/// sees the cause rather than its own timeout.
pub(super) const PENDING_INPUT_WAIT: Duration = Duration::from_secs(3);

/// Who is queueing input. Background commits (GC batches) also hold a
/// sub-quota of half the queue, so they never take foreground room.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum InputPriority {
    Foreground,
    Background,
}

/// Queued Meta input bytes (R71a). A full queue is backpressure: a write waits
/// up to `wait` for earlier input to commit. Only a charge that can never fit
/// is `IO_CAPACITY`; a queue that stays full is `META_STORE_BUSY` (EAGAIN).
pub(super) struct InputQueue {
    pub(super) total: Arc<Semaphore>,
    total_limit: usize,
    background: Arc<Semaphore>,
    background_limit: usize,
    wait: Duration,
}

/// Input charge held until the writer has settled the group.
pub(super) struct InputCharge {
    _total: OwnedSemaphorePermit,
    _background: Option<OwnedSemaphorePermit>,
}

impl InputQueue {
    pub(super) fn new(total_limit: usize, wait: Duration) -> Self {
        let background_limit = total_limit / 2;
        Self {
            total: Arc::new(Semaphore::new(total_limit)),
            total_limit,
            background: Arc::new(Semaphore::new(background_limit)),
            background_limit,
            wait,
        }
    }

    pub(super) async fn admit(&self, bytes: usize, priority: InputPriority) -> Result<InputCharge> {
        let limit = match priority {
            InputPriority::Foreground => self.total_limit,
            InputPriority::Background => self.background_limit,
        };
        if bytes > limit {
            return Err(journal::full("Meta input exceeds the queue capacity"));
        }
        let permits =
            u32::try_from(bytes).map_err(|_| journal::full("queued Meta input charge overflow"))?;
        let deadline = tokio::time::Instant::now() + self.wait;
        let background = match priority {
            InputPriority::Background => {
                Some(Self::acquire(&self.background, permits, deadline).await?)
            }
            InputPriority::Foreground => None,
        };
        Ok(InputCharge {
            _total: Self::acquire(&self.total, permits, deadline).await?,
            _background: background,
        })
    }

    async fn acquire(
        semaphore: &Arc<Semaphore>,
        permits: u32,
        deadline: tokio::time::Instant,
    ) -> Result<OwnedSemaphorePermit> {
        if let Ok(permit) = Arc::clone(semaphore).try_acquire_many_owned(permits) {
            return Ok(permit);
        }
        match tokio::time::timeout_at(deadline, Arc::clone(semaphore).acquire_many_owned(permits))
            .await
        {
            Ok(Ok(permit)) => Ok(permit),
            Ok(Err(_)) => Err(unavailable("Meta input queue is closed")),
            Err(_) => Err(Error::coded(
                afs_error::META_STORE_BUSY,
                "queued Meta input stayed full until the admission wait expired",
            )),
        }
    }
}

pub(super) const PROTECTED_KEY_BYTES: u64 = 4 * 1024 * 1024;

impl Mutation {
    pub(super) fn capacity_charge(&self) -> Result<u64> {
        match self {
            Self::Transaction(txn) => history::transaction_charge(txn),
            Self::RegisterNode(key, lease) => history::registration_charge(key, lease),
        }
    }
    fn protect(
        &self,
        keys: &mut std::collections::BTreeSet<RequestKey>,
        bytes: &mut u64,
        limit: usize,
    ) -> Result<()> {
        let mut add = |key: &RequestKey| -> Result<()> {
            if keys.contains(key) {
                return Ok(());
            }
            let next = bytes
                .checked_add(history::protection_charge(key)?)
                .ok_or_else(|| unavailable("protected request bytes overflow"))?;
            if keys.len() == limit || next > PROTECTED_KEY_BYTES {
                return Err(unavailable(
                    "prepared request protection capacity exhausted",
                ));
            }
            keys.insert(key.clone());
            *bytes = next;
            Ok(())
        };
        match self {
            Self::RegisterNode(key, _) => add(key),
            Self::Transaction(txn) => {
                add(&txn.request)?;
                for mutation in &txn.mutations {
                    if let TxnMutation::RecordRequestOutcome(outcome) = mutation {
                        add(&outcome.request)?;
                    }
                }
                Ok(())
            }
        }
    }
    pub(super) fn operation_name(&self) -> &'static str {
        match self {
            Self::Transaction(txn) => txn.operation.as_str(),
            Self::RegisterNode(_, _) => StoreOperation::RegisterNode.as_str(),
        }
    }
}

/// Admission identities survive caller cancellation until the writer settles.
/// The same short mutex serializes admission and retirement selection; it is
/// never held across backend I/O. All copies use the existing protection budget.
#[derive(Default)]
pub(super) struct InFlightState {
    pub(super) active: std::collections::BTreeMap<Arc<RequestKey>, usize>,
    pub(super) retiring: std::collections::BTreeSet<Arc<RequestKey>>,
    pub(super) bytes: u64,
    pub(super) failed: bool,
}

pub(super) struct InFlight {
    pub(super) state: Mutex<InFlightState>,
    pub(super) limit: usize,
}

pub(super) struct InFlightGuard {
    owner: Arc<InFlight>,
    keys: Vec<Arc<RequestKey>>,
    retirement: bool,
    holder_bytes: u64,
}

impl InFlight {
    fn charge(key: &RequestKey) -> Result<u64> {
        // Registry, queued holder, selection snapshot and retirement holder.
        history::protection_charge(key)?
            .checked_mul(4)
            .ok_or_else(|| unavailable("in-flight identity charge overflow"))
    }
    pub(super) fn admit(self: &Arc<Self>, operation: &Mutation) -> Result<InFlightGuard> {
        let mut keys = std::collections::BTreeSet::new();
        operation.protect(&mut keys, &mut 0, self.limit)?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| unavailable("in-flight identity lock poisoned"))?;
        let holder_bytes = 128u64 + keys.len() as u64 * 32;
        if state.failed {
            return Err(unavailable("in-flight identity invariant failed"));
        }
        let mut extra = holder_bytes;
        let mut count = 0;
        for key in &keys {
            if state.retiring.contains(key) {
                return Err(unavailable(
                    "request outcome retirement in progress; retry admission",
                ));
            }
            if !state.active.contains_key(key) {
                count += 1;
                extra += Self::charge(key)?;
            }
        }
        if state.active.len() + state.retiring.len() + count > self.limit
            || state
                .bytes
                .checked_add(extra)
                .is_none_or(|n| n > PROTECTED_KEY_BYTES)
        {
            return Err(unavailable(
                "in-flight request protection capacity exhausted",
            ));
        }
        state.bytes += extra;
        let held = keys
            .into_iter()
            .map(|key| {
                let key = state
                    .active
                    .get_key_value(&key)
                    .map(|(key, _)| Arc::clone(key))
                    .unwrap_or_else(|| Arc::new(key));
                *state.active.entry(Arc::clone(&key)).or_default() += 1;
                key
            })
            .collect();
        Ok(InFlightGuard {
            owner: Arc::clone(self),
            keys: held,
            retirement: false,
            holder_bytes,
        })
    }
    pub(super) fn prepare_history(
        self: &Arc<Self>,
        current: &State,
        now: u64,
        mut limits: history::Limits,
    ) -> Result<Option<(State, history::Delta, InFlightGuard)>> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| unavailable("in-flight identity lock poisoned"))?;
        if state.failed {
            return Err(unavailable("in-flight identity invariant failed"));
        }
        let protected = state
            .active
            .keys()
            .chain(state.retiring.iter())
            .map(|key| key.as_ref().clone())
            .collect();
        let slots = self
            .limit
            .saturating_sub(state.active.len() + state.retiring.len());
        if slots == 0 {
            return Ok(None);
        }
        limits.cleanup_items = limits.cleanup_items.min(slots);
        loop {
            let Some((next, delta)) = current.prepare_history(now, limits, &protected)? else {
                return Ok(None);
            };
            let extra = delta.requests.iter().try_fold(0u64, |sum, entry| {
                sum.checked_add(Self::charge(&entry.key)?)
                    .ok_or_else(|| unavailable("retirement identity charge overflow"))
            })?;
            if state
                .bytes
                .checked_add(extra)
                .is_some_and(|n| n <= PROTECTED_KEY_BYTES)
            {
                let keys: Vec<_> = delta
                    .requests
                    .iter()
                    .map(|entry| Arc::new(entry.key.clone()))
                    .collect();
                state.retiring.extend(keys.iter().cloned());
                state.bytes += extra;
                return Ok(Some((
                    next,
                    delta,
                    InFlightGuard {
                        owner: Arc::clone(self),
                        keys,
                        retirement: true,
                        holder_bytes: 0,
                    },
                )));
            }
            if limits.cleanup_items <= 1 {
                return Ok(None);
            }
            limits.cleanup_items /= 2;
        }
    }
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        // Poisoning is fail-closed at the next admission/cleanup; never panic
        // while dropping an already-settled caller's protection.
        if let Ok(mut state) = self.owner.state.lock() {
            if let Some(bytes) = state.bytes.checked_sub(self.holder_bytes) {
                state.bytes = bytes;
            } else {
                state.failed = true;
            }
            for key in &self.keys {
                let removed = if self.retirement {
                    state.retiring.remove(key)
                } else if let Some(count) = state.active.get_mut(key) {
                    *count -= 1;
                    if *count == 0 {
                        state.active.remove(key);
                        true
                    } else {
                        false
                    }
                } else {
                    false
                };
                if removed {
                    match InFlight::charge(key)
                        .ok()
                        .and_then(|charge| state.bytes.checked_sub(charge))
                    {
                        Some(bytes) => state.bytes = bytes,
                        None => state.failed = true,
                    }
                }
            }
        }
    }
}

pub(super) struct Pending {
    pub(super) identity: InFlightGuard,
    pub(super) capacity: Option<InputCharge>,
    pub(super) operation: Mutation,
    pub(super) reply: oneshot::Sender<Result<TxnOutcome>>,
}
