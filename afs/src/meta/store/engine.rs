//! The Store engine: open, checkpoint loop, log commit loop and the MetaStore implementation.

use super::*;

pub struct Store {
    pub(super) in_flight: Arc<InFlight>,
    pub(super) read_budget: Arc<ReadBudget>,
    pub(super) input: InputQueue,
    pub(super) visible: Arc<Visible>,
    pub(super) writes: mpsc::Sender<Pending>,
    backend: Arc<dyn journal::MetaLog>,
    pub(super) next_request_id: AtomicU64,
}

/// Store-side options; the log backend owns none of these.
#[derive(Clone, Copy, Debug)]
pub(crate) struct StoreOptions {
    pub(crate) history: history::Limits,
    /// Checkpoint schedule and WAL capacity wait. `None` runs neither; it is
    /// also treated as `None` when the backend does not support checkpoints.
    pub(crate) maintenance: Option<journal::MaintenancePolicy>,
    /// Logical charge a checkpoint capture may hold before encoding; at least
    /// the backend's `checkpoint_limits().image_max_bytes`.
    pub(crate) capture_charge_max_bytes: usize,
}
impl Default for StoreOptions {
    fn default() -> Self {
        Self {
            history: history::Limits::default(),
            maintenance: Some(journal::MaintenancePolicy::default()),
            capture_charge_max_bytes: 2 * 1024 * 1024 * 1024,
        }
    }
}

impl Store {
    #[cfg(test)]
    pub(super) async fn open_log_with_read_limits(
        log: Arc<dyn journal::MetaLog>,
        limits: ReadLimits,
    ) -> Result<Self> {
        Self::open_log_with_policies(log, StoreOptions::default(), limits).await
    }
    #[cfg(test)]
    pub(super) fn read_budget_stats(&self) -> ReadBudgetStats {
        self.read_budget.stats()
    }
    /// Replays already-decided records before serving. Does not re-execute
    /// client requests, conditions, clocks, Node IO, or external storage IO.
    /// Uses the default maintenance policy when the backend supports checkpoints.
    pub async fn open_log(log: Arc<dyn journal::MetaLog>) -> Result<Self> {
        Self::open_log_with_options(log, StoreOptions::default()).await
    }
    #[cfg(test)]
    pub(crate) async fn open_log_with_history(
        log: Arc<dyn journal::MetaLog>,
        history: history::Limits,
    ) -> Result<Self> {
        Self::open_log_with_options(
            log,
            StoreOptions {
                history,
                ..StoreOptions::default()
            },
        )
        .await
    }
    pub(crate) async fn open_log_with_options(
        log: Arc<dyn journal::MetaLog>,
        options: StoreOptions,
    ) -> Result<Self> {
        Self::open_log_with_policies(log, options, ReadLimits::default()).await
    }
    async fn open_log_with_policies(
        log: Arc<dyn journal::MetaLog>,
        options: StoreOptions,
        reads: ReadLimits,
    ) -> Result<Self> {
        let history = options.history.validate()?;
        let reads = reads.validate()?;
        log.limits().validate()?;
        let maintenance = options
            .maintenance
            .map(journal::MaintenancePolicy::validate)
            .transpose()?
            .filter(|_| log.supports_checkpoint());
        let recovered = log.recover().await?;
        let image_max_bytes = log.checkpoint_limits().validate()?.image_max_bytes;
        if options.capture_charge_max_bytes < image_max_bytes {
            return Err(journal::invalid_log("invalid finite checkpoint limits"));
        }
        let metrics = StoreMetrics::new()
            .map_err(|error| unavailable(format!("Meta metrics initialization: {error}")))?;
        if let Some(image) = &recovered.checkpoint {
            StoreMetrics::set(&metrics.checkpoint_bytes, image.bytes.len() as u64);
            StoreMetrics::set(&metrics.checkpoint_sequence, image.covered.sequence);
        }
        let mut committed = match recovered.checkpoint {
            Some(image) => {
                if image.covered.sequence > recovered.tip.sequence {
                    return Err(journal::invalid_log("checkpoint exceeds recovered log tip"));
                }
                log::StateImage::decode(&image.bytes, image_max_bytes)?
            }
            None => State::default(),
        };
        for record in recovered.records {
            committed = committed.replay_record(&record)?;
        }
        committed.ensure_history_index()?;
        let mut recovered_tip = recovered.tip.at(recovered.term);
        // A lowered history budget cannot expose an over-budget authority.
        // Persist exact startup removals before service admission; failure does
        // not serve a partially prepared state or erase the original chain.
        while !committed.history_fits(history) {
            let Some((next, delta)) = committed.prepare_history(
                now_unix_ms(),
                history,
                &std::collections::BTreeSet::new(),
            )?
            else {
                return Err(journal::full(
                    "recovered history cannot fit configured limits",
                ));
            };
            let removed_requests = delta.requests.len() as u64;
            let removed_events = next.events_removed.saturating_sub(committed.events_removed);
            let record = log::Record::History(delta);
            let size = journal::frame_size(recovered_tip, &record)?;
            if size > log.limits().record_max_bytes || !log.can_append(recovered_tip, size).await? {
                return Err(journal::full(
                    "startup retention has no bounded WAL capacity",
                ));
            }
            recovered_tip =
                recovered_tip.advanced(log.append_and_sync(recovered_tip, vec![record]).await?);
            metrics.wal_batches.inc();
            metrics.wal_records.inc();
            metrics.wal_framed_bytes.inc_by(size as u64);
            metrics.request_evictions.inc_by(removed_requests);
            metrics.event_evictions.inc_by(removed_events);
            committed = next;
        }
        committed.log_cursor = recovered_tip;
        metrics.publish(&committed);
        let visible = Arc::new(Visible {
            metrics,
            committed: ArcSwap::from_pointee(committed),
            checkpoint_active: AtomicBool::new(false),
            checkpoint_wakeup: Arc::new(Notify::new()),
            checkpoint_finished: Arc::new(Notify::new()),
            poisoned: AtomicBool::new(false),
            maintenance,
            capture_charge_max_bytes: options.capture_charge_max_bytes,
        });
        if let Some(policy) = maintenance {
            tokio::spawn(run_checkpoint_loop(
                Arc::downgrade(&visible),
                Arc::downgrade(&log),
                Arc::clone(&visible.checkpoint_wakeup),
                policy,
            ));
        }
        let in_flight = Arc::new(InFlight {
            state: Mutex::new(InFlightState::default()),
            limit: history.protected_requests,
        });
        let (writes, receiver) = mpsc::channel(1024);
        tokio::spawn(run_log_commit_loop(
            receiver,
            Arc::clone(&visible),
            Arc::clone(&log),
            recovered_tip,
            history,
            Arc::clone(&in_flight),
        ));
        Ok(Self {
            in_flight,
            read_budget: ReadBudget::new(reads)?,
            input: InputQueue::new(PENDING_INPUT_BYTES, PENDING_INPUT_WAIT),
            visible,
            writes,
            backend: log,
            next_request_id: AtomicU64::new(1),
        })
    }

    /// Register this authority's fixed collectors with its process Registry.
    /// Call once before listener admission. Duplicate registration returns an
    /// error (possibly after partial registration) and never alters authority.
    pub fn register_metrics(
        &self,
        registry: &afs_metrics::Registry,
    ) -> std::result::Result<(), afs_metrics::MetricsError> {
        self.visible.metrics.register(registry)
    }

    /// Persist one already-published state without creating a business revision.
    /// Returns an error when the selected backend cannot install checkpoints.
    pub async fn checkpoint(&self) -> Result<journal::LogTip> {
        self.ensure_available()?;
        checkpoint_visible(Arc::clone(&self.visible), Arc::clone(&self.backend)).await
    }

    pub(super) fn ensure_available(&self) -> Result<()> {
        if self.visible.poisoned.load(Ordering::Acquire) {
            Err(unavailable(
                "store backend commit failed; restart Meta to reload authority",
            ))
        } else {
            Ok(())
        }
    }

    pub(super) async fn committed(&self) -> Result<Arc<State>> {
        self.ensure_available()?;
        let state = self.visible.committed.load_full();
        self.ensure_available()?;
        Ok(state)
    }
}

async fn checkpoint_visible(
    visible: Arc<Visible>,
    log: Arc<dyn journal::MetaLog>,
) -> Result<journal::LogTip> {
    let image_max_bytes = log.checkpoint_limits().validate()?.image_max_bytes;
    let capture_charge_max_bytes = visible.capture_charge_max_bytes;
    if visible
        .checkpoint_active
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return Err(unavailable("checkpoint already in progress"));
    }
    visible.metrics.checkpoint_inflight.set(1);
    let guard = CheckpointGuard {
        visible: Arc::clone(&visible),
        started: Instant::now(),
        completed: false,
    };
    // Dropping the caller/observer never aborts this task or releases its gate.
    tokio::spawn(async move {
        let mut guard = guard;
        if visible.poisoned.load(Ordering::Acquire) {
            return Err(unavailable("store unavailable before checkpoint capture"));
        }
        let state = visible.committed.load_full();
        let covered = state.log_cursor.tip;
        let bytes = tokio::task::spawn_blocking(move || {
            if log::StateImage::capture_charge(&state)? > capture_charge_max_bytes {
                return Err(journal::full("checkpoint capture logical charge exceeded"));
            }
            log::StateImage::encode(&state, image_max_bytes)
        })
        .await
        .map_err(|error| unavailable(format!("checkpoint encoder task: {error}")))??;
        let payload_bytes = bytes.len() as u64;
        log.install_checkpoint(covered, bytes).await?;
        StoreMetrics::set(&visible.metrics.checkpoint_bytes, payload_bytes);
        StoreMetrics::set(&visible.metrics.checkpoint_sequence, covered.sequence);
        visible.metrics.checkpoints.inc();
        guard.completed = true;
        Ok(covered)
    })
    .await
    .map_err(|error| unavailable(format!("checkpoint task: {error}")))?
}

async fn run_checkpoint_loop(
    visible: std::sync::Weak<Visible>,
    log: std::sync::Weak<dyn journal::MetaLog>,
    wakeup: Arc<Notify>,
    policy: journal::MaintenancePolicy,
) {
    let mut last_success = tokio::time::Instant::now();
    let mut last_attempt = None::<tokio::time::Instant>;
    loop {
        let forced =
            tokio::select! {_=wakeup.notified()=>true,_=tokio::time::sleep(policy.poll)=>false};
        let (Some(visible), Some(log)) = (visible.upgrade(), log.upgrade()) else {
            break;
        };
        if visible.poisoned.load(Ordering::Acquire) {
            break;
        }
        if visible.checkpoint_active.load(Ordering::Acquire)
            || last_attempt.is_some_and(|last| last.elapsed() < policy.poll)
        {
            continue;
        }
        let status = match log.status().await {
            Ok(status) => status,
            Err(error) => {
                last_attempt = Some(tokio::time::Instant::now());
                tracing::warn!(error=%error,"Meta checkpoint status failed");
                continue;
            }
        };
        let Some(records) = status.tip.sequence.checked_sub(status.checkpoint.sequence) else {
            tracing::warn!("Meta checkpoint position exceeds WAL tip");
            continue;
        };
        if !forced
            && (records == 0
                || (records < policy.wal_records
                    && status.uncheckpointed_bytes_upper_bound < policy.wal_bytes
                    && last_success.elapsed() < policy.interval))
        {
            continue;
        }
        last_attempt = Some(tokio::time::Instant::now());
        match tokio::time::timeout(policy.checkpoint_timeout, checkpoint_visible(visible, log))
            .await
        {
            Ok(Ok(_)) => last_success = tokio::time::Instant::now(),
            Ok(Err(error)) => {
                tracing::warn!(error=%error,"Meta checkpoint failed; old recovery chain retained")
            }
            Err(_) => tracing::warn!(
                "Meta checkpoint observation deadline exceeded; actual task retains its gate and charge"
            ),
        }
        // Only weak ownership crosses the next sleep, so an idle maintenance
        // loop cannot retain a retired Store or its backend root lock.
    }
}

async fn wait_log_capacity(
    visible: &Arc<Visible>,
    log: &Arc<dyn journal::MetaLog>,
    tip: journal::LogCursor,
    bytes: usize,
) -> Result<bool> {
    if log.can_append(tip, bytes).await? {
        return Ok(true);
    }
    let Some(policy) = visible.maintenance else {
        return Ok(false);
    };
    let deadline = tokio::time::Instant::now()
        .checked_add(policy.capacity_wait)
        .ok_or_else(|| journal::invalid_log("capacity wait deadline overflow"))?;
    loop {
        let finished = visible.checkpoint_finished.notified();
        if visible.poisoned.load(Ordering::Acquire) {
            return Err(unavailable(
                "store unavailable while waiting for WAL capacity",
            ));
        }
        if log.can_append(tip, bytes).await? {
            return Ok(true);
        }
        if tokio::time::Instant::now() >= deadline {
            return Ok(false);
        }
        visible.checkpoint_wakeup.notify_one();
        tokio::select! {_=finished=>{},_=tokio::time::sleep_until(deadline)=>return Ok(false),_=tokio::time::sleep(policy.poll)=>{}}
    }
}

enum LogGroup {
    Committed(journal::LogCursor),
    NotAppended,
}

type LogReply = (oneshot::Sender<Result<TxnOutcome>>, Result<TxnOutcome>);

async fn persist_log_group(
    visible: &Arc<Visible>,
    log: &Arc<dyn journal::MetaLog>,
    staged: &State,
    tip: journal::LogCursor,
    records: &mut Vec<log::Record>,
    replies: &mut Vec<LogReply>,
    framed_bytes: usize,
) -> Result<LogGroup> {
    let next = if records.is_empty() {
        tip
    } else {
        match wait_log_capacity(visible, log, tip, framed_bytes).await {
            Ok(true) => {}
            Ok(false) => {
                visible.metrics.wal_capacity_rejections.inc();
                records.clear();
                for (reply, _) in replies.drain(..) {
                    let _ = reply.send(Err(unavailable(
                        "WAL capacity wait expired before append; group not written",
                    )));
                }
                return Ok(LogGroup::NotAppended);
            }
            Err(error) => {
                visible.poisoned.store(true, Ordering::Release);
                for (reply, _) in replies.drain(..) {
                    let _ = reply.send(Err(group_failure("WAL admission failed", &error)));
                }
                return Err(error);
            }
        }
        let record_count = records.len() as u64;
        let request_evictions = records
            .iter()
            .map(|record| match record {
                log::Record::Decision {
                    history: Some(delta),
                    ..
                }
                | log::Record::History(delta) => delta.requests.len() as u64,
                _ => 0,
            })
            .sum::<u64>();
        let append_timer = profile::Timer::start();
        match log.append_and_sync(tip, std::mem::take(records)).await {
            Ok(next) => {
                let next = tip.advanced(next);
                let append_sync_ms = append_timer.ms();
                let publish_timer = profile::Timer::start();
                #[cfg(test)]
                crash_tests::pause(
                    "after_sync_before_publish",
                    &format!(
                        "visible={} staged={}",
                        visible.committed.load().revision.0,
                        staged.revision.0
                    ),
                );
                let mut published = staged.clone();
                published.log_cursor = next;
                let retained = published.history_stats();
                tracing::debug!(
                    history.requests = retained.requests,
                    history.request_bytes = retained.request_bytes,
                    history.events = retained.events,
                    history.event_bytes = retained.event_bytes,
                    history.index_base_bytes = retained.index_base_bytes,
                    "published bounded Meta history"
                );
                let published = Arc::new(published);
                let event_evictions = published
                    .events_removed
                    .saturating_sub(visible.committed.load().events_removed);
                visible.committed.store(Arc::clone(&published));
                visible.metrics.publish(&published);
                visible.metrics.wal_batches.inc();
                visible.metrics.wal_records.inc_by(record_count);
                visible.metrics.wal_framed_bytes.inc_by(framed_bytes as u64);
                visible.metrics.request_evictions.inc_by(request_evictions);
                visible.metrics.event_evictions.inc_by(event_evictions);
                if profile::enabled() {
                    profile::record(serde_json::json!({
                        "event": "afs_meta_store_profile", "backend": "canonical_log",
                        "bytes": framed_bytes, "records": record_count,
                        "revision": staged.revision.0,
                        "append_sync_ms": append_sync_ms, "publish_ms": publish_timer.ms()
                    }));
                }
                next
            }
            Err(error) => {
                visible.metrics.wal_failures.inc();
                visible.poisoned.store(true, Ordering::Release);
                for (reply, _) in replies.drain(..) {
                    let _ = reply.send(Err(group_failure("operation log commit failed", &error)));
                }
                return Err(error);
            }
        }
    };
    for (reply, result) in replies.drain(..) {
        let _ = reply.send(result);
    }
    Ok(LogGroup::Committed(next))
}

/// Resource deferral must not suppress request/event retention on the same tick.
enum AuthorityCleanup<'a> {
    Grants(&'a mut Option<MetaKey>),
    Commands(&'a mut Option<MetaKey>),
}

async fn persist_authority_cleanup(
    visible: &Arc<Visible>,
    log: &Arc<dyn journal::MetaLog>,
    tip: journal::LogCursor,
    cleanup: AuthorityCleanup<'_>,
    history: history::Limits,
    in_flight: &Arc<InFlight>,
) -> Result<journal::LogCursor> {
    let limits = log.limits();
    let current = visible.committed.load_full();
    let max_deletes = history.event_count.min(history.cleanup_items);
    let prepared = match cleanup {
        AuthorityCleanup::Commands(cursor) => current.prepare_root_command_cleanup(
            cursor,
            now_unix_ms(),
            limits.record_max_bytes,
            max_deletes,
        ),
        AuthorityCleanup::Grants(cursor) => current.prepare_root_grant_cleanup(
            cursor,
            now_unix_ms(),
            limits.record_max_bytes,
            max_deletes,
        ),
    };
    let (mut next, decision) = match prepared {
        Ok(Some(prepared)) => prepared,
        Ok(None) => return Ok(tip),
        Err(error) => {
            afs_logging::warn!("meta.authority_cleanup_deferred"; "error" => error.to_string());
            return Ok(tip);
        }
    };
    let retention = match in_flight.prepare_history(&next, now_unix_ms(), history) {
        Ok(retention) => retention,
        Err(error) => {
            afs_logging::warn!("meta.authority_cleanup_deferred"; "error" => error.to_string());
            return Ok(tip);
        }
    };
    let (delta, _retirement) = match retention {
        Some((cleaned, delta, guard)) => {
            next = cleaned;
            (Some(delta), Some(guard))
        }
        None => (None, None),
    };
    if !next.history_fits(history) {
        return Ok(tip);
    }
    let record = log::Record::Decision {
        decision,
        history: delta,
    };
    let size = match journal::frame_size(tip, &record) {
        Ok(size) if size <= limits.record_max_bytes => size,
        _ => {
            afs_logging::warn!("meta.authority_cleanup_deferred"; "reason" => "framed record exceeds configured limit");
            return Ok(tip);
        }
    };
    let mut records = vec![record];
    let mut replies = Vec::new();
    match persist_log_group(visible, log, &next, tip, &mut records, &mut replies, size).await? {
        LogGroup::Committed(next) => Ok(next),
        LogGroup::NotAppended => Ok(tip),
    }
}

/// Borrowed inputs shared by the commit loop's stages.
struct CommitContext<'a> {
    visible: &'a Arc<Visible>,
    log: &'a Arc<dyn journal::MetaLog>,
    history: history::Limits,
    in_flight: &'a Arc<InFlight>,
    limits: journal::AppendLimits,
}

/// Resume points of the bounded authority cleanup scans.
#[derive(Default)]
struct CleanupCursors {
    grants: Option<MetaKey>,
    commands: Option<MetaKey>,
}

/// One retention tick: grant and command cleanup, then request/event history.
/// `Err` stops the commit loop (the Store is then poisoned).
async fn cleanup_tick_step(
    context: &CommitContext<'_>,
    mut tip: journal::LogCursor,
    cursors: &mut CleanupCursors,
) -> Result<journal::LogCursor> {
    let CommitContext {
        visible,
        log,
        history,
        in_flight,
        limits,
    } = *context;
    tip = persist_authority_cleanup(
        visible,
        log,
        tip,
        AuthorityCleanup::Grants(&mut cursors.grants),
        history,
        in_flight,
    )
    .await?;
    tip = persist_authority_cleanup(
        visible,
        log,
        tip,
        AuthorityCleanup::Commands(&mut cursors.commands),
        history,
        in_flight,
    )
    .await?;
    let current = visible.committed.load_full();
    let Some((next, delta, _retirement)) =
        in_flight.prepare_history(&current, now_unix_ms(), history)?
    else {
        return Ok(tip);
    };
    let record = log::Record::History(delta);
    let size = match journal::frame_size(tip, &record) {
        Ok(size) if size <= limits.record_max_bytes => size,
        _ => return Ok(tip),
    };
    let mut records = vec![record];
    let mut replies = Vec::new();
    match persist_log_group(visible, log, &next, tip, &mut records, &mut replies, size).await? {
        LogGroup::Committed(next) => Ok(next),
        LogGroup::NotAppended => Ok(tip),
    }
}

/// A ready request prepared against the staged state of its group.
struct PreparedItem {
    next: State,
    outcome: TxnOutcome,
    record: Option<log::Record>,
    size: usize,
}

/// Decide one ready request against `staged`, attach retention and frame it at
/// its position in the group. `Err` is that request's reply; retention guards
/// already taken stay in `retirements` until the group settles.
fn prepare_ready_item(
    context: &CommitContext<'_>,
    staged: &State,
    operation: Mutation,
    group: (journal::LogCursor, usize),
    retirements: &mut Vec<InFlightGuard>,
) -> Result<PreparedItem> {
    let CommitContext {
        history,
        in_flight,
        limits,
        ..
    } = *context;
    let (tip, grouped) = group;
    let (mut next, outcome, record) = match operation {
        Mutation::Transaction(txn) => {
            staged.prepare_txn_record(txn, now_unix_ms(), limits.record_max_bytes)
        }
        Mutation::RegisterNode(request, lease) => {
            staged.prepare_node_record(request, lease, now_unix_ms(), limits.record_max_bytes)
        }
    }?;
    let record = match record {
        Some(decision) => {
            let cleanup = in_flight.prepare_history(&next, now_unix_ms(), history)?;
            let delta = cleanup.map(|(cleaned, delta, guard)| {
                retirements.push(guard);
                next = cleaned;
                delta
            });
            if !next.history_fits(history) {
                return Err(journal::full(
                    "protected prepared history exceeds count/byte limits",
                ));
            }
            Some(log::Record::Decision {
                decision,
                history: delta,
            })
        }
        None => None,
    };
    let size = match &record {
        Some(record) => {
            let sequence = tip
                .tip
                .sequence
                .checked_add(grouped as u64)
                .ok_or_else(|| journal::invalid_log("group sequence exhausted"))?;
            journal::frame_size(
                journal::LogTip {
                    sequence,
                    record_hash: tip.tip.record_hash,
                }
                .at(tip.term),
                record,
            )?
        }
        None => 0,
    };
    if size > limits.record_max_bytes {
        return Err(journal::full("record exceeds framed byte limit"));
    }
    Ok(PreparedItem {
        next,
        outcome,
        record,
        size,
    })
}

async fn run_log_commit_loop(
    mut receiver: mpsc::Receiver<Pending>,
    visible: Arc<Visible>,
    log: Arc<dyn journal::MetaLog>,
    mut tip: journal::LogCursor,
    history: history::Limits,
    in_flight: Arc<InFlight>,
) {
    let limits = log.limits();
    // The deadline must survive ready requests, including retries that produce
    // no new business record. Otherwise an active caller can starve expiry.
    let period = history.poll();
    let mut cleanup_tick = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
    cleanup_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut cursors = CleanupCursors::default();
    let context = CommitContext {
        visible: &visible,
        log: &log,
        history,
        in_flight: &in_flight,
        limits,
    };
    'receive: loop {
        let first = tokio::select! {
            biased;
            _=cleanup_tick.tick()=>{
                match cleanup_tick_step(&context, tip, &mut cursors).await {
                    Ok(next) => tip = next,
                    Err(_) => break,
                }
                continue;
            },
            item=receiver.recv()=>match item {Some(item)=>item,None=>break},
        };
        // Gather only ready work: no fixed 1ms delay on an otherwise idle Meta.
        let mut batch = VecDeque::from([first]);
        tokio::task::yield_now().await;
        while batch.len() < limits.batch_records_max {
            match receiver.try_recv() {
                Ok(item) => batch.push_back(item),
                Err(_) => break,
            }
        }
        let mut retirements = Vec::new();
        let mut input_holds = Vec::new();
        let mut staged = visible.committed.load_full().as_ref().clone();
        let mut records = Vec::new();
        let mut replies = Vec::new();
        let mut bytes = 0usize;
        while let Some(item) = batch.pop_front() {
            // A cancelled caller cannot release queued input capacity or its
            // prepared identity before this writer has settled the group.
            input_holds.push((item.capacity, item.identity));
            let prepared = match prepare_ready_item(
                &context,
                &staged,
                item.operation,
                (tip, records.len()),
                &mut retirements,
            ) {
                Ok(prepared) => prepared,
                Err(error) => {
                    replies.push((item.reply, Err(error)));
                    continue;
                }
            };
            if bytes
                .checked_add(prepared.size)
                .is_none_or(|total| total > limits.batch_max_bytes)
            {
                match persist_log_group(
                    &visible,
                    &log,
                    &staged,
                    tip,
                    &mut records,
                    &mut replies,
                    bytes,
                )
                .await
                {
                    Ok(LogGroup::Committed(next_tip)) => {
                        tip = next_tip;
                        bytes = 0;
                    }
                    Ok(LogGroup::NotAppended) => {
                        let _ = item
                            .reply
                            .send(Err(unavailable("earlier prepared group was not appended")));
                        for pending in batch.drain(..) {
                            let _ = pending.reply.send(Err(unavailable(
                                "ready batch rejected before preparation due to WAL capacity",
                            )));
                        }
                        continue 'receive;
                    }
                    Err(_) => break 'receive,
                }
            }
            bytes += prepared.size;
            staged = prepared.next;
            if let Some(record) = prepared.record {
                records.push(record);
            }
            replies.push((item.reply, Ok(prepared.outcome)));
        }
        match persist_log_group(
            &visible,
            &log,
            &staged,
            tip,
            &mut records,
            &mut replies,
            bytes,
        )
        .await
        {
            Ok(LogGroup::Committed(next)) => tip = next,
            Ok(LogGroup::NotAppended) => continue,
            Err(_) => break,
        }
    }
    visible.poisoned.store(true, Ordering::Release);
}

pub(super) fn tracing_result(result: std::result::Result<&TxnOutcome, &Error>) -> &'static str {
    match result {
        Ok(TxnOutcome::Committed { .. }) => "committed",
        Ok(TxnOutcome::ConditionFailed { .. }) => "condition_failed",
        Err(_) => "error",
    }
}

pub(super) fn unavailable(message: impl Into<String>) -> Error {
    Error::coded(afs_error::IO_UNAVAILABLE, message)
}

/// The error each request of a failed log group receives. A fencing refusal
/// keeps `META_LOG_FENCED` (nothing was written; a newer writer owns the
/// log); every other failure stays unavailable, as before.
fn group_failure(context: &str, error: &Error) -> Error {
    let code = if error.code() == afs_error::META_LOG_FENCED {
        afs_error::META_LOG_FENCED
    } else {
        afs_error::IO_UNAVAILABLE
    };
    Error::coded(code, format!("{context}: {error}"))
}

impl MetaStore for Store {
    fn health(&self) -> MetaFuture<'_, ()> {
        Box::pin(async move {
            self.ensure_available()?;
            self.backend.health().await?;
            self.ensure_available()
        })
    }

    fn backend_readiness(&self) -> MetaFuture<'_, BackendReadiness> {
        Box::pin(async move {
            let persistence = self.backend.persistence();
            if let Err(error) = self.ensure_available() {
                return Ok(BackendReadiness::observed(
                    persistence,
                    false,
                    format!("store unavailable before backend probe: {error}"),
                ));
            }
            match self.backend.health().await {
                Ok(()) => match self.ensure_available() {
                    Ok(()) => Ok(BackendReadiness::observed(
                        persistence,
                        true,
                        format!("{} backend health probe passed", persistence.as_str()),
                    )),
                    Err(error) => Ok(BackendReadiness::observed(
                        persistence,
                        false,
                        format!("store unavailable after backend probe: {error}"),
                    )),
                },
                Err(error) => Ok(BackendReadiness::observed(
                    persistence,
                    false,
                    format!("backend health probe failed: {error}"),
                )),
            }
        })
    }

    fn read_view(&self) -> MetaFuture<'_, MetaReadView> {
        Box::pin(async move {
            let state = self.committed().await?;
            self.read_budget.pin(state, &self.visible)
        })
    }

    fn read(&self, read: MetaRead) -> MetaFuture<'_, MetaSnapshot> {
        Box::pin(async move { self.committed().await?.read(read) })
    }

    fn compare_and_commit(&self, txn: MetaTxn) -> MetaFuture<'_, TxnOutcome> {
        Box::pin(async move {
            self.mutate(Mutation::Transaction(txn), InputPriority::Foreground)
                .await
        })
    }

    fn compare_and_commit_background(&self, txn: MetaTxn) -> MetaFuture<'_, TxnOutcome> {
        Box::pin(async move {
            self.mutate(Mutation::Transaction(txn), InputPriority::Background)
                .await
        })
    }

    fn watch(&self, after_revision: StoreRevision, limit: usize) -> MetaFuture<'_, WatchBatch> {
        Box::pin(async move { self.committed().await?.watch(after_revision, limit) })
    }

    fn register_node_session(
        &self,
        request: RequestKey,
        lease: NodeSessionLease,
    ) -> MetaFuture<'_, TxnOutcome> {
        Box::pin(async move {
            self.mutate(
                Mutation::RegisterNode(request, lease),
                InputPriority::Foreground,
            )
            .await
        })
    }

    fn scan_roots_for_recovery<'a>(
        &'a self,
        home_node_id: &'a str,
    ) -> MetaFuture<'a, RecoveryScan> {
        Box::pin(async move {
            self.committed()
                .await?
                .scan_roots_for_recovery(home_node_id)
        })
    }

    fn scan_owner_roots_for_recovery<'a>(
        &'a self,
        home_node_id: &'a str,
    ) -> MetaFuture<'a, OwnerRootScan> {
        Box::pin(async move {
            self.committed()
                .await?
                .scan_owner_roots_for_recovery(home_node_id)
        })
    }
}

/// Creates the explicit error used when no backend is configured.
pub fn unavailable_meta_store() -> Error {
    Error::coded(
        afs_error::META_STORE_UNIMPLEMENTED,
        "MetaStore backend is not configured",
    )
}
