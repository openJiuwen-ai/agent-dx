//! `PeerRangeSource`: verified peer ranges with candidate rotation.
//!
//! Each operation walks its own candidate list in Meta order. Operations on
//! the same peer context share one batch attempt; a failed batch retries each
//! operation once alone on the same copy before that operation advances.

use super::*;

/// The fixed inputs of one rotation over a batch.
struct PeerRangeRound<'r> {
    batch: &'r ReadBatch,
    sources: &'r [ChunkSources],
    version: &'r FileVersionId,
    final_owned: bool,
    deadline: Instant,
}

/// Rotation state: next candidate per operation, operations still to read
/// in the next pass, and the error returned once a list is exhausted.
struct PeerRangeProgress {
    /// Remote candidates per operation, in source order; a busy-refresh may
    /// replace one operation's list with Meta's current one (D1).
    candidates: Vec<Vec<SourceCandidate>>,
    /// Operations whose candidates were already refreshed once.
    refreshed: Vec<bool>,
    /// Operations whose sources were re-authorized after a cache holder
    /// refused its grant (!297 P1), at most once each.
    reauthorized: Vec<bool>,
    next: Vec<usize>,
    pending: Vec<usize>,
    last_error: Option<Error>,
    /// Wait before re-asking a holder that answered with backpressure.
    busy_backoff: Duration,
}

/// Least time left in a read for a busy-refresh lookup to be worth it.
const MIN_REFRESH_BUDGET: Duration = Duration::from_millis(50);
/// Longest wait for one in-read source lookup (busy refresh or
/// re-authorization); never more than half of the read's time left, so the
/// read keeps time to retry the copies it already has (!297).
const MAX_REFRESH_WAIT: Duration = Duration::from_millis(250);

/// Backpressure retries mirror the replica sender (R71b): 5ms doubling to 200ms.
const BUSY_BACKOFF_START: Duration = Duration::from_millis(5);
const BUSY_BACKOFF_MAX: Duration = Duration::from_millis(200);

/// One remote read's scope (see [`keeping_fetched`]).
struct Fetched {
    /// In-flight marks of the Chunks this read fetched from a copy, kept
    /// until the read has staged them (review/18 a1).
    marks: std::cell::RefCell<Vec<crate::node::read::holders::InFlightGuard>>,
    /// Whether this read may ask a holder to wait (!318 P2-4).
    holder_wait: bool,
    /// When this read stops trying holders (!318 round 2 item 2): one
    /// deadline for all its groups and parallel children.
    holder_deadline: Option<Instant>,
}

/// Holders together get at most this share (1/n) of a read's time left when
/// it starts; the rest stays with the copies it already has.
const HOLDER_READ_SHARE: u32 = 2;

tokio::task_local! {
    static FETCHED: Fetched;
    /// The holder deadline of the logical read this task serves (!318 round
    /// 3 item 1): fixed once, before the read is split into parts.
    static LOGICAL_HOLDER_DEADLINE: Instant;
}

/// Runs one logical read (all its parts, groups, parallel children and
/// retries) under one holder deadline: its start plus 1/[`HOLDER_READ_SHARE`]
/// of the time left until `deadline`. Nested calls keep the outer deadline.
pub(super) async fn for_logical_read<T>(
    deadline: Instant,
    read: impl std::future::Future<Output = T>,
) -> T {
    if LOGICAL_HOLDER_DEADLINE.try_with(|_| ()).is_ok() {
        return read.await;
    }
    let now = Instant::now();
    LOGICAL_HOLDER_DEADLINE
        .scope(
            now + deadline.saturating_duration_since(now) / HOLDER_READ_SHARE,
            read,
        )
        .await
}

/// Runs `read` with a scope that keeps the in-flight marks of the Chunks it
/// fetches until it ends.
///
/// `holder_wait`: whether the read may ask holders to wait for a Chunk in
/// flight there. Only a read of one Chunk may (!318 P2-4): a read of several
/// keeps each Chunk it fetched in flight until all of them are staged, so
/// two such reads asking each other for their second Chunks would each wait
/// for a Chunk the other installs only after its own wait. A read that may
/// wait marks nothing but that one Chunk, for which waits only go to
/// higher-ranked holders (re-review P2), so no wait cycle can form.
///
/// `deadline`: the read's own. Its holder attempts, in every group and
/// parallel child, end by one shared holder deadline: the read's start plus
/// 1/[`HOLDER_READ_SHARE`] of its time left. Holders not yet tried by then
/// are skipped, so several silent holders cannot take the time of the copies
/// one Chunk after another (!318 round 2 item 2).
pub(super) async fn keeping_fetched<T>(
    holder_wait: bool,
    deadline: Option<Instant>,
    read: impl std::future::Future<Output = T>,
) -> T {
    let now = Instant::now();
    // The logical read's, when one is set: parts of one read share it.
    let holder_deadline = LOGICAL_HOLDER_DEADLINE
        .try_with(|limit| *limit)
        .ok()
        .or_else(|| {
            deadline
                .map(|deadline| now + deadline.saturating_duration_since(now) / HOLDER_READ_SHARE)
        });
    FETCHED
        .scope(
            Fetched {
                marks: std::cell::RefCell::new(Vec::new()),
                holder_wait,
                holder_deadline,
            },
            read,
        )
        .await
}

/// The current read's shared holder deadline, if it has one.
fn holder_deadline() -> Option<Instant> {
    FETCHED
        .try_with(|fetched| fetched.holder_deadline)
        .ok()
        .flatten()
        .or_else(|| LOGICAL_HOLDER_DEADLINE.try_with(|limit| *limit).ok())
}

/// The deadline of one attempt: the read's, and for a holder also the
/// read's shared holder deadline.
fn attempt_deadline(deadline: Instant, holder: bool) -> Instant {
    match holder_deadline() {
        Some(limit) if holder => deadline.min(limit),
        _ => deadline,
    }
}

/// Keeps `marks` until the enclosing [`keeping_fetched`] scope ends, or
/// drops them now outside one.
fn keep_fetched(marks: Vec<crate::node::read::holders::InFlightGuard>) {
    let _ = FETCHED.try_with(move |fetched| fetched.marks.borrow_mut().extend(marks));
}

/// Whether the current read may ask a holder to wait; none outside a
/// [`keeping_fetched`] scope.
pub(crate) fn holder_wait_allowed() -> bool {
    FETCHED
        .try_with(|fetched| fetched.holder_wait)
        .unwrap_or(false)
}

impl DfsReadEngine {
    /// Measurement only (review/18 §8.3): times one remote read stage until
    /// the returned timer drops, whatever the stage's end (!318 P3-2).
    pub(super) fn time_route(
        &self,
        stage: &'static str,
    ) -> Option<crate::node::metrics::work::ShareTimer> {
        self.local
            .read_work_metrics()
            .map(|metrics| metrics.share.route(stage))
    }

    pub(super) async fn read_remote_ranges_async(
        &self,
        batch: &ReadBatch,
        sources: &[ChunkSources],
        out: &mut [u8],
        final_owned: bool,
        deadline: Option<Instant>,
    ) -> Result<()> {
        let version = batch
            .file_version_id
            .clone()
            .ok_or_else(|| invalid("DFS peer read requires a fixed FileVersion"))?;
        let candidates: Vec<Vec<SourceCandidate>> = batch
            .ops
            .iter()
            .map(|op| {
                sources
                    .iter()
                    .find(|set| set.chunk_id == op.chunk_id)
                    .map(|set| {
                        set.sources
                            .iter()
                            .filter(|source| self.is_remote_candidate(source))
                            .cloned()
                            .collect()
                    })
                    .unwrap_or_default()
            })
            .collect();
        let round = PeerRangeRound {
            batch,
            sources,
            version: &version,
            final_owned,
            deadline: deadline.unwrap_or_else(|| Instant::now() + self.read_pipeline_timeout()),
        };
        let mut progress = PeerRangeProgress {
            candidates,
            refreshed: vec![false; batch.ops.len()],
            reauthorized: vec![false; batch.ops.len()],
            next: vec![0; batch.ops.len()],
            pending: (0..batch.ops.len()).collect(),
            last_error: None,
            busy_backoff: BUSY_BACKOFF_START,
        };
        while !progress.pending.is_empty() {
            let groups = Self::group_pending_ranges(&round, &mut progress)?;
            progress.pending.clear();
            for group in groups {
                self.read_range_group(&round, &group, out, &mut progress)
                    .await?;
            }
        }
        Ok(())
    }

    /// Group pending operations by peer context. CRC-segment objects are
    /// always read alone. An operation without a next candidate ends the read.
    fn group_pending_ranges(
        round: &PeerRangeRound<'_>,
        progress: &mut PeerRangeProgress,
    ) -> Result<Vec<Vec<usize>>> {
        let (batch, candidates, next) = (round.batch, &progress.candidates, &progress.next);
        let mut groups: Vec<Vec<usize>> = Vec::new();
        for &index in &progress.pending {
            let Some(source) = candidates[index].get(next[index]) else {
                return Err(progress
                    .last_error
                    .take()
                    .unwrap_or_else(|| corrupt("committed Chunk has no readable copy")));
            };
            if let Some(group) = groups.iter_mut().find(|group| {
                let first = group[0];
                !round.sources.iter().any(|set| {
                    (set.chunk_id == batch.ops[index].chunk_id
                        || set.chunk_id == batch.ops[first].chunk_id)
                        && set
                            .chunk
                            .as_ref()
                            .is_some_and(ChunkObject::is_segment_crc32c)
                }) && same_peer_context(&candidates[first][next[first]], source)
            }) {
                group.push(index);
            } else {
                groups.push(vec![index]);
            }
        }
        Ok(groups)
    }

    /// One batch attempt for a group on each operation's current candidate.
    async fn read_range_group(
        &self,
        round: &PeerRangeRound<'_>,
        group: &[usize],
        out: &mut [u8],
        progress: &mut PeerRangeProgress,
    ) -> Result<()> {
        let batch = round.batch;
        let holder_group = group.iter().all(|&index| {
            progress.candidates[index][progress.next[index]]
                .grant
                .is_holder()
        });
        if holder_group && Instant::now() >= attempt_deadline(round.deadline, true) {
            // The read's holder time is over: go on to the copies.
            self.local
                .record_read_cache_event("holder_skipped_deadline");
            progress.last_error = Some(Error::coded(
                afs_error::NODE_TRANSFER_NOT_READY,
                "the read's holder deadline passed",
            ));
            for &index in group {
                progress.next[index] += 1;
                progress.pending.push(index);
            }
            return Ok(());
        }
        let mut length = 0;
        let operations = group
            .iter()
            .map(|&index| {
                let mut op = batch.ops[index].clone();
                op.output_offset = length;
                length += op.length as usize; // validated by ReadBatch
                ResolvedReadOp {
                    op,
                    source: progress.candidates[index][progress.next[index]].clone(),
                }
            })
            .collect();
        let attempt = PeerReadBatch {
            file_version_id: round.version.clone(),
            layout_root_id: batch.layout_root_id.clone(),
            operations,
        };
        // Only the private, single contiguous final response may be
        // used directly. Borrowed output and complex batches retain
        // per-attempt isolation and the existing retry scheduler.
        let direct = round.final_owned
            && batch.ops.len() == 1
            && group.len() == 1
            && batch.ops[0].output_offset == 0
            && length == out.len();
        let mut scratch = if direct { Vec::new() } else { vec![0; length] };
        let integrity = round
            .sources
            .iter()
            .find(|set| set.chunk_id == batch.ops[group[0]].chunk_id)
            .and_then(|set| set.chunk.as_ref())
            .filter(|chunk| chunk.is_segment_crc32c());
        let target = if direct { &mut *out } else { &mut scratch };
        let fetch = async {
            if let Some(chunk) = integrity {
                let table = self
                    .transfer
                    .read_crc32c_range_table_async(&attempt, chunk, target)
                    .await?;
                if let Some(table) = table {
                    self.remember_trusted_crc_table(chunk, table);
                }
                Ok(())
            } else {
                self.transfer.read_ranges_async(&attempt, target).await
            }
        };
        let choices = group
            .iter()
            .map(|&i| progress.candidates[i].len() - progress.next[i])
            .min()
            .unwrap_or(1);
        // review/18 a1: in flight only while fetched from a copy that is not
        // a holder (a holder may itself be waiting for this reader).
        // Nothing is tracked without sharing (!318 P2-5).
        let flights = self
            .distributed_cache
            .as_ref()
            .and_then(|cache| cache.in_flight.get());
        let marks: Vec<_> = match flights {
            Some(flights)
                if attempt
                    .operations
                    .iter()
                    .all(|item| !item.source.grant.is_holder()) =>
            {
                attempt
                    .operations
                    .iter()
                    .map(|item| flights.begin(&item.op.chunk_id))
                    .collect()
            }
            _ => Vec::new(),
        };
        let role = {
            let source = &attempt.operations[0].source;
            if source.grant.is_holder() {
                "peer_holder"
            } else if source.role == CopyRole::VerifiedCache {
                "verified_cache"
            } else {
                "durable"
            }
        };
        // Recorded as `cancelled` if this read is dropped during the attempt.
        let timer = self
            .local
            .read_work_metrics()
            .map(|metrics| metrics.share.peer_attempt(role));
        let holder = attempt
            .operations
            .iter()
            .all(|item| item.source.grant.is_holder());
        #[cfg(test)]
        if holder {
            self.test_hooks
                .holder_attempts
                .lock()
                .unwrap()
                .push((Instant::now(), attempt_deadline(round.deadline, true)));
        }
        let result = self
            .try_peer_before(
                attempt_deadline(round.deadline, holder),
                choices,
                holder,
                fetch,
            )
            .await;
        if let Some(timer) = timer {
            let outcome = match &result {
                Ok(()) => "ok",
                Err(error) if crate::node::fault::classify::peer_not_ready(error) => "not_ready",
                Err(error) if crate::node::fault::classify::retryable_backpressure(error) => "busy",
                Err(error) if crate::node::fault::classify::read_authority_rejected(error) => {
                    "denied"
                }
                Err(_) => "error",
            };
            timer.end(outcome);
        }
        if result.is_ok() {
            keep_fetched(marks);
        } else {
            drop(marks);
        }
        match result {
            Ok(()) => {
                if attempt
                    .operations
                    .iter()
                    .any(|item| !self.source_matches(batch, &item.source))
                {
                    return Err(external_denied(
                        "native peer read authority expired before publication",
                    ));
                }
                for item in &attempt.operations {
                    self.record_source_bytes(&item.source, item.op.length);
                }
                if direct {
                    return Ok(());
                }
                for (&index, item) in group.iter().zip(&attempt.operations) {
                    let original = &batch.ops[index];
                    let len = original.length as usize;
                    self.local
                        .record_read_copy("attempt_to_staging", original.length);
                    out[original.output_offset..original.output_offset + len].copy_from_slice(
                        &scratch[item.op.output_offset..item.op.output_offset + len],
                    );
                }
                Ok(())
            }
            Err(error) => {
                // H2 step 2: a version holder that has no current copy, or no
                // longer accepts this read, is skipped; it is not a failed
                // copy and does not end the read.
                if attempt
                    .operations
                    .iter()
                    .all(|item| item.source.grant.is_holder())
                {
                    let not_ready = crate::node::fault::classify::peer_not_ready(&error);
                    let denied = crate::node::fault::classify::read_authority_rejected(&error);
                    // A holder out of validation or read slots is busy, not a
                    // bad copy: skip it without demotion and keep this read's
                    // sources (code review S2, N5).
                    let busy = crate::node::fault::classify::retryable_backpressure(&error);
                    if not_ready || denied || busy {
                        if not_ready {
                            self.local
                                .record_read_cache_event("holder_answered_not_ready");
                        } else if busy {
                            self.local.record_read_cache_event("holder_answered_busy");
                        } else if let Some(lists) = &self.holder_lists {
                            lists.forget(round.version, &batch.layout_root_id);
                        }
                        progress.last_error = Some(error);
                        for &index in group {
                            progress.next[index] += 1;
                            progress.pending.push(index);
                        }
                        return Ok(());
                    }
                }
                self.invalidate_native_batch_on_error(batch, &error);
                if crate::node::fault::classify::read_authority_rejected(&error) {
                    // A cache holder refuses a grant once its copy is retired
                    // (Deleting at Meta). Ask Meta again for this version and
                    // layout: a valid caller gets current sources and grants;
                    // an invalid one is refused there too, and the read ends
                    // with the denial as before (!297 P1).
                    let error = self
                        .retry_after_denial(round, group, &attempt, progress, error)
                        .await?;
                    progress.last_error = Some(error);
                    progress.pending.extend_from_slice(group);
                    return Ok(());
                }
                self.capture_native_read_authority()?;
                // A full holder is backpressure, not a bad copy. When it is the
                // last candidate for some operation in the group, wait and ask it
                // again within the read deadline instead of ending the read with
                // EAGAIN; with other candidates left, move on as before.
                if crate::node::fault::classify::retryable_backpressure(&error) && choices <= 1 {
                    if self.refresh_busy_candidates(round, group, progress).await {
                        // A holder published since the lookup: try it now
                        // instead of backing off on the busy writer.
                        progress.last_error = Some(error);
                        progress.pending.extend_from_slice(group);
                        return Ok(());
                    }
                    if Instant::now() + progress.busy_backoff >= round.deadline {
                        // No other copy and no time left: the read timed out.
                        return Err(Error::coded(
                            afs_error::CLIENT_DEADLINE_EXCEEDED,
                            format!(
                                "DFS peer stayed busy until the read deadline: {}",
                                error.message()
                            ),
                        ));
                    }
                    tokio::time::sleep(progress.busy_backoff).await;
                    progress.busy_backoff = (progress.busy_backoff * 2).min(BUSY_BACKOFF_MAX);
                    progress.last_error = Some(error);
                    progress.pending.extend_from_slice(group);
                    return Ok(());
                }
                // A batch error does not say which copy failed; a lone one does.
                if let [item] = attempt.operations.as_slice() {
                    self.note_suspect_copy(&error, item);
                    self.demote_cache_copy(&item.source);
                }
                if self.distribution.parallelism > 1
                    && error.kind() == afs_error::ErrorKind::DeadlineExceeded
                {
                    progress.last_error = Some(error);
                    for &index in group {
                        progress.next[index] += 1;
                        progress.pending.push(index);
                    }
                    return Ok(());
                }
                progress.last_error = Some(error);
                self.retry_group_singly(round, group, &attempt, out, progress)
                    .await
            }
        }
    }

    /// One attempt within its share of the read's time left: `1/choices`
    /// with parallel reads, and always for a holder (!318 P1), which is
    /// optional and never the last copy, so a slow or silent holder leaves
    /// the remaining copies their time. Otherwise the whole time left.
    async fn try_peer_before(
        &self,
        deadline: Instant,
        choices: usize,
        holder: bool,
        fetch: impl std::future::Future<Output = Result<()>>,
    ) -> Result<()> {
        if self.distribution.parallelism <= 1 && !holder {
            return fetch.await;
        }
        let budget = deadline.saturating_duration_since(Instant::now())
            / u32::try_from(choices.max(1)).unwrap_or(u32::MAX);
        tokio::time::timeout(budget, fetch).await.map_err(|_| {
            Error::coded(
                afs_error::CLIENT_DEADLINE_EXCEEDED,
                "peer attempt exhausted its share of the read deadline",
            )
        })?
    }

    /// D1: the last candidate of some operation answered busy. While this
    /// Node runs the native read cache, ask Meta once per operation for its
    /// current sources, bypassing the cached reply: a cache copy published
    /// after the lookup becomes a new first choice. Returns whether any
    /// operation gained a candidate. A failed refresh is not a read error;
    /// the caller keeps its busy backoff.
    async fn refresh_busy_candidates(
        &self,
        round: &PeerRangeRound<'_>,
        group: &[usize],
        progress: &mut PeerRangeProgress,
    ) -> bool {
        if self.distributed_cache.is_none() {
            return false;
        }
        // Never spend the read's own deadline on metadata: with too little
        // time left keep the busy backoff (review MEDIUM).
        if round.deadline.saturating_duration_since(Instant::now()) < MIN_REFRESH_BUDGET {
            return false;
        }
        let indices: Vec<usize> = group
            .iter()
            .copied()
            .filter(|&index| !progress.refreshed[index])
            .collect();
        if indices.is_empty() {
            return false;
        }
        let ops: Vec<ChunkReadOp> = indices
            .iter()
            .map(|&index| round.batch.ops[index].clone())
            .collect();
        for &index in &indices {
            progress.refreshed[index] = true;
        }
        let refresh = ReadBatch {
            file_version_id: Some(round.version.clone()),
            layout_root_id: round.batch.layout_root_id.clone(),
            ops,
            lookahead: Vec::new(),
        };
        // Drop this read's cached entries unless another read on this Node
        // already refreshed them, then look up through the shared per-version
        // gate: concurrent refreshes here wait for one Meta query and reuse
        // its reply. The waiter is bounded by the read deadline.
        self.drop_unrefreshed_sources(&refresh, progress, &indices);
        let fresh = match self.lookup_in_read(round, &refresh).await {
            Ok(fresh) => fresh,
            Err(error) => {
                afs_logging::debug!("dfs.read_busy_refresh_failed"; "error" => error.to_string());
                return false;
            }
        };
        let mut grew = false;
        for &index in &indices {
            let chunk = &round.batch.ops[index].chunk_id;
            let Some(mut set) = fresh.iter().find(|set| &set.chunk_id == chunk).cloned() else {
                continue;
            };
            set.sources.retain(|s| self.source_matches(round.batch, s));
            self.order_candidates(round.batch, &mut set);
            let current = &progress.candidates[index];
            let candidates: Vec<SourceCandidate> = set
                .sources
                .into_iter()
                .filter(|source| self.is_remote_candidate(source))
                .collect();
            let gained = candidates
                .iter()
                .any(|new| !current.iter().any(|old| old.copy_id == new.copy_id));
            if gained {
                progress.candidates[index] = candidates;
                progress.next[index] = 0;
                grew = true;
            }
        }
        if grew {
            self.local.record_read_cache_event("busy_refreshed");
        }
        grew
    }

    /// One source lookup inside a read, bounded by its own short deadline
    /// ([`MAX_REFRESH_WAIT`], at most half the time left) instead of the
    /// read's deadline: a slow Meta cannot use up the time the read
    /// needs to retry the copies it already has.
    async fn lookup_in_read(
        &self,
        round: &PeerRangeRound<'_>,
        refresh: &ReadBatch,
    ) -> Result<Vec<ChunkSources>> {
        let left = round.deadline.saturating_duration_since(Instant::now());
        let deadline = Instant::now() + MAX_REFRESH_WAIT.min(left / 2);
        self.sources_for_attempt_async(refresh, true, deadline, Arc::new(()))
            .await
    }

    /// After an authority denial: `Ok(error)` when the operations got fresh
    /// sources and grants and should be read again; `Err` ends the read with
    /// the denial, or with Meta's own refusal of the caller.
    async fn retry_after_denial(
        &self,
        round: &PeerRangeRound<'_>,
        group: &[usize],
        attempt: &PeerReadBatch,
        progress: &mut PeerRangeProgress,
        error: Error,
    ) -> Result<Error> {
        match self
            .reauthorize_after_cache_denial(round, group, attempt, progress)
            .await
        {
            Ok(true) => Ok(error),
            Ok(false) => Err(error),
            Err(lookup) if crate::node::fault::classify::read_authority_rejected(&lookup) => {
                Err(lookup)
            }
            Err(_) => Err(error),
        }
    }

    /// !297 P1: a cache holder answered PermissionDenied (its copy was
    /// retired after this read's lookup). Re-acquire sources and grants for
    /// the same version and layout, once per operation and within the read
    /// deadline, without the refused copies. `Ok(false)`: nothing to retry
    /// (no cache copy involved, no time, or no other copy); `Err`: Meta
    /// refused the lookup (for example the caller itself is no longer valid).
    async fn reauthorize_after_cache_denial(
        &self,
        round: &PeerRangeRound<'_>,
        group: &[usize],
        attempt: &PeerReadBatch,
        progress: &mut PeerRangeProgress,
    ) -> Result<bool> {
        let refused: BTreeSet<&crate::dfs::CopyId> = attempt
            .operations
            .iter()
            .filter(|item| item.source.role == CopyRole::VerifiedCache)
            .map(|item| &item.source.copy_id)
            .collect();
        if refused.is_empty() {
            return Ok(false);
        }
        for item in &attempt.operations {
            self.demote_cache_copy(&item.source);
        }
        let indices: Vec<usize> = group
            .iter()
            .copied()
            .filter(|&index| !progress.reauthorized[index])
            .collect();
        if indices.is_empty()
            || round.deadline.saturating_duration_since(Instant::now()) < MIN_REFRESH_BUDGET
        {
            return Ok(false);
        }
        for &index in &indices {
            progress.reauthorized[index] = true;
        }
        let refresh = ReadBatch {
            file_version_id: Some(round.version.clone()),
            layout_root_id: round.batch.layout_root_id.clone(),
            ops: indices
                .iter()
                .map(|&index| round.batch.ops[index].clone())
                .collect(),
            lookahead: Vec::new(),
        };
        let fresh = self.lookup_in_read(round, &refresh).await?;
        let mut replaced = Vec::new();
        for &index in &indices {
            let chunk = &round.batch.ops[index].chunk_id;
            let Some(mut set) = fresh.iter().find(|set| &set.chunk_id == chunk).cloned() else {
                return Ok(false);
            };
            set.sources.retain(|s| self.source_matches(round.batch, s));
            self.order_candidates(round.batch, &mut set);
            let candidates: Vec<SourceCandidate> = set
                .sources
                .into_iter()
                .filter(|source| {
                    self.is_remote_candidate(source) && !refused.contains(&source.copy_id)
                })
                .collect();
            if candidates.is_empty() {
                return Ok(false);
            }
            replaced.push((index, candidates));
        }
        for (index, candidates) in replaced {
            progress.candidates[index] = candidates;
            progress.next[index] = 0;
        }
        self.local.record_read_cache_event("denied_reauthorized");
        Ok(true)
    }

    /// Removes cached sources of the refreshed Chunks whose cached copy set
    /// is still the one this read started with.
    fn drop_unrefreshed_sources(
        &self,
        refresh: &ReadBatch,
        progress: &PeerRangeProgress,
        indices: &[usize],
    ) {
        let Some(version) = refresh.file_version_id.as_ref() else {
            return;
        };
        let Ok(mut cache) = self.source_cache.lock() else {
            return;
        };
        let mut stale = BTreeSet::new();
        for (op, index) in refresh.ops.iter().zip(indices) {
            let key = (
                version.clone(),
                refresh.layout_root_id.clone(),
                op.chunk_id.clone(),
            );
            let refreshed = cache.get(&key).is_some_and(|cached| {
                cached
                    .sources
                    .sources
                    .iter()
                    .filter(|s| self.is_remote_candidate(s))
                    .any(|s| {
                        !progress.candidates[*index]
                            .iter()
                            .any(|old| old.copy_id == s.copy_id)
                    })
            });
            if !refreshed {
                stale.insert(op.chunk_id.clone());
            }
        }
        cache.retain(|key, _| {
            &key.0 != version || key.1 != refresh.layout_root_id || !stale.contains(&key.2)
        });
        cache.record(&self.local);
    }

    /// Keeps a table whose digest is this Chunk's trusted Meta identity.
    pub(super) fn remember_trusted_crc_table(
        &self,
        chunk: &ChunkObject,
        table: crate::node::chunk_crc32c::CrcTable,
    ) {
        // With CRC checking off the bytes were not verified: never reuse.
        if self.distributed_cache.is_none()
            || self.local.peer_read_crc_mode() == crate::config::DfsReadCrcMode::Off
            || !table.valid_shape(chunk.length)
            || table.descriptor_digest() != chunk.content_digest
        {
            return;
        }
        if let Ok(mut tables) = self.trusted_crc_tables.lock()
            && tables.get(&chunk.id).is_none()
        {
            tables.insert(chunk.id.clone(), Arc::new(table));
        }
    }

    /// The remembered trusted table of a Chunk, if any.
    pub(super) fn trusted_crc_table(
        &self,
        chunk: &ChunkId,
    ) -> Option<Arc<crate::node::chunk_crc32c::CrcTable>> {
        self.trusted_crc_tables.lock().ok()?.get(chunk).cloned()
    }

    /// D0: reader-side payload by the role of the copy that served it, so a
    /// fan-out shows who supplied whom without inferring it from senders.
    pub(super) fn record_source_bytes(&self, source: &SourceCandidate, bytes: u64) {
        let role = match source.role {
            _ if source.grant.is_holder() => "peer_holder",
            CopyRole::VerifiedCache => "verified_cache",
            _ => "durable",
        };
        self.local.record_read_source_bytes(role, bytes);
    }

    /// Queues the copy for its holder's review when its bytes failed this
    /// reader's content verification (R59). Never waits.
    fn note_suspect_copy(&self, error: &Error, item: &ResolvedReadOp) {
        if !crate::node::fault::classify::remote_copy_content_suspect(error) {
            return;
        }
        if let (
            CopyLocation::Node {
                node_id,
                node_epoch,
                ..
            },
            Some(endpoint),
        ) = (&item.source.location, item.source.node_data_endpoint())
        {
            self.suspect_copies
                .report(crate::node::fault::suspect::SuspectCopy {
                    node_id: node_id.clone(),
                    node_epoch: *node_epoch,
                    endpoint: endpoint.to_owned(),
                    chunk_id: item.source.chunk_id.clone(),
                });
        }
    }

    /// A batch error does not identify which copy failed. Before discarding
    /// a valid source for its neighbours, retry each operation on that same
    /// source once with a fresh scratch. Each candidate gets at most one
    /// batch and one single-op attempt; failed singles advance exactly once.
    async fn retry_group_singly(
        &self,
        round: &PeerRangeRound<'_>,
        group: &[usize],
        attempt: &PeerReadBatch,
        out: &mut [u8],
        progress: &mut PeerRangeProgress,
    ) -> Result<()> {
        let batch = round.batch;
        for (index, item) in group.iter().copied().zip(&attempt.operations) {
            if group.len() > 1 {
                let mut single = item.clone();
                single.op.output_offset = 0;
                let single_batch = PeerReadBatch {
                    file_version_id: round.version.clone(),
                    layout_root_id: batch.layout_root_id.clone(),
                    operations: vec![single],
                };
                let mut single_scratch = vec![0; item.op.length as usize];
                let choices = progress.candidates[index].len() - progress.next[index];
                let fetch = self
                    .transfer
                    .read_ranges_async(&single_batch, &mut single_scratch);
                let holder = item.source.grant.is_holder();
                match self
                    .try_peer_before(
                        attempt_deadline(round.deadline, holder),
                        choices,
                        holder,
                        fetch,
                    )
                    .await
                {
                    Ok(()) => {
                        if !self.source_matches(batch, &item.source) {
                            return Err(external_denied(
                                "native peer read authority expired before publication",
                            ));
                        }
                        let original = &batch.ops[index];
                        self.record_source_bytes(&item.source, original.length);
                        self.local
                            .record_read_copy("attempt_to_staging", original.length);
                        out[original.output_offset..original.output_offset + single_scratch.len()]
                            .copy_from_slice(&single_scratch);
                        continue;
                    }
                    Err(error) => {
                        self.invalidate_native_batch_on_error(batch, &error);
                        if crate::node::fault::classify::read_authority_rejected(&error) {
                            // Same rule as a batch denial (!297 P1).
                            let error = self
                                .retry_after_denial(round, &[index], &single_batch, progress, error)
                                .await?;
                            progress.last_error = Some(error);
                            progress.pending.push(index);
                            continue;
                        }
                        self.capture_native_read_authority()?;
                        self.note_suspect_copy(&error, item);
                        if !crate::node::fault::classify::retryable_backpressure(&error) {
                            self.demote_cache_copy(&item.source);
                        }
                        progress.last_error = Some(error);
                    }
                }
            }
            progress.next[index] += 1;
            progress.pending.push(index);
        }
        Ok(())
    }
}
