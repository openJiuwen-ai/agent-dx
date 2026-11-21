//! Reader-side OwnerFs HIXL directory batches.
//!
//! A batch is the Home content of several small files of one directory,
//! fetched through one `PrepareBatchHixl` and one native batch READ. Unlike a
//! window, a batch outlives the open that triggered it, so a file is served
//! from a batch only when the open reading it sees the identity, size, mtime
//! and ctime the batch recorded (close-to-open), and only while this node has
//! not written the file since the batch was requested. An open claims its
//! file's slice once and keeps it until release, like a window.

use super::{BudgetLease, OwnerHixlReader, ReadTicket, WindowBudget, protocol_error};
use crate::node::data_plane::hixl::{SlotChecksum, checked_slot_length};
use crate::node::rpc::hixl::is_unsupported;
use crate::node::vfs::ownerfs::OPEN_PREFETCH_LIMIT;
use afs_protocol::node_data::{
    HixlLane, OwnerPrepareBatchHixlReply, OwnerPrepareBatchHixlRequest, RootAccess,
    owner_files_client::OwnerFilesClient,
};
use prost::bytes::Bytes;
use std::collections::{HashMap, VecDeque};
use std::ops::Range;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::watch;
use tonic::transport::Channel;

/// Resident batches of all directories may hold this many group sizes, or
/// half the host budget when that is more.
const BATCH_BUDGET_GROUPS: usize = 4;
const MAX_BATCHES_PER_PARENT: usize = 2;
const MAX_BATCH_PARENTS: usize = 256;
const MAX_INVALIDATED: usize = 4096;
/// A directory whose batch failed, or that has had nothing resident for this
/// long, may start a batch again on its next miss.
const PARENT_RETRY: Duration = Duration::from_secs(5);
/// A resident batch no open has claimed from for this long may make room for
/// another batch.
const BATCH_IDLE: Duration = Duration::from_secs(1);

/// The Home version an open or a batch saw.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct FileVersion {
    pub(super) size: u64,
    pub(super) mtime_ns: u64,
    pub(super) ctime_ns: u64,
}

/// Where the file of an eligible open sits for batches.
pub(super) struct BatchRoute {
    /// Root and parent path: one directory of this Home.
    pub(super) parent: Vec<u8>,
    pub(super) parent_path: Vec<u8>,
    pub(super) name: Vec<u8>,
    pub(super) version: FileVersion,
    /// Auto below `min_file_bytes`: a batch or the ordinary path serves the
    /// file, never a window.
    pub(super) batch_only: bool,
}

/// What an open should do about its file after asking the store.
pub(super) enum Claim {
    /// The file's slice; it now belongs to the open. A lookahead names the
    /// batch to fetch next.
    Hit(Bytes, Option<Vec<u8>>),
    /// A batch of this directory that may hold the file is in flight.
    Wait(watch::Receiver<Option<afs_error::Result<()>>>),
    /// This open should start a batch at its own name.
    Start,
    Failed(afs_error::Error),
    Miss,
}

struct BatchSlice {
    bytes: Bytes,
    version: FileVersion,
    batch: u64,
}

struct PendingBatch {
    anchor: Vec<u8>,
    lookahead: bool,
    done: watch::Receiver<Option<afs_error::Result<()>>>,
}

struct ParentBatches {
    /// Where the newest batch stopped; a miss exactly here continues.
    next: Option<Vec<u8>>,
    pending: Option<PendingBatch>,
    retry_after: Option<Instant>,
    failure: Option<(Vec<u8>, afs_error::Error)>,
    /// A batch an open needed found no room; the next miss may start one.
    starved: bool,
    touched: Instant,
}

struct ResidentBatch {
    id: u64,
    parent: Vec<u8>,
    /// Where the batch starts; its directory resumes here when the batch is
    /// dropped before any claim.
    anchor: Vec<u8>,
    bytes: usize,
    claimed: usize,
    unclaimed: usize,
    next: Vec<u8>,
    /// The batch after this one has started.
    lookahead: bool,
    /// A lookahead fetched this batch before any open asked for it.
    speculative: bool,
    /// Landed or last claimed from.
    touched: Instant,
}

/// A batch request that holds its budget until the batch lands.
pub(super) struct BatchStart {
    done: watch::Sender<Option<afs_error::Result<()>>>,
    generation: u64,
    reserved: usize,
    lease: BudgetLease,
}

/// Identity, version and byte range of each file a batch packed.
pub(super) type PackedFiles = Vec<(Vec<u8>, FileVersion, Range<usize>)>;

/// Verified bytes of one batch and where each packed file sits in them.
pub(super) struct FetchedBatch {
    pub(super) bytes: Vec<u8>,
    pub(super) files: PackedFiles,
    pub(super) next: Vec<u8>,
}

pub(super) enum BatchFetched {
    Batch(FetchedBatch),
    /// Home explicitly lacks the batch capability; try the single-file path.
    Unavailable,
}

/// Per-Home batch state shared by every open of that Home.
#[derive(Default)]
pub(super) struct BatchStore {
    slices: HashMap<Vec<u8>, BatchSlice>,
    parents: HashMap<Vec<u8>, ParentBatches>,
    /// Least recently used first.
    resident: VecDeque<ResidentBatch>,
    /// Bytes of resident batches plus reservations of batches in flight.
    held: usize,
    next_id: u64,
    generation: u64,
    invalidated: HashMap<Vec<u8>, u64>,
    /// A batch requested before this generation lands empty.
    floor: u64,
}

impl BatchStore {
    pub(super) fn has_pending(&self, parent: &[u8]) -> bool {
        self.parents
            .get(parent)
            .is_some_and(|batches| batches.pending.is_some())
    }

    /// Claims the open's slice when its version matches, otherwise decides
    /// whether to wait for a batch in flight, start one or give up. Only a
    /// first touch of a directory, a miss exactly where the newest batch
    /// stopped, or a miss after a start found no room starts a batch.
    pub(super) fn claim(&mut self, identity: &[u8], route: &BatchRoute, may_start: bool) -> Claim {
        if let Some(slice) = self.slices.remove(identity) {
            if slice.version == route.version {
                let lookahead = self.account(slice.batch, slice.bytes.len(), true);
                return Claim::Hit(slice.bytes, lookahead);
            }
            self.account(slice.batch, 0, false);
        }
        let now = Instant::now();
        let has_resident = self
            .resident
            .iter()
            .any(|batch| batch.parent == route.parent);
        let Some(parent) = self.parents.get_mut(&route.parent) else {
            return if may_start { Claim::Start } else { Claim::Miss };
        };
        if let Some(pending) = &parent.pending {
            return if pending.anchor <= route.name {
                Claim::Wait(pending.done.clone())
            } else {
                Claim::Miss
            };
        }
        if parent.retry_after.is_some_and(|until| now < until) {
            if let Some((anchor, error)) = &parent.failure
                && *anchor <= route.name
            {
                return Claim::Failed(error.clone());
            }
            return Claim::Miss;
        }
        if !may_start {
            return Claim::Miss;
        }
        let idle = !has_resident && now.duration_since(parent.touched) >= PARENT_RETRY;
        if idle || parent.starved || parent.next.as_deref() == Some(route.name.as_slice()) {
            Claim::Start
        } else {
            Claim::Miss
        }
    }

    /// Counts one slice leaving the store. Once a quarter of a batch is
    /// claimed, every hit returns the anchor of the next batch until a
    /// lookahead there starts.
    fn account(&mut self, id: u64, claimed: usize, hit: bool) -> Option<Vec<u8>> {
        let index = self.resident.iter().position(|batch| batch.id == id)?;
        let mut batch = self.resident.remove(index)?;
        batch.unclaimed = batch.unclaimed.saturating_sub(1);
        batch.claimed = batch.claimed.saturating_add(claimed);
        if hit {
            batch.touched = Instant::now();
        }
        let lookahead = (hit
            && !batch.lookahead
            && !batch.next.is_empty()
            && batch.claimed.saturating_mul(4) >= batch.bytes)
            .then(|| batch.next.clone());
        if batch.unclaimed == 0 {
            self.held = self.held.saturating_sub(batch.bytes);
        } else {
            self.resident.push_back(batch);
        }
        lookahead
    }

    /// Registers a batch an open needs, of `parent` at `anchor`, within
    /// `cap` and the shared window budget. Room comes only from batches idle
    /// for [`BATCH_IDLE`] and lookaheads nothing has claimed from, so batches
    /// other directories are reading stay; a start without room is refused.
    pub(super) fn begin(
        &mut self,
        parent: &[u8],
        anchor: &[u8],
        reserve: usize,
        cap: usize,
        budget: &Arc<WindowBudget>,
    ) -> Option<BatchStart> {
        self.begin_at(parent, anchor, reserve, cap, budget, false, Instant::now())
    }

    /// Registers the lookahead after a batch of `parent` that stopped at
    /// `anchor`. It takes room only from idle batches.
    pub(super) fn begin_lookahead(
        &mut self,
        parent: &[u8],
        anchor: &[u8],
        reserve: usize,
        cap: usize,
        budget: &Arc<WindowBudget>,
    ) -> Option<BatchStart> {
        self.begin_at(parent, anchor, reserve, cap, budget, true, Instant::now())
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn begin_at(
        &mut self,
        parent: &[u8],
        anchor: &[u8],
        reserve: usize,
        cap: usize,
        budget: &Arc<WindowBudget>,
        lookahead: bool,
        now: Instant,
    ) -> Option<BatchStart> {
        if let Some(state) = self.parents.get(parent)
            && (state.pending.is_some() || state.retry_after.is_some_and(|until| now < until))
        {
            return None;
        }
        if !self.parents.contains_key(parent) && self.parents.len() >= MAX_BATCH_PARENTS {
            let resident = &self.resident;
            self.parents.retain(|key, state| {
                state.pending.is_some()
                    || resident.iter().any(|batch| batch.parent == *key)
                    || now.duration_since(state.touched) < PARENT_RETRY
            });
            if self.parents.len() >= MAX_BATCH_PARENTS {
                return None;
            }
        }
        let lease = loop {
            if self.held.saturating_add(reserve) <= cap
                && let Some(lease) = budget.reserve(reserve)
            {
                break lease;
            }
            if !self.evict_for(now, !lookahead) {
                if !lookahead && let Some(state) = self.parents.get_mut(parent) {
                    state.starved = true;
                }
                return None;
            }
        };
        self.held = self.held.saturating_add(reserve);
        if lookahead
            && let Some(batch) = self
                .resident
                .iter_mut()
                .find(|batch| batch.parent == parent && batch.next == anchor)
        {
            batch.lookahead = true;
        }
        let (done, receiver) = watch::channel(None);
        let state = self
            .parents
            .entry(parent.to_vec())
            .or_insert_with(|| ParentBatches {
                next: None,
                pending: None,
                retry_after: None,
                failure: None,
                starved: false,
                touched: now,
            });
        state.touched = now;
        state.starved = false;
        state.pending = Some(PendingBatch {
            anchor: anchor.to_vec(),
            lookahead,
            done: receiver,
        });
        Some(BatchStart {
            done,
            generation: self.generation,
            reserved: reserve,
            lease,
        })
    }

    /// Publishes a landed batch, or marks the directory to retry later, and
    /// wakes the opens waiting for it with the result. Files this
    /// node invalidated after the batch was requested are dropped.
    pub(super) fn finish(
        &mut self,
        parent: &[u8],
        start: BatchStart,
        fetched: afs_error::Result<Option<FetchedBatch>>,
    ) {
        let BatchStart {
            done,
            generation,
            reserved,
            lease,
        } = start;
        self.held = self.held.saturating_sub(reserved);
        let status = fetched.as_ref().map(|_| ()).map_err(Clone::clone);
        self.publish(parent, generation, lease, fetched);
        done.send_replace(Some(status));
    }

    fn publish(
        &mut self,
        parent: &[u8],
        generation: u64,
        lease: BudgetLease,
        fetched: afs_error::Result<Option<FetchedBatch>>,
    ) {
        let now = Instant::now();
        let Some(state) = self.parents.get_mut(parent) else {
            return;
        };
        let Some(pending) = state.pending.take() else {
            return;
        };
        state.touched = now;
        state.failure = None;
        let batch = match fetched {
            Ok(Some(batch)) => batch,
            Ok(None) => {
                state.retry_after = now.checked_add(PARENT_RETRY);
                return;
            }
            Err(error) => {
                state.failure = Some((pending.anchor, error));
                state.retry_after = now.checked_add(PARENT_RETRY);
                return;
            }
        };
        state.retry_after = None;
        state.next = (!batch.next.is_empty()).then(|| batch.next.clone());
        if generation < self.floor {
            return;
        }
        let files = batch
            .files
            .into_iter()
            .filter(|(identity, _, _)| {
                self.invalidated
                    .get(identity)
                    .is_none_or(|invalidated| *invalidated <= generation)
            })
            .collect::<Vec<_>>();
        if files.is_empty() {
            return;
        }
        let body = lease.into_bytes(batch.bytes);
        let bytes = body.len();
        self.next_id = self.next_id.wrapping_add(1);
        let id = self.next_id;
        let mut unclaimed = 0;
        for (identity, version, range) in files {
            let slice = BatchSlice {
                bytes: body.slice(range),
                version,
                batch: id,
            };
            if let Some(replaced) = self.slices.insert(identity, slice) {
                self.account(replaced.batch, 0, false);
            }
            unclaimed += 1;
        }
        self.held = self.held.saturating_add(bytes);
        self.resident.push_back(ResidentBatch {
            id,
            parent: parent.to_vec(),
            anchor: pending.anchor,
            bytes,
            claimed: 0,
            unclaimed,
            next: batch.next,
            lookahead: false,
            speculative: pending.lookahead,
            touched: now,
        });
        while self
            .resident
            .iter()
            .filter(|batch| batch.parent == parent)
            .count()
            > MAX_BATCHES_PER_PARENT
        {
            let Some(index) = self
                .resident
                .iter()
                .position(|batch| batch.parent == parent)
            else {
                break;
            };
            self.evict_at(index);
        }
    }

    /// Drops a file this node changed: its slice now, and its slice in any
    /// batch already requested when that batch lands.
    pub(super) fn invalidate(&mut self, identity: &[u8]) {
        self.generation = self.generation.wrapping_add(1);
        if self.invalidated.len() >= MAX_INVALIDATED {
            self.invalidated.clear();
            self.floor = self.generation;
        }
        self.invalidated.insert(identity.to_vec(), self.generation);
        if let Some(slice) = self.slices.remove(identity) {
            self.account(slice.batch, 0, false);
        }
    }

    /// Drops the least recently used idle batch or, when `needed`, the
    /// oldest lookahead nothing has claimed from. `false` when every resident
    /// batch is being read.
    fn evict_for(&mut self, now: Instant, needed: bool) -> bool {
        let idle = self
            .resident
            .iter()
            .position(|batch| now.saturating_duration_since(batch.touched) >= BATCH_IDLE);
        let index = idle.or_else(|| {
            needed
                .then(|| {
                    self.resident
                        .iter()
                        .position(|batch| batch.speculative && batch.claimed == 0)
                })
                .flatten()
        });
        let Some(index) = index else {
            return false;
        };
        self.evict_at(index);
        true
    }

    fn evict_at(&mut self, index: usize) {
        let Some(batch) = self.resident.remove(index) else {
            return;
        };
        self.slices.retain(|_, slice| slice.batch != batch.id);
        self.held = self.held.saturating_sub(batch.bytes);
        // Nothing was read from it: its directory resumes at its anchor.
        if batch.claimed == 0
            && let Some(state) = self.parents.get_mut(&batch.parent)
            && state.next.as_deref() == Some(batch.next.as_slice())
        {
            state.next = Some(batch.anchor);
        }
    }

    /// Batch bytes resident or reserved.
    #[cfg(test)]
    pub(super) fn held(&self) -> usize {
        self.held
    }
}

/// The batch request of one directory.
pub(super) struct BatchRequest {
    pub(super) home: String,
    pub(super) client: OwnerFilesClient<Channel>,
    pub(super) access: RootAccess,
    pub(super) parent_path: Vec<u8>,
    pub(super) anchor: Vec<u8>,
    pub(super) max_file_bytes: u64,
    pub(super) deadline: Instant,
}

/// Bytes one batch may reserve before Home answers.
pub(super) fn batch_reservation(reader: &OwnerHixlReader) -> usize {
    reader.policy.max_group_bytes.max(reader.slot_bytes())
}

/// Resident batches of one Home stay within this many bytes.
pub(super) fn batch_cap(reader: &OwnerHixlReader) -> usize {
    let half_budget = usize::try_from(reader.policy.host_budget_bytes / 2).unwrap_or(usize::MAX);
    batch_reservation(reader)
        .saturating_mul(BATCH_BUDGET_GROUPS)
        .max(half_budget)
}

impl OwnerHixlReader {
    /// Fetches one verified batch. Runs as an owned task, like a window fetch.
    pub(super) async fn fetch_batch(
        self: Arc<Self>,
        request: BatchRequest,
    ) -> afs_error::Result<BatchFetched> {
        let started = Instant::now();
        let result = self.fetch_batch_inner(&request).await;
        if let Some(metrics) = &self.owner_metrics {
            metrics.observe("client", "read_hixl_batch", started.elapsed());
        }
        result
    }

    async fn fetch_batch_inner(&self, request: &BatchRequest) -> afs_error::Result<BatchFetched> {
        let deadline = request.deadline;
        let Some(mut lease) = self
            .acquire(&request.home, &request.client, &request.access, deadline)
            .await?
        else {
            return Ok(BatchFetched::Unavailable);
        };
        let (lane_id, remote, slot_bytes, slots) = self.read_lane(&request.home, &lease.session)?;
        let slots = slots.min(self.policy.max_group_bytes / slot_bytes.max(1));
        let max_file_bytes = request.max_file_bytes.min(slot_bytes as u64);
        if slots == 0 || max_file_bytes <= OPEN_PREFETCH_LIMIT {
            return Ok(BatchFetched::Unavailable);
        }
        let prepare = OwnerPrepareBatchHixlRequest {
            session_id: lease.session.reply.session_id,
            read_id: self.next_read_id()?,
            access: Some(request.access.clone()),
            parent_path: request.parent_path.clone(),
            anchor_name: request.anchor.clone(),
            lane_id: u32::try_from(lane_id)
                .map_err(|_| protocol_error("OwnerFs HIXL lane overflow"))?,
            slot_bytes: slot_bytes as u64,
            max_bytes: (slot_bytes * slots) as u64,
            min_file_bytes: OPEN_PREFETCH_LIMIT,
            max_file_bytes,
            prior_release: lease.prior_release(),
        };
        let reply = match self
            .prepare(
                &mut lease,
                &prepare,
                prepare.prior_release.as_ref(),
                deadline,
                |mut client, command| async move { client.prepare_batch_hixl(command).await },
            )
            .await
        {
            Ok(reply) => reply,
            Err(error) if is_unsupported(&error) => return Ok(BatchFetched::Unavailable),
            Err(error) => return Err(error),
        };
        let checksum = SlotChecksum::from_reply(lease.session.reply.slot_checksum_algorithm)
            .ok_or_else(|| protocol_error("OwnerFs HIXL slot checksum is not negotiated"))?;
        let files =
            match validate_batch_prepare(&prepare, &reply, &remote, slot_bytes, slots, checksum) {
                Ok(files) => files,
                Err(error) => {
                    self.reject_prepare(
                        &lease.session,
                        (!reply.slots.is_empty()).then(|| (&reply).into()),
                    )
                    .await;
                    return Err(error);
                }
            };
        if reply.slots.is_empty() {
            return Ok(BatchFetched::Batch(FetchedBatch {
                bytes: Vec::new(),
                files: Vec::new(),
                next: reply.next_name,
            }));
        }
        let ticket: ReadTicket = (&reply).into();
        let bytes = self
            .pull_slots(
                lease,
                lane_id,
                reply.engine_id,
                reply.slots,
                ticket,
                checksum,
                deadline,
            )
            .await?;
        Ok(BatchFetched::Batch(FetchedBatch {
            bytes,
            files,
            next: reply.next_name,
        }))
    }
}

/// Checks a batch reply against its request and returns, per packed file,
/// its identity, version and byte range in the pulled buffer. Files must be
/// in name order, inside their slot and must not overlap.
fn validate_batch_prepare(
    request: &OwnerPrepareBatchHixlRequest,
    reply: &OwnerPrepareBatchHixlReply,
    lane: &HixlLane,
    slot_bytes: usize,
    max_slots: usize,
    checksum: SlotChecksum,
) -> afs_error::Result<PackedFiles> {
    if reply.session_id != request.session_id
        || reply.read_id != request.read_id
        || reply.lane_id != request.lane_id
        || reply.bytes > request.max_bytes
    {
        return Err(protocol_error("OwnerFs HIXL batch reply identity differs"));
    }
    if reply.slots.is_empty() {
        return if reply.files.is_empty() && reply.bytes == 0 {
            Ok(Vec::new())
        } else {
            Err(protocol_error("OwnerFs HIXL empty batch carries files"))
        };
    }
    if reply.engine_id != lane.engine_id
        || reply.engine_generation != lane.engine_generation
        || reply.slots.len() > max_slots
    {
        return Err(protocol_error("OwnerFs HIXL batch reply lane differs"));
    }
    let mut starts = Vec::with_capacity(reply.slots.len());
    let mut total = 0_usize;
    for slot in &reply.slots {
        let length = checked_slot_length(
            slot.address,
            slot.slot_generation,
            slot.length,
            slot.checksum.len(),
            slot_bytes,
            checksum,
        )?;
        starts.push((total, length));
        total = total
            .checked_add(length)
            .ok_or_else(|| protocol_error("OwnerFs HIXL slot length overflow"))?;
    }
    if total as u64 != reply.bytes {
        return Err(protocol_error("OwnerFs HIXL slots do not cover the batch"));
    }
    let mut files = Vec::with_capacity(reply.files.len());
    let mut previous: Option<(&[u8], usize, usize)> = None;
    for file in &reply.files {
        let invalid = || protocol_error("OwnerFs HIXL batch file is invalid");
        let slot = usize::try_from(file.slot_index).map_err(|_| invalid())?;
        let (start, length) = *starts.get(slot).ok_or_else(invalid)?;
        let offset = usize::try_from(file.offset_in_slot).map_err(|_| invalid())?;
        let size = usize::try_from(file.size).map_err(|_| invalid())?;
        let end = offset.checked_add(size).ok_or_else(invalid)?;
        let identity = file
            .identity
            .as_ref()
            .map(|identity| identity.opaque.clone())
            .filter(|identity| !identity.is_empty())
            .ok_or_else(invalid)?;
        if size == 0 || end > length || file.name.is_empty() {
            return Err(invalid());
        }
        if let Some((name, previous_slot, previous_end)) = previous
            && (file.name.as_slice() <= name
                || slot < previous_slot
                || (slot == previous_slot && offset < previous_end))
        {
            return Err(invalid());
        }
        previous = Some((&file.name, slot, end));
        files.push((
            identity,
            FileVersion {
                size: file.size,
                mtime_ns: file.mtime_ns,
                ctime_ns: file.ctime_ns,
            },
            start + offset..start + end,
        ));
    }
    Ok(files)
}
