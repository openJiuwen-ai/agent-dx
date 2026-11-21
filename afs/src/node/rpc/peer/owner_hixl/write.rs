//! Writer-side orchestration of one Home file write per HIXL ticket.

use super::{
    HixlLane, HixlReleaseOutcome, HomeLease, HomeSession, HomeSlot, MAX_SESSION_WRITE_TICKETS,
    OwnerHixlReader, OwnerHixlSlot, OwnerReadSession, PREPARE_ATTEMPTS, ParkedFlush, SlotChecksum,
    protocol_error, remaining, retries_prepare,
};
use crate::config::DataMode;
use crate::node::rpc::hixl::runtime_error;
use crate::node::rpc::peer::{
    OwnerRuntime, file_handle, owner_dispatched_write_error, root_access,
};
use crate::node::vfs::ownerfs::{
    files::RemoteFile,
    remote::{write_status_requires_execution_evidence, write_was_not_executed},
    root::RootGrant,
};
use crate::node::vfs::types::WriteOptions;
use afs_protocol::node_data::{
    OwnerAbortWriteHixlReply, OwnerAbortWriteHixlRequest, OwnerFinishWriteHixlRequest, OwnerHandle,
    OwnerPrepareWriteHixlReply, OwnerPrepareWriteHixlRequest, RootAccess,
    owner_abort_write_hixl_reply::Recovery, owner_files_client::OwnerFilesClient,
};
use afs_tracing::Instrument;
use afs_transport::grpc::error_status::status_to_error;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tonic::transport::Channel;

impl OwnerReadSession {
    /// Confirms a usable session before sending a potentially truncating Open.
    pub(in crate::node::rpc::peer) fn ensure_session_for_open(
        &self,
        runtime: &OwnerRuntime,
        client: &OwnerFilesClient<Channel>,
        grant: &RootGrant,
    ) -> afs_error::Result<()> {
        let access = root_access(grant);
        let deadline = self.reader.write_deadline()?;
        runtime.block_on(async {
            self.reader
                .acquire(&self.home, client, &access, deadline)
                .await?
                .map(|_| ())
                .ok_or_else(crate::node::rpc::peer::owner_hixl_write_unsupported)
        })
    }

    /// Returns an unused grant before the corresponding Home file is closed.
    pub(in crate::node::rpc::peer) fn release_write_window(
        &self,
        runtime: &OwnerRuntime,
        handle: &[u8],
    ) {
        let reader = &self.reader;
        let _ = runtime.block_on(async {
            let pool = reader
                .homes
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(&self.home)
                .cloned();
            if let Some(pool) = pool {
                for slot in &pool.slots {
                    let mut guard = slot.lock().await;
                    if let Some(session) = guard.session.clone()
                        && guard
                            .write_window
                            .as_ref()
                            .is_some_and(|window| window.handle.opaque == handle)
                    {
                        let _ = guard.return_write_window(&session, reader.timeout).await;
                    }
                }
            }
            Ok::<(), afs_error::Error>(())
        });
    }

    /// `None` permits another body transport. Once Prepare is sent or a
    /// granted window is used for DMA, every failure is terminal for this call.
    // Keep the peer call aligned with RemoteFiles::write_with_options.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::node::rpc::peer) fn write(
        &self,
        runtime: &OwnerRuntime,
        client: &OwnerFilesClient<Channel>,
        grant: &RootGrant,
        file: &RemoteFile,
        offset: u64,
        data: &[u8],
        options: WriteOptions,
    ) -> afs_error::Result<Option<usize>> {
        let policy = &self.reader.policy;
        if data.is_empty() || (policy.mode == DataMode::Auto && data.len() < policy.min_write_bytes)
        {
            return Ok(None);
        }
        let reader = self.reader.clone();
        let request = WriteRequest {
            home: self.home.clone(),
            client: client.clone(),
            access: root_access(grant),
            handle: file_handle(file),
            offset,
            bytes: data.to_vec(),
            options,
            keep_next: runtime.handle().is_some(),
            deadline: reader.write_deadline()?,
        };
        // The owned task holds the lease and the payload until native completion
        // and Commit or Abort, even if the caller stops waiting on the join.
        let span = afs_tracing::tracing::Span::current();
        let commit_started = Arc::new(AtomicBool::new(false));
        let result = runtime.block_on(async {
            tokio::spawn(
                reader
                    .write_inner(request, commit_started.clone())
                    .instrument(span),
            )
            .await
            .map_err(write_worker_error)?
        });
        result.map_err(|error| {
            if commit_started.load(Ordering::Acquire) {
                owner_dispatched_write_error(error)
            } else {
                before_commit_error(error)
            }
        })
    }
}

struct WriteRequest {
    home: String,
    client: OwnerFilesClient<Channel>,
    access: RootAccess,
    handle: OwnerHandle,
    offset: u64,
    bytes: Vec<u8>,
    options: WriteOptions,
    keep_next: bool,
    deadline: Instant,
}

impl OwnerHixlReader {
    fn write_deadline(&self) -> afs_error::Result<Instant> {
        Instant::now().checked_add(self.timeout).ok_or_else(|| {
            afs_error::Error::coded(
                afs_error::CLIENT_ARGUMENT_INVALID,
                "OwnerFs HIXL write deadline overflows",
            )
        })
    }

    async fn write_inner(
        self: Arc<Self>,
        request: WriteRequest,
        commit_started: Arc<AtomicBool>,
    ) -> afs_error::Result<Option<usize>> {
        let Some(mut lease) = self
            .acquire_for(
                &request.home,
                &request.client,
                &request.access,
                request.deadline,
                Some(&request.handle),
            )
            .await?
        else {
            return Ok(None);
        };
        let (lane_id, remote, slot_bytes, slots) = self.read_lane(&request.home, &lease.session)?;
        if request.bytes.len() > slot_bytes.saturating_mul(slots) {
            lease
                .guard
                .return_write_window(&lease.session, self.timeout)
                .await?;
            // Splitting a logical write could interleave appends or turn a
            // failed write into a partial side effect. Auto may choose gRPC.
            return if self.policy.mode == DataMode::Auto {
                Ok(None)
            } else {
                Err(afs_error::Error::coded(
                    afs_error::NODE_TRANSFER_RESOURCE_EXHAUSTED,
                    "OwnerFs HIXL write exceeds one ticket's lane capacity",
                ))
            };
        }
        let lane = self
            .native
            .lane(lane_id)
            .ok_or_else(|| protocol_error("OwnerFs HIXL local write lane is absent"))?;
        let length = u32::try_from(request.bytes.len())
            .map_err(|_| protocol_error("OwnerFs HIXL write length overflow"))?;
        let (algorithm, digest) = body_checksum(&lease.session.reply, &request.bytes)?;
        let prepare = OwnerPrepareWriteHixlRequest {
            session_id: lease.session.reply.session_id,
            write_id: self.next_read_id()?,
            access: Some(request.access),
            handle: Some(request.handle),
            offset: request.offset,
            length,
            kill_suidgid: request.options.kill_suidgid,
            data_checksum: digest,
            data_checksum_algorithm: algorithm,
            lane_id: u32::try_from(lane_id)
                .map_err(|_| protocol_error("OwnerFs HIXL lane overflow"))?,
            slot_bytes: u64::try_from(slot_bytes)
                .map_err(|_| protocol_error("OwnerFs HIXL window length overflow"))?,
        };
        // A writer cannot piggyback a release on Prepare. Drain a parked read
        // before the write reserves from the same Home export pool.
        if let Some(ticket) = lease.guard.parked.as_ref() {
            self.release(&lease.session, ticket, HixlReleaseOutcome::Completed, true)
                .await?;
            lease.guard.parked = None;
        }
        if lease.guard.write_window.as_ref().is_some_and(|window| {
            !window.matches_lane(&prepare, &remote)
                || window.capacity < request.bytes.len()
                || Instant::now() >= window.expires
        }) {
            lease
                .guard
                .return_write_window(&lease.session, self.timeout)
                .await?;
        }
        lease.guard.write_tickets += 1;
        // Legacy Homes ignore request_next and would interpret cancel_next as
        // an ordinary Abort. Only ask for a grant with the updated protocol.
        let request_next = request.keep_next
            && lease.session.reply.write_checksum_negotiated
            && lease.guard.write_tickets < MAX_SESSION_WRITE_TICKETS;
        let cached = lease.guard.write_window.take();
        let mut ticket = PendingWrite::new(lease, &prepare, &remote, request_next, cached.as_ref());
        let windows = if let Some(window) = cached {
            window.into_prefix(request.bytes.len())
        } else {
            let reply = write_rpc_with_retry(
                &ticket.lease.session.client,
                &prepare,
                request.deadline,
                |mut client, command| async move { client.prepare_write_hixl(command).await },
            )
            .await;
            match reply.and_then(|reply| validate_write_slots(&prepare, &reply, &remote, slots)) {
                Ok(windows) => windows,
                Err(error) => {
                    // The reply may have been lost or malformed. Clean up with
                    // only the locally selected session, write ID and generation.
                    let _ = ticket.abort(false, self.write_deadline()?).await;
                    return Err(error);
                }
            }
        };
        let deadline = request.deadline;
        let remote_engine = remote.engine_id.clone();
        let transfer = tokio::task::spawn_blocking(move || {
            lane.write_batch(&remote_engine, &windows, request.bytes, deadline)
        })
        .await;
        let (error, unknown) = match transfer {
            Ok(Ok(())) => {
                // Every retry carries the same Commit. Once an attempt is
                // ambiguous, only Home's receipt or fence resolves the write.
                let deadline = self.write_deadline()?;
                commit_started.store(true, Ordering::Release);
                let mut ambiguous = false;
                let reply = write_rpc_with_retry_observing(
                    &ticket.lease.session.client,
                    &ticket.finish,
                    deadline,
                    |error| ambiguous |= commit_needs_recovery(error),
                    |mut client, command| async move { client.commit_write_hixl(command).await },
                )
                .await;
                let reply = match reply {
                    Ok(reply) if reply.written <= length => reply,
                    Err(error) if !ambiguous => {
                        if write_was_not_executed(&error) {
                            ticket.resolved = true;
                        } else {
                            ticket.cancel_successor(self.timeout).await;
                        }
                        return Err(error);
                    }
                    result => {
                        let cause = match result {
                            Ok(_) => {
                                protocol_error("OwnerFs HIXL written count exceeds request length")
                            }
                            Err(error) => error,
                        };
                        if let Ok(outcome) = ticket.recover(self.write_deadline()?, length).await {
                            ticket.resolved = true;
                            return outcome.and_then(written_count);
                        }
                        ticket.cancel_successor(self.timeout).await;
                        return Err(afs_error::Error::coded(
                            afs_error::NODE_OWNER_WRITE_UNKNOWN,
                            format!("OwnerFs HIXL write outcome is unknown: {cause}"),
                        ));
                    }
                };
                ticket.resolved = true;
                if let Some(next) = reply.next_window {
                    // Commit is already done. An unusable successor cannot
                    // turn the acknowledged file write into a failed write.
                    if let Ok(window) = WriteWindow::new(
                        &prepare,
                        &ticket.finish,
                        &remote,
                        slots,
                        next,
                        self.release_park,
                    ) {
                        ticket.lease.guard.write_window = Some(window);
                    } else {
                        ticket.cancel_successor(self.timeout).await;
                    }
                }
                return written_count(reply.written);
            }
            // The native API uses the same in-flight bit for READ and WRITE.
            Ok(Err(error)) => {
                let unknown = error.read_may_be_in_flight;
                (runtime_error(error), unknown)
            }
            Err(error) => (write_worker_error(error), true),
        };
        if ticket.abort(unknown, self.write_deadline()?).await.is_ok() && !unknown {
            ticket.resolved = true;
        }
        if unknown {
            let _ = tokio::task::spawn_blocking(move || self.native.quarantine()).await;
        }
        Err(error)
    }
}

fn written_count(written: u32) -> afs_error::Result<Option<usize>> {
    usize::try_from(written)
        .map(Some)
        .map_err(|_| protocol_error("OwnerFs HIXL written count overflow"))
}

fn commit_needs_recovery(error: &afs_error::Error) -> bool {
    !write_was_not_executed(error)
        && (write_status_requires_execution_evidence(error)
            || error.code() == afs_error::NODE_OWNER_WRITE_UNKNOWN)
}

fn body_checksum(
    reply: &super::OwnerNegotiateHixlReply,
    data: &[u8],
) -> afs_error::Result<(i32, Vec<u8>)> {
    if !reply.write_checksum_negotiated {
        return Ok((0, SlotChecksum::Blake3.digest(data)));
    }
    let algorithm = SlotChecksum::from_reply(reply.slot_checksum_algorithm)
        .ok_or_else(|| protocol_error("OwnerFs HIXL write checksum is unsupported"))?;
    Ok((algorithm.wire() as i32, algorithm.digest(data)))
}

/// Single-use export grant received over the authenticated session. The
/// session lease serializes consumption and return, including idle cleanup.
pub(super) struct WriteWindow {
    access: RootAccess,
    handle: OwnerHandle,
    descriptor: OwnerPrepareWriteHixlReply,
    windows: Vec<(u64, usize)>,
    capacity: usize,
    expires: Instant,
}

impl WriteWindow {
    fn new(
        request: &OwnerPrepareWriteHixlRequest,
        finish: &OwnerFinishWriteHixlRequest,
        lane: &HixlLane,
        max_slots: usize,
        descriptor: OwnerPrepareWriteHixlReply,
        park: Duration,
    ) -> afs_error::Result<Self> {
        if !finish.request_next
            || descriptor.write_id.is_empty()
            || descriptor.write_id.len() > 256
            || descriptor.write_id == request.write_id
            || descriptor.write_id == finish.window_token
        {
            return Err(protocol_error("OwnerFs HIXL successor token is invalid"));
        }
        let access = request
            .access
            .clone()
            .ok_or_else(|| protocol_error("OwnerFs HIXL write access is absent"))?;
        let handle = request
            .handle
            .clone()
            .ok_or_else(|| protocol_error("OwnerFs HIXL write handle is absent"))?;
        let windows =
            checked_write_slots(request, &descriptor, lane, max_slots, &descriptor.write_id)?;
        let capacity = windows
            .iter()
            .try_fold(0_usize, |total, (_, len)| total.checked_add(*len))
            .ok_or_else(|| protocol_error("OwnerFs HIXL write window capacity overflows"))?;
        let expires = Instant::now()
            .checked_add(park)
            .ok_or_else(|| protocol_error("OwnerFs HIXL write window expiry overflows"))?;
        Ok(Self {
            access,
            handle,
            descriptor,
            windows,
            capacity,
            expires,
        })
    }

    pub(super) fn can_reuse(&self, access: &RootAccess, handle: &OwnerHandle) -> bool {
        self.access == *access && self.handle == *handle && Instant::now() < self.expires
    }

    fn matches_lane(&self, request: &OwnerPrepareWriteHixlRequest, lane: &HixlLane) -> bool {
        self.descriptor.session_id == request.session_id
            && self.descriptor.lane_id == request.lane_id
            && self.descriptor.engine_id == lane.engine_id
            && self.descriptor.engine_generation == lane.engine_generation
    }

    pub(super) fn token(&self) -> &str {
        &self.descriptor.write_id
    }

    pub(super) fn expires(&self) -> Instant {
        self.expires
    }

    fn into_prefix(self, mut remaining: usize) -> Vec<(u64, usize)> {
        let mut windows = self.windows;
        windows.retain_mut(|(_, length)| {
            *length = (*length).min(remaining);
            remaining -= *length;
            *length != 0
        });
        windows
    }
}

impl HomeSlot {
    pub(super) async fn return_write_window(
        &mut self,
        session: &HomeSession,
        timeout: Duration,
    ) -> afs_error::Result<()> {
        let Some(window) = self.write_window.as_ref() else {
            return Ok(());
        };
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| protocol_error("OwnerFs HIXL write return deadline overflows"))?;
        let result = abort_write(
            session,
            window.descriptor.engine_generation,
            window.token(),
            false,
            deadline,
        )
        .await;
        self.write_window = None;
        if result.is_err() {
            session.poisoned.store(true, Ordering::Release);
        }
        result
    }
}

impl ParkedFlush {
    pub(super) async fn run_write(
        self,
        slot: Arc<Mutex<HomeSlot>>,
        token: String,
        expires: Instant,
    ) {
        tokio::time::sleep_until(expires.into()).await;
        let Ok(_permit) = self.pool.permits.clone().acquire_owned().await else {
            return;
        };
        let mut guard = slot.lock_owned().await;
        if let Some(session) = guard.session.clone()
            && guard
                .write_window
                .as_ref()
                .is_some_and(|window| window.token() == token && window.expires == expires)
        {
            let _ = guard.return_write_window(&session, self.timeout).await;
        }
    }
}

async fn abort_write(
    session: &HomeSession,
    engine_generation: u64,
    write_id: &str,
    unknown: bool,
    deadline: Instant,
) -> afs_error::Result<()> {
    let abort = OwnerAbortWriteHixlRequest {
        session_id: session.reply.session_id,
        write_id: write_id.to_owned(),
        engine_generation,
        unknown,
        cancel_next: false,
        recover_commit: None,
    };
    send_abort(session, &abort, deadline).await.map(|_| ())
}

async fn send_abort(
    session: &HomeSession,
    abort: &OwnerAbortWriteHixlRequest,
    deadline: Instant,
) -> afs_error::Result<OwnerAbortWriteHixlReply> {
    write_rpc_with_retry(
        &session.client,
        abort,
        deadline,
        |mut client, command| async move { client.abort_write_hixl(command).await },
    )
    .await
}

/// An unresolved transaction fences the session before dropping its lease.
/// Home must retain prepared slots on session close or expiry when native
/// completion was never reported.
struct PendingWrite {
    lease: HomeLease,
    finish: OwnerFinishWriteHixlRequest,
    resolved: bool,
}

impl PendingWrite {
    fn new(
        lease: HomeLease,
        request: &OwnerPrepareWriteHixlRequest,
        lane: &HixlLane,
        request_next: bool,
        cached: Option<&WriteWindow>,
    ) -> Self {
        Self {
            lease,
            finish: OwnerFinishWriteHixlRequest {
                session_id: request.session_id,
                write_id: request.write_id.clone(),
                engine_generation: lane.engine_generation,
                request_next,
                reused: cached.map(|_| request.clone()),
                window_token: cached.map_or_else(String::new, |window| window.token().to_owned()),
            },
            resolved: false,
        }
    }

    /// A legacy Home only cancels the successor and returns an empty reply.
    /// Only a typed receipt can establish execution or non-execution.
    async fn recover(
        &self,
        deadline: Instant,
        length: u32,
    ) -> afs_error::Result<afs_error::Result<u32>> {
        let request = OwnerAbortWriteHixlRequest {
            session_id: self.finish.session_id,
            write_id: self.finish.write_id.clone(),
            engine_generation: self.finish.engine_generation,
            unknown: false,
            cancel_next: true,
            recover_commit: Some(self.finish.clone()),
        };
        let reply = send_abort(&self.lease.session, &request, deadline).await?;
        match reply.recovery {
            Some(Recovery::Written(written)) if written <= length => Ok(Ok(written)),
            Some(Recovery::Failure(failure)) => {
                let code = tonic::Code::from_i32(failure.grpc_code);
                if code == tonic::Code::Ok || code as i32 != failure.grpc_code {
                    return Err(protocol_error("OwnerFs HIXL invalid recovery status"));
                }
                let status =
                    tonic::Status::with_details(code, failure.message, failure.details.into());
                Ok(Err(status_to_error(status)))
            }
            _ => Err(protocol_error("OwnerFs HIXL recovery has no valid receipt")),
        }
    }

    /// The immutable commit may still run. Tell Home that its successor can
    /// never be used, without cancelling or replaying the file operation.
    async fn cancel_successor(&self, timeout: Duration) {
        if !self.finish.request_next {
            return;
        }
        let Some(deadline) = Instant::now().checked_add(timeout) else {
            self.lease.session.poisoned.store(true, Ordering::Release);
            return;
        };
        let request = OwnerAbortWriteHixlRequest {
            session_id: self.finish.session_id,
            write_id: self.finish.write_id.clone(),
            engine_generation: self.finish.engine_generation,
            unknown: false,
            cancel_next: true,
            recover_commit: None,
        };
        if send_abort(&self.lease.session, &request, deadline)
            .await
            .is_err()
        {
            self.lease.session.poisoned.store(true, Ordering::Release);
        }
    }

    async fn abort(&self, unknown: bool, deadline: Instant) -> afs_error::Result<()> {
        let id = if self.finish.window_token.is_empty() {
            &self.finish.write_id
        } else {
            &self.finish.window_token
        };
        abort_write(
            &self.lease.session,
            self.finish.engine_generation,
            id,
            unknown,
            deadline,
        )
        .await
    }
}

impl Drop for PendingWrite {
    fn drop(&mut self) {
        if !self.resolved {
            self.lease.session.poisoned.store(true, Ordering::Release);
        }
    }
}

fn validate_write_slots(
    request: &OwnerPrepareWriteHixlRequest,
    reply: &OwnerPrepareWriteHixlReply,
    lane: &HixlLane,
    max_slots: usize,
) -> afs_error::Result<Vec<(u64, usize)>> {
    let windows = checked_write_slots(request, reply, lane, max_slots, &request.write_id)?;
    let length = usize::try_from(request.length)
        .map_err(|_| protocol_error("OwnerFs HIXL write length overflow"))?;
    if windows
        .iter()
        .try_fold(0_usize, |total, (_, len)| total.checked_add(*len))
        != Some(length)
    {
        return Err(protocol_error(
            "OwnerFs HIXL write slots do not cover the body",
        ));
    }
    Ok(windows)
}

fn checked_write_slots(
    request: &OwnerPrepareWriteHixlRequest,
    reply: &OwnerPrepareWriteHixlReply,
    lane: &HixlLane,
    max_slots: usize,
    expected_id: &str,
) -> afs_error::Result<Vec<(u64, usize)>> {
    if reply.session_id != request.session_id
        || reply.write_id != expected_id
        || reply.lane_id != request.lane_id
        || reply.engine_id != lane.engine_id
        || reply.engine_generation != lane.engine_generation
        || reply.slots.is_empty()
        || reply.slots.len() > max_slots
    {
        return Err(protocol_error(
            "OwnerFs HIXL write prepare reply identity or slot count differs",
        ));
    }
    let mut slot_ids = HashSet::with_capacity(reply.slots.len());
    let mut address_ranges = Vec::with_capacity(reply.slots.len());
    let mut windows = Vec::with_capacity(reply.slots.len());
    for OwnerHixlSlot {
        slot_id,
        slot_generation,
        address,
        length,
        checksum,
    } in &reply.slots
    {
        let end = address
            .checked_add(*length)
            .ok_or_else(|| protocol_error("OwnerFs HIXL write slot address overflow"))?;
        if *slot_id >= u64::from(lane.export_slots)
            || !slot_ids.insert(*slot_id)
            || *slot_generation == 0
            || *address == 0
            || *length == 0
            || *length > request.slot_bytes
            || !checksum.is_empty()
        {
            return Err(protocol_error("OwnerFs HIXL write slot is invalid"));
        }
        address_ranges.push((*address, end));
        windows.push((
            *address,
            usize::try_from(*length)
                .map_err(|_| protocol_error("OwnerFs HIXL write slot length overflow"))?,
        ));
    }
    address_ranges.sort_unstable();
    if address_ranges.windows(2).any(|pair| pair[0].1 > pair[1].0) {
        return Err(protocol_error("OwnerFs HIXL write slots overlap"));
    }
    Ok(windows)
}

/// Replays the same immutable request under a shared stage deadline. Reserve
/// time for another attempt even when the first RPC times out.
async fn write_rpc_with_retry<Req: Clone, Reply, SendReply>(
    client: &OwnerFilesClient<Channel>,
    request: &Req,
    deadline: Instant,
    send: impl Fn(OwnerFilesClient<Channel>, tonic::Request<Req>) -> SendReply,
) -> afs_error::Result<Reply>
where
    SendReply: std::future::Future<Output = Result<tonic::Response<Reply>, tonic::Status>>,
{
    write_rpc_with_retry_observing(client, request, deadline, |_| {}, send).await
}

async fn write_rpc_with_retry_observing<Req: Clone, Reply, SendReply>(
    client: &OwnerFilesClient<Channel>,
    request: &Req,
    deadline: Instant,
    mut observe_error: impl FnMut(&afs_error::Error),
    send: impl Fn(OwnerFilesClient<Channel>, tonic::Request<Req>) -> SendReply,
) -> afs_error::Result<Reply>
where
    SendReply: std::future::Future<Output = Result<tonic::Response<Reply>, tonic::Status>>,
{
    const ATTEMPTS: u32 = PREPARE_ATTEMPTS as u32;
    for left in (1..=ATTEMPTS).rev() {
        let timeout = remaining(deadline)? / left;
        let mut command = afs_tracing::request_with_current_context(request.clone());
        command.set_timeout(timeout);
        let error = match tokio::time::timeout(timeout, send(client.clone(), command)).await {
            Ok(Ok(reply)) => return Ok(reply.into_inner()),
            Ok(Err(status)) => status_to_error(status),
            Err(_) => afs_error::Error::coded(
                afs_error::CLIENT_DEADLINE_EXCEEDED,
                "OwnerFs HIXL write RPC timed out",
            ),
        };
        observe_error(&error);
        if left == 1
            || write_was_not_executed(&error)
            || !(retries_prepare(&error) || transport_cancelled(&error))
        {
            return Err(error);
        }
    }
    Err(protocol_error("OwnerFs HIXL write RPC produced no result"))
}

fn transport_cancelled(error: &afs_error::Error) -> bool {
    error.code() == afs_error::CLIENT_REMOTE_STATUS
        && error.kind() == afs_error::ErrorKind::Cancelled
}

/// No file mutation can happen before Commit, even if native DMA completion
/// is unknown. Keep these failures from poisoning the VFS file handle.
fn before_commit_error(error: afs_error::Error) -> afs_error::Error {
    use afs_error::ErrorKind;

    if write_was_not_executed(&error) {
        return error;
    }
    let code = match error.kind() {
        ErrorKind::DeadlineExceeded => afs_error::NODE_OWNER_WRITE_NOT_EXECUTED_TIMEOUT,
        ErrorKind::Unavailable if error.code() != afs_error::NODE_OWNER_GRANT_UNAVAILABLE => {
            afs_error::NODE_OWNER_WRITE_NOT_EXECUTED_UNAVAILABLE
        }
        ErrorKind::Cancelled => afs_error::NODE_OWNER_WRITE_NOT_EXECUTED_INTERRUPTED,
        ErrorKind::Internal | ErrorKind::DataLoss | ErrorKind::Unknown => {
            afs_error::NODE_OWNER_WRITE_NOT_EXECUTED_IO
        }
        _ => return error,
    };
    afs_error::Error::coded(
        code,
        format!("OwnerFs HIXL write failed before Commit: {error}"),
    )
}

fn write_worker_error(error: tokio::task::JoinError) -> afs_error::Error {
    afs_error::Error::coded(afs_error::CLIENT_WORKER_FAILED, error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn staging_failure_does_not_fence_the_file_handle() {
        let error = afs_error::Error::coded(
            afs_error::CLIENT_PROTOCOL_VIOLATION,
            "Prepare returned a malformed descriptor",
        );
        let error = before_commit_error(error);
        assert_eq!(error.code(), afs_error::NODE_OWNER_WRITE_NOT_EXECUTED_IO);
        assert_eq!(crate::error::errno(&error), libc::EIO);
    }

    #[test]
    fn write_body_checksum_honors_negotiation_and_legacy_wire_shape() {
        use afs_protocol::node_data::DfsDigestAlgorithm;

        let body = b"123456789";
        let mut reply = super::super::OwnerNegotiateHixlReply {
            write_checksum_negotiated: true,
            slot_checksum_algorithm: DfsDigestAlgorithm::Crc32c as i32,
            ..Default::default()
        };
        assert_eq!(
            body_checksum(&reply, body).unwrap(),
            (
                DfsDigestAlgorithm::Crc32c as i32,
                0xe306_9283_u32.to_le_bytes().to_vec()
            )
        );
        reply.slot_checksum_algorithm = DfsDigestAlgorithm::Blake3 as i32;
        let blake = blake3::hash(body).as_bytes().to_vec();
        assert_eq!(
            body_checksum(&reply, body).unwrap(),
            (DfsDigestAlgorithm::Blake3 as i32, blake.clone())
        );
        reply.write_checksum_negotiated = false;
        reply.slot_checksum_algorithm = DfsDigestAlgorithm::Crc32c as i32;
        assert_eq!(
            body_checksum(&reply, body).unwrap(),
            (DfsDigestAlgorithm::Unspecified as i32, blake)
        );
    }

    #[test]
    fn validates_all_write_windows_before_native_transfer() {
        let lane = HixlLane {
            lane_id: 1,
            engine_id: "127.0.0.1:7930".into(),
            engine_generation: 6,
            window_bytes: 8,
            export_slots: 2,
        };
        let request = OwnerPrepareWriteHixlRequest {
            session_id: 9,
            write_id: "write-1".into(),
            lane_id: lane.lane_id,
            length: 10,
            slot_bytes: 8,
            ..Default::default()
        };
        let reply = OwnerPrepareWriteHixlReply {
            session_id: request.session_id,
            write_id: request.write_id.clone(),
            lane_id: lane.lane_id,
            engine_id: lane.engine_id.clone(),
            engine_generation: lane.engine_generation,
            slots: vec![
                OwnerHixlSlot {
                    slot_id: 1,
                    slot_generation: 2,
                    address: 128,
                    length: 8,
                    checksum: vec![],
                },
                OwnerHixlSlot {
                    slot_id: 0,
                    slot_generation: 1,
                    address: 64,
                    length: 2,
                    checksum: vec![],
                },
            ],
        };
        assert_eq!(
            validate_write_slots(&request, &reply, &lane, 2).unwrap(),
            [(128, 8), (64, 2)],
            "wire order determines each slot's slice of the body",
        );
        let mutations: [fn(&mut OwnerPrepareWriteHixlReply); 9] = [
            |reply| reply.session_id += 1,
            |reply| reply.engine_generation += 1,
            |reply| reply.write_id.push('x'),
            |reply| reply.slots[1].slot_generation = 0,
            |reply| reply.slots[1].slot_id = 1,
            |reply| reply.slots[1].address = 127,
            |reply| reply.slots[1].address = u64::MAX,
            |reply| reply.slots[1].length = 1,
            |reply| reply.slots[1].length = 9,
        ];
        for mutate in mutations {
            let mut bad = reply.clone();
            mutate(&mut bad);
            assert_eq!(
                validate_write_slots(&request, &bad, &lane, 2)
                    .unwrap_err()
                    .code(),
                afs_error::CLIENT_PROTOCOL_VIOLATION,
            );
        }
    }
}
