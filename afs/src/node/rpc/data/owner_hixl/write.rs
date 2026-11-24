//! Home-side HIXL WRITE tickets. File mutation starts only after the authenticated
//! writer reports terminal native completion and Home validates a private snapshot.

use super::{
    Cell, Instant, OWNER_HIXL_EXPORT_DEADLINE, Operation, OwnerHixlService, PendingExport,
    PresentedRootAccess, Registry, RemoteFile, Response, Session, SlotChecksum, Status,
    coded_status, error_to_status, exhausted, export_descriptor, presented_access,
    remote_file_for_handle, runtime_status,
};
use crate::node::storage::MAX_TRANSFER_BYTES;
use crate::node::vfs::types::WriteOptions;
use afs_protocol::node_data::{
    OwnerAbortWriteHixlReply, OwnerAbortWriteHixlRequest, OwnerCommitWriteHixlReply,
    OwnerFinishWriteHixlRequest, OwnerPrepareWriteHixlReply, OwnerPrepareWriteHixlRequest,
    OwnerWriteHixlFailure, owner_abort_write_hixl_reply::Recovery,
};
use prost::Message;
use std::sync::{Arc, MutexGuard};

// Never evict a write receipt from a live session: even after a lost reply,
// a delayed Prepare must not turn a completed append into another append.
const MAX_WRITE_RECORDS: usize = 4096;
const MAX_WRITE_ID_BYTES: usize = 256;
const WINDOW_TOKEN_PREFIX: &str = "window-";

pub(super) struct WriteRecord {
    fingerprint: blake3::Hash,
    engine_generation: u64,
    finish_fingerprint: Option<blake3::Hash>,
    state: WriteState,
}

enum WriteState {
    Preparing,
    Ready(Arc<WriteTarget>),
    Granted(Arc<WriteTarget>),
    Committing(Arc<WriteTarget>),
    Committed {
        result: Result<OwnerCommitWriteHixlReply, Status>,
        pinned: Option<Arc<WriteTarget>>,
    },
    Aborted,
    Unknown(Arc<WriteTarget>),
}

struct WriteTarget {
    access: PresentedRootAccess,
    file: RemoteFile,
    offset: u64,
    options: WriteOptions,
    length: usize,
    checksum: SlotChecksum,
    digest: Vec<u8>,
    lane: usize,
    window: Arc<WriteWindow>,
}

struct WriteWindow {
    reply: OwnerPrepareWriteHixlReply,
    slots: Vec<WriteSlot>,
}

struct WriteSlot {
    // Exposed local pointer from the exclusive reservation; no Rust borrow
    // survives descriptor publication. The provider pins the allocation.
    local_address: usize,
    length: usize,
}

impl WriteRecord {
    fn target(&self) -> Option<&WriteTarget> {
        match &self.state {
            WriteState::Ready(target)
            | WriteState::Granted(target)
            | WriteState::Committing(target)
            | WriteState::Unknown(target) => Some(target),
            WriteState::Committed { pinned, .. } => pinned.as_deref(),
            WriteState::Preparing | WriteState::Aborted => None,
        }
    }

    pub(super) fn needs_quarantine(&self) -> bool {
        matches!(
            self.state,
            WriteState::Ready(_)
                | WriteState::Granted(_)
                | WriteState::Unknown(_)
                | WriteState::Committed {
                    pinned: Some(_),
                    ..
                }
        )
    }

    pub(super) fn lane_id(&self) -> Option<usize> {
        self.target().map(|target| target.lane)
    }

    pub(super) fn slot_count(&self) -> usize {
        self.target().map_or(0, |target| target.window.slots.len())
    }
}

impl OwnerHixlService {
    /// Authorizes and pins the target windows without modifying the file.
    pub async fn prepare_write(
        &self,
        peer: String,
        request: OwnerPrepareWriteHixlRequest,
    ) -> Result<Response<OwnerPrepareWriteHixlReply>, Status> {
        if !self.buffered_writes {
            return Err(super::unsupported_status(
                "OwnerFs HIXL buffered writes are disabled",
            ));
        }
        let service = self.clone();
        self.spawn_phase(Operation::Prepare, move || {
            service.prepare_write_blocking(&peer, request)
        })
        .await
        .map(Response::new)
    }

    /// Consumes an authenticated native completion and returns Home's actual write result.
    pub async fn commit_write(
        &self,
        peer: String,
        request: OwnerFinishWriteHixlRequest,
    ) -> Result<Response<OwnerCommitWriteHixlReply>, Status> {
        if !self.buffered_writes {
            return Err(super::unsupported_status(
                "OwnerFs HIXL buffered writes are disabled",
            ));
        }
        let service = self.clone();
        self.spawn_phase(Operation::Validate, move || {
            service.commit_write_blocking(&peer, &request)
        })
        .await
        .map(Response::new)
    }

    /// Releases a terminal transfer or quarantines a transfer with unknown native completion.
    pub async fn abort_write(
        &self,
        peer: String,
        request: OwnerAbortWriteHixlRequest,
    ) -> Result<Response<OwnerAbortWriteHixlReply>, Status> {
        if !self.buffered_writes {
            return Err(super::unsupported_status(
                "OwnerFs HIXL buffered writes are disabled",
            ));
        }
        let service = self.clone();
        self.spawn_phase(Operation::Release, move || {
            service.abort_write_blocking(&peer, request)
        })
        .await
        .map(Response::new)
    }

    fn prepare_write_blocking(
        &self,
        peer: &str,
        request: OwnerPrepareWriteHixlRequest,
    ) -> Result<OwnerPrepareWriteHixlReply, Status> {
        validate_operation_id(request.session_id, &request.write_id)?;
        let access = presented_access(request.access.clone()).map_err(error_to_status)?;
        let checksum = self
            .lock_registry()?
            .bound_session(request.session_id, peer, &access.home_session_id)?
            .checksum;
        let input = WriteInput::parse(&request, access, checksum)?;
        let (lane, length) = (input.lane, input.length);
        let fingerprint = blake3::hash(&request.encode_to_vec());
        self.source
            .authorize_write(peer, &input.access, &input.file)
            .map_err(error_to_status)?;

        let (window_bytes, engine, _preparing) = {
            let mut registry = self.prepare_registry(peer, request.session_id, None)?;
            loop {
                let session = registry.admitting_session(request.session_id, peer)?;
                let Some(record) = session.writes.get(&request.write_id) else {
                    break;
                };
                if record.fingerprint != fingerprint {
                    return Err(invalid_write(
                        "OwnerFs HIXL write replay differs from its prepare",
                    ));
                }
                match &record.state {
                    WriteState::Ready(target) => {
                        let reply = target.window.descriptor(&request.write_id, target.length);
                        session.last_used = Instant::now();
                        return Ok(reply);
                    }
                    WriteState::Preparing => registry = self.wait_for_write(registry)?,
                    _ => {
                        return Err(invalid_write(
                            "OwnerFs HIXL write ticket is no longer writable",
                        ));
                    }
                }
            }
            let session = registry.admitting_session(request.session_id, peer)?;
            check_write_capacity(session)?;
            let window_bytes = session.prepare_slot_bytes(lane, request.slot_bytes)?;
            let engine = session.lanes[lane].clone();
            self.check_prepare_capacity(
                &registry,
                lane,
                length.div_ceil(window_bytes),
                "OwnerFs HIXL write exceeds the lane export pool",
            )?;
            let session = registry.admitting_session(request.session_id, peer)?;
            session.last_used = Instant::now();
            session.writes.insert(
                request.write_id.clone(),
                WriteRecord {
                    fingerprint,
                    engine_generation: engine.generation,
                    finish_fingerprint: None,
                    state: WriteState::Preparing,
                },
            );
            (window_bytes, engine, self.track_prepare(&registry))
        };

        let rollback_failed = Cell::new(false);
        let reply = (|| {
            // Reserve full capacity in the slots already needed by the write so a
            // tiny first write can grant a larger successor without another export.
            let lengths = (0..length.div_ceil(window_bytes))
                .map(|index| (MAX_TRANSFER_BYTES - index * window_bytes).min(window_bytes));
            let mut exports = self.reserve_exports(lane, lengths, &rollback_failed)?;
            let mut slots = Vec::with_capacity(exports.len());
            let mut windows = Vec::with_capacity(exports.len());
            for export in &mut exports {
                let bytes = export.as_mut_slice();
                // Exported memory may be readable too; never disclose previous slot contents.
                bytes.fill(0);
                windows.push(WriteSlot {
                    local_address: bytes.as_mut_ptr().expose_provenance(),
                    length: bytes.len(),
                });
                slots.push(export_descriptor(
                    export,
                    export.window().length,
                    Vec::new(),
                ));
            }
            let reply = OwnerPrepareWriteHixlReply {
                session_id: request.session_id,
                write_id: request.write_id.clone(),
                lane_id: request.lane_id,
                engine_id: engine.engine_id,
                engine_generation: engine.generation,
                slots,
            };
            let mut registry = self.lock_registry()?;
            let session = registry.admitting_session(request.session_id, peer)?;
            let record = write_record(session, &request.write_id, engine.generation)?;
            if !matches!(record.state, WriteState::Preparing) {
                return Err(invalid_write(
                    "OwnerFs HIXL write was aborted before publication",
                ));
            }
            let target = input.target(
                &request,
                Arc::new(WriteWindow {
                    reply,
                    slots: windows,
                }),
            );
            let reply = target.window.descriptor(&request.write_id, length);
            exports.into_iter().for_each(PendingExport::disarm);
            record.state = WriteState::Ready(Arc::new(target));
            session.last_used = Instant::now();
            self.transfers.writes_changed.notify_all();
            Ok(reply)
        })();
        if reply.is_err() {
            let mut registry = self.lock_registry()?;
            if let Ok(session) = registry.session_mut(request.session_id, peer)
                && session
                    .writes
                    .get(&request.write_id)
                    .is_some_and(|record| matches!(record.state, WriteState::Preparing))
            {
                session.writes.remove(&request.write_id);
            }
            self.transfers.writes_changed.notify_all();
            if rollback_failed.get() {
                self.quarantine_pending(&mut registry, true)?;
            }
        }
        reply
    }

    fn commit_write_blocking(
        &self,
        peer: &str,
        request: &OwnerFinishWriteHixlRequest,
    ) -> Result<OwnerCommitWriteHixlReply, Status> {
        validate_operation_id(request.session_id, &request.write_id)?;
        if request.reused.is_some() == request.window_token.is_empty() {
            return Err(invalid_write(
                "OwnerFs HIXL reused write requires metadata and token",
            ));
        }
        let fingerprint = blake3::hash(&request.encode_to_vec());
        let target = {
            let mut registry = self.lock_registry()?;
            loop {
                let frozen = registry.admission_frozen;
                let session = registry.session_mut(request.session_id, peer)?;
                let open = !session.closed && !frozen;
                if let Some(record) = session.writes.get_mut(&request.write_id) {
                    check_generation(record, request.engine_generation)?;
                    if record
                        .finish_fingerprint
                        .is_some_and(|hash| hash != fingerprint)
                    {
                        return Err(invalid_write("OwnerFs HIXL write commit replay differs"));
                    }
                    match &record.state {
                        WriteState::Ready(target) if open && request.reused.is_none() => {
                            let target = target.clone();
                            record.finish_fingerprint = Some(fingerprint);
                            record.state = WriteState::Committing(target.clone());
                            session.last_used = Instant::now();
                            break target;
                        }
                        WriteState::Committing(_) => {
                            registry = self.wait_for_write(registry)?;
                            continue;
                        }
                        WriteState::Committed { result, .. } => return result.clone(),
                        _ => {
                            return Err(invalid_write(
                                "OwnerFs HIXL write ticket cannot be committed",
                            ));
                        }
                    }
                }
                let reused = request.reused.as_ref().filter(|_| open).ok_or_else(|| {
                    invalid_write("OwnerFs HIXL write ticket cannot be committed")
                })?;
                break Self::consume_write_window(session, request, reused, fingerprint)?;
            }
        };
        // The first commit establishes native completion. Do not hold the registry across file I/O.
        let mut result = self.write_target(peer, &target);
        if let (Some(metrics), Ok(reply)) = (&self.owner_metrics, &result) {
            metrics.record_payload("server", "write", "hixl", u64::from(reply.written));
        }
        let reusable = request.request_next
            && result
                .as_ref()
                .is_ok_and(|reply| reply.written as usize == target.length);
        // Prepare zeroed the full reservation; a compliant writer only modifies
        // its transferred prefix. Keep the untouched tail and clear this prefix.
        if reusable {
            target.scrub_written_prefix();
        }
        let mut registry = self.lock_registry()?;
        let admitting = !registry.admission_frozen && !registry.draining;
        let session = registry.session_mut(request.session_id, peer)?;
        let release = if reusable
            && admitting
            && !session.closed
            && session.cancelled_successors.get(&request.write_id)
                != Some(&request.engine_generation)
            && check_write_capacity(session).is_ok()
        {
            let token = format!("{WINDOW_TOKEN_PREFIX}{}", uuid::Uuid::new_v4());
            let descriptor = target.window.descriptor(&token, target.window.capacity());
            session.writes.insert(
                token,
                WriteRecord {
                    fingerprint,
                    engine_generation: request.engine_generation,
                    finish_fingerprint: None,
                    state: WriteState::Granted(target.clone()),
                },
            );
            if let Ok(reply) = &mut result {
                reply.next_window = Some(descriptor);
            }
            Ok(())
        } else {
            self.release_write_target(&target)
        };
        write_record(session, &request.write_id, request.engine_generation)?.state =
            WriteState::Committed {
                result: result.clone(),
                pinned: release.is_err().then_some(target),
            };
        session.last_used = Instant::now();
        self.transfers.writes_changed.notify_all();
        if let Err(error) = release {
            afs_logging::error!("ownerfs.hixl_write_release_failed"; "error" => error.to_string());
            // Keep the file result authoritative even if recycling failed.
            let _ = self.quarantine_pending(&mut registry, true);
        }
        result
    }

    fn consume_write_window(
        session: &mut Session,
        request: &OwnerFinishWriteHixlRequest,
        reused: &OwnerPrepareWriteHixlRequest,
        fingerprint: blake3::Hash,
    ) -> Result<Arc<WriteTarget>, Status> {
        validate_write_id(request.session_id, &request.window_token)?;
        if reused.session_id != request.session_id || reused.write_id != request.write_id {
            return Err(invalid_write("OwnerFs HIXL reused write identity differs"));
        }
        let access = presented_access(reused.access.clone()).map_err(error_to_status)?;
        let input = WriteInput::parse(reused, access, session.checksum)?;
        let slot_bytes = session.prepare_slot_bytes(input.lane, reused.slot_bytes)?;
        let record = write_record(session, &request.window_token, request.engine_generation)?;
        let WriteState::Granted(grant) = &record.state else {
            return Err(invalid_write(
                "OwnerFs HIXL write window is no longer available",
            ));
        };
        if input.access != grant.access || input.file != grant.file || input.lane != grant.lane {
            return Err(Status::permission_denied(
                "OwnerFs HIXL write window binding differs",
            ));
        }
        if input.length > grant.window.capacity()
            || grant
                .window
                .slots
                .iter()
                .any(|slot| slot.length > slot_bytes)
        {
            return Err(invalid_write(
                "OwnerFs HIXL reused write exceeds its window",
            ));
        }
        let target = Arc::new(input.target(reused, grant.window.clone()));
        // Transfer ownership of the allocation under the registry lock. The consumed
        // grant never becomes live again, including when a prior reply is replayed.
        session.writes.remove(&request.window_token);
        session.writes.insert(
            request.write_id.clone(),
            WriteRecord {
                fingerprint,
                engine_generation: request.engine_generation,
                finish_fingerprint: Some(fingerprint),
                state: WriteState::Committing(target.clone()),
            },
        );
        session.last_used = Instant::now();
        Ok(target)
    }
    fn write_target(
        &self,
        peer: &str,
        target: &WriteTarget,
    ) -> Result<OwnerCommitWriteHixlReply, Status> {
        let mut data = Vec::new();
        data.try_reserve_exact(target.length)
            .map_err(|_| exhausted("OwnerFs HIXL write snapshot allocation failed"))?;
        for (slot, length) in target.window.prefix(target.length) {
            // SAFETY: the authenticated writer confirmed native completion and retired its
            // use of the group. The Committing record keeps the initialized allocation pinned
            // and excludes Abort/recycling until the snapshot and file operation finish.
            let bytes = unsafe {
                std::slice::from_raw_parts(
                    std::ptr::with_exposed_provenance(slot.local_address),
                    length,
                )
            };
            data.extend_from_slice(bytes);
        }
        if target.checksum.digest(&data) != target.digest {
            return Err(coded_status(
                afs_error::NODE_OWNER_WRITE_NOT_EXECUTED_CHECKSUM,
                "OwnerFs HIXL write payload checksum mismatch",
            ));
        }
        let written = self
            .source
            .write(
                peer,
                &target.access,
                &target.file,
                target.offset,
                &data,
                target.options,
            )
            .map_err(error_to_status)?;
        let written = u32::try_from(written)
            .ok()
            .filter(|written| (*written as usize) <= target.length)
            .ok_or_else(|| Status::internal("OwnerFs HIXL write count exceeds request length"))?;
        Ok(OwnerCommitWriteHixlReply {
            written,
            next_window: None,
        })
    }

    /// Resolve a lost Commit without performing any file I/O. Only the exact
    /// authenticated ticket may be fenced or retrieve its cached receipt.
    fn recover_write_blocking(
        &self,
        peer: &str,
        request: &OwnerFinishWriteHixlRequest,
    ) -> Result<OwnerAbortWriteHixlReply, Status> {
        validate_operation_id(request.session_id, &request.write_id)?;
        if request.reused.is_some() == request.window_token.is_empty() {
            return Err(invalid_write(
                "OwnerFs HIXL reused write requires metadata and token",
            ));
        }
        let fingerprint = blake3::hash(&request.encode_to_vec());
        let mut registry = self.lock_registry()?;
        loop {
            let session = registry.session_mut(request.session_id, peer)?;
            let target = if let Some(record) = session.writes.get_mut(&request.write_id) {
                check_generation(record, request.engine_generation)?;
                if record
                    .finish_fingerprint
                    .is_some_and(|hash| hash != fingerprint)
                {
                    return Err(invalid_write(
                        "OwnerFs HIXL write recovery differs from commit",
                    ));
                }
                match &mut record.state {
                    WriteState::Ready(target) if !session.closed && request.reused.is_none() => {
                        record.finish_fingerprint = Some(fingerprint);
                        target.clone()
                    }
                    WriteState::Committing(_) => {
                        registry = self.wait_for_write(registry)?;
                        continue;
                    }
                    WriteState::Committed { result, .. } => {
                        let next = result
                            .as_mut()
                            .ok()
                            .and_then(|reply| reply.next_window.take());
                        let receipt = recovery_receipt(result);
                        if let Some(next) = next {
                            self.discard_successor(
                                &mut registry,
                                peer,
                                request.session_id,
                                &next.write_id,
                            )?;
                        }
                        return Ok(receipt);
                    }
                    _ => {
                        return Err(invalid_write(
                            "OwnerFs HIXL write ticket cannot be recovered",
                        ));
                    }
                }
            } else {
                let reused = request
                    .reused
                    .as_ref()
                    .filter(|_| !session.closed)
                    .ok_or_else(|| {
                        invalid_write("OwnerFs HIXL write ticket cannot be recovered")
                    })?;
                Self::consume_write_window(session, request, reused, fingerprint)?
            };
            let result = Err(coded_status(
                afs_error::NODE_OWNER_WRITE_NOT_EXECUTED,
                "OwnerFs HIXL Commit was fenced before file mutation",
            ));
            let receipt = recovery_receipt(&result);
            let release = self.release_write_target(&target);
            write_record(session, &request.write_id, request.engine_generation)?.state =
                WriteState::Committed {
                    result,
                    pinned: release.is_err().then_some(target),
                };
            session.last_used = Instant::now();
            self.transfers.writes_changed.notify_all();
            if let Err(error) = release {
                afs_logging::error!("ownerfs.hixl_write_release_failed"; "error" => error.to_string());
                let _ = self.quarantine_pending(&mut registry, true);
            }
            return Ok(receipt);
        }
    }

    fn discard_successor(
        &self,
        registry: &mut Registry,
        peer: &str,
        session_id: u64,
        token: &str,
    ) -> Result<(), Status> {
        let session = registry.session_mut(session_id, peer)?;
        if let Some(grant) = session.writes.get_mut(token)
            && let WriteState::Granted(target) = &grant.state
        {
            if let Err(error) = self.release_write_target(target) {
                grant.state = WriteState::Unknown(target.clone());
                afs_logging::error!("ownerfs.hixl_write_release_failed"; "error" => error.to_string());
                let _ = self.quarantine_pending(registry, true);
            } else {
                session.writes.remove(token);
            }
        }
        Ok(())
    }

    fn abort_write_blocking(
        &self,
        peer: &str,
        request: OwnerAbortWriteHixlRequest,
    ) -> Result<OwnerAbortWriteHixlReply, Status> {
        if let Some(finish) = request.recover_commit.as_ref() {
            if !request.cancel_next
                || request.unknown
                || request.session_id != finish.session_id
                || request.write_id != finish.write_id
                || request.engine_generation != finish.engine_generation
            {
                return Err(invalid_write(
                    "OwnerFs HIXL write recovery identity differs",
                ));
            }
            return self.recover_write_blocking(peer, finish);
        }
        validate_write_id(request.session_id, &request.write_id)?;
        let mut registry = self.lock_registry()?;
        let session = registry.session_mut(request.session_id, peer)?;
        if request.cancel_next {
            if request.unknown
                || !session
                    .lanes
                    .iter()
                    .any(|lane| lane.generation == request.engine_generation)
            {
                return Err(invalid_write("invalid HIXL write successor cancellation"));
            }
            validate_operation_id(request.session_id, &request.write_id)?;
            let next = if let Some(record) = session.writes.get_mut(&request.write_id) {
                if record.engine_generation != request.engine_generation {
                    return Err(invalid_write("write cancellation generation differs"));
                }
                match &mut record.state {
                    WriteState::Committed { result, .. } => result
                        .as_mut()
                        .ok()
                        .and_then(|reply| reply.next_window.take())
                        .map(|window| window.write_id),
                    _ => None,
                }
            } else {
                None
            };
            // Cancellation may overtake the Commit RPC. Remember only the refusal
            // of its successor, leaving the file write and its receipt intact.
            if !session.cancelled_successors.contains_key(&request.write_id) {
                if session.cancelled_successors.len() >= MAX_WRITE_RECORDS {
                    return Err(exhausted("HIXL successor cancellation table is full"));
                }
                session
                    .cancelled_successors
                    .insert(request.write_id.clone(), request.engine_generation);
            }
            if let Some(token) = next
                && let Some(grant) = session.writes.get_mut(&token)
                && let WriteState::Granted(target) = &grant.state
            {
                if let Err(error) = self.release_write_target(target) {
                    grant.state = WriteState::Unknown(target.clone());
                    self.quarantine_pending(&mut registry, true)?;
                    return Err(error);
                }
                session.writes.remove(&token);
            }
            return Ok(OwnerAbortWriteHixlReply::default());
        }
        if !session.writes.contains_key(&request.write_id) {
            if !session
                .lanes
                .iter()
                .any(|lane| lane.generation == request.engine_generation)
            {
                return Err(Status::permission_denied(
                    "OwnerFs HIXL write generation does not match",
                ));
            }
            if request.write_id.starts_with(WINDOW_TOKEN_PREFIX) {
                return Ok(OwnerAbortWriteHixlReply::default());
            }
            check_write_capacity(session)?;
            // Abort can arrive ahead of a delayed Prepare. Fence the ID even if no slot exists yet.
            session.writes.insert(
                request.write_id.clone(),
                WriteRecord {
                    fingerprint: blake3::Hash::from_bytes([0; 32]),
                    engine_generation: request.engine_generation,
                    finish_fingerprint: None,
                    state: WriteState::Aborted,
                },
            );
        }
        let record = write_record(session, &request.write_id, request.engine_generation)?;
        let (pinned, release_error) = match &record.state {
            WriteState::Ready(target) | WriteState::Granted(target) => {
                let error = if request.unknown {
                    None
                } else {
                    self.release_write_target(target).err()
                };
                let pinned = request.unknown || error.is_some();
                record.state = if pinned {
                    WriteState::Unknown(target.clone())
                } else {
                    WriteState::Aborted
                };
                (pinned, error)
            }
            WriteState::Unknown(_) if request.unknown => (true, None),
            WriteState::Preparing | WriteState::Aborted => {
                record.state = WriteState::Aborted;
                (false, None)
            }
            _ => return Err(invalid_write("OwnerFs HIXL write ticket cannot be aborted")),
        };
        if !pinned && request.write_id.starts_with(WINDOW_TOKEN_PREFIX) {
            session.writes.remove(&request.write_id);
        }
        session.last_used = Instant::now();
        self.transfers.writes_changed.notify_all();
        if pinned {
            self.quarantine_pending(&mut registry, true)?;
        }
        if let Some(error) = release_error {
            return Err(error);
        }
        Ok(OwnerAbortWriteHixlReply::default())
    }

    fn wait_for_write<'a>(
        &'a self,
        registry: MutexGuard<'a, Registry>,
    ) -> Result<MutexGuard<'a, Registry>, Status> {
        let (registry, wait) = self
            .transfers
            .writes_changed
            .wait_timeout(registry, OWNER_HIXL_EXPORT_DEADLINE)
            .map_err(|_| Status::internal("OwnerFs HIXL registry lock is poisoned"))?;
        if wait.timed_out() {
            return Err(Status::deadline_exceeded(
                "OwnerFs HIXL write is still in progress",
            ));
        }
        Ok(registry)
    }

    fn release_write_target(&self, target: &WriteTarget) -> Result<(), Status> {
        let slots = target
            .window
            .reply
            .slots
            .iter()
            .map(|slot| (slot.slot_id, slot.slot_generation))
            .collect();
        // SAFETY: the authenticated writer reported terminal completion. A ready Abort
        // has no local borrow, and Commit has finished all its borrows before releasing.
        unsafe { self.provider.release_exports(target.lane, slots) }.map_err(runtime_status)
    }
}

fn recovery_receipt(
    result: &Result<OwnerCommitWriteHixlReply, Status>,
) -> OwnerAbortWriteHixlReply {
    let outcome = match result {
        Ok(reply) => Recovery::Written(reply.written),
        Err(status) => Recovery::Failure(OwnerWriteHixlFailure {
            grpc_code: status.code() as i32,
            message: status.message().to_owned(),
            details: status.details().to_vec(),
        }),
    };
    OwnerAbortWriteHixlReply {
        recovery: Some(outcome),
    }
}

fn validate_write_id(session_id: u64, write_id: &str) -> Result<(), Status> {
    if session_id == 0 || write_id.is_empty() || write_id.len() > MAX_WRITE_ID_BYTES {
        Err(invalid_write(
            "OwnerFs HIXL write ticket identity is invalid",
        ))
    } else {
        Ok(())
    }
}

fn check_write_capacity(session: &Session) -> Result<(), Status> {
    if session.writes.len() >= MAX_WRITE_RECORDS {
        return Err(exhausted(
            "OwnerFs HIXL write ticket capacity is exhausted; negotiate a new session",
        ));
    }
    Ok(())
}

fn write_record<'a>(
    session: &'a mut Session,
    write_id: &str,
    generation: u64,
) -> Result<&'a mut WriteRecord, Status> {
    let record = session
        .writes
        .get_mut(write_id)
        .ok_or_else(|| invalid_write("OwnerFs HIXL write ticket is unknown"))?;
    check_generation(record, generation)?;
    Ok(record)
}

fn check_generation(record: &WriteRecord, generation: u64) -> Result<(), Status> {
    if generation == 0 || record.engine_generation != generation {
        return Err(Status::permission_denied(
            "OwnerFs HIXL write generation does not match",
        ));
    }
    Ok(())
}

fn validate_operation_id(session_id: u64, write_id: &str) -> Result<(), Status> {
    validate_write_id(session_id, write_id)?;
    if write_id.starts_with(WINDOW_TOKEN_PREFIX) {
        return Err(invalid_write(
            "OwnerFs HIXL write ID uses a reserved prefix",
        ));
    }
    Ok(())
}

struct WriteInput {
    access: PresentedRootAccess,
    file: RemoteFile,
    lane: usize,
    length: usize,
    checksum: SlotChecksum,
    digest: Vec<u8>,
}

impl WriteInput {
    fn parse(
        request: &OwnerPrepareWriteHixlRequest,
        access: PresentedRootAccess,
        checksum: SlotChecksum,
    ) -> Result<Self, Status> {
        let length = usize::try_from(request.length)
            .ok()
            .filter(|length| (1..=MAX_TRANSFER_BYTES).contains(length))
            .ok_or_else(|| invalid_write("OwnerFs HIXL write length is invalid"))?;
        let lane = usize::try_from(request.lane_id)
            .map_err(|_| invalid_write("OwnerFs HIXL write lane overflows"))?;
        if request.data_checksum_algorithm != checksum.wire() as i32 {
            return Err(invalid_write(
                "OwnerFs HIXL write digest algorithm differs from negotiation",
            ));
        }
        if request.data_checksum.len() != checksum.len() {
            return Err(invalid_write("OwnerFs HIXL write digest length is invalid"));
        }
        let handle = request
            .handle
            .as_ref()
            .ok_or_else(|| invalid_write("OwnerFs HIXL write is missing its open handle"))?;
        let file = remote_file_for_handle(&access, handle.opaque.clone());
        Ok(Self {
            access,
            file,
            lane,
            length,
            checksum,
            digest: request.data_checksum.clone(),
        })
    }

    fn target(
        self,
        request: &OwnerPrepareWriteHixlRequest,
        window: Arc<WriteWindow>,
    ) -> WriteTarget {
        WriteTarget {
            access: self.access,
            file: self.file,
            offset: request.offset,
            options: WriteOptions {
                kill_suidgid: request.kill_suidgid,
            },
            length: self.length,
            checksum: self.checksum,
            digest: self.digest,
            lane: self.lane,
            window,
        }
    }
}

impl WriteTarget {
    fn scrub_written_prefix(&self) {
        for (slot, length) in self.window.prefix(self.length) {
            // SAFETY: only the Committing record owns the reservation. Native WRITE
            // completed, and this prefix has no live Rust borrow or published successor.
            unsafe {
                std::ptr::with_exposed_provenance_mut::<u8>(slot.local_address)
                    .write_bytes(0, length);
            }
        }
    }
}

impl WriteWindow {
    fn capacity(&self) -> usize {
        self.slots.iter().map(|slot| slot.length).sum()
    }

    fn prefix(&self, length: usize) -> impl Iterator<Item = (&WriteSlot, usize)> {
        self.slots.iter().scan(length, |remaining, slot| {
            let length = slot.length.min(*remaining);
            *remaining -= length;
            (length != 0).then_some((slot, length))
        })
    }

    fn descriptor(&self, id: &str, length: usize) -> OwnerPrepareWriteHixlReply {
        let mut reply = self.reply.clone();
        reply.write_id = id.to_owned();
        let mut lengths = self.prefix(length).map(|(_, length)| length as u64);
        reply.slots.retain_mut(|slot| {
            if let Some(length) = lengths.next() {
                slot.length = length;
                true
            } else {
                false
            }
        });
        reply
    }
}

fn invalid_write(message: &'static str) -> Status {
    coded_status(afs_error::NODE_TRANSFER_INVALID, message)
}
