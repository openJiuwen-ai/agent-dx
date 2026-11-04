//! Native reads through the verified peer disk cache.

use super::*;

/// One low-frequency fill stage. Total encloses the whole fill. A streaming
/// receive also includes private disk writes; store then covers only the Ready
/// commit. Nested producer/write observations must not be added to this parent.
struct PeerFillStage<'a> {
    local: &'a LocalChunkStore,
    pub(super) stage: &'static str,
    pub(super) clock: PeerFillClock,
    active: bool,
}

impl<'a> PeerFillStage<'a> {
    fn start(local: &'a LocalChunkStore, stage: &'static str) -> Self {
        Self::since(local, stage, PeerFillClock::start())
    }

    fn since(local: &'a LocalChunkStore, stage: &'static str, clock: PeerFillClock) -> Self {
        Self {
            local,
            stage,
            clock,
            active: true,
        }
    }
}

impl Drop for PeerFillStage<'_> {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let (wall_ns, cpu_ns) = self.clock.elapsed();
        self.local
            .record_peer_cache_fill_stage(self.stage, wall_ns, cpu_ns);
    }
}

#[cfg(test)]
pub(super) struct PeerCacheRecheckHook {
    pub(super) stage: &'static str,
    pub(super) entered: std::sync::mpsc::Sender<()>,
    pub(super) release: std::sync::mpsc::Receiver<()>,
}

impl DfsReadEngine {
    pub(super) fn source_matches(&self, batch: &ReadBatch, source: &SourceCandidate) -> bool {
        let grant = &source.read_grant;
        Some(&grant.file_version_id) == batch.file_version_id.as_ref()
            && grant.layout_root_id == batch.layout_root_id
            && grant.namespace_id == self.namespace_id
            && grant.caller_node_id == self.local_node_id
            && grant.caller_node_epoch != 0
            && grant.expires_at_unix_ms > now_unix_ms()
            && !grant.token.is_empty()
    }

    pub(super) fn peer_cache_source_eligible(
        &self,
        batch: &ReadBatch,
        chunk: &ChunkObject,
        source: &SourceCandidate,
    ) -> bool {
        self.source_matches(batch, source)
            && source.state.is_readable()
            && matches!(
                source.role,
                CopyRole::DurableReplica | CopyRole::VerifiedCache
            )
            && source.chunk_id == chunk.id
            && !source.copy_id.0.is_empty()
            && source
                .data_endpoint
                .as_deref()
                .is_some_and(|endpoint| !endpoint.is_empty())
            && matches!(&source.location, CopyLocation::Node {
                node_id, node_epoch, device_id, device_epoch, catalog_revision,
            } if node_id != &self.local_node_id && !node_id.is_empty() && *node_epoch != 0
                && !device_id.is_empty() && *device_epoch != 0 && *catalog_revision != 0)
    }

    fn check_peer_cache_authority(
        &self,
        batch: &ReadBatch,
        source: &SourceCandidate,
        authority: &NativeReadAuthoritySnapshot,
    ) -> Result<()> {
        self.check_native_read_authority(authority)?;
        if !self.source_matches(batch, source)
            || (authority.epoch != 0 && source.read_grant.caller_node_epoch != authority.epoch)
        {
            return Err(external_denied(
                "Peer cache read grant expired or changed before publication",
            ));
        }
        Ok(())
    }

    /// Disposable cache I/O is never a source/authorization result. The
    /// caller keeps its own final grant check and the normal verified Peer
    /// fallback; this also covers rechecks after waiting for another fill.
    fn read_optional_peer_cache(
        &self,
        operation: &mut super::super::peer_cache::PeerCacheOperation<'_>,
        chunk: &ChunkObject,
        offset: u64,
        output: &mut [u8],
    ) -> bool {
        match operation.read_range(chunk, offset, output) {
            Ok(hit) => hit,
            Err(error) => {
                self.local.record_peer_disk_cache_event("corrupt_or_io");
                afs_logging::warn!("dfs.peer_cache_read_failed"; "error" => error.to_string());
                false
            }
        }
    }

    #[cfg(test)]
    fn wait_peer_cache_recheck_for_test(&self, stage: &'static str) {
        let hook = {
            let mut slot = self.peer_cache_recheck_hook.lock().unwrap();
            if slot.as_ref().is_some_and(|hook| hook.stage == stage) {
                slot.take()
            } else {
                None
            }
        };
        if let Some(hook) = hook {
            hook.entered.send(()).unwrap();
            hook.release.recv_timeout(Duration::from_secs(5)).unwrap();
        }
    }

    pub(super) async fn read_native_peer_cache_mode_async(
        &self,
        batch: &ReadBatch,
        sources: &[ChunkSources],
        out: &mut [u8],
        final_owned: bool,
        stream_only: bool,
    ) -> Result<bool> {
        let Some(cache) = &self.peer_cache else {
            return Ok(false);
        };
        // The cache never changes external-backend routing or complex batch
        // scatter/retry semantics. Only the currently requested object is read.
        if self.external_backend.is_some() || batch.ops.len() != 1 {
            return Ok(false);
        }
        if cache.namespace_id() != &self.namespace_id {
            return Err(invalid("Peer cache namespace differs from the read engine"));
        }
        let op = &batch.ops[0];
        let Some(set) = sources.iter().find(|set| set.chunk_id == op.chunk_id) else {
            return Ok(false);
        };
        let Some(chunk) = &set.chunk else {
            return Ok(false);
        };
        if chunk.is_segment_crc32c() {
            return Ok(false);
        }
        if chunk.id != op.chunk_id
            || chunk.length == 0
            || set.allowed_ranges.len() > MAX_DFS_READ_OPS
            || set.allowed_ranges.iter().any(|(start, length)| {
                *length == 0
                    || start
                        .checked_add(*length)
                        .is_none_or(|end| end > chunk.length)
            })
        {
            self.invalidate_native_batch_authority(batch);
            return Err(invalid(
                "Peer cache source has an invalid immutable object or grant range",
            ));
        }
        if chunk.length > crate::node::chunk::MAX_STAGED_CHUNK_BYTES as u64
            || set.allowed_ranges.is_empty()
        {
            return Ok(false);
        }
        if !range_covered(&set.allowed_ranges, op.chunk_offset, op.length) {
            return Err(external_denied(
                "Peer cache requested range is outside the committed grant",
            ));
        }
        let source = set
            .sources
            .iter()
            .find(|source| self.peer_cache_source_eligible(batch, chunk, source));
        let Some(source) = source else {
            return Ok(false);
        };
        let authority = self.capture_native_read_authority()?;
        self.check_peer_cache_authority(batch, source, &authority)?;
        // Acquire the one synchronous cache operation before any full-object
        // allocation. No retained payload, additional fill or prefetch queue.
        let mut total = PeerFillStage {
            local: &self.local,
            stage: "total",
            clock: PeerFillClock::start(),
            active: false,
        };
        let mut operation = match crate::node::bridge::local_io(|| cache.operation()) {
            Ok(operation) => operation,
            Err(error) => {
                self.local.record_peer_disk_cache_event("fallback");
                afs_logging::warn!("dfs.peer_cache_unavailable"; "error" => error.to_string());
                return Ok(false);
            }
        };
        let lock_wait_elapsed = total.clock.elapsed();
        let lookup_clock = PeerFillClock::start();
        self.check_peer_cache_authority(batch, source, &authority)?;
        let start = op.output_offset;
        let end = start + op.length as usize; // batch validated before scheduling
        if crate::node::bridge::local_io(|| {
            self.read_optional_peer_cache(
                &mut operation,
                chunk,
                op.chunk_offset,
                &mut out[start..end],
            )
        }) {
            self.local.record_peer_disk_cache_event("hit");
            self.check_peer_cache_authority(batch, source, &authority)?;
            return Ok(true);
        }
        self.local.record_peer_disk_cache_event("miss");
        // Partial layout grants permit their requested ranges, never an
        // expanded full-object Peer fetch. Unknown old-server grants fall back.
        if !final_owned
            || !range_covered(&set.allowed_ranges, 0, chunk.length)
            || chunk
                .length
                .checked_add(out.len() as u64)
                .is_none_or(|bytes| bytes > self.config.max_inflight_bytes)
        {
            self.local.record_peer_disk_cache_event("fallback");
            return Ok(false);
        }
        if !operation.has_control_capacity() {
            self.local.record_peer_disk_cache_event("fallback");
            return Ok(false);
        }
        // Share only immutable data work. Every caller retains its own grant,
        // deadline and final delivery check; a stopped leader cannot authorize
        // another request. Flight wait and the one body permit never hold the
        // cache metadata lock.
        let mut leader = match cache
            .fill_flight_async(chunk, || {
                self.check_peer_cache_authority(batch, source, &authority)
            })
            .await?
        {
            crate::node::peer_cache::FillRole::Leader(leader) => leader,
            crate::node::peer_cache::FillRole::Finished(outcome) => {
                self.check_peer_cache_authority(batch, source, &authority)?;
                match outcome {
                    crate::node::peer_cache::FillOutcome::Ready => {
                        #[cfg(test)]
                        self.wait_peer_cache_recheck_for_test("follower_ready");
                        let hit = crate::node::bridge::local_io(|| {
                            self.read_optional_peer_cache(
                                &mut operation,
                                chunk,
                                op.chunk_offset,
                                &mut out[start..end],
                            )
                        });
                        self.check_peer_cache_authority(batch, source, &authority)?;
                        return Ok(hit);
                    }
                    crate::node::peer_cache::FillOutcome::Uncached => return Ok(false),
                    crate::node::peer_cache::FillOutcome::Failed(_) => {
                        // A flight is shared by object, not by source, grant or
                        // deadline. Its leader's failure cannot decide another
                        // caller's read, including source-specific corruption.
                        // Continue once through the existing bounded range path;
                        // do not rejoin this still-retained terminal flight.
                        self.local.record_peer_disk_cache_event("fallback");
                        return Ok(false);
                    }
                }
            }
        };
        let result = async {
            let mut use_stream = self.transfer.supports_verified_object_sink()
                && (crate::node::chunk::MAX_STAGED_CHUNK_BYTES as u64)
                    .checked_add(out.len() as u64)
                    .is_some_and(|bytes| bytes <= self.config.max_inflight_bytes);
            if stream_only && !use_stream {
                leader.finish(crate::node::peer_cache::FillOutcome::Uncached);
                return Ok(false);
            }
            if use_stream {
                operation.acquire_fill_async(true, || {
                    self.check_peer_cache_authority(batch, source, &authority)
                }).await?;
            } else {
                operation
                    .acquire_fill_async(false, || self.check_peer_cache_authority(batch, source, &authority)).await?;
            }
            // A completed earlier fill may have become Ready while this leader
            // waited for the global body allowance. Recheck before any allocation.
            #[cfg(test)]
            self.wait_peer_cache_recheck_for_test("leader_recheck");
            if crate::node::bridge::local_io(||self.read_optional_peer_cache(
                &mut operation,
                chunk,
                op.chunk_offset,
                &mut out[start..end],
            )) {
                self.check_peer_cache_authority(batch, source, &authority)?;
                leader.finish(crate::node::peer_cache::FillOutcome::Ready);
                return Ok(true);
            }
            // Emit fill timings only after an actual complete-object fill has been
            // admitted. A cache hit or range fallback never increments fill counts.
            let lookup_elapsed = lookup_clock.elapsed();
            // The guard was declared before `operation`, so total also includes
            // its drain/observation drop and the private body's release.
            total.active = true;
            self.local.record_peer_cache_fill_stage(
                "lock_wait",
                lock_wait_elapsed.0,
                lock_wait_elapsed.1,
            );
            self.local
                .record_peer_cache_fill_stage("lookup", lookup_elapsed.0, lookup_elapsed.1);
            let mut preparation = Some(PeerFillStage::start(&self.local, "prepare"));
            self.check_peer_cache_authority(batch, source, &authority)?;
            let length = usize::try_from(chunk.length)
                .map_err(|_| invalid("Peer cache object length is too large"))?;
            // Only an unsupported streaming adapter allocates the legacy body.
            // After that decision, retries keep the old mechanism rather than hold
            // a full Vec alongside a new streaming pipeline reservation.
            let mut _legacy_fill_work = None;
            let mut body = Vec::new();
            let version = batch
                .file_version_id
                .clone()
                .ok_or_else(|| invalid("verified Peer cache fill requires a fixed FileVersion"))?;
            let mut last_error = None;
            // Each distinct eligible source gets one full-object attempt. A proof
            // is consumed entirely in its success branch, never mixed with another
            // attempt or reconstructed by rehashing an already completed stream.
            let mut attempted_copies = BTreeSet::new();
            for source in set
                .sources
                .iter()
                .filter(|source| self.peer_cache_source_eligible(batch, chunk, source))
                .take(MAX_DFS_READ_OPS)
            {
                if !attempted_copies.insert(&source.copy_id) {
                    continue;
                }
                self.check_peer_cache_authority(batch, source, &authority)?;
                let attempt = PeerReadBatch {
                    file_version_id: version.clone(),
                    layout_root_id: batch.layout_root_id.clone(),
                    operations: vec![ResolvedReadOp {
                        op: ChunkReadOp {
                            chunk_id: chunk.id.clone(),
                            chunk_offset: 0,
                            length: chunk.length,
                            output_offset: 0,
                        },
                        source: source.clone(),
                    }],
                };
                if use_stream {
                    // The adapter's Node-shared session owns transport work until
                    // its producers and every handed-off frame have drained.
                    let mut sink = crate::node::bridge::local_io(||operation.begin_stream(chunk))?;
                    if stream_only && !sink.is_caching() {
                        leader.finish(crate::node::peer_cache::FillOutcome::Uncached);
                        return Ok(false);
                    }
                    let output_pointer = out[start..end].as_ptr() as usize;
                    let output_length = end - start;
                    let prepare_elapsed = preparation.as_ref().map(|stage| stage.clock.elapsed());
                    let receive_clock = PeerFillClock::start();
                    let received = self.transfer.read_verified_object_to_sink_async(
                        &attempt,
                        chunk,
                        op.chunk_offset,
                        &mut out[start..end],
                        &mut sink,
                        None,
                    ).await;
                    if !received.as_ref().is_ok_and(Option::is_none) {
                        // An unsupported probe starts no RPC. Retain the legacy
                        // prepare clock in that case instead of inventing an
                        // extra receive observation or overlapping prepare work.
                        if let Some(mut stage) = preparation.take() {
                            stage.active = false;
                        }
                        if let Some(elapsed) = prepare_elapsed {
                            self.local
                                .record_peer_cache_fill_stage("prepare", elapsed.0, elapsed.1);
                        }
                        let elapsed = receive_clock.elapsed();
                        self.local.record_peer_cache_fill_stage(
                            "receive_verify",
                            elapsed.0,
                            elapsed.1,
                        );
                        preparation = Some(PeerFillStage::start(&self.local, "prepare"));
                    }
                    match received {
                        Ok(Some(completed)) => {
                            if completed.chunk() != chunk
                                || completed.requested_offset() != op.chunk_offset
                                || completed.requested_length() != output_length
                                || completed.root_digest() != &chunk.content_digest.bytes
                                || !completed
                                    .matches_private_output_identity(output_pointer, output_length)
                            {
                                return Err(corrupt(
                                    "streamed Peer completion changed trusted identity or private output",
                                ));
                            }
                            self.check_peer_cache_authority(batch, source, &authority)?;
                            drop(preparation.take());
                            let stored = {
                                let _store = PeerFillStage::start(&self.local, "store");
                                crate::node::bridge::local_io(||sink.commit(completed))
                            };
                            let _delivery = PeerFillStage::start(&self.local, "deliver");
                            leader.finish(if stored.as_ref().is_ok_and(|stored| *stored) {
                                crate::node::peer_cache::FillOutcome::Ready
                            } else {
                                crate::node::peer_cache::FillOutcome::Uncached
                            });
                            match stored {
                                Ok(true) => self.local.record_peer_disk_cache_event("fill"),
                                Ok(false) => self.local.record_peer_disk_cache_event("fill_failed"),
                                Err(error) => {
                                    self.local.record_peer_disk_cache_event("fill_failed");
                                    afs_logging::warn!("dfs.peer_cache_stream_commit_failed"; "error" => error.to_string());
                                }
                            }
                            // The adapter already extracted the range into the
                            // unpublished final output and counted actual copies.
                            // Cache I/O/quota failure does not cause another RPC.
                            self.check_peer_cache_authority(batch, source, &authority)?;
                            return Ok(true);
                        }
                        Ok(None) => use_stream = false,
                        Err(error) => {
                            self.local.record_peer_disk_cache_event("fill_failed");
                            self.invalidate_native_batch_on_error(batch, &error);
                            if crate::node::fault::classify::read_authority_rejected(&error) {
                                return Err(error);
                            }
                            self.check_native_read_authority(&authority)?;
                            last_error = Some(error);
                            continue;
                        }
                    }
                }
                // A parallel child may never switch to the old full-body or
                // range transport. Its batch owner joins every child and drops
                // all fragment scratch before the original serial fallback.
                if stream_only {
                    leader.finish(crate::node::peer_cache::FillOutcome::Uncached);
                    return Ok(false);
                }
                // The unsupported sink and its pipeline allowance have dropped
                // before the original complete-object Vec is admitted.
                if body.is_empty() {
                    // The streaming adapter and sink have drained before changing
                    // admission modes. Legacy owns the original exclusive body
                    // permit, so its full Vec never coexists with another stream.
                    operation.release_fill();
                    operation.acquire_fill_async(false,|| {
                        self.check_peer_cache_authority(batch, source, &authority)
                    }).await?;
                    _legacy_fill_work = self.local.peer_disk_cache_fill_work(length, out.len());
                    body.resize(length, 0);
                }
                drop(preparation.take());
                let received = {
                    let _receive = PeerFillStage::start(&self.local, "receive_verify");
                    self.transfer.read_verified_object_async(
                        &attempt,
                        chunk,
                        &mut body,
                        cache.proof_budget(),
                    ).await
                };
                // Preparation resumes for proof identity/authorization checks, or
                // for error bookkeeping and the next bounded copy attempt.
                preparation = Some(PeerFillStage::start(&self.local, "prepare"));
                match received {
                    Ok(verified) => {
                        if verified.chunk() != chunk
                            || verified.bytes().len() as u64 != chunk.length
                        {
                            return Err(corrupt(
                                "verified Peer object proof changed trusted identity",
                            ));
                        }
                        if !verified.uses_completed_stream_proof()
                            && self.transfer.verification_metrics().is_none()
                        {
                            self.local.record_read_hash("root_fallback", chunk.length);
                        }
                        self.check_peer_cache_authority(batch, source, &authority)?;
                        drop(preparation.take());
                        let stored = {
                            let _store = PeerFillStage::start(&self.local, "store");
                            crate::node::bridge::local_io(||operation.store_verified(&verified))
                        };
                        let _delivery = PeerFillStage::start(&self.local, "deliver");
                        leader.finish(if stored.as_ref().is_ok_and(|stored| *stored) {
                            crate::node::peer_cache::FillOutcome::Ready
                        } else {
                            crate::node::peer_cache::FillOutcome::Uncached
                        });
                        match stored {
                            Ok(true) => self.local.record_peer_disk_cache_event("fill"),
                            Ok(false) => self.local.record_peer_disk_cache_event("fill_failed"),
                            Err(error) => {
                                self.local.record_peer_disk_cache_event("fill_failed");
                                afs_logging::warn!("dfs.peer_cache_write_failed"; "error" => error.to_string());
                            }
                        }
                        self.check_peer_cache_authority(batch, source, &authority)?;
                        let chunk_start = op.chunk_offset as usize;
                        out[start..end].copy_from_slice(
                            &verified.bytes()[chunk_start..chunk_start + op.length as usize],
                        );
                        self.local.record_read_copy("fill_to_output", op.length);
                        self.check_peer_cache_authority(batch, source, &authority)?;
                        return Ok(true);
                    }
                    Err(error) => {
                        self.local.record_peer_disk_cache_event("fill_failed");
                        self.invalidate_native_batch_on_error(batch, &error);
                        if crate::node::fault::classify::read_authority_rejected(&error) {
                            return Err(error);
                        }
                        self.check_native_read_authority(&authority)?;
                        last_error = Some(error);
                    }
                }
            }
            Err(last_error
                .unwrap_or_else(|| corrupt("Peer cache fill has no readable authorized source")))
        }.await;
        if let Err(error) = &result {
            // An individual leader's grant/session decision cannot revoke an
            // independently authorized follower. No Ready is installed when
            // the precommit check fails; followers take their own safe path.
            let outcome = if crate::node::fault::classify::read_authority_rejected(error) {
                crate::node::peer_cache::FillOutcome::Uncached
            } else {
                crate::node::peer_cache::FillOutcome::Failed(error.clone())
            };
            leader.finish(outcome);
        }
        result
    }
}
