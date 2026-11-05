//! One verified cache read operation and its on-disk helpers.

use super::*;

impl<'a> PeerCacheOperation<'a> {
    pub(crate) fn has_control_capacity(&self) -> bool {
        self._metadata.is_some()
    }

    pub(crate) fn acquire_fill(&mut self, check: impl Fn() -> Result<()>) -> Result<()> {
        self.acquire_fill_mode(false, check)
    }

    pub(crate) fn acquire_stream_fill(&mut self, check: impl Fn() -> Result<()>) -> Result<()> {
        self.acquire_fill_mode(true, check)
    }

    pub(crate) fn try_acquire_stream_fill(&mut self) -> Result<bool> {
        if self.fill.is_some() {
            return Ok(true);
        }
        let Some(mut state) = self.cache.try_lock_state()? else {
            return Ok(false);
        };
        if state.closed
            || state.foreground_waiters != 0
            || state.fill_active
            || state.stream_active >= 2
        {
            return Ok(false);
        }
        state.stream_active += 1;
        self.fill = Some(FillPermit {
            cache: self.cache,
            streaming: true,
        });
        drop(state);
        Ok(true)
    }

    pub(crate) fn release_fill(&mut self) {
        drop(self.fill.take());
    }

    fn acquire_fill_mode(&mut self, streaming: bool, check: impl Fn() -> Result<()>) -> Result<()> {
        if let Some(fill) = &self.fill {
            if fill.streaming == streaming || !fill.streaming {
                return Ok(());
            }
            return Err(invalid(
                "legacy fill requires releasing the stream allowance",
            ));
        }
        let _waiting = FillWaiter::new(self.cache)?;
        loop {
            check()?;
            let mut state = self.cache.lock_state()?;
            if state.closed {
                return Err(unavailable("Peer cache is stopped"));
            }
            if !state.fill_active
                && if streaming {
                    state.stream_active < 2
                } else {
                    state.stream_active == 0
                }
            {
                if streaming {
                    state.stream_active += 1;
                } else {
                    state.fill_active = true;
                }
                self.fill = Some(FillPermit {
                    cache: self.cache,
                    streaming,
                });
                drop(state);
                self.cache.report_resources();
                return Ok(());
            }
            drop(
                self.cache
                    .changed
                    .wait_timeout(state, Duration::from_millis(25))
                    .map_err(|_| unavailable("Peer fill admission lock poisoned"))?,
            );
        }
    }

    pub(crate) async fn acquire_fill_async(
        &mut self,
        streaming: bool,
        check: impl Fn() -> Result<()> + Send + Sync,
    ) -> Result<()> {
        if let Some(fill) = &self.fill {
            if fill.streaming == streaming || !fill.streaming {
                return Ok(());
            }
            return Err(invalid(
                "legacy fill requires releasing the stream allowance",
            ));
        }
        let _waiting = FillWaiter::new(self.cache)?;
        loop {
            let changed = self.cache.asynchronous_change.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            check()?;
            let admitted = {
                let mut state = self.cache.lock_state()?;
                if state.closed {
                    return Err(unavailable("Peer cache is stopped"));
                }
                if !state.fill_active
                    && if streaming {
                        state.stream_active < 2
                    } else {
                        state.stream_active == 0
                    }
                {
                    if streaming {
                        state.stream_active += 1
                    } else {
                        state.fill_active = true
                    }
                    self.fill = Some(FillPermit {
                        cache: self.cache,
                        streaming,
                    });
                    true
                } else {
                    false
                }
            };
            if admitted {
                self.cache.report_resources();
                return Ok(());
            }
            let _ = tokio::time::timeout(Duration::from_millis(25), changed).await;
        }
    }

    pub fn hash_work(&self) -> PeerCacheHashWork {
        self.state.stats.hash_work
    }

    /// A miss/error may have modified private output; never publish that output.
    pub fn begin_stream<'operation>(
        &'operation mut self,
        chunk: &ChunkObject,
    ) -> Result<PeerStreamSink<'operation>> {
        self.acquire_stream_fill(|| Ok(()))?;
        self.start_stream(chunk, true, None)
    }

    pub(crate) fn try_begin_stream<'operation>(
        &'operation mut self,
        chunk: &ChunkObject,
    ) -> Result<Option<PeerStreamSink<'operation>>> {
        if !self.try_acquire_stream_fill()? {
            return Ok(None);
        }
        let generation = {
            let Some(mut state) = self.cache.try_lock_state()? else {
                return Ok(None);
            };
            let generation = state.next_pending_generation;
            state.next_pending_generation = generation
                .checked_add(1)
                .ok_or_else(|| unavailable("Peer pending generation exhausted"))?;
            generation
        };
        self.start_stream(chunk, false, Some(generation)).map(Some)
    }

    fn start_stream<'operation>(
        &'operation mut self,
        chunk: &ChunkObject,
        allow_eviction: bool,
        reserved_generation: Option<u64>,
    ) -> Result<PeerStreamSink<'operation>> {
        validate_chunk(chunk)?;
        if chunk.length > MAX_OBJECT_BYTES {
            return Err(invalid("Peer stream object exceeds bound"));
        }
        let key = self.cache.key(chunk)?;
        let generation = if let Some(generation) = reserved_generation {
            generation
        } else {
            let mut state = self.cache.lock_state()?;
            let generation = state.next_pending_generation;
            state.next_pending_generation = generation
                .checked_add(1)
                .ok_or_else(|| unavailable("Peer pending generation exhausted"))?;
            generation
        };
        let mut sink = PeerStreamSink {
            cache: self.cache,
            _fill: self
                .fill
                .as_ref()
                .ok_or_else(|| unavailable("Peer fill allowance missing"))?,
            chunk: chunk.clone(),
            key,
            received: 0,
            header_length: 0,
            file: None,
            pending_identity: None,
            pending_generation: generation,
            owns_pending: false,
            committed: false,
            prepare_error: None,
            allow_eviction,
            entry_lease: None,
            identity: Arc::new(()),
            #[cfg(test)]
            write_delay: std::time::Duration::ZERO,
        };
        if let Err(error) = sink.prepare() {
            sink.prepare_error = Some(error.clone());
            sink.disable(&error);
        }
        Ok(sink)
    }

    pub fn read_range(&mut self, chunk: &ChunkObject, offset: u64, out: &mut [u8]) -> Result<bool> {
        if self._metadata.is_none() {
            return self.read_range_root_fallback(chunk, offset, out);
        }
        let prepare_started = Instant::now();
        let selected = match self.select_entry(chunk, offset, out.len()) {
            Ok(Some(selected)) => selected,
            early => {
                self.state.stats.read_stages.prepare_ns = self
                    .state
                    .stats
                    .read_stages
                    .prepare_ns
                    .saturating_add(elapsed_ns(prepare_started));
                return early.map(|_| false);
            }
        };
        // Only indexed hit attempts: misses above do not count as disk hits.
        // Includes opening/header/identity/range verification and finalization.
        let _diagnostic = self.cache.read_work.as_ref().and_then(|metrics| {
            metrics.coarse_diagnostic("cache_hit_read_verify", out.len() as u64)
        });
        let object_diagnostic = self.cache.read_work.as_ref().and_then(|metrics| {
            metrics.object_diagnostic(
                crate::node::metrics::read_timing::ReadObjectDiagnosticStage::CacheHitReadVerify,
                &chunk.id.0,
            )
        });
        let mut finalize_started = None;
        let result = self.read_verified_range(
            chunk,
            offset,
            out,
            &selected,
            prepare_started,
            &mut finalize_started,
        );
        let finalize_started = finalize_started.unwrap_or_else(Instant::now);
        self.settle_range_read(result, selected, finalize_started, object_diagnostic)
    }

    /// Look up the indexed entry and pin its generation; a miss is `None`.
    fn select_entry(
        &mut self,
        chunk: &ChunkObject,
        offset: u64,
        out_len: usize,
    ) -> Result<Option<SelectedEntry<'a>>> {
        self.cache.check_root()?;
        validate_chunk(chunk)?;
        if offset
            .checked_add(out_len as u64)
            .is_none_or(|end| end > chunk.length)
        {
            return Err(invalid("Peer cache range exceeds immutable object"));
        }
        let key = self.cache.key(chunk)?;
        let selected = {
            let state = self.cache.lock_state()?;
            state.entries.get(&key).map(|entry| {
                (
                    entry.chunk.clone(),
                    entry.charge,
                    entry.identity,
                    entry.touched,
                    entry.proof.clone(),
                    entry.generation.clone(),
                )
            })
        };
        let Some((object, charge, identity, touched, proof, generation)) = selected else {
            self.state.stats.misses = self.state.stats.misses.saturating_add(1);
            return Ok(None);
        };
        if object != *chunk {
            return Err(invalid("Peer cache index identity differs"));
        }
        let pin = EntryPin {
            cache: self.cache,
            generation: Some(generation),
        };
        self.state.entries.insert(
            key.clone(),
            Entry {
                chunk: object,
                charge,
                identity,
                touched,
                proof,
                generation: Arc::new(()),
                _metadata: None,
                #[cfg(test)]
                drop_observation: None,
            },
        );
        Ok(Some(SelectedEntry {
            key,
            expected: identity,
            pin,
        }))
    }

    /// Open the pinned file, check its identity and header, then verify the
    /// requested range against a reusable proof or a complete root scan.
    fn read_verified_range(
        &mut self,
        chunk: &ChunkObject,
        offset: u64,
        out: &mut [u8],
        selected: &SelectedEntry<'a>,
        prepare_started: Instant,
        finalize_started: &mut Option<Instant>,
    ) -> Result<()> {
        let key = selected.key.as_str();
        let path = self.cache.path(key);
        let prepared = (|| {
            let (mut file, before) = open_private(&path)?;
            if (before.dev(), before.ino()) != selected.expected {
                return Err(invalid("Peer cache ready file was replaced"));
            }
            let (header, body_start) = read_header(&mut file)?;
            if header.version != VERSION
                || header.namespace_id != self.cache.options.namespace_id
                || header.chunk != *chunk
            {
                return Err(corrupt("Peer cache header identity differs"));
            }
            validate_file_length(&header, &before)?;
            let identity = file_identity(&before);
            let reusable = self.state.entries.get(key).and_then(|entry| {
                entry
                    .proof
                    .as_ref()
                    .filter(|proof| proof.identity == identity)
                    .map(|proof| proof.outboard.clone())
            });
            Ok((file, before, body_start, header, reusable))
        })();
        self.state.stats.read_stages.prepare_ns = self
            .state
            .stats
            .read_stages
            .prepare_ns
            .saturating_add(elapsed_ns(prepare_started));
        let (mut file, before, body_start, header, reusable) = prepared?;
        let fresh = if let Some(outboard) = reusable {
            match scan_outboard(
                &file,
                body_start,
                chunk,
                offset,
                out,
                &outboard,
                &mut self.state.stats,
            ) {
                Ok(()) => None,
                Err(error) if error.code() == afs_error::IO_CAPACITY => {
                    self.state.budget.record_range_fullscan_fallback();
                    // Optional proof work must not create a new read failure
                    // when another live proof temporarily holds the ledger.
                    let started = Instant::now();
                    let result = scan_body(
                        &mut file,
                        body_start,
                        chunk,
                        offset,
                        out,
                        &mut self.state.stats,
                        None,
                    );
                    self.state.stats.read_stages.stream_read_verify_ns = self
                        .state
                        .stats
                        .read_stages
                        .stream_read_verify_ns
                        .saturating_add(elapsed_ns(started));
                    result?
                }
                Err(error) => return Err(error),
            }
        } else {
            // Metadata changes invalidate old summaries before a complete root scan.
            let started = Instant::now();
            if let Some(entry) = self.state.entries.get_mut(key) {
                entry.proof = None;
            }
            refresh_metadata(&mut self.state);
            let index = proof_can_fit(&self.state);
            let peer = if index {
                read_disk_outboard(&mut file, &header, &self.state.budget)?
            } else {
                None
            };
            // Without the retained complete FileIdentity we cannot claim
            // unchanged bytes merely because a trailer is well shaped.
            self.state.budget.record_range_fullscan_unretained();
            let result = scan_body(
                &mut file,
                body_start,
                chunk,
                offset,
                out,
                &mut self.state.stats,
                index.then(|| (self.cache.proof_budget(), peer)),
            );
            self.state.stats.read_stages.stream_read_verify_ns = self
                .state
                .stats
                .read_stages
                .stream_read_verify_ns
                .saturating_add(elapsed_ns(started));
            result?
        };
        *finalize_started = Some(Instant::now());
        let after = file.metadata().map_err(Error::from)?;
        let current = fs::symlink_metadata(&path).map_err(Error::from)?;
        if file_identity(&before) != file_identity(&after)
            || file_identity(&before) != file_identity(&current)
        {
            return Err(corrupt("Peer cache file changed during verified read"));
        }
        if fresh.is_some() {
            install_proof(&mut self.state, key, file_identity(&after), fresh);
        }
        Ok(())
    }

    /// Count the hit or retire a corrupt entry, then publish the verified
    /// proof back to the shared index while the pin's generation is current.
    fn settle_range_read(
        &mut self,
        result: Result<()>,
        selected: SelectedEntry<'a>,
        finalize_started: Instant,
        object_diagnostic: Option<crate::node::metrics::read_timing::ReadObjectDiagnosticSpan>,
    ) -> Result<bool> {
        let outcome = (|| match result {
            Ok(()) => {
                if let Some(entry) = self.state.entries.get_mut(&selected.key) {
                    entry.touched = self.cache.bucket();
                }
                self.state.stats.hits = self.state.stats.hits.saturating_add(1);
                Ok(true)
            }
            Err(error) if crate::node::fault::classify::cached_copy_is_invalid(&error) => {
                self.state.stats.corruptions = self.state.stats.corruptions.saturating_add(1);
                self.state.stats.misses = self.state.stats.misses.saturating_add(1);
                self.cache
                    .retire(&selected.key, selected.pin.generation.as_ref(), false)?;
                Ok(false)
            }
            Err(error) => Err(error),
        })();
        self.state.stats.read_stages.finalize_ns = self
            .state
            .stats
            .read_stages
            .finalize_ns
            .saturating_add(elapsed_ns(finalize_started));
        let local_entry = self.state.entries.remove(&selected.key);
        let old_proof = if outcome.as_ref().is_ok_and(|hit| *hit) {
            let mut state = self.cache.lock_state()?;
            if let Some(entry) = state.entries.get_mut(&selected.key)
                && selected
                    .pin
                    .generation
                    .as_ref()
                    .is_some_and(|generation| Arc::ptr_eq(generation, &entry.generation))
            {
                entry.touched = self.cache.bucket();
                let previous = entry.proof.take();
                entry.proof = local_entry.as_ref().and_then(|entry| entry.proof.clone());
                refresh_metadata(&mut state);
                previous
            } else {
                None
            }
        } else {
            None
        };
        drop(old_proof);
        drop(local_entry);
        drop(selected.pin);
        if let Some(diagnostic) = object_diagnostic {
            diagnostic.finish(outcome.as_ref().is_ok_and(|hit| *hit));
        }
        outcome
    }

    fn read_range_root_fallback(
        &mut self,
        chunk: &ChunkObject,
        offset: u64,
        out: &mut [u8],
    ) -> Result<bool> {
        self.cache.check_root()?;
        validate_chunk(chunk)?;
        if offset
            .checked_add(out.len() as u64)
            .is_none_or(|end| end > chunk.length)
        {
            return Err(invalid("Peer cache range exceeds immutable object"));
        }
        let key = self.cache.key(chunk)?;
        let selected = {
            let state = self.cache.lock_state()?;
            state
                .entries
                .get(&key)
                .map(|entry| (entry.identity, entry.generation.clone()))
        };
        let Some((expected, generation)) = selected else {
            self.state.stats.misses += 1;
            return Ok(false);
        };
        let pin = EntryPin {
            cache: self.cache,
            generation: Some(generation),
        };
        let object_diagnostic = self.cache.read_work.as_ref().and_then(|metrics| {
            metrics.object_diagnostic(
                crate::node::metrics::read_timing::ReadObjectDiagnosticStage::CacheHitRootFallback,
                &chunk.id.0,
            )
        });
        let result = (|| {
            let (mut file, before) = open_private(&self.cache.path(&key))?;
            if (before.dev(), before.ino()) != expected {
                return Err(invalid("Peer cache ready file was replaced"));
            }
            let (header, body_start) = read_header(&mut file)?;
            if header.version != VERSION
                || header.namespace_id != self.cache.options.namespace_id
                || header.chunk != *chunk
            {
                return Err(corrupt("Peer cache header identity differs"));
            }
            validate_file_length(&header, &before)?;
            self.cache.proof_budget.record_range_fullscan_fallback();
            let started = Instant::now();
            let scanned = scan_body(
                &mut file,
                body_start,
                chunk,
                offset,
                out,
                &mut self.state.stats,
                None,
            );
            self.state.stats.read_stages.stream_read_verify_ns += elapsed_ns(started);
            scanned?;
            let after = file.metadata().map_err(Error::from)?;
            let current = fs::symlink_metadata(self.cache.path(&key)).map_err(Error::from)?;
            if file_identity(&before) != file_identity(&after)
                || file_identity(&after) != file_identity(&current)
            {
                return Err(corrupt("Peer cache file changed during verified read"));
            }
            Ok(())
        })();
        let outcome = match result {
            Ok(()) => {
                self.state.stats.hits += 1;
                Ok(true)
            }
            Err(error) if crate::node::fault::classify::cached_copy_is_invalid(&error) => {
                self.state.stats.corruptions += 1;
                self.state.stats.misses += 1;
                self.cache.retire(&key, pin.generation.as_ref(), false)?;
                Ok(false)
            }
            Err(error) => Err(error),
        };
        if let Some(diagnostic) = object_diagnostic {
            diagnostic.finish(outcome.as_ref().is_ok_and(|hit| *hit));
        }
        outcome
    }

    /// Only borrowed typed proof is admitted; this method does not hash again.
    pub fn store_verified(&mut self, verified: &VerifiedPeerChunk<'_>) -> Result<bool> {
        if verified.chunk.length > MAX_OBJECT_BYTES {
            return Ok(false);
        }
        self.acquire_fill(|| Ok(()))?;
        let mut sink = self.begin_stream(verified.chunk)?;
        if let Some(error) = sink.prepare_error.take() {
            return Err(error);
        }
        sink.write(0, verified.bytes)?;
        let Some(mut file) = sink.file.take() else {
            return Ok(false);
        };
        let outboard = verified
            .outboard
            .as_ref()
            .filter(|proof| proof.uses_budget(&sink.cache.proof_budget));
        let trailer = outboard.map_or(&[][..], |proof| proof.encoded());
        if let Some(proof) = outboard {
            validate_outboard(verified.chunk, proof)?;
        }
        let result = (|| {
            file.write_all(trailer).map_err(Error::from)?;
            file.seek(SeekFrom::Start(0)).map_err(Error::from)?;
            write_header(
                &mut file,
                &Header {
                    version: VERSION,
                    namespace_id: sink.cache.options.namespace_id.clone(),
                    chunk: verified.chunk.clone(),
                    outboard_length: trailer.len() as u32,
                },
            )?;
            file.flush().map_err(Error::from)?;
            sink.cache.check_root()?;
            let before = file.metadata().map_err(Error::from)?;
            validate_private_file(&before)?;
            let current = fs::symlink_metadata(sink.cache.pending_path(sink.pending_generation))
                .map_err(Error::from)?;
            if sink.pending_identity != Some((before.dev(), before.ino()))
                || file_identity(&before) != file_identity(&current)
            {
                return Err(corrupt("Peer cache staging identity changed"));
            }
            let charge = allocated(&before)?;
            if sink
                .cache
                .account_pending_growth(sink.pending_generation, charge)?
            {
                return Err(unavailable("Peer cache allocation exceeds reserved blocks"));
            }
            match fs::symlink_metadata(sink.cache.path(&sink.key)) {
                Ok(_) => return Err(invalid("Peer cache publication refuses an untracked file")),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(Error::from(error)),
            }
            fs::rename(
                sink.cache.pending_path(sink.pending_generation),
                sink.cache.path(&sink.key),
            )
            .map_err(Error::from)?;
            sink.committed = true;
            sink.owns_pending = false;
            let published = file
                .metadata()
                .ok()
                .map(|metadata| file_identity(&metadata));
            sink.cache.publish_entry(
                (sink.pending_generation, sink.key.clone()),
                verified.chunk.clone(),
                charge,
                published.unwrap_or_else(|| file_identity(&before)),
                if published.is_some() {
                    outboard.cloned()
                } else {
                    None
                },
                sink.entry_lease.take(),
            )?;
            Ok(true)
        })();
        drop(file);
        // Sink Drop performs identity-bound cleanup; keep I/O errors visible to
        // the caller, which retains the original verified private output.
        result
    }
}

pub(super) fn scan_body(
    file: &mut File,
    body_start: u64,
    chunk: &ChunkObject,
    offset: u64,
    out: &mut [u8],
    stats: &mut PeerCacheStats,
    proof: Option<(Arc<OutboardBudget>, Option<SharedOutboard>)>,
) -> Result<Option<SharedOutboard>> {
    file.seek(SeekFrom::Start(body_start))
        .map_err(Error::from)?;
    let mut scratch = [0_u8; SCRATCH_BYTES];
    stats.scratch_peak_bytes = stats.scratch_peak_bytes.max(SCRATCH_BYTES as u64);
    let mut builder = proof.map(|(budget, peer)| match peer {
        Some(peer) => OutboardBuilder::validating(chunk.clone(), peer, budget),
        None => OutboardBuilder::new(chunk.length, budget),
    });
    let mut hash = blake3::Hasher::new();
    let mut position = 0_u64;
    while position < chunk.length {
        let requested = (chunk.length - position).min(SCRATCH_BYTES as u64) as usize;
        stats.data_read_calls = stats.data_read_calls.saturating_add(1);
        let read = file.read(&mut scratch[..requested]).map_err(Error::from)?;
        if read == 0 {
            return Err(corrupt("Peer cache body is truncated"));
        }
        stats.hash_bytes_total = stats.hash_bytes_total.saturating_add(read as u64);
        stats.hash_work.cache_fullscan_bytes = stats
            .hash_work
            .cache_fullscan_bytes
            .saturating_add(read as u64);
        if let Some(builder) = &mut builder {
            builder.update(&scratch[..read]).map_err(|error| {
                if error.code() == afs_error::IO_CAPACITY {
                    error
                } else {
                    corrupt("Peer cache outboard differs from complete body")
                }
            })?;
        } else {
            hash.update(&scratch[..read]);
        }
        stats.copied_bytes_total = stats.copied_bytes_total.saturating_add(copy_overlap(
            position,
            &scratch[..read],
            offset,
            out,
        ));
        position += read as u64;
    }
    if let Some(builder) = builder {
        let built = builder.finish().map_err(|error| {
            if error.code() == afs_error::IO_CAPACITY {
                error
            } else {
                corrupt("Peer cache outboard or root differs from complete body")
            }
        })?;
        if built.digest != chunk.content_digest {
            return Err(corrupt("Peer cache complete body digest differs"));
        }
        stats.hash_work.outboard_parent_bytes = stats
            .hash_work
            .outboard_parent_bytes
            .saturating_add(built.parent_hash_bytes);
        Ok(built.outboard)
    } else if hash.finalize().as_bytes() != &chunk.content_digest.bytes {
        Err(corrupt("Peer cache complete body digest differs"))
    } else {
        Ok(None)
    }
}

pub(super) fn scan_outboard(
    file: &File,
    body_start: u64,
    chunk: &ChunkObject,
    offset: u64,
    out: &mut [u8],
    proof: &SharedOutboard,
    stats: &mut PeerCacheStats,
) -> Result<()> {
    validate_outboard(chunk, proof)?;
    let mut work = OutboardReadWork::default();
    let started = Instant::now();
    let result = proof.read_ranges_into_with_work(
        file,
        body_start,
        chunk,
        &[VerifiedRangeCopy {
            chunk_offset: offset,
            output_offset: 0,
            length: out.len(),
        }],
        out,
        &mut work,
    );
    stats.hash_bytes_total = stats.hash_bytes_total.saturating_add(work.hash_bytes);
    stats.hash_work.cache_segment_bytes = stats
        .hash_work
        .cache_segment_bytes
        .saturating_add(work.hash_bytes);
    stats.hash_work.outboard_parent_bytes = stats
        .hash_work
        .outboard_parent_bytes
        .saturating_add(work.parent_hash_bytes);
    stats.data_read_calls = stats.data_read_calls.saturating_add(work.read_calls);
    stats.copied_bytes_total = stats.copied_bytes_total.saturating_add(work.copied_bytes);
    stats.scratch_peak_bytes = stats.scratch_peak_bytes.max(work.scratch_peak as u64);
    stats.read_stages.data_read_ns = stats.read_stages.data_read_ns.saturating_add(work.read_ns);
    stats.read_stages.segment_verify_ns = stats
        .read_stages
        .segment_verify_ns
        .saturating_add(work.verify_ns);
    // End-to-end overlap is measured separately; this is one local operation.
    stats.read_stages.stream_read_verify_ns = stats
        .read_stages
        .stream_read_verify_ns
        .saturating_add(elapsed_ns(started));
    result.map(|_| ()).map_err(|error| {
        if error.code() == afs_error::IO_INVALID {
            corrupt("Peer cache coverage proof differs from trusted object")
        } else {
            error
        }
    })
}

fn copy_overlap(position: u64, bytes: &[u8], offset: u64, out: &mut [u8]) -> u64 {
    let start = offset.max(position);
    let stop = (offset + out.len() as u64).min(position + bytes.len() as u64);
    if stop > start {
        out[(start - offset) as usize..(stop - offset) as usize]
            .copy_from_slice(&bytes[(start - position) as usize..(stop - position) as usize]);
        stop - start
    } else {
        0
    }
}

pub(super) fn entry_metadata_charge(key: &str, chunk: &ChunkObject) -> usize {
    // Chunk IDs are bounded to 512 bytes. Two entry slots per live item cover
    // B-tree node occupancy; the fixed index reservation also covers sparse
    // roots in the indexed/retired maps. The charge survives generation pins.
    2 * (std::mem::size_of::<Entry>() + key.len() + chunk.id.0.capacity() + 256)
}

pub(super) fn refresh_metadata(state: &mut CacheState) {
    let pointers = (state.entries.len() + state.retired.len())
        * std::mem::size_of::<Option<Box<VerifiedDiskOutboard>>>();
    let proofs: usize = state
        .entries
        .values()
        .chain(state.retired.values().map(|retired| &retired.entry))
        .filter_map(|entry| entry.proof.as_ref())
        .map(|proof| proof.outboard.charged_bytes() + proof_allocation_charge() as usize)
        .sum();
    // Resident cache ownership only; shared budget.used() also includes local
    // readers and in-flight Arc holders after an entry has been evicted.
    state.stats.proof_metadata_bytes = (pointers + proofs) as u64;
    state.stats.proof_metadata_peak_bytes = state
        .stats
        .proof_metadata_peak_bytes
        .max(state.stats.proof_metadata_bytes);
}

pub(super) fn proof_can_fit(state: &CacheState) -> bool {
    #[cfg(test)]
    if state.test_hooks.proof_limit.is_some_and(|limit| {
        state
            .stats
            .proof_metadata_bytes
            .saturating_add(proof_allocation_charge())
            > limit
    }) {
        return false;
    }
    state.index_lease.is_some() || state.local_index_available
}

fn proof_allocation_charge() -> u64 {
    let logical = std::mem::size_of::<VerifiedDiskOutboard>() as u64;
    logical.div_ceil(64) * 64 + 64
}

pub(super) fn install_proof(
    state: &mut CacheState,
    key: &str,
    identity: FileIdentity,
    outboard: Option<SharedOutboard>,
) {
    if let Some(entry) = state.entries.get_mut(key) {
        entry.proof = None;
    }
    refresh_metadata(state);
    if !proof_can_fit(state) {
        return;
    }
    let Some(outboard) = outboard.filter(|proof| proof.uses_budget(&state.budget)) else {
        return;
    };
    loop {
        // The pointer index, identity wrapper and shared proof all use the same
        // resident sublimit. This promotion never charges the proof twice total.
        if let Some(lease) = state
            .budget
            .try_reserve_resident(proof_allocation_charge() as usize)
            && outboard.try_mark_resident()
        {
            if let Some(entry) = state.entries.get_mut(key) {
                entry.proof = Some(Arc::new(VerifiedDiskOutboard {
                    identity,
                    outboard,
                    _lease: lease,
                }));
            }
            break;
        }
        // Retire only old proof ownership. Healthy body entries remain usable;
        // outside Arc holders keep their own resident charge until last drop.
        let victim = state
            .entries
            .values_mut()
            .filter(|entry| entry.proof.is_some())
            .min_by_key(|entry| entry.touched);
        let Some(entry) = victim else {
            break;
        };
        entry.proof = None;
        refresh_metadata(state);
    }
    refresh_metadata(state);
}

pub(super) fn validate_outboard(chunk: &ChunkObject, proof: &ChunkOutboard) -> Result<()> {
    if proof.object_length() != chunk.length || proof.digest() != &chunk.content_digest {
        return Err(corrupt("Peer outboard differs from trusted Meta object"));
    }
    Ok(())
}

pub(super) fn validate_file_length(header: &Header, metadata: &fs::Metadata) -> Result<()> {
    let expected = (MAX_HEADER_BYTES as u64)
        .checked_add(header.chunk.length)
        .and_then(|length| length.checked_add(u64::from(header.outboard_length)))
        .ok_or_else(|| corrupt("Peer cache file length overflow"))?;
    if header.outboard_length as usize > MAX_OUTBOARD_BYTES || metadata.len() != expected {
        return Err(corrupt(
            "Peer cache body or outboard trailer length differs",
        ));
    }
    Ok(())
}

pub(super) fn read_disk_outboard(
    file: &mut File,
    header: &Header,
    budget: &Arc<OutboardBudget>,
) -> Result<Option<SharedOutboard>> {
    let length = header.outboard_length as usize;
    if length == 0 {
        return Ok(None);
    }
    if !(64..=MAX_OUTBOARD_BYTES).contains(&length) {
        return Err(corrupt("Peer cache outboard trailer exceeds bound"));
    }
    let Some(mut lease) = budget.try_reserve(ChunkOutboard::resident_charge(length)?) else {
        return Ok(None);
    };
    let mut encoded = Vec::new();
    if encoded.try_reserve_exact(length).is_err() {
        return Ok(None);
    }
    if lease
        .retain_capacity(ChunkOutboard::resident_charge(encoded.capacity())?)
        .is_err()
    {
        return Ok(None);
    }
    encoded.resize(length, 0);
    file.seek(SeekFrom::Start(
        MAX_HEADER_BYTES as u64 + header.chunk.length,
    ))
    .map_err(Error::from)?;
    file.read_exact(&mut encoded)
        .map_err(|_| corrupt("Peer cache outboard trailer is truncated"))?;
    match ChunkOutboard::from_encoded(&header.chunk, encoded, lease) {
        Ok(proof) => Ok(Some(proof)),
        Err(error) if error.code() == afs_error::IO_CAPACITY => Ok(None),
        Err(error) => Err(error),
    }
}

pub(super) fn write_header(file: &mut File, header: &Header) -> Result<()> {
    let encoded =
        serde_json::to_vec(header).map_err(|_| invalid("Peer cache header cannot be encoded"))?;
    if encoded.len() > MAX_HEADER_BYTES - 12 {
        return Err(invalid("Peer cache header exceeds bound"));
    }
    let mut block = [0_u8; MAX_HEADER_BYTES];
    block[..8].copy_from_slice(MAGIC);
    block[8..12].copy_from_slice(&(encoded.len() as u32).to_le_bytes());
    block[12..12 + encoded.len()].copy_from_slice(&encoded);
    file.write_all(&block).map_err(Error::from)
}

pub(super) fn report_work(
    metrics: &crate::node::metrics::work::ReadWorkMetrics,
    current: PeerCacheHashWork,
    before: PeerCacheHashWork,
) {
    metrics.hash(
        "cache_segment_hash",
        current
            .cache_segment_bytes
            .saturating_sub(before.cache_segment_bytes),
    );
    metrics.hash(
        "cache_full_hash",
        current
            .cache_fullscan_bytes
            .saturating_sub(before.cache_fullscan_bytes),
    );
    metrics.hash(
        "outboard_parent_hash",
        current
            .outboard_parent_bytes
            .saturating_sub(before.outboard_parent_bytes),
    );
    metrics.hash(
        "segment_index_bytes",
        current
            .segment_index_bytes
            .saturating_sub(before.segment_index_bytes),
    );
}

fn elapsed_ns(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

pub(super) fn report_stages(
    metrics: &crate::node::metrics::work::ReadWorkMetrics,
    current: PeerCacheReadStages,
    before: PeerCacheReadStages,
) {
    for (stage, current, before) in [
        ("prepare", current.prepare_ns, before.prepare_ns),
        ("data_read", current.data_read_ns, before.data_read_ns),
        (
            "segment_verify",
            current.segment_verify_ns,
            before.segment_verify_ns,
        ),
        (
            "stream_read_verify",
            current.stream_read_verify_ns,
            before.stream_read_verify_ns,
        ),
        ("finalize", current.finalize_ns, before.finalize_ns),
    ] {
        let delta = current.saturating_sub(before);
        if delta != 0 {
            metrics.peer_cache_stage_ns(stage, delta);
        }
    }
}

impl Drop for PeerCacheOperation<'_> {
    fn drop(&mut self) {
        self.state.entries.clear();
        // Explicitly release the real fill allowance after all sink borrowers
        // have ended and before announcing this operation's drain completion.
        drop(self.fill.take());
        let had_metadata = self._metadata.is_some();
        drop(self._metadata.take());
        if let Ok(mut state) = self.cache.state.lock() {
            add_stats(&mut state.stats, &self.state.stats);
            state.operations = state.operations.saturating_sub(1);
            if had_metadata {
                state.operation_metadata_bytes =
                    state.operation_metadata_bytes.saturating_sub(8192);
            }
        }
        self.cache.changed.notify_all();
        self.cache.asynchronous_change.notify_waiters();
        self.cache.report_resources();
        if let Some(metrics) = &self.cache.read_work {
            report_work(
                metrics,
                self.state.stats.hash_work,
                PeerCacheHashWork::default(),
            );
            metrics.peer_cache_data_reads(self.state.stats.data_read_calls);
            metrics.copy("cache_to_output", self.state.stats.copied_bytes_total);
            report_stages(
                metrics,
                self.state.stats.read_stages,
                PeerCacheReadStages::default(),
            );
        }
    }
}

/// An indexed entry selected for a verified range read, pinned to its
/// generation until the read settles.
struct SelectedEntry<'a> {
    key: String,
    expected: (u64, u64),
    pin: EntryPin<'a>,
}
