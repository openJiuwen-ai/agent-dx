//! DiskPeerCache opening, lookup and retirement.

use super::*;

impl DiskPeerCache {
    pub fn open(options: PeerCacheOptions) -> Result<Self> {
        Self::open_with_outboard_budget(
            options,
            OutboardBudget::new(MAX_PEER_CACHE_PROOF_BYTES as usize),
        )
    }

    pub(crate) fn open_with_outboard_budget(
        options: PeerCacheOptions,
        proof_budget: Arc<OutboardBudget>,
    ) -> Result<Self> {
        if !options.directory.is_absolute()
            || options.capacity_bytes == 0
            || options.namespace_id.0.is_empty()
            || options.namespace_id.0.len() > 512
        {
            return Err(invalid(
                "Peer cache requires an absolute directory and positive budget",
            ));
        }
        reject_symlink_components(&options.directory)?;
        DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(&options.directory)
            .map_err(Error::from)?;
        reject_symlink_components(&options.directory)?;
        let root = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_DIRECTORY | libc::O_CLOEXEC)
            .open(&options.directory)
            .map_err(Error::from)?;
        let metadata = root.metadata().map_err(Error::from)?;
        if !metadata.is_dir() || metadata.mode() & 0o077 != 0 || metadata.uid() != effective_uid() {
            return Err(invalid(
                "Peer cache directory must be private and owned by this user",
            ));
        }
        reject_symlink_components(&options.directory)?;
        let current = fs::symlink_metadata(&options.directory).map_err(Error::from)?;
        if (current.dev(), current.ino()) != (metadata.dev(), metadata.ino()) {
            return Err(invalid("Peer cache directory changed during open"));
        }
        // All child operations resolve through the pinned Linux directory FD,
        // never through a configured pathname that can be replaced later.
        let directory = fd_directory(&root);
        let marker_path = directory.join(MARKER);
        let mut marker = match OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&marker_path)
        {
            Ok(mut file) => {
                if fs::read_dir(&directory).map_err(Error::from)?.any(|entry| {
                    entry
                        .map(|entry| entry.file_name() != MARKER)
                        .unwrap_or(true)
                }) {
                    drop(file);
                    fs::remove_file(marker_path).map_err(Error::from)?;
                    return Err(invalid(
                        "Peer cache refuses an unmarked non-empty directory",
                    ));
                }
                file.write_all(crate::format::NODE_PEER_CACHE_MARKER_CONTENTS.as_bytes())
                    .map_err(Error::from)?;
                file.sync_all().map_err(Error::from)?;
                file
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => OpenOptions::new()
                .read(true)
                .write(true)
                .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
                .open(marker_path)
                .map_err(Error::from)?,
            Err(error) => return Err(Error::from(error)),
        };
        validate_private_file(&marker.metadata().map_err(Error::from)?)?;
        // SAFETY: the descriptor belongs to marker; flock does not own it.
        if unsafe { libc::flock(marker.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            return Err(unavailable("Peer cache directory is already in use"));
        }
        marker.seek(SeekFrom::Start(0)).map_err(Error::from)?;
        let mut contents = String::new();
        Read::by_ref(&mut marker)
            .take(128)
            .read_to_string(&mut contents)
            .map_err(Error::from)?;
        // Version first: a marker of another version is a format mismatch.
        // The directory is disposable but never deleted automatically.
        if contents != crate::format::NODE_PEER_CACHE_MARKER_CONTENTS
            && let Some(found) = contents
                .strip_prefix(crate::format::NODE_PEER_CACHE_MARKER_PREFIX)
                .and_then(|rest| rest.strip_suffix('\n'))
                .and_then(|digits| digits.parse::<u64>().ok())
        {
            crate::format::check(
                crate::format::FormatKind::NodePeerCacheMarker,
                options.directory.join(MARKER).display(),
                found,
            )?;
        }
        if contents != crate::format::NODE_PEER_CACHE_MARKER_CONTENTS {
            return Err(invalid("Peer cache marker is not recognized"));
        }
        let cache = Self {
            options,
            root,
            _marker: marker,
            started: Instant::now(),
            state: Mutex::new(CacheState::new(proof_budget.clone())),
            changed: Condvar::new(),
            asynchronous_change: tokio::sync::Notify::new(),
            max_flights: 8,
            proof_budget,
            read_work: None,
        };
        cache.recover()?;
        Ok(cache)
    }

    pub(crate) fn max_inflight_objects(&self) -> usize {
        self.max_flights
    }

    pub(crate) fn try_contains_ready(&self, chunk: &ChunkObject) -> Result<Option<bool>> {
        let key = self.key(chunk)?;
        Ok(self
            .try_lock_state()?
            .map(|state| state.entries.contains_key(&key)))
    }

    pub(crate) fn proof_budget(&self) -> Arc<OutboardBudget> {
        self.proof_budget.clone()
    }

    pub fn namespace_id(&self) -> &NamespaceId {
        &self.options.namespace_id
    }

    pub fn with_read_work_metrics(
        mut self,
        metrics: crate::node::read_work::ReadWorkMetrics,
    ) -> Self {
        if let Ok(state) = self.state.lock() {
            report_work(
                &metrics,
                state.stats.hash_work,
                PeerCacheHashWork::default(),
            );
            metrics.peer_cache_data_reads(state.stats.data_read_calls);
            metrics.copy("cache_to_output", state.stats.copied_bytes_total);
            report_stages(
                &metrics,
                state.stats.read_stages,
                PeerCacheReadStages::default(),
            );
            metrics.peer_cache_metadata(
                state.stats.proof_metadata_bytes,
                state.stats.proof_metadata_peak_bytes,
            );
        }
        self.read_work = Some(metrics);
        self
    }

    pub(crate) fn with_max_inflight_objects(mut self, maximum: usize) -> Result<Self> {
        if !(1..=8).contains(&maximum) {
            return Err(invalid(
                "Peer cache metadata flights must be between one and eight",
            ));
        }
        self.max_flights = maximum;
        Ok(self)
    }

    pub fn operation(&self) -> Result<PeerCacheOperation<'_>> {
        self.operation_mode(false)?
            .ok_or_else(|| unavailable("Peer cache operation unavailable"))
    }

    pub(crate) fn try_operation(&self) -> Result<Option<PeerCacheOperation<'_>>> {
        self.operation_mode(true)
    }

    fn operation_mode(&self, background: bool) -> Result<Option<PeerCacheOperation<'_>>> {
        self.check_root()?;
        let wait = self
            .read_work
            .as_ref()
            .and_then(|metrics| metrics.coarse_diagnostic("cache_lock_wait", 0));
        let mut state = if background {
            let Some(state) = self.try_lock_state()? else {
                return Ok(None);
            };
            state
        } else {
            self.lock_state()?
        };
        drop(wait);
        // Bound operation-local entry/proof snapshots independently of workers.
        if state.closed || state.operations >= 128 {
            if background {
                return Ok(None);
            }
            return Err(unavailable(
                "Peer cache is stopped or operation slots are full",
            ));
        }
        let metadata = self.proof_budget.try_reserve(8192);
        if background && (metadata.is_none() || state.foreground_waiters != 0) {
            return Ok(None);
        }
        state.operations += 1;
        if metadata.is_some() {
            state.operation_metadata_bytes += 8192;
        }
        let mut local = CacheState::new_local(self.proof_budget());
        local.local_index_available = state.index_lease.is_some();
        #[cfg(test)]
        {
            local.proof_limit = state.proof_limit;
        }
        drop(state);
        if !background {
            self.report_resources();
        }
        Ok(Some(PeerCacheOperation {
            cache: self,
            state: local,
            fill: None,
            _metadata: metadata,
        }))
    }

    pub(crate) fn close_admission(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.closed = true;
            self.changed.notify_all();
            self.asynchronous_change.notify_waiters();
            for flight in state.flights.values() {
                flight.changed.notify_all();
                flight.asynchronous_change.notify_waiters();
            }
        }
    }

    pub fn shutdown(&self) -> Result<()> {
        self.close_admission();
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut state = self
            .state
            .lock()
            .map_err(|_| unavailable("Peer cache lock poisoned"))?;
        while state.operations != 0
            || state.fill_active
            || state.stream_active != 0
            || state.flight_live != 0
            || state.deleting_slots != 0
            || state.retired.values().any(|retired| retired.deleting)
        {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(unavailable("Peer cache drain deadline exceeded"));
            }
            state = self
                .changed
                .wait_timeout(state, remaining)
                .map_err(|_| unavailable("Peer cache drain lock poisoned"))?
                .0;
        }
        // Preserve healthy disk entries for startup recovery. Charges and proof
        // owners leave the lock before final destruction.
        let entries = std::mem::take(&mut state.entries);
        let released_entry_metadata: usize = entries
            .iter()
            .map(|(key, entry)| entry_metadata_charge(key, &entry.chunk))
            .sum();
        state.entry_metadata_bytes = state
            .entry_metadata_bytes
            .saturating_sub(released_entry_metadata);
        refresh_metadata(&mut state);
        drop(state);
        drop(entries);
        self.cleanup_retired()?;
        let index = self.lock_state()?.index_lease.take();
        drop(index);
        self.report_resources();
        Ok(())
    }

    pub fn stats(&self) -> Result<PeerCacheStats> {
        let state = self
            .state
            .lock()
            .map_err(|_| unavailable("Peer cache lock poisoned"))?;
        let mut stats = state.stats.clone();
        stats.disk_bytes = state.used_bytes;
        stats.entries = state.entries.len();
        Ok(stats)
    }

    pub(super) fn check_root(&self) -> Result<()> {
        reject_symlink_components(&self.options.directory)?;
        let current = fs::symlink_metadata(&self.options.directory).map_err(Error::from)?;
        let pinned = self.root.metadata().map_err(Error::from)?;
        if !current.is_dir()
            || current.mode() & 0o077 != 0
            || current.uid() != effective_uid()
            || pinned.mode() & 0o077 != 0
            || pinned.uid() != effective_uid()
            || (current.dev(), current.ino()) != (pinned.dev(), pinned.ino())
        {
            return Err(invalid("Peer cache root identity changed"));
        }
        Ok(())
    }

    pub(super) fn key(&self, chunk: &ChunkObject) -> Result<String> {
        let input = serde_json::to_vec(&(&self.options.namespace_id, chunk))
            .map_err(|_| invalid("Peer cache object identity cannot be encoded"))?;
        Ok(blake3::hash(&input).to_hex().to_string())
    }

    pub(super) fn directory(&self) -> PathBuf {
        fd_directory(&self.root)
    }

    pub(super) fn bucket(&self) -> u64 {
        self.started.elapsed().as_secs()
    }

    pub(super) fn pending_path(&self, generation: u64) -> PathBuf {
        self.directory()
            .join(format!("{PENDING}-{generation:016x}"))
    }

    pub(super) fn path(&self, key: &str) -> PathBuf {
        self.directory().join(format!("{key}.ready"))
    }

    fn recover(&self) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| unavailable("Peer cache lock poisoned"))?;
        // Preflight bounded names and allocated space before any private cleanup.
        let mut ready = Vec::new();
        let mut pending = Vec::new();
        let mut total = 0_u64;
        for entry in fs::read_dir(self.directory()).map_err(Error::from)? {
            let entry = entry.map_err(Error::from)?;
            let name = entry.file_name();
            if name == MARKER {
                continue;
            }
            let name = name
                .to_str()
                .ok_or_else(|| invalid("Peer cache contains a foreign name"))?;
            let metadata = fs::symlink_metadata(entry.path()).map_err(Error::from)?;
            validate_private_file(&metadata)?;
            let charge = allocated(&metadata)?;
            total = total
                .checked_add(charge)
                .ok_or_else(|| invalid("Peer cache size overflow"))?;
            if name == PENDING || valid_pending_name(name) {
                pending.push((entry.path(), charge));
            } else if valid_ready_name(name) {
                ready.push((name.trim_end_matches(".ready").to_owned(), metadata));
            } else {
                return Err(invalid(
                    "Peer cache contains foreign files; preserved without cleanup",
                ));
            }
            if ready.len() + pending.len() > MAX_PEER_CACHE_ENTRIES
                || total > self.options.capacity_bytes
            {
                return Err(invalid("Peer cache recovery exceeds disk or index budget"));
            }
        }
        state.used_bytes = total;
        state.stats.disk_peak_bytes = state.stats.disk_peak_bytes.max(total);
        for (path, charge) in pending {
            // Startup is exclusive; every accepted private file was preflighted.
            fs::remove_file(path).map_err(Error::from)?;
            state.used_bytes = state.used_bytes.saturating_sub(charge);
        }
        for (key, metadata) in ready {
            let (mut file, _) = open_private(&self.path(&key))?;
            // Derived data (format rule 3): an entry of another version is
            // recognised by its magic and discarded before its header is
            // parsed; the authoritative LocalChunkStore is elsewhere.
            if other_entry_version(&mut file)?.is_some() {
                drop(file);
                fs::remove_file(self.path(&key)).map_err(Error::from)?;
                state.used_bytes = state.used_bytes.saturating_sub(allocated(&metadata)?);
                continue;
            }
            let (header, body_start) = read_header(&mut file)?;
            validate_chunk(&header.chunk)?;
            if header.version != VERSION
                || header.namespace_id != self.options.namespace_id
                || self.key(&header.chunk)? != key
                || header.chunk.length > MAX_OBJECT_BYTES
            {
                return Err(invalid("Peer cache ready identity is foreign; preserved"));
            }
            // Recovery is single-threaded before this cache is published.
            // Make room by retiring optional summaries, never healthy bodies.
            let metadata_lease = loop {
                if let Some(lease) = self
                    .proof_budget
                    .try_reserve_resident(entry_metadata_charge(&key, &header.chunk))
                {
                    break lease;
                }
                let Some(entry) = state
                    .entries
                    .values_mut()
                    .filter(|entry| entry.proof.is_some())
                    .min_by_key(|entry| entry.touched)
                else {
                    return Err(unavailable(
                        "Peer cache recovery metadata capacity exceeded",
                    ));
                };
                entry.proof = None;
                refresh_metadata(&mut state);
            };
            state.entries.insert(
                key.clone(),
                Entry {
                    chunk: header.chunk.clone(),
                    charge: allocated(&metadata)?,
                    identity: (metadata.dev(), metadata.ino()),
                    touched: 0,
                    proof: None,
                    generation: Arc::new(()),
                    _metadata: Some(metadata_lease),
                    #[cfg(test)]
                    drop_observation: None,
                },
            );
            state.entry_metadata_bytes += entry_metadata_charge(&key, &header.chunk);
            refresh_metadata(&mut state);
            let before = file.metadata().map_err(Error::from)?;
            let result = (|| {
                validate_file_length(&header, &before)?;
                let index = proof_can_fit(&state);
                let peer = if index {
                    read_disk_outboard(&mut file, &header, &state.budget)?
                } else {
                    None
                };
                scan_body(
                    &mut file,
                    body_start,
                    &header.chunk,
                    0,
                    &mut [],
                    &mut state.stats,
                    index.then(|| (self.proof_budget(), peer)),
                )
            })();
            match result {
                Ok(outboard) => {
                    let after = file.metadata().map_err(Error::from)?;
                    let current = fs::symlink_metadata(self.path(&key)).map_err(Error::from)?;
                    if file_identity(&before) != file_identity(&after)
                        || file_identity(&after) != file_identity(&current)
                    {
                        return Err(corrupt("Peer cache file changed during recovery"));
                    }
                    install_proof(&mut state, &key, file_identity(&after), outboard);
                }
                Err(_) => {
                    state.stats.corruptions = state.stats.corruptions.saturating_add(1);
                    drop(file);
                    self.bootstrap_remove_entry(&mut state, &key, false)?;
                }
            }
        }
        Ok(())
    }

    fn bootstrap_remove_entry(
        &self,
        state: &mut CacheState,
        key: &str,
        eviction: bool,
    ) -> Result<()> {
        self.check_root()?;
        let Some(entry) = state.entries.get(key) else {
            return Ok(());
        };
        #[cfg(test)]
        if state.fail_remove {
            return Err(unavailable("injected Peer cache cleanup failure"));
        }
        match fs::symlink_metadata(self.path(key)) {
            Ok(metadata) => {
                validate_private_file(&metadata)?;
                if (metadata.dev(), metadata.ino()) != entry.identity {
                    return Err(invalid("Peer cache cleanup refuses a replaced file"));
                }
                fs::remove_file(self.path(key)).map_err(Error::from)?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(Error::from(error)),
        }
        let charge = entry.charge;
        let metadata_charge = entry_metadata_charge(key, &entry.chunk);
        state.entry_metadata_bytes = state.entry_metadata_bytes.saturating_sub(metadata_charge);
        state.entries.remove(key);
        refresh_metadata(state);
        state.used_bytes = state.used_bytes.saturating_sub(charge);
        if eviction {
            state.stats.evictions = state.stats.evictions.saturating_add(1);
        }
        Ok(())
    }
}

pub(super) fn add_stats(total: &mut PeerCacheStats, local: &PeerCacheStats) {
    macro_rules! add { ($($field:ident),* $(,)?) => { $(total.$field = total.$field.saturating_add(local.$field);)* }; }
    add!(
        hits,
        misses,
        corruptions,
        hash_bytes_total,
        data_read_calls,
        copied_bytes_total
    );
    total.scratch_peak_bytes = total.scratch_peak_bytes.max(local.scratch_peak_bytes);
    total.hash_work.cache_segment_bytes += local.hash_work.cache_segment_bytes;
    total.hash_work.cache_fullscan_bytes += local.hash_work.cache_fullscan_bytes;
    total.hash_work.segment_index_bytes += local.hash_work.segment_index_bytes;
    total.hash_work.outboard_parent_bytes += local.hash_work.outboard_parent_bytes;
    total.read_stages.prepare_ns += local.read_stages.prepare_ns;
    total.read_stages.data_read_ns += local.read_stages.data_read_ns;
    total.read_stages.segment_verify_ns += local.read_stages.segment_verify_ns;
    total.read_stages.stream_read_verify_ns += local.read_stages.stream_read_verify_ns;
    total.read_stages.finalize_ns += local.read_stages.finalize_ns;
}

impl Drop for DiskPeerCache {
    fn drop(&mut self) {
        if let Some(metrics) = &self.read_work
            && let Ok(state) = self.state.get_mut()
        {
            metrics.peer_cache_metadata(0, state.stats.proof_metadata_peak_bytes);
        }
    }
}
