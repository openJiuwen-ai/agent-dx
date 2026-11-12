//! Single-flight cache fill and its RAII guards.

use super::*;

impl DiskPeerCache {
    pub(super) fn report_resources(&self) {
        let Some(metrics) = &self.read_work else {
            return;
        };
        let Ok(state) = self.state.lock() else {
            return;
        };
        let index = if state.index_lease.is_some() {
            MAX_PEER_CACHE_ENTRIES * std::mem::size_of::<Option<Arc<VerifiedDiskOutboard>>>()
        } else {
            0
        };
        // Fixed pre-registered in-memory gauges publish under the short metadata
        // lock: no stale snapshot can overwrite a newer resource observation.
        metrics.peer_cache_metadata(
            state.stats.proof_metadata_bytes,
            state.stats.proof_metadata_peak_bytes,
        );
        metrics.peer_cache_resources(&[
            ("operations", state.operations as u64),
            ("flights", state.flight_live as u64),
            (
                "fill_body_bytes",
                if state.fill_active || state.stream_active != 0 {
                    MAX_OBJECT_BYTES
                } else {
                    0
                },
            ),
            ("indexed_entries", state.entries.len() as u64),
            (
                "retired_entries",
                (state.retired.len() + state.deleting_slots) as u64,
            ),
            ("retired_disk_bytes", state.retired_disk_bytes),
            (
                "control_bytes",
                (state.entry_metadata_bytes
                    + state.flight_metadata_bytes
                    + state.operation_metadata_bytes
                    + state.pending_metadata_charge
                    + index) as u64,
            ),
        ]);
    }

    #[cfg(test)]
    pub(crate) fn has_fill_flight_for_test(&self) -> bool {
        !self.state.lock().unwrap().flights.is_empty()
    }

    #[cfg(test)]
    pub(crate) fn waiting_fill_readers_for_test(&self) -> usize {
        self.state
            .lock()
            .unwrap()
            .flights
            .values()
            .map(|flight| Arc::strong_count(flight).saturating_sub(2))
            .sum()
    }

    pub(super) fn lock_state(&self) -> Result<std::sync::MutexGuard<'_, CacheState>> {
        self.state
            .lock()
            .map_err(|_| unavailable("Peer cache lock poisoned"))
    }

    pub(super) fn try_lock_state(&self) -> Result<Option<std::sync::MutexGuard<'_, CacheState>>> {
        match self.state.try_lock() {
            Ok(state) => Ok(Some(state)),
            Err(std::sync::TryLockError::WouldBlock) => Ok(None),
            Err(std::sync::TryLockError::Poisoned(_)) => {
                Err(unavailable("Peer cache lock poisoned"))
            }
        }
    }

    pub(super) fn cleanup_retired(&self) -> Result<()> {
        loop {
            let ticket = {
                let mut state = self.lock_state()?;
                let Some((key, retired)) = state.retired.iter_mut().find(|(_, retired)| {
                    !retired.deleting && Arc::strong_count(&retired.entry.generation) == 1
                }) else {
                    return Ok(());
                };
                retired.deleting = true;
                (
                    key.clone(),
                    retired.entry.generation.clone(),
                    retired.entry.identity,
                )
            };
            let (key, generation, identity) = ticket;
            let result = (|| {
                self.check_root()?;
                #[cfg(test)]
                if self.lock_state()?.test_hooks.fail_remove {
                    return Err(unavailable("injected Peer cache cleanup failure"));
                }
                match fs::symlink_metadata(self.path(&key)) {
                    Ok(metadata) => {
                        validate_private_file(&metadata)?;
                        if (metadata.dev(), metadata.ino()) != identity {
                            return Err(invalid("Peer cache cleanup refuses a replaced file"));
                        }
                        fs::remove_file(self.path(&key)).map_err(Error::from)
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
                    Err(error) => Err(Error::from(error)),
                }
            })();
            let removed = {
                let mut state = self.lock_state()?;
                if let Some(retired) = state.retired.get_mut(&key)
                    && Arc::ptr_eq(&generation, &retired.entry.generation)
                {
                    retired.deleting = false;
                    if result.is_ok() {
                        let removed = state.retired.remove(&key);
                        if let Some(retired) = &removed {
                            state.used_bytes =
                                state.used_bytes.saturating_sub(retired.entry.charge);
                            state.retired_disk_bytes = state
                                .retired_disk_bytes
                                .saturating_sub(retired.entry.charge);
                            state.deleting_slots += 1;
                        }
                        refresh_metadata(&mut state);
                        removed
                    } else {
                        None
                    }
                } else {
                    None
                }
            };
            let metadata_charge = removed
                .as_ref()
                .map(|retired| entry_metadata_charge(&key, &retired.entry.chunk));
            drop(removed);
            if let Some(charge) = metadata_charge {
                let mut state = self.lock_state()?;
                state.entry_metadata_bytes = state.entry_metadata_bytes.saturating_sub(charge);
                state.deleting_slots = state.deleting_slots.saturating_sub(1);
            }
            self.changed.notify_all();
            self.asynchronous_change.notify_waiters();
            self.report_resources();
            result?;
        }
    }

    pub(super) fn retire(
        &self,
        key: &str,
        generation: Option<&Arc<()>>,
        eviction: bool,
    ) -> Result<()> {
        {
            let mut state = self.lock_state()?;
            let eligible = state.entries.get(key).is_some_and(|entry| {
                generation.is_none_or(|generation| Arc::ptr_eq(generation, &entry.generation))
            });
            if eligible && let Some(entry) = state.entries.remove(key) {
                state.retired_disk_bytes = state.retired_disk_bytes.saturating_add(entry.charge);
                state.retired.insert(
                    key.to_owned(),
                    RetiredEntry {
                        entry,
                        deleting: false,
                    },
                );
                if eviction {
                    state.stats.evictions = state.stats.evictions.saturating_add(1);
                }
                refresh_metadata(&mut state);
            }
        }
        self.cleanup_retired()
    }

    pub(super) fn reserve_entry_metadata(&self, charge: usize) -> Result<Option<OutboardLease>> {
        loop {
            if let Some(lease) = self.proof_budget.try_reserve_resident(charge) {
                return Ok(Some(lease));
            }
            // Retire acceleration metadata, never a healthy body or its trust
            // identity. Existing readers keep the wrapper's actual lease alive.
            let old = {
                let mut state = self.lock_state()?;
                let victim = state
                    .entries
                    .values_mut()
                    .filter(|entry| entry.proof.is_some())
                    .min_by_key(|entry| entry.touched);
                let old = victim.and_then(|entry| entry.proof.take());
                refresh_metadata(&mut state);
                old
            };
            let released = old.is_some();
            drop(old);
            if !released {
                return Ok(None);
            }
        }
    }

    pub(super) fn reserve_pending(
        &self,
        key: &str,
        generation: u64,
        charge: u64,
        metadata_charge: usize,
        metadata: &mut Option<OutboardLease>,
        allow_eviction: bool,
    ) -> Result<bool> {
        if !allow_eviction {
            let Some(mut state) = self.try_lock_state()? else {
                return Ok(false);
            };
            if state.closed
                || state.foreground_waiters != 0
                || state.entries.contains_key(key)
                || state.retired.contains_key(key)
                || state.pending.len() >= self.max_flights
                || state.used_bytes.saturating_add(charge) > self.options.capacity_bytes
                || state.entries.len()
                    + state.retired.len()
                    + state.deleting_slots
                    + state.pending.len()
                    >= MAX_PEER_CACHE_ENTRIES
            {
                return Ok(false);
            }
            state.used_bytes += charge;
            state.pending_charge += charge;
            state.pending_metadata_charge += metadata_charge;
            state.pending.insert(
                generation,
                PendingEntry {
                    charge,
                    identity: None,
                    active: true,
                    metadata_charge,
                    _metadata: metadata.take(),
                },
            );
            state.stats.disk_peak_bytes = state.stats.disk_peak_bytes.max(state.used_bytes);
            return Ok(true);
        }
        self.retire(key, None, false)?;
        loop {
            self.cleanup_retired()?;
            let victim = {
                let mut state = self.lock_state()?;
                if state.closed
                    || state.retired.contains_key(key)
                    || charge > self.options.capacity_bytes
                    || state.pending.len() >= self.max_flights
                {
                    return Ok(false);
                }
                if state.used_bytes.saturating_add(charge) <= self.options.capacity_bytes
                    && state.entries.len()
                        + state.retired.len()
                        + state.deleting_slots
                        + state.pending.len()
                        < MAX_PEER_CACHE_ENTRIES
                {
                    state.used_bytes += charge;
                    state.pending_charge += charge;
                    state.pending_metadata_charge += metadata_charge;
                    state.pending.insert(
                        generation,
                        PendingEntry {
                            charge,
                            identity: None,
                            active: true,
                            metadata_charge,
                            _metadata: metadata.take(),
                        },
                    );
                    state.stats.disk_peak_bytes = state.stats.disk_peak_bytes.max(state.used_bytes);
                    return Ok(true);
                }
                state
                    .entries
                    .iter()
                    .min_by_key(|(_, entry)| entry.touched)
                    .map(|(key, _)| key.clone())
            };
            let Some(victim) = victim else {
                return Ok(false);
            };
            self.retire(&victim, None, true)?;
        }
    }

    pub(super) fn cleanup_pending(&self) -> Result<()> {
        // Only inactive owned generations are eligible. An active stream's file
        // and an unidentified/replaced file are never another fill's cleanup.
        let retired: Vec<_> = self
            .lock_state()?
            .pending
            .iter()
            .filter(|(_, entry)| !entry.active)
            .map(|(generation, entry)| (*generation, entry.identity))
            .collect();
        for (generation, identity) in retired {
            if let Err(error) = self.clear_pending(generation, identity) {
                // Its full charge and metadata lease remain in the bounded table.
                // Another object may still fit without touching this generation.
                afs_logging::warn!("dfs.peer_pending_cleanup_failed"; "error" => error.to_string());
            }
        }
        Ok(())
    }

    pub(super) fn clear_pending(
        &self,
        generation: u64,
        expected: Option<(u64, u64)>,
    ) -> Result<()> {
        self.check_root()?;
        #[cfg(test)]
        if self.lock_state()?.test_hooks.fail_remove {
            return Err(unavailable("injected Peer cache cleanup failure"));
        }
        match fs::symlink_metadata(self.pending_path(generation)) {
            Ok(metadata) => {
                validate_private_file(&metadata)?;
                if expected.is_none_or(|identity| identity != (metadata.dev(), metadata.ino())) {
                    return Err(invalid(
                        "Peer cache cleanup refuses an unowned pending file",
                    ));
                }
                fs::remove_file(self.pending_path(generation)).map_err(Error::from)?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(Error::from(error)),
        }
        let removed = {
            let mut state = self.lock_state()?;
            remove_pending_charge(&mut state, generation)
        };
        drop(removed);
        Ok(())
    }

    pub(super) fn account_pending_growth(&self, generation: u64, charge: u64) -> Result<bool> {
        let mut state = self.lock_state()?;
        let pending = state
            .pending
            .get_mut(&generation)
            .ok_or_else(|| unavailable("Peer pending reservation missing"))?;
        if charge <= pending.charge {
            return Ok(false);
        }
        let extra = charge - pending.charge;
        pending.charge = charge;
        state.used_bytes = state.used_bytes.saturating_add(extra);
        state.pending_charge = state.pending_charge.saturating_add(extra);
        state.stats.disk_peak_bytes = state.stats.disk_peak_bytes.max(state.used_bytes);
        Ok(true)
    }

    pub(super) fn publish_entry(
        &self,
        pending_key: (u64, String),
        chunk: ChunkObject,
        charge: u64,
        identity: FileIdentity,
        proof: Option<SharedOutboard>,
        metadata: Option<OutboardLease>,
    ) -> Result<()> {
        let (generation, key) = pending_key;
        let metadata = {
            let mut state = self.lock_state()?;
            state
                .pending
                .get_mut(&generation)
                .and_then(|entry| entry._metadata.take())
                .or(metadata)
        };
        let mut local = CacheState::new_local(self.proof_budget());
        {
            let state = self.lock_state()?;
            local.local_index_available = state.index_lease.is_some();
            #[cfg(test)]
            {
                local.test_hooks.proof_limit = state.test_hooks.proof_limit;
            }
        }
        local.entries.insert(
            key.clone(),
            Entry {
                chunk,
                charge,
                identity: (identity.0, identity.1),
                touched: self.bucket(),
                proof: None,
                generation: Arc::new(()),
                _metadata: metadata,
                #[cfg(test)]
                drop_observation: None,
            },
        );
        install_proof(&mut local, &key, identity, proof);
        let entry = local
            .entries
            .remove(&key)
            .ok_or_else(|| unavailable("Peer cache entry missing"))?;
        let mut state = self.lock_state()?;
        let removed = remove_pending_charge(&mut state, generation);
        state.used_bytes += charge;
        drop(removed);
        state.entry_metadata_bytes += entry_metadata_charge(&key, &entry.chunk);
        state.entries.insert(key, entry);
        refresh_metadata(&mut state);
        drop(state);
        self.report_resources();
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn fill_flight<'a>(
        &'a self,
        chunk: &ChunkObject,
        check: impl Fn() -> Result<()>,
    ) -> Result<FillRole<'a>> {
        let key = self.key(chunk)?;
        let (flight, leader) = loop {
            check()?;
            let mut state = self.lock_state()?;
            if state.closed {
                return Err(unavailable("Peer cache is stopped"));
            }
            if let Some(flight) = state.flights.get(&key) {
                break (flight.clone(), false);
            }
            if state.flight_live < self.max_flights {
                let Some(metadata) = self
                    .proof_budget
                    .try_reserve(std::mem::size_of::<FillFlight>() + 512 + key.capacity() + 256)
                else {
                    return Ok(FillRole::Finished(FillOutcome::Uncached));
                };
                let flight = Arc::new(FillFlight {
                    result: Mutex::new(None),
                    changed: Condvar::new(),
                    asynchronous_change: tokio::sync::Notify::new(),
                    _metadata: metadata,
                });
                state.flights.insert(key.clone(), flight.clone());
                state.flight_live += 1;
                state.flight_metadata_bytes +=
                    std::mem::size_of::<FillFlight>() + 512 + key.capacity() + 256;
                break (flight, true);
            }
            drop(
                self.changed
                    .wait_timeout(state, Duration::from_millis(25))
                    .map_err(|_| unavailable("Peer flight admission lock poisoned"))?,
            );
        };
        if leader {
            self.report_resources();
            return Ok(FillRole::Leader(FillLeader {
                cache: self,
                key,
                flight: Some(flight),
                completed: false,
            }));
        }
        let outcome = (|| loop {
            check()?;
            if self.lock_state()?.closed {
                return Err(unavailable("Peer cache is stopped"));
            }
            let result = flight
                .result
                .lock()
                .map_err(|_| unavailable("Peer flight lock poisoned"))?;
            if let Some(outcome) = &*result {
                return Ok(outcome.clone());
            }
            drop(
                flight
                    .changed
                    .wait_timeout(result, Duration::from_millis(25))
                    .map_err(|_| unavailable("Peer flight wait lock poisoned"))?,
            );
        })();
        self.release_flight(&key, flight);
        outcome.map(FillRole::Finished)
    }
    pub(crate) async fn fill_flight_async<'a>(
        &'a self,
        chunk: &ChunkObject,
        check: impl Fn() -> Result<()> + Send + Sync,
    ) -> Result<FillRole<'a>> {
        let key = self.key(chunk)?;
        let (flight, leader) = loop {
            let changed = self.asynchronous_change.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            check()?;
            let selected = {
                let mut state = self.lock_state()?;
                if state.closed {
                    return Err(unavailable("Peer cache is stopped"));
                }
                if let Some(flight) = state.flights.get(&key) {
                    Some((flight.clone(), false))
                } else if state.flight_live < self.max_flights {
                    let charge = std::mem::size_of::<FillFlight>() + 512 + key.capacity() + 256;
                    let Some(metadata) = self.proof_budget.try_reserve(charge) else {
                        return Ok(FillRole::Finished(FillOutcome::Uncached));
                    };
                    let flight = Arc::new(FillFlight {
                        result: Mutex::new(None),
                        changed: Condvar::new(),
                        asynchronous_change: tokio::sync::Notify::new(),
                        _metadata: metadata,
                    });
                    state.flights.insert(key.clone(), flight.clone());
                    state.flight_live += 1;
                    state.flight_metadata_bytes += charge;
                    Some((flight, true))
                } else {
                    None
                }
            };
            if let Some(selected) = selected {
                break selected;
            }
            let _ = tokio::time::timeout(Duration::from_millis(25), changed).await;
        };
        if leader {
            self.report_resources();
            return Ok(FillRole::Leader(FillLeader {
                cache: self,
                key,
                flight: Some(flight),
                completed: false,
            }));
        }
        let follower = AsyncFillFollower {
            cache: self,
            key,
            flight: Some(flight),
        };
        let flight = follower
            .flight
            .as_ref()
            .ok_or_else(|| unavailable("Peer follower flight missing"))?;
        let outcome = async {
            loop {
                let changed = flight.asynchronous_change.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                check()?;
                if self.lock_state()?.closed {
                    return Err(unavailable("Peer cache is stopped"));
                }
                let ready = flight
                    .result
                    .lock()
                    .map_err(|_| unavailable("Peer flight lock poisoned"))?
                    .clone();
                if let Some(outcome) = ready {
                    return Ok(outcome);
                }
                let _ = tokio::time::timeout(Duration::from_millis(25), changed).await;
            }
        }
        .await;
        drop(follower);
        outcome.map(FillRole::Finished)
    }

    pub(crate) fn try_fill_flight(
        self: &Arc<Self>,
        chunk: &ChunkObject,
    ) -> Result<Option<OwnedFillLeader>> {
        let key = self.key(chunk)?;
        let Some(mut state) = self.try_lock_state()? else {
            return Ok(None);
        };
        if state.closed
            || state.foreground_waiters != 0
            || state.flights.contains_key(&key)
            || state.flight_live >= self.max_flights
        {
            return Ok(None);
        }
        let charge = std::mem::size_of::<FillFlight>() + 512 + key.capacity() + 256;
        let Some(metadata) = self.proof_budget.try_reserve(charge) else {
            return Ok(None);
        };
        let flight = Arc::new(FillFlight {
            result: Mutex::new(None),
            changed: Condvar::new(),
            asynchronous_change: tokio::sync::Notify::new(),
            _metadata: metadata,
        });
        state.flights.insert(key.clone(), flight.clone());
        state.flight_live += 1;
        state.flight_metadata_bytes += charge;
        drop(state);
        Ok(Some(OwnedFillLeader {
            cache: self.clone(),
            key,
            flight: Some(flight),
            completed: false,
        }))
    }

    fn release_flight(&self, key: &str, flight: Arc<FillFlight>) {
        let removed = if let Ok(mut state) = self.state.lock() {
            let same = state
                .flights
                .get(key)
                .is_some_and(|current| Arc::ptr_eq(current, &flight));
            drop(flight);
            if same
                && state
                    .flights
                    .get(key)
                    .is_some_and(|current| Arc::strong_count(current) == 1)
            {
                state.flights.remove(key)
            } else {
                None
            }
        } else {
            None
        };
        let released = removed.is_some();
        drop(removed);
        if released && let Ok(mut state) = self.state.lock() {
            state.flight_live = state.flight_live.saturating_sub(1);
            state.flight_metadata_bytes = state
                .flight_metadata_bytes
                .saturating_sub(std::mem::size_of::<FillFlight>() + 512 + key.len() + 256);
        }
        self.changed.notify_all();
        self.asynchronous_change.notify_waiters();
        self.report_resources();
    }
}

struct AsyncFillFollower<'a> {
    pub(super) cache: &'a DiskPeerCache,
    pub(super) key: String,
    pub(super) flight: Option<Arc<FillFlight>>,
}

impl Drop for AsyncFillFollower<'_> {
    fn drop(&mut self) {
        if let Some(flight) = self.flight.take() {
            self.cache.release_flight(&self.key, flight);
        }
    }
}

fn finish_flight(flight: &Option<Arc<FillFlight>>, completed: &mut bool, outcome: FillOutcome) {
    if *completed {
        return;
    }
    let outcome = match outcome {
        FillOutcome::Failed(error) if error.message().len() > 512 => {
            afs_logging::warn!("dfs.peer_fill_shared_error_truncated"; "code" => error.code().to_string());
            let mut boundary = 512;
            while !error.message().is_char_boundary(boundary) {
                boundary -= 1;
            }
            FillOutcome::Failed(Error::new(
                error.code(),
                error.kind(),
                &error.message()[..boundary],
            ))
        }
        outcome => outcome,
    };
    if let Some(flight) = flight
        && let Ok(mut result) = flight.result.lock()
    {
        *result = Some(outcome);
        *completed = true;
        flight.changed.notify_all();
        flight.asynchronous_change.notify_waiters();
    }
}

impl FillLeader<'_> {
    pub(crate) fn finish(&mut self, outcome: FillOutcome) {
        finish_flight(&self.flight, &mut self.completed, outcome);
    }
}

impl OwnedFillLeader {
    pub(crate) fn finish(&mut self, outcome: FillOutcome) {
        finish_flight(&self.flight, &mut self.completed, outcome);
    }
}

impl Drop for OwnedFillLeader {
    fn drop(&mut self) {
        self.finish(FillOutcome::Failed(unavailable(
            "Peer prefetch stopped before completion",
        )));
        if let Some(flight) = self.flight.take() {
            self.cache.release_flight(&self.key, flight);
        }
    }
}

impl Drop for FillLeader<'_> {
    fn drop(&mut self) {
        self.finish(FillOutcome::Failed(unavailable(
            "Peer fill leader stopped before completion",
        )));
        if let Some(flight) = self.flight.take() {
            self.cache.release_flight(&self.key, flight);
        }
    }
}

pub(super) struct FillWaiter<'a>(&'a DiskPeerCache);

impl<'a> FillWaiter<'a> {
    pub(super) fn new(cache: &'a DiskPeerCache) -> Result<Self> {
        cache.lock_state()?.foreground_waiters += 1;
        Ok(Self(cache))
    }
}

impl Drop for FillWaiter<'_> {
    fn drop(&mut self) {
        if let Ok(mut state) = self.0.state.lock() {
            state.foreground_waiters = state.foreground_waiters.saturating_sub(1);
        }
        self.0.changed.notify_all();
        self.0.asynchronous_change.notify_waiters();
    }
}

impl Drop for FillPermit<'_> {
    fn drop(&mut self) {
        if let Ok(mut state) = self.cache.state.lock() {
            if self.streaming {
                state.stream_active = state.stream_active.saturating_sub(1);
            } else {
                state.fill_active = false;
            }
        }
        self.cache.changed.notify_all();
        self.cache.asynchronous_change.notify_waiters();
        self.cache.report_resources();
    }
}

impl Drop for EntryPin<'_> {
    fn drop(&mut self) {
        drop(self.generation.take());
        if let Err(error) = self.cache.cleanup_retired() {
            afs_logging::warn!("dfs.peer_cache_retired_cleanup_failed"; "error" => error.to_string());
        }
    }
}
