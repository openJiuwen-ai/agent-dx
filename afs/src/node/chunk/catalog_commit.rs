//! Caller-driven catalog commit groups. No body or worker thread is retained.
use super::*;

const MAX_REQUESTS: usize = 256;
const CONTROL_LIMIT: usize = 1024 * 1024;
// Covers the fixed slot table, one group's index vector and bounded errors.
const FIXED_ALLOWANCE: usize = 64 * 1024;

#[derive(Debug)]
struct Slot {
    generation: u64,
    ids: Vec<ChunkId>,
    charge: usize,
    records: Option<Vec<LocalChunkRecord>>,
    preparing_bytes: usize,
    result: Option<std::result::Result<u64, Arc<Error>>>,
}

#[derive(Debug)]
struct State {
    slots: [Option<Slot>; MAX_REQUESTS],
    next_generation: u64,
    limit: usize,
    bytes: usize,
    peak_bytes: usize,
    writer: bool,
    exclusive: bool,
    exclusive_waiters: usize,
    waiters: usize,
    closed: bool,
    deadline: Option<Instant>,
    failure: Option<Arc<Error>>,
    groups: u64,
    max_group: usize,
    actual_peak: usize,
}

#[derive(Debug)]
pub(super) struct CatalogCommitter {
    state: Mutex<State>,
    changed: Condvar,
    metrics: Mutex<Option<super::super::read_work::ReadWorkMetrics>>,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug)]
pub(crate) struct CatalogCommitStats {
    pub requests: usize,
    pub ready: usize,
    pub waiters: usize,
    pub writer: bool,
    pub closed: bool,
    pub reserved_bytes: usize,
    pub peak_reserved_bytes: usize,
    pub actual_peak_bytes: usize,
    pub groups: u64,
    pub max_group: usize,
    pub failed: bool,
    pub exclusive: bool,
}

impl Default for CatalogCommitter {
    fn default() -> Self {
        Self {
            state: Mutex::new(State {
                slots: std::array::from_fn(|_| None),
                next_generation: 1,
                limit: MAX_REQUESTS,
                bytes: FIXED_ALLOWANCE,
                peak_bytes: FIXED_ALLOWANCE,
                writer: false,
                exclusive: false,
                exclusive_waiters: 0,
                waiters: 0,
                closed: false,
                deadline: None,
                failure: None,
                groups: 0,
                max_group: 0,
                actual_peak: std::mem::size_of::<Self>(),
            }),
            changed: Condvar::new(),
            metrics: Mutex::new(None),
        }
    }
}

pub(super) struct Admission<'a> {
    owner: &'a CatalogCommitter,
    slot: Option<(usize, u64)>,
    namespace_changed: std::cell::Cell<bool>,
    confirmed: std::cell::Cell<bool>,
}

impl CatalogCommitter {
    #[cfg(any(feature = "dfs", test))]
    pub(super) fn set_metrics(&self, metrics: super::super::read_work::ReadWorkMetrics) {
        *self
            .metrics
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(metrics);
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        self.publish(&state);
    }

    fn publish(&self, state: &State) {
        if let Some(metrics) = self
            .metrics
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_ref()
        {
            metrics.catalog_commit_resources(&[
                ("requests", state.slots.iter().flatten().count() as u64),
                ("reserved_bytes", state.bytes as u64),
                ("peak_reserved_bytes", state.peak_bytes as u64),
                ("actual_peak_bytes", state.actual_peak as u64),
                ("control_limit_bytes", CONTROL_LIMIT as u64),
                ("groups", state.groups),
                ("max_group", state.max_group as u64),
                ("failed", u64::from(state.failure.is_some())),
                ("exclusive", u64::from(state.exclusive)),
            ]);
        }
    }

    #[cfg(any(feature = "dfs", test))]
    pub(super) fn configure(&mut self, limit: usize) -> Result<()> {
        if !(1..=MAX_REQUESTS).contains(&limit) {
            return Err(invalid("catalog commit queue must be 1..=256"));
        }
        self.state
            .get_mut()
            .map_err(|_| invalid("catalog admission is poisoned"))?
            .limit = limit;
        Ok(())
    }

    fn healthy(state: &State) -> Result<()> {
        state
            .failure
            .as_ref()
            .map_or(Ok(()), |error| Err(error.as_ref().clone()))
    }

    fn check(state: &State) -> Result<()> {
        Self::healthy(state)?;
        if state.closed {
            return Err(invalid("catalog admission is closed"));
        }
        Ok(())
    }

    fn wait<'a>(
        &self,
        state: std::sync::MutexGuard<'a, State>,
    ) -> Result<std::sync::MutexGuard<'a, State>> {
        let wait = state
            .deadline
            .map_or(Duration::from_millis(50), |deadline| {
                deadline
                    .saturating_duration_since(Instant::now())
                    .min(Duration::from_millis(50))
            });
        if wait.is_zero() {
            return Err(Error::coded(
                afs_error::IO_TIMEOUT,
                "catalog admission stop deadline expired",
            ));
        }
        self.changed
            .wait_timeout(state, wait)
            .map(|(state, _)| state)
            .map_err(|_| invalid("catalog admission wait is poisoned"))
    }

    pub(super) fn admit<'a, 'b>(
        &'a self,
        ids: impl Iterator<Item = &'b ChunkId> + Clone,
        charge: usize,
    ) -> Result<Admission<'a>> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| invalid("catalog admission is poisoned"))?;
        if charge > CONTROL_LIMIT - FIXED_ALLOWANCE {
            state.exclusive_waiters += 1;
            loop {
                if let Err(error) = Self::check(&state) {
                    state.exclusive_waiters -= 1;
                    self.changed.notify_all();
                    return Err(error);
                }
                if !state.writer && !state.exclusive && state.slots.iter().all(Option::is_none) {
                    state.exclusive_waiters -= 1;
                    state.exclusive = true;
                    self.publish(&state);
                    return Ok(Admission {
                        owner: self,
                        slot: None,
                        namespace_changed: std::cell::Cell::new(false),
                        confirmed: std::cell::Cell::new(false),
                    });
                }
                match self.wait(state) {
                    Ok(next) => state = next,
                    Err(error) => {
                        // Restore the waiter count even on the deadline path.
                        let mut state = self
                            .state
                            .lock()
                            .map_err(|_| invalid("catalog admission is poisoned"))?;
                        state.exclusive_waiters -= 1;
                        self.changed.notify_all();
                        return Err(error);
                    }
                }
            }
        }
        loop {
            Self::check(&state)?;
            let conflict = state.slots.iter().flatten().any(|slot| {
                ids.clone()
                    .any(|id| slot.ids.iter().any(|active| active == id))
            });
            let index = state.slots.iter().position(Option::is_none);
            if !state.exclusive
                && state.exclusive_waiters == 0
                && !conflict
                && state.slots.iter().flatten().count() < state.limit
                && state.bytes.saturating_add(charge) <= CONTROL_LIMIT
                && let Some(index) = index
            {
                let generation = state.next_generation;
                state.next_generation = generation
                    .checked_add(1)
                    .ok_or_else(|| invalid("catalog generation exhausted"))?;
                let ids: Vec<_> = ids.cloned().collect();
                state.slots[index] = Some(Slot {
                    generation,
                    ids,
                    charge,
                    records: None,
                    preparing_bytes: 0,
                    result: None,
                });
                state.bytes += charge;
                state.peak_bytes = state.peak_bytes.max(state.bytes);
                if let Err(error) = Self::observe_actual(&mut state, 0) {
                    state.slots[index] = None;
                    state.bytes -= charge;
                    self.changed.notify_all();
                    return Err(error);
                }
                self.publish(&state);
                return Ok(Admission {
                    owner: self,
                    slot: Some((index, generation)),
                    namespace_changed: std::cell::Cell::new(false),
                    confirmed: std::cell::Cell::new(false),
                });
            }
            state.waiters += 1;
            match self.wait(state) {
                Ok(mut next) => {
                    next.waiters -= 1;
                    state = next;
                    self.changed.notify_all();
                }
                Err(error) => {
                    let mut state = self
                        .state
                        .lock()
                        .map_err(|_| invalid("catalog admission is poisoned"))?;
                    state.waiters -= 1;
                    self.changed.notify_all();
                    return Err(error);
                }
            }
        }
    }

    fn actual(state: &State, scratch: usize) -> usize {
        std::mem::size_of::<Self>()
            + scratch
            + state
                .slots
                .iter()
                .flatten()
                .map(|slot| {
                    slot.preparing_bytes
                        + slot.ids.capacity() * std::mem::size_of::<ChunkId>()
                        + slot.ids.iter().map(|id| id.0.capacity()).sum::<usize>()
                        + slot.records.as_ref().map_or(0, record_capacity)
                })
                .sum::<usize>()
    }

    fn observe_actual(state: &mut State, scratch: usize) -> Result<()> {
        let bytes = Self::actual(state, scratch);
        state.actual_peak = state.actual_peak.max(bytes);
        if bytes > CONTROL_LIMIT || bytes > state.bytes {
            return Err(invalid(
                "catalog control allocation exceeded its reserved capacity",
            ));
        }
        Ok(())
    }

    #[cfg(any(feature = "dfs", test))]
    pub(super) fn note_stop(&self, deadline: Instant) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.deadline = Some(state.deadline.map_or(deadline, |old| old.min(deadline)));
        self.changed.notify_all();
    }

    #[cfg(any(feature = "dfs", test))]
    pub(super) fn shutdown(&self) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| invalid("catalog admission is poisoned"))?;
        state.closed = true;
        self.changed.notify_all();
        // Synchronous owners keep their flights and bytes until actual I/O ends.
        // The existing process watchdog handles an uninterruptible filesystem call.
        while state.writer
            || state.exclusive
            || state.exclusive_waiters != 0
            || state.waiters != 0
            || state.slots.iter().any(Option::is_some)
        {
            state = self
                .changed
                .wait(state)
                .map_err(|_| invalid("catalog drain is poisoned"))?;
        }
        state
            .failure
            .as_ref()
            .map_or(Ok(()), |error| Err(error.as_ref().clone()))
    }

    pub(super) fn fail(&self, error: Error) -> Arc<Error> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let failure = state.failure.get_or_insert_with(|| Arc::new(error)).clone();
        self.publish(&state);
        self.changed.notify_all();
        failure
    }

    #[cfg(test)]
    pub(super) fn stats(&self) -> CatalogCommitStats {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        CatalogCommitStats {
            requests: state.slots.iter().flatten().count(),
            ready: state
                .slots
                .iter()
                .flatten()
                .filter(|slot| slot.records.is_some() && slot.result.is_none())
                .count(),
            waiters: state.waiters + state.exclusive_waiters,
            writer: state.writer,
            closed: state.closed,
            reserved_bytes: state.bytes,
            peak_reserved_bytes: state.peak_bytes,
            actual_peak_bytes: state.actual_peak,
            groups: state.groups,
            max_group: state.max_group,
            failed: state.failure.is_some(),
            exclusive: state.exclusive,
        }
    }
}

impl Admission<'_> {
    pub(super) fn namespace_changed(&self) {
        self.namespace_changed.set(true);
    }
    pub(super) fn confirm(&self) {
        self.confirmed.set(true);
    }
    pub(super) fn fail_if_unconfirmed(&self, error: Error) {
        if self.namespace_changed.get() && !self.confirmed.get() {
            self.owner.fail(error);
        }
    }
}

impl Drop for Admission<'_> {
    fn drop(&mut self) {
        self.fail_if_unconfirmed(invalid(
            "installed chunk namespace did not complete its durability barriers",
        ));
        let mut state = self
            .owner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some((index, generation)) = self.slot {
            if state.slots[index]
                .as_ref()
                .is_some_and(|slot| slot.generation == generation)
                && let Some(slot) = state.slots[index].take()
            {
                let charge = slot.charge;
                // Free key/record/result allocations before refunding their lease.
                drop(slot);
                state.bytes = state.bytes.saturating_sub(charge);
            }
        } else {
            state.exclusive = false;
        }
        self.owner.publish(&state);
        self.owner.changed.notify_all();
    }
}

pub(super) fn record_allocation(record: &LocalChunkRecord) -> usize {
    let PhysicalLocation::PerChunkFile { relative_path } = &record.location;
    std::mem::size_of::<LocalChunkRecord>()
        + record.chunk.id.0.capacity()
        + record.device_id.capacity()
        + relative_path.capacity()
        + record
            .segment_crc
            .as_ref()
            .map_or(0, |t| t.checksums.capacity() * 4)
}

pub(super) fn record_capacity(records: &Vec<LocalChunkRecord>) -> usize {
    records.capacity() * std::mem::size_of::<LocalChunkRecord>()
        + records
            .iter()
            .map(|record| {
                let PhysicalLocation::PerChunkFile { relative_path } = &record.location;
                record.chunk.id.0.capacity()
                    + record.device_id.capacity()
                    + relative_path.capacity()
                    + record
                        .segment_crc
                        .as_ref()
                        .map_or(0, |t| t.checksums.capacity() * 4)
            })
            .sum::<usize>()
}

pub(super) fn charge_for<'a>(ids: impl Iterator<Item = &'a ChunkId>, device: &str) -> usize {
    ids.fold(4096_usize, |total, id| {
        // Covers actual keys, new records, dedup state, a complete frozen group,
        // and both JSON/checksum scratch capacities. Not a body allocation.
        total.saturating_add(
            (std::mem::size_of::<LocalChunkRecord>()
                + id.0.capacity().saturating_mul(2)
                + device.len()
                + 512)
                .saturating_mul(16),
        )
    })
}

struct Writer<'a> {
    owner: &'a CatalogCommitter,
    finished: bool,
}
impl Drop for Writer<'_> {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        let failure = self.owner.fail(invalid(
            "catalog commit writer terminated before completing its group",
        ));
        let mut state = self
            .owner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        for slot in state.slots.iter_mut().flatten() {
            if slot.result.is_none() {
                slot.result = Some(Err(failure.clone()));
            }
        }
        state.writer = false;
        self.owner.changed.notify_all();
    }
}

impl LocalChunkStore {
    pub(super) fn admit_catalog<'a>(&'a self, staged: &[StagedChunk]) -> Result<Admission<'a>> {
        self.catalog_committer.admit(
            staged.iter().map(|item| &item.chunk.id),
            charge_for(staged.iter().map(|item| &item.chunk.id), &self.device_id),
        )
    }

    pub(super) fn observe_catalog_preparing(
        &self,
        admission: &Admission<'_>,
        bytes: usize,
    ) -> Result<()> {
        if let Some((index, generation)) = admission.slot {
            let mut state = self
                .catalog_committer
                .state
                .lock()
                .map_err(|_| invalid("catalog admission is poisoned"))?;
            let slot = state.slots[index]
                .as_mut()
                .filter(|slot| slot.generation == generation)
                .ok_or_else(|| invalid("catalog preparation flight changed"))?;
            slot.preparing_bytes = bytes;
            CatalogCommitter::observe_actual(&mut state, 0)?;
            self.catalog_committer.publish(&state);
        }
        Ok(())
    }

    pub(super) fn commit_catalog(
        &self,
        admission: &Admission<'_>,
        records: Vec<LocalChunkRecord>,
    ) -> Result<u64> {
        if records.is_empty() {
            let state = self
                .catalog_committer
                .state
                .lock()
                .map_err(|_| invalid("catalog admission is poisoned"))?;
            CatalogCommitter::healthy(&state)?;
            return Ok(self
                .catalog
                .lock()
                .map_err(|_| invalid("local chunk catalog lock is poisoned"))?
                .revision);
        }
        let Some((index, generation)) = admission.slot else {
            // Oversized original batches retain their original synchronous metadata
            // cost, outside the bounded new queue. They do not get split into ACKs.
            let result = self.write_catalog_group(records, true);
            let mut state = self
                .catalog_committer
                .state
                .lock()
                .map_err(|_| invalid("catalog admission is poisoned"))?;
            state.groups += u64::from(result.is_ok());
            state.max_group = state.max_group.max(1);
            self.catalog_committer.publish(&state);
            return result;
        };
        {
            let mut state = self
                .catalog_committer
                .state
                .lock()
                .map_err(|_| invalid("catalog admission is poisoned"))?;
            CatalogCommitter::healthy(&state)?;
            let slot = state.slots[index]
                .as_mut()
                .filter(|slot| slot.generation == generation)
                .ok_or_else(|| invalid("catalog install reservation changed"))?;
            slot.preparing_bytes = 0;
            slot.records = Some(records);
            CatalogCommitter::observe_actual(&mut state, 0)?;
        }
        #[cfg(test)]
        self.catalog_commit_test.before_group();
        let mut state = self
            .catalog_committer
            .state
            .lock()
            .map_err(|_| invalid("catalog admission is poisoned"))?;
        loop {
            let slot = state.slots[index]
                .as_ref()
                .filter(|slot| slot.generation == generation)
                .ok_or_else(|| invalid("catalog install reservation was released too early"))?;
            if let Some(result) = &slot.result {
                return result.clone().map_err(|error| error.as_ref().clone());
            }
            if !state.writer {
                state.writer = true;
                drop(state);
                self.drive_catalog_group()?;
                state = self
                    .catalog_committer
                    .state
                    .lock()
                    .map_err(|_| invalid("catalog admission is poisoned"))?;
                continue;
            }
            // Queued/active ownership cannot be abandoned at a stop deadline.
            state = self
                .catalog_committer
                .changed
                .wait(state)
                .map_err(|_| invalid("catalog completion wait is poisoned"))?;
        }
    }

    fn drive_catalog_group(&self) -> Result<()> {
        let mut writer = Writer {
            owner: &self.catalog_committer,
            finished: false,
        };
        let (tickets, records) = {
            let mut state = self
                .catalog_committer
                .state
                .lock()
                .map_err(|_| invalid("catalog admission is poisoned"))?;
            let mut tickets = Vec::new();
            let count: usize = state
                .slots
                .iter()
                .flatten()
                .filter(|slot| slot.result.is_none())
                .filter_map(|slot| slot.records.as_ref())
                .map(Vec::len)
                .sum();
            let mut records = Vec::with_capacity(count);
            // The empty destination array coexists with queued source arrays
            // during ownership transfer. Count that actual capacity as well.
            CatalogCommitter::observe_actual(
                &mut state,
                records.capacity() * std::mem::size_of::<LocalChunkRecord>(),
            )?;
            for (index, slot) in state.slots.iter_mut().enumerate() {
                let Some(slot) = slot else {
                    continue;
                };
                if slot.result.is_none()
                    && let Some(items) = slot.records.take()
                {
                    tickets.push((index, slot.generation));
                    records.extend(items);
                }
            }
            CatalogCommitter::observe_actual(
                &mut state,
                record_capacity(&records)
                    + tickets.capacity() * std::mem::size_of::<(usize, u64)>(),
            )?;
            (tickets, records)
        };
        let result = self.write_catalog_group(records, false).map_err(Arc::new);
        let mut state = self
            .catalog_committer
            .state
            .lock()
            .map_err(|_| invalid("catalog admission is poisoned"))?;
        state.groups += u64::from(result.is_ok());
        state.max_group = state.max_group.max(tickets.len());
        for (index, generation) in tickets {
            let slot = state.slots[index]
                .as_mut()
                .filter(|slot| slot.generation == generation)
                .ok_or_else(|| invalid("catalog flight disappeared during a durable write"))?;
            slot.result = Some(result.clone());
        }
        // Hand off after this one group, even under continuously arriving work.
        state.writer = false;
        writer.finished = true;
        self.catalog_committer.publish(&state);
        self.catalog_committer.changed.notify_all();
        Ok(())
    }

    fn write_catalog_group(
        &self,
        mut records: Vec<LocalChunkRecord>,
        exclusive: bool,
    ) -> Result<u64> {
        let revision = {
            let state = self
                .catalog_committer
                .state
                .lock()
                .map_err(|_| invalid("catalog admission is poisoned"))?;
            CatalogCommitter::healthy(&state)?;
            self.catalog
                .lock()
                .map_err(|_| invalid("local chunk catalog lock is poisoned"))?
                .revision
                .checked_add(1)
                .ok_or_else(|| invalid("local catalog revision exhausted"))?
        };
        for record in &mut records {
            record.catalog_revision = revision;
        }
        #[cfg(test)]
        if self
            .catalog_commit_test
            .fail_chunks_dir
            .swap(false, Ordering::AcqRel)
        {
            return Err(Error::coded(
                afs_error::IO_OTHER,
                "injected chunks namespace barrier failure",
            ));
        }
        {
            let _diagnostic = self.coarse_diagnostic("persist_chunks_dir_sync", 0);
            File::open(&self.chunks)
                .and_then(|directory| directory.sync_all())
                .map_err(Error::from)?;
        }
        let checksum_input = serde_json::to_vec(&(revision, &records))
            .map_err(|error| invalid(format!("cannot checksum local chunk catalog: {error}")))?;
        if !exclusive {
            let mut state = self
                .catalog_committer
                .state
                .lock()
                .map_err(|_| invalid("catalog admission is poisoned"))?;
            CatalogCommitter::observe_actual(
                &mut state,
                record_capacity(&records) + checksum_input.capacity(),
            )?;
        }
        let checksum = blake3::hash(&checksum_input).to_hex().to_string();
        drop(checksum_input);
        let txn = CatalogTxn {
            revision,
            records,
            checksum,
        };
        #[cfg(test)]
        {
            let step = self
                .catalog_commit_test
                .fail_wal_step
                .swap(0, Ordering::AcqRel);
            if step != 0 {
                let mut encoded = serde_json::to_vec(&txn).unwrap();
                encoded.push(b'\n');
                let mut file = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&self.catalog_path)
                    .map_err(Error::from)?;
                let cut = if step == 1 {
                    encoded.len() / 2
                } else {
                    encoded.len()
                };
                file.write_all(&encoded[..cut]).map_err(Error::from)?;
                if step == 3 {
                    file.sync_all().map_err(Error::from)?;
                }
                return Err(self
                    .catalog_committer
                    .fail(Error::coded(
                        afs_error::IO_OTHER,
                        format!("injected catalog write/sync/parent barrier failure {step}"),
                    ))
                    .as_ref()
                    .clone());
            }
        }
        let result = append_catalog_txn_observed(
            &self.catalog_path,
            &txn,
            self.read_work.as_ref(),
            |capacity| {
                if !exclusive {
                    let mut state = self
                        .catalog_committer
                        .state
                        .lock()
                        .map_err(|_| invalid("catalog admission is poisoned"))?;
                    CatalogCommitter::observe_actual(
                        &mut state,
                        record_capacity(&txn.records) + txn.checksum.capacity() + capacity,
                    )?;
                }
                #[cfg(test)]
                self.catalog_commit_test.pause_wal();
                Ok(())
            },
        );
        if let Err(error) = result {
            return Err(self.catalog_committer.fail(error).as_ref().clone());
        }
        let mut catalog = self
            .catalog
            .lock()
            .map_err(|_| invalid("local chunk catalog lock is poisoned"))?;
        if catalog.revision.checked_add(1) != Some(revision) {
            drop(catalog);
            return Err(self
                .catalog_committer
                .fail(invalid("catalog changed outside its single writer"))
                .as_ref()
                .clone());
        }
        for record in txn.records {
            catalog.records.insert(record.chunk.id.clone(), record);
        }
        catalog.revision = revision;
        Ok(revision)
    }
}
