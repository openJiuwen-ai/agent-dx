//! Global and embedded ShardScheduler scheduling.

// Let library tests reuse the integration Redis fixture through its crate path.
#[cfg(test)]
extern crate self as adx_coordinator;

pub use adx_core::snapshots;
pub mod auth;
pub mod autoscaling;
mod journal;
pub mod metrics;
mod queue;
pub mod routes;
pub mod rpc;
mod shard;
pub mod storage;

use adx_core::{Assignment, EnvironmentSpec, Error, Result};
use adx_scheduling::{Framework, PlacedEnvironment, Snapshot};
pub use adx_scheduling::{Node, Placement};
use journal::MutationJournal;
pub use queue::TenantQueue;
pub use shard::SchedulingStats;
use shard::{Attempt, ShardScheduler};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Cooperative scheduling limits; a single plugin call is not preemptible.
#[derive(Clone, Debug)]
pub struct SchedulerConfig {
    pub max_attempts: usize,
    pub max_duration: Duration,
    pub candidate_cache_entries: usize,
    pub mutation_history: usize,
}
impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            max_attempts: 256,
            max_duration: Duration::from_millis(10),
            candidate_cache_entries: 32,
            mutation_history: 65536,
        }
    }
}
#[derive(Debug, Default)]
pub struct RoundOutcome {
    pub assignments: Vec<Assignment>,
    pub attempted: usize,
    pub yielded: bool,
    pub snapshot_revision: u64,
    /// Earlier successful reservations remain valid if a later plugin fails.
    pub error: Option<Error>,
}
pub struct Coordinator {
    shards: Vec<ShardScheduler>,
    node_shards: BTreeMap<String, usize>,
    requests: BTreeMap<String, (usize, EnvironmentSpec)>,
    next_shard: usize,
    next_node_shard: usize,
    generation: u64,
    generation_source: Option<Arc<dyn Fn() -> Result<u64> + Send + Sync>>,
    config: SchedulerConfig,
    snapshot: Arc<Snapshot>,
    journal: MutationJournal,
    ready: VecDeque<usize>,
    retired: BTreeSet<String>,
    volume_placement: BTreeMap<String, VolumePlacement>,
}

/// Coordinator-resolved storage topology and a currently unavailable exclusive UID.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VolumePlacement {
    /// Domains used to refresh queued topology after node mount reports change.
    pub storage_domains: BTreeSet<String>,
    pub nodes: BTreeSet<String>,
    pub waiting_for: Option<String>,
}
impl VolumePlacement {
    fn allows(&self, node: &str) -> bool {
        self.waiting_for.is_none() && self.nodes.contains(node)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingRequest {
    pub spec: EnvironmentSpec,
    pub enqueue_time_millis: u64,
}

impl Coordinator {
    pub(crate) fn use_generation_source(&mut self, session: storage::Session) {
        self.generation_source = Some(Arc::new(move || session.next_generation()));
    }
    pub fn metrics(&self) -> String {
        self.metrics_excluding(&BTreeSet::new()).finish()
    }
    pub(crate) fn metrics_excluding(
        &self,
        unavailable: &BTreeSet<String>,
    ) -> adx_observability::metrics::Text {
        let mut text = adx_observability::metrics::Text::default();
        for (id, shard) in self.shards.iter().enumerate() {
            shard.metrics(&mut text, id, unavailable);
        }
        text
    }

    /// Rebuild only persisted allocations. Waiting requests are intentionally
    /// not recovered. Nodes must re-register before new work can be placed.
    pub fn restore(saved: &storage::StoredSnapshot, placement: Placement) -> Result<Self> {
        Self::restore_with_config(saved, placement, SchedulerConfig::default())
    }
    pub fn restore_with_config(
        saved: &storage::StoredSnapshot,
        placement: Placement,
        config: SchedulerConfig,
    ) -> Result<Self> {
        saved.validate()?;
        let mut coordinator = Self::with_config(saved.shard_count, placement, config)?;
        coordinator.generation = saved.generation;
        for (id, registration) in &saved.nodes {
            let mut node = registration.node.clone();
            node.available = false;
            coordinator
                .node_shards
                .insert(id.clone(), registration.shard_id);
            coordinator.shards[registration.shard_id].register(node.clone())?;
            Arc::make_mut(&mut coordinator.snapshot).update_node(node);
        }
        for (id, environment) in &saved.environments {
            if environment.resources_held() {
                let shard = environment.assignment.shard_id;
                coordinator.shards[shard].restore(&environment.spec, &environment.assignment)?;
                coordinator
                    .requests
                    .insert(id.clone(), (shard, environment.spec.clone()));
                Arc::make_mut(&mut coordinator.snapshot).place(PlacedEnvironment {
                    spec: environment.spec.clone(),
                    node_id: environment.assignment.node_id.clone(),
                });
            } else {
                coordinator.retired.insert(id.clone());
            }
        }
        Ok(coordinator)
    }
    pub fn new(shard_count: usize, placement: Placement) -> Result<Self> {
        Self::with_framework(shard_count, Framework::builtin(placement))
    }

    pub fn with_framework(shard_count: usize, framework: Framework) -> Result<Self> {
        Self::with_framework_config(shard_count, framework, SchedulerConfig::default())
    }
    pub fn with_config(
        shard_count: usize,
        placement: Placement,
        config: SchedulerConfig,
    ) -> Result<Self> {
        Self::with_framework_config(shard_count, Framework::builtin(placement), config)
    }
    pub fn with_framework_config(
        shard_count: usize,
        framework: Framework,
        config: SchedulerConfig,
    ) -> Result<Self> {
        if config.max_attempts == 0 || config.max_duration.is_zero() || config.mutation_history == 0
        {
            return Err(Error::Invalid(
                "round and journal limits must be positive".into(),
            ));
        }
        if shard_count == 0 {
            return Err(Error::Invalid("shard count must be positive".into()));
        }
        let framework = Arc::new(framework);
        Ok(Self {
            shards: (0..shard_count)
                .map(|_| ShardScheduler::new(framework.clone()))
                .collect(),
            node_shards: BTreeMap::new(),
            requests: BTreeMap::new(),
            next_shard: 0,
            next_node_shard: 0,
            generation: 0,
            generation_source: None,
            journal: MutationJournal::new(config.mutation_history),
            config,
            snapshot: Arc::new(Snapshot::default()),
            ready: VecDeque::new(),
            retired: BTreeSet::new(),
            volume_placement: BTreeMap::new(),
        })
    }

    pub fn register(&mut self, node: Node) -> Result<usize> {
        if node.id.trim().is_empty() {
            return Err(Error::Invalid("node id is required".into()));
        }
        node.validate()?;
        let shard = if let Some(shard) = self.node_shards.get(&node.id) {
            *shard
        } else {
            let shard = (0..self.shards.len())
                .map(|offset| (self.next_node_shard + offset) % self.shards.len())
                .min_by_key(|id| self.shards[*id].node_count())
                .expect("Coordinator is constructed with at least one scheduling shard");
            self.next_node_shard = (shard + 1) % self.shards.len();
            self.node_shards.insert(node.id.clone(), shard);
            shard
        };
        self.shards[shard].register(node.clone())?;
        Arc::make_mut(&mut self.snapshot).update_node(node.clone());
        self.journal.record(&node.id);
        self.wake_pending(None);
        Ok(shard)
    }

    pub(crate) fn register_at(&mut self, node: Node, shard: usize) -> Result<usize> {
        if shard >= self.shards.len()
            || self
                .node_shards
                .get(&node.id)
                .is_some_and(|old| *old != shard)
        {
            return Err(Error::Conflict);
        }
        self.node_shards.insert(node.id.clone(), shard);
        self.register(node)
    }

    /// Global chooses the shard; queued Snapshots can move when a different
    /// shard has eligible capacity. The shard still owns final reservation.
    pub fn submit_recovery(&mut self, spec: EnvironmentSpec) -> Result<usize> {
        self.retired.remove(&spec.id);
        if !self.requests.contains_key(&spec.id) {
            // Global still rotates; skip shards without any live node.
            for offset in 0..self.shards.len() {
                let shard = (self.next_shard + offset) % self.shards.len();
                if self
                    .snapshot
                    .nodes()
                    .values()
                    .any(|n| n.available && self.node_shards.get(&n.id) == Some(&shard))
                {
                    self.next_shard = shard;
                    break;
                }
            }
        }
        self.submit(spec)
    }
    pub fn submit(&mut self, spec: EnvironmentSpec) -> Result<usize> {
        spec.validate()?;
        if self.retired.contains(&spec.id) {
            return Err(Error::Conflict);
        }
        if let Some((shard, existing)) = self.requests.get(&spec.id) {
            return if *existing == spec {
                Ok(*shard)
            } else {
                Err(Error::Conflict)
            };
        }
        let preferred = if spec.snapshot_id.is_some() {
            self.candidate_shard(&spec, &BTreeSet::new(), None)?
        } else {
            None
        };
        let shard = preferred.unwrap_or_else(|| {
            // Affinity-selected requests do not advance ordinary shard rotation.
            let shard = self.next_shard;
            self.next_shard = (shard + 1) % self.shards.len();
            shard
        });
        self.requests.insert(spec.id.clone(), (shard, spec.clone()));
        self.shards[shard].enqueue(spec);
        self.wake(shard);
        Ok(shard)
    }

    /// Use the same hard filters and score for Snapshot affinity and queued
    /// Volume requests that gain accessible capacity on another shard.
    fn candidate_shard(
        &mut self,
        spec: &EnvironmentSpec,
        excluded: &BTreeSet<String>,
        skip: Option<usize>,
    ) -> Result<Option<usize>> {
        let mut best: Option<(u64, String, usize)> = None;
        for (id, shard) in self.shards.iter_mut().enumerate() {
            if skip == Some(id) {
                continue;
            }
            if let Some((score, node)) = shard.best_candidate(
                spec,
                &self.snapshot,
                excluded,
                self.volume_placement.get(&spec.id),
            )? {
                if best.as_ref().is_none_or(|(old, old_node, _)| {
                    score > *old || (score == *old && node < *old_node)
                }) {
                    best = Some((score, node, id));
                }
            }
        }
        Ok(best.map(|(_, _, id)| id))
    }

    fn defer_or_transfer(&mut self, shard: usize, entry: queue::Entry) -> Result<()> {
        if entry.spec.snapshot_id.is_none() && !self.volume_placement.contains_key(&entry.spec.id) {
            self.shards[shard].defer(entry);
            return Ok(());
        }
        let excluded = self.shards[shard].exclusions(&entry.spec.id);
        let target = match self.candidate_shard(&entry.spec, &excluded, Some(shard)) {
            Ok(target) => target,
            Err(error) => {
                self.shards[shard].defer(entry);
                return Err(error);
            }
        };
        let Some(target) = target else {
            self.shards[shard].defer(entry);
            return Ok(());
        };
        let Some((owner, _)) = self.requests.get_mut(&entry.spec.id) else {
            self.shards[shard].defer(entry);
            return Err(Error::Conflict);
        };
        *owner = target;
        self.shards[shard].forget_queued(&entry.spec.id);
        self.shards[target].accept_transfer(entry, excluded);
        self.wake(target);
        Ok(())
    }

    pub fn set_volume_placement(&mut self, id: &str, placement: VolumePlacement) {
        self.volume_placement.insert(id.to_owned(), placement);
        if let Some((shard, _)) = self.requests.get(id) {
            self.wake(*shard);
        }
    }

    /// Inspect current placement constraints, including held reservations.
    /// This does not cancel requests or imply that execution has not started.
    pub fn scheduling_failure(
        &self,
        spec: &EnvironmentSpec,
    ) -> Result<adx_core::scheduling::SchedulingFailure> {
        spec.validate()?;
        let mut failure = adx_core::scheduling::SchedulingFailure {
            snapshot_revision: self.snapshot.revision,
            requested: spec.resources,
            ..Default::default()
        };
        for shard in &self.shards {
            shard.diagnose(
                spec,
                &self.snapshot,
                &mut failure,
                self.volume_placement.get(&spec.id),
            )?;
        }
        if failure.node_count == 0 {
            failure.rejections.insert("no_workers".into(), 1);
        }
        Ok(failure)
    }

    /// Remove an unassigned request after its central queue deadline.
    /// Once a shard has reserved an Assignment, lifecycle completion owns cleanup.
    pub fn cancel_pending(&mut self, id: &str) -> bool {
        let Some((shard, _)) = self.requests.get(id) else {
            return false;
        };
        if self.shards[*shard].assignment(id).is_some() {
            return false;
        }
        for shard in &mut self.shards {
            shard.forget_queued(id);
        }
        self.requests.remove(id);
        self.volume_placement.remove(id);
        true
    }

    /// Apply a confirmed Redis owner before exposing it or scheduling further work.
    /// Also removes an identical queued center request; replay never double charges.
    pub fn accept_claim(&mut self, spec: &EnvironmentSpec, assignment: &Assignment) -> Result<()> {
        spec.validate()?;
        if assignment.environment_id != spec.id
            || assignment.generation == 0
            || self.node_shards.get(&assignment.node_id) != Some(&assignment.shard_id)
        {
            return Err(Error::Conflict);
        }
        if let Some((shard, existing)) = self.requests.get(&spec.id) {
            if existing != spec {
                return Err(Error::Conflict);
            }
            if let Some(old) = self.shards[*shard].assignment(&spec.id).cloned() {
                if old == *assignment {
                    self.generation = self.generation.max(assignment.generation);
                    return Ok(());
                }
                self.release(&old)?;
            }
        }
        for shard in &mut self.shards {
            shard.forget_queued(&spec.id);
        }
        self.requests.remove(&spec.id);
        self.restore_assignment(spec, assignment)?;
        self.generation = self.generation.max(assignment.generation);
        Ok(())
    }
    /// Apply an authoritative result that no longer holds resources. Includes
    /// late commits first observed through a repeated ownership claim.
    pub fn retire_claim(&mut self, id: &str) -> Result<()> {
        let assignment = self
            .requests
            .get(id)
            .and_then(|(shard, _)| self.shards[*shard].assignment(id))
            .cloned();
        if let Some(assignment) = assignment {
            self.release(&assignment)?;
        }
        for shard in &mut self.shards {
            shard.forget_queued(id);
        }
        self.requests.remove(id);
        self.retired.insert(id.to_string());
        Ok(())
    }
    /// Clear the retired scheduling marker after storage confirms explicit deletion.
    pub(crate) fn forget_deleted(&mut self, id: &str) -> Result<()> {
        if self.requests.contains_key(id) || self.snapshot.environments().contains_key(id) {
            return Err(Error::Conflict);
        }
        self.retired.remove(id);
        Ok(())
    }
    pub fn local_candidate(
        &self,
        spec: &EnvironmentSpec,
        node: &str,
        devices: &[adx_core::scheduling::DeviceAllocation],
    ) -> Result<bool> {
        if self
            .volume_placement
            .get(&spec.id)
            .is_some_and(|placement| !placement.allows(node))
        {
            return Ok(false);
        }
        let Some(shard) = self.node_shards.get(node) else {
            return Ok(false);
        };
        self.shards[*shard].local_candidate(spec, node, devices, &self.snapshot)
    }
    pub fn snapshot(&self) -> Arc<Snapshot> {
        self.snapshot.clone()
    }
    pub fn node_resources(&self, id: &str) -> Option<(adx_core::Resources, adx_core::Resources)> {
        let shard = *self.node_shards.get(id)?;
        self.shards.get(shard)?.node_resources(id)
    }
    pub fn node_devices(
        &self,
        id: &str,
    ) -> Option<(
        Vec<adx_core::scheduling::Device>,
        Vec<adx_core::scheduling::Device>,
    )> {
        let shard = *self.node_shards.get(id)?;
        self.shards.get(shard)?.node_devices(id)
    }
    pub fn pending(&self, shard: usize) -> Result<usize> {
        self.shards
            .get(shard)
            .map(ShardScheduler::pending)
            .ok_or(Error::NotFound)
    }
    pub fn pending_requests(&self) -> Vec<PendingRequest> {
        let mut requests = self
            .shards
            .iter()
            .flat_map(ShardScheduler::pending_requests)
            .map(|entry| PendingRequest {
                spec: entry.spec.clone(),
                enqueue_time_millis: entry.enqueue_time_millis,
            })
            .collect::<Vec<_>>();
        requests.sort_by(|left, right| {
            left.enqueue_time_millis
                .cmp(&right.enqueue_time_millis)
                .then_with(|| left.spec.id.cmp(&right.spec.id))
        });
        requests
    }
    pub fn stats(&self, shard: usize) -> Result<SchedulingStats> {
        self.shards
            .get(shard)
            .map(|d| d.stats)
            .ok_or(Error::NotFound)
    }
    fn wake(&mut self, shard: usize) {
        if !self.ready.contains(&shard) {
            self.ready.push_back(shard);
        }
    }
    fn wake_pending(&mut self, exclude: Option<usize>) {
        for id in 0..self.shards.len() {
            if Some(id) != exclude && self.shards[id].pending() > 0 {
                self.wake(id);
            }
        }
    }
    /// Coalesced event-loop interface. Every wake observes an already-published
    /// snapshot, including status-only node changes. Empty/stalled queues sleep.
    pub fn take_ready_shard(&mut self) -> Option<usize> {
        self.ready.pop_front()
    }
    /// A returned reservation must be kept until the node confirms cleanup.
    pub fn schedule(&mut self, shard: usize) -> Result<Option<Assignment>> {
        let mut round = self.run_round(shard, 1)?;
        if let Some(error) = round.error {
            return Err(error);
        }
        Ok(round.assignments.pop())
    }
    pub fn schedule_round(&mut self, shard: usize) -> Result<RoundOutcome> {
        self.run_round(shard, usize::MAX)
    }
    fn run_round(&mut self, shard: usize, max_assignments: usize) -> Result<RoundOutcome> {
        let d = self.shards.get_mut(shard).ok_or(Error::NotFound)?;
        self.ready.retain(|id| *id != shard);
        d.begin_round();
        let started = Instant::now();
        // Roots pin the base for this bounded round; subsequent writes are a
        // structurally shared reservation overlay visible to the next request.
        let base = self.snapshot.clone();
        let mut outcome = RoundOutcome {
            snapshot_revision: base.revision,
            ..Default::default()
        };
        while self.shards[shard].remaining_sweep() {
            if outcome.attempted >= self.config.max_attempts
                || outcome.assignments.len() >= max_assignments
                || started.elapsed() >= self.config.max_duration
            {
                outcome.yielded = true;
                break;
            }
            let allocated = match &self.generation_source {
                Some(source) => source(),
                None => self.generation.checked_add(1).ok_or(Error::Conflict),
            };
            let generation = match allocated {
                Ok(value) => value,
                Err(error) => {
                    outcome.error = Some(error);
                    break;
                }
            };
            outcome.attempted += 1;
            match self.shards[shard].attempt(
                shard,
                generation,
                &self.snapshot,
                &self.journal,
                &self.config,
                &self.volume_placement,
            ) {
                Ok(Attempt::Assigned(assignment, spec)) => {
                    self.generation = generation;
                    Arc::make_mut(&mut self.snapshot).place(PlacedEnvironment {
                        spec,
                        node_id: assignment.node_id.clone(),
                    });
                    self.journal.record(&assignment.node_id);
                    self.wake_pending(Some(shard));
                    outcome.assignments.push(assignment);
                }
                Ok(Attempt::Deferred(entry)) => {
                    if let Err(error) = self.defer_or_transfer(shard, entry) {
                        outcome.error = Some(error);
                        break;
                    }
                }
                Ok(Attempt::Empty) => (),
                Err(error) => {
                    outcome.error = Some(error);
                    break;
                }
            }
        }
        if outcome.error.is_none()
            && (outcome.yielded
                || self.shards[shard].has_deferred_arrivals()
                || (!outcome.assignments.is_empty() && self.shards[shard].pending() > 0))
        {
            self.wake(shard);
        }
        Ok(outcome)
    }

    /// Reconsider a candidate only after a fresh ready report.
    pub(crate) fn reconsider_node(&mut self, node: &str) {
        for shard in &mut self.shards {
            shard.reconsider_node(node);
        }
        self.wake_pending(None);
    }
    /// Only after the owning node rejects before execution or confirms cleanup.
    /// Retains request identity, rolls back the old reservation and excludes
    /// rejected nodes for this request. A live Environment must not use this path.
    pub fn retry(&mut self, assignment: &Assignment) -> Result<()> {
        let spec = self
            .requests
            .get(&assignment.environment_id)
            .ok_or(Error::NotFound)?
            .1
            .clone();
        self.shards
            .get_mut(assignment.shard_id)
            .ok_or(Error::NotFound)?
            .retry(assignment, spec)?;
        Arc::make_mut(&mut self.snapshot).remove(&assignment.environment_id);
        self.journal.record(&assignment.node_id);
        self.wake_pending(None);
        Ok(())
    }
    /// Apply a committed same-node resume to the incremental scheduling view.
    pub fn restore_assignment(
        &mut self,
        spec: &EnvironmentSpec,
        assignment: &Assignment,
    ) -> Result<()> {
        if self.requests.contains_key(&spec.id)
            || self.node_shards.get(&assignment.node_id) != Some(&assignment.shard_id)
        {
            return Err(Error::Conflict);
        }
        self.shards
            .get_mut(assignment.shard_id)
            .ok_or(Error::NotFound)?
            .restore(spec, assignment)?;
        self.requests
            .insert(spec.id.clone(), (assignment.shard_id, spec.clone()));
        Arc::make_mut(&mut self.snapshot).place(PlacedEnvironment {
            spec: spec.clone(),
            node_id: assignment.node_id.clone(),
        });
        self.retired.remove(&spec.id);
        self.journal.record(&assignment.node_id);
        self.wake_pending(None);
        Ok(())
    }
    pub fn release(&mut self, assignment: &Assignment) -> Result<()> {
        self.shards
            .get_mut(assignment.shard_id)
            .ok_or(Error::NotFound)?
            .release(assignment)?;
        self.requests.remove(&assignment.environment_id);
        Arc::make_mut(&mut self.snapshot).remove(&assignment.environment_id);
        self.journal.record(&assignment.node_id);
        self.wake_pending(None);
        Ok(())
    }
}
