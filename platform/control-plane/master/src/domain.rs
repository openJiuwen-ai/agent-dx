use crate::{
    journal::MutationJournal,
    queue::{Entry, TenantQueue},
    SchedulerConfig,
};
use adx_core::scheduling::{select_devices, Device, DeviceLedger};
use adx_core::{Assignment, Error, InstanceSpec, ResourceLedger, Result};
use adx_scheduling::{query::Prepared, Candidate, Framework, Node, Snapshot};
use std::{
    cmp::Reverse,
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::Arc,
};

struct NodeState {
    node: Node,
    scalar: ResourceLedger,
    devices: DeviceLedger,
    free_devices: Vec<Device>,
}
/// IDs, tenant and priority control queue order, not placement in the eligible
/// builtin scalar profile. They stay on each queued request, not its computation
/// signature. Execution environment and scalar demand remain distinct.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Signature {
    image: String,
    runtime: String,
    cpu: u64,
    memory: u64,
}
impl From<&InstanceSpec> for Signature {
    fn from(r: &InstanceSpec) -> Self {
        Self {
            image: r.image.clone(),
            runtime: r.runtime.clone(),
            cpu: r.resources.cpu_millis,
            memory: r.resources.memory_bytes,
        }
    }
}
#[derive(Default)]
struct Candidates {
    cursor: u64,
    ranked: BTreeSet<(Reverse<u64>, String)>,
    scores: BTreeMap<String, u64>,
}
impl Candidates {
    fn update(&mut self, id: &str, score: Option<u64>) {
        if let Some(old) = self.scores.remove(id) {
            self.ranked.remove(&(Reverse(old), id.into()));
        }
        if let Some(score) = score {
            self.scores.insert(id.into(), score);
            self.ranked.insert((Reverse(score), id.into()));
        }
    }
}
#[derive(Clone, Copy, Debug, Default)]
pub struct SchedulingStats {
    pub candidate_evaluations: u64,
    pub cache_hits: u64,
    pub cache_builds: u64,
    pub journal_rebuilds: u64,
    pub reconciled_nodes: u64,
    pub query_preparations: u64,
    pub attempts: u64,
}
pub(crate) struct Domain {
    nodes: BTreeMap<String, NodeState>,
    queue: TenantQueue,
    deferred: TenantQueue,
    deferred_arrivals: bool,
    sequence: u64,
    assigned: BTreeMap<String, Assignment>,
    framework: Arc<Framework>,
    cache: BTreeMap<Signature, Candidates>,
    cache_order: VecDeque<Signature>,
    excluded: BTreeMap<String, BTreeSet<String>>,
    pub stats: SchedulingStats,
}
impl Domain {
    pub fn new(framework: Arc<Framework>) -> Self {
        Self {
            nodes: BTreeMap::new(),
            queue: TenantQueue::default(),
            deferred: TenantQueue::default(),
            deferred_arrivals: false,
            sequence: 0,
            assigned: BTreeMap::new(),
            framework,
            cache: BTreeMap::new(),
            cache_order: VecDeque::new(),
            excluded: BTreeMap::new(),
            stats: SchedulingStats::default(),
        }
    }
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }
    pub fn restore(&mut self, spec: &InstanceSpec, assignment: &Assignment) -> Result<()> {
        if self.assigned.contains_key(&spec.id) {
            return Err(Error::Conflict);
        }
        let state = self
            .nodes
            .get_mut(&assignment.node_id)
            .ok_or(Error::NotFound)?;
        let mut scalar = state.scalar.clone();
        let mut devices = state.devices.clone();
        scalar.restore(&spec.id, spec.resources)?;
        devices.restore(&spec.id, &assignment.devices)?;
        state.scalar = scalar;
        state.devices = devices;
        state.free_devices = state.devices.available();
        self.assigned.insert(spec.id.clone(), assignment.clone());
        Ok(())
    }
    pub fn pending(&self) -> usize {
        self.queue.len() + self.deferred.len()
    }
    pub fn remaining_sweep(&self) -> bool {
        !self.queue.is_empty()
    }
    pub fn has_deferred_arrivals(&self) -> bool {
        self.deferred_arrivals
    }
    pub fn begin_round(&mut self) {
        if self.queue.is_empty() {
            std::mem::swap(&mut self.queue, &mut self.deferred);
            self.deferred_arrivals = false;
        }
    }
    pub fn register(&mut self, node: Node) -> Result<()> {
        node.validate()?;
        if let Some(state) = self.nodes.get_mut(&node.id) {
            state.devices.update(node.devices.clone())?;
            state.free_devices = state.devices.available();
            state.scalar.set_capacity(node.capacity);
            state.node = node;
        } else {
            let mut devices = DeviceLedger::default();
            devices.update(node.devices.clone())?;
            let free_devices = devices.available();
            self.nodes.insert(
                node.id.clone(),
                NodeState {
                    scalar: ResourceLedger::new(node.capacity),
                    node,
                    devices,
                    free_devices,
                },
            );
        }
        Ok(())
    }
    pub fn enqueue(&mut self, request: InstanceSpec) {
        // Start the next sweep before adding new work to an exhausted one.
        // Deferred tickets must participate in priority/FIFO ordering.
        self.begin_round();
        // Once a request has been deferred, keep the current sweep finite.
        // Continuous arrivals must not postpone retrying recovered capacity forever.
        // The next sweep merges these tickets with deferred work using the same
        // tenant rotation and original priority/FIFO sequence.
        let queue = if self.deferred.is_empty() {
            &mut self.queue
        } else {
            self.deferred_arrivals = true;
            &mut self.deferred
        };
        queue.restore(Entry {
            sequence: self.sequence,
            spec: request,
        });
        self.sequence = self
            .sequence
            .checked_add(1)
            .expect("queue sequence exhausted");
    }
    fn select(
        &mut self,
        request: &InstanceSpec,
        snapshot: &Snapshot,
        journal: &MutationJournal,
        config: &SchedulerConfig,
    ) -> Result<Option<String>> {
        if config.candidate_cache_entries == 0
            || !self.framework.supports_aggregation(request, snapshot)
        {
            self.stats.query_preparations += 1;
            let prepared = Prepared::new(request, snapshot);
            let mut best: Option<(u64, String)> = None;
            for state in self.nodes.values() {
                if self
                    .excluded
                    .get(&request.id)
                    .is_some_and(|ids| ids.contains(&state.node.id))
                {
                    continue;
                }
                self.stats.candidate_evaluations += 1;
                let candidate = Candidate {
                    node: &state.node,
                    available: state.scalar.available(),
                    devices: &state.free_devices,
                    snapshot,
                    prepared: Some(&prepared),
                };
                if let Some(score) = self.framework.evaluate(request, &candidate)? {
                    if best.as_ref().is_none_or(|(old, id)| {
                        score > *old || (score == *old && state.node.id < *id)
                    }) {
                        best = Some((score, state.node.id.clone()));
                    }
                }
            }
            return Ok(best.map(|(_, id)| id));
        }
        let key = Signature::from(request);
        let updates = if let Some(cached) = self.cache.get(&key) {
            self.stats.cache_hits += 1;
            let changes = journal.since(cached.cursor);
            if changes.is_none() {
                self.stats.journal_rebuilds += 1;
            }
            changes
        } else {
            None
        };
        if !self.cache.contains_key(&key) {
            while self.cache.len() >= config.candidate_cache_entries {
                if let Some(old) = self.cache_order.pop_front() {
                    self.cache.remove(&old);
                }
            }
            self.cache_order.push_back(key.clone());
        }
        let cached = self.cache.entry(key).or_default();
        let ids = if let Some(updates) = updates {
            updates
        } else {
            self.stats.cache_builds += 1;
            *cached = Candidates::default();
            self.nodes.keys().cloned().collect()
        };
        let prepared = Prepared::default(); // capability gate excludes every peer/topology policy
        for id in ids {
            let Some(state) = self.nodes.get(&id) else {
                continue;
            };
            self.stats.candidate_evaluations += 1;
            self.stats.reconciled_nodes += 1;
            let score = self.framework.evaluate(
                request,
                &Candidate {
                    node: &state.node,
                    available: state.scalar.available(),
                    devices: &state.free_devices,
                    snapshot,
                    prepared: Some(&prepared),
                },
            )?;
            cached.update(&id, score);
        }
        cached.cursor = journal.sequence();
        // Exclusions are request-local, while ranking only depends on the
        // eligible scalar signature. Never delete a failed node from the
        // shared cache: other requests must still be able to select it.
        Ok(cached
            .ranked
            .iter()
            .find(|(_, id)| {
                !self
                    .excluded
                    .get(&request.id)
                    .is_some_and(|ids| ids.contains(id))
            })
            .map(|(_, id)| id.clone()))
    }
    /// One queue item, never a whole-queue drain. Failed fits remain deferred
    /// until the sweep completes; tickets preserve same-priority FIFO on retry.
    pub fn attempt(
        &mut self,
        domain_id: usize,
        generation: u64,
        snapshot: &Snapshot,
        journal: &MutationJournal,
        config: &SchedulerConfig,
    ) -> Result<Option<(Assignment, InstanceSpec)>> {
        let Some(entry) = self.queue.pop_entry() else {
            return Ok(None);
        };
        self.stats.attempts += 1;
        let best = match self.select(&entry.spec, snapshot, journal, config) {
            Ok(best) => best,
            Err(e) => {
                self.queue.restore(entry);
                return Err(e);
            }
        };
        let Some(node_id) = best else {
            self.deferred.restore(entry);
            return Ok(None);
        };
        let state = self.nodes.get_mut(&node_id).expect("selected node exists");
        let r = &entry.spec;
        let reserved = (|| {
            let allocations = select_devices(&r.scheduling.devices, &state.free_devices)?;
            state.scalar.reserve(&r.id, r.resources)?;
            if let Err(e) = state
                .devices
                .reserve(&r.id, &r.scheduling.devices, &allocations)
            {
                state.scalar.release(&r.id).expect("just reserved");
                return Err(e);
            }
            Ok(allocations)
        })();
        let devices = match reserved {
            Ok(d) => d,
            Err(e) => {
                self.queue.restore(entry);
                return Err(e);
            }
        };
        if !devices.is_empty() {
            state.free_devices = state.devices.available();
        }
        let assignment = Assignment {
            instance_id: r.id.clone(),
            node_id,
            domain_id,
            generation,
            devices,
        };
        self.assigned.insert(r.id.clone(), assignment.clone());
        Ok(Some((assignment, entry.spec)))
    }
    pub fn retry(&mut self, assignment: &Assignment, spec: InstanceSpec) -> Result<()> {
        let mut excluded = self
            .excluded
            .get(&assignment.instance_id)
            .cloned()
            .unwrap_or_default();
        excluded.insert(assignment.node_id.clone());
        self.release(assignment)?;
        self.excluded
            .insert(assignment.instance_id.clone(), excluded);
        self.enqueue(spec);
        Ok(())
    }
    pub fn release(&mut self, assignment: &Assignment) -> Result<()> {
        if self.assigned.get(&assignment.instance_id) != Some(assignment) {
            return Err(Error::Conflict);
        }
        let state = self
            .nodes
            .get_mut(&assignment.node_id)
            .ok_or(Error::NotFound)?;
        state.scalar.release(&assignment.instance_id)?;
        state.devices.release(&assignment.instance_id)?;
        if !assignment.devices.is_empty() {
            state.free_devices = state.devices.available();
        }
        self.assigned.remove(&assignment.instance_id);
        self.excluded.remove(&assignment.instance_id);
        Ok(())
    }
}
