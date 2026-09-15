//! Global and embedded Domain scheduling.

pub mod auth;
mod domain;
mod journal;
mod queue;
pub mod routes;
pub mod rpc;
pub mod storage;

use adx_core::{Assignment, Error, InstanceSpec, Result};
use adx_scheduling::{Framework, PlacedInstance, Snapshot};
pub use adx_scheduling::{Node, Placement};
use domain::Domain;
pub use domain::SchedulingStats;
use journal::MutationJournal;
pub use queue::TenantQueue;
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
pub struct Master {
    domains: Vec<Domain>,
    node_domains: BTreeMap<String, usize>,
    requests: BTreeMap<String, (usize, InstanceSpec)>,
    next_domain: usize,
    next_node_domain: usize,
    generation: u64,
    config: SchedulerConfig,
    snapshot: Arc<Snapshot>,
    journal: MutationJournal,
    ready: VecDeque<usize>,
    retired: BTreeSet<String>,
}

impl Master {
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
        let mut master = Self::with_config(saved.domain_count, placement, config)?;
        master.generation = saved.generation;
        for (id, registration) in &saved.nodes {
            let mut node = registration.node.clone();
            node.available = false;
            master
                .node_domains
                .insert(id.clone(), registration.domain_id);
            master.domains[registration.domain_id].register(node.clone())?;
            Arc::make_mut(&mut master.snapshot).update_node(node);
        }
        for (id, instance) in &saved.instances {
            if instance.resources_held() {
                let domain = instance.assignment.domain_id;
                master.domains[domain].restore(&instance.spec, &instance.assignment)?;
                master
                    .requests
                    .insert(id.clone(), (domain, instance.spec.clone()));
                Arc::make_mut(&mut master.snapshot).place(PlacedInstance {
                    spec: instance.spec.clone(),
                    node_id: instance.assignment.node_id.clone(),
                });
            } else {
                master.retired.insert(id.clone());
            }
        }
        Ok(master)
    }
    pub fn new(domain_count: usize, placement: Placement) -> Result<Self> {
        Self::with_framework(domain_count, Framework::builtin(placement))
    }

    pub fn with_framework(domain_count: usize, framework: Framework) -> Result<Self> {
        Self::with_framework_config(domain_count, framework, SchedulerConfig::default())
    }
    pub fn with_config(
        domain_count: usize,
        placement: Placement,
        config: SchedulerConfig,
    ) -> Result<Self> {
        Self::with_framework_config(domain_count, Framework::builtin(placement), config)
    }
    pub fn with_framework_config(
        domain_count: usize,
        framework: Framework,
        config: SchedulerConfig,
    ) -> Result<Self> {
        if config.max_attempts == 0 || config.max_duration.is_zero() || config.mutation_history == 0
        {
            return Err(Error::Invalid(
                "round and journal limits must be positive".into(),
            ));
        }
        if domain_count == 0 {
            return Err(Error::Invalid("domain count must be positive".into()));
        }
        let framework = Arc::new(framework);
        Ok(Self {
            domains: (0..domain_count)
                .map(|_| Domain::new(framework.clone()))
                .collect(),
            node_domains: BTreeMap::new(),
            requests: BTreeMap::new(),
            next_domain: 0,
            next_node_domain: 0,
            generation: 0,
            journal: MutationJournal::new(config.mutation_history),
            config,
            snapshot: Arc::new(Snapshot::default()),
            ready: VecDeque::new(),
            retired: BTreeSet::new(),
        })
    }

    pub fn register(&mut self, node: Node) -> Result<usize> {
        if node.id.trim().is_empty() {
            return Err(Error::Invalid("node id is required".into()));
        }
        node.validate()?;
        let domain = if let Some(domain) = self.node_domains.get(&node.id) {
            *domain
        } else {
            let domain = (0..self.domains.len())
                .map(|offset| (self.next_node_domain + offset) % self.domains.len())
                .min_by_key(|id| self.domains[*id].node_count())
                .unwrap();
            self.next_node_domain = (domain + 1) % self.domains.len();
            self.node_domains.insert(node.id.clone(), domain);
            domain
        };
        self.domains[domain].register(node.clone())?;
        Arc::make_mut(&mut self.snapshot).update_node(node.clone());
        self.journal.record(&node.id);
        self.wake_pending(None);
        Ok(domain)
    }

    /// Global chooses the domain only. Unscheduled work stays in its Domain queue.
    pub fn submit(&mut self, spec: InstanceSpec) -> Result<usize> {
        spec.validate()?;
        if self.retired.contains(&spec.id) {
            return Err(Error::Conflict);
        }
        if let Some((domain, existing)) = self.requests.get(&spec.id) {
            return if *existing == spec {
                Ok(*domain)
            } else {
                Err(Error::Conflict)
            };
        }
        let domain = self.next_domain;
        self.next_domain = (domain + 1) % self.domains.len();
        self.requests
            .insert(spec.id.clone(), (domain, spec.clone()));
        self.domains[domain].enqueue(spec);
        self.wake(domain);
        Ok(domain)
    }

    pub fn snapshot(&self) -> Arc<Snapshot> {
        self.snapshot.clone()
    }
    pub fn pending(&self, domain: usize) -> Result<usize> {
        self.domains
            .get(domain)
            .map(Domain::pending)
            .ok_or(Error::NotFound)
    }
    pub fn stats(&self, domain: usize) -> Result<SchedulingStats> {
        self.domains
            .get(domain)
            .map(|d| d.stats)
            .ok_or(Error::NotFound)
    }
    fn wake(&mut self, domain: usize) {
        if !self.ready.contains(&domain) {
            self.ready.push_back(domain);
        }
    }
    fn wake_pending(&mut self, exclude: Option<usize>) {
        for id in 0..self.domains.len() {
            if Some(id) != exclude && self.domains[id].pending() > 0 {
                self.wake(id);
            }
        }
    }
    /// Coalesced event-loop interface. Every wake observes an already-published
    /// snapshot, including status-only node changes. Empty/stalled queues sleep.
    pub fn take_ready_domain(&mut self) -> Option<usize> {
        self.ready.pop_front()
    }
    /// A returned reservation must be kept until the node confirms cleanup.
    pub fn schedule(&mut self, domain: usize) -> Result<Option<Assignment>> {
        let mut round = self.run_round(domain, 1)?;
        if let Some(error) = round.error {
            return Err(error);
        }
        Ok(round.assignments.pop())
    }
    pub fn schedule_round(&mut self, domain: usize) -> Result<RoundOutcome> {
        self.run_round(domain, usize::MAX)
    }
    fn run_round(&mut self, domain: usize, max_assignments: usize) -> Result<RoundOutcome> {
        let d = self.domains.get_mut(domain).ok_or(Error::NotFound)?;
        self.ready.retain(|id| *id != domain);
        d.begin_round();
        let started = Instant::now();
        // Roots pin the base for this bounded round; subsequent writes are a
        // structurally shared reservation overlay visible to the next request.
        let base = self.snapshot.clone();
        let mut outcome = RoundOutcome {
            snapshot_revision: base.revision,
            ..Default::default()
        };
        while self.domains[domain].remaining_sweep() {
            if outcome.attempted >= self.config.max_attempts
                || outcome.assignments.len() >= max_assignments
                || started.elapsed() >= self.config.max_duration
            {
                outcome.yielded = true;
                break;
            }
            let Some(generation) = self.generation.checked_add(1) else {
                outcome.error = Some(Error::Conflict);
                break;
            };
            outcome.attempted += 1;
            match self.domains[domain].attempt(
                domain,
                generation,
                &self.snapshot,
                &self.journal,
                &self.config,
            ) {
                Ok(Some((assignment, spec))) => {
                    self.generation = generation;
                    Arc::make_mut(&mut self.snapshot).place(PlacedInstance {
                        spec,
                        node_id: assignment.node_id.clone(),
                    });
                    self.journal.record(&assignment.node_id);
                    self.wake_pending(Some(domain));
                    outcome.assignments.push(assignment);
                }
                Ok(None) => (),
                Err(error) => {
                    outcome.error = Some(error);
                    break;
                }
            }
        }
        if outcome.error.is_none()
            && (outcome.yielded
                || (!outcome.assignments.is_empty() && self.domains[domain].pending() > 0))
        {
            self.wake(domain);
        }
        Ok(outcome)
    }

    /// Only after the owning node rejects before execution or confirms cleanup.
    /// Retains request identity, rolls back the old reservation and excludes
    /// rejected nodes for this request. A live Instance must not use this path.
    pub fn retry(&mut self, assignment: &Assignment) -> Result<()> {
        let spec = self
            .requests
            .get(&assignment.instance_id)
            .ok_or(Error::NotFound)?
            .1
            .clone();
        self.domains
            .get_mut(assignment.domain_id)
            .ok_or(Error::NotFound)?
            .retry(assignment, spec)?;
        Arc::make_mut(&mut self.snapshot).remove(&assignment.instance_id);
        self.journal.record(&assignment.node_id);
        self.wake_pending(None);
        Ok(())
    }
    pub fn release(&mut self, assignment: &Assignment) -> Result<()> {
        self.domains
            .get_mut(assignment.domain_id)
            .ok_or(Error::NotFound)?
            .release(assignment)?;
        self.requests.remove(&assignment.instance_id);
        Arc::make_mut(&mut self.snapshot).remove(&assignment.instance_id);
        self.journal.record(&assignment.node_id);
        self.wake_pending(None);
        Ok(())
    }
}
