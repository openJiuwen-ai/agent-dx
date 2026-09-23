use adx_core::EnvironmentSpec;
use std::{
    cmp::Reverse,
    collections::{BTreeMap, VecDeque},
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Clone)]
pub(crate) struct Entry {
    pub sequence: u64,
    pub enqueue_time_millis: u64,
    pub spec: EnvironmentSpec,
}
impl Entry {
    pub(crate) fn new(sequence: u64, spec: EnvironmentSpec) -> Self {
        Self {
            sequence,
            enqueue_time_millis: unix_millis(),
            spec,
        }
    }
}
#[derive(Default)]
pub struct TenantQueue {
    tenants: VecDeque<String>,
    queues: BTreeMap<String, BTreeMap<(Reverse<i32>, u64), Entry>>,
    len: usize,
    sequence: u64,
}
impl TenantQueue {
    pub fn push(&mut self, request: EnvironmentSpec) {
        let sequence = self.sequence;
        self.sequence = self
            .sequence
            .checked_add(1)
            .expect("queue sequence exhausted");
        self.restore(Entry::new(sequence, request));
    }
    pub(crate) fn restore(&mut self, entry: Entry) {
        self.sequence = self.sequence.max(entry.sequence + 1);
        let tenant_id = entry.spec.tenant_id.clone();
        let priority = entry.spec.priority;
        if !self.queues.contains_key(&tenant_id) {
            self.tenants.push_back(tenant_id.clone());
        }
        self.queues
            .entry(tenant_id)
            .or_default()
            .insert((Reverse(priority), entry.sequence), entry);
        self.len += 1;
    }
    pub(crate) fn remove(&mut self, id: &str) {
        for queue in self.queues.values_mut() {
            let before = queue.len();
            queue.retain(|_, entry| entry.spec.id != id);
            self.len -= before - queue.len();
        }
        self.queues.retain(|_, q| !q.is_empty());
        self.tenants.retain(|t| self.queues.contains_key(t));
    }
    pub fn len(&self) -> usize {
        self.len
    }
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
    pub(crate) fn pop_entry(&mut self) -> Option<Entry> {
        let tenant = self.tenants.pop_front()?;
        let queue = self.queues.get_mut(&tenant).expect("tenant queue exists");
        let (_, entry) = queue.pop_first().expect("nonempty tenant queue");
        if queue.is_empty() {
            self.queues.remove(&tenant);
        } else {
            self.tenants.push_back(tenant);
        }
        self.len -= 1;
        Some(entry)
    }
    pub fn pop(&mut self) -> Option<EnvironmentSpec> {
        self.pop_entry().map(|e| e.spec)
    }
    pub(crate) fn entries(&self) -> impl Iterator<Item = &Entry> {
        self.queues.values().flat_map(BTreeMap::values)
    }
}

fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}
