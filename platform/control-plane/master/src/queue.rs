use adx_core::InstanceSpec;
use std::{
    cmp::Reverse,
    collections::{BTreeMap, VecDeque},
};

pub(crate) struct Entry {
    pub sequence: u64,
    pub spec: InstanceSpec,
}
#[derive(Default)]
pub struct TenantQueue {
    tenants: VecDeque<String>,
    queues: BTreeMap<String, BTreeMap<(Reverse<i32>, u64), InstanceSpec>>,
    len: usize,
    sequence: u64,
}
impl TenantQueue {
    pub fn push(&mut self, request: InstanceSpec) {
        let sequence = self.sequence;
        self.sequence = self
            .sequence
            .checked_add(1)
            .expect("queue sequence exhausted");
        self.restore(Entry {
            sequence,
            spec: request,
        });
    }
    pub(crate) fn restore(&mut self, entry: Entry) {
        let request = entry.spec;
        self.sequence = self.sequence.max(entry.sequence + 1);
        if !self.queues.contains_key(&request.tenant_id) {
            self.tenants.push_back(request.tenant_id.clone());
        }
        self.queues
            .entry(request.tenant_id.clone())
            .or_default()
            .insert((Reverse(request.priority), entry.sequence), request);
        self.len += 1;
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
        let ((_, sequence), spec) = queue.pop_first().expect("nonempty tenant queue");
        if queue.is_empty() {
            self.queues.remove(&tenant);
        } else {
            self.tenants.push_back(tenant);
        }
        self.len -= 1;
        Some(Entry { sequence, spec })
    }
    pub fn pop(&mut self) -> Option<InstanceSpec> {
        self.pop_entry().map(|e| e.spec)
    }
}
