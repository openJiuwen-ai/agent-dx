use adx_core::CapsuleSpec;
use std::{
    cmp::Reverse,
    collections::{BTreeMap, VecDeque},
};

pub(crate) struct Entry {
    pub sequence: u64,
    pub spec: CapsuleSpec,
}
#[derive(Default)]
pub struct TenantQueue {
    tenants: VecDeque<String>,
    queues: BTreeMap<String, BTreeMap<(Reverse<i32>, u64), CapsuleSpec>>,
    len: usize,
    sequence: u64,
}
impl TenantQueue {
    pub fn push(&mut self, request: CapsuleSpec) {
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
    pub(crate) fn remove(&mut self, id: &str) {
        for queue in self.queues.values_mut() {
            let before = queue.len();
            queue.retain(|_, spec| spec.id != id);
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
        let ((_, sequence), spec) = queue.pop_first().expect("nonempty tenant queue");
        if queue.is_empty() {
            self.queues.remove(&tenant);
        } else {
            self.tenants.push_back(tenant);
        }
        self.len -= 1;
        Some(Entry { sequence, spec })
    }
    pub fn pop(&mut self) -> Option<CapsuleSpec> {
        self.pop_entry().map(|e| e.spec)
    }
}
