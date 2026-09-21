//! Built-ins are ordinary plugins, registered by Framework::builtin/new.
use crate::{Candidate, Filter, Placement, Score, MAX_SCORE};
use adx_core::{CapsuleSpec, Result};

pub struct NodeAvailable;
impl Filter for NodeAvailable {
    fn name(&self) -> &'static str {
        "node-available"
    }
    fn filter(&self, _: &CapsuleSpec, candidate: &Candidate<'_>) -> Result<bool> {
        Ok(candidate.node.available)
    }
}
pub struct ResourceFit;
impl Filter for ResourceFit {
    fn name(&self) -> &'static str {
        "resource-fit"
    }
    fn filter(&self, request: &CapsuleSpec, candidate: &Candidate<'_>) -> Result<bool> {
        Ok(request.resources.fits(&candidate.available))
    }
}
pub struct ResourceBalance(pub Placement);
impl Score for ResourceBalance {
    fn name(&self) -> &'static str {
        "resource-balance"
    }
    fn score(&self, request: &CapsuleSpec, candidate: &Candidate<'_>) -> Result<u32> {
        let available = candidate.available.saturating_sub(request.resources);
        let capacity = candidate.node.capacity;
        let fraction = |free: u64, total: u64| -> u32 {
            if total == 0 {
                0
            } else {
                (u128::from(free.min(total)) * 1_000_000 / u128::from(total)) as u32
            }
        };
        let free = fraction(available.cpu_millis, capacity.cpu_millis)
            + fraction(available.memory_bytes, capacity.memory_bytes)
            + fraction(available.disk_bytes, capacity.disk_bytes);
        Ok(match self.0 {
            Placement::Pack => MAX_SCORE - free,
            Placement::Spread => free,
        })
    }
}
