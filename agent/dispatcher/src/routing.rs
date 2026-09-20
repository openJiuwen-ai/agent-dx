use adx_agent_core::{encode_key, Scope};
use sha2::{Digest, Sha256};

fn hash(parts: &[&str]) -> [u8; 32] {
    Sha256::digest(encode_key(parts).as_bytes()).into()
}

pub use adx_agent_core::routing::HashRing;

/// Per-process/per-Session seed, then a local cursor. Never written to Redis.
#[derive(Debug)]
pub struct RoundRobin {
    seed: u64,
    next: u64,
}
impl RoundRobin {
    pub fn new(boot_id: &str, scope: &Scope) -> Self {
        let digest = hash(&["instance-cursor-v1", boot_id, &scope.key()]);
        Self::with_seed(u64::from_be_bytes(
            digest[..8].try_into().expect("eight bytes"),
        ))
    }
    pub fn with_seed(seed: u64) -> Self {
        Self { seed, next: 0 }
    }
    pub fn choose<'a>(&mut self, sorted_ids: &'a [String]) -> Option<&'a str> {
        if sorted_ids.is_empty() {
            return None;
        }
        let index = (self.seed.wrapping_add(self.next) % sorted_ids.len() as u64) as usize;
        self.next = self.next.wrapping_add(1);
        Some(&sorted_ids[index])
    }
}
