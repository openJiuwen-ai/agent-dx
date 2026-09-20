//! Shared Dispatcher preference; never assigns ownership of a Session.
use crate::{encode_key, Scope};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DispatcherMember {
    pub node_id: String,
    pub boot_id: String,
    pub address: String,
}
fn hash(parts: &[&str]) -> [u8; 32] {
    Sha256::digest(encode_key(parts).as_bytes()).into()
}
/// Locality preference only. Any live Dispatcher can serve a valid Session.
#[derive(Debug, Clone, Default)]
pub struct HashRing {
    members: Vec<DispatcherMember>,
    points: Vec<([u8; 32], usize)>,
}
impl HashRing {
    pub fn new(mut members: Vec<DispatcherMember>) -> Self {
        members.sort_by(|a, b| a.node_id.cmp(&b.node_id));
        members.dedup_by(|a, b| a.node_id == b.node_id);
        let mut points = Vec::with_capacity(members.len() * 128);
        for (index, member) in members.iter().enumerate() {
            for replica in 0..128 {
                points.push((
                    hash(&["dispatcher-ring-v1", &member.node_id, &replica.to_string()]),
                    index,
                ));
            }
        }
        points.sort_unstable();
        Self { members, points }
    }
    pub fn preferred(&self, scope: &Scope) -> Option<&DispatcherMember> {
        if self.points.is_empty() {
            return None;
        }
        let point = hash(&["session-route-v1", &scope.key()]);
        let index = self.points.partition_point(|(p, _)| *p < point) % self.points.len();
        Some(&self.members[self.points[index].1])
    }
    pub fn members(&self) -> &[DispatcherMember] {
        &self.members
    }
}
