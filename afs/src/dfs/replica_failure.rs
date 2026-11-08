//! What a write does when a synchronous replica fails (R12, decision 3).
use serde::{Deserialize, Serialize};

use super::ReplicationConfig;

/// What a write does when a synchronous replica cannot be written (decision 3).
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub enum ReplicaFailurePolicy {
    /// Commit with the copies that did persist, if there are at least
    /// `min_degraded_copies`, and record the missing ones as replica debt in
    /// the same transaction.
    #[default]
    Degrade,
    /// Every synchronous copy or the write fails.
    Strict,
}

/// The immutable filesystem-wide replica failure contract. It lives beside
/// [`ReplicationConfig`] rather than in it: that record is postcard-encoded and
/// cannot gain a field.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ReplicaFailureConfig {
    pub policy: ReplicaFailurePolicy,
    pub min_degraded_copies: u16,
}

impl Default for ReplicaFailureConfig {
    fn default() -> Self {
        Self {
            policy: ReplicaFailurePolicy::Degrade,
            min_degraded_copies: 1,
        }
    }
}

impl ReplicaFailureConfig {
    pub fn strict() -> Self {
        Self {
            policy: ReplicaFailurePolicy::Strict,
            min_degraded_copies: 1,
        }
    }

    pub fn is_valid_for(&self, replication: &ReplicationConfig) -> bool {
        self.min_degraded_copies > 0 && self.min_degraded_copies <= replication.sync_required_copies
    }

    /// The fewest durable copies a write may still commit with, when that is
    /// below the synchronous count. `None` means every write is strict. A
    /// degraded write keeps the local-copy rule; node and failure-domain minima
    /// describe a complete placement and do not apply to it.
    pub fn degraded_floor(&self, replication: &ReplicationConfig) -> Option<usize> {
        match self.policy {
            ReplicaFailurePolicy::Degrade
                if self.min_degraded_copies < replication.sync_required_copies =>
            {
                Some(usize::from(self.min_degraded_copies))
            }
            _ => None,
        }
    }
}

impl ReplicaFailureConfig {
    pub(crate) fn wire(self) -> afs_protocol::meta::DfsReplicaFailureConfig {
        use afs_protocol::meta::DfsReplicaFailurePolicy as Wire;
        afs_protocol::meta::DfsReplicaFailureConfig {
            policy: match self.policy {
                ReplicaFailurePolicy::Degrade => Wire::Degrade as i32,
                ReplicaFailurePolicy::Strict => Wire::Strict as i32,
            },
            min_degraded_copies: u32::from(self.min_degraded_copies),
        }
    }

    /// Rejects a missing or unspecified policy and an out-of-range count; the
    /// caller still checks it against its replication config.
    pub(crate) fn from_wire(
        value: Option<afs_protocol::meta::DfsReplicaFailureConfig>,
    ) -> afs_error::Result<Self> {
        use afs_protocol::meta::DfsReplicaFailurePolicy as Wire;
        let invalid = |message: &str| {
            afs_error::Error::coded(afs_error::CLIENT_PROTOCOL_VIOLATION, message.to_owned())
        };
        let value = value.ok_or_else(|| invalid("DFS replica failure config is required"))?;
        let policy = match Wire::try_from(value.policy) {
            Ok(Wire::Degrade) => ReplicaFailurePolicy::Degrade,
            Ok(Wire::Strict) => ReplicaFailurePolicy::Strict,
            Ok(Wire::Unspecified) | Err(_) => {
                return Err(invalid("DFS replica failure policy is invalid"));
            }
        };
        let min_degraded_copies = u16::try_from(value.min_degraded_copies)
            .ok()
            .filter(|copies| *copies > 0)
            .ok_or_else(|| invalid("DFS min_degraded_copies is invalid"))?;
        Ok(Self {
            policy,
            min_degraded_copies,
        })
    }
}
