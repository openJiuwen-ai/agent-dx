use crate::{Error, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LifecyclePolicy {
    /// Zero disables idle deletion; this is not a Environment maximum lifetime.
    pub idle_timeout_seconds: u64,
    pub restart: Option<RestartPolicy>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RestartPolicy {
    pub max_attempts: u32,
    pub initial_backoff_seconds: u64,
    pub max_backoff_seconds: u64,
}
impl LifecyclePolicy {
    pub fn validate(&self) -> Result<()> {
        if let Some(p) = &self.restart {
            if p.max_attempts == 0
                || p.initial_backoff_seconds == 0
                || p.max_backoff_seconds < p.initial_backoff_seconds
            {
                return Err(Error::Invalid(
                    "restart requires attempts and 0 < initial backoff <= maximum backoff".into(),
                ));
            }
        }
        Ok(())
    }
}
impl RestartPolicy {
    pub fn backoff_seconds(&self, completed_attempts: u32) -> u64 {
        self.initial_backoff_seconds
            .saturating_mul(1_u64.checked_shl(completed_attempts).unwrap_or(u64::MAX))
            .min(self.max_backoff_seconds)
    }
}
