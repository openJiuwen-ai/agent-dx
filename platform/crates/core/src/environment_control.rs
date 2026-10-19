//! Compatibility configuration and a durable absolute runtime lease. Lifecycle
//! state remains owned by EnvironmentRecord; this record never schedules a VM.
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fmt};

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnvironmentCompatibility {
    pub template_id: String,
    pub envd_version: String,
    pub envd_access_token: String,
    pub traffic_access_token: String,
    pub allow_public_traffic: bool,
    pub timeout_seconds: u64,
    pub timeout_action: String,
    #[serde(default = "default_pause_retention")]
    pub pause_retention_seconds: u64,
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
}
fn default_pause_retention() -> u64 {
    90_000
}
impl fmt::Debug for EnvironmentCompatibility {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EnvironmentCompatibility")
            .field("template_id", &self.template_id)
            .field("envd_version", &self.envd_version)
            .field("credentials", &"[redacted]")
            .field("timeout_seconds", &self.timeout_seconds)
            .field("timeout_action", &self.timeout_action)
            .finish()
    }
}
impl EnvironmentCompatibility {
    pub fn validate(&self) -> Result<()> {
        if self.template_id.is_empty()
            || self.envd_version.is_empty()
            || self.envd_access_token.len() < 32
            || self.traffic_access_token.len() < 32
            || self.timeout_seconds == 0
            || self.timeout_seconds > 86_400
            || !matches!(self.timeout_action.as_str(), "delete" | "pause")
            || self.pause_retention_seconds == 0
            || self.metadata.len() > 128
            || self
                .metadata
                .iter()
                .any(|(k, v)| k.is_empty() || k.len() > 256 || v.len() > 4096)
        {
            return Err(Error::Invalid(
                "invalid sandbox compatibility configuration".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvironmentControlRecord {
    pub environment_id: String,
    pub compatibility: EnvironmentCompatibility,
    pub expires_at_unix_ms: u64,
    pub started_at_unix_ms: u64,
    pub revision: u64,
    pub generation: u64,
    pub expiry_claimed: bool,
}
impl EnvironmentControlRecord {
    pub fn new(
        environment_id: String,
        compatibility: EnvironmentCompatibility,
        generation: u64,
    ) -> Self {
        Self {
            environment_id,
            compatibility,
            generation,
            expires_at_unix_ms: 0,
            started_at_unix_ms: 0,
            revision: 1,
            expiry_claimed: false,
        }
    }
    fn advance(&mut self) -> Result<()> {
        self.revision = self.revision.checked_add(1).ok_or(Error::Conflict)?;
        Ok(())
    }
    pub fn begin_run(&mut self, now: u64, generation: u64) -> Result<()> {
        if self.expiry_claimed {
            return Err(Error::Conflict);
        }
        self.expires_at_unix_ms = deadline(now, self.compatibility.timeout_seconds)?;
        self.started_at_unix_ms = now;
        self.generation = generation;
        self.advance()
    }
    pub fn set_timeout(&mut self, now: u64, seconds: u64, extend_only: bool) -> Result<()> {
        if self.expiry_claimed || self.expires_at_unix_ms == 0 || now >= self.expires_at_unix_ms {
            return Err(Error::Conflict);
        }
        let next = deadline(now, seconds)?;
        self.expires_at_unix_ms = if extend_only {
            self.expires_at_unix_ms.max(next)
        } else {
            next
        };
        self.advance()
    }
    pub fn claim_expiry(&mut self, now: u64) -> Result<bool> {
        if self.expiry_claimed || self.expires_at_unix_ms == 0 || now < self.expires_at_unix_ms {
            return Ok(false);
        }
        self.expiry_claimed = true;
        self.advance()?;
        Ok(true)
    }
    pub fn pause(&mut self) -> Result<()> {
        self.expires_at_unix_ms = 0;
        self.expiry_claimed = false;
        self.advance()
    }
    pub fn rebind(&mut self, generation: u64) -> Result<()> {
        if self.generation != generation {
            self.generation = generation;
            self.advance()?;
        }
        Ok(())
    }
}
fn deadline(now: u64, seconds: u64) -> Result<u64> {
    if seconds > 86_400 {
        return Err(Error::Invalid("timeout exceeds 86400 seconds".into()));
    }
    now.checked_add(seconds.checked_mul(1000).ok_or(Error::Conflict)?)
        .ok_or(Error::Conflict)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn record() -> EnvironmentControlRecord {
        EnvironmentControlRecord::new(
            "s".into(),
            EnvironmentCompatibility {
                template_id: "base".into(),
                envd_version: "0.9.0".into(),
                envd_access_token: "a".repeat(32),
                traffic_access_token: "b".repeat(32),
                allow_public_traffic: true,
                timeout_seconds: 60,
                timeout_action: "delete".into(),
                pause_retention_seconds: 90_000,
                metadata: BTreeMap::new(),
            },
            1,
        )
    }
    #[test]
    fn timeout_is_absolute_connect_only_extends_and_expiry_cannot_revive() {
        let mut c = record();
        c.begin_run(1000, 1).unwrap();
        c.set_timeout(2000, 10, true).unwrap();
        assert_eq!(c.expires_at_unix_ms, 61000);
        c.set_timeout(3000, 2, false).unwrap();
        assert_eq!(c.expires_at_unix_ms, 5000);
        assert!(c.set_timeout(5000, 60, false).is_err());
        assert!(c.claim_expiry(5000).unwrap());
        assert!(!c.claim_expiry(5000).unwrap());
        assert!(c.begin_run(6000, 2).is_err());
    }
    #[test]
    fn zero_timeout_expires_now_and_rebind_does_not_extend() {
        let mut c = record();
        c.begin_run(1000, 1).unwrap();
        c.rebind(2).unwrap();
        assert_eq!(c.expires_at_unix_ms, 61000);
        c.set_timeout(2000, 0, false).unwrap();
        assert!(c.claim_expiry(2000).unwrap());
    }
    #[test]
    fn explicit_pause_creates_new_run_but_debug_redacts_credentials() {
        let mut c = record();
        c.begin_run(1000, 1).unwrap();
        c.pause().unwrap();
        c.begin_run(90000, 2).unwrap();
        assert_eq!(c.expires_at_unix_ms, 150000);
        assert!(!format!("{c:?}").contains(&"a".repeat(32)));
    }
}
